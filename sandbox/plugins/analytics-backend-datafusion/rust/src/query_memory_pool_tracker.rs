/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Per-query memory tracking via DataFusion's MemoryPool trait.
//!
//! Each query gets its own [`QueryMemoryPool`] that wraps the global pool.
//! All allocations flow through the global pool (so the global limit is
//! still enforced), but each query also tracks its own current and peak
//! usage independently.
//!
//! [`QueryTrackingContext`] owns the per-query pool and tracker, auto-registers
//! in the global [`QueryRegistry`] on creation, and marks the query completed
//! on [`Drop`]. The registry retains completed entries so Java can retrieve
//! final metrics via JNI before explicitly draining them.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Once};
use std::time::Instant;

use dashmap::DashMap;
use datafusion::execution::memory_pool::GreedyMemoryPool;
use once_cell::sync::Lazy;
use log::{debug, info};

use datafusion::common::DataFusionError;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};

// ---------------------------------------------------------------------------
// TEMP: synthetic prefill for FFM-cost benchmarking
//
// On the first call to either `count_active_queries` or `collect_active_query_stats`,
// seed QUERY_REGISTRY with PREFILL_TRACKER_COUNT synthetic trackers so we can
// measure the end-to-end FFM + registry-walk cost in the probe logs without
// needing real queries to land in the registry. Remove before merging.
// ---------------------------------------------------------------------------

const PREFILL_TRACKER_COUNT: i64 = 100;
const PREFILL_CONTEXT_ID_BASE: i64 = 900_000_000;
static PREFILL_ONCE: Once = Once::new();

// ---------------------------------------------------------------------------
// Per-query memory pool
// ---------------------------------------------------------------------------

/// A per-query MemoryPool that delegates to a shared global pool while
/// independently tracking this query's current and peak memory usage.
#[derive(Debug)]
pub struct QueryMemoryPool {
    inner: Arc<dyn MemoryPool>,
    current_bytes: AtomicUsize,
    peak_bytes: AtomicUsize,
}

impl QueryMemoryPool {
    pub fn new(inner: Arc<dyn MemoryPool>) -> Self {
        Self {
            inner,
            current_bytes: AtomicUsize::new(0),
            peak_bytes: AtomicUsize::new(0),
        }
    }

    pub fn current_bytes(&self) -> usize {
        self.current_bytes.load(Ordering::Relaxed)
    }

    pub fn peak_bytes(&self) -> usize {
        self.peak_bytes.load(Ordering::Relaxed)
    }

    fn track_grow(&self, additional: usize) {
        let old = self.current_bytes.fetch_add(additional, Ordering::Relaxed);
        self.peak_bytes
            .fetch_max(old + additional, Ordering::Relaxed);
    }

    fn track_shrink(&self, shrink: usize) {
        self.current_bytes.fetch_sub(shrink, Ordering::Relaxed);
    }
}

impl MemoryPool for QueryMemoryPool {
    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer);
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer);
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
        self.track_grow(additional);
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.track_shrink(shrink);
        self.inner.shrink(reservation, shrink);
    }

    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> Result<(), DataFusionError> {
        self.inner.try_grow(reservation, additional)?;
        self.track_grow(additional);
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }
}

// ---------------------------------------------------------------------------
// Per-query tracker (metrics snapshot)
// ---------------------------------------------------------------------------

/// Holds per-query metrics: memory pool reference, wall-clock timing, and
/// completion status. Shared via `Arc` between the context and the registry.
#[derive(Debug)]
pub struct QueryTracker {
    pub start_time: Instant,
    pub context_id: i64,
    pub memory_pool: Arc<QueryMemoryPool>,
    completed: AtomicBool,
    wall_nanos: std::sync::atomic::AtomicU64,
}

impl QueryTracker {
    /// Wall-clock duration. Returns the frozen snapshot if completed,
    /// otherwise returns live elapsed time.
    pub fn wall_secs(&self) -> f64 {
        let nanos = self.wall_nanos.load(Ordering::Acquire);
        if nanos > 0 {
            nanos as f64 / 1_000_000_000.0
        } else {
            self.start_time.elapsed().as_secs_f64()
        }
    }

    pub fn is_completed(&self) -> bool {
        self.completed.load(Ordering::Acquire)
    }

    /// Snapshot wall time and mark completed.
    fn mark_completed(&self) {
        let elapsed_nanos = self.start_time.elapsed().as_nanos() as u64;
        self.wall_nanos.store(elapsed_nanos, Ordering::Release);
        self.completed.store(true, Ordering::Release);
    }
}

// ---------------------------------------------------------------------------
// Global registry
// ---------------------------------------------------------------------------

// Crate-visible so `ffm.rs` tests can insert/remove entries directly when
// exercising the two-phase FFM functions. Not part of the public API.
pub(crate) static QUERY_REGISTRY: Lazy<DashMap<i64, Arc<QueryTracker>>> = Lazy::new(DashMap::new);

/// Remove a completed tracker from the registry and return it.
/// Called from JNI after Java has consumed the metrics.
pub fn drain_completed_query(context_id: i64) -> Option<Arc<QueryTracker>> {
    QUERY_REGISTRY
        .remove_if(&context_id, |_, t| t.is_completed())
        .map(|(_, t)| t)
}

/// One-shot synthetic prefill used for FFM-cost benchmarking. Idempotent —
/// only the first call actually inserts trackers. Invoked by both
/// [`count_active_queries`] and [`collect_active_query_stats`] so the first
/// call on either path triggers the prefill.
fn ensure_prefill() {
    // TEMP: one-shot prefill of synthetic trackers so the FFM path has something
    // to walk while we measure its cost. Remove before merging.
    PREFILL_ONCE.call_once(|| {
        let global: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(usize::MAX));
        for i in 0..PREFILL_TRACKER_COUNT {
            let ctx_id = PREFILL_CONTEXT_ID_BASE + i;
            let query_pool = Arc::new(QueryMemoryPool::new(Arc::clone(&global)));
            // Seed current/peak to non-zero values so the log lines are distinguishable.
            query_pool
                .current_bytes
                .store((1024 * (i as usize + 1)) as usize, Ordering::Relaxed);
            query_pool
                .peak_bytes
                .store((2048 * (i as usize + 1)) as usize, Ordering::Relaxed);
            let tracker = Arc::new(QueryTracker {
                start_time: Instant::now(),
                context_id: ctx_id,
                memory_pool: query_pool,
                completed: AtomicBool::new(false),
                wall_nanos: std::sync::atomic::AtomicU64::new(0),
            });
            QUERY_REGISTRY.insert(ctx_id, tracker);
        }
        eprintln!(
            "SBP-NM[rust]: PREFILL seeded {} synthetic trackers into QUERY_REGISTRY (registry_size={})",
            PREFILL_TRACKER_COUNT,
            QUERY_REGISTRY.len()
        );
    });
}

/// Count the number of active (not-completed) trackers currently registered in
/// [`QUERY_REGISTRY`]. Read-only: does not modify the registry.
///
/// This is the Phase 1 helper for the two-phase FFM snapshot protocol (see
/// `df_active_query_stats_size`). The returned value is a **triple count** —
/// i.e. the number of `(context_id, current_bytes, peak_bytes)` triples a
/// subsequent [`collect_active_query_stats`] call would emit if given enough
/// capacity — not a long count.
pub fn count_active_queries() -> i64 {
    ensure_prefill();

    let walk_start = Instant::now();
    let registry_size = QUERY_REGISTRY.len();

    let mut active: i64 = 0;
    let mut completed_skipped: usize = 0;
    for entry in QUERY_REGISTRY.iter() {
        if entry.value().is_completed() {
            completed_skipped += 1;
        } else {
            active += 1;
        }
    }

    let walk_elapsed = walk_start.elapsed();
    eprintln!(
        "SBP-NM[rust]: count_active_queries -> {} active, {} completed skipped (registry_size={}) walk_us={} walk_ns={}",
        active,
        completed_skipped,
        registry_size,
        walk_elapsed.as_micros(),
        walk_elapsed.as_nanos(),
    );
    info!(
        "SBP-NM[rust]: count_active_queries -> {} active, {} completed skipped (registry_size={}) walk_us={}",
        active,
        completed_skipped,
        registry_size,
        walk_elapsed.as_micros(),
    );
    active
}

/// Collect `(context_id, current_bytes, peak_bytes)` triples for every active
/// (not-completed) query currently registered in [`QUERY_REGISTRY`], up to
/// `max_triples` triples.
///
/// This is the Phase 2 helper for the two-phase FFM snapshot protocol (see
/// `df_active_query_stats_copy`). It is a read-only sampler intended for
/// per-cycle polling by SBP via that FFM entry point. It does not modify the
/// registry; completed trackers are left in place so that
/// [`drain_completed_query`] remains the single owner of removal.
///
/// If the number of active queries exceeds `max_triples`, the function
/// **truncates silently** at `max_triples` — the returned `Vec` has length
/// exactly `max_triples` and the remaining active entries are left for the
/// next cycle. There is no error return path for this case; the caller observes
/// the truncation by comparing `max_triples` against the size probe.
pub fn collect_active_query_stats(max_triples: usize) -> Vec<(i64, i64, i64)> {
    ensure_prefill();

    let walk_start = Instant::now();
    let registry_size = QUERY_REGISTRY.len();
    eprintln!(
        "SBP-NM[rust]: collect_active_query_stats(max_triples={}), registry_size={}",
        max_triples, registry_size
    );
    debug!(
        "SBP-NM[rust]: collect_active_query_stats(max_triples={}), registry_size={}",
        max_triples, registry_size
    );

    let mut out = Vec::new();
    let mut completed_skipped: usize = 0;
    let mut truncated: usize = 0;
    for entry in QUERY_REGISTRY.iter() {
        if entry.value().is_completed() {
            completed_skipped += 1;
            continue;
        }
        if out.len() >= max_triples {
            // Silent truncation — the registry grew between the size probe and
            // this copy. Leave the remainder for the next SBP cycle.
            truncated += 1;
            continue;
        }
        let ctx = *entry.key();
        let current = entry.value().memory_pool.current_bytes() as i64;
        let peak = entry.value().memory_pool.peak_bytes() as i64;
        debug!(
            "SBP-NM[rust]:   active query ctx={} current={} peak={}",
            ctx, current, peak
        );
        out.push((ctx, current, peak));
    }

    let walk_elapsed = walk_start.elapsed();
    eprintln!(
        "SBP-NM[rust]: collect_active_query_stats -> {} active emitted, {} truncated, {} completed skipped (registry_size={}) walk_us={} walk_ns={}",
        out.len(),
        truncated,
        completed_skipped,
        registry_size,
        walk_elapsed.as_micros(),
        walk_elapsed.as_nanos(),
    );
    info!(
        "SBP-NM[rust]: collect_active_query_stats -> {} active emitted, {} truncated, {} completed skipped (registry_size={}) walk_us={}",
        out.len(),
        truncated,
        completed_skipped,
        registry_size,
        walk_elapsed.as_micros(),
    );
    out
}

// ---------------------------------------------------------------------------
// QueryTrackingContext
// ---------------------------------------------------------------------------

/// Per-query context that owns the memory pool and tracker.
///
/// - On creation: registers the tracker in the global registry.
/// - On [`Drop`]: marks the tracker completed and logs final metrics.
///   The tracker stays in the registry for JNI retrieval.
///
/// For `context_id == 0` (unset), no tracking is performed.
#[derive(Debug)]
pub struct QueryTrackingContext {
    tracker: Option<Arc<QueryTracker>>,
}

impl QueryTrackingContext {
    /// Create a new query context. If `context_id` is 0, tracking is
    /// disabled and `memory_pool()` returns `None`.
    pub fn new(context_id: i64, global_pool: Arc<dyn MemoryPool>) -> Self {
        if context_id == 0 {
            return Self { tracker: None };
        }
        let query_pool = Arc::new(QueryMemoryPool::new(global_pool));
        let tracker = Arc::new(QueryTracker {
            start_time: Instant::now(),
            context_id,
            memory_pool: query_pool,
            completed: AtomicBool::new(false),
            wall_nanos: std::sync::atomic::AtomicU64::new(0),
        });
        QUERY_REGISTRY.insert(context_id, Arc::clone(&tracker));
        eprintln!(
            "SBP-NM[rust]: QueryTrackingContext::new registered ctx={} (registry_size={})",
            context_id,
            QUERY_REGISTRY.len()
        );
        Self {
            tracker: Some(tracker),
        }
    }

    /// The per-query memory pool to install in a `RuntimeEnv`, or `None`
    /// if tracking is disabled.
    pub fn memory_pool(&self) -> Option<Arc<QueryMemoryPool>> {
        self.tracker.as_ref().map(|t| Arc::clone(&t.memory_pool))
    }
}

impl Drop for QueryTrackingContext {
    fn drop(&mut self) {
        if let Some(tracker) = &self.tracker {
            tracker.mark_completed();
            eprintln!(
                "SBP-NM[rust]: QueryTrackingContext::drop ctx={} wall={:.3}s current={}B peak={}B",
                tracker.context_id,
                tracker.wall_secs(),
                tracker.memory_pool.current_bytes(),
                tracker.memory_pool.peak_bytes(),
            );
            debug!(
                "Query completed ctx={}: wall={:.3}s, mem_current={}B, mem_peak={}B",
                tracker.context_id,
                tracker.wall_secs(),
                tracker.memory_pool.current_bytes(),
                tracker.memory_pool.peak_bytes(),
            );
        }
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    //! Crate-visible helpers for tests that stress `QUERY_REGISTRY`.
    //!
    //! The two property tests (one in this module, one in `ffm::tests`) both
    //! insert, mutate, and remove trackers in tight loops. Running them in
    //! parallel occasionally triggers DashMap iteration artifacts where a
    //! just-inserted entry is missed by an ongoing iterator. To keep both
    //! tests deterministic they acquire this shared mutex for the duration of
    //! their run, serializing them with respect to each other while still
    //! allowing all other tests (which don't stress the registry nearly as
    //! hard) to run concurrently.
    use std::sync::Mutex;
    pub static PROPERTY_TEST_LOCK: Mutex<()> = Mutex::new(());
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::GreedyMemoryPool;
    use std::thread;
    use std::time::Duration;

    fn make_global_pool(limit: usize) -> Arc<dyn MemoryPool> {
        Arc::new(GreedyMemoryPool::new(limit))
    }

    fn make_reservation(pool: &Arc<dyn MemoryPool>, name: &str) -> MemoryReservation {
        MemoryConsumer::new(name).register(pool)
    }

    // -----------------------------------------------------------------------
    // QueryMemoryPool tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_query_pool_tracks_current_and_peak() {
        let global = make_global_pool(1_000_000);
        let qp = Arc::new(QueryMemoryPool::new(global));
        let pool: Arc<dyn MemoryPool> = qp.clone();
        let mut reservation = make_reservation(&pool, "test");

        reservation.try_grow(1000).unwrap();
        assert_eq!(qp.current_bytes(), 1000);
        assert_eq!(qp.peak_bytes(), 1000);

        reservation.try_grow(500).unwrap();
        assert_eq!(qp.current_bytes(), 1500);
        assert_eq!(qp.peak_bytes(), 1500);

        reservation.shrink(800);
        assert_eq!(qp.current_bytes(), 700);
        assert_eq!(qp.peak_bytes(), 1500);

        reservation.try_grow(200).unwrap();
        assert_eq!(qp.current_bytes(), 900);
        assert_eq!(qp.peak_bytes(), 1500);
    }

    #[test]
    fn test_query_pool_current_returns_to_zero_on_drop() {
        let global = make_global_pool(1_000_000);
        let qp = Arc::new(QueryMemoryPool::new(global));
        let pool: Arc<dyn MemoryPool> = qp.clone();

        {
            let mut reservation = make_reservation(&pool, "test");
            reservation.try_grow(5000).unwrap();
            assert_eq!(qp.current_bytes(), 5000);
            assert_eq!(qp.peak_bytes(), 5000);
        }

        assert_eq!(qp.current_bytes(), 0);
        assert_eq!(qp.peak_bytes(), 5000);
    }

    #[test]
    fn test_query_pool_delegates_reserved_to_inner() {
        let global = make_global_pool(1_000_000);
        let qp = Arc::new(QueryMemoryPool::new(global));
        let pool: Arc<dyn MemoryPool> = qp.clone();
        let mut reservation = make_reservation(&pool, "test");

        reservation.try_grow(2000).unwrap();
        assert!(pool.reserved() >= 2000);
    }

    // -----------------------------------------------------------------------
    // QueryTrackingContext lifecycle tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_context_returns_none_pool_for_zero_id() {
        let global = make_global_pool(10_000);
        let ctx = QueryTrackingContext::new(0, global);
        assert!(ctx.memory_pool().is_none());
    }

    #[test]
    fn test_context_registers_in_registry() {
        let global = make_global_pool(10_000);
        let ctx_id = 50_000;
        let ctx = QueryTrackingContext::new(ctx_id, global);
        assert!(ctx.memory_pool().is_some());
        assert!(QUERY_REGISTRY.contains_key(&ctx_id));

        drop(ctx);
        // Still in registry after drop (completed, not drained)
        assert!(QUERY_REGISTRY.contains_key(&ctx_id));
        assert!(QUERY_REGISTRY.get(&ctx_id).unwrap().is_completed());

        // Drain removes it
        let drained = drain_completed_query(ctx_id);
        assert!(drained.is_some());
        assert!(!QUERY_REGISTRY.contains_key(&ctx_id));
    }

    #[test]
    fn test_drop_marks_completed_and_freezes_wall_time() {
        let global = make_global_pool(10_000);
        let ctx_id = 50_001;
        let ctx = QueryTrackingContext::new(ctx_id, global);

        thread::sleep(Duration::from_millis(50));
        drop(ctx);

        let tracker = QUERY_REGISTRY.get(&ctx_id).unwrap();
        assert!(tracker.is_completed());
        let frozen = tracker.wall_secs();
        thread::sleep(Duration::from_millis(50));
        assert!((tracker.wall_secs() - frozen).abs() < 0.001);

        drop(tracker);
        QUERY_REGISTRY.remove(&ctx_id);
    }

    #[test]
    fn test_wall_secs_ticks_while_running() {
        let global = make_global_pool(10_000);
        let ctx_id = 50_002;
        let _ctx = QueryTrackingContext::new(ctx_id, global);

        let t1 = QUERY_REGISTRY.get(&ctx_id).unwrap().wall_secs();
        thread::sleep(Duration::from_millis(50));
        let t2 = QUERY_REGISTRY.get(&ctx_id).unwrap().wall_secs();
        assert!(t2 - t1 >= 0.04);

        drop(_ctx);
        QUERY_REGISTRY.remove(&ctx_id);
    }

    #[test]
    fn test_drain_returns_none_for_active_query() {
        let global = make_global_pool(10_000);
        let ctx_id = 50_003;
        let _ctx = QueryTrackingContext::new(ctx_id, global);

        // Cannot drain while still active
        assert!(drain_completed_query(ctx_id).is_none());
        assert!(QUERY_REGISTRY.contains_key(&ctx_id));

        drop(_ctx);
        assert!(drain_completed_query(ctx_id).is_some());
    }

    #[test]
    fn test_drain_nonexistent_is_none() {
        assert!(drain_completed_query(99_999).is_none());
    }

    #[test]
    fn test_memory_tracking_through_full_lifecycle() {
        let global = make_global_pool(1_000_000);
        let ctx_id = 50_004;
        let ctx = QueryTrackingContext::new(ctx_id, global);
        let qp = ctx.memory_pool().unwrap();
        let pool: Arc<dyn MemoryPool> = qp.clone();
        let mut reservation = make_reservation(&pool, "lifecycle_test");

        reservation.try_grow(5000).unwrap();
        assert_eq!(qp.current_bytes(), 5000);
        assert_eq!(qp.peak_bytes(), 5000);

        reservation.try_grow(3000).unwrap();
        assert_eq!(qp.current_bytes(), 8000);
        assert_eq!(qp.peak_bytes(), 8000);

        reservation.shrink(6000);
        assert_eq!(qp.current_bytes(), 2000);
        assert_eq!(qp.peak_bytes(), 8000);

        // Drop context — marks completed
        drop(ctx);
        {
            let tracker = QUERY_REGISTRY.get(&ctx_id).unwrap();
            assert!(tracker.is_completed());
            assert_eq!(tracker.memory_pool.peak_bytes(), 8000);
            assert!(tracker.wall_secs() > 0.0);
        }

        // Drop reservation — current goes to 0, peak stays
        drop(reservation);
        assert_eq!(qp.current_bytes(), 0);
        assert_eq!(qp.peak_bytes(), 8000);

        QUERY_REGISTRY.remove(&ctx_id);
    }

    #[test]
    fn test_multiple_concurrent_queries() {
        let global = make_global_pool(1_000_000);
        let ctx_a_id = 50_005;
        let ctx_b_id = 50_006;

        let ctx_a = QueryTrackingContext::new(ctx_a_id, Arc::clone(&global));
        let ctx_b = QueryTrackingContext::new(ctx_b_id, Arc::clone(&global));

        let pool_a = ctx_a.memory_pool().unwrap();
        let pool_b = ctx_b.memory_pool().unwrap();

        let mut res_a = make_reservation(&(pool_a.clone() as Arc<dyn MemoryPool>), "query_a");
        res_a.try_grow(3000).unwrap();

        let mut res_b = make_reservation(&(pool_b.clone() as Arc<dyn MemoryPool>), "query_b");
        res_b.try_grow(7000).unwrap();

        assert_eq!(pool_a.current_bytes(), 3000);
        assert_eq!(pool_b.current_bytes(), 7000);
        assert!(global.reserved() >= 10000);

        // Drop one, other keeps running
        drop(ctx_a);
        assert!(QUERY_REGISTRY.get(&ctx_a_id).unwrap().is_completed());
        assert!(!QUERY_REGISTRY.get(&ctx_b_id).unwrap().is_completed());

        drop(res_a);
        drop(res_b);
        drop(ctx_b);
        QUERY_REGISTRY.remove(&ctx_a_id);
        QUERY_REGISTRY.remove(&ctx_b_id);
    }

    // -----------------------------------------------------------------------
    // Query lifecycle tests (simulating stream completion and error paths)
    // -----------------------------------------------------------------------

    #[test]
    fn test_context_completes_on_normal_drop_with_stream() {
        // Simulates: query succeeds → stream is consumed → handle dropped
        let global = make_global_pool(1_000_000);
        let ctx_id = 50_010;

        let ctx = QueryTrackingContext::new(ctx_id, global);
        let qp = ctx.memory_pool().unwrap();
        let pool: Arc<dyn MemoryPool> = qp.clone();
        let mut reservation = make_reservation(&pool, "stream_data");

        // Simulate allocations during stream consumption
        reservation.try_grow(4000).unwrap();
        assert_eq!(qp.peak_bytes(), 4000);
        assert!(!QUERY_REGISTRY.get(&ctx_id).unwrap().is_completed());

        // Stream fully consumed — reservation and context dropped together
        // (mirrors QueryStreamHandle being dropped in streamClose)
        drop(reservation);
        drop(ctx);

        let tracker = QUERY_REGISTRY.get(&ctx_id).unwrap();
        assert!(tracker.is_completed());
        assert_eq!(tracker.memory_pool.peak_bytes(), 4000);
        assert_eq!(tracker.memory_pool.current_bytes(), 0);
        assert!(tracker.wall_secs() > 0.0);

        drop(tracker);
        QUERY_REGISTRY.remove(&ctx_id);
    }

    #[test]
    fn test_context_completes_on_error_drop() {
        // Simulates: query execution fails → context dropped without
        // explicit cleanup (the error path in executeQueryPhaseAsync)
        let global = make_global_pool(1_000_000);
        let ctx_id = 50_011;

        {
            let ctx = QueryTrackingContext::new(ctx_id, global);
            let _pool = ctx.memory_pool();
            assert!(!QUERY_REGISTRY.get(&ctx_id).unwrap().is_completed());

            // Simulate error: context goes out of scope, no stream was created
        } // ctx dropped here — should still mark completed

        let tracker = QUERY_REGISTRY.get(&ctx_id).unwrap();
        assert!(tracker.is_completed());
        assert_eq!(tracker.memory_pool.peak_bytes(), 0);
        assert_eq!(tracker.memory_pool.current_bytes(), 0);

        drop(tracker);
        let drained = drain_completed_query(ctx_id);
        assert!(drained.is_some());
    }

    // -----------------------------------------------------------------------
    // collect_active_query_stats tests (Task 4)
    //
    // NOTE: QUERY_REGISTRY is a process-wide static shared across all tests
    // in this module. These tests use unique context IDs in the 60_000+ range
    // and filter the result to only their own IDs so other tests running in
    // parallel don't interfere.
    // -----------------------------------------------------------------------

    /// 4.1 — With no entries registered by this test, `collect_active_query_stats`
    /// returns a `Vec` that does NOT contain any triple for context IDs we
    /// never inserted. (Weak check: the registry may hold entries from other
    /// tests running in parallel; the stronger filter-by-our-ids pattern is
    /// exercised by tests 4.2–4.5.)
    #[test]
    fn test_collect_active_empty_registry() {
        let never_inserted_id: i64 = 60_001;
        // Defensive: ensure our id is absent (no other test uses 60_001).
        QUERY_REGISTRY.remove(&never_inserted_id);

        let triples = collect_active_query_stats(1024);
        assert!(
            triples.iter().all(|(ctx, _, _)| *ctx != never_inserted_id),
            "result unexpectedly contained a triple for never-inserted id {}",
            never_inserted_id
        );
    }

    /// 4.2 — Insert N=3 active trackers with unique ids; the returned Vec
    /// contains all three, their context_ids match, and the values are sane
    /// (current=0, peak=0 since no reservations were made).
    #[test]
    fn test_collect_active_returns_inserted_triples() {
        let global = make_global_pool(1_000_000);
        let ids: [i64; 3] = [60_010, 60_011, 60_012];
        let contexts: Vec<QueryTrackingContext> = ids
            .iter()
            .map(|id| QueryTrackingContext::new(*id, Arc::clone(&global)))
            .collect();

        let triples = collect_active_query_stats(1024);

        // Filter to only our test ids.
        let ours: Vec<&(i64, i64, i64)> = triples.iter().filter(|(ctx, _, _)| ids.contains(ctx)).collect();
        assert_eq!(
            ours.len(),
            ids.len(),
            "expected all {} inserted ids to appear, got {}: ours={:?}",
            ids.len(),
            ours.len(),
            ours
        );
        for id in &ids {
            let found = ours.iter().find(|(ctx, _, _)| ctx == id).expect("id present");
            assert_eq!(found.1, 0, "current_bytes for {}", id);
            assert_eq!(found.2, 0, "peak_bytes for {}", id);
        }

        // Cleanup: drop contexts and remove from registry.
        drop(contexts);
        for id in &ids {
            QUERY_REGISTRY.remove(id);
        }
    }

    /// 4.3 — Completed trackers are skipped. Insert 2 active + 2 completed
    /// (dropped) trackers. The result contains only the active ones.
    #[test]
    fn test_collect_active_skips_completed() {
        let global = make_global_pool(1_000_000);
        let active_ids: [i64; 2] = [60_020, 60_021];
        let completed_ids: [i64; 2] = [60_022, 60_023];

        let _active: Vec<QueryTrackingContext> = active_ids
            .iter()
            .map(|id| QueryTrackingContext::new(*id, Arc::clone(&global)))
            .collect();

        // Create and immediately drop — these are now "completed" trackers
        // that linger in the registry until drained.
        for id in &completed_ids {
            let ctx = QueryTrackingContext::new(*id, Arc::clone(&global));
            drop(ctx);
            assert!(QUERY_REGISTRY.get(id).unwrap().is_completed());
        }

        let triples = collect_active_query_stats(1024);
        let returned_ids: Vec<i64> = triples.iter().map(|(ctx, _, _)| *ctx).collect();

        for id in &active_ids {
            assert!(returned_ids.contains(id), "active id {} missing from result", id);
        }
        for id in &completed_ids {
            assert!(
                !returned_ids.contains(id),
                "completed id {} unexpectedly present in result",
                id
            );
        }

        // Cleanup.
        drop(_active);
        for id in active_ids.iter().chain(completed_ids.iter()) {
            QUERY_REGISTRY.remove(id);
        }
    }

    /// 4.5 (helper test) — If the number of active trackers exceeds `max_triples`,
    /// `collect_active_query_stats` silently truncates: the returned `Vec` has
    /// length exactly `max_triples`, every returned id is one of ours, and no
    /// error is produced.
    #[test]
    fn test_collect_active_truncates_silently() {
        let global = make_global_pool(1_000_000);
        let active_ids: [i64; 5] = [60_030, 60_031, 60_032, 60_033, 60_034];
        let completed_ids: [i64; 2] = [60_035, 60_036];

        let _active: Vec<QueryTrackingContext> = active_ids
            .iter()
            .map(|id| QueryTrackingContext::new(*id, Arc::clone(&global)))
            .collect();

        // Completed trackers must also be skipped during truncation.
        for id in &completed_ids {
            let ctx = QueryTrackingContext::new(*id, Arc::clone(&global));
            drop(ctx);
            assert!(QUERY_REGISTRY.get(id).unwrap().is_completed());
        }

        // cap=2 with 5 active inserted — must return a Vec of length exactly 2.
        // Other tests running in parallel may have also inserted active trackers,
        // so we only assert the length and that returned ids are non-completed.
        let triples = collect_active_query_stats(2);
        assert_eq!(
            triples.len(),
            2,
            "expected truncation at 2 triples, got {} (triples={:?})",
            triples.len(),
            triples
        );
        let active_set: std::collections::HashSet<i64> = active_ids.iter().copied().collect();
        let completed_set: std::collections::HashSet<i64> = completed_ids.iter().copied().collect();
        for (ctx, _cur, _peak) in &triples {
            assert!(
                !completed_set.contains(ctx),
                "completed id {} unexpectedly returned by truncated collect",
                ctx
            );
            // We cannot assert every id is one of ours because other tests in
            // the same module may have live trackers too. But we can assert
            // that if it IS one of ours, it's in the active set (not completed).
            if active_set.contains(ctx) || completed_set.contains(ctx) {
                assert!(active_set.contains(ctx), "id {} must be active, not completed", ctx);
            }
        }

        // Cleanup.
        drop(_active);
        for id in active_ids.iter().chain(completed_ids.iter()) {
            QUERY_REGISTRY.remove(id);
        }
    }

    /// 4.5 — Property-style: across randomized allocation histories against
    /// real QueryMemoryPools, every emitted triple satisfies `current <= peak`.
    ///
    /// Uses a deterministic LCG seeded from the iteration index so the test
    /// is reproducible while still exercising many (grow, shrink) sequences.
    #[test]
    fn test_collect_active_current_le_peak_property() {
        // Serialize against the sibling property test in `ffm::tests` — both
        // churn QUERY_REGISTRY hard enough to trigger DashMap iteration
        // artifacts when run in parallel.
        let _lock = crate::query_memory_pool_tracker::test_support::PROPERTY_TEST_LOCK
            .lock()
            .unwrap();

        const ITERATIONS: u64 = 50;
        const TRACKERS_PER_ITER: usize = 10;
        const OPS_PER_TRACKER: usize = 5;
        const BASE_ID: i64 = 60_100;

        // Simple LCG (Numerical Recipes): x_{n+1} = 1664525 * x_n + 1013904223 (mod 2^32)
        fn lcg_next(state: &mut u64) -> u64 {
            *state = state.wrapping_mul(1664525).wrapping_add(1013904223) & 0xFFFF_FFFF;
            *state
        }

        for iter in 0..ITERATIONS {
            let global = make_global_pool(10_000_000);
            let mut seed: u64 = iter.wrapping_mul(2654435761) ^ 0xDEAD_BEEF;

            // Build trackers with reservations we can mutate.
            struct Holder {
                id: i64,
                ctx: QueryTrackingContext,
                reservation: MemoryReservation,
                live_bytes: usize,
            }

            let mut holders: Vec<Holder> = Vec::with_capacity(TRACKERS_PER_ITER);
            for j in 0..TRACKERS_PER_ITER {
                let id = BASE_ID + (iter as i64) * 1_000 + j as i64;
                QUERY_REGISTRY.remove(&id); // defensive cleanup from prior runs
                let ctx = QueryTrackingContext::new(id, Arc::clone(&global));
                let qp = ctx.memory_pool().expect("tracker installed");
                let pool: Arc<dyn MemoryPool> = qp.clone();
                let reservation = make_reservation(&pool, "prop_test");
                holders.push(Holder { id, ctx, reservation, live_bytes: 0 });
            }

            // Apply random grow/shrink operations.
            for h in holders.iter_mut() {
                for _ in 0..OPS_PER_TRACKER {
                    let r = lcg_next(&mut seed);
                    // ~60% grow, ~40% shrink when there's something to shrink.
                    let grow = (r % 10) < 6 || h.live_bytes == 0;
                    if grow {
                        let delta = ((r >> 8) % 1024 + 1) as usize;
                        h.reservation.try_grow(delta).expect("grow succeeds under 10MB pool");
                        h.live_bytes += delta;
                    } else {
                        let delta = ((r >> 8) as usize % h.live_bytes).max(1);
                        h.reservation.shrink(delta);
                        h.live_bytes -= delta;
                    }
                }
            }

            // Sample and assert the invariant on our ids.
            let our_ids: std::collections::HashSet<i64> = holders.iter().map(|h| h.id).collect();
            let triples = collect_active_query_stats(10_000);
            let mut matched = 0usize;
            for (ctx, current, peak) in &triples {
                if our_ids.contains(ctx) {
                    assert!(
                        *current <= *peak,
                        "invariant violated: ctx={} current={} > peak={} (iter {})",
                        ctx,
                        current,
                        peak,
                        iter
                    );
                    assert!(*current >= 0, "current must be non-negative");
                    assert!(*peak >= 0, "peak must be non-negative");
                    matched += 1;
                }
            }
            assert_eq!(
                matched,
                TRACKERS_PER_ITER,
                "iter {}: expected all {} trackers present, got {}",
                iter,
                TRACKERS_PER_ITER,
                matched
            );

            // Cleanup.
            for h in holders {
                drop(h.reservation);
                drop(h.ctx);
                QUERY_REGISTRY.remove(&h.id);
            }
        }
    }
}
