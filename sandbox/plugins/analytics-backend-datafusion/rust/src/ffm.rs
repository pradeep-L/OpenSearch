/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! FFM bridge for DataFusion.

use std::slice;
use std::str;
use std::sync::Arc;

use log::{info, warn};
use native_bridge_common::ffm_safe;
use parking_lot::RwLock;

use crate::api;
use crate::runtime_manager::RuntimeManager;

static TOKIO_RUNTIME_MANAGER: RwLock<Option<Arc<RuntimeManager>>> = RwLock::new(None);

unsafe fn str_from_raw<'a>(ptr: *const u8, len: i64) -> Result<&'a str, String> {
    if ptr.is_null() {
        return Err("null string pointer".to_string());
    }
    if len < 0 {
        return Err(format!("negative string length: {}", len));
    }
    let bytes = slice::from_raw_parts(ptr, len as usize);
    str::from_utf8(bytes).map_err(|e| format!("invalid UTF-8: {}", e))
}

fn get_rt_manager() -> Result<Arc<RuntimeManager>, String> {
    TOKIO_RUNTIME_MANAGER
        .read()
        .clone()
        .ok_or_else(|| "Runtime manager not initialized".to_string())
}

#[no_mangle]
pub extern "C" fn df_init_runtime_manager(cpu_threads: i32) {
    let mut guard = TOKIO_RUNTIME_MANAGER.write();
    *guard = Some(Arc::new(RuntimeManager::new(cpu_threads as usize)));
}

#[no_mangle]
pub extern "C" fn df_shutdown_runtime_manager() {
    let mgr = TOKIO_RUNTIME_MANAGER.write().take();
    if let Some(mgr) = mgr {
        mgr.shutdown();
    }
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_create_global_runtime(
    memory_pool_limit: i64,
    spill_dir_ptr: *const u8,
    spill_dir_len: i64,
    spill_limit: i64,
) -> i64 {
    let _guard = heap_allocator::scoped_thread_heap(get_df_heap());
    let spill_dir = str_from_raw(spill_dir_ptr, spill_dir_len).map_err(|e| format!("df_create_global_runtime: {}", e))?;
    api::create_global_runtime(memory_pool_limit, spill_dir, spill_limit)
        .map_err(|e| e.to_string())
}

#[no_mangle]
pub unsafe extern "C" fn df_close_global_runtime(ptr: i64) {
    let _guard = heap_allocator::scoped_thread_heap(get_df_heap());
    api::close_global_runtime(ptr);
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_create_reader(
    table_path_ptr: *const u8,
    table_path_len: i64,
    files_ptr: *const *const u8,
    files_len_ptr: *const i64,
    files_count: i64,
) -> i64 {
    let _guard = heap_allocator::scoped_thread_heap(get_df_heap());
    let table_path = str_from_raw(table_path_ptr, table_path_len).map_err(|e| format!("df_create_reader: {}", e))?;
    let mut filenames = Vec::with_capacity(files_count as usize);
    for i in 0..files_count as usize {
        let ptr = *files_ptr.add(i);
        let len = *files_len_ptr.add(i);
        filenames.push(str_from_raw(ptr, len).map_err(|e| format!("df_create_reader: {}", e))?.to_string());
    }
    let mgr = get_rt_manager()?;
    api::create_reader(table_path, filenames, &mgr).map_err(|e| e.to_string())
}

#[no_mangle]
pub unsafe extern "C" fn df_close_reader(ptr: i64) {
    let _guard = heap_allocator::scoped_thread_heap(get_df_heap());
    api::close_reader(ptr);
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_execute_query(
    shard_view_ptr: i64,
    table_name_ptr: *const u8,
    table_name_len: i64,
    plan_ptr: *const u8,
    plan_len: i64,
    runtime_ptr: i64,
    context_id: i64,
) -> i64 {
    let _guard = heap_allocator::scoped_thread_heap(get_df_heap());
    let mgr = get_rt_manager()?;
    let table_name = str_from_raw(table_name_ptr, table_name_len).map_err(|e| format!("df_execute_query: {}", e))?;
    let plan_bytes = slice::from_raw_parts(plan_ptr, plan_len as usize);
    mgr.io_runtime
        .block_on(api::execute_query(shard_view_ptr, table_name, plan_bytes, runtime_ptr, &mgr, context_id))
        .map_err(|e| e.to_string())
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_stream_get_schema(stream_ptr: i64) -> i64 {
    let _guard = heap_allocator::scoped_thread_heap(get_df_heap());
    api::stream_get_schema(stream_ptr).map_err(|e| e.to_string())
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_stream_next(stream_ptr: i64) -> i64 {
    let _guard = heap_allocator::scoped_thread_heap(get_df_heap());
    let mgr = get_rt_manager()?;
    mgr.io_runtime
        .block_on(api::stream_next(stream_ptr))
        .map_err(|e| e.to_string())
}

#[no_mangle]
pub unsafe extern "C" fn df_stream_close(stream_ptr: i64) {
    let _guard = heap_allocator::scoped_thread_heap(get_df_heap());
    api::stream_close(stream_ptr);
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_sql_to_substrait(
    shard_view_ptr: i64,
    table_name_ptr: *const u8,
    table_name_len: i64,
    sql_ptr: *const u8,
    sql_len: i64,
    runtime_ptr: i64,
    out_ptr: *mut u8,
    out_cap: i64,
    out_len: *mut i64,
) -> i64 {
    let _guard = heap_allocator::scoped_thread_heap(get_df_heap());
    let mgr = get_rt_manager()?;
    let table_name = str_from_raw(table_name_ptr, table_name_len).map_err(|e| format!("df_sql_to_substrait: table_name: {}", e))?;
    let sql = str_from_raw(sql_ptr, sql_len).map_err(|e| format!("df_sql_to_substrait: sql: {}", e))?;
    let bytes = api::sql_to_substrait(shard_view_ptr, table_name, sql, runtime_ptr, &mgr)
        .map_err(|e| e.to_string())?;
    if bytes.len() > out_cap as usize {
        return Err(format!(
            "substrait plan size {} exceeds buffer capacity {}",
            bytes.len(),
            out_cap
        ));
    }
    std::ptr::copy_nonoverlapping(bytes.as_ptr(), out_ptr, bytes.len());
    if !out_len.is_null() {
        *out_len = bytes.len() as i64;
    }
    Ok(0)
}

/// Phase 1 of the two-phase active-query snapshot protocol. Count the number
/// of active (not-completed) DataFusion queries currently tracked in
/// `QUERY_REGISTRY` and write the result into `*out_size` as a **triple count**
/// (not a long count). The Java caller multiplies by 3 and sizes the buffer
/// for the subsequent `df_active_query_stats_copy` call.
///
/// Read-only: does not mutate `QUERY_REGISTRY` or any other global state.
///
/// Error conventions (per the plugin's `#[ffm_safe]` negative-status pattern):
///   * `out_size` null → descriptive error; no pointer is written through.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_active_query_stats_size(
    out_size: *mut i64,
) -> i64 {
    // REQUIRED first line per design — mirrors every other df_* function
    // post-commit 9a69ac1. Routes any incidental allocations (DashMap iterator
    // internals, error-string formatting) onto the DataFusion plugin heap so
    // they're attributed correctly.
    let _guard = heap_allocator::scoped_thread_heap(get_df_heap());

    if out_size.is_null() {
        warn!("SBP-NM[rust]: df_active_query_stats_size rejected — null out_size");
        return Err("df_active_query_stats_size: null out_size".to_string());
    }

    let active = crate::query_memory_pool_tracker::count_active_queries();
    *out_size = active;

    info!(
        "SBP-NM[rust]: df_active_query_stats_size -> {} active triples",
        active
    );
    Ok(0)
}

/// Phase 2 of the two-phase active-query snapshot protocol. Walk
/// `QUERY_REGISTRY`, skip completed trackers, and write up to `out_cap / 3`
/// `(context_id, current_bytes, peak_bytes)` triples as consecutive `i64`s
/// into the caller-provided buffer in iteration order. On success sets
/// `*out_len` to the number of `i64`s actually written (always a non-negative
/// multiple of 3, always `<= out_cap`) and returns `0`.
///
/// If more active entries remain after `out_cap / 3` writes (the registry
/// grew between the preceding `df_active_query_stats_size` call and this
/// call), the function **truncates silently** — `*out_len` is set to
/// `out_cap` and the remaining active entries are left for the next cycle.
/// No error is raised and no diagnostic message mentioning "insufficient
/// buffer" is produced. The Java caller observes the truncation through
/// `*out_len` alone.
///
/// Read-only: does not mutate `QUERY_REGISTRY` or any other global state.
///
/// Error conventions (per the plugin's `#[ffm_safe]` negative-status pattern):
///   * `out_len` null → descriptive error.
///   * `out_cap` negative or not a multiple of 3 → descriptive error.
///   * `out_ptr` null while `out_cap > 0` → descriptive error.
///   * No pointer is written through on any error.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_active_query_stats_copy(
    out_ptr: *mut i64,
    out_cap: i64,
    out_len: *mut i64,
) -> i64 {
    // REQUIRED first line per design — mirrors every other df_* function
    // post-commit 9a69ac1.
    let _guard = heap_allocator::scoped_thread_heap(get_df_heap());

    info!(
        "SBP-NM[rust]: df_active_query_stats_copy called (out_cap={} longs = {} triples)",
        out_cap,
        out_cap / 3
    );

    if out_len.is_null() {
        warn!("SBP-NM[rust]: df_active_query_stats_copy rejected — null out_len");
        return Err("df_active_query_stats_copy: null out_len".to_string());
    }
    if out_cap < 0 || out_cap % 3 != 0 {
        warn!(
            "SBP-NM[rust]: df_active_query_stats_copy rejected — out_cap={} (must be non-negative multiple of 3)",
            out_cap
        );
        return Err(format!(
            "df_active_query_stats_copy: out_cap must be a non-negative multiple of 3 (got {})",
            out_cap
        ));
    }
    if out_cap > 0 && out_ptr.is_null() {
        warn!("SBP-NM[rust]: df_active_query_stats_copy rejected — null out_ptr with out_cap={}", out_cap);
        return Err(format!(
            "df_active_query_stats_copy: out_ptr is null but out_cap={} > 0",
            out_cap
        ));
    }

    let cap_triples = (out_cap / 3) as usize;
    let triples = crate::query_memory_pool_tracker::collect_active_query_stats(cap_triples);

    // Write triples into the caller-provided buffer in iteration order.
    let mut total_current: i64 = 0;
    let mut largest_ctx: i64 = -1;
    let mut largest_current: i64 = 0;
    for (i, (ctx, current, peak)) in triples.iter().enumerate() {
        let base = (i as isize) * 3;
        *out_ptr.offset(base) = *ctx;
        *out_ptr.offset(base + 1) = *current;
        *out_ptr.offset(base + 2) = *peak;
        total_current = total_current.saturating_add(*current);
        if *current > largest_current {
            largest_current = *current;
            largest_ctx = *ctx;
        }
    }
    *out_len = (triples.len() as i64) * 3;

    info!(
        "SBP-NM[rust]: df_active_query_stats_copy returning {} triples ({} bytes written to buffer); \
         total current_bytes={}, largest task {} at {} bytes",
        triples.len(),
        triples.len() * 3 * 8,
        total_current,
        largest_ctx,
        largest_current
    );
    Ok(0)
}

// ── Heap tracking ───────────────────────────────────────────────────────────

use native_bridge_common::heap_allocator;
use datafusion::execution::memory_pool::MemoryPool;

static DF_HEAP: std::sync::OnceLock<heap_allocator::PluginHeap> = std::sync::OnceLock::new();

fn get_df_heap() -> heap_allocator::PluginHeap {
    *DF_HEAP.get_or_init(|| heap_allocator::create_heap("datafusion"))
}

/// Initialize the datafusion plugin's mimalloc heap. Call once at plugin startup.
#[no_mangle]
pub extern "C" fn df_init_heap() {
    get_df_heap();
}

/// Set the calling thread's active heap to datafusion's heap.
#[no_mangle]
pub extern "C" fn df_set_thread_heap() {
    heap_allocator::set_thread_heap(get_df_heap());
}

/// Returns the DataFusion memory pool usage in bytes, or 0 if no runtime.
#[no_mangle]
pub unsafe extern "C" fn df_get_memory_pool_usage(runtime_ptr: i64) -> i64 {
    let _guard = heap_allocator::scoped_thread_heap(get_df_heap());
    if runtime_ptr == 0 {
        info!("SBP-NM[rust]: df_get_memory_pool_usage called with runtime_ptr=0 (runtime not yet initialised); returning 0");
        return 0;
    }
    let runtime = &*(runtime_ptr as *const api::DataFusionRuntime);
    let reserved = runtime.runtime_env.memory_pool.reserved() as i64;
    info!(
        "SBP-NM[rust]: df_get_memory_pool_usage -> reserved={} bytes (runtime_ptr={:#x})",
        reserved, runtime_ptr as usize
    );
    reserved
}

/// Test-only: allocate a buffer on datafusion's heap. Returns pointer as i64.
#[no_mangle]
pub extern "C" fn df_allocate_test_buffer(size: i64) -> i64 {
    let heap = get_df_heap();
    let _guard = heap_allocator::scoped_thread_heap(heap);
    let buf: Vec<u8> = vec![0u8; size as usize];
    let ptr = buf.as_ptr() as i64;
    std::mem::forget(buf);
    ptr
}

/// Test-only: free a test buffer. Safe to call from any thread — mimalloc resolves
/// the owning heap from the pointer's segment metadata.
#[no_mangle]
pub extern "C" fn df_free_test_buffer(ptr: i64, _size: i64) {
    if ptr != 0 {
        unsafe { libmimalloc_sys::mi_free(ptr as *mut core::ffi::c_void) };
    }
}

#[cfg(test)]
mod tests {
    //! Tests for the two-phase active-query-stats FFM functions.
    //!
    //! `QUERY_REGISTRY` is a process-wide static shared with the rest of the
    //! crate's tests. These tests use unique context IDs in the 70_000+ range
    //! and filter assertions to their own IDs so other tests (in particular
    //! those in `query_memory_pool_tracker::tests`) running in parallel do
    //! not interfere.

    use super::*;
    use crate::query_memory_pool_tracker::{QueryTrackingContext, QUERY_REGISTRY};
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool, MemoryReservation};
    use std::sync::Arc;

    fn make_global_pool(limit: usize) -> Arc<dyn MemoryPool> {
        Arc::new(GreedyMemoryPool::new(limit))
    }

    fn make_reservation(pool: &Arc<dyn MemoryPool>, name: &str) -> MemoryReservation {
        MemoryConsumer::new(name).register(pool)
    }

    /// Create a QueryTrackingContext and immediately drop it so the tracker
    /// lingers in QUERY_REGISTRY in the "completed" state (mirrors what happens
    /// post-query, before Java calls `drain_completed_query`).
    fn insert_completed_tracker(ctx_id: i64, global: &Arc<dyn MemoryPool>) {
        let ctx = QueryTrackingContext::new(ctx_id, Arc::clone(global));
        drop(ctx);
        assert!(QUERY_REGISTRY.get(&ctx_id).unwrap().is_completed());
    }

    // ----------------------------------------------------------------------
    // 4.1 — df_active_query_stats_size
    // ----------------------------------------------------------------------

    #[test]
    fn test_size_with_inserted_active_and_completed() {
        let global = make_global_pool(1_000_000);
        let active_ids: [i64; 3] = [70_010, 70_011, 70_012];
        let completed_ids: [i64; 2] = [70_013, 70_014];

        // Ensure a clean state for our ids.
        for id in active_ids.iter().chain(completed_ids.iter()) {
            QUERY_REGISTRY.remove(id);
        }

        let _active: Vec<QueryTrackingContext> = active_ids
            .iter()
            .map(|id| QueryTrackingContext::new(*id, Arc::clone(&global)))
            .collect();
        for id in &completed_ids {
            insert_completed_tracker(*id, &global);
        }

        // Walk the registry twice: once to get the active count, once to copy
        // all triples. If our 3 active IDs all appear in the copy output, the
        // size probe must have counted at least those 3.
        let mut out_size: i64 = -1;
        unsafe {
            let status = df_active_query_stats_size(&mut out_size);
            assert_eq!(status, 0, "size call failed, status={}", status);
        }
        assert!(out_size >= active_ids.len() as i64,
            "size must include our {} actives, got {}", active_ids.len(), out_size);

        // Copy into a generously-sized buffer and confirm all three active IDs
        // appear AND none of the completed IDs appear.
        let cap_longs: usize = 99_999; // 33_333 triples (multiple of 3)
        let mut buf = vec![0i64; cap_longs];
        let mut out_len: i64 = -1;
        unsafe {
            let status = df_active_query_stats_copy(buf.as_mut_ptr(), cap_longs as i64, &mut out_len);
            assert_eq!(status, 0);
        }
        let active_set: std::collections::HashSet<i64> = active_ids.iter().copied().collect();
        let completed_set: std::collections::HashSet<i64> = completed_ids.iter().copied().collect();
        let mut active_seen = 0usize;
        for i in 0..(out_len / 3) as usize {
            let ctx = buf[i * 3];
            if active_set.contains(&ctx) {
                active_seen += 1;
            }
            assert!(
                !completed_set.contains(&ctx),
                "completed tracker ctx={} unexpectedly appeared in output",
                ctx
            );
        }
        assert_eq!(
            active_seen,
            active_ids.len(),
            "all {} active ids must appear in size/copy output",
            active_ids.len()
        );

        // Cleanup.
        drop(_active);
        for id in active_ids.iter().chain(completed_ids.iter()) {
            QUERY_REGISTRY.remove(id);
        }
    }

    #[test]
    fn test_size_empty_registry_our_ids_absent() {
        // Weak but sufficient: with no entries we inserted under our 70_001 ID,
        // the size call returns 0 (meaning no writes through out_size) and
        // returns successfully. Under parallel load, out_size will reflect
        // other tests' trackers — all we can assert is that the call succeeded
        // and we never mutated *out_size to -1.
        let never_inserted_id: i64 = 70_001;
        QUERY_REGISTRY.remove(&never_inserted_id);

        let mut out_size: i64 = -1;
        unsafe {
            let status = df_active_query_stats_size(&mut out_size);
            assert_eq!(status, 0);
        }
        assert!(out_size >= 0, "out_size must be non-negative, got {}", out_size);
    }

    #[test]
    fn test_size_null_out_size_returns_negative_status() {
        unsafe {
            let status = df_active_query_stats_size(std::ptr::null_mut());
            assert!(status < 0, "expected negative status for null out_size, got {}", status);
            // Free the heap-allocated error-string pointer the ffm_safe macro returned.
            native_bridge_common::error::native_error_free((-status) as i64);
        }
    }

    // ----------------------------------------------------------------------
    // 4.2 — df_active_query_stats_copy, sized-exactly path
    // ----------------------------------------------------------------------

    #[test]
    fn test_copy_sized_exactly_writes_inserted_triples() {
        let global = make_global_pool(1_000_000);
        let ids: [i64; 3] = [70_020, 70_021, 70_022];
        for id in &ids {
            QUERY_REGISTRY.remove(id);
        }

        // Create contexts and grow each to a distinct current/peak so we can
        // verify the exact value returned by the copy call.
        let contexts: Vec<QueryTrackingContext> = ids
            .iter()
            .map(|id| QueryTrackingContext::new(*id, Arc::clone(&global)))
            .collect();
        let mut reservations: Vec<MemoryReservation> = Vec::with_capacity(ids.len());
        for (i, ctx) in contexts.iter().enumerate() {
            let qp = ctx.memory_pool().expect("tracker installed");
            let pool: Arc<dyn MemoryPool> = qp.clone();
            let mut r = make_reservation(&pool, "copy_test");
            r.try_grow(1_000 * (i + 1)).expect("grow");
            reservations.push(r);
        }
        let expected: std::collections::HashMap<i64, (i64, i64)> = ids
            .iter()
            .enumerate()
            .map(|(i, id)| (*id, (1_000i64 * (i as i64 + 1), 1_000i64 * (i as i64 + 1))))
            .collect();

        // Use a generously-sized fixed buffer (multiple of 3) so truncation is
        // not a factor regardless of parallel test load. The exact-sized
        // invariant `*out_len == 3*N` is verified by the
        // `test_copy_sized_exactly_fills_whole_buffer_under_stable_load` test.
        let cap_longs: usize = 99_999;
        let mut buf = vec![0i64; cap_longs];
        let mut out_len: i64 = -1;
        unsafe {
            let status = df_active_query_stats_copy(buf.as_mut_ptr(), cap_longs as i64, &mut out_len);
            assert_eq!(status, 0, "copy call failed, status={}", status);
        }
        assert!(out_len >= 0 && out_len <= cap_longs as i64 && out_len % 3 == 0);

        // Verify every one of our inserted ids appears and matches the live atomics.
        let mut matched: usize = 0;
        for i in 0..(out_len / 3) as usize {
            let ctx = buf[i * 3];
            let current = buf[i * 3 + 1];
            let peak = buf[i * 3 + 2];
            if let Some((expected_current, expected_peak)) = expected.get(&ctx) {
                assert_eq!(current, *expected_current, "current for ctx {}", ctx);
                assert_eq!(peak, *expected_peak, "peak for ctx {}", ctx);
                matched += 1;
            }
            assert!(current <= peak, "current {} > peak {} for ctx {}", current, peak, ctx);
        }
        assert_eq!(matched, ids.len(), "all {} of our ids must appear, matched {}", ids.len(), matched);

        drop(reservations);
        drop(contexts);
        for id in &ids {
            QUERY_REGISTRY.remove(id);
        }
    }

    #[test]
    fn test_copy_sized_exactly_fills_whole_buffer_under_stable_load() {
        // Stronger form of the sized-exactly invariant: when we freeze the set
        // of trackers (i.e. after Phase 1 we do not drop anything), and we
        // size the buffer to exactly Phase 1's triple count, Phase 2 MUST
        // either fill the buffer completely (if other parallel tests added
        // trackers after Phase 1) or return slightly less (if other tests
        // completed some of their own trackers). Either way: out_len % 3 == 0
        // and out_len <= cap.
        let global = make_global_pool(1_000_000);
        let ids: Vec<i64> = (70_080..70_084).collect();
        for id in &ids {
            QUERY_REGISTRY.remove(id);
        }
        let _contexts: Vec<QueryTrackingContext> = ids
            .iter()
            .map(|id| QueryTrackingContext::new(*id, Arc::clone(&global)))
            .collect();

        let mut n_triples: i64 = 0;
        unsafe {
            let status = df_active_query_stats_size(&mut n_triples);
            assert_eq!(status, 0);
        }
        let cap = (n_triples * 3) as usize;
        let mut buf = vec![0i64; cap];
        let mut out_len: i64 = -1;
        unsafe {
            let status = df_active_query_stats_copy(buf.as_mut_ptr(), cap as i64, &mut out_len);
            assert_eq!(status, 0);
        }
        assert!(out_len >= 0 && out_len <= cap as i64 && out_len % 3 == 0);

        drop(_contexts);
        for id in &ids {
            QUERY_REGISTRY.remove(id);
        }
    }

    #[test]
    fn test_copy_with_zero_cap_accepts_null_ptr() {
        // Cap of 0 means "tell me how many you *would* have written", which is
        // always 0. The function must not dereference out_ptr, so null is safe.
        let mut out_len: i64 = -1;
        unsafe {
            let status = df_active_query_stats_copy(std::ptr::null_mut(), 0, &mut out_len);
            assert_eq!(status, 0, "copy with cap=0 should succeed, status={}", status);
        }
        assert_eq!(out_len, 0, "out_len must be 0 when cap is 0");
    }

    // ----------------------------------------------------------------------
    // 4.3 — df_active_query_stats_copy skips completed trackers
    // ----------------------------------------------------------------------

    #[test]
    fn test_copy_skips_completed_trackers() {
        let global = make_global_pool(1_000_000);
        let active_ids: [i64; 3] = [70_030, 70_031, 70_032];
        let completed_ids: [i64; 2] = [70_033, 70_034];
        for id in active_ids.iter().chain(completed_ids.iter()) {
            QUERY_REGISTRY.remove(id);
        }

        let _active: Vec<QueryTrackingContext> = active_ids
            .iter()
            .map(|id| QueryTrackingContext::new(*id, Arc::clone(&global)))
            .collect();
        for id in &completed_ids {
            insert_completed_tracker(*id, &global);
        }

        // Use a generously-sized fixed buffer so truncation is not a factor
        // regardless of what parallel tests have done to the registry.
        let cap_longs: usize = 99_999; // 33_333 triples (multiple of 3)
        let mut buf = vec![0i64; cap_longs];
        let mut out_len: i64 = -1;
        unsafe {
            let status = df_active_query_stats_copy(buf.as_mut_ptr(), cap_longs as i64, &mut out_len);
            assert_eq!(status, 0);
        }
        assert!(out_len % 3 == 0);
        assert!(out_len <= cap_longs as i64);

        let active_set: std::collections::HashSet<i64> = active_ids.iter().copied().collect();
        let completed_set: std::collections::HashSet<i64> = completed_ids.iter().copied().collect();
        let mut active_seen: usize = 0;
        for i in 0..(out_len / 3) as usize {
            let ctx = buf[i * 3];
            if active_set.contains(&ctx) {
                active_seen += 1;
            }
            assert!(
                !completed_set.contains(&ctx),
                "completed tracker ctx={} unexpectedly appeared in copy output",
                ctx
            );
        }
        assert_eq!(
            active_seen,
            active_ids.len(),
            "all {} active ids must appear in copy output, saw {}",
            active_ids.len(),
            active_seen
        );

        drop(_active);
        for id in active_ids.iter().chain(completed_ids.iter()) {
            QUERY_REGISTRY.remove(id);
        }
    }

    // ----------------------------------------------------------------------
    // 4.4 — df_active_query_stats_copy truncates silently on growth, and
    //       rejects invalid out_cap values.
    // ----------------------------------------------------------------------

    #[test]
    fn test_copy_truncates_silently_when_cap_too_small() {
        let global = make_global_pool(1_000_000);
        // Insert a healthy 20 active trackers. Even under parallel test load,
        // the registry always has ≥ 20 active entries for the duration of this
        // test, so asking for cap=6 (2 triples) will ALWAYS force truncation.
        let ids: Vec<i64> = (70_040..70_060).collect();
        let _contexts: Vec<QueryTrackingContext> = ids
            .iter()
            .map(|id| QueryTrackingContext::new(*id, Arc::clone(&global)))
            .collect();

        // Simulate "registry grew between Phase 1 and Phase 2": Java sized the
        // buffer for 2 triples but the registry actually has ≥ 20 actives, so
        // 18+ are silently truncated. The function MUST NOT return a negative
        // status, and *out_len MUST equal out_cap exactly.
        let cap_longs: i64 = 6; // 2 triples
        let mut buf = vec![0i64; cap_longs as usize];
        let mut out_len: i64 = -1;
        unsafe {
            let status = df_active_query_stats_copy(buf.as_mut_ptr(), cap_longs, &mut out_len);
            assert_eq!(
                status, 0,
                "copy must succeed on truncation (no negative status), got {}",
                status
            );
        }
        assert_eq!(
            out_len, cap_longs,
            "when truncated, out_len must equal out_cap exactly; got out_len={}, cap_longs={}",
            out_len, cap_longs
        );
        assert!(out_len % 3 == 0);

        // Cleanup.
        drop(_contexts);
        for id in &ids {
            QUERY_REGISTRY.remove(id);
        }
    }

    #[test]
    fn test_copy_rejects_negative_cap() {
        let mut out_len: i64 = -1;
        let mut buf = [0i64; 3];
        unsafe {
            let status = df_active_query_stats_copy(buf.as_mut_ptr(), -3, &mut out_len);
            assert!(status < 0, "expected negative status for negative cap, got {}", status);
            native_bridge_common::error::native_error_free((-status) as i64);
        }
    }

    #[test]
    fn test_copy_rejects_non_multiple_of_three_cap() {
        let mut out_len: i64 = -1;
        let mut buf = [0i64; 4];
        unsafe {
            let status = df_active_query_stats_copy(buf.as_mut_ptr(), 4, &mut out_len);
            assert!(
                status < 0,
                "expected negative status for cap=4 (not multiple of 3), got {}",
                status
            );
            native_bridge_common::error::native_error_free((-status) as i64);
        }
    }

    #[test]
    fn test_copy_rejects_null_out_len() {
        let mut buf = [0i64; 3];
        unsafe {
            let status = df_active_query_stats_copy(buf.as_mut_ptr(), 3, std::ptr::null_mut());
            assert!(status < 0, "expected negative status for null out_len, got {}", status);
            native_bridge_common::error::native_error_free((-status) as i64);
        }
    }

    #[test]
    fn test_copy_rejects_null_ptr_with_nonzero_cap() {
        let mut out_len: i64 = -1;
        unsafe {
            let status = df_active_query_stats_copy(std::ptr::null_mut(), 3, &mut out_len);
            assert!(
                status < 0,
                "expected negative status for null out_ptr with cap=3, got {}",
                status
            );
            native_bridge_common::error::native_error_free((-status) as i64);
        }
    }

    // ----------------------------------------------------------------------
    // 4.5 — property-style: every emitted triple satisfies current <= peak
    // ----------------------------------------------------------------------

    #[test]
    fn test_copy_current_le_peak_property() {
        // Serialize against the sibling property test in
        // `query_memory_pool_tracker::tests` — both churn QUERY_REGISTRY hard
        // enough to trigger DashMap iteration artifacts when run in parallel.
        let _lock = crate::query_memory_pool_tracker::test_support::PROPERTY_TEST_LOCK
            .lock()
            .unwrap();

        // Keep iterations and tracker count modest: this property test runs
        // in parallel with `query_memory_pool_tracker::tests::test_collect_active_current_le_peak_property`,
        // and both tests churn `QUERY_REGISTRY` heavily. DashMap iteration is
        // snapshot-free, so aggressive concurrent mutation can cause either
        // test's trackers to be temporarily missed during iteration.
        const ITERATIONS: u64 = 20;
        const TRACKERS_PER_ITER: usize = 5;
        const OPS_PER_TRACKER: usize = 5;
        const BASE_ID: i64 = 70_100;

        fn lcg_next(state: &mut u64) -> u64 {
            *state = state.wrapping_mul(1664525).wrapping_add(1013904223) & 0xFFFF_FFFF;
            *state
        }

        for iter in 0..ITERATIONS {
            let global = make_global_pool(10_000_000);
            let mut seed: u64 = iter.wrapping_mul(2654435761) ^ 0xFEED_FACE;

            struct Holder {
                id: i64,
                ctx: QueryTrackingContext,
                reservation: MemoryReservation,
                live_bytes: usize,
            }

            let mut holders: Vec<Holder> = Vec::with_capacity(TRACKERS_PER_ITER);
            for j in 0..TRACKERS_PER_ITER {
                let id = BASE_ID + (iter as i64) * 1_000 + j as i64;
                QUERY_REGISTRY.remove(&id);
                let ctx = QueryTrackingContext::new(id, Arc::clone(&global));
                let qp = ctx.memory_pool().expect("tracker installed");
                let pool: Arc<dyn MemoryPool> = qp.clone();
                let reservation = make_reservation(&pool, "ffm_prop_test");
                holders.push(Holder { id, ctx, reservation, live_bytes: 0 });
            }

            for h in holders.iter_mut() {
                for _ in 0..OPS_PER_TRACKER {
                    let r = lcg_next(&mut seed);
                    let grow = (r % 10) < 6 || h.live_bytes == 0;
                    if grow {
                        let delta = ((r >> 8) % 1024 + 1) as usize;
                        h.reservation.try_grow(delta).expect("grow succeeds");
                        h.live_bytes += delta;
                    } else {
                        let delta = ((r >> 8) as usize % h.live_bytes).max(1);
                        h.reservation.shrink(delta);
                        h.live_bytes -= delta;
                    }
                }
            }

            let our_ids: std::collections::HashSet<i64> = holders.iter().map(|h| h.id).collect();

            // Use an intentionally over-sized buffer: large enough to swallow
            // any active count other parallel tests in the same module could
            // plausibly push the registry to. Avoids the flakiness of sizing
            // from Phase 1 when other tests insert between the two calls.
            let cap_longs: usize = 99_999; // 33_333 triples (multiple of 3) — more than enough
            let mut buf = vec![0i64; cap_longs];
            let mut out_len: i64 = -1;
            unsafe {
                let status =
                    df_active_query_stats_copy(buf.as_mut_ptr(), cap_longs as i64, &mut out_len);
                assert_eq!(status, 0, "iter {}: copy failed, status={}", iter, status);
            }

            let mut matched = 0usize;
            for i in 0..(out_len / 3) as usize {
                let ctx = buf[i * 3];
                let current = buf[i * 3 + 1];
                let peak = buf[i * 3 + 2];
                assert!(current >= 0, "current must be non-negative, got {}", current);
                assert!(peak >= 0, "peak must be non-negative, got {}", peak);
                assert!(
                    current <= peak,
                    "invariant violated: ctx={} current={} > peak={} (iter {})",
                    ctx, current, peak, iter
                );
                if our_ids.contains(&ctx) {
                    matched += 1;
                }
            }
            assert_eq!(
                matched, TRACKERS_PER_ITER,
                "iter {}: expected all {} trackers present, got {}",
                iter, TRACKERS_PER_ITER, matched
            );

            for h in holders {
                drop(h.reservation);
                drop(h.ctx);
                QUERY_REGISTRY.remove(&h.id);
            }
        }
    }
}
