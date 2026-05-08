/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.trackers;

import org.opensearch.action.search.SearchShardTask;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.search.backpressure.settings.SearchShardTaskSettings;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskCancellation;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

// PBT: using OpenSearch randomized-testing (randomLongBetween) — jqwik not present in this workspace; upgrade path is a future enhancement.
public class NativeMemoryUsageTrackerTests extends OpenSearchTestCase {

    // ---- Shared constants used across multiple tests. Tuned so the conditions are easy to reason about. ----
    private static final long POOL_LIMIT = 10L * 1024 * 1024 * 1024; // 10 GiB
    private static final double VARIANCE = 2.0;
    private static final double PERCENT = 0.005; // 0.5 % of pool ≈ 53.7 MiB floor
    private static final int DEFAULT_WINDOW = 10;

    /**
     * Helper that builds a Mockito-stubbed {@link SearchShardTask} with a caller-chosen id. The project's
     * {@code SearchBackpressureTestHelpers#createMockTaskWithResourceStats(...)} ignores its taskId argument and
     * assigns a random id internally; we can't use it when a test needs to correlate a task back to a specific
     * snapshot entry.
     */
    private static SearchShardTask mockTaskWithId(long id) {
        SearchShardTask task = mock(SearchShardTask.class);
        when(task.getId()).thenReturn(id);
        return task;
    }

    /**
     * Convenience constructor that returns a tracker wired to a mutable supplier and a real {@link ClusterSettings}
     * that already includes {@link SearchShardTaskSettings#SETTING_NATIVE_HEAP_MOVING_AVERAGE_WINDOW_SIZE} via
     * {@link ClusterSettings#BUILT_IN_CLUSTER_SETTINGS}.
     */
    private static NativeMemoryUsageTracker newTracker(int windowSize, Supplier<long[]> statsSupplier, ClusterSettings clusterSettings) {
        return new NativeMemoryUsageTracker(
            () -> VARIANCE,
            () -> PERCENT,
            () -> POOL_LIMIT,
            windowSize,
            clusterSettings,
            SearchShardTaskSettings.SETTING_NATIVE_HEAP_MOVING_AVERAGE_WINDOW_SIZE,
            tripleStreamProvider(statsSupplier)
        );
    }

    /**
     * Adapts a legacy {@code Supplier<long[]>} (flat triple stream, same layout as the FFM wire format:
     * {@code [ctx, current, peak, ctx, current, peak, ...]}) into a {@link GetActiveQueryMemoryStats}
     * provider. Keeps the existing property-based tests, which assemble randomized triple streams, working
     * unchanged after the interface refactor.
     */
    private static GetActiveQueryMemoryStats tripleStreamProvider(Supplier<long[]> streamSupplier) {
        return () -> {
            long[] raw = streamSupplier.get();
            if (raw == null || raw.length == 0) {
                return Map.of();
            }
            if (raw.length % 3 != 0) {
                return Map.of();
            }
            Map<Long, long[]> snapshot = new HashMap<>(raw.length / 3);
            for (int i = 0; i + 2 < raw.length; i += 3) {
                snapshot.put(raw[i], new long[] { raw[i + 1], raw[i + 2] });
            }
            return snapshot;
        };
    }

    private static ClusterSettings defaultClusterSettings() {
        return new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    }

    /**
     * Primes the moving average by seeding {@code count} observations via repeated refresh + update calls. Each
     * cycle installs a single-entry snapshot whose peak equals {@code peak}, so the moving average after
     * {@code count} cycles is exactly {@code peak}.
     */
    private static void primeMovingAverage(
        NativeMemoryUsageTracker tracker,
        AtomicReference<long[]> supplierRef,
        long taskId,
        long peak,
        int count
    ) {
        SearchShardTask task = mockTaskWithId(taskId);
        // current=0 is fine here; priming is about pushing peaks into the moving average, not cancellation.
        supplierRef.set(new long[] { taskId, 0L, peak });
        for (int i = 0; i < count; i++) {
            tracker.refreshStats();
            tracker.update(task);
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // 18.1 — Property: decoding round-trip (R1.3, R7.1)
    // ---------------------------------------------------------------------------------------------------------
    //
    // For any Rust-produced stream [ctx_i, cur_i, peak_i] (i = 0..n-1) with unique ctx_i values and
    // cur_i <= peak_i, after refreshStats() the snapshot must reflect every triple. We probe the snapshot via
    // stats(activeTasks), which reads snapshot.get(t.getId())[0] for every task in the list — so currentMax/
    // currentAvg can be derived from the inputs and compared to the tracker's computed Stats.
    //
    // Validates: Requirements 1.3, 7.1
    public void testDecodingRoundTripProperty() {
        AtomicReference<long[]> supplier = new AtomicReference<>(new long[0]);
        NativeMemoryUsageTracker tracker = newTracker(DEFAULT_WINDOW, supplier::get, defaultClusterSettings());

        for (int iter = 0; iter < 50; iter++) {
            int n = randomInt(50); // n in [0, 50]
            long[] raw = new long[3 * n];
            long[] currents = new long[n];
            long[] peaks = new long[n];
            Set<Long> ids = new HashSet<>();
            List<Task> activeTasks = new ArrayList<>(n);

            for (int i = 0; i < n; i++) {
                long ctxId;
                do {
                    ctxId = randomLongBetween(1L, 1_000_000L);
                } while (ids.add(ctxId) == false);

                long current = randomLongBetween(0L, 1_000_000L);
                long peak = randomLongBetween(current, current + 1_000_000L);
                raw[3 * i] = ctxId;
                raw[3 * i + 1] = current;
                raw[3 * i + 2] = peak;
                currents[i] = current;
                peaks[i] = peak;
                activeTasks.add(mockTaskWithId(ctxId));
            }

            supplier.set(raw);
            tracker.refreshStats();

            long expectedMax = n == 0 ? 0L : Arrays.stream(currents).max().getAsLong();
            long expectedAvg = n == 0 ? 0L : (long) Arrays.stream(currents).average().getAsDouble();
            NativeMemoryUsageTracker.Stats expected = new NativeMemoryUsageTracker.Stats(0L, expectedMax, expectedAvg, 0L);
            assertEquals("iter=" + iter + " n=" + n, expected, tracker.stats(activeTasks));
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // 18.2 — Property: refresh idempotence (R7.2)
    // ---------------------------------------------------------------------------------------------------------
    //
    // Two successive refreshStats() calls against the same supplier output produce observationally equal
    // snapshots (same stats for the same active-task list).
    //
    // Validates: Requirement 7.2
    public void testRefreshIdempotenceProperty() {
        AtomicReference<long[]> supplier = new AtomicReference<>(new long[0]);
        NativeMemoryUsageTracker tracker = newTracker(DEFAULT_WINDOW, supplier::get, defaultClusterSettings());

        for (int iter = 0; iter < 20; iter++) {
            int n = randomIntBetween(1, 20);
            long[] raw = new long[3 * n];
            Set<Long> ids = new HashSet<>();
            List<Task> activeTasks = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                long ctxId;
                do {
                    ctxId = randomLongBetween(1L, 1_000_000L);
                } while (ids.add(ctxId) == false);
                long current = randomLongBetween(0L, 1_000_000L);
                long peak = randomLongBetween(current, current + 1_000_000L);
                raw[3 * i] = ctxId;
                raw[3 * i + 1] = current;
                raw[3 * i + 2] = peak;
                activeTasks.add(mockTaskWithId(ctxId));
            }

            supplier.set(raw);
            tracker.refreshStats();
            NativeMemoryUsageTracker.Stats first = (NativeMemoryUsageTracker.Stats) tracker.stats(activeTasks);
            tracker.refreshStats();
            NativeMemoryUsageTracker.Stats second = (NativeMemoryUsageTracker.Stats) tracker.stats(activeTasks);
            assertEquals("refreshStats must be idempotent (iter=" + iter + ")", first, second);
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // 18.3 — Property: threshold monotonicity (R1.5, R7.3)
    // ---------------------------------------------------------------------------------------------------------
    //
    // For fixed (movingAverage, variance, percent, poolLimit): if evaluate(task) returns non-empty at
    // currentBytes = x, it also returns non-empty at currentBytes = 2x (strictly larger, still positive,
    // still above both floors).
    //
    // Validates: Requirements 1.5, 7.3
    public void testThresholdMonotonicityProperty() {
        // Prime the moving average with peak=100 MiB for DEFAULT_WINDOW cycles.
        final long peak = 100L * 1024 * 1024; // 100 MiB
        final long taskId = 42L;

        for (int iter = 0; iter < 20; iter++) {
            AtomicReference<long[]> supplier = new AtomicReference<>(new long[0]);
            NativeMemoryUsageTracker tracker = newTracker(DEFAULT_WINDOW, supplier::get, defaultClusterSettings());
            primeMovingAverage(tracker, supplier, taskId, peak, DEFAULT_WINDOW);

            // Pick an x that clearly breaches both floors: floor = 0.005 * 10 GiB ≈ 53.7 MiB; allowed = 2 * 100 MiB = 200 MiB.
            long lowerBound = (long) Math.max(PERCENT * POOL_LIMIT, VARIANCE * peak) + 1;
            long x = randomLongBetween(lowerBound, lowerBound * 4);
            long twoX = x * 2;

            SearchShardTask task = mockTaskWithId(taskId);

            supplier.set(new long[] { taskId, x, peak });
            tracker.refreshStats();
            Optional<TaskCancellation.Reason> atX = tracker.checkAndMaybeGetCancellationReason(task);
            assertTrue("Expected cancellation at x=" + x + " (iter=" + iter + ")", atX.isPresent());

            supplier.set(new long[] { taskId, twoX, peak });
            tracker.refreshStats();
            Optional<TaskCancellation.Reason> atTwoX = tracker.checkAndMaybeGetCancellationReason(task);
            assertTrue("Expected cancellation at 2x=" + twoX + " (iter=" + iter + ")", atTwoX.isPresent());
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // 18.4 — Property: threshold floors (R1.5, R1.6, R7.4)
    // ---------------------------------------------------------------------------------------------------------
    //
    // No cancellation unless BOTH floors are breached. Randomize a peak so the variance floor is a known value,
    // then pick a current that breaches only one of the floors and assert Optional.empty().
    //
    // Validates: Requirements 1.5, 1.6, 7.4
    public void testThresholdFloorsProperty() {
        final long taskId = 7L;

        for (int iter = 0; iter < 20; iter++) {
            // peakBytes chosen so variance*avg (= 2 * peak) is strictly < PERCENT*POOL_LIMIT.
            // That way we can craft a current that breaches the variance floor but not the percent floor.
            long peak = randomLongBetween(1L, 10L * 1024 * 1024); // up to 10 MiB → allowed <= 20 MiB < 53.7 MiB floor
            long percentFloor = (long) (PERCENT * POOL_LIMIT); // ≈ 53.7 MiB
            long allowed = (long) (VARIANCE * peak);
            assertTrue("test setup invariant: variance floor should be < percent floor", allowed < percentFloor);

            AtomicReference<long[]> supplier = new AtomicReference<>(new long[0]);
            NativeMemoryUsageTracker tracker = newTracker(DEFAULT_WINDOW, supplier::get, defaultClusterSettings());
            primeMovingAverage(tracker, supplier, taskId, peak, DEFAULT_WINDOW);

            SearchShardTask task = mockTaskWithId(taskId);

            // Case A: current breaches variance floor only (current in [allowed, percentFloor-1]).
            long currentA = randomLongBetween(allowed, Math.max(allowed, percentFloor - 1));
            supplier.set(new long[] { taskId, currentA, peak });
            tracker.refreshStats();
            assertFalse(
                "only variance floor breached must not cancel (current=" + currentA + " iter=" + iter + ")",
                tracker.checkAndMaybeGetCancellationReason(task).isPresent()
            );

            // Case B: current breaches percent floor only. Pick another tracker with a large peak so allowed > current.
            long bigPeak = 200L * 1024 * 1024; // 200 MiB → allowed = 400 MiB > 53.7 MiB floor
            AtomicReference<long[]> supplier2 = new AtomicReference<>(new long[0]);
            NativeMemoryUsageTracker tracker2 = newTracker(DEFAULT_WINDOW, supplier2::get, defaultClusterSettings());
            primeMovingAverage(tracker2, supplier2, taskId, bigPeak, DEFAULT_WINDOW);
            long allowed2 = (long) (VARIANCE * bigPeak); // 400 MiB
            long currentB = randomLongBetween(percentFloor, allowed2 - 1);
            supplier2.set(new long[] { taskId, currentB, bigPeak });
            tracker2.refreshStats();
            assertFalse(
                "only percent floor breached must not cancel (current=" + currentB + " iter=" + iter + ")",
                tracker2.checkAndMaybeGetCancellationReason(task).isPresent()
            );

            // Case C: both breached → cancellation present.
            long currentC = Math.max(percentFloor, allowed2) + 1;
            supplier2.set(new long[] { taskId, currentC, bigPeak });
            tracker2.refreshStats();
            assertTrue(
                "both floors breached must cancel (current=" + currentC + " iter=" + iter + ")",
                tracker2.checkAndMaybeGetCancellationReason(task).isPresent()
            );
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // 18.5 — Test: cold-start behavior (R1.7, R7.5)
    // ---------------------------------------------------------------------------------------------------------
    //
    // With fewer than windowSize observations, MovingAverage.isReady() is false and evaluate() returns empty
    // regardless of currentBytes.
    //
    // Validates: Requirements 1.7, 7.5
    public void testColdStartReturnsEmpty() {
        final long taskId = 123L;
        final int windowSize = 10;
        AtomicReference<long[]> supplier = new AtomicReference<>(new long[0]);
        NativeMemoryUsageTracker tracker = newTracker(windowSize, supplier::get, defaultClusterSettings());

        // Record fewer observations than the window.
        primeMovingAverage(tracker, supplier, taskId, 1L, 5);

        // Install a snapshot whose current is so large it would breach every floor many times over.
        supplier.set(new long[] { taskId, POOL_LIMIT, POOL_LIMIT });
        tracker.refreshStats();

        SearchShardTask task = mockTaskWithId(taskId);
        assertFalse(
            "cold start: evaluate must return empty even for huge current",
            tracker.checkAndMaybeGetCancellationReason(task).isPresent()
        );
    }

    // ---------------------------------------------------------------------------------------------------------
    // 18.6 — Test: completed-task stability (R1.10, R7.7)
    // ---------------------------------------------------------------------------------------------------------
    //
    // update(task) for a task NOT in the snapshot is a no-op — it must not throw, and must not affect the
    // moving average. We verify the second half indirectly: prime the moving average to ready with peak=P,
    // then call update(absentTask) several times, then check that stats.rollingAvg still equals P (rather
    // than being dragged toward 0 by the "absent" updates).
    //
    // Validates: Requirements 1.10, 7.7
    public void testCompletedTaskUpdateIsNoop() {
        final long presentTaskId = 1L;
        final long absentTaskId = 999L;
        final long peak = 1_000_000L;
        AtomicReference<long[]> supplier = new AtomicReference<>(new long[0]);
        NativeMemoryUsageTracker tracker = newTracker(DEFAULT_WINDOW, supplier::get, defaultClusterSettings());

        primeMovingAverage(tracker, supplier, presentTaskId, peak, DEFAULT_WINDOW);

        // Snapshot the rolling avg after priming — it should be exactly `peak`.
        NativeMemoryUsageTracker.Stats before = (NativeMemoryUsageTracker.Stats) tracker.stats(List.of());
        NativeMemoryUsageTracker.Stats expectedBefore = new NativeMemoryUsageTracker.Stats(0L, 0L, 0L, peak);
        assertEquals(expectedBefore, before);

        // Now call update() for a task that's not in the snapshot. Must not throw, must not touch the moving avg.
        SearchShardTask absentTask = mockTaskWithId(absentTaskId);
        tracker.update(absentTask); // first call
        tracker.update(absentTask); // second call
        tracker.update(absentTask); // third call

        NativeMemoryUsageTracker.Stats after = (NativeMemoryUsageTracker.Stats) tracker.stats(List.of());
        assertEquals("absent update must not affect rolling avg", expectedBefore, after);
    }

    // ---------------------------------------------------------------------------------------------------------
    // 18.7 — Test: degradation on supplier failure (R6.1, R6.2, R6.3)
    // ---------------------------------------------------------------------------------------------------------
    //
    // refreshStats() must:
    // - not throw when the supplier throws (R6.1, R6.3)
    // - retain the previous snapshot on failure (R6.2)
    // - recover on the next successful call
    // - tolerate a null return value without crashing
    //
    // Validates: Requirements 6.1, 6.2, 6.3
    public void testRefreshStatsDegradesSafely() {
        AtomicReference<Supplier<long[]>> supplierHolder = new AtomicReference<>(() -> new long[0]);
        NativeMemoryUsageTracker tracker = newTracker(DEFAULT_WINDOW, () -> supplierHolder.get().get(), defaultClusterSettings());

        final long taskId = 11L;
        SearchShardTask task = mockTaskWithId(taskId);

        // 1. Prime a known-good snapshot so we have something to "retain".
        supplierHolder.set(() -> new long[] { taskId, 500L, 700L });
        tracker.refreshStats();
        NativeMemoryUsageTracker.Stats good = (NativeMemoryUsageTracker.Stats) tracker.stats(List.of(task));
        assertEquals(new NativeMemoryUsageTracker.Stats(0L, 500L, 500L, 0L), good);

        // 2. Supplier starts throwing — refreshStats must not rethrow, must retain the previous snapshot.
        supplierHolder.set(() -> { throw new RuntimeException("native call boom"); });
        tracker.refreshStats(); // must not throw
        NativeMemoryUsageTracker.Stats afterThrow = (NativeMemoryUsageTracker.Stats) tracker.stats(List.of(task));
        assertEquals("snapshot must be retained after supplier exception", good, afterThrow);

        // 3. Supplier returns null — still must not throw.
        supplierHolder.set(() -> null);
        tracker.refreshStats();
        // After null, the snapshot is explicitly reset to empty per NativeMemoryUsageTracker.refreshStats().
        NativeMemoryUsageTracker.Stats afterNull = (NativeMemoryUsageTracker.Stats) tracker.stats(List.of(task));
        assertEquals(new NativeMemoryUsageTracker.Stats(0L, 0L, 0L, 0L), afterNull);

        // 4. Supplier recovers — snapshot updates to reflect new data.
        supplierHolder.set(() -> new long[] { taskId, 1234L, 5678L });
        tracker.refreshStats();
        NativeMemoryUsageTracker.Stats recovered = (NativeMemoryUsageTracker.Stats) tracker.stats(List.of(task));
        assertEquals(new NativeMemoryUsageTracker.Stats(0L, 1234L, 1234L, 0L), recovered);
    }

    // ---------------------------------------------------------------------------------------------------------
    // 18.8 — Test: updateWindowSize preserves observations (R3.6)
    // ---------------------------------------------------------------------------------------------------------
    //
    // Updating the cluster setting `native_heap_moving_average_window_size` must replace the live MovingAverage
    // via copyWithSize(newSize) — preserving any prior observations. We assert that behavior indirectly: record
    // 3 peaks at window=5 (not ready), grow the window to 10 via applySettings, record 7 more. If the 3 prior
    // observations were preserved, total = 10 → ready → a breaching snapshot cancels. If they were dropped
    // (as HeapUsageTracker#updateWindowSize does), total = 7 → not ready → no cancellation.
    //
    // Validates: Requirement 3.6
    public void testUpdateWindowSizePreservesObservations() {
        ClusterSettings clusterSettings = defaultClusterSettings();
        AtomicReference<long[]> supplier = new AtomicReference<>(new long[0]);
        NativeMemoryUsageTracker tracker = newTracker(5, supplier::get, clusterSettings);

        final long taskId = 314L;
        final long peak = 100L * 1024 * 1024; // 100 MiB; variance floor = 200 MiB, percent floor ≈ 53.7 MiB
        SearchShardTask task = mockTaskWithId(taskId);

        // Record 3 observations at window=5 — not ready yet.
        supplier.set(new long[] { taskId, 0L, peak });
        for (int i = 0; i < 3; i++) {
            tracker.refreshStats();
            tracker.update(task);
        }

        // Sanity: a breaching snapshot now must NOT cancel (window not full; not ready).
        long breachingCurrent = Math.max((long) (PERCENT * POOL_LIMIT), (long) (VARIANCE * peak)) + 1;
        supplier.set(new long[] { taskId, breachingCurrent, peak });
        tracker.refreshStats();
        assertFalse("not ready with 3/5 observations — must not cancel", tracker.checkAndMaybeGetCancellationReason(task).isPresent());

        // Grow the window to 10 via the cluster setting — fires updateWindowSize consumer, which calls copyWithSize(10).
        clusterSettings.applySettings(
            Settings.builder().put(SearchShardTaskSettings.SETTING_NATIVE_HEAP_MOVING_AVERAGE_WINDOW_SIZE.getKey(), 10).build()
        );

        // Record 6 more peak observations. If the 3 prior were preserved, total = 3 + 6 = 9 → still not ready with window=10.
        supplier.set(new long[] { taskId, 0L, peak });
        for (int i = 0; i < 6; i++) {
            tracker.refreshStats();
            tracker.update(task);
        }
        supplier.set(new long[] { taskId, breachingCurrent, peak });
        tracker.refreshStats();
        assertFalse(
            "9/10 observations after window grow — must still not cancel",
            tracker.checkAndMaybeGetCancellationReason(task).isPresent()
        );

        // One more observation → total = 10 → ready → cancellation fires. If copyWithSize had discarded the
        // 3 prior observations, the total would only be 7 and this assertion would fail.
        supplier.set(new long[] { taskId, 0L, peak });
        tracker.refreshStats();
        tracker.update(task);

        supplier.set(new long[] { taskId, breachingCurrent, peak });
        tracker.refreshStats();
        assertTrue("10/10 observations (3 preserved + 7 new) — must cancel", tracker.checkAndMaybeGetCancellationReason(task).isPresent());
    }
}
