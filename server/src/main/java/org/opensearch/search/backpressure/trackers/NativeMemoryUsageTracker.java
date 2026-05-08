/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.trackers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.util.MovingAverage;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.backpressure.trackers.TaskResourceUsageTrackers.TaskResourceUsageTracker;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskCancellation;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.DoubleSupplier;
import java.util.function.LongSupplier;

import static org.opensearch.search.backpressure.trackers.TaskResourceUsageTrackerType.NATIVE_MEMORY_USAGE_TRACKER;

/**
 * NativeMemoryUsageTracker evaluates whether a {@link org.opensearch.tasks.Task} has consumed too much native
 * (off-heap) memory in the DataFusion query engine. It pulls a per-query {@code (currentBytes, peakBytes)}
 * snapshot once per SBP cycle via an injected {@link GetActiveQueryMemoryStats}, keyed by
 * {@code SearchShardTask.getId()}, and compares each task's current usage to a rolling average of
 * completed-task peaks plus a percent-of-pool floor. The stats surface mirrors {@link HeapUsageTracker.Stats}
 * field-for-field so existing {@code _nodes/stats/search_backpressure} consumers pick up the new fields
 * by name.
 *
 * @opensearch.internal
 */
public class NativeMemoryUsageTracker extends TaskResourceUsageTracker {
    private static final Logger logger = LogManager.getLogger(NativeMemoryUsageTracker.class);

    // ────────────────────────────────────────────────────────────────────────────────────────────────────
    // Plugin → Core bridge for the native-memory tracker (POC).
    //
    // The DataFusion plugin lives in `sandbox/`, so `SearchBackpressureService` (in `server/`) cannot import
    // from it directly. At plugin init the plugin calls {@link #registerSuppliers(Suppliers)} with real
    // suppliers; SBP's 5-arg backward-compat constructor reads from {@link #getRegisteredSuppliers()} at
    // construction time to decide whether to enable native-memory tracking.
    //
    // Registration happens in `Plugin#createComponents(...)`, which `Node.java` calls before constructing
    // `SearchBackpressureService`. When no plugin registers (stock OpenSearch), `getRegisteredSuppliers()`
    // returns {@link Suppliers#DISABLED} and the tracker is effectively off.
    // ────────────────────────────────────────────────────────────────────────────────────────────────────

    /**
     * Bundle of suppliers that wire the native-memory tracker into the plugin's DataFusion runtime.
     * The scalar runtime / pool / reserved-bytes values are pulled individually on each SBP cycle; the
     * active-query snapshot itself is pulled through a dedicated {@link GetActiveQueryMemoryStats}
     * provider so the triple-decoding logic can live in the plugin alongside the FFM binding.
     * {@link #DISABLED} is provided for the "no plugin registered" path.
     */
    public static final class Suppliers {
        public static final Suppliers DISABLED = new Suppliers(() -> 0L, () -> 0L, () -> -1L, GetActiveQueryMemoryStats.EMPTY);

        public final LongSupplier runtimePtrSupplier;
        public final LongSupplier poolLimitSupplier;
        public final LongSupplier reservedBytesSupplier;
        public final GetActiveQueryMemoryStats activeStatsProvider;

        public Suppliers(
            LongSupplier runtimePtrSupplier,
            LongSupplier poolLimitSupplier,
            LongSupplier reservedBytesSupplier,
            GetActiveQueryMemoryStats activeStatsProvider
        ) {
            this.runtimePtrSupplier = Objects.requireNonNull(runtimePtrSupplier, "runtimePtrSupplier");
            this.poolLimitSupplier = Objects.requireNonNull(poolLimitSupplier, "poolLimitSupplier");
            this.reservedBytesSupplier = Objects.requireNonNull(reservedBytesSupplier, "reservedBytesSupplier");
            this.activeStatsProvider = Objects.requireNonNull(activeStatsProvider, "activeStatsProvider");
        }
    }

    private static volatile Suppliers registeredSuppliers = Suppliers.DISABLED;

    /**
     * Registers plugin-side suppliers so {@link org.opensearch.search.backpressure.SearchBackpressureService}
     * picks them up when constructed via the 5-arg backward-compat constructor. Intended to be called exactly
     * once, at plugin {@code createComponents} time, before {@code SearchBackpressureService} is constructed.
     *
     * <p>Calling this after SBP has been constructed is a no-op for that SBP instance — the suppliers are
     * captured at construction time.
     *
     * @param suppliers the plugin-side suppliers (must be non-null)
     */
    public static void registerSuppliers(Suppliers suppliers) {
        registeredSuppliers = Objects.requireNonNull(suppliers, "suppliers");
        logger.info("Native-memory tracker suppliers registered (runtime-ptr/pool-limit/reserved/active-stats)");
    }

    /** Returns the registered suppliers, or {@link Suppliers#DISABLED} if none were registered. */
    public static Suppliers getRegisteredSuppliers() {
        return registeredSuppliers;
    }

    /** Resets the registry. Test-only. */
    static void resetRegisteredSuppliersForTesting() {
        registeredSuppliers = Suppliers.DISABLED;
    }

    private final DoubleSupplier nativeMemVarianceSupplier;
    private final DoubleSupplier nativeMemPercentThresholdSupplier;
    private final LongSupplier nativeMemoryLimitSupplier;
    private final GetActiveQueryMemoryStats activeStatsProvider;

    /**
     * Latest per-query snapshot. Keys are {@code SearchShardTask.getId()}; values are
     * {@code [currentBytes, peakBytes]}. Replaced atomically by a single volatile assignment in
     * {@link #refreshStats()} so readers never observe a partial map.
     */
    private volatile Map<Long, long[]> snapshot = Map.of();

    private final AtomicReference<MovingAverage> movingAverageReference;

    public NativeMemoryUsageTracker(
        DoubleSupplier nativeMemVarianceSupplier,
        DoubleSupplier nativeMemPercentThresholdSupplier,
        LongSupplier nativeMemoryLimitSupplier,
        int movingAverageWindowSize,
        ClusterSettings clusterSettings,
        Setting<Integer> windowSizeSetting,
        GetActiveQueryMemoryStats activeStatsProvider
    ) {
        this.nativeMemVarianceSupplier = nativeMemVarianceSupplier;
        this.nativeMemPercentThresholdSupplier = nativeMemPercentThresholdSupplier;
        this.nativeMemoryLimitSupplier = nativeMemoryLimitSupplier;
        this.activeStatsProvider = Objects.requireNonNull(activeStatsProvider, "activeStatsProvider");
        // NOTE: MovingAverage(0) throws IllegalArgumentException. The cluster setting
        // `search_backpressure.search_shard_task.native_heap_moving_average_window_size` is validated to >= 0,
        // so a value of 0 would throw here. This matches HeapUsageTracker's behavior exactly (a pre-existing
        // symmetry issue between the setting validator and MovingAverage); we do not add special-casing here.
        this.movingAverageReference = new AtomicReference<>(new MovingAverage(movingAverageWindowSize));
        clusterSettings.addSettingsUpdateConsumer(windowSizeSetting, this::updateWindowSize);
        setDefaultResourceUsageBreachEvaluator();
    }

    @Override
    public String name() {
        return NATIVE_MEMORY_USAGE_TRACKER.getName();
    }

    /**
     * Refreshes the per-query snapshot by asking the {@link GetActiveQueryMemoryStats} provider for a fresh
     * map of {@code taskId -> [currentBytes, peakBytes]}. Guarantees:
     * <ul>
     *   <li>Never throws — any exception from the provider is caught and logged at DEBUG (R6.1, R6.2, R6.3).</li>
     *   <li>Replaces {@link #snapshot} atomically via a single volatile write; readers never see a partial map.</li>
     *   <li>On provider failure the previous snapshot is retained unchanged.</li>
     * </ul>
     */
    public void refreshStats() {
        try {
            Map<Long, long[]> next = activeStatsProvider.getActiveQueryMemoryStats();
            if (next == null || next.isEmpty()) {
                this.snapshot = Map.of();
                logger.info("SBP-NM: refresh -> no active queries; snapshot cleared");
                return;
            }
            long totalBytes = 0L;
            long largestBytes = 0L;
            long largestTaskId = -1L;
            for (Map.Entry<Long, long[]> e : next.entrySet()) {
                long[] stats = e.getValue();
                if (stats == null || stats.length < 2) {
                    continue;
                }
                long current = stats[0];
                totalBytes += current;
                if (current > largestBytes) {
                    largestBytes = current;
                    largestTaskId = e.getKey();
                }
            }
            this.snapshot = next;
            long poolLimit = nativeMemoryLimitSupplier.getAsLong();
            double pctUsed = (poolLimit > 0L) ? ((double) totalBytes / (double) poolLimit) * 100.0 : -1.0;
            logger.info(
                "SBP-NM: refresh -> tracking {} queries, total={} ({}% of pool limit {}), largest task {} at {}",
                next.size(),
                new ByteSizeValue(totalBytes),
                String.format(java.util.Locale.ROOT, "%.2f", pctUsed),
                new ByteSizeValue(poolLimit),
                largestTaskId,
                new ByteSizeValue(largestBytes)
            );
        } catch (Exception e) {
            logger.debug("failed to refresh native memory stats; retaining previous snapshot", e);
            // retain previous snapshot — callers see stale data, not a gap (R6.1, R6.2, R6.3)
        }
    }

    /**
     * Builds the resource-usage breach evaluator for this tracker. Called from the constructor; factored out
     * because a lambda referencing instance fields cannot be written directly in the constructor before
     * {@code super(...)} has completed. Mirrors {@link HeapUsageTracker}'s pattern and implements the
     * threshold algorithm from design §Threshold Math (R1.5, R1.6, R1.7).
     */
    private void setDefaultResourceUsageBreachEvaluator() {
        this.resourceUsageBreachEvaluator = (task) -> {
            long[] stats = snapshot.get(task.getId());
            if (stats == null) {
                logger.info("SBP-NM: evaluate task {} -> SKIP (not in snapshot)", task.getId());
                return Optional.empty();
            }

            MovingAverage movingAverage = movingAverageReference.get();
            if (movingAverage.isReady() == false) {
                logger.info("SBP-NM: evaluate task {} -> SKIP (cold start; need more completions for moving average)", task.getId());
                return Optional.empty();
            }

            long current = stats[0];
            double avg = movingAverage.getAverage();
            double variance = nativeMemVarianceSupplier.getAsDouble();
            double allowed = avg * variance;
            long poolLimit = nativeMemoryLimitSupplier.getAsLong();
            double floor = nativeMemPercentThresholdSupplier.getAsDouble() * poolLimit;

            if (current < floor || current < allowed) {
                logger.info(
                    "SBP-NM: evaluate task {} -> SKIP (current={} < floor={} or < variance*avg={})",
                    task.getId(),
                    new ByteSizeValue(current),
                    new ByteSizeValue((long) floor),
                    new ByteSizeValue((long) allowed)
                );
                return Optional.empty();
            }

            long threshold = (long) Math.max(floor, allowed);
            // Cancellation score matches HeapUsageTracker: larger ratio = higher priority.
            int score = (avg > 0.0) ? (int) (current / Math.max(1L, (long) avg)) : Integer.MAX_VALUE;
            logger.info(
                "SBP-NM: evaluate task {} -> BREACH (current={} >= threshold={}; rolling avg={}, score={})",
                task.getId(),
                new ByteSizeValue(current),
                new ByteSizeValue(threshold),
                new ByteSizeValue((long) avg),
                score
            );
            return Optional.of(
                new TaskCancellation.Reason(
                    "native memory usage exceeded ["
                        + new ByteSizeValue(current)
                        + " >= "
                        + new ByteSizeValue(threshold)
                        + "] (rolling avg "
                        + new ByteSizeValue((long) avg)
                        + ")",
                    score
                )
            );
        };
    }

    @Override
    public void update(Task task) {
        long[] stats = snapshot.get(task.getId());
        if (stats != null) {
            movingAverageReference.get().record(stats[1]); // peak_bytes, R1.9
        }
        // Absent task: no-op, must not throw (R1.10, R7.7)
    }

    /**
     * Replaces the live {@link MovingAverage} with a new one whose window is {@code newSize}, preserving prior
     * observations via {@link MovingAverage#copyWithSize(int)} (R3.6). This is a deliberate deviation from
     * {@link HeapUsageTracker#updateWindowSize(int)}, which allocates a fresh {@code MovingAverage(newSize)}
     * and discards prior observations.
     */
    private void updateWindowSize(int newSize) {
        movingAverageReference.updateAndGet(current -> current.copyWithSize(newSize));
    }

    @Override
    public TaskResourceUsageTracker.Stats stats(List<? extends Task> activeTasks) {
        long currentMax = activeTasks.stream().mapToLong(t -> {
            long[] s = snapshot.get(t.getId());
            return s == null ? 0L : s[0];
        }).max().orElse(0L);
        long currentAvg = (long) activeTasks.stream().mapToLong(t -> {
            long[] s = snapshot.get(t.getId());
            return s == null ? 0L : s[0];
        }).average().orElse(0.0);
        return new Stats(getCancellations(), currentMax, currentAvg, (long) movingAverageReference.get().getAverage());
    }

    /**
     * Stats related to {@code NativeMemoryUsageTracker}. Field names and wire/JSON shape are intentionally
     * identical to {@link HeapUsageTracker.Stats} so existing {@code _nodes/stats/search_backpressure}
     * consumers and dashboards pick up the new fields by name without schema changes (R8.7).
     */
    public static class Stats implements TaskResourceUsageTracker.Stats {
        private final long cancellationCount;
        private final long currentMax;
        private final long currentAvg;
        private final long rollingAvg;

        public Stats(long cancellationCount, long currentMax, long currentAvg, long rollingAvg) {
            this.cancellationCount = cancellationCount;
            this.currentMax = currentMax;
            this.currentAvg = currentAvg;
            this.rollingAvg = rollingAvg;
        }

        public Stats(StreamInput in) throws IOException {
            this(in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject()
                .field("cancellation_count", cancellationCount)
                .humanReadableField("current_max_bytes", "current_max", new ByteSizeValue(currentMax))
                .humanReadableField("current_avg_bytes", "current_avg", new ByteSizeValue(currentAvg))
                .humanReadableField("rolling_avg_bytes", "rolling_avg", new ByteSizeValue(rollingAvg))
                .endObject();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(cancellationCount);
            out.writeVLong(currentMax);
            out.writeVLong(currentAvg);
            out.writeVLong(rollingAvg);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Stats stats = (Stats) o;
            return cancellationCount == stats.cancellationCount
                && currentMax == stats.currentMax
                && currentAvg == stats.currentAvg
                && rollingAvg == stats.rollingAvg;
        }

        @Override
        public int hashCode() {
            return Objects.hash(cancellationCount, currentMax, currentAvg, rollingAvg);
        }
    }
}
