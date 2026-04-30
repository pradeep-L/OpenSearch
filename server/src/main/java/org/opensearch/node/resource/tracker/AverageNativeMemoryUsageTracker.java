/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node.resource.tracker;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Constants;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.monitor.os.OsProbe;
import org.opensearch.threadpool.ThreadPool;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;

/**
 * AverageNativeMemoryUsageTracker reports a rolling percentage that represents how much of the
 * DataFusion memory pool is currently consumed by off-heap native allocations belonging to this
 * JVM.
 *
 * <p>The instantaneous usage returned from {@link #getUsage()} is computed as:
 * <pre>
 *     rssAnon         = /proc/self/status RssAnon   (anonymous resident memory for this process)
 *     heapCommitted   = JVM heap committed bytes    (MemoryMXBean)
 *     nativeOffHeap   = max(0, rssAnon - heapCommitted)
 *     usage%          = (nativeOffHeap * 100) / datafusion.memory_pool_limit_bytes
 * </pre>
 *
 * <p>Rationale:
 * <ul>
 *   <li>{@code RssAnon} is the process's anonymous resident memory. On Linux it includes the JVM
 *       heap (which is anonymous private memory) plus every other native mapping the process has
 *       produced: Arrow buffers, JNI allocations, DataFusion pool pages, direct ByteBuffers, etc.
 *   <li>Subtracting the committed heap removes the portion already accounted for by the JVM,
 *       leaving a reasonable estimate of native off-heap memory in use. This is not a perfect
 *       isolation of DataFusion's allocations — other off-heap users share the same pool of
 *       anonymous pages — but it tracks DataFusion closely in practice because Arrow/DataFusion
 *       native allocations dominate off-heap growth under analytics workloads.
 *   <li>The denominator is the configured DataFusion pool limit
 *       ({@code datafusion.memory_pool_limit_bytes}). If the setting is absent (e.g. the DataFusion
 *       plugin is not installed), the default {@code Runtime.maxMemory() / 4} is used — the same
 *       default the plugin itself applies.
 * </ul>
 *
 * <p>On non-Linux platforms {@code RssAnon} is unavailable; this tracker reports {@code 0} so the
 * admission controller behaves as a no-op rather than producing false rejections.
 *
 * <p>For observability, each {@link #getUsage()} call also logs the underlying probe values and
 * per-probe read latencies at INFO. These values do not affect the returned usage.
 */
public class AverageNativeMemoryUsageTracker extends AbstractAverageUsageTracker {

    private static final Logger LOGGER = LogManager.getLogger(AverageNativeMemoryUsageTracker.class);

    /** Setting key registered by the DataFusion plugin. Kept as a string to avoid a server-side dependency. */
    static final String DATAFUSION_MEMORY_POOL_LIMIT_SETTING = "datafusion.memory_pool_limit_bytes";

    private final long poolLimitBytes;
    private final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();

    public AverageNativeMemoryUsageTracker(
        ThreadPool threadPool,
        TimeValue pollingInterval,
        TimeValue windowDuration,
        Settings settings
    ) {
        super(threadPool, pollingInterval, windowDuration);
        this.poolLimitBytes = resolvePoolLimit(settings);
        LOGGER.info(
            "AverageNativeMemoryUsageTracker initialised with datafusion pool limit = {} bytes ({} MB)",
            poolLimitBytes,
            poolLimitBytes / (1024L * 1024L)
        );
    }

    private static long resolvePoolLimit(Settings settings) {
        long defaultLimit = Runtime.getRuntime().maxMemory() / 4L;
        // Read by key name so that this class does not need to depend on the DataFusion plugin.
        long configured = settings.getAsLong(DATAFUSION_MEMORY_POOL_LIMIT_SETTING, defaultLimit);
        if (configured <= 0) {
            LOGGER.warn(
                "Configured {} = {} is non-positive; falling back to default {} bytes",
                DATAFUSION_MEMORY_POOL_LIMIT_SETTING,
                configured,
                defaultLimit
            );
            return defaultLimit;
        }
        return configured;
    }

    /**
     * Returns the instantaneous percentage of the DataFusion memory pool consumed by the JVM's
     * native off-heap anonymous memory. Always in the range {@code [0, 100]}.
     */
    @Override
    public long getUsage() {
        if (!Constants.LINUX) {
            // /proc/self/status is Linux-only; no reliable process-level RssAnon source elsewhere.
            return 0;
        }

        OsProbe osProbe = OsProbe.getInstance();

        // /proc/self/status: RssAnon
        final long statusStart = System.nanoTime();
        final long rssAnon = osProbe.getProcessRssAnonFromStatus();
        final long statusReadNanos = System.nanoTime() - statusStart;
        if (rssAnon < 0) {
            LOGGER.warn("Unable to read RssAnon from /proc/self/status; reporting 0% usage");
            return 0;
        }

        // JVM committed heap — matches the anonymous pages the heap actually occupies
        // and is the correct thing to subtract from RssAnon.
        final long heapStart = System.nanoTime();
        final long heapCommitted = memoryMXBean.getHeapMemoryUsage().getCommitted();
        final long heapReadNanos = System.nanoTime() - heapStart;

        final long nativeOffHeap = Math.max(0L, rssAnon - heapCommitted);

        // usage% = nativeOffHeap * 100 / poolLimitBytes, clamped to [0, 100].
        long usage = (nativeOffHeap * 100L) / Math.max(1L, poolLimitBytes);
        if (usage < 0L) usage = 0L;
        if (usage > 100L) usage = 100L;

        // Observability-only: read additional probes for comparison.
        final long smapsStart = System.nanoTime();
        final long rssAnonSmapsRollup = osProbe.getProcessRssAnon();
        final long smapsReadNanos = System.nanoTime() - smapsStart;

        final long heapUsedStart = System.nanoTime();
        final long heapUsed = memoryMXBean.getHeapMemoryUsage().getUsed();
        final long heapUsedReadNanos = System.nanoTime() - heapUsedStart;

        LOGGER.info(
            "Recording native memory usage: {}% "
                + "(rssAnon={}MB, heapCommitted={}MB, heapUsed={}MB, nativeOffHeap={}MB, poolLimit={}MB, "
                + "rssAnonSmapsRollup={}MB) "
                + "readLatencyMicros[status={}, heapCommitted={}, smapsRollup={}, heapUsed={}]",
            usage,
            rssAnon / (1024L * 1024L),
            heapCommitted / (1024L * 1024L),
            heapUsed / (1024L * 1024L),
            nativeOffHeap / (1024L * 1024L),
            poolLimitBytes / (1024L * 1024L),
            rssAnonSmapsRollup / (1024L * 1024L),
            statusReadNanos / 1_000L,
            heapReadNanos / 1_000L,
            smapsReadNanos / 1_000L,
            heapUsedReadNanos / 1_000L
        );
        return usage;
    }
}
