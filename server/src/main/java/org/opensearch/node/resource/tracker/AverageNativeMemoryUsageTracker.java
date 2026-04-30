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
import org.opensearch.common.unit.TimeValue;
import org.opensearch.monitor.os.OsProbe;
import org.opensearch.monitor.process.NativePluginMemoryProbe;
import org.opensearch.threadpool.ThreadPool;

/**
 * AverageNativeMemoryUsageTracker tracks the average native (physical) memory usage on the node
 * by polling the OS-level memory stats every (pollingInterval) and keeping track of the rolling
 * average over a defined time window (windowDuration).
 *
 * On Linux, it uses available memory from /proc/meminfo (MemAvailable) which accounts for
 * reclaimable page cache and slab memory, giving a more accurate picture of actual memory
 * pressure. On other platforms, it falls back to free physical memory.
 *
 * <p>For observability, on each {@link #getUsage()} call this tracker additionally reads and
 * logs (at INFO) four diagnostic metrics: the process-level {@code RssAnon} from
 * {@code /proc/self/smaps_rollup} (the {@code Anonymous} line) and from {@code /proc/self/status}
 * (the {@code RssAnon} line), the JVM heap used (from the {@code MemoryMXBean}), and the total
 * bytes used by native (Rust) plugin heaps exposed by {@code NativeLibraryLoader}. These values
 * do not affect the returned usage and are intended purely for operator visibility.
 */
public class AverageNativeMemoryUsageTracker extends AbstractAverageUsageTracker {

    private static final Logger LOGGER = LogManager.getLogger(AverageNativeMemoryUsageTracker.class);

    public AverageNativeMemoryUsageTracker(ThreadPool threadPool, TimeValue pollingInterval, TimeValue windowDuration) {
        super(threadPool, pollingInterval, windowDuration);
    }

    /**
     * Get current native memory usage percentage using OS-level physical memory stats.
     * Prefers available memory (MemAvailable) over free memory (MemFree) as it provides
     * a more accurate measure of actual memory pressure on the node.
     */
    @Override
    public long getUsage() {
        OsProbe osProbe = OsProbe.getInstance();
        long totalMemory = osProbe.getTotalPhysicalMemorySize();
        if (totalMemory <= 0) {
            LOGGER.warn("Unable to retrieve total physical memory size");
            return 0;
        }
        long availableMemory = osProbe.getAvailableMemorySize();
        long unusedMemory;
        if (availableMemory >= 0) {
            // Use available memory (includes reclaimable cache) for a more accurate picture
            unusedMemory = availableMemory;
        } else {
            LOGGER.warn("unable to retrieve available memory");
            return 0;
        }
        long usedMemory = totalMemory - unusedMemory;
        long usage = usedMemory * 100 / totalMemory;

        // Observability-only: log process-level and native plugin memory alongside the node-level usage.
        // These values do NOT influence the returned admission-control usage value.
        final long processRssAnon = osProbe.getProcessRssAnon();
        final long processRssAnonFromStatus = osProbe.getProcessRssAnonFromStatus();
      //  final long rustPluginHeapUsed = NativePluginMemoryProbe.getRustPluginHeapUsedBytes();
        final long jvmHeapUsed = osProbe.getJvmHeapUsed();
        LOGGER.info(
            "Recording native memory usage: {}% (totalMemory={}B, usedMemory={}B, "
                + "processRssAnonSmapsRollup={}B, processRssAnonymous={}B, jvmHeapUsed={}B)",
            usage,
            totalMemory,
            usedMemory,
            processRssAnon,
            processRssAnonFromStatus,
            jvmHeapUsed
        );
        return usage;
    }
}
