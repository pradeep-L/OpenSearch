/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.search.backpressure.trackers.GetActiveQueryMemoryStats;

import java.util.HashMap;
import java.util.Map;

/**
 * DataFusion-backed implementation of {@link GetActiveQueryMemoryStats}. Calls
 * {@link NativeBridge#getActiveQueryStats()} once per invocation, decodes the returned flat
 * {@code long[]} of {@code [ctx0, current0, peak0, ctx1, current1, peak1, ...]} into a
 * {@code Map<Long, long[]>} keyed by {@code SearchShardTask.getId()}, and returns it.
 *
 * <p>Degrades safely: any {@link RuntimeException} from the FFM call is caught and logged at
 * DEBUG; the method returns {@link Map#of()} in that case so the tracker treats the cycle as
 * "no data" rather than aborting.
 *
 * @opensearch.internal
 */
public final class DataFusionActiveQueryMemoryStats implements GetActiveQueryMemoryStats {

    private static final Logger logger = LogManager.getLogger(DataFusionActiveQueryMemoryStats.class);

    @Override
    public Map<Long, long[]> getActiveQueryMemoryStats() {
        final long[] raw;
        try {
            raw = NativeBridge.getActiveQueryStats();
        } catch (RuntimeException e) {
            logger.debug("getActiveQueryStats failed; returning empty snapshot", e);
            return Map.of();
        }

        if (raw == null || raw.length == 0) {
            return Map.of();
        }
        if (raw.length % 3 != 0) {
            logger.debug("native memory stats returned length {} not multiple of 3; returning empty snapshot", raw.length);
            return Map.of();
        }

        Map<Long, long[]> snapshot = new HashMap<>(raw.length / 3);
        for (int i = 0; i + 2 < raw.length; i += 3) {
            long taskId = raw[i];
            long current = raw[i + 1];
            long peak = raw[i + 2];
            snapshot.put(taskId, new long[] { current, peak });
        }
        return snapshot;
    }
}
