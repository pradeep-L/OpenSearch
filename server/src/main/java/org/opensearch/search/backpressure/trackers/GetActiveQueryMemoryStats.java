/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.trackers;

import java.util.Map;

/**
 * SPI that provides a snapshot of active per-query native-memory stats keyed by
 * {@code SearchShardTask.getId()}. Production implementations live in plugins
 * (e.g. {@code analytics-backend-datafusion}) and back the call with an FFM downcall;
 * the {@code server} module only sees this interface so there's no hard dependency
 * on the plugin.
 *
 * <p>Called once per SBP cycle from
 * {@link NativeMemoryUsageTracker#refreshStats()} before per-task evaluation.
 *
 * @opensearch.internal
 */
@FunctionalInterface
public interface GetActiveQueryMemoryStats {

    /** Provider that always reports no active queries. Used as the "disabled" default. */
    GetActiveQueryMemoryStats EMPTY = Map::of;

    /**
     * Returns a fresh snapshot of active per-query native-memory stats.
     *
     * <p><b>Layout:</b> the returned map is keyed by {@code SearchShardTask.getId()};
     * each value is a two-element array {@code [currentBytes, peakBytes]} where
     * {@code currentBytes <= peakBytes}.
     *
     * <p><b>Contract:</b> implementations MUST NOT throw. Any native-call failure
     * should be caught and logged internally; in that case return
     * {@link Map#of() Map.of()} so the tracker keeps its previous snapshot and
     * SBP cycles are never aborted by this call.
     *
     * @return a non-null map (empty when no queries are active or on error)
     */
    Map<Long, long[]> getActiveQueryMemoryStats();
}
