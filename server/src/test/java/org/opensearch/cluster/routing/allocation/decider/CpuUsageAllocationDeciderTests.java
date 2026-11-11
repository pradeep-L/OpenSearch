/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation.decider;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.node.NodeResourceUsageStats;

import java.util.function.LongSupplier;

public class CpuUsageAllocationDeciderTests extends AbstractResourceUsageAllocationDeciderTestCase {

    @Override
    protected AbstractResourceUsageAllocationDecider createDecider(
        Settings settings,
        ClusterSettings clusterSettings,
        LongSupplier currentTimeMillis
    ) {
        return new CpuUsageAllocationDecider(settings, clusterSettings, currentTimeMillis);
    }

    @Override
    protected String deciderName() {
        return CpuUsageAllocationDecider.NAME;
    }

    @Override
    protected String resourceName() {
        return "CPU";
    }

    @Override
    protected Setting<Boolean> enabledSetting() {
        return CpuUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_RESOURCE_CPU_ENABLED_SETTING;
    }

    @Override
    protected Setting<Double> throttleThresholdSetting() {
        return CpuUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_RESOURCE_CPU_THROTTLE_THRESHOLD_SETTING;
    }

    @Override
    protected Setting<Double> denyThresholdSetting() {
        return CpuUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_RESOURCE_CPU_DENY_THRESHOLD_SETTING;
    }

    @Override
    protected Setting<TimeValue> statsMaxAgeSetting() {
        return CpuUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_RESOURCE_CPU_STATS_MAX_AGE_SETTING;
    }

    @Override
    protected NodeResourceUsageStats stats(String nodeId, long timestamp, double usage, double otherUsage) {
        return newStats(nodeId, timestamp, otherUsage, usage);
    }
}
