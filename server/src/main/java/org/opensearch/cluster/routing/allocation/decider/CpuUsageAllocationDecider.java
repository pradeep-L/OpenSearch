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
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.node.NodeResourceUsageStats;

import java.util.function.LongSupplier;

/**
 * An allocation decider that throttles or denies placing shards on a node whose OpenSearch process CPU usage is too
 * high. The value used is the node's rolling average CPU usage percentage, as reported in
 * {@link org.opensearch.cluster.ClusterInfo#getNodeResourceUsageStats()}.
 * <p>
 * The decider is controlled by the following dynamic settings:
 * <ul>
 *     <li>{@code cluster.routing.allocation.resource.cpu.enabled} turns the decider on. Defaults to {@code false}.</li>
 *     <li>{@code cluster.routing.allocation.resource.cpu.throttle_threshold} is the CPU percentage at or above which
 *     allocations to the node are throttled. Defaults to {@code 90}.</li>
 *     <li>{@code cluster.routing.allocation.resource.cpu.deny_threshold} is the CPU percentage at or above which
 *     allocations to the node are denied, except for unassigned primaries, which are throttled. Defaults to
 *     {@code 95}.</li>
 *     <li>{@code cluster.routing.allocation.resource.cpu.stats_max_age} is the maximum age of a node's stats sample. Older
 *     samples are ignored. Defaults to {@code 2m}.</li>
 * </ul>
 *
 * @opensearch.internal
 */
public class CpuUsageAllocationDecider extends AbstractResourceUsageAllocationDecider {

    public static final String NAME = "cpu_usage";

    public static final Setting<Boolean> CLUSTER_ROUTING_ALLOCATION_RESOURCE_CPU_ENABLED_SETTING = Setting.boolSetting(
        "cluster.routing.allocation.resource.cpu.enabled",
        false,
        Property.Dynamic,
        Property.NodeScope
    );

    public static final Setting<Double> CLUSTER_ROUTING_ALLOCATION_RESOURCE_CPU_THROTTLE_THRESHOLD_SETTING = Setting.doubleSetting(
        "cluster.routing.allocation.resource.cpu.throttle_threshold",
        90.0,
        0.0,
        100.0,
        new ThresholdValidator(
            "cluster.routing.allocation.resource.cpu.throttle_threshold",
            true,
            () -> CpuUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_RESOURCE_CPU_DENY_THRESHOLD_SETTING
        ),
        Property.Dynamic,
        Property.NodeScope
    );

    public static final Setting<Double> CLUSTER_ROUTING_ALLOCATION_RESOURCE_CPU_DENY_THRESHOLD_SETTING = Setting.doubleSetting(
        "cluster.routing.allocation.resource.cpu.deny_threshold",
        95.0,
        0.0,
        100.0,
        new ThresholdValidator(
            "cluster.routing.allocation.resource.cpu.deny_threshold",
            false,
            () -> CpuUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_RESOURCE_CPU_THROTTLE_THRESHOLD_SETTING
        ),
        Property.Dynamic,
        Property.NodeScope
    );

    public static final Setting<TimeValue> CLUSTER_ROUTING_ALLOCATION_RESOURCE_CPU_STATS_MAX_AGE_SETTING = Setting.timeSetting(
        "cluster.routing.allocation.resource.cpu.stats_max_age",
        TimeValue.timeValueMinutes(2),
        TimeValue.timeValueMillis(1),
        Property.Dynamic,
        Property.NodeScope
    );

    public CpuUsageAllocationDecider(Settings settings, ClusterSettings clusterSettings) {
        this(settings, clusterSettings, System::currentTimeMillis);
    }

    CpuUsageAllocationDecider(Settings settings, ClusterSettings clusterSettings, LongSupplier currentTimeMillisSupplier) {
        super(
            NAME,
            "CPU",
            settings,
            clusterSettings,
            CLUSTER_ROUTING_ALLOCATION_RESOURCE_CPU_ENABLED_SETTING,
            CLUSTER_ROUTING_ALLOCATION_RESOURCE_CPU_THROTTLE_THRESHOLD_SETTING,
            CLUSTER_ROUTING_ALLOCATION_RESOURCE_CPU_DENY_THRESHOLD_SETTING,
            CLUSTER_ROUTING_ALLOCATION_RESOURCE_CPU_STATS_MAX_AGE_SETTING,
            currentTimeMillisSupplier
        );
    }

    @Override
    protected double usagePercent(NodeResourceUsageStats stats) {
        return stats.getCpuUtilizationPercent();
    }
}
