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
 * An allocation decider that throttles or denies placing shards on a node whose JVM heap usage is too high. The value
 * used is the node's rolling average of heap used over max heap, as a percentage, as reported in
 * {@link org.opensearch.cluster.ClusterInfo#getNodeResourceUsageStats()}.
 * <p>
 * The decider is controlled by the following dynamic settings:
 * <ul>
 *     <li>{@code cluster.routing.allocation.resource.jvm.enabled} turns the decider on. Defaults to {@code false}.</li>
 *     <li>{@code cluster.routing.allocation.resource.jvm.throttle_threshold} is the heap percentage at or above which
 *     allocations to the node are throttled. Defaults to {@code 90}.</li>
 *     <li>{@code cluster.routing.allocation.resource.jvm.deny_threshold} is the heap percentage at or above which
 *     allocations to the node are denied, except for unassigned primaries, which are throttled. Defaults to
 *     {@code 95}.</li>
 *     <li>{@code cluster.routing.allocation.resource.jvm.stats_max_age} is the maximum age of a node's stats sample. Older
 *     samples are ignored. Defaults to {@code 2m}.</li>
 * </ul>
 *
 * @opensearch.internal
 */
public class JvmMemoryUsageAllocationDecider extends AbstractResourceUsageAllocationDecider {

    public static final String NAME = "jvm_memory_usage";

    public static final Setting<Boolean> CLUSTER_ROUTING_ALLOCATION_RESOURCE_JVM_ENABLED_SETTING = Setting.boolSetting(
        "cluster.routing.allocation.resource.jvm.enabled",
        false,
        Property.Dynamic,
        Property.NodeScope
    );

    public static final Setting<Double> CLUSTER_ROUTING_ALLOCATION_RESOURCE_JVM_THROTTLE_THRESHOLD_SETTING = Setting.doubleSetting(
        "cluster.routing.allocation.resource.jvm.throttle_threshold",
        90.0,
        0.0,
        100.0,
        new ThresholdValidator(
            "cluster.routing.allocation.resource.jvm.throttle_threshold",
            true,
            () -> JvmMemoryUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_RESOURCE_JVM_DENY_THRESHOLD_SETTING
        ),
        Property.Dynamic,
        Property.NodeScope
    );

    public static final Setting<Double> CLUSTER_ROUTING_ALLOCATION_RESOURCE_JVM_DENY_THRESHOLD_SETTING = Setting.doubleSetting(
        "cluster.routing.allocation.resource.jvm.deny_threshold",
        95.0,
        0.0,
        100.0,
        new ThresholdValidator(
            "cluster.routing.allocation.resource.jvm.deny_threshold",
            false,
            () -> JvmMemoryUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_RESOURCE_JVM_THROTTLE_THRESHOLD_SETTING
        ),
        Property.Dynamic,
        Property.NodeScope
    );

    public static final Setting<TimeValue> CLUSTER_ROUTING_ALLOCATION_RESOURCE_JVM_STATS_MAX_AGE_SETTING = Setting.timeSetting(
        "cluster.routing.allocation.resource.jvm.stats_max_age",
        TimeValue.timeValueMinutes(2),
        TimeValue.timeValueMillis(1),
        Property.Dynamic,
        Property.NodeScope
    );

    public JvmMemoryUsageAllocationDecider(Settings settings, ClusterSettings clusterSettings) {
        this(settings, clusterSettings, System::currentTimeMillis);
    }

    JvmMemoryUsageAllocationDecider(Settings settings, ClusterSettings clusterSettings, LongSupplier currentTimeMillisSupplier) {
        super(
            NAME,
            "JVM heap",
            settings,
            clusterSettings,
            CLUSTER_ROUTING_ALLOCATION_RESOURCE_JVM_ENABLED_SETTING,
            CLUSTER_ROUTING_ALLOCATION_RESOURCE_JVM_THROTTLE_THRESHOLD_SETTING,
            CLUSTER_ROUTING_ALLOCATION_RESOURCE_JVM_DENY_THRESHOLD_SETTING,
            CLUSTER_ROUTING_ALLOCATION_RESOURCE_JVM_STATS_MAX_AGE_SETTING,
            currentTimeMillisSupplier
        );
    }

    @Override
    protected double usagePercent(NodeResourceUsageStats stats) {
        return stats.getMemoryUtilizationPercent();
    }
}
