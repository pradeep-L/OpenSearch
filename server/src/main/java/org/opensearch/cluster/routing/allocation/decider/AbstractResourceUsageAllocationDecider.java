/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation.decider;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterInfo;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.node.NodeResourceUsageStats;

import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Base class for allocation deciders that stop shards from being placed on a node whose resource usage, as reported in
 * {@link ClusterInfo#getNodeResourceUsageStats()}, is too high.
 * <p>
 * Only {@link #canAllocate(ShardRouting, RoutingNode, RoutingAllocation)} is gated, which covers new shard assignment and
 * relocation targets. Shards that are already placed are never moved by these deciders. The decision is:
 * <ul>
 *     <li>usage at or above the deny threshold: {@link Decision#NO}, except for unassigned primaries, which get
 *     {@link Decision#THROTTLE} so that resource pressure alone never leaves the cluster red</li>
 *     <li>usage at or above the throttle threshold: {@link Decision#THROTTLE}</li>
 *     <li>otherwise: {@link Decision#YES}</li>
 * </ul>
 * The decider fails open (returns {@link Decision#YES}) when it is disabled, when the cluster has a single data node, and
 * when the usage value is missing, unknown (zero, negative or not finite) or older than the configured max age.
 *
 * @opensearch.internal
 */
abstract class AbstractResourceUsageAllocationDecider extends AllocationDecider {

    private static final Logger logger = LogManager.getLogger(AbstractResourceUsageAllocationDecider.class);

    private final String name;
    private final String resourceName;
    private final Setting<Double> throttleThresholdSetting;
    private final Setting<Double> denyThresholdSetting;
    private final LongSupplier currentTimeMillisSupplier;

    // Updated by settings-update consumers on the cluster applier thread and read during allocation, hence volatile.
    private volatile boolean enabled;
    private volatile double throttleThreshold;
    private volatile double denyThreshold;
    private volatile TimeValue statsMaxAge;

    AbstractResourceUsageAllocationDecider(
        String name,
        String resourceName,
        Settings settings,
        ClusterSettings clusterSettings,
        Setting<Boolean> enabledSetting,
        Setting<Double> throttleThresholdSetting,
        Setting<Double> denyThresholdSetting,
        Setting<TimeValue> statsMaxAgeSetting,
        LongSupplier currentTimeMillisSupplier
    ) {
        this.name = name;
        this.resourceName = resourceName;
        this.throttleThresholdSetting = throttleThresholdSetting;
        this.denyThresholdSetting = denyThresholdSetting;
        this.currentTimeMillisSupplier = currentTimeMillisSupplier;
        this.enabled = enabledSetting.get(settings);
        this.throttleThreshold = throttleThresholdSetting.get(settings);
        this.denyThreshold = denyThresholdSetting.get(settings);
        this.statsMaxAge = statsMaxAgeSetting.get(settings);
        assert throttleThreshold <= denyThreshold : "throttle threshold [" + throttleThreshold + "] > deny threshold [" + denyThreshold + "]";
        clusterSettings.addSettingsUpdateConsumer(enabledSetting, this::setEnabled);
        clusterSettings.addSettingsUpdateConsumer(throttleThresholdSetting, this::setThrottleThreshold);
        clusterSettings.addSettingsUpdateConsumer(denyThresholdSetting, this::setDenyThreshold);
        clusterSettings.addSettingsUpdateConsumer(statsMaxAgeSetting, this::setStatsMaxAge);
    }

    /**
     * Returns the usage percentage (0 to 100) this decider gates on.
     */
    protected abstract double usagePercent(NodeResourceUsageStats stats);

    private void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    private void setThrottleThreshold(double throttleThreshold) {
        this.throttleThreshold = throttleThreshold;
    }

    private void setDenyThreshold(double denyThreshold) {
        this.denyThreshold = denyThreshold;
    }

    private void setStatsMaxAge(TimeValue statsMaxAge) {
        this.statsMaxAge = statsMaxAge;
    }

    @Override
    public final Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (enabled == false) {
            return allocation.decision(Decision.YES, name, "the %s usage allocation decider is disabled", resourceName);
        }
        if (allocation.nodes().getDataNodes().size() <= 1) {
            return allocation.decision(Decision.YES, name, "there is only a single data node present");
        }
        final ClusterInfo clusterInfo = allocation.clusterInfo();
        if (clusterInfo == null) {
            return allocation.decision(Decision.YES, name, "the cluster info is unavailable");
        }
        final Map<String, NodeResourceUsageStats> usageStats = clusterInfo.getNodeResourceUsageStats();
        if (usageStats == null || usageStats.isEmpty()) {
            return allocation.decision(Decision.YES, name, "node resource usage stats are unavailable");
        }
        final NodeResourceUsageStats stats = usageStats.get(node.nodeId());
        if (stats == null) {
            return allocation.decision(Decision.YES, name, "no resource usage stats are available for the node");
        }
        final double usage = usagePercent(stats);
        if (Double.isFinite(usage) == false || usage <= 0) {
            // The trackers report 0 until their window is full and the CPU probe reports -1 when unavailable.
            return allocation.decision(Decision.YES, name, "the node's %s usage is unknown [%s]", resourceName, usage);
        }
        final TimeValue maxAge = statsMaxAge;
        // The timestamp comes from the data node's clock. A negative age (data node clock ahead) counts as fresh.
        final long ageMillis = currentTimeMillisSupplier.getAsLong() - stats.getTimestamp();
        if (ageMillis > maxAge.millis()) {
            return allocation.decision(
                Decision.YES,
                name,
                "the node's resource usage stats are stale, age [%s] is greater than the max age [%s]",
                TimeValue.timeValueMillis(ageMillis),
                maxAge
            );
        }

        // Read each threshold once so that one decision uses a consistent pair of values.
        final double deny = denyThreshold;
        final double throttle = throttleThreshold;
        final String usageString = formatPercent(usage);
        if (usage >= deny) {
            if (shardRouting.primary() && shardRouting.unassigned()) {
                logger.trace("[{}] usage [{}] on node [{}] above deny threshold, throttling unassigned primary", name, usage, node.nodeId());
                return allocation.decision(
                    Decision.THROTTLE,
                    name,
                    "the node's %s usage [%s] is at or above the deny threshold [%s=%s]; "
                        + "throttling instead of denying because the shard is an unassigned primary",
                    resourceName,
                    usageString,
                    denyThresholdSetting.getKey(),
                    formatPercent(deny)
                );
            }
            logger.trace("[{}] usage [{}] on node [{}] above deny threshold", name, usage, node.nodeId());
            return allocation.decision(
                Decision.NO,
                name,
                "the node's %s usage [%s] is at or above the deny threshold [%s=%s]",
                resourceName,
                usageString,
                denyThresholdSetting.getKey(),
                formatPercent(deny)
            );
        }
        if (usage >= throttle) {
            logger.trace("[{}] usage [{}] on node [{}] above throttle threshold", name, usage, node.nodeId());
            return allocation.decision(
                Decision.THROTTLE,
                name,
                "the node's %s usage [%s] is at or above the throttle threshold [%s=%s]",
                resourceName,
                usageString,
                throttleThresholdSetting.getKey(),
                formatPercent(throttle)
            );
        }
        return allocation.decision(
            Decision.YES,
            name,
            "the node's %s usage [%s] is below the throttle threshold [%s]",
            resourceName,
            usageString,
            formatPercent(throttle)
        );
    }

    private static String formatPercent(double value) {
        return String.format(Locale.ROOT, "%.1f%%", value);
    }

    /**
     * Validates that a throttle threshold is less than or equal to its paired deny threshold, or the reverse. The paired
     * setting is supplied lazily so that the two {@code static final} settings can reference each other without a static
     * initialization cycle.
     *
     * @opensearch.internal
     */
    static final class ThresholdValidator implements Setting.Validator<Double> {

        private final String key;
        private final boolean isThrottleThreshold;
        private final Supplier<Setting<Double>> pairedSetting;

        /**
         * @param key                 the key of the setting this validator is attached to
         * @param isThrottleThreshold true if that setting is the throttle threshold, false if it is the deny threshold
         * @param pairedSetting       supplies the other threshold of the pair
         */
        ThresholdValidator(String key, boolean isThrottleThreshold, Supplier<Setting<Double>> pairedSetting) {
            this.key = key;
            this.isThrottleThreshold = isThrottleThreshold;
            this.pairedSetting = pairedSetting;
        }

        @Override
        public void validate(Double value) {}

        @Override
        public void validate(Double value, Map<Setting<?>, Object> settings) {
            final Setting<Double> paired = pairedSetting.get();
            final Double pairedValue = (Double) settings.get(paired);
            if (pairedValue == null) {
                return;
            }
            final double throttle = isThrottleThreshold ? value : pairedValue;
            final double deny = isThrottleThreshold ? pairedValue : value;
            if (throttle > deny) {
                final String throttleKey = isThrottleThreshold ? key : paired.getKey();
                final String denyKey = isThrottleThreshold ? paired.getKey() : key;
                throw new IllegalArgumentException(
                    "["
                        + throttleKey
                        + "="
                        + throttle
                        + "] must be less than or equal to ["
                        + denyKey
                        + "="
                        + deny
                        + "]"
                );
            }
        }

        @Override
        public Iterator<Setting<?>> settings() {
            final List<Setting<?>> settings = List.of(pairedSetting.get());
            return settings.iterator();
        }
    }
}
