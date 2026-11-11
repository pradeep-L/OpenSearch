/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation.decider;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterInfo;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.node.IoUsageStats;
import org.opensearch.node.NodeResourceUsageStats;
import org.opensearch.snapshots.EmptySnapshotsInfoService;
import org.opensearch.test.gateway.TestGatewayAllocator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * Shared tests for {@link AbstractResourceUsageAllocationDecider} subclasses. Each subclass supplies its decider, settings
 * and a way to build {@link NodeResourceUsageStats} with a given value for its own resource.
 */
public abstract class AbstractResourceUsageAllocationDeciderTestCase extends OpenSearchAllocationTestCase {

    private static final long NOW = 1_000_000_000L;
    private static final String HOT = "node_hot";
    private static final String COOL = "node_cool";

    protected abstract AbstractResourceUsageAllocationDecider createDecider(
        Settings settings,
        ClusterSettings clusterSettings,
        LongSupplier currentTimeMillis
    );

    protected abstract String deciderName();

    protected abstract String resourceName();

    protected abstract Setting<Boolean> enabledSetting();

    protected abstract Setting<Double> throttleThresholdSetting();

    protected abstract Setting<Double> denyThresholdSetting();

    protected abstract Setting<TimeValue> statsMaxAgeSetting();

    /**
     * Builds stats where this decider's resource is {@code usage} and the other gated resource is {@code otherUsage}.
     */
    protected abstract NodeResourceUsageStats stats(String nodeId, long timestamp, double usage, double otherUsage);

    private Settings enabledSettings() {
        return Settings.builder().put(enabledSetting().getKey(), true).build();
    }

    private AbstractResourceUsageAllocationDecider enabledDecider(LongSupplier clock) {
        final Settings settings = enabledSettings();
        return createDecider(settings, new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), clock);
    }

    private NodeResourceUsageStats stats(String nodeId, double usage) {
        return stats(nodeId, NOW, usage, 0.0);
    }

    // ---- threshold boundaries ----

    public void testYesBelowThrottleThreshold() {
        final Decision decision = decideReplica(enabledDecider(() -> NOW), Map.of(HOT, stats(HOT, 89.9)));
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertThat(explanation(decision), containsString("[89.9%] is below the throttle threshold [90.0%]"));
        assertThat(decision.label(), equalTo(deciderName()));
    }

    public void testThrottleAtThrottleThreshold() {
        final Decision decision = decideReplica(enabledDecider(() -> NOW), Map.of(HOT, stats(HOT, 90.0)));
        assertThat(decision.type(), equalTo(Decision.Type.THROTTLE));
        assertThat(
            explanation(decision),
            containsString(
                "the node's "
                    + resourceName()
                    + " usage [90.0%] is at or above the throttle threshold ["
                    + throttleThresholdSetting().getKey()
                    + "=90.0%]"
            )
        );
    }

    public void testThrottleJustBelowDenyThreshold() {
        final Decision decision = decideReplica(enabledDecider(() -> NOW), Map.of(HOT, stats(HOT, 94.9)));
        assertThat(decision.type(), equalTo(Decision.Type.THROTTLE));
    }

    public void testNoAtDenyThreshold() {
        final Decision decision = decideReplica(enabledDecider(() -> NOW), Map.of(HOT, stats(HOT, 95.0)));
        assertThat(decision.type(), equalTo(Decision.Type.NO));
        assertThat(
            explanation(decision),
            containsString(
                "the node's "
                    + resourceName()
                    + " usage [95.0%] is at or above the deny threshold ["
                    + denyThresholdSetting().getKey()
                    + "=95.0%]"
            )
        );
    }

    public void testNoAtFullUsage() {
        final Decision decision = decideReplica(enabledDecider(() -> NOW), Map.of(HOT, stats(HOT, 100.0)));
        assertThat(decision.type(), equalTo(Decision.Type.NO));
    }

    // ---- primaries ----

    public void testUnassignedPrimaryIsThrottledInsteadOfDenied() {
        final AbstractResourceUsageAllocationDecider decider = enabledDecider(() -> NOW);
        final Map<String, NodeResourceUsageStats> usage = Map.of(HOT, stats(HOT, 99.0));
        for (RecoverySource source : List.of(
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            RecoverySource.ExistingStoreRecoverySource.INSTANCE
        )) {
            final ShardRouting primary = ShardRouting.newUnassigned(
                shardId(),
                true,
                source,
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test")
            );
            final Decision decision = decide(decider, primary, usage, 2);
            assertThat(source.toString(), decision.type(), equalTo(Decision.Type.THROTTLE));
            assertThat(explanation(decision), containsString("throttling instead of denying because the shard is an unassigned primary"));
        }
    }

    public void testAssignedPrimaryRelocatingToHotNodeIsDenied() {
        final ShardRouting startedPrimary = TestShardRouting.newShardRouting(shardId(), COOL, true, ShardRoutingState.STARTED);
        final Decision decision = decide(enabledDecider(() -> NOW), startedPrimary, Map.of(HOT, stats(HOT, 99.0)), 2);
        assertThat(decision.type(), equalTo(Decision.Type.NO));
        assertThat(explanation(decision), not(containsString("unassigned primary")));
    }

    // ---- fail open ----

    public void testDisabledReturnsYes() {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final AbstractResourceUsageAllocationDecider decider = createDecider(Settings.EMPTY, clusterSettings, () -> NOW);
        final Decision decision = decideReplica(decider, Map.of(HOT, stats(HOT, 100.0)));
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertThat(explanation(decision), containsString("usage allocation decider is disabled"));
    }

    public void testSingleDataNodeReturnsYes() {
        final Decision decision = decide(enabledDecider(() -> NOW), unassignedReplica(), Map.of(HOT, stats(HOT, 100.0)), 1);
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertThat(explanation(decision), containsString("single data node"));
    }

    public void testNullClusterInfoReturnsYes() {
        final RoutingAllocation allocation = allocation(null, 2);
        final Decision decision = enabledDecider(() -> NOW).canAllocate(unassignedReplica(), routingNode(allocation, HOT), allocation);
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertThat(explanation(decision), containsString("cluster info is unavailable"));
    }

    public void testEmptyStatsReturnsYes() {
        final Decision decision = decideReplica(enabledDecider(() -> NOW), Map.of());
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertThat(explanation(decision), containsString("node resource usage stats are unavailable"));
    }

    public void testMissingNodeEntryReturnsYes() {
        final Decision decision = decideReplica(enabledDecider(() -> NOW), Map.of(COOL, stats(COOL, 100.0)));
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertThat(explanation(decision), containsString("no resource usage stats are available for the node"));
    }

    public void testUnknownValuesReturnYes() {
        final AbstractResourceUsageAllocationDecider decider = enabledDecider(() -> NOW);
        for (double unknown : new double[] { 0.0, -1.0, Double.NaN, Double.POSITIVE_INFINITY }) {
            final Decision decision = decideReplica(decider, Map.of(HOT, stats(HOT, unknown)));
            assertThat(Double.toString(unknown), decision.type(), equalTo(Decision.Type.YES));
            assertThat(explanation(decision), containsString("usage is unknown"));
        }
    }

    public void testStaleStatsReturnYes() {
        final AtomicLong clock = new AtomicLong(NOW);
        final AbstractResourceUsageAllocationDecider decider = enabledDecider(clock::get);
        final Map<String, NodeResourceUsageStats> usage = Map.of(HOT, stats(HOT, 100.0));

        // exactly at the default max age (2m) the sample still counts as fresh
        clock.set(NOW + TimeValue.timeValueMinutes(2).millis());
        assertThat(decideReplica(decider, usage).type(), equalTo(Decision.Type.NO));

        clock.set(NOW + TimeValue.timeValueMinutes(2).millis() + 1);
        final Decision decision = decideReplica(decider, usage);
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertThat(explanation(decision), containsString("stats are stale"));
        assertThat(explanation(decision), containsString("max age [2m]"));
    }

    public void testFutureTimestampCountsAsFresh() {
        final Decision decision = decideReplica(enabledDecider(() -> NOW), Map.of(HOT, stats(HOT, NOW + 60_000, 100.0, 0.0)));
        assertThat(decision.type(), equalTo(Decision.Type.NO));
    }

    public void testOnlyOwnResourceIsEvaluated() {
        final Decision decision = decideReplica(enabledDecider(() -> NOW), Map.of(HOT, stats(HOT, NOW, 10.0, 100.0)));
        assertThat(decision.type(), equalTo(Decision.Type.YES));
    }

    // ---- dynamic settings ----

    public void testDynamicSettingsUpdates() {
        final Settings nodeSettings = enabledSettings();
        final ClusterSettings clusterSettings = new ClusterSettings(nodeSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final AtomicLong clock = new AtomicLong(NOW);
        final AbstractResourceUsageAllocationDecider decider = createDecider(nodeSettings, clusterSettings, clock::get);
        final Map<String, NodeResourceUsageStats> usage = Map.of(HOT, stats(HOT, 80.0));
        assertThat(decideReplica(decider, usage).type(), equalTo(Decision.Type.YES));

        clusterSettings.applySettings(
            Settings.builder().put(throttleThresholdSetting().getKey(), 70.0).put(denyThresholdSetting().getKey(), 75.0).build()
        );
        final Decision denied = decideReplica(decider, usage);
        assertThat(denied.type(), equalTo(Decision.Type.NO));
        assertThat(explanation(denied), containsString(denyThresholdSetting().getKey() + "=75.0%"));

        clusterSettings.applySettings(
            Settings.builder().put(throttleThresholdSetting().getKey(), 70.0).put(denyThresholdSetting().getKey(), 85.0).build()
        );
        assertThat(decideReplica(decider, usage).type(), equalTo(Decision.Type.THROTTLE));

        // a 10ms old sample is fresh under the default max age (2m) and stale once the max age is lowered to 1ms
        clock.set(NOW + 10);
        assertThat(decideReplica(decider, usage).type(), equalTo(Decision.Type.THROTTLE));
        clusterSettings.applySettings(
            Settings.builder()
                .put(throttleThresholdSetting().getKey(), 70.0)
                .put(denyThresholdSetting().getKey(), 85.0)
                .put(statsMaxAgeSetting().getKey(), "1ms")
                .build()
        );
        assertThat(explanation(decideReplica(decider, usage)), containsString("stats are stale"));

        clusterSettings.applySettings(Settings.builder().put(enabledSetting().getKey(), false).build());
        assertThat(explanation(decideReplica(decider, Map.of(HOT, stats(HOT, 100.0)))), containsString("disabled"));
    }

    // ---- settings validation ----

    public void testThresholdDefaults() {
        assertThat(enabledSetting().get(Settings.EMPTY), equalTo(false));
        assertThat(throttleThresholdSetting().get(Settings.EMPTY), equalTo(90.0));
        assertThat(denyThresholdSetting().get(Settings.EMPTY), equalTo(95.0));
        assertThat(statsMaxAgeSetting().get(Settings.EMPTY), equalTo(TimeValue.timeValueMinutes(2)));
    }

    public void testThresholdOutOfRangeIsRejected() {
        for (Setting<Double> setting : List.of(throttleThresholdSetting(), denyThresholdSetting())) {
            for (double value : new double[] { -1.0, 100.1 }) {
                expectThrows(IllegalArgumentException.class, () -> setting.get(Settings.builder().put(setting.getKey(), value).build()));
            }
        }
    }

    public void testThrottleAboveDenyIsRejected() {
        final Settings settings = Settings.builder()
            .put(throttleThresholdSetting().getKey(), 96.0)
            .put(denyThresholdSetting().getKey(), 95.0)
            .build();
        final IllegalArgumentException e1 = expectThrows(IllegalArgumentException.class, () -> throttleThresholdSetting().get(settings));
        assertThat(e1.getMessage(), containsString(throttleThresholdSetting().getKey() + "=96.0"));
        assertThat(e1.getMessage(), containsString(denyThresholdSetting().getKey() + "=95.0"));
        final IllegalArgumentException e2 = expectThrows(IllegalArgumentException.class, () -> denyThresholdSetting().get(settings));
        assertThat(e2.getMessage(), containsString(throttleThresholdSetting().getKey() + "=96.0"));

        // lowering only the deny threshold below the default throttle threshold is rejected as well
        final Settings lowDeny = Settings.builder().put(denyThresholdSetting().getKey(), 85.0).build();
        expectThrows(IllegalArgumentException.class, () -> denyThresholdSetting().get(lowDeny));
    }

    public void testThrottleEqualToDenyIsAccepted() {
        final Settings settings = Settings.builder()
            .put(throttleThresholdSetting().getKey(), 80.0)
            .put(denyThresholdSetting().getKey(), 80.0)
            .build();
        assertThat(throttleThresholdSetting().get(settings), equalTo(80.0));
        assertThat(denyThresholdSetting().get(settings), equalTo(80.0));
    }

    public void testInvalidDynamicUpdateIsRejected() {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        createDecider(Settings.EMPTY, clusterSettings, () -> NOW);
        expectThrows(
            IllegalArgumentException.class,
            () -> clusterSettings.applySettings(Settings.builder().put(throttleThresholdSetting().getKey(), 99.0).build())
        );
    }

    public void testNonPositiveMaxAgeIsRejected() {
        expectThrows(
            IllegalArgumentException.class,
            () -> statsMaxAgeSetting().get(Settings.builder().put(statsMaxAgeSetting().getKey(), "0ms").build())
        );
    }

    // ---- end to end through AllocationService ----

    public void testRerouteKeepsShardsOffHotNode() {
        final Settings settings = enabledSettings();
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final AbstractResourceUsageAllocationDecider decider = createDecider(settings, clusterSettings, System::currentTimeMillis);
        final long now = System.currentTimeMillis();
        final Map<String, NodeResourceUsageStats> usage = new HashMap<>();
        usage.put(HOT, stats(HOT, now, 97.0, 0.0));
        usage.put("node_cool_1", stats("node_cool_1", now, 10.0, 0.0));
        usage.put("node_cool_2", stats("node_cool_2", now, 10.0, 0.0));
        final ClusterInfo clusterInfo = clusterInfo(usage);

        final AllocationService service = new MockAllocationService(
            new AllocationDeciders(
                List.of(
                    new SameShardAllocationDecider(settings, clusterSettings),
                    new ReplicaAfterPrimaryActiveAllocationDecider(),
                    decider
                )
            ),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(settings),
            () -> clusterInfo,
            EmptySnapshotsInfoService.INSTANCE
        );

        final Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .build();
        ClusterState state = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(RoutingTable.builder().addAsNew(metadata.index("test")).build())
            .nodes(DiscoveryNodes.builder().add(newNode(HOT)).add(newNode("node_cool_1")).add(newNode("node_cool_2")))
            .build();

        state = service.reroute(state, "reroute");
        state = applyStartedShardsUntilNoChange(state, service);

        assertThat(state.getRoutingNodes().node(HOT).size(), equalTo(0));
        assertThat(state.routingTable().shardsWithState(ShardRoutingState.STARTED).size(), equalTo(2));
        assertThat(state.getRoutingNodes().node("node_cool_1").size(), equalTo(1));
        assertThat(state.getRoutingNodes().node("node_cool_2").size(), equalTo(1));
    }

    // ---- helpers ----

    private static ShardId shardId() {
        return new ShardId(new Index("test", "_na_"), 0);
    }

    private static ShardRouting unassignedReplica() {
        return ShardRouting.newUnassigned(
            shardId(),
            false,
            RecoverySource.PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.REPLICA_ADDED, "test")
        );
    }

    private static ClusterInfo clusterInfo(Map<String, NodeResourceUsageStats> usage) {
        return new ClusterInfo(Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), usage);
    }

    private Decision decideReplica(AbstractResourceUsageAllocationDecider decider, Map<String, NodeResourceUsageStats> usage) {
        return decide(decider, unassignedReplica(), usage, 2);
    }

    private Decision decide(
        AbstractResourceUsageAllocationDecider decider,
        ShardRouting shard,
        Map<String, NodeResourceUsageStats> usage,
        int numDataNodes
    ) {
        final RoutingAllocation allocation = allocation(clusterInfo(usage), numDataNodes);
        return decider.canAllocate(shard, routingNode(allocation, HOT), allocation);
    }

    private RoutingAllocation allocation(ClusterInfo clusterInfo, int numDataNodes) {
        final Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .build();
        final DiscoveryNodes.Builder nodes = DiscoveryNodes.builder().add(newNode(HOT));
        if (numDataNodes > 1) {
            nodes.add(newNode(COOL));
        }
        final ClusterState state = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(RoutingTable.builder().addAsNew(metadata.index("test")).build())
            .nodes(nodes)
            .build();
        final RoutingAllocation allocation = new RoutingAllocation(
            new AllocationDeciders(List.of()),
            state.getRoutingNodes(),
            state,
            clusterInfo,
            null,
            System.nanoTime()
        );
        allocation.debugDecision(true);
        return allocation;
    }

    private static RoutingNode routingNode(RoutingAllocation allocation, String nodeId) {
        return new RoutingNode(nodeId, allocation.nodes().get(nodeId));
    }

    private static String explanation(Decision decision) {
        return ((Decision.Single) decision).getExplanation();
    }

    protected static NodeResourceUsageStats newStats(String nodeId, long timestamp, double memoryPercent, double cpuPercent) {
        return new NodeResourceUsageStats(nodeId, timestamp, memoryPercent, cpuPercent, new IoUsageStats(0.0), 0.0);
    }
}
