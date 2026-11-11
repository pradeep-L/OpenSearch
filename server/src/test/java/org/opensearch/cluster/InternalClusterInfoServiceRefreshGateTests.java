/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster;

import org.opensearch.Version;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.coordination.DeterministicTaskQueue;
import org.opensearch.cluster.coordination.MockSinglePrioritizingExecutor;
import org.opensearch.cluster.coordination.NoClusterManagerBlockService;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.allocation.DiskThresholdSettings;
import org.opensearch.cluster.routing.allocation.decider.CpuUsageAllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.JvmMemoryUsageAllocationDecider;
import org.opensearch.cluster.service.ClusterApplier;
import org.opensearch.cluster.service.ClusterApplierService;
import org.opensearch.cluster.service.ClusterManagerService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.cluster.service.FakeThreadPoolClusterManagerService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.PrioritizedOpenSearchThreadPoolExecutor;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.node.Node;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpClient;
import org.opensearch.threadpool.ThreadPool;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.opensearch.cluster.InternalClusterInfoService.INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests that {@link InternalClusterInfoService} refreshes cluster info while the disk threshold decider or either resource
 * usage decider is enabled, and only then.
 */
public class InternalClusterInfoServiceRefreshGateTests extends OpenSearchTestCase {

    private static final Setting<Boolean> DISK = DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING;
    private static final Setting<Boolean> CPU = CpuUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_RESOURCE_CPU_ENABLED_SETTING;
    private static final Setting<Boolean> JVM = JvmMemoryUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_RESOURCE_JVM_ENABLED_SETTING;

    private static Settings flags(boolean disk, boolean cpu, boolean jvm) {
        return Settings.builder().put(DISK.getKey(), disk).put(CPU.getKey(), cpu).put(JVM.getKey(), jvm).build();
    }

    private static InternalClusterInfoService service(Settings settings, ClusterSettings clusterSettings) {
        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        return new InternalClusterInfoService(settings, clusterService, mock(ThreadPool.class), null);
    }

    public void testRefreshEnabledForEachFlagCombination() {
        for (boolean disk : new boolean[] { false, true }) {
            for (boolean cpu : new boolean[] { false, true }) {
                for (boolean jvm : new boolean[] { false, true }) {
                    final Settings settings = flags(disk, cpu, jvm);
                    final InternalClusterInfoService service = service(
                        settings,
                        new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
                    );
                    assertThat(settings.toString(), service.isRefreshEnabled(), equalTo(disk || cpu || jvm));
                }
            }
        }
    }

    public void testRefreshEnabledFollowsDynamicUpdates() {
        final Settings settings = flags(false, false, false);
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final InternalClusterInfoService service = service(settings, clusterSettings);
        assertFalse(service.isRefreshEnabled());

        clusterSettings.applySettings(Settings.builder().put(CPU.getKey(), true).build());
        assertTrue(service.isRefreshEnabled());

        clusterSettings.applySettings(Settings.builder().put(JVM.getKey(), true).build());
        assertTrue(service.isRefreshEnabled());

        clusterSettings.applySettings(Settings.builder().put(JVM.getKey(), false).build());
        assertFalse(service.isRefreshEnabled());

        clusterSettings.applySettings(Settings.builder().put(DISK.getKey(), true).build());
        assertTrue(service.isRefreshEnabled());
    }

    public void testDefaultSettingsKeepDiskOnlyBehaviour() {
        // disk threshold decider is enabled by default, resource deciders are disabled by default
        final InternalClusterInfoService service = service(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        assertTrue(service.isRefreshEnabled());
    }

    public void testScheduledRefreshRunsWithOnlyResourceDeciderEnabled() {
        final Setting<Boolean> resource = randomBoolean() ? CPU : JVM;
        final Settings settings = Settings.builder().put(DISK.getKey(), false).put(resource.getKey(), true).build();
        assertThat(requestsOverIntervals(settings, 3), equalTo(6)); // nodes stats + indices stats per interval
    }

    public void testScheduledRefreshSkippedWhenAllDisabled() {
        assertThat(requestsOverIntervals(flags(false, false, false), 3), equalTo(0));
    }

    public void testScheduledRefreshRunsWithOnlyDiskDeciderEnabled() {
        assertThat(requestsOverIntervals(flags(true, false, false), 3), equalTo(6));
    }

    /**
     * Elects the local node as cluster-manager, then counts the stats requests sent over the given number of refresh
     * intervals. The initial refresh on election is not counted.
     */
    private int requestsOverIntervals(Settings flagSettings, int intervals) {
        final DiscoveryNode discoveryNode = new DiscoveryNode("test", buildNewFakeTransportAddress(), Version.CURRENT);
        final DiscoveryNodes noClusterManager = DiscoveryNodes.builder().add(discoveryNode).localNodeId(discoveryNode.getId()).build();
        final DiscoveryNodes localClusterManager = DiscoveryNodes.builder(noClusterManager)
            .clusterManagerNodeId(discoveryNode.getId())
            .build();

        final Settings settings = Settings.builder()
            .put(Node.NODE_NAME_SETTING.getKey(), discoveryNode.getName())
            .put(flagSettings)
            .build();
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue(settings, random());
        final ThreadPool threadPool = deterministicTaskQueue.getThreadPool();

        final ClusterApplierService clusterApplierService = new ClusterApplierService("test", settings, clusterSettings, threadPool) {
            @Override
            protected PrioritizedOpenSearchThreadPoolExecutor createThreadPoolExecutor() {
                return new MockSinglePrioritizingExecutor("mock-executor", deterministicTaskQueue, threadPool);
            }
        };
        final ClusterManagerService clusterManagerService = new FakeThreadPoolClusterManagerService(
            "test",
            "clusterManagerService",
            threadPool,
            r -> fail("cluster-manager service should not run any tasks")
        );
        final ClusterService clusterService = new ClusterService(settings, clusterSettings, clusterManagerService, clusterApplierService);

        final CountingClient client = new CountingClient(threadPool);
        final InternalClusterInfoService clusterInfoService = new InternalClusterInfoService(settings, clusterService, threadPool, client);
        clusterService.addListener(clusterInfoService);
        clusterInfoService.addListener(ignored -> {});

        clusterService.setNodeConnectionsService(ClusterServiceUtils.createNoOpNodeConnectionsService());
        clusterApplierService.setInitialState(ClusterState.builder(new ClusterName("cluster")).nodes(noClusterManager).build());
        clusterManagerService.setClusterStatePublisher((event, publishListener, ackListener) -> fail("should not publish"));
        clusterManagerService.setClusterStateSupplier(clusterApplierService::state);
        clusterService.start();

        final AtomicBoolean becameClusterManager = new AtomicBoolean();
        clusterApplierService.onNewClusterState(
            "become cluster-manager",
            () -> ClusterState.builder(new ClusterName("cluster")).nodes(localClusterManager).build(),
            new ClusterApplier.ClusterApplyListener() {
                @Override
                public void onSuccess(String source) {
                    assertTrue(becameClusterManager.compareAndSet(false, true));
                }

                @Override
                public void onFailure(String source, Exception e) {
                    throw new AssertionError("unexpected", e);
                }
            }
        );
        while (becameClusterManager.get() == false) {
            deterministicTaskQueue.runAllRunnableTasks();
        }

        final int initialRequestCount = client.requestCount;
        final long interval = INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL_SETTING.get(settings).millis();
        final long endTime = deterministicTaskQueue.getCurrentTimeMillis() + interval * intervals;
        // the refresh task reschedules itself every interval, so this stops right after the run at endTime
        while (deterministicTaskQueue.hasDeferredTasks() && deterministicTaskQueue.getCurrentTimeMillis() < endTime) {
            deterministicTaskQueue.advanceTime();
            deterministicTaskQueue.runAllRunnableTasks();
        }
        return client.requestCount - initialRequestCount;
    }

    private static class CountingClient extends NoOpClient {

        int requestCount;

        CountingClient(ThreadPool threadPool) {
            super(threadPool);
        }

        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            if (request instanceof NodesStatsRequest || request instanceof IndicesStatsRequest) {
                requestCount++;
                // ClusterInfoService handles ClusterBlockExceptions quietly, so we invent such an exception to avoid excess logging
                listener.onFailure(new ClusterBlockException(Set.of(NoClusterManagerBlockService.NO_CLUSTER_MANAGER_BLOCK_ALL)));
            } else {
                fail("unexpected action: " + action.name());
            }
        }
    }
}
