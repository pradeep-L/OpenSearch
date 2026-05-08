/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure;

import org.opensearch.Version;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.search.backpressure.settings.SearchBackpressureSettings;
import org.opensearch.search.backpressure.settings.SearchShardTaskSettings;
import org.opensearch.search.backpressure.trackers.GetActiveQueryMemoryStats;
import org.opensearch.search.backpressure.trackers.NativeMemoryUsageTracker;
import org.opensearch.search.backpressure.trackers.NodeDuressTrackers;
import org.opensearch.search.backpressure.trackers.NodeDuressTrackers.NodeDuressTracker;
import org.opensearch.search.backpressure.trackers.TaskResourceUsageTrackerType;
import org.opensearch.search.backpressure.trackers.TaskResourceUsageTrackers;
import org.opensearch.tasks.TaskCancellationService;
import org.opensearch.tasks.TaskManager;
import org.opensearch.tasks.TaskResourceTrackingService;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.wlm.ResourceType;
import org.opensearch.wlm.WorkloadGroupService;
import org.opensearch.wlm.WorkloadGroupTask;
import org.junit.After;
import org.junit.Before;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.search.backpressure.SearchBackpressureTestHelpers.createMockTaskWithResourceStats;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Service-level duress-gating test for {@link NativeMemoryUsageTracker}. Drives
 * {@link SearchBackpressureService#doRun()} with a fake {@code Supplier<long[]>} that reports a task whose
 * current bytes would comfortably breach both threshold floors, and asserts that:
 * <ol>
 *   <li>No {@code NATIVE_MEMORY_USAGE_TRACKER} cancellation fires while
 *       {@code nodeDuressTrackers.isResourceInDuress(NATIVE_MEMORY)} is {@code false} — even though the
 *       node is in duress overall (via {@code MEMORY}) so {@code doRun()} does not early-return.</li>
 *   <li>Flipping the NATIVE_MEMORY duress flag to {@code true} causes the same fixture to produce a
 *       cancellation.</li>
 * </ol>
 *
 * <p>This test lives in {@code org.opensearch.search.backpressure} (not
 * {@code ...backpressure.trackers}) because the package-private 9-arg test constructor of
 * {@link SearchBackpressureService} and its package-private {@code doRun()} method are only visible within
 * this package. The production {@link NativeMemoryUsageTracker} itself lives one package down, in
 * {@code ...backpressure.trackers}, and is imported here.
 *
 * <p>Validates: Requirements 1.8, 7.6
 */
public class NativeMemoryUsageTrackerSbpDuressGatingTests extends OpenSearchTestCase {

    private MockTransportService transportService;
    private TaskManager taskManager;
    private ThreadPool threadPool;
    private WorkloadGroupService workloadGroupService;

    @Before
    public void setup() {
        threadPool = new TestThreadPool(getClass().getName());
        workloadGroupService = mock(WorkloadGroupService.class);
        transportService = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool, NoopTracer.INSTANCE);
        transportService.start();
        transportService.acceptIncomingRequests();
        taskManager = transportService.getTaskManager();
        taskManager.setTaskCancellationService(new TaskCancellationService(transportService));
    }

    @After
    public void cleanup() {
        transportService.close();
        ThreadPool.terminate(threadPool, 5, TimeUnit.SECONDS);
    }

    /**
     * Verifies the duress gate for the native-memory tracker end-to-end through {@code doRun()}.
     *
     * <p>Validates: Requirements 1.8, 7.6
     */
    public void testNativeMemoryCancellationOnlyWhenNativeMemoryInDuress() throws Exception {
        // --- Duress flags the test will flip. MEMORY is always in duress so the node-level gate in doRun()
        // never early-returns; that lets us prove the NATIVE_MEMORY-specific trackerApplyConditions gate
        // is what keeps the tracker silent, not the coarse "isNodeInDuress" check. ---
        final AtomicBoolean nativeInDuress = new AtomicBoolean(false);
        final AtomicBoolean memoryInDuress = new AtomicBoolean(true);

        // numSuccessiveBreaches = 1 so the Streak in NodeDuressTracker reaches "in duress" on the first
        // observation and the test doesn't need to pre-pump doRun() cycles.
        NodeDuressTracker cpuTracker = new NodeDuressTracker(() -> false, () -> 1);
        NodeDuressTracker memoryTracker = new NodeDuressTracker(memoryInDuress::get, () -> 1);
        NodeDuressTracker nativeTracker = new NodeDuressTracker(nativeInDuress::get, () -> 1);

        EnumMap<ResourceType, NodeDuressTracker> duressMap = new EnumMap<>(ResourceType.class);
        duressMap.put(ResourceType.CPU, cpuTracker);
        duressMap.put(ResourceType.MEMORY, memoryTracker);
        duressMap.put(ResourceType.NATIVE_MEMORY, nativeTracker);
        // Resource-duress cache always expires so each isResourceInDuress(...) call re-evaluates the predicate.
        NodeDuressTrackers nodeDuressTrackers = new NodeDuressTrackers(duressMap, () -> true);

        // --- Build the single SearchShardTask fixture. createMockTaskWithResourceStats ignores the taskId
        // argument (pre-existing behavior — it assigns a random id internally), so we read the actual id
        // back after construction and feed THAT id to the fake stats supplier. ---
        SearchShardTask task = createMockTaskWithResourceStats(
            SearchShardTask.class,
            /*cpuUsage*/ 100L,
            /*heapUsage*/ 200L,
            /*taskId (ignored)*/ 0L
        );
        final long taskId = task.getId();
        task.setWorkloadGroupId(threadPool.getThreadContext());

        final long poolLimit = 10L * 1024 * 1024 * 1024L;                // 10 GiB
        final long peak = 100L * 1024 * 1024L;                            // 100 MiB
        // High current that comfortably breaches both the percent floor (0.5% * 10 GiB ≈ 53.7 MiB)
        // and the variance floor (2 * primed avg of 100 MiB = 200 MiB).
        final long highCurrent = 1L * 1024 * 1024 * 1024L;                // 1 GiB

        // Mutable holder so the test can swap what the supplier returns between phases.
        final AtomicReference<long[]> fakeStats = new AtomicReference<>(new long[] { taskId, 0L, peak });

        // Adapt the raw triple stream into a GetActiveQueryMemoryStats provider — production decoding lives in
        // the plugin (DataFusionActiveQueryMemoryStats); this is the test-side equivalent that keeps the
        // existing long[] fixtures unchanged.
        final GetActiveQueryMemoryStats statsProvider = () -> {
            long[] raw = fakeStats.get();
            if (raw == null || raw.length == 0 || raw.length % 3 != 0) {
                return java.util.Map.of();
            }
            java.util.Map<Long, long[]> snapshot = new java.util.HashMap<>(raw.length / 3);
            for (int i = 0; i + 2 < raw.length; i += 3) {
                snapshot.put(raw[i], new long[] { raw[i + 1], raw[i + 2] });
            }
            return snapshot;
        };

        // --- Build a NativeMemoryUsageTracker directly and register ONLY it in the SearchShardTask trackers.
        // Keeping CPU / HEAP / ELAPSED out of the map guarantees that any verify() on mockTaskManager
        // observes only the native tracker's decisions. ---
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final double variance = 2.0;
        final double percent = 0.005;
        final int windowSize = 10;

        NativeMemoryUsageTracker nativeMemTracker = new NativeMemoryUsageTracker(
            () -> variance,
            () -> percent,
            () -> poolLimit,
            windowSize,
            clusterSettings,
            SearchShardTaskSettings.SETTING_NATIVE_HEAP_MOVING_AVERAGE_WINDOW_SIZE,
            statsProvider
        );

        // Prime the moving average so isReady() is true before the service ever calls refreshStats(). Each
        // priming cycle refreshes the snapshot to [taskId, 0, peak] and then update(task) records the peak.
        for (int i = 0; i < windowSize; i++) {
            nativeMemTracker.refreshStats();
            nativeMemTracker.update(task);
        }

        TaskResourceUsageTrackers shardTrackers = new TaskResourceUsageTrackers();
        shardTrackers.addTracker(nativeMemTracker, TaskResourceUsageTrackerType.NATIVE_MEMORY_USAGE_TRACKER);

        // --- Settings: force totalHeapPercentThreshold to 0 so the "heap usage dominated by search" check in
        // doRun() always passes, letting the SearchShardTask list through to per-tracker evaluation.
        // We spy() the settings wrapper so we can return a mocked SearchShardTaskSettings — same pattern
        // as testSearchShardTaskInFlightCancellation in SearchBackpressureServiceTests. ---
        SearchBackpressureSettings settings = spy(
            new SearchBackpressureSettings(
                Settings.builder().put(SearchBackpressureSettings.SETTING_MODE.getKey(), "enforced").build(),
                clusterSettings
            )
        );
        // Hoist the real shard-settings values BEFORE any when(...).thenReturn(...) chain; nesting a spy-backed
        // call inside a thenReturn() triggers Mockito's UnfinishedStubbingException because the inner invocation
        // is interpreted as a stubbing target itself.
        SearchShardTaskSettings realShardSettings = settings.getSearchShardTaskSettings();
        double realCancellationRateNanos = realShardSettings.getCancellationRateNanos();
        double realCancellationBurst = realShardSettings.getCancellationBurst();
        double realCancellationRatio = realShardSettings.getCancellationRatio();
        double realCancellationRate = realShardSettings.getCancellationRate();

        SearchShardTaskSettings shardSettings = mock(SearchShardTaskSettings.class);
        when(shardSettings.getTotalHeapPercentThreshold()).thenReturn(0.0);
        when(shardSettings.getCancellationRateNanos()).thenReturn(realCancellationRateNanos);
        when(shardSettings.getCancellationBurst()).thenReturn(realCancellationBurst);
        when(shardSettings.getCancellationRatio()).thenReturn(realCancellationRatio);
        when(shardSettings.getCancellationRate()).thenReturn(realCancellationRate);
        when(settings.getSearchShardTaskSettings()).thenReturn(shardSettings);

        TaskResourceTrackingService mockTracking = mock(TaskResourceTrackingService.class);
        Map<Long, WorkloadGroupTask> activeTasks = new HashMap<>();
        activeTasks.put(taskId, task);
        doReturn(activeTasks).when(mockTracking).getResourceAwareTasks();

        TaskManager mockTaskManager = spy(taskManager);

        SearchBackpressureService service = new SearchBackpressureService(
            settings,
            mockTracking,
            threadPool,
            System::nanoTime,
            nodeDuressTrackers,
            /*searchTaskTrackers*/ new TaskResourceUsageTrackers(),
            shardTrackers,
            mockTaskManager,
            workloadGroupService
        );

        when(workloadGroupService.shouldSBPHandle(any())).thenReturn(true);

        // --- Phase 1: NATIVE_MEMORY is NOT in duress. Even though a task's current bytes would breach every
        // threshold the tracker evaluates, the service-level gate in trackerApplyConditions must prevent
        // the native tracker from being applied this cycle. ---
        fakeStats.set(new long[] { taskId, highCurrent, peak });
        long cancellationsBeforePhase1 = nativeMemTracker.getCancellations();
        service.doRun();
        verify(mockTaskManager, times(0)).cancelTaskAndDescendants(any(), anyString(), anyBoolean(), any());
        assertEquals(
            "no native-memory cancellations must be produced when NATIVE_MEMORY is not in duress (R1.8, R7.6)",
            cancellationsBeforePhase1,
            nativeMemTracker.getCancellations()
        );

        // --- Phase 2: flip NATIVE_MEMORY duress to true. The same task, with the same current bytes, must
        // now be cancelled by the native tracker. ---
        nativeInDuress.set(true);
        service.doRun();
        verify(mockTaskManager, times(1)).cancelTaskAndDescendants(any(), anyString(), anyBoolean(), any());
        assertEquals(
            "native-memory cancellation must fire exactly once after NATIVE_MEMORY duress is set (R1.8, R7.6)",
            cancellationsBeforePhase1 + 1L,
            nativeMemTracker.getCancellations()
        );
    }
}
