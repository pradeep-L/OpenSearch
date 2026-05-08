/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.probe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.core.common.unit.ByteSizeValue;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Debug probe (POC) that periodically calls {@link NativeBridge#getActiveQueryStats()} and logs
 * the decoded per-query {@code (contextId, currentBytes, peakBytes)} triples plus the wall-clock
 * elapsed time of the FFM call.
 *
 * <p>The probe owns a single daemon {@link ScheduledExecutorService} named
 * {@value #THREAD_NAME} and runs one cycle every {@value #SAMPLE_INTERVAL_MILLIS} ms. All
 * output goes through {@link LogManager#getLogger(Class)}; there is no REST/stats/cancellation
 * surface.
 *
 * <p><b>Enablement.</b> The probe is off by default. It is constructed and started by
 * {@code DataFusionPlugin.createComponents(...)} only when the JVM system property
 * {@code opensearch.datafusion.active_query_stats_probe.enabled} is set to {@code true}
 * (case-insensitive, after {@code trim()}). The property is read once at plugin
 * {@code createComponents} time; changes at runtime are not observed.
 *
 * <p><b>Self-disable.</b> If the worker catches {@link NoClassDefFoundError},
 * {@link UnsatisfiedLinkError}, or {@link ExceptionInInitializerError} from the native bridge,
 * it emits a single WARN line, cancels its own scheduled future, and goes silent — broken
 * native setups produce one actionable line, not a flood.
 *
 * <p><b>Thread-safety.</b> The single-worker executor serializes cycles, so the mutable
 * worker state ({@code cycleCounter}, {@code selfDisabled}) is read and written only from the
 * worker thread and needs no volatile.
 */
public final class ActiveQueryStatsFfmProbe {

    /** Sample period (ms). Hard-coded per the POC spec; no setting is exposed. */
    public static final long SAMPLE_INTERVAL_MILLIS = 1000L;

    /** Daemon thread name; matches the design for thread-dump visibility. */
    public static final String THREAD_NAME = "datafusion-active-query-stats-probe";

    private static final Logger logger = LogManager.getLogger(ActiveQueryStatsFfmProbe.class);

    private final ScheduledExecutorService executor;
    private volatile ScheduledFuture<?> future;

    // Worker-thread-only state — no volatile needed (single-thread executor is the barrier).
    private long cycleCounter;
    private boolean selfDisabled;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Construct a probe bound to {@link NativeBridge#getActiveQueryStats()} as its stats source.
     * Nothing is scheduled until {@link #start()} is called.
     */
    public ActiveQueryStatsFfmProbe() {
        ThreadFactory threadFactory = r -> {
            Thread t = new Thread(r, THREAD_NAME);
            t.setDaemon(true);
            return t;
        };
        this.executor = Executors.newSingleThreadScheduledExecutor(threadFactory);
    }

    /**
     * Start the scheduled worker. Idempotent: a second call is a no-op.
     *
     * <p>Submits one task via {@link ScheduledExecutorService#scheduleAtFixedRate} with an
     * initial delay of {@code 0} so the first cycle runs immediately and subsequent cycles run
     * every {@link #SAMPLE_INTERVAL_MILLIS} ms.
     */
    public void start() {
        if (started.compareAndSet(false, true) == false) {
            return;
        }
        this.future = executor.scheduleAtFixedRate(this::probeCycle, 0L, SAMPLE_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
        logger.info("[probe] started interval={}ms thread={}", SAMPLE_INTERVAL_MILLIS, THREAD_NAME);
    }

    /**
     * Cancel the scheduled task, {@link ScheduledExecutorService#shutdownNow()} the executor,
     * and {@link ScheduledExecutorService#awaitTermination(long, TimeUnit)} for up to 2
     * seconds. Logs WARN and returns without throwing if the executor does not terminate in
     * time. Idempotent.
     */
    public void close() {
        if (closed.compareAndSet(false, true) == false) {
            return;
        }
        executor.shutdownNow();
        try {
            if (executor.awaitTermination(2L, TimeUnit.SECONDS) == false) {
                logger.warn("[probe] executor did not terminate within 2s");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Single probe cycle — runs on the worker thread. Called by the
     * {@link ScheduledExecutorService}.
     */
    private void probeCycle() {
        if (selfDisabled) {
            // Defensive: the future should already be cancelled in this state.
            return;
        }
        try {
            long startNanos = System.nanoTime();
            long[] arr = NativeBridge.getActiveQueryStats();
            long endNanos = System.nanoTime();
            long elapsedNs = endNanos - startNanos;
            long elapsedUs = elapsedNs / 1_000L;
            long elapsedMs = elapsedNs / 1_000_000L;

            if (arr.length == 0) {
                logger.info("[probe] cycle={} activeQueries=0 elapsedUs={} elapsedMs={}", cycleCounter + 1, elapsedUs, elapsedMs);
            } else {
                int n = arr.length / 3;
                logger.info("[probe] cycle={} activeQueries={} elapsedUs={} elapsedMs={}", cycleCounter + 1, n, elapsedUs, elapsedMs);
                for (int i = 0; i < n; i++) {
                    long ctx = arr[3 * i];
                    long current = arr[3 * i + 1];
                    long peak = arr[3 * i + 2];
                    logger.info(
                        "[probe] cycle={} triple={} ctx={} currentBytes={} ({}) peakBytes={} ({})",
                        cycleCounter + 1,
                        i,
                        ctx,
                        current,
                        new ByteSizeValue(current),
                        peak,
                        new ByteSizeValue(peak)
                    );
                }
            }
        } catch (NoClassDefFoundError | UnsatisfiedLinkError | ExceptionInInitializerError t) {
            // Terminal: the native runtime is unavailable. Log BEFORE cancel so the WARN line
            // is preserved (scheduleAtFixedRate treats an uncaught throw as an implicit cancel
            // which would silently lose the log line).
            selfDisabled = true;
            logger.warn("[probe] self-disabling: terminalError={} message={}", t.getClass().getName(), t.getMessage());
            if (future != null) {
                future.cancel(false);
            }
        } catch (Throwable t) {
            // Cycle-local: log and continue. Next scheduled cycle will run normally.
            logger.warn("[probe] cycle={} cycle-local error class={} message={}", cycleCounter + 1, t.getClass().getName(), t.getMessage());
        } finally {
            cycleCounter++;
        }
    }
}
