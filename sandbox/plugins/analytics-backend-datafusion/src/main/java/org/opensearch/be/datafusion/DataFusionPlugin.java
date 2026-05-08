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
import org.opensearch.be.datafusion.probe.ActiveQueryStatsFfmProbe;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.dataformat.ReaderManagerConfig;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchBackEndPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;

/**
 * Main plugin class for the DataFusion native engine integration.
 * <p>
 * Owns the {@link DataFusionService} lifecycle (memory pool, native runtime).
 * Analytics query capabilities are declared in {@link DataFusionAnalyticsBackendPlugin},
 * which is SPI-discovered and receives this plugin instance via its constructor.
 */
public class DataFusionPlugin extends Plugin implements SearchBackEndPlugin<DatafusionReader> {

    private static final Logger logger = LogManager.getLogger(DataFusionPlugin.class);

    /** Memory pool limit for the DataFusion runtime. */
    public static final Setting<Long> DATAFUSION_MEMORY_POOL_LIMIT = Setting.longSetting(
        "datafusion.memory_pool_limit_bytes",
        Runtime.getRuntime().maxMemory() / 4,
        0L,
        Setting.Property.NodeScope
    );

    /** Spill memory limit — when exceeded, DataFusion spills to disk. */
    public static final Setting<Long> DATAFUSION_SPILL_MEMORY_LIMIT = Setting.longSetting(
        "datafusion.spill_memory_limit_bytes",
        Runtime.getRuntime().maxMemory() / 8,
        0L,
        Setting.Property.NodeScope
    );

    private static final String SUPPORTED_FORMAT = "parquet";

    private volatile DataFusionService dataFusionService;
    private volatile DataFormatRegistry dataFormatRegistry;
    private volatile SimpleExtension.ExtensionCollection substraitExtensions;
    private volatile ActiveQueryStatsFfmProbe activeQueryStatsFfmProbe;

    /**
     * Creates the DataFusion plugin.
     */
    public DataFusionPlugin() {}

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier,
        DataFormatRegistry dataFormatRegistry
    ) {
        this.dataFormatRegistry = dataFormatRegistry;
        Settings settings = environment.settings();
        long memoryPoolLimit = DATAFUSION_MEMORY_POOL_LIMIT.get(settings);
        long spillMemoryLimit = DATAFUSION_SPILL_MEMORY_LIMIT.get(settings);
        String spillDir = environment.dataFiles()[0].getParent().resolve("tmp").toAbsolutePath().toString();

        dataFusionService = DataFusionService.builder()
            .memoryPoolLimit(memoryPoolLimit)
            .spillMemoryLimit(spillMemoryLimit)
            .spillDirectory(spillDir)
            .build();
        dataFusionService.start();
        logger.debug("DataFusion plugin initialized — memory pool {}B, spill limit {}B", memoryPoolLimit, spillMemoryLimit);

        // Bridge native-memory tracking into core SearchBackpressureService (POC). This MUST happen before
        // Node.java constructs SearchBackpressureService (which reads these suppliers via its 5-arg
        // backward-compat constructor). createComponents runs before SBP construction per Node.java.
        final long poolLimitForSbp = memoryPoolLimit;
        org.opensearch.search.backpressure.trackers.NativeMemoryUsageTracker.registerSuppliers(
            new org.opensearch.search.backpressure.trackers.NativeMemoryUsageTracker.Suppliers(() -> {
                NativeRuntimeHandle h = dataFusionService.getNativeRuntime();
                return h.isOpen() ? h.get() : 0L;
            }, () -> poolLimitForSbp, () -> {
                NativeRuntimeHandle h = dataFusionService.getNativeRuntime();
                if (!h.isOpen()) return -1L;
                try {
                    return org.opensearch.be.datafusion.nativelib.NativeBridge.getMemoryPoolUsage(h.get());
                } catch (RuntimeException e) {
                    logger.debug("getMemoryPoolUsage failed; reporting -1 as error sentinel", e);
                    return -1L;
                }
            }, new DataFusionActiveQueryMemoryStats())
        );
        logger.info("Wired DataFusion runtime into SearchBackpressureService native-memory tracker");

        this.substraitExtensions = loadSubstraitExtensions();

        // POC debug probe: periodically sample active-query native-memory stats via the FFM
        // binding and log the decoded triples plus the call latency. Off by default; opt in
        // with -Dopensearch.datafusion.active_query_stats_probe.enabled=true. See
        // .kiro/specs/active-query-stats-ffm-probe/.
        String probeEnabledRaw = System.getProperty("opensearch.datafusion.active_query_stats_probe.enabled");
        if (probeEnabledRaw != null && Boolean.parseBoolean(probeEnabledRaw.trim())) {
            this.activeQueryStatsFfmProbe = new ActiveQueryStatsFfmProbe();
            this.activeQueryStatsFfmProbe.start();
        } else {
            logger.debug("active query stats FFM probe disabled (system property not set to true)");
        }

        return Collections.singletonList(dataFusionService);
    }

    private static SimpleExtension.ExtensionCollection loadSubstraitExtensions() {
        Thread t = Thread.currentThread();
        ClassLoader previous = t.getContextClassLoader();
        try {
            t.setContextClassLoader(DataFusionPlugin.class.getClassLoader());
            return DefaultExtensionCatalog.DEFAULT_COLLECTION;
        } finally {
            t.setContextClassLoader(previous);
        }
    }

    SimpleExtension.ExtensionCollection getSubstraitExtensions() {
        return substraitExtensions;
    }

    DataFormatRegistry getDataFormatRegistry() {
        return dataFormatRegistry;
    }

    DataFusionService getDataFusionService() {
        return dataFusionService;
    }

    @Override
    public String name() {
        return "datafusion";
    }

    @Override
    public EngineReaderManager<DatafusionReader> createReaderManager(ReaderManagerConfig settings) throws IOException {
        return new DatafusionReaderManager(settings.format(), settings.shardPath(), dataFusionService);
    }

    @Override
    public List<String> getSupportedFormats() {
        return List.of(SUPPORTED_FORMAT);
    }

    @Override
    public void close() throws IOException {
        if (activeQueryStatsFfmProbe != null) {
            activeQueryStatsFfmProbe.close();
        }
        if (dataFusionService != null) {
            dataFusionService.close();
        }
    }
}
