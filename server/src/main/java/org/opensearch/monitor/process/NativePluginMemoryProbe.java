/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.monitor.process;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Method;

/**
 * Probes memory usage reported by native (Rust) plugins that register mimalloc heaps via the
 * {@code NativeLibraryLoader} SPI in the {@code sandbox/libs/dataformat-native} library.
 *
 * <p>The {@code sandbox} modules are not visible to {@code server} at compile time (and are not
 * included in non-snapshot builds), so access is done reflectively. When the SPI class is not on
 * the classpath — which is the common case in production builds today — all methods return {@code
 * -1} and log a single DEBUG line the first time they are called.
 *
 * <p>This probe is observability-only and must not influence admission-control decisions.
 */
public final class NativePluginMemoryProbe {

    private static final Logger LOGGER = LogManager.getLogger(NativePluginMemoryProbe.class);

    private static final String NATIVE_LIBRARY_LOADER_CLASS = "org.opensearch.nativebridge.spi.NativeLibraryLoader";

    private static final Method HEAP_COUNT_METHOD;
    private static final Method HEAP_USED_METHOD;
    private static final boolean AVAILABLE;

    static {
        Method heapCount = null;
        Method heapUsed = null;
        boolean available = false;
        try {
            final Class<?> loaderClass = Class.forName(NATIVE_LIBRARY_LOADER_CLASS);
            heapCount = loaderClass.getMethod("heapCount");
            heapUsed = loaderClass.getMethod("heapUsed", int.class);
            available = true;
            LOGGER.debug("NativeLibraryLoader SPI detected; native plugin heap tracking is enabled");
        } catch (ClassNotFoundException e) {
            LOGGER.debug("NativeLibraryLoader SPI not on classpath; native plugin heap tracking disabled");
        } catch (NoSuchMethodException e) {
            LOGGER.debug("NativeLibraryLoader SPI is missing expected methods; native plugin heap tracking disabled", e);
        }
        HEAP_COUNT_METHOD = heapCount;
        HEAP_USED_METHOD = heapUsed;
        AVAILABLE = available;
    }

    private NativePluginMemoryProbe() {}

    /**
     * Returns whether the native plugin memory SPI is available on the classpath.
     */
    public static boolean isAvailable() {
        return AVAILABLE;
    }

    /**
     * Returns the total bytes currently used across all registered native (Rust) plugin heaps,
     * or {@code -1} if the SPI is not available or the call fails.
     *
     * <p>The value is the sum of {@code heapUsed(i)} for {@code i} in {@code [0, heapCount())}.
     * Logs the total at DEBUG on success.
     */
    public static long getRustPluginHeapUsedBytes() {
        if (!AVAILABLE) {
            return -1;
        }
        try {
            final int count = (int) HEAP_COUNT_METHOD.invoke(null);
            if (count <= 0) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("no native plugin heaps registered");
                }
                return 0L;
            }
            long total = 0L;
            for (int i = 0; i < count; i++) {
                final long used = (long) HEAP_USED_METHOD.invoke(null, i);
                if (used > 0) {
                    total += used;
                }
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("native plugin heap used across {} heap(s): {} bytes", count, total);
            }
            return total;
        } catch (Exception e) {
            LOGGER.warn("failed to read native plugin heap usage via SPI", e);
            return -1;
        }
    }
}
