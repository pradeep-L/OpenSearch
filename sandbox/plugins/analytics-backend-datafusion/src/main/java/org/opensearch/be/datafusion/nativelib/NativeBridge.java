/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.nativelib;

import org.opensearch.analytics.backend.jni.NativeHandle;
import org.opensearch.core.action.ActionListener;
import org.opensearch.nativebridge.spi.NativeCall;
import org.opensearch.nativebridge.spi.NativeLibraryLoader;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/**
 * FFM bridge to native DataFusion library.
 *
 * <h2>Pointer lifecycle (no Arena needed)</h2>
 * <p>Native pointers returned by {@code createGlobalRuntime}, {@code createDatafusionReader},
 * and {@code executeQueryAsync} are opaque {@code long} values — Rust heap addresses cast to
 * {@code i64}. They are <b>not</b> {@code MemorySegment}s and do not require an Arena. They
 * live until explicitly freed by the corresponding close method.</p>
 *
 * <h2>Arena usage</h2>
 * <p>{@link NativeCall} creates a confined Arena for short-lived allocations (strings, byte
 * arrays) that are only needed for the duration of the FFM call. The Arena is closed
 * immediately after the call returns, freeing all temp memory.</p>
 *
 * <h2>Error convention</h2>
 * <p>Functions return {@code i64}: {@code >= 0} is success, {@code < 0} is a negated pointer
 * to a heap-allocated error string. {@link NativeCall#invoke} reads and frees the error,
 * then throws.</p>
 */
public final class NativeBridge {

    private static final MethodHandle INIT_RUNTIME_MANAGER;
    private static final MethodHandle SHUTDOWN_RUNTIME_MANAGER;
    private static final MethodHandle CREATE_GLOBAL_RUNTIME;
    private static final MethodHandle CLOSE_GLOBAL_RUNTIME;
    private static final MethodHandle CREATE_READER;
    private static final MethodHandle CLOSE_READER;
    private static final MethodHandle EXECUTE_QUERY;
    private static final MethodHandle STREAM_GET_SCHEMA;
    private static final MethodHandle STREAM_NEXT;
    private static final MethodHandle STREAM_CLOSE;
    private static final MethodHandle SQL_TO_SUBSTRAIT;
    private static final MethodHandle INIT_HEAP;
    private static final MethodHandle GET_MEMORY_POOL_USAGE;
    private static final MethodHandle ACTIVE_QUERY_STATS_SIZE;
    private static final MethodHandle ACTIVE_QUERY_STATS_COPY;
    private static final MethodHandle DF_ALLOCATE_TEST_BUFFER;
    private static final MethodHandle DF_FREE_TEST_BUFFER;

    static {
        SymbolLookup lib = NativeLibraryLoader.symbolLookup();
        Linker linker = Linker.nativeLinker();

        INIT_RUNTIME_MANAGER = linker.downcallHandle(
            lib.find("df_init_runtime_manager").orElseThrow(),
            FunctionDescriptor.ofVoid(ValueLayout.JAVA_INT)
        );

        SHUTDOWN_RUNTIME_MANAGER = linker.downcallHandle(
            lib.find("df_shutdown_runtime_manager").orElseThrow(),
            FunctionDescriptor.ofVoid()
        );

        CREATE_GLOBAL_RUNTIME = linker.downcallHandle(
            lib.find("df_create_global_runtime").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG
            )
        );

        CLOSE_GLOBAL_RUNTIME = linker.downcallHandle(
            lib.find("df_close_global_runtime").orElseThrow(),
            FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG)
        );

        CREATE_READER = linker.downcallHandle(
            lib.find("df_create_reader").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG
            )
        );

        CLOSE_READER = linker.downcallHandle(lib.find("df_close_reader").orElseThrow(), FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG));

        EXECUTE_QUERY = linker.downcallHandle(
            lib.find("df_execute_query").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG
            )
        );

        STREAM_GET_SCHEMA = linker.downcallHandle(
            lib.find("df_stream_get_schema").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );

        STREAM_NEXT = linker.downcallHandle(
            lib.find("df_stream_next").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );

        STREAM_CLOSE = linker.downcallHandle(lib.find("df_stream_close").orElseThrow(), FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG));

        // i64 df_sql_to_substrait(shard_ptr, table_ptr, table_len, sql_ptr, sql_len, runtime_ptr, out_ptr, out_cap, out_len)
        SQL_TO_SUBSTRAIT = linker.downcallHandle(
            lib.find("df_sql_to_substrait").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS
            )
        );

        INIT_HEAP = linker.downcallHandle(lib.find("df_init_heap").orElseThrow(), FunctionDescriptor.ofVoid());
        GET_MEMORY_POOL_USAGE = linker.downcallHandle(
            lib.find("df_get_memory_pool_usage").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );

        // Two-phase snapshot protocol for active per-query DataFusion memory stats.
        //
        // Phase 1: i64 df_active_query_stats_size(out_size: *mut i64)
        // Writes the active triple count N (not a long count) into *out_size.
        // Phase 2: i64 df_active_query_stats_copy(out_ptr: *mut i64, out_cap: i64, out_len: *mut i64)
        // Writes up to out_cap / 3 (ctx, current, peak) triples into out_ptr in iteration
        // order; sets *out_len to the number of i64s actually written (always a multiple of 3,
        // always <= out_cap). Truncates silently when the registry grew between phases.
        ACTIVE_QUERY_STATS_SIZE = linker.downcallHandle(
            lib.find("df_active_query_stats_size").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,    // return status (0 = ok, < 0 = negated error-string ptr)
                ValueLayout.ADDRESS       // out_size (single-long out pointer, triple count)
            )
        );
        ACTIVE_QUERY_STATS_COPY = linker.downcallHandle(
            lib.find("df_active_query_stats_copy").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,    // return status (0 = ok, < 0 = negated error-string ptr)
                ValueLayout.ADDRESS,      // out_ptr (i64 buffer, capacity >= out_cap longs)
                ValueLayout.JAVA_LONG,    // out_cap (number of longs; non-negative multiple of 3)
                ValueLayout.ADDRESS       // out_len (single-long out pointer, in longs written)
            )
        );
        DF_ALLOCATE_TEST_BUFFER = linker.downcallHandle(
            lib.find("df_allocate_test_buffer").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );
        DF_FREE_TEST_BUFFER = linker.downcallHandle(
            lib.find("df_free_test_buffer").orElseThrow(),
            FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );
    }

    private NativeBridge() {}

    // ---- Tokio runtime management (no Arena needed — no string/buffer args) ----

    public static void initTokioRuntimeManager(int cpuThreads) {
        NativeCall.invokeVoid(INIT_RUNTIME_MANAGER, cpuThreads);
    }

    public static void shutdownTokioRuntimeManager() {
        NativeCall.invokeVoid(SHUTDOWN_RUNTIME_MANAGER);
    }

    // ---- DataFusion runtime (confined Arena for spillDir string only) ----

    /**
     * Creates a global DataFusion runtime. Returns an opaque native pointer ({@code long}).
     * This pointer is <b>not</b> a MemorySegment — it's a Rust heap address that lives
     * until {@link #closeGlobalRuntime} is called.
     */
    public static long createGlobalRuntime(long memoryLimit, long cacheManagerPtr, String spillDir, long spillLimit) {
        try (var call = new NativeCall()) {
            var dir = call.str(spillDir);
            return call.invoke(CREATE_GLOBAL_RUNTIME, memoryLimit, dir.segment(), dir.len(), spillLimit);
        }
    }

    /** Frees the native runtime. Safe to call once. */
    public static void closeGlobalRuntime(long ptr) {
        NativeCall.invokeVoid(CLOSE_GLOBAL_RUNTIME, ptr);
    }

    // ---- Reader management (confined Arena for path + file strings) ----

    /**
     * Creates a native reader. Returns an opaque native pointer.
     * Freed by {@link #closeDatafusionReader}.
     */
    public static long createDatafusionReader(String path, String[] files) {
        try (var call = new NativeCall()) {
            var p = call.str(path);
            var f = call.strArray(files);
            return call.invoke(CREATE_READER, p.segment(), p.len(), f.ptrs(), f.lens(), f.count());
        }
    }

    public static void closeDatafusionReader(long ptr) {
        NativeCall.invokeVoid(CLOSE_READER, ptr);
    }

    // ---- Query execution (confined Arena for tableName + plan bytes) ----

    public static void executeQueryAsync(
        long readerPtr,
        String tableName,
        byte[] substraitPlan,
        long runtimePtr,
        long contextId,
        ActionListener<Long> listener
    ) {
        try {
            NativeHandle.validatePointer(readerPtr, "reader");
            NativeHandle.validatePointer(runtimePtr, "runtime");
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        try (var call = new NativeCall()) {
            var table = call.str(tableName);
            long result = call.invoke(
                EXECUTE_QUERY,
                readerPtr,
                table.segment(),
                table.len(),
                call.bytes(substraitPlan),
                (long) substraitPlan.length,
                runtimePtr,
                contextId
            );
            listener.onResponse(result);
        } catch (Throwable t) {
            listener.onFailure(t instanceof Exception ? (Exception) t : new RuntimeException(t));
        }
    }

    // ---- Stream operations (no Arena needed — only long args) ----

    public static void streamGetSchema(long streamPtr, ActionListener<Long> listener) {
        try {
            NativeHandle.validatePointer(streamPtr, "stream");
            long result = NativeLibraryLoader.checkResult((long) STREAM_GET_SCHEMA.invokeExact(streamPtr));
            listener.onResponse(result);
        } catch (Throwable t) {
            listener.onFailure(t instanceof Exception ? (Exception) t : new RuntimeException(t));
        }
    }

    public static void streamNext(long runtimePtr, long streamPtr, ActionListener<Long> listener) {
        try {
            NativeHandle.validatePointer(streamPtr, "stream");
            long result = NativeLibraryLoader.checkResult((long) STREAM_NEXT.invokeExact(streamPtr));
            listener.onResponse(result);
        } catch (Throwable t) {
            listener.onFailure(t instanceof Exception ? (Exception) t : new RuntimeException(t));
        }
    }

    public static void streamClose(long streamPtr) {
        NativeCall.invokeVoid(STREAM_CLOSE, streamPtr);
    }

    // ---- Heap tracking ----

    /** Initializes the datafusion plugin's mimalloc heap. Call once at startup. */
    public static void initHeap() {
        try {
            INIT_HEAP.invokeExact();
        } catch (Throwable t) {
            throw new RuntimeException("df initHeap failed", t);
        }
    }

    /** Returns the DataFusion memory pool usage in bytes. */
    public static long getMemoryPoolUsage(long runtimePtr) {
        try {
            return (long) GET_MEMORY_POOL_USAGE.invokeExact(runtimePtr);
        } catch (Throwable t) {
            throw new RuntimeException("getMemoryPoolUsage failed", t);
        }
    }

    /**
     * Returns active per-query DataFusion memory stats as a flat {@code long[]} using the
     * two-phase size-then-copy FFM protocol.
     *
     * <p>Wire layout: {@code [ctx0, current0, peak0, ctx1, current1, peak1, ...]} — the
     * returned array length is always a multiple of 3. Returns an empty array ({@code long[0]})
     * when no queries are active.
     *
     * <h4>Protocol</h4>
     * <ol>
     *   <li>Open a single confined {@link NativeCall} (and its underlying {@code Arena}).</li>
     *   <li><b>Phase 1</b>: invoke {@code df_active_query_stats_size} and read {@code *out_size}
     *       as the active triple count {@code N}.</li>
     *   <li>Fast path: when {@code N == 0}, return {@code new long[0]} without making the
     *       Phase 2 call.</li>
     *   <li><b>Phase 2</b>: allocate a {@code 3 * N} long buffer in the same {@code Arena},
     *       invoke {@code df_active_query_stats_copy} with {@code out_cap = 3 * N}, and read
     *       {@code *out_len} as the authoritative long count {@code writtenLongs}.</li>
     *   <li>Copy exactly {@code writtenLongs} longs into a fresh JVM {@code long[]} before the
     *       {@code Arena} closes, so the returned array is Arena-lifetime-independent.</li>
     * </ol>
     *
     * <h4>Race semantics between Phase 1 and Phase 2</h4>
     * <p>The native {@code QUERY_REGISTRY} is not frozen between the two calls. Two legitimate
     * outcomes follow:
     * <ul>
     *   <li>{@code writtenLongs < 3 * N}: queries completed between Phase 1 and Phase 2, so
     *       Phase 2 produced fewer triples than Phase 1 counted. The returned array has length
     *       {@code writtenLongs}; it is <b>not</b> padded back to {@code 3 * N}.</li>
     *   <li>{@code writtenLongs == 3 * N}: either the registry was stable, or it grew and
     *       Phase 2 silently truncated the overflow. Newly-registered queries that did not fit
     *       will be observed on the next SBP cycle.</li>
     * </ul>
     *
     * <h4>Lifetime contract</h4>
     * <p>Uses the existing {@link NativeCall#buf(int)} and {@link NativeCall#longOut()} helpers;
     * the confined {@code Arena} owned by the single {@link NativeCall} is shared across both
     * phase calls inside one try-with-resources block. No new SPI helper is required.
     *
     * <h4>Safety cap</h4>
     * <p>The triple count reported by Phase 1 is bounded to {@code 8_000_000} triples
     * (~192 MB) before Phase 2 is attempted. The expected hardware ceiling is ~85 concurrent
     * queries; the cap exists only to prevent a runaway size probe from requesting a multi-GB
     * {@code Arena} allocation. Exceeding the cap throws {@link RuntimeException}.
     *
     * <h4>Errors</h4>
     * <p>Any {@link RuntimeException} raised by either phase (negative native status, invalid
     * length reported by Phase 2, allocation failures) is propagated unchanged. The caller
     * should treat such failures as "skip this cycle" rather than retrying.
     *
     * @return a non-null {@code long[]} whose length is a multiple of 3
     * @throws RuntimeException if either native phase fails or the safety cap is exceeded
     */
    public static long[] getActiveQueryStats() {
        try (var call = new NativeCall()) {
            // --- Phase 1: size probe ---
            var sizeOut = call.longOut();
            call.invoke(ACTIVE_QUERY_STATS_SIZE, sizeOut);
            long nTriples = sizeOut.get(ValueLayout.JAVA_LONG, 0);
            if (nTriples < 0) {
                throw new RuntimeException("df_active_query_stats_size: native reported negative triple count " + nTriples);
            }

            // Fast path: empty registry — skip Phase 2.
            if (nTriples == 0L) {
                return new long[0];
            }

            // --- Phase 2: copy ---
            // Cap the allocation to a sane upper bound so a runaway size probe never attempts
            // a multi-GB Arena allocation. 8M triples (24M longs, 192 MB) is well above the
            // documented "expected hardware" ceiling (~85 concurrent queries).
            final long maxTriples = 8_000_000L;
            if (nTriples > maxTriples) {
                throw new RuntimeException("df_active_query_stats_size: triple count " + nTriples + " exceeds safety cap " + maxTriples);
            }
            int capLongs = Math.toIntExact(nTriples * 3L);
            var buf = call.buf(capLongs * Long.BYTES);
            var lenOut = call.longOut();
            call.invoke(ACTIVE_QUERY_STATS_COPY, buf, (long) capLongs, lenOut);

            int writtenLongs = Math.toIntExact(lenOut.get(ValueLayout.JAVA_LONG, 0));
            if (writtenLongs < 0 || writtenLongs > capLongs || writtenLongs % 3 != 0) {
                throw new RuntimeException(
                    "df_active_query_stats_copy: native reported invalid length " + writtenLongs + " for cap " + capLongs
                );
            }

            // Copy native buffer into a JVM long[] BEFORE the Arena closes so the caller holds
            // an Arena-lifetime-independent array. writtenLongs may be < capLongs (queries
            // completed between Phase 1 and Phase 2) or == capLongs (registry stable, or it
            // grew and Phase 2 silently truncated — either way, we took what Phase 2 gave us).
            long[] out = new long[writtenLongs];
            for (int i = 0; i < writtenLongs; i++) {
                out[i] = buf.get(ValueLayout.JAVA_LONG, (long) i * Long.BYTES);
            }
            return out;
        }
    }

    /** Allocates a test buffer on datafusion's heap. Returns native pointer. */
    public static long allocateTestBuffer(long size) {
        try {
            return (long) DF_ALLOCATE_TEST_BUFFER.invokeExact(size);
        } catch (Throwable t) {
            throw new RuntimeException("df allocateTestBuffer failed", t);
        }
    }

    /** Frees a test buffer. Safe to call from any thread. */
    public static void freeTestBuffer(long ptr, long size) {
        try {
            DF_FREE_TEST_BUFFER.invokeExact(ptr, size);
        } catch (Throwable t) {
            throw new RuntimeException("df freeTestBuffer failed", t);
        }
    }

    // ---- Stubs ----

    public static byte[] sqlToSubstrait(long readerPtr, String tableName, String sql, long runtimePtr) {
        NativeHandle.validatePointer(readerPtr, "reader");
        NativeHandle.validatePointer(runtimePtr, "runtime");
        try (var call = new NativeCall()) {
            var table = call.str(tableName);
            var query = call.str(sql);
            var out = call.outBuffer(1024 * 1024);
            call.invoke(
                SQL_TO_SUBSTRAIT,
                readerPtr,
                table.segment(),
                table.len(),
                query.segment(),
                query.len(),
                runtimePtr,
                out.data(),
                (long) out.capacity(),
                out.lenOut()
            );
            return out.toByteArray();
        }
    }

    public static void cacheManagerAddFiles(long runtimePtr, String[] filePaths) {}

    public static void cacheManagerRemoveFiles(long runtimePtr, String[] filePaths) {}

    public static void initLogger() {}
}
