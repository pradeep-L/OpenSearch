/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.trackers;

import org.opensearch.common.logging.DeprecationLogger;

/**
 * Defines the type of TaskResourceUsageTracker.
 */
public enum TaskResourceUsageTrackerType {
    CPU_USAGE_TRACKER("cpu_usage_tracker"),
    HEAP_USAGE_TRACKER("heap_usage_tracker"),
    ELAPSED_TIME_TRACKER("elapsed_time_tracker"),
    NATIVE_MEMORY_USAGE_TRACKER("native_memory_usage_tracker");

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(TaskResourceUsageTrackerType.class);

    private final String name;

    TaskResourceUsageTrackerType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static TaskResourceUsageTrackerType fromName(String name) {
        switch (name) {
            case "cpu_usage_tracker":
                return CPU_USAGE_TRACKER;
            case "heap_usage_tracker":
                return HEAP_USAGE_TRACKER;
            case "elapsed_time_tracker":
                return ELAPSED_TIME_TRACKER;
            case "native_memory_usage_tracker":
                return NATIVE_MEMORY_USAGE_TRACKER;
            default:
                // Emit a deprecation-log entry so that future renames / removals of tracker names
                // have an auditable path (per R8.6 of the SBP native-memory tracker spec).
                deprecationLogger.deprecate(
                    "task_resource_usage_tracker_type_unknown_name",
                    "Unknown TaskResourceUsageTrackerType name [" + name + "]; this may indicate a removed or renamed tracker type."
                );
                throw new IllegalArgumentException("Invalid TaskResourceUsageTrackerType: " + name);
        }
    }
}
