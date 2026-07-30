package org.mapdb.store;

/**
 * Whether a store runs a background {@link MaintenanceExecutor} (R6). {@code DISABLED} is the
 * default and keeps everything correct via synchronous fallbacks ("disabled = still
 * correct"): compaction/checkpoint remain callable inline; correctness never depends on the
 * background thread — only file/log size and latency do.
 */
public enum MaintenanceMode {
    DISABLED,
    ENABLED
}
