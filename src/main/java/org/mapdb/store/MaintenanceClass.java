package org.mapdb.store;

/** The kind of deferred work a {@link MaintenanceTask} performs (R6 maintenance framework). */
public enum MaintenanceClass {
    /** Page consolidation / incremental compaction (e.g. StoreDirect {@code compactStep}). */
    COMPACT,
    /** Hybrid-cache demotion (materialized -> serialized); owned by the cache layer (R5, not yet present). */
    DEMOTE,
    /** WAL checkpoint / log truncation. */
    CHECKPOINT,
    /** Free-space / recid reclamation. */
    RECLAIM
}
