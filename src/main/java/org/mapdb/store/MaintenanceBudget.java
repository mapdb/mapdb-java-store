package org.mapdb.store;

import java.util.concurrent.TimeUnit;

/**
 * A small, bounded work allowance handed to {@link MaintenanceTask#run} (R6). Budgets keep each
 * background tick short so the store stays available between ticks and foreground latency is not
 * held hostage by a long maintenance pass. A task must honor whatever fields apply to it and stop
 * at the first limit reached.
 */
public final class MaintenanceBudget {

    /** Max whole pages a page-oriented task (e.g. compaction) may touch this tick. */
    public final int maxPages;
    /** Max records a record-oriented task may touch this tick (0 = unbounded/not applicable). */
    public final int maxRecords;
    /** Max bytes a byte-oriented task may move this tick (0 = unbounded/not applicable). */
    public final int maxBytes;
    /** Soft wall-clock ceiling for the tick in nanos (0 = none). */
    public final long maxNanos;
    /** Whether the task may fsync during this tick. */
    public final boolean mayFsync;

    public MaintenanceBudget(int maxPages, int maxRecords, int maxBytes, long maxNanos, boolean mayFsync) {
        this.maxPages = maxPages;
        this.maxRecords = maxRecords;
        this.maxBytes = maxBytes;
        this.maxNanos = maxNanos;
        this.mayFsync = mayFsync;
    }

    /** Conservative default: {@link StoreDirect#DEFAULT_COMPACT_STEP_PAGES} pages, ~20 ms, fsync allowed. */
    public static MaintenanceBudget defaultBudget() {
        return new MaintenanceBudget(StoreDirect.DEFAULT_COMPACT_STEP_PAGES, 0, 0,
                TimeUnit.MILLISECONDS.toNanos(20), true);
    }
}
