package org.mapdb.store;

import org.mapdb.DBException;

import java.util.concurrent.TimeUnit;

/**
 * The WAL cleaner as an R6 maintenance task: each tick asks {@link StoreWAL} for one budgeted
 * slice of cleaning, and the store decides whether any is due.
 *
 * <p>This replaced {@code WalCheckpointTask}, which was a low-frequency <em>policy owner</em>
 * standing in front of a whole-store rewrite — it could only choose the moment at which the store
 * paused, never shorten the pause. The incremental cleaner made the work itself divisible, so the task now does
 * the same thing every other maintenance task does: honor {@link MaintenanceBudget} and report what
 * it moved. {@code maxBytes}, {@code maxRecords}, {@code maxNanos} and {@code mayFsync} all mean
 * something here for the first time, and {@link MaintenanceResult#bytesDone} carries real bytes
 * rather than the zero the old task reported.
 *
 * <p>P7-safe in the sense {@link MaintenanceTask} requires: a tick can be skipped or dropped at any
 * point and the store stays correct, because a cycle only becomes visible when its {@code 'K'} is
 * forced. Commit's inline clean remains the synchronous fallback, so a store that never runs this
 * task is still bounded.
 */
final class WalCleanerTask implements MaintenanceTask {

    /** Backoff after a tick that found nothing to do. */
    private static final long IDLE_PERIOD = TimeUnit.SECONDS.toNanos(1);

    private final StoreWAL store;
    private volatile long nextEligibleNanos = 0;

    WalCleanerTask(StoreWAL store) { this.store = store; }

    @Override public String name() { return "wal-cleaner"; }

    @Override public MaintenanceClass kind() { return MaintenanceClass.CHECKPOINT; }

    @Override public boolean wantsRun(long nowNanos) {
        return !store.isClosed() && nowNanos >= nextEligibleNanos;
    }

    @Override public MaintenanceResult run(MaintenanceBudget budget) {
        long written;
        try {
            written = store.maintenanceCleanStep(budget);
        } catch (DBException.StoreClosed e) {
            nextEligibleNanos = System.nanoTime() + IDLE_PERIOD;
            return MaintenanceResult.noProgress(IDLE_PERIOD);
        }
        if (written < 0) {
            // Nothing due, or deferred behind a delta transaction's LSN reservation (C15). Back
            // off; the next commit that crosses the trigger will make this worth running again.
            nextEligibleNanos = System.nanoTime() + IDLE_PERIOD;
            return MaintenanceResult.noProgress(IDLE_PERIOD);
        }
        // Zero bytes with a cycle still open is progress: the tick spent its budget on W10's
        // verification scan, which reads and writes nothing but must finish before the mark.
        nextEligibleNanos = 0;
        return MaintenanceResult.progressBytes(written);
    }

    @Override public void onShutdown(boolean graceful) { /* nothing to release */ }
}
