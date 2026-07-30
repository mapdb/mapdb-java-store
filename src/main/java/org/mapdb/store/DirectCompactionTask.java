package org.mapdb.store;

import org.mapdb.DBException;

import java.util.concurrent.TimeUnit;

/**
 * Drives {@link StoreDirect#compactStep} in bounded batches under a {@link MaintenanceExecutor} (R6).
 * Each tick reclaims up to {@code budget.maxPages} trailing pages (default
 * {@link StoreDirect#DEFAULT_COMPACT_STEP_PAGES}); consecutive zero-progress ticks back off
 * exponentially until a mutator {@link #signalChurn signals} new fragmentation. P7-safe: the store
 * remains fully correct whether this task runs, is skipped, or is killed mid-step (the underlying
 * step is crash-safe and bounded).
 */
final class DirectCompactionTask implements MaintenanceTask {

    private static final long BASE_BACKOFF = TimeUnit.MILLISECONDS.toNanos(20);
    private static final long MAX_BACKOFF = TimeUnit.SECONDS.toNanos(2);

    private final StoreDirect store;
    private volatile long nextEligibleNanos = 0;
    /**
     * Backoff from a FAILED step, kept separate so {@link #signalChurn} (fired on every delete)
     * cannot erase it — otherwise delete churn retries a faulting step at tick rate again.
     * Only a successfully completed step with progress clears it early; otherwise it expires.
     */
    private volatile long failureBackoffUntilNanos = 0;
    private int zeroStreak = 0;

    DirectCompactionTask(StoreDirect store) { this.store = store; }

    @Override public String name() { return "direct-compaction"; }

    @Override public MaintenanceClass kind() { return MaintenanceClass.COMPACT; }

    @Override public boolean wantsRun(long nowNanos) {
        return !store.isClosed()
                && nowNanos >= Math.max(nextEligibleNanos, failureBackoffUntilNanos);
    }

    @Override public MaintenanceResult run(MaintenanceBudget budget) {
        int n;
        try {
            n = store.compactStep(Math.max(1, budget.maxPages));
        } catch (DBException.StoreClosed e) {
            return MaintenanceResult.noProgress(MAX_BACKOFF);
        } catch (RuntimeException e) {
            // A faulting step (e.g. DataCorruption) must back off like a zero-progress tick;
            // without this bookkeeping the executor would silently retry it every tick forever.
            zeroStreak = 6;
            long until = System.nanoTime() + MAX_BACKOFF;
            nextEligibleNanos = until;
            failureBackoffUntilNanos = until; // churn-proof: see field doc
            throw e; // the executor records it as the task's last failure
        }
        long now = System.nanoTime();
        if (n > 0) {
            zeroStreak = 0;
            nextEligibleNanos = now;
            failureBackoffUntilNanos = 0; // real progress: whatever faulted before is behind us
            return MaintenanceResult.progress(n);
        }
        zeroStreak = Math.min(zeroStreak + 1, 6);
        long backoff = Math.min(MAX_BACKOFF, BASE_BACKOFF << zeroStreak);
        nextEligibleNanos = now + backoff;
        return MaintenanceResult.noProgress(backoff);
    }

    @Override public void onShutdown(boolean graceful) { /* nothing to release */ }

    /**
     * A mutator hint that fresh fragmentation exists: clear the zero-progress backoff so the
     * next tick runs. Deliberately does NOT touch {@link #failureBackoffUntilNanos} — churn
     * says there is new work, not that a faulting step stopped faulting.
     */
    void signalChurn() {
        zeroStreak = 0;
        nextEligibleNanos = 0;
    }
}
