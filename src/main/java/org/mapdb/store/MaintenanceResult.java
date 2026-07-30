package org.mapdb.store;

/** Outcome of one {@link MaintenanceTask#run} tick (R6). */
public final class MaintenanceResult {

    public final int pagesDone;
    public final long bytesDone;
    public final boolean madeProgress;
    /** Hint that useful work likely remains and the executor should reschedule promptly. */
    public final boolean moreSoon;
    /** Suggested delay before this task is worth running again (nanos; advisory). */
    public final long retryAfterNanos;

    private MaintenanceResult(int pagesDone, long bytesDone, boolean madeProgress,
                             boolean moreSoon, long retryAfterNanos) {
        this.pagesDone = pagesDone;
        this.bytesDone = bytesDone;
        this.madeProgress = madeProgress;
        this.moreSoon = moreSoon;
        this.retryAfterNanos = retryAfterNanos;
    }

    /** Made progress (touched {@code pagesDone} pages) and probably has more to do. */
    public static MaintenanceResult progress(int pagesDone) {
        return new MaintenanceResult(pagesDone, 0, true, true, 0);
    }

    /** Made progress moving {@code bytesDone} bytes and probably has more to do. */
    public static MaintenanceResult progressBytes(long bytesDone) {
        return new MaintenanceResult(0, bytesDone, true, true, 0);
    }

    /** No progress this tick; back off for at least {@code retryAfterNanos}. */
    public static MaintenanceResult noProgress(long retryAfterNanos) {
        return new MaintenanceResult(0, 0, false, false, retryAfterNanos);
    }
}
