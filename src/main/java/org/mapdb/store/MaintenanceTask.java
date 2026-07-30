package org.mapdb.store;

/**
 * A unit of deferred, droppable work registered with a {@link MaintenanceExecutor} (R6). Every task
 * must be P7-safe: skipping or killing it mid-run leaves the store correct — only
 * unperformed maintenance results. A task's {@link #run} must enter its store only through public
 * store APIs that already enforce the lock order (segment then structural); the executor holds no
 * store lock while selecting or sleeping.
 */
public interface MaintenanceTask {

    String name();

    MaintenanceClass kind();

    /** Cheap, lock-free predicate: is this task worth running at {@code nowNanos}? */
    boolean wantsRun(long nowNanos);

    /** Do a bounded slice of work within {@code budget}. Must be safe to abandon at any time. */
    MaintenanceResult run(MaintenanceBudget budget) throws Exception;

    /** Called once when the executor stops. {@code graceful} = clean shutdown vs abrupt drop. */
    void onShutdown(boolean graceful);
}
