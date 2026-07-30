package org.mapdb.store;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The single owner of a store's (or, later, a DB's) deferred work (R6). One daemon thread
 * drives a small registry of {@link MaintenanceTask}s under bounded {@link MaintenanceBudget}s.
 *
 * <h2>P7: disabled = still correct</h2>
 * The executor is entirely optional. A store with {@link MaintenanceMode#DISABLED} owns no executor
 * and relies on synchronous fallbacks ({@code compactStep}/{@code compact}/{@code checkpoint}); with
 * maintenance enabled the SAME operations are merely also driven in the background. Killing or
 * skipping the executor never affects reads/writes/commit/reopen/verify — only file/log size and
 * latency.
 *
 * <h2>Locks</h2>
 * The executor holds NO store lock while selecting tasks, sleeping, backing off, or dispatching. A
 * task's {@link MaintenanceTask#run} enters its store only through public APIs that enforce the
 * existing lock order (segment then structural); no new lock order is introduced and
 * {@code DeadlockAsserts} is unchanged. Cross-store nesting (a WAL task calling its inner store) is
 * the same one-way nesting already used by commit.
 *
 * <h2>Shutdown</h2>
 * {@link #shutdown} flips to STOPPING, wakes the thread, and joins it: a currently-running bounded
 * task finishes (or reaches its budget) and then the loop exits — the executor never interrupts a
 * task mid-write. {@code onShutdown} then fires for each task. A store's {@code close()} shuts down
 * its owned executor BEFORE taking any store lock, so an in-flight task that holds a store lock can
 * finish and release it (no shutdown deadlock).
 */
public final class MaintenanceExecutor {

    /** Handle returned by {@link #register}; {@link #cancel} deregisters the task (no shutdown). */
    public interface Handle {
        void cancel();
    }

    private enum State { RUNNING, STOPPING, STOPPED }

    private final Thread thread;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition wake = lock.newCondition();
    private final CopyOnWriteArrayList<MaintenanceTask> tasks = new CopyOnWriteArrayList<>();
    private final long tickNanos;
    private final MaintenanceBudget budget;
    private volatile State state = State.RUNNING;
    private volatile Throwable lastTaskFailure;
    private volatile long taskFailureCount;
    private boolean pending = false; // a signal arrived; skip the next await
    private final AtomicBoolean shutdownCallbacksRun = new AtomicBoolean(); // onShutdown fires exactly once

    public MaintenanceExecutor(String name) {
        this(name, TimeUnit.MILLISECONDS.toNanos(50), MaintenanceBudget.defaultBudget());
    }

    public MaintenanceExecutor(String name, long tickNanos, MaintenanceBudget budget) {
        this.tickNanos = tickNanos;
        this.budget = budget;
        this.thread = new Thread(this::loop, "mapdb5-maint-" + name);
        this.thread.setDaemon(true);
        this.thread.start();
    }

    /** Register a task; the executor wakes to consider it immediately. Rejected once stopping/stopped. */
    public Handle register(MaintenanceTask task) {
        if (task == null) throw new NullPointerException("task");
        lock.lock();
        try {
            if (state != State.RUNNING) throw new IllegalStateException("executor is not running");
            tasks.add(task);
            pending = true;
            wake.signalAll();
        } finally {
            lock.unlock();
        }
        return () -> tasks.remove(task);
    }

    /** Wake the loop (e.g. after churn that likely created new maintenance work). */
    public void signal() {
        lock.lock();
        try {
            pending = true;
            wake.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public boolean isRunning() { return state == State.RUNNING; }

    /** Last exception a task threw out of {@code run()}, or null; the executor keeps running. */
    public Throwable lastTaskFailure() { return lastTaskFailure; }

    /** Number of task runs that ended in an exception (each also backed off by its task). */
    public long taskFailureCount() { return taskFailureCount; }

    private void loop() {
        try {
            while (state == State.RUNNING) {
                boolean moreSoon = false;
                long now = System.nanoTime();
                for (MaintenanceTask t : tasks) {
                    if (state != State.RUNNING) break;
                    boolean want;
                    try {
                        want = t.wantsRun(now);
                    } catch (Exception e) {
                        want = false; // P7: a misbehaving predicate must not kill the executor
                    }
                    if (!want) continue;
                    try {
                        MaintenanceResult r = t.run(budget);
                        if (r != null && r.moreSoon) moreSoon = true;
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        // P7: a failing task must not crash the executor or the store. Errors
                        // (VirtualMachineError, etc.) intentionally propagate and stop the thread.
                        lastTaskFailure = e; // surfaced, not just swallowed
                        taskFailureCount++;
                    }
                }
                lock.lock();
                try {
                    if (state != State.RUNNING) break;
                    if (!moreSoon && !pending) {
                        try {
                            wake.awaitNanos(tickNanos);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    pending = false;
                } finally {
                    lock.unlock();
                }
            }
        } finally {
            state = State.STOPPED;
        }
    }

    /**
     * Stop the executor: no new task runs start; the currently running bounded task finishes; the
     * thread is joined; then {@link MaintenanceTask#onShutdown}(graceful) fires for each task.
     */
    public void shutdown(boolean graceful) {
        lock.lock();
        try {
            if (state != State.STOPPED) state = State.STOPPING;
            pending = true;
            wake.signalAll();
        } finally {
            lock.unlock();
        }
        try {
            thread.join();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        // run onShutdown EXACTLY ONCE even under concurrent shutdown() calls
        if (!shutdownCallbacksRun.compareAndSet(false, true)) return;
        for (MaintenanceTask t : tasks) {
            try {
                t.onShutdown(graceful);
            } catch (Exception ignore) {
                // shutdown callbacks are best-effort
            }
        }
        tasks.clear();
    }

    // ---------- test hooks ----------

    boolean testThreadAlive() { return thread.isAlive(); }

    int testTaskCount() { return tasks.size(); }
}
