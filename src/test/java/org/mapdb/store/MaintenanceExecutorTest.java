package org.mapdb.store;

import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * R6 maintenance framework: the single daemon executor, P7 "disabled = still correct", the
 * StoreDirect compaction task driving {@code compactStep} to completion, and clean shutdown.
 */
public class MaintenanceExecutorTest {

    private final List<File> files = new ArrayList<>();

    private File newFile() throws IOException {
        File f = Files.createTempFile("mapdb-maint", ".db").toFile();
        f.delete();
        files.add(f);
        return f;
    }

    @After public void cleanup() {
        for (File f : files) f.delete();
        files.clear();
    }

    private static boolean anyMaintThreadAlive() {
        for (Thread t : Thread.getAllStackTraces().keySet())
            if (t.isAlive() && t.getName().startsWith("mapdb-maint-")) return true;
        return false;
    }

    // ---------- the executor drives a registered task and shuts down cleanly ----------

    @Test public void executor_runs_task_and_shuts_down() throws Exception {
        MaintenanceExecutor ex = new MaintenanceExecutor("unit",
                java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(5), MaintenanceBudget.defaultBudget());
        AtomicInteger runs = new AtomicInteger();
        AtomicInteger shutdowns = new AtomicInteger();
        MaintenanceTask task = new MaintenanceTask() {
            @Override public String name() { return "unit-task"; }
            @Override public MaintenanceClass kind() { return MaintenanceClass.RECLAIM; }
            @Override public boolean wantsRun(long nowNanos) { return true; }
            @Override public MaintenanceResult run(MaintenanceBudget b) {
                return runs.incrementAndGet() < 3
                        ? MaintenanceResult.progress(1)
                        : MaintenanceResult.noProgress(java.util.concurrent.TimeUnit.SECONDS.toNanos(1));
            }
            @Override public void onShutdown(boolean graceful) { shutdowns.incrementAndGet(); }
        };
        ex.register(task);
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
        while (runs.get() < 3 && System.nanoTime() < deadline) Thread.sleep(5);
        assertTrue("task ran at least 3 times", runs.get() >= 3);
        assertTrue(ex.isRunning());

        ex.shutdown(true);
        assertFalse("thread joined after shutdown", ex.testThreadAlive());
        assertEquals("onShutdown fired once", 1, shutdowns.get());
    }

    // ---------- a failing task must not crash the executor (P7) ----------

    @Test public void failing_task_does_not_crash_executor() throws Exception {
        MaintenanceExecutor ex = new MaintenanceExecutor("unit-fail",
                java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(5), MaintenanceBudget.defaultBudget());
        AtomicInteger good = new AtomicInteger();
        ex.register(new MaintenanceTask() {
            @Override public String name() { return "boom"; }
            @Override public MaintenanceClass kind() { return MaintenanceClass.RECLAIM; }
            @Override public boolean wantsRun(long n) { return true; }
            @Override public MaintenanceResult run(MaintenanceBudget b) { throw new RuntimeException("boom"); }
            @Override public void onShutdown(boolean g) { }
        });
        ex.register(new MaintenanceTask() {
            @Override public String name() { return "good"; }
            @Override public MaintenanceClass kind() { return MaintenanceClass.RECLAIM; }
            @Override public boolean wantsRun(long n) { return true; }
            @Override public MaintenanceResult run(MaintenanceBudget b) { good.incrementAndGet(); return MaintenanceResult.noProgress(0); }
            @Override public void onShutdown(boolean g) { }
        });
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(3);
        while (good.get() < 3 && System.nanoTime() < deadline) Thread.sleep(5);
        assertTrue("executor kept running past a throwing task", good.get() >= 3);
        assertTrue(ex.isRunning());
        assertTrue("task failure recorded, not just swallowed", ex.taskFailureCount() > 0);
        assertEquals("boom", ex.lastTaskFailure().getMessage());
        ex.shutdown(false);
        assertFalse(ex.testThreadAlive());
    }

    // ---------- disabled maintenance is still fully correct (P7) ----------

    @Test public void disabled_maintenance_is_correct() throws IOException {
        File f = newFile();
        StoreDirect s = new StoreDirect(f); // default: maintenance DISABLED, no background thread
        try {
            long keeper = s.put(Fixtures.payload(1, 1, 1000), Fixtures.RAW);
            List<Long> doomed = new ArrayList<>();
            for (int i = 0; i < 30; i++) doomed.add(s.put(Fixtures.payload(100 + i, 1, 120_000), Fixtures.RAW));
            long fullTail = s.testFileTail();
            for (long r : doomed) s.delete(r, Fixtures.RAW);

            // no background thread exists; synchronous compaction still reclaims
            long reclaimed = s.compactIncremental();
            s.verify();
            assertTrue("synchronous compaction reclaims with maintenance disabled", reclaimed > 0);
            assertTrue(s.testFileTail() < fullTail);
            assertArrayEquals(Fixtures.payload(1, 1, 1000), s.get(keeper, Fixtures.RAW));
        } finally {
            s.close();
        }
        StoreDirect re = new StoreDirect(f);
        try { re.verify(); } finally { re.close(); }
    }

    // ---------- the compaction task drives compactStep to completion in the background ----------

    @Test public void executor_drives_compaction_to_completion() throws Exception {
        File f = newFile();
        StoreDirect s = new StoreDirect(f);
        try {
            s.startMaintenance(); // owned executor + DirectCompactionTask
            long keeper = s.put(Fixtures.payload(1, 1, 1000), Fixtures.RAW);
            List<Long> doomed = new ArrayList<>();
            for (int i = 0; i < 50; i++) doomed.add(s.put(Fixtures.payload(200 + i, 1, 150_000), Fixtures.RAW));
            long fullTail = s.testFileTail();
            for (long r : doomed) s.delete(r, Fixtures.RAW); // deletes signal the compaction task

            // wait for the background compactor to shrink the file
            long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
            long tail = fullTail;
            while (System.nanoTime() < deadline) {
                tail = s.testFileTail();
                if (tail < fullTail / 2) break;
                Thread.sleep(20);
            }
            assertTrue("background compaction shrank the file (" + tail + " < " + fullTail + ")", tail < fullTail);
            s.verify();
            assertArrayEquals(Fixtures.payload(1, 1, 1000), s.get(keeper, Fixtures.RAW));
        } finally {
            s.close(); // stops the executor first, then closes
        }
        assertFalse("no maintenance thread survives close", anyMaintThreadAlive());
        StoreDirect re = new StoreDirect(f);
        try { re.verify(); } finally { re.close(); }
    }

    // ---------- clean shutdown: close() stops the executor and joins its thread ----------

    @Test public void clean_shutdown_joins_executor_thread() throws Exception {
        File f = newFile();
        StoreDirect s = new StoreDirect(f);
        s.startMaintenance();
        for (int i = 0; i < 20; i++) s.put(Fixtures.payload(i, 1, 40_000), Fixtures.RAW);
        assertTrue(anyMaintThreadAlive());
        s.close();
        // give the daemon a beat to fully die if it were leaking; it must already be gone
        Thread.sleep(50);
        assertFalse("executor thread must be joined by close()", anyMaintThreadAlive());
        assertTrue(s.isClosed());
        StoreDirect re = new StoreDirect(f);
        try { re.verify(); } finally { re.close(); }
    }

    // ---------- a failed step's backoff must survive delete churn ----------

    /** StoreDirect whose compactStep throws an injectable fault before touching the store. */
    private static final class FaultableStore extends StoreDirect {
        volatile RuntimeException fault;
        FaultableStore(File f) { super(f); }
        @Override public int compactStep(int maxPages) {
            RuntimeException e = fault;
            if (e != null) throw e;
            return super.compactStep(maxPages);
        }
    }

    @Test public void failure_backoff_survives_churn_and_clears_on_progress() throws IOException {
        File f = newFile();
        FaultableStore s = new FaultableStore(f);
        try {
            // trailing reclaimable pages so a later non-faulting step makes real progress
            List<Long> doomed = new ArrayList<>();
            for (int i = 0; i < 30; i++) doomed.add(s.put(Fixtures.payload(i, 1, 120_000), Fixtures.RAW));
            for (long r : doomed) s.delete(r, Fixtures.RAW);

            DirectCompactionTask t = new DirectCompactionTask(s);
            assertTrue(t.wantsRun(System.nanoTime()));

            // (a) a faulting step backs the task off
            s.fault = new RuntimeException("step fault");
            try {
                t.run(MaintenanceBudget.defaultBudget());
                fail("expected the injected step fault");
            } catch (RuntimeException expected) {
                assertEquals("step fault", expected.getMessage());
            }
            assertFalse("failed step must back off", t.wantsRun(System.nanoTime()));

            // (b) delete churn must NOT erase the failure backoff (pre-fix: signalChurn
            // zeroed nextEligibleNanos, so a faulting step retried at tick rate again)
            t.signalChurn();
            assertFalse("churn must not erase the failure backoff", t.wantsRun(System.nanoTime()));

            // (c1) the backoff is bounded: after the ~2s window the task is eligible again
            assertTrue("failure backoff must expire",
                    t.wantsRun(System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(3)));

            // (c2) a successful step with progress clears the failure backoff immediately
            s.fault = null;
            MaintenanceResult r = t.run(MaintenanceBudget.defaultBudget());
            assertTrue("recovered step reclaims pages", r.madeProgress && r.pagesDone > 0);
            assertTrue("progress clears the failure backoff", t.wantsRun(System.nanoTime()));
        } finally {
            s.close();
        }
    }

    // ---------- StoreWAL owns a coarse checkpoint executor and shuts it down on close ----------

    @Test public void wal_maintenance_starts_and_shuts_down() throws Exception {
        File f = newFile();
        StoreWAL s = new StoreWAL(f);
        try {
            s.startMaintenance();
            assertTrue(anyMaintThreadAlive());
            for (int i = 0; i < 10; i++) {
                s.put(Fixtures.payload(i, 1, 500), Fixtures.RAW);
            }
            s.commit();
            s.verify();
        } finally {
            s.close();
        }
        Thread.sleep(50);
        assertFalse("WAL maintenance thread joined on close", anyMaintThreadAlive());
        StoreWAL re = new StoreWAL(f);
        try { re.verify(); } finally { re.close(); }
    }
}
