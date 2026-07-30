package org.mapdb.btree;

import org.junit.Test;
import org.mapdb.DBException;
import org.mapdb.ser.LongFormat;
import org.mapdb.ser.Serializer;
import org.mapdb.store.RecordRead;
import org.mapdb.store.Store;
import org.mapdb.store.StoreOnHeap;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Lock-protocol hardening tests. Deadlock freedom rests on a writer only ever acquiring a node
 * lock while holding none, so it never blocks while holding; these tests pin that discipline.
 * Run with {@code -ea} (the surefire default here), so the zero-held node-lock checker in
 * {@link BTreeMap#lockNode} is active: these are the cases that would catch an overlap-discipline
 * bug (striped-table aliasing) or an exception-path lock leak. Three concerns:
 * <ul>
 *   <li>{@link #contendedStressNoDeadlock} — many writers on interleaved keys force frequent
 *       leaf+dir splits and several root grows; asserts completion (a deadlock manifests as the
 *       watchdog timeout), final contents vs a per-thread oracle, and an empty lock table;</li>
 *   <li>{@link #plainUpdateFaultDoesNotLeakLock}/{@link #splitFaultDoesNotLeakLock} — a store
 *       throw with the leaf lock held must release it, so a retry on the SAME (non-reentrant)
 *       thread completes instead of self-deadlocking;</li>
 *   <li>{@link #propagationFaultPoisonsMap} — a propagation failure AFTER the split published
 *       poisons the map so later ops fail fast rather than parking forever in {@code leftEdge}.</li>
 * </ul>
 */
public class BTreeMapLockProtocolTest {

    // ---- reflective peek at the private node-lock table (assert it drains) ----
    private static int lockCount(BTreeMap<?, ?> map) {
        try {
            Field f = BTreeMap.class.getDeclaredField("nodeLocks");
            f.setAccessible(true);
            return ((Map<?, ?>) f.get(map)).size();
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    // =================================================================================
    // (a) contended stress — deadlock manifests as the watchdog timeout
    // =================================================================================
    @Test
    public void contendedStressNoDeadlock() throws Exception {
        final int nThreads = 8;
        final int keysPerThread = 256;      // 8*256 = 2048 keys
        final int opsPerThread = 60_000;    // heavy churn
        final int maxNodeSize = 4;          // small -> frequent splits + several root grows
        final long seed = 0xB7EEL;

        Store store = new StoreOnHeap();
        // interleaved keys: thread t owns residue class t (mod nThreads), so adjacent keys share
        // a leaf and threads contend on the SAME node locks and split it concurrently — while each
        // key stays private to one thread, keeping the final state a deterministic per-thread oracle.
        final BTreeMap<Long, Long> map =
                BTreeMap.create(store, LongFormat.INSTANCE, LongFormat.INSTANCE, maxNodeSize);

        ExecutorService exec = Executors.newFixedThreadPool(nThreads);
        try {
            java.util.List<Callable<Map<Long, Long>>> tasks = new java.util.ArrayList<>();
            for (int t = 0; t < nThreads; t++) {
                final int tid = t;
                tasks.add(() -> {
                    Random rnd = new Random(seed + tid);
                    HashMap<Long, Long> expected = new HashMap<>();
                    for (int i = 0; i < opsPerThread; i++) {
                        long k = tid + (long) nThreads * rnd.nextInt(keysPerThread);
                        switch (rnd.nextInt(3)) {
                            case 0 -> { long v = rnd.nextLong(); map.put(k, v); expected.put(k, v); }
                            case 1 -> { map.remove(k); expected.remove(k); }
                            default -> {                     // replace: no-op when absent
                                long v = rnd.nextLong();
                                Long prev = map.replace(k, v);
                                boolean present = expected.containsKey(k);
                                assertEquals("replace() return disagrees with oracle for key " + k,
                                        present ? expected.get(k) : null, prev);
                                if (present) expected.put(k, v);
                            }
                        }
                    }
                    return expected;
                });
            }
            List<Future<Map<Long, Long>>> futures = exec.invokeAll(tasks, 60, TimeUnit.SECONDS);

            HashMap<Long, Long> oracle = new HashMap<>();
            for (Future<Map<Long, Long>> f : futures) {
                if (f.isCancelled()) fail("a worker did not finish within 60s — probable deadlock");
                oracle.putAll(f.get()); // ExecutionException (e.g. a lock-protocol AssertionError) fails here
            }

            // final state is fully quiescent: every worker joined, so reads are consistent
            assertEquals("map size vs oracle", oracle.size(), map.sizeLong());
            for (Map.Entry<Long, Long> e : oracle.entrySet())
                assertEquals("value for key " + e.getKey(), e.getValue(), map.get(e.getKey()));
            // no stray keys in the map beyond the oracle
            long seen = 0;
            for (var it = map.entryIterator(); it.hasNext(); ) {
                Map.Entry<Long, Long> e = it.next();
                assertTrue("map has key absent from oracle: " + e.getKey(), oracle.containsKey(e.getKey()));
                seen++;
            }
            assertEquals("map iteration count vs oracle", oracle.size(), seen);

            assertEquals("node-lock table must be empty after a clean run", 0, lockCount(map));
        } finally {
            exec.shutdownNow();
        }
    }

    // =================================================================================
    // (b) failure-injection lock-leak: a store throw under the leaf lock must release it,
    //     so a retry on the SAME (non-reentrant) thread completes instead of self-deadlocking.
    // =================================================================================
    @Test
    public void plainUpdateFaultDoesNotLeakLock() {
        FaultStore fs = new FaultStore(new StoreOnHeap());
        BTreeMap<Long, Long> map = BTreeMap.create(fs, LongFormat.INSTANCE, LongFormat.INSTANCE, 8);
        map.put(1L, 100L);

        fs.failUpdateAfter(0); // the value-overwrite store.update (held under the leaf lock) throws
        try {
            map.put(1L, 200L);
            fail("expected the injected update fault to surface");
        } catch (DBException.DataCorruption expected) { /* injected */ }
        assertEquals("leaf lock leaked on the faulted update path", 0, lockCount(map));

        // retry on the SAME thread: pre-fix this self-deadlocks on the non-reentrant recid
        // (or trips the reentrant-lock assert under -ea); post-fix it just completes.
        assertEquals(Long.valueOf(100L), map.put(1L, 200L));
        assertEquals(Long.valueOf(200L), map.get(1L));
        assertEquals(0, lockCount(map));
    }

    @Test
    public void splitFaultDoesNotLeakLock() {
        FaultStore fs = new FaultStore(new StoreOnHeap());
        BTreeMap<Long, Long> map = BTreeMap.create(fs, LongFormat.INSTANCE, LongFormat.INSTANCE, 4);
        for (long k = 1; k <= 4; k++) map.put(k, k * 10); // fill the root leaf (no split yet)

        fs.failPutAfter(0); // store.put(B) — the FIRST put of the split, held under the leaf lock
        try {
            map.put(5L, 50L);
            fail("expected the injected split fault to surface");
        } catch (DBException.DataCorruption expected) { /* injected, pre-publication */ }
        assertEquals("leaf lock leaked on the faulted split path", 0, lockCount(map));

        // pre-publication failure does NOT poison (nothing was published), so the retry succeeds
        map.put(5L, 50L);
        assertEquals(Long.valueOf(50L), map.get(5L));
        for (long k = 1; k <= 4; k++) assertEquals(Long.valueOf(k * 10), map.get(k));
        assertEquals(0, lockCount(map));
    }

    // =================================================================================
    // (c) poison: a propagation failure AFTER the split published must poison the map so later
    //     ops fail fast (checkPoison / leftEdge) instead of parking forever.
    // =================================================================================
    @Test
    public void propagationFaultPoisonsMap() {
        FaultStore fs = new FaultStore(new StoreOnHeap());
        BTreeMap<Long, Long> map = BTreeMap.create(fs, LongFormat.INSTANCE, LongFormat.INSTANCE, 4);
        for (long k = 1; k <= 4; k++) map.put(k, k * 10); // fill the root leaf

        // during the splitting put: put#0 = store.put(B) OK; A is then published via store.update;
        // put#1 = store.put(newRoot) in the root grow throws — a failure AFTER publication.
        fs.failPutAfter(1);
        try {
            map.put(5L, 50L);
            fail("expected the injected root-grow fault to surface");
        } catch (DBException.DataCorruption expected) { /* injected */ }

        // the map is now poisoned: every op entry fails fast rather than hanging
        try {
            map.get(1L);
            fail("expected a poisoned map to reject reads");
        } catch (DBException.DataCorruption poison) {
            assertTrue("poison message: " + poison.getMessage(),
                    poison.getMessage().contains("poisoned"));
        }
        try {
            map.put(6L, 60L);
            fail("expected a poisoned map to reject writes");
        } catch (DBException.DataCorruption poison) {
            assertTrue(poison.getMessage().contains("poisoned"));
        }
        assertEquals("no node lock may leak even when the map poisons", 0, lockCount(map));
    }

    // ---------------------------------------------------------------------------------
    // Fault-injecting Store wrapper: throws once from the (armed) Nth put/update, otherwise
    // fully transparent. isThreadSafe() delegates so the node locks stay active.
    // ---------------------------------------------------------------------------------
    private static final class FaultStore implements Store {
        private final Store delegate;
        private final AtomicInteger putCountdown = new AtomicInteger(-1);    // -1 = disarmed
        private final AtomicInteger updateCountdown = new AtomicInteger(-1);

        FaultStore(Store delegate) { this.delegate = delegate; }

        /** After {@code n} successful puts, the next put throws (one-shot). */
        void failPutAfter(int n) { putCountdown.set(n); }
        /** After {@code n} successful updates, the next update throws (one-shot). */
        void failUpdateAfter(int n) { updateCountdown.set(n); }

        private static void maybeFail(AtomicInteger c, String what) {
            int v = c.get();
            if (v < 0) return;
            if (c.getAndDecrement() == 0)
                throw new DBException.DataCorruption("injected " + what + " fault");
        }

        @Override public long preallocate() { return delegate.preallocate(); }
        @Override public void preallocate(int count, long[] into) { delegate.preallocate(count, into); }

        @Override public <R> long put(R record, Serializer<R> serializer) {
            maybeFail(putCountdown, "put");
            return delegate.put(record, serializer);
        }

        @Override public <R> R get(long recid, Serializer<R> serializer) {
            return delegate.get(recid, serializer);
        }

        @Override public long read(long recid, RecordRead action) { return delegate.read(recid, action); }

        @Override public <R> void update(long recid, R record, Serializer<R> serializer) {
            maybeFail(updateCountdown, "update");
            delegate.update(recid, record, serializer);
        }

        @Override public <R> boolean compareAndSwap(long recid, R expected, R next, Serializer<R> serializer) {
            return delegate.compareAndSwap(recid, expected, next, serializer);
        }

        @Override public <R> void delete(long recid, Serializer<R> serializer) { delegate.delete(recid, serializer); }
        @Override public void commit() { delegate.commit(); }
        @Override public void compact() { delegate.compact(); }
        @Override public void close() { delegate.close(); }
        @Override public boolean isClosed() { return delegate.isClosed(); }
        @Override public void verify() { delegate.verify(); }
        @Override public PrimitiveIterator.OfLong getAllRecids() { return delegate.getAllRecids(); }
        @Override public boolean isThreadSafe() { return delegate.isThreadSafe(); }
        @Override public boolean isReadOnly() { return delegate.isReadOnly(); }
        @Override public long getCurrentSize() { return delegate.getCurrentSize(); }
    }
}
