package org.mapdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.mapdb.btree.BTreeMap;
import org.mapdb.db.Atomic;
import org.mapdb.htree.HTreeMap;
import org.mapdb.ser.LongFormat;
import org.mapdb.ser.Serializers;
import org.mapdb.ser.StringGroupFormat;
import org.mapdb.store.Store;
import org.mapdb.store.StoreOnHeap;

public class BindTest {

    @Test public void secondaryIndexesHistogramAndDeleteSink() {
        Store store = new StoreOnHeap(true);
        HTreeMap<Long, String> primary = HTreeMap.create(
                store, Serializers.LONG, Serializers.STRING, 0, 4, 8);
        primary.put(1L, "red,round");

        Map<Long, Integer> lengths = new ConcurrentHashMap<>();
        Bind.secondaryValue(primary, lengths, (key, value) -> value.length());
        assertEquals(Integer.valueOf(9), lengths.get(1L));

        Map<String, Long> inverse = new ConcurrentHashMap<>();
        Bind.mapInverse(primary, inverse);
        Set<Bind.Tuple2<String, Long>> keys = ConcurrentHashMap.newKeySet();
        Bind.secondaryKeys(primary, keys,
                (key, value) -> Arrays.asList(value.split(",")));
        ConcurrentHashMap<Character, Long> histogram = new ConcurrentHashMap<>();
        Bind.histogram(primary, histogram, (key, value) -> value.charAt(0));
        Map<Long, String> deleted = new ConcurrentHashMap<>();
        Bind.mapPutAfterDelete(primary, deleted);

        primary.put(2L, "blue,square");
        primary.put(1L, "red,square");
        assertEquals(Long.valueOf(1), inverse.get("red,square"));
        assertFalse(inverse.containsKey("red,round"));
        assertTrue(keys.contains(new Bind.Tuple2<>("square", 1L)));
        assertFalse(keys.contains(new Bind.Tuple2<>("round", 1L)));
        assertEquals(Long.valueOf(1), histogram.get('r'));
        assertEquals(Long.valueOf(1), histogram.get('b'));

        primary.remove(2L);
        assertEquals("blue,square", deleted.get(2L));
        assertFalse(histogram.containsKey('b'));
        store.close();
    }

    @Test public void sizeBindingAndBTreeCompatibilityInterface() {
        Store store = new StoreOnHeap(true);
        BTreeMap<Long, String> primary = BTreeMap.create(
                store, LongFormat.INSTANCE, StringGroupFormat.INSTANCE, 8, false);
        primary.put(1L, "one");
        long counterRecid = store.put(0L, Serializers.LONG);
        Atomic.Long counter = new Atomic.Long(store, counterRecid);
        Bind.size(primary, counter);
        assertEquals(1, counter.get());
        primary.put(2L, "two");
        primary.put(2L, "two updated");
        primary.remove(1L);
        assertEquals(1, counter.get());
        store.close();
    }

    /** N threads hammering the SAME keys on a secondaryValue-bound map: at quiescence the secondary
     *  index must equal derive(primary) for every key. Because the order-sensitive binding installs
     *  a SYNCHRONOUS listener (fired under the leaf/segment lock that serialized the mutation),
     *  same-key listener events are totally ordered with the mutations, so the last writer's value
     *  wins in the index too. (Deferred firing could reorder same-key events under a preemption at
     *  the unlock/fire boundary and leave the index on a losing value; this stress test guards the
     *  invariant holds and the binding stays crash-free under contention.) */
    @Test public void concurrentSameKeyWritersKeepSecondaryConsistentBTree() throws Exception {
        Store store = new StoreOnHeap(true);
        BTreeMap<Long, String> primary = BTreeMap.create(
                store, LongFormat.INSTANCE, StringGroupFormat.INSTANCE, 8, false);
        try {
            assertSecondaryConsistentUnderContention(primary);
        } finally {
            store.close();
        }
    }

    @Test public void concurrentSameKeyWritersKeepSecondaryConsistentHTree() throws Exception {
        Store store = new StoreOnHeap(true);
        HTreeMap<Long, String> primary = HTreeMap.create(
                store, Serializers.LONG, Serializers.STRING, 0, 4, 8);
        try {
            assertSecondaryConsistentUnderContention(primary);
        } finally {
            store.close();
        }
    }

    private static void assertSecondaryConsistentUnderContention(
            ModificationAwareMap<Long, String> primary) throws Exception {
        // secondary value = "d:" + primary value, so we can check secondary == derive(primary).
        Map<Long, String> secondary = new ConcurrentHashMap<>();
        Bind.secondaryValue(primary, secondary, (key, value) -> "d:" + value);

        int keyCount = 4;
        int threadCount = 8;
        int iterations = 8000;
        Thread[] threads = new Thread[threadCount];
        final java.util.concurrent.atomic.AtomicReference<Throwable> failure =
                new java.util.concurrent.atomic.AtomicReference<>();
        final java.util.concurrent.CyclicBarrier barrier =
                new java.util.concurrent.CyclicBarrier(threadCount);
        for (int t = 0; t < threadCount; t++) {
            final int tid = t;
            threads[t] = new Thread(() -> {
                try {
                    barrier.await(30, java.util.concurrent.TimeUnit.SECONDS);
                    // All threads target the SAME key each iteration to maximize same-key
                    // contention, so the winning leaf-mutation and its listener are constantly
                    // racing. Every listener must have run (deferred or sync) by join().
                    for (int i = 0; i < iterations; i++) {
                        long key = i % keyCount;
                        primary.put(key, "t" + tid + "-i" + i);
                    }
                } catch (Throwable e) { failure.compareAndSet(null, e); }
            });
            threads[t].setDaemon(true); // a straggler must not keep the forked JVM alive
        }
        for (Thread thread : threads) thread.start();
        for (Thread thread : threads) joinOrFail(thread);
        if (failure.get() != null) throw new AssertionError(failure.get());
        // No settling write: assert the CONCURRENT end-state is consistent. All deferred/sync
        // listeners have run by join(), so with correct ordering the sync listener of the last
        // same-key mutation left secondary pointing at the winning primary value. Deferred (wrong)
        // ordering can leave secondary on a losing value and this check fails.
        for (long key = 0; key < keyCount; key++) {
            String primaryValue = primary.get(key);
            assertEquals("secondary must track winning primary value for key " + key,
                    primaryValue == null ? null : "d:" + primaryValue, secondary.get(key));
        }
        // No stale keys: every secondary key must still be present in primary.
        for (Long key : secondary.keySet())
            assertTrue("secondary key " + key + " absent from primary", primary.containsKey(key));
    }

    /** Ordering discriminator for the sync-listener design: while writer T1's sync listener is
     *  still executing (blocked) INSIDE the primary's leaf/segment lock, a second same-key writer
     *  T2 must be held out — so T2's index update can never be clobbered by T1's late-running
     *  listener. Two layers: the no-progress-window check (T2 makes no observable progress while
     *  T1's listener holds the lock) is STRONG-PROBABILISTIC — a starved T2 could also show no
     *  progress — but the final-state check is DETERMINISTIC whenever T2 did overtake: under the
     *  old deferred firing T2 completes inside the window, and T1's released listener then always
     *  overwrites the fresh secondary entry with the stale one, failing the last assertion. */
    @Test public void blockedSyncListenerHoldsOutSameKeyWriterBTree() throws Exception {
        Store store = new StoreOnHeap(true);
        BTreeMap<Long, String> primary = BTreeMap.create(
                store, LongFormat.INSTANCE, StringGroupFormat.INSTANCE, 8, false);
        try {
            assertBlockedListenerHoldsOutSameKeyWriter(primary);
        } finally {
            store.close();
        }
    }

    @Test public void blockedSyncListenerHoldsOutSameKeyWriterHTree() throws Exception {
        Store store = new StoreOnHeap(true);
        HTreeMap<Long, String> primary = HTreeMap.create(
                store, Serializers.LONG, Serializers.STRING, 0, 4, 8);
        try {
            assertBlockedListenerHoldsOutSameKeyWriter(primary);
        } finally {
            store.close();
        }
    }

    private static void assertBlockedListenerHoldsOutSameKeyWriter(
            ModificationAwareMap<Long, String> primary) throws Exception {
        Map<Long, String> secondary = new ConcurrentHashMap<>();
        java.util.concurrent.CountDownLatch entered = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        Bind.secondaryValue(primary, secondary, (key, value) -> {
            if (value.equals("A")) {
                entered.countDown();
                try {
                    if (!release.await(30, java.util.concurrent.TimeUnit.SECONDS))
                        throw new IllegalStateException("release latch timed out");
                } catch (InterruptedException e) { throw new IllegalStateException(e); }
            }
            return "d:" + value;
        });
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread t1 = new Thread(() -> {
            try { primary.put(1L, "A"); } catch (Throwable e) { failure.compareAndSet(null, e); }
        });
        t1.setDaemon(true);
        t1.start();
        assertTrue("listener never entered", entered.await(30, java.util.concurrent.TimeUnit.SECONDS));
        // progress-observable side effect: writerDone bumps only after T2's put RETURNS
        java.util.concurrent.atomic.AtomicInteger writerDone =
                new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.CountDownLatch writerStarting = new java.util.concurrent.CountDownLatch(1);
        Thread t2 = new Thread(() -> {
            try {
                writerStarting.countDown(); // handshake: about to enter put
                primary.put(1L, "B");
                writerDone.incrementAndGet();
            } catch (Throwable e) { failure.compareAndSet(null, e); }
        });
        t2.setDaemon(true);
        t2.start();
        assertTrue("writer never started", writerStarting.await(30, java.util.concurrent.TimeUnit.SECONDS));
        Thread.sleep(300); // no-progress window (strong-probabilistic; see method javadoc)
        assertEquals("second same-key writer overtook the in-flight sync listener",
                0, writerDone.get());
        assertNull("secondary updated by an overtaking writer", secondary.get(1L));
        release.countDown();
        joinOrFail(t1);
        joinOrFail(t2);
        if (failure.get() != null) throw new AssertionError(failure.get());
        assertEquals(1, writerDone.get());
        assertEquals("B", primary.get(1L));
        // DETERMINISTIC on overtake: deferred firing would leave the stale d:A here
        assertEquals("d:B", secondary.get(1L));
    }

    /** Order-sensitive bindings mutate the secondary under the primary's lock, so binding a map
     *  to itself would re-enter that lock; Bind must reject the direct self-cycle. */
    @Test(expected = IllegalArgumentException.class)
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void selfBindingIsRejected() {
        Store store = new StoreOnHeap(true);
        try {
            HTreeMap<Long, String> primary = HTreeMap.create(
                    store, Serializers.LONG, Serializers.STRING, 0, 4, 8);
            Bind.secondaryValue(primary, (Map) primary, (key, value) -> value);
        } finally {
            store.close();
        }
    }

    private static void joinOrFail(Thread thread) throws InterruptedException {
        thread.join(30_000);
        if (thread.isAlive())
            throw new AssertionError("thread did not finish within 30s (deadlock?): " + thread);
    }

    @Test(expected = IllegalArgumentException.class)
    public void uniqueSecondaryRejectsDuplicateDerivedKeys() {
        Store store = new StoreOnHeap(true);
        try {
            HTreeMap<Long, String> primary = HTreeMap.create(
                    store, Serializers.LONG, Serializers.STRING, 0, 4, 8);
            Map<Integer, Long> unique = new ConcurrentHashMap<>();
            Bind.secondaryKey(primary, unique, (key, value) -> value.length());
            primary.put(1L, "same");
            primary.put(2L, "size");
        } finally {
            store.close();
        }
    }
}
