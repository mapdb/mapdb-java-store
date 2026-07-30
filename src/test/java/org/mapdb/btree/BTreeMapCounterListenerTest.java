package org.mapdb.btree;

import org.junit.After;
import org.junit.Test;
import org.mapdb.ser.LongFormat;
import org.mapdb.store.Store;
import org.mapdb.store.StoreOnHeap;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Feature A (O(1) size counter) and Feature B (modification listener) tests for
 * {@link BTreeMap}. Runs on {@link StoreOnHeap} (thread-safe by default) so the
 * concurrent cases exercise the real node-lock + CAS-counter protocol.
 */
public class BTreeMapCounterListenerTest {

    private Store store;

    private BTreeMap<Long, Long> counterMap(int maxNodeSize) {
        store = new StoreOnHeap();
        return BTreeMap.create(store, LongFormat.INSTANCE, LongFormat.INSTANCE, maxNodeSize, true);
    }

    private BTreeMap<Long, Long> plainMap(int maxNodeSize) {
        store = new StoreOnHeap();
        return BTreeMap.create(store, LongFormat.INSTANCE, LongFormat.INSTANCE, maxNodeSize, false);
    }

    @After
    public void tearDown() {
        if (store != null && !store.isClosed()) {
            store.verify();
            store.close();
        }
    }

    private static long traversalCount(BTreeMap<Long, Long> m) {
        long c = 0;
        for (Iterator<Map.Entry<Long, Long>> it = m.entryIterator(); it.hasNext(); ) { it.next(); c++; }
        return c;
    }

    // ---------------- counter: sequential ----------------

    @Test
    public void counterDisabledByDefault() {
        BTreeMap<Long, Long> m = plainMap(8);
        assertEquals(0L, m.counterRecid());
        m.put(1L, 1L);
        assertEquals(1L, m.sizeLong()); // falls back to traversal
    }

    @Test
    public void counterEnabledExposesRecid() {
        BTreeMap<Long, Long> m = counterMap(8);
        assertTrue(m.counterRecid() > 0);
        assertEquals(0L, m.sizeLong());
    }

    @Test
    public void counterInsertUpdateRemoveClear() {
        BTreeMap<Long, Long> m = counterMap(6);

        // inserts
        for (long i = 0; i < 100; i++) {
            assertNull(m.put(i, i));
            assertEquals(i + 1, m.sizeLong());
        }
        assertEquals(100L, m.sizeLong());
        assertEquals(traversalCount(m), m.sizeLong());

        // updates do NOT change the counter
        for (long i = 0; i < 100; i++) {
            assertEquals(Long.valueOf(i), m.put(i, i * 10));
        }
        assertEquals(100L, m.sizeLong());

        // putIfAbsent on present key: no change
        assertEquals(Long.valueOf(0L), m.putIfAbsent(0L, 999L));
        assertEquals(100L, m.sizeLong());
        // putIfAbsent on absent key: +1
        assertNull(m.putIfAbsent(1000L, 1L));
        assertEquals(101L, m.sizeLong());
        m.remove(1000L);
        assertEquals(100L, m.sizeLong());

        // replace of present key: no change
        assertEquals(Long.valueOf(0L), m.replace(0L, 7L));
        assertEquals(100L, m.sizeLong());
        // replace of absent key: no change (nothing replaced)
        assertNull(m.replace(5000L, 1L));
        assertEquals(100L, m.sizeLong());

        // removes
        for (long i = 0; i < 50; i++) {
            assertEquals(Long.valueOf(i == 0 ? 7L : i * 10), m.remove(i));
            assertEquals(100 - (i + 1), m.sizeLong());
        }
        assertEquals(50L, m.sizeLong());
        assertEquals(traversalCount(m), m.sizeLong());

        // remove of absent key: no change
        assertNull(m.remove(0L));
        assertEquals(50L, m.sizeLong());

        // clear resets to 0
        m.clear();
        assertEquals(0L, m.sizeLong());
        assertEquals(0L, traversalCount(m));
    }

    @Test
    public void counterMatchesTraversalAfterMixedOps() {
        BTreeMap<Long, Long> m = counterMap(4); // small nodes -> many splits
        java.util.Random rnd = new java.util.Random(42);
        java.util.TreeMap<Long, Long> ref = new java.util.TreeMap<>();
        for (int i = 0; i < 5000; i++) {
            long k = rnd.nextInt(500);
            if (rnd.nextBoolean()) {
                Long prev = m.put(k, k);
                boolean wasPresent = ref.put(k, k) != null;
                assertEquals(wasPresent, prev != null);
            } else {
                Long prev = m.remove(k);
                boolean wasPresent = ref.remove(k) != null;
                assertEquals(wasPresent, prev != null);
            }
            assertEquals(ref.size(), m.sizeLong());
        }
        assertEquals(traversalCount(m), m.sizeLong());
    }

    // ---------------- counter: concurrent ----------------

    @Test
    public void counterConcurrentDisjointKeys() throws Exception {
        final BTreeMap<Long, Long> m = counterMap(8);
        final int threads = 8, perThread = 5000;
        final CyclicBarrier barrier = new CyclicBarrier(threads);
        List<Thread> ts = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final long base = (long) t * perThread;
            Thread th = new Thread(() -> {
                try { barrier.await(); } catch (Exception e) { throw new RuntimeException(e); }
                for (long i = 0; i < perThread; i++) m.put(base + i, base + i);
            });
            ts.add(th);
            th.start();
        }
        for (Thread th : ts) th.join();

        long expected = (long) threads * perThread;
        assertEquals(expected, m.sizeLong());
        assertEquals(traversalCount(m), m.sizeLong());
    }

    @Test
    public void counterConcurrentPutRemoveSameKeyspace() throws Exception {
        final BTreeMap<Long, Long> m = counterMap(6);
        final int keyspace = 2000;
        // pre-fill even keys
        for (long k = 0; k < keyspace; k += 2) m.put(k, k);

        final int threads = 8;
        final int opsPerThread = 20000;
        final CyclicBarrier barrier = new CyclicBarrier(threads);
        List<Thread> ts = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final int seed = t;
            Thread th = new Thread(() -> {
                java.util.Random rnd = new java.util.Random(seed);
                try { barrier.await(); } catch (Exception e) { throw new RuntimeException(e); }
                for (int i = 0; i < opsPerThread; i++) {
                    long k = rnd.nextInt(keyspace);
                    if (rnd.nextBoolean()) m.put(k, k);
                    else m.remove(k);
                }
            });
            ts.add(th);
            th.start();
        }
        for (Thread th : ts) th.join();

        // After all threads join, the counter must equal the actual live entry count.
        assertEquals(traversalCount(m), m.sizeLong());
    }

    @Test
    public void counterConcurrentInsertThenRemoveToEmpty() throws Exception {
        final BTreeMap<Long, Long> m = counterMap(6);
        final int threads = 6, perThread = 4000;
        // phase 1: concurrent disjoint inserts
        runDisjoint(m, threads, perThread, true);
        assertEquals((long) threads * perThread, m.sizeLong());
        assertEquals(traversalCount(m), m.sizeLong());
        // phase 2: concurrent disjoint removes of the same keys
        runDisjoint(m, threads, perThread, false);
        assertEquals(0L, m.sizeLong());
        assertEquals(0L, traversalCount(m));
    }

    private static void runDisjoint(BTreeMap<Long, Long> m, int threads, int perThread, boolean insert)
            throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(threads);
        List<Thread> ts = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final long base = (long) t * perThread;
            Thread th = new Thread(() -> {
                try { barrier.await(); } catch (Exception e) { throw new RuntimeException(e); }
                for (long i = 0; i < perThread; i++) {
                    if (insert) m.put(base + i, base + i);
                    else m.remove(base + i);
                }
            });
            ts.add(th);
            th.start();
        }
        for (Thread th : ts) th.join();
    }

    // ---------------- listener ----------------

    private static final class Event {
        final long key; final Long oldV; final Long newV;
        Event(long key, Long oldV, Long newV) { this.key = key; this.oldV = oldV; this.newV = newV; }
    }

    @Test
    public void listenerInsertUpdateRemove() {
        BTreeMap<Long, Long> m = plainMap(8);
        List<Event> events = new ArrayList<>();
        m.addModificationListener((k, o, n) -> events.add(new Event(k, o, n)));

        m.put(5L, 50L);                 // insert: old=null new=50
        m.put(5L, 60L);                 // update: old=50 new=60
        assertEquals(Long.valueOf(60L), m.remove(5L)); // remove: old=60 new=null

        assertEquals(3, events.size());
        assertEvent(events.get(0), 5L, null, 50L);
        assertEvent(events.get(1), 5L, 50L, 60L);
        assertEvent(events.get(2), 5L, 60L, null);
    }

    @Test
    public void listenerReplaceAndConditionalOps() {
        BTreeMap<Long, Long> m = plainMap(8);
        List<Event> events = new ArrayList<>();
        m.addModificationListener((k, o, n) -> events.add(new Event(k, o, n)));

        m.put(1L, 10L);                 // insert
        m.replace(1L, 20L);             // update via replace(K,V): old=10 new=20
        m.replace(1L, 20L, 30L);        // update via replace(K,V,V): old=20 new=30
        // failed conditional ops fire NOTHING:
        m.replace(1L, 999L, 40L);       // value mismatch -> no event
        m.replace(2L, 5L);              // absent -> no event
        m.putIfAbsent(1L, 77L);         // present -> no event, no change
        m.remove(1L, 999L);             // value mismatch -> no event
        m.putIfAbsent(2L, 22L);         // insert via putIfAbsent
        assertTrue(m.remove(2L, 22L));  // conditional remove success -> remove event

        assertEquals(5, events.size());
        assertEvent(events.get(0), 1L, null, 10L);
        assertEvent(events.get(1), 1L, 10L, 20L);
        assertEvent(events.get(2), 1L, 20L, 30L);
        assertEvent(events.get(3), 2L, null, 22L);
        assertEvent(events.get(4), 2L, 22L, null);
    }

    @Test
    public void listenerClearFiresRemovalForEveryEntry() {
        BTreeMap<Long, Long> m = counterMap(4);
        List<Event> events = new ArrayList<>();
        m.addModificationListener((k, o, n) -> events.add(new Event(k, o, n)));

        for (long i = 0; i < 25; i++) m.put(i, i * 10);
        events.clear();
        m.clear();

        assertEquals(25, events.size());
        for (int i = 0; i < 25; i++) {
            assertEvent(events.get(i), i, i * 10L, null);
        }
        assertEquals(0L, m.sizeLong());
        assertEquals(0L, traversalCount(m));
    }

    @Test
    public void multipleListenersAllFire() {
        BTreeMap<Long, Long> m = plainMap(8);
        AtomicLong a = new AtomicLong();
        AtomicLong b = new AtomicLong();
        m.addModificationListener((k, o, n) -> a.incrementAndGet());
        m.addModificationListener((k, o, n) -> b.incrementAndGet());

        m.put(1L, 1L);
        m.put(1L, 2L);
        m.remove(1L);
        assertEquals(3L, a.get());
        assertEquals(3L, b.get());
    }

    @Test
    public void listenerConcurrentEventCountMatchesOps() throws Exception {
        final BTreeMap<Long, Long> m = counterMap(6);
        final ConcurrentLinkedQueue<Event> events = new ConcurrentLinkedQueue<>();
        m.addModificationListener((k, o, n) -> events.add(new Event(k, o, n)));

        final int threads = 6, perThread = 3000;
        runDisjoint(m, threads, perThread, true); // all inserts
        // every insert fires exactly one insert-event (old==null)
        assertEquals((long) threads * perThread, events.size());
        for (Event e : events) assertNull("insert events have null oldValue", e.oldV);
        assertEquals((long) threads * perThread, m.sizeLong());
    }

    // ---------------- bulk build ----------------

    @Test
    public void bulkBuildCounter() {
        store = new StoreOnHeap();
        int n = 3000;
        List<Map.Entry<Long, Long>> entries = new ArrayList<>();
        for (long i = 0; i < n; i++) entries.add(new AbstractMap.SimpleImmutableEntry<>(i, i * 2));
        BTreeMap<Long, Long> m = BTreeMap.createFromSorted(
                store, LongFormat.INSTANCE, LongFormat.INSTANCE, 16, entries.iterator(), true);

        assertTrue(m.counterRecid() > 0);
        assertEquals(n, m.sizeLong());
        assertEquals(traversalCount(m), m.sizeLong());

        // counter keeps tracking after the bulk build
        assertNull(m.put((long) n, 1L));
        assertEquals(n + 1, m.sizeLong());
        m.remove(0L);
        assertEquals(n, m.sizeLong());
    }

    @Test
    public void bulkBuildNoCounter() {
        store = new StoreOnHeap();
        List<Map.Entry<Long, Long>> entries = new ArrayList<>();
        for (long i = 0; i < 100; i++) entries.add(new AbstractMap.SimpleImmutableEntry<>(i, i));
        BTreeMap<Long, Long> m = BTreeMap.createFromSorted(
                store, LongFormat.INSTANCE, LongFormat.INSTANCE, 16, entries.iterator(), false);
        assertEquals(0L, m.counterRecid());
        assertEquals(100L, m.sizeLong()); // traversal fallback
    }

    @Test
    public void bulkBuildEmptyCounterStartsAtZeroAndTracksLaterWrites() {
        store = new StoreOnHeap();
        BTreeMap<Long, Long> m = BTreeMap.createFromSorted(
                store, LongFormat.INSTANCE, LongFormat.INSTANCE, 16,
                java.util.Collections.emptyIterator(), true);

        assertTrue(m.counterRecid() > 0);
        assertEquals(0L, m.sizeLong());
        assertNull(m.put(1L, 10L));
        assertEquals(1L, m.sizeLong());
    }

    // ---------------- reopen ----------------

    @Test
    public void reopenWithCounterRecid() {
        BTreeMap<Long, Long> m = counterMap(8);
        for (long i = 0; i < 200; i++) m.put(i, i);
        long rrr = m.rootRecidRecid();
        long cr = m.counterRecid();
        assertEquals(200L, m.sizeLong());

        BTreeMap<Long, Long> re = BTreeMap.open(
                store, rrr, LongFormat.INSTANCE, LongFormat.INSTANCE, 8, cr);
        assertEquals(200L, re.sizeLong());
        re.put(200L, 200L);
        assertEquals(201L, re.sizeLong());
        // original handle observes it too (shared record)
        assertEquals(201L, m.sizeLong());
    }

    private static void assertEvent(Event e, long key, Long oldV, Long newV) {
        assertEquals(key, e.key);
        assertEquals(oldV, e.oldV);
        assertEquals(newV, e.newV);
    }
}
