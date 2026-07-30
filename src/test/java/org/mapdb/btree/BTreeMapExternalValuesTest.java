package org.mapdb.btree;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.mapdb.ser.LongFormat;
import org.mapdb.ser.StringGroupFormat;
import org.mapdb.store.Store;
import org.mapdb.store.StoreByteArray;
import org.mapdb.store.StoreDirect;
import org.mapdb.store.StoreOnHeap;

public class BTreeMapExternalValuesTest {

    @Test public void operationsViewsAndReopenAcrossStores() {
        for (Store store : Arrays.asList(
                new StoreOnHeap(true), new StoreByteArray(true), new StoreDirect(false, true))) {
            BTreeMap<Long, String> map = BTreeMap.createExternalValues(store,
                    LongFormat.INSTANCE, StringGroupFormat.INSTANCE, 4, true);
            assertFalse(map.valueInline());
            for (long i = 0; i < 40; i++) assertNull(map.put(i, "v" + i));
            assertEquals("v5", map.put(5L, "updated"));
            assertEquals("updated", map.get(5L));
            assertEquals("v6", map.replace(6L, "six"));
            assertTrue(map.replace(7L, "v7", "seven"));
            assertFalse(map.replace(7L, "v7", "wrong"));
            assertTrue(map.remove(8L, "v8"));
            assertEquals("v9", map.subMap(9L, true, 12L, false).remove(9L));
            Map.Entry<Long, String> first = map.pollFirstEntry();
            assertEquals(Long.valueOf(0), first.getKey());
            assertEquals("v0", first.getValue());
            assertEquals(37L, map.sizeLong());

            BTreeMap<Long, String> reopened = BTreeMap.openExternalValues(store,
                    map.rootRecidRecid(), LongFormat.INSTANCE, StringGroupFormat.INSTANCE,
                    4, map.counterRecid());
            assertEquals("updated", reopened.get(5L));
            assertEquals("seven", reopened.get(7L));
            assertEquals(37, reopened.entrySet().size());
            reopened.clear();
            assertTrue(reopened.isEmpty());
            assertEquals(0L, reopened.sizeLong());
            store.verify();
            store.close();
        }
    }

    // Readers hold externalValueLock.readLock() across store.get (not lock-free), which is what
    // keeps a concurrent remove from deleting+reusing the recid mid-read.
    @Test public void readerNeverObservesReusedExternalValueUnderConcurrentRemove() throws Exception {
        Store store = new StoreDirect(false, true);
        BTreeMap<Long, String> map = BTreeMap.createExternalValues(store,
                LongFormat.INSTANCE, StringGroupFormat.INSTANCE, 8, false);
        map.put(1L, "one");
        int baselineRecids = recidCount(store);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread reader = new Thread(() -> {
            try {
                for (int i = 0; i < 20_000; i++) {
                    String value = map.get(1L);
                    if (value != null && !value.equals("one"))
                        throw new AssertionError("unexpected value " + value);
                }
            } catch (Throwable e) { failure.set(e); }
        });
        reader.setDaemon(true);
        reader.start();
        for (int i = 0; i < 2_000; i++) {
            map.remove(1L);
            map.put(2L, "unrelated");
            map.remove(2L);
            map.put(1L, "one");
        }
        joinOrFail(reader);
        if (failure.get() != null) throw new AssertionError(failure.get());
        assertTrue("external value records leaked under churn",
                recidCount(store) <= baselineRecids + 2);
        store.close();
    }

    /** Multi-threaded put/get/remove/iterate churn on one external-value map, then a full clear:
     *  no external-value recids may leak (bounded getAllRecids count after the map empties). */
    @Test public void concurrentChurnLeaksNoExternalValueRecids() throws Exception {
        Store store = new StoreDirect(false, true);
        BTreeMap<Long, String> map = BTreeMap.createExternalValues(store,
                LongFormat.INSTANCE, StringGroupFormat.INSTANCE, 8, false);
        int baselineRecids = recidCount(store); // structure only, empty map
        int keyCount = 256;
        int threadCount = 6;
        int iterations = 6000;
        Thread[] threads = new Thread[threadCount];
        AtomicReference<Throwable> failure = new AtomicReference<>();
        java.util.concurrent.CyclicBarrier barrier = new java.util.concurrent.CyclicBarrier(threadCount);
        for (int t = 0; t < threadCount; t++) {
            final int tid = t;
            threads[t] = new Thread(() -> {
                try {
                    barrier.await(30, java.util.concurrent.TimeUnit.SECONDS);
                    java.util.Random rnd = new java.util.Random(tid);
                    for (int i = 0; i < iterations; i++) {
                        long key = rnd.nextInt(keyCount);
                        switch (i & 3) {
                            case 0: map.put(key, "t" + tid + "v" + i); break;
                            case 1: map.get(key); break;
                            case 2: map.remove(key); break;
                            default:
                                int seen = 0;
                                java.util.Iterator<Map.Entry<Long, String>> it = map.entryIterator();
                                while (it.hasNext() && seen < 32) { it.next(); seen++; }
                        }
                    }
                } catch (Throwable e) { failure.compareAndSet(null, e); }
            });
            threads[t].setDaemon(true); // a straggler must not keep the forked JVM alive
        }
        for (Thread thread : threads) thread.start();
        for (Thread thread : threads) joinOrFail(thread);
        if (failure.get() != null) throw new AssertionError(failure.get());
        for (long key = 0; key < keyCount; key++) map.remove(key);
        store.verify();
        // The emptied map keeps its split leaf/dir nodes (mapdb3 semantics: no merge on remove),
        // bounded by the key count, but every external VALUE recid must be gone. A value leak would
        // scale with the ~thousands of churn operations, far exceeding this structural bound.
        int structuralBound = baselineRecids + keyCount;
        assertTrue("external value records leaked under concurrent churn: "
                        + recidCount(store) + " > " + structuralBound,
                recidCount(store) <= structuralBound);
        store.close();
    }

    private static void joinOrFail(Thread thread) throws InterruptedException {
        thread.join(30_000);
        if (thread.isAlive())
            throw new AssertionError("thread did not finish within 30s (deadlock?): " + thread);
    }

    private static int recidCount(Store store) {
        int count = 0;
        java.util.PrimitiveIterator.OfLong recids = store.getAllRecids();
        while (recids.hasNext()) { recids.nextLong(); count++; }
        return count;
    }
}
