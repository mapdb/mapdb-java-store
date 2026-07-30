package org.mapdb.indextree;

import org.junit.Test;
import org.mapdb.store.Store;
import org.mapdb.store.StoreByteArray;
import org.mapdb.store.StoreDirect;
import org.mapdb.store.StoreOnHeap;
import org.mapdb.store.StoreWAL;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * {@link IndexTreeLongLongMap}: basic primitive ops, default-value semantics, exact
 * containsKey/size, a randomized model-based fuzz vs {@code HashMap<Long,Long>} (run on
 * both an object store and a serializing store), and a commit+reopen durability test.
 */
public class IndexTreeLongLongMapTest {

    private interface Case { void run(Store store); }

    private static void onBothStores(Case c) {
        for (Store store : new Store[]{new StoreOnHeap(), new StoreByteArray(), new StoreDirect()}) {
            c.run(store);
            store.verify();
            store.close();
        }
    }

    @Test public void basicOps() {
        onBothStores(store -> {
            IndexTreeLongLongMap m = IndexTreeLongLongMap.create(store, -1L);
            assertTrue(m.isEmpty());
            assertEquals(0, m.size());
            assertEquals(-1L, m.get(42));           // absent -> default
            assertFalse(m.containsKey(42));

            m.put(42, 100);
            assertEquals(1, m.size());
            assertFalse(m.isEmpty());
            assertTrue(m.containsKey(42));
            assertEquals(100, m.get(42));

            m.put(42, 200);                          // overwrite, no size change
            assertEquals(1, m.size());
            assertEquals(200, m.get(42));

            m.put(0, 0);                             // value 0 is a real, distinct entry
            assertTrue(m.containsKey(0));
            assertEquals(0, m.get(0));
            assertEquals(2, m.size());

            m.put(IndexTreeLongLongMap.MAX_KEY, -999); // max key + negative value
            assertEquals(-999, m.get(IndexTreeLongLongMap.MAX_KEY));
            assertEquals(3, m.size());

            assertTrue(m.remove(42));
            assertFalse(m.remove(42));               // idempotent
            assertEquals(-1L, m.get(42));
            assertEquals(2, m.size());

            store.verify();
            m.clear();
            assertEquals(0, m.size());
            assertTrue(m.isEmpty());
            assertEquals(-1L, m.get(0));
        });
    }

    @Test public void putIfAbsentAndAddTo() {
        onBothStores(store -> {
            IndexTreeLongLongMap m = IndexTreeLongLongMap.create(store, 0L);
            assertEquals(5, m.putIfAbsent(1, 5));
            assertEquals(5, m.putIfAbsent(1, 9));    // already present: returns existing
            assertEquals(5, m.get(1));
            assertEquals(1, m.size());

            assertEquals(10, m.addTo(2, 10));        // absent: default(0)+10
            assertEquals(13, m.addTo(2, 3));
            assertEquals(13, m.get(2));
            assertEquals(2, m.size());
        });
    }

    @Test public void outOfRangeKeyRejected() {
        onBothStores(store -> {
            IndexTreeLongLongMap m = IndexTreeLongLongMap.create(store);
            // negative and > MAX_KEY (== Long.MAX_VALUE) are both out of range
            for (long bad : new long[]{-1, Long.MIN_VALUE, Long.MAX_VALUE}) {
                assertFalse(m.containsKey(bad));
                assertEquals(0L, m.getOrDefault(bad, 0L));
                for (Runnable r : new Runnable[]{
                        () -> m.put(bad, 1), () -> m.get(bad), () -> m.remove(bad)}) {
                    try { r.run(); org.junit.Assert.fail("expected IAE for " + bad); }
                    catch (IllegalArgumentException expected) { /* ok */ }
                }
            }
            // MAX_KEY itself is valid
            m.put(IndexTreeLongLongMap.MAX_KEY, 55);
            assertEquals(55, m.get(IndexTreeLongLongMap.MAX_KEY));
        });
    }

    @Test public void forEachVisitsAll() {
        onBothStores(store -> {
            IndexTreeLongLongMap m = IndexTreeLongLongMap.create(store);
            Map<Long, Long> oracle = new HashMap<>();
            Random rnd = new Random(7);
            for (int i = 0; i < 2000; i++) {
                long k = rnd.nextInt(5000);
                long v = rnd.nextLong();
                m.put(k, v);
                oracle.put(k, v);
            }
            Map<Long, Long> seen = new HashMap<>();
            m.forEach((k, v) -> assertTrue("dup key " + k, seen.put(k, v) == null));
            assertEquals(oracle, seen);
            assertEquals(oracle.size(), m.size());
        });
    }

    @Test public void fuzzVsHashMap() {
        onBothStores(store -> {
            IndexTreeLongLongMap m = IndexTreeLongLongMap.create(store, 0L);
            Map<Long, Long> oracle = new HashMap<>();
            Random rnd = new Random(0xF0FA1L);

            for (int i = 0; i < 40_000; i++) {
                // mix narrow keys (deep shared chains) with the wide range (<= MAX_KEY)
                long key = (rnd.nextInt(4) > 0 ? rnd.nextInt(1 << 12) : (rnd.nextLong() >>> 2));
                int roll = rnd.nextInt(100);
                if (roll < 55) {
                    long v = rnd.nextLong();
                    m.put(key, v);
                    oracle.put(key, v);
                } else if (roll < 80) {
                    assertEquals("remove[" + i + "]", oracle.remove(key) != null, m.remove(key));
                } else if (roll < 90) {
                    assertEquals("contains[" + i + "]", oracle.containsKey(key), m.containsKey(key));
                } else {
                    Long exp = oracle.get(key);
                    assertEquals("get[" + i + "]", exp == null ? 0L : (long) exp, m.get(key));
                }
                if (i % 4000 == 0) assertEquals(oracle.size(), m.size());
            }

            assertEquals(oracle.size(), m.size());
            for (Map.Entry<Long, Long> e : oracle.entrySet()) {
                assertTrue(m.containsKey(e.getKey()));
                assertEquals((long) e.getValue(), m.get(e.getKey()));
            }
            Map<Long, Long> folded = new HashMap<>();
            m.forEach(folded::put);
            assertEquals(oracle, folded);
            store.verify();
        });
    }

    @Test public void committedContentSurvivesReopen() throws IOException {
        File file = File.createTempFile("indextree-llm", ".db");
        file.delete();
        try {
            Map<Long, Long> oracle = new TreeMap<>();
            long headerRecid;
            {
                StoreWAL store = new StoreWAL(file);
                IndexTreeLongLongMap m = IndexTreeLongLongMap.create(store, -7L);
                headerRecid = m.headerRecid();
                Random rnd = new Random(99);
                for (int i = 0; i < 10_000; i++) {
                    long k = rnd.nextInt(20_000);
                    long v = rnd.nextLong();
                    m.put(k, v);
                    oracle.put(k, v);
                }
                for (int i = 0; i < 2000; i++) {                 // some removals too
                    long k = rnd.nextInt(20_000);
                    assertEquals(oracle.remove(k) != null, m.remove(k));
                }
                store.commit();
                store.verify();
                store.close();
            }
            {
                StoreWAL store = new StoreWAL(file);
                store.verify();
                IndexTreeLongLongMap m = IndexTreeLongLongMap.open(store, headerRecid);
                assertEquals(-7L, m.defaultValue());
                assertEquals(oracle.size(), m.size());
                for (Map.Entry<Long, Long> e : oracle.entrySet()) {
                    assertEquals("reopen get " + e.getKey(), (long) e.getValue(), m.get(e.getKey()));
                }
                assertEquals(-7L, m.get(123_456));               // absent -> persisted default
                // reopened map keeps working
                m.put(123_456, 5);
                assertEquals(5, m.get(123_456));
                store.commit();
                store.verify();
                store.close();
            }
        } finally {
            file.delete();
        }
    }
}
