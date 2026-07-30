package org.mapdb.ported;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mapdb.btree.BTreeMap;
import org.mapdb.ser.GroupFormat;
import org.mapdb.ser.LongFormat;
import org.mapdb.ser.ObjectArrayFormat;
import org.mapdb.ser.Serializers;
import org.mapdb.store.Store;
import org.mapdb.store.StoreByteArray;
import org.mapdb.store.StoreDirect;
import org.mapdb.store.StoreOnHeap;
import org.mapdb.store.StoreWAL;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * BTreeMap regression scenarios ported from the historical suites, expressed using only
 * the mapdb5 BTreeMap skeleton API (put/get/remove/containsKey/sizeLong/entryIterator).
 * Run across every store dialect.
 */
@RunWith(Parameterized.class)
public class PortedBTreeScenarioTest {

    @Parameterized.Parameters(name = "{0}")
    public static List<String> stores() {
        return java.util.Arrays.asList("Direct", "ByteArray", "OnHeap", "WAL");
    }

    @Parameterized.Parameter public String storeName;

    private Store store;
    private File tmp;

    private Store store() throws Exception {
        switch (storeName) {
            case "Direct" -> { return new StoreDirect(); }
            case "ByteArray" -> { return new StoreByteArray(); }
            case "OnHeap" -> { return new StoreOnHeap(); }
            case "WAL" -> {
                tmp = File.createTempFile("mapdb5-ported-btree", ".wal");
                tmp.delete();
                return new StoreWAL(tmp);
            }
            default -> throw new AssertionError(storeName);
        }
    }

    @After public void tearDown() {
        try { if (store != null && !store.isClosed()) store.close(); } catch (Throwable ignore) {}
        if (tmp != null) { try { tmp.delete(); } catch (Throwable ignore) {} }
    }

    private BTreeMap<Long, Long> longMap(int maxNodeSize) throws Exception {
        store = store();
        return BTreeMap.create(store, LongFormat.INSTANCE, LongFormat.INSTANCE, maxNodeSize);
    }

    private BTreeMap<String, String> stringMap(int maxNodeSize) throws Exception {
        store = store();
        GroupFormat<String> f = new ObjectArrayFormat<>(Serializers.STRING);
        return BTreeMap.create(store, f, f, maxNodeSize);
    }

    // Varied maxNodeSize — the BTreeMap TCK only ever uses 6 and 8. mapdb3
    // BTreeMapTest.large_node_size probes 10/200/6000. Smaller/larger fanouts change the
    // split depth and directory-node shape; this pins insert+get+iteration across them.
    @Test public void varying_maxNodeSize() throws Exception {
        for (int mns : new int[]{4, 5, 16, 33, 128}) {
            BTreeMap<Long, Long> m = longMap(mns);
            final int N = 5000;
            for (long k = 0; k < N; k++) assertNull(m.put(k, k * 3 + 1));
            assertEquals(N, m.sizeLong());
            for (long k = 0; k < N; k++) {
                assertEquals("mns=" + mns + " key=" + k, Long.valueOf(k * 3 + 1), m.get(k));
                assertTrue(m.containsKey(k));
            }
            long prev = Long.MIN_VALUE, count = 0;
            Iterator<Map.Entry<Long, Long>> it = m.entryIterator();
            while (it.hasNext()) {
                Map.Entry<Long, Long> e = it.next();
                assertTrue("order broke mns=" + mns, e.getKey() > prev);
                prev = e.getKey();
                count++;
            }
            assertEquals(N, count);
            store.verify();
            store.close();
            store = null;
        }
    }

    // ported from mapdb1/2 Issue164Test: containsKey on a fresh map is false, then after
    // put the value is retrievable.
    @Test public void issue164_containsKey_then_put_get() throws Exception {
        BTreeMap<String, String> m = stringMap(8);
        assertFalse(m.containsKey("t1"));
        assertNull(m.get("t1"));
        m.put("t1", "value");
        assertTrue(m.containsKey("t1"));
        assertNotNull(m.get("t1"));
        assertEquals("value", m.get("t1"));
        store.verify();
    }

    // ported from mapdb1/2 Issue37Test: 10k puts, iterate — every key exactly once,
    // count == size. (Originally HTreeMap keySet/values/entrySet; folded into one
    // entryIterator pass.)
    @Test public void issue37_iterator_no_duplicate_keys() throws Exception {
        BTreeMap<Long, Long> m = longMap(8);
        final int N = 10_000;
        for (long i = 0; i < N; i++) m.put(i, i);
        assertEquals(N, m.sizeLong());
        java.util.TreeSet<Long> seen = new java.util.TreeSet<>();
        Iterator<Map.Entry<Long, Long>> it = m.entryIterator();
        while (it.hasNext()) {
            Long k = it.next().getKey();
            assertTrue("duplicate key " + k, seen.add(k));
        }
        assertEquals(N, seen.size());
        assertEquals(Long.valueOf(0), seen.first());
        assertEquals(Long.valueOf(N - 1), seen.last());
        store.verify();
    }

    // ported from mapdb1/2 Issue157Test: two threads write the same key range in opposite
    // directions; afterward every key is present with one of the two writers' values (no
    // torn/garbage value). mapdb5's single writer-lock serializes the puts — the property
    // still holds and guards against corruption.
    @Test public void issue157_concurrent_bidirectional_put() throws Exception {
        final BTreeMap<Long, String> m = BTreeMap.create(
                (store = store()), LongFormat.INSTANCE, new ObjectArrayFormat<>(Serializers.STRING), 8);
        final int M = 5000;
        final AtomicReference<Throwable> err = new AtomicReference<>();
        Thread t1 = new Thread(() -> {
            try { for (long i = 0; i <= M; i++) m.put(i, "foo"); }
            catch (Throwable e) { err.set(e); }
        });
        Thread t2 = new Thread(() -> {
            try { for (long i = M; i >= 0; i--) m.put(i, "bar"); }
            catch (Throwable e) { err.set(e); }
        });
        t1.start(); t2.start();
        t1.join(); t2.join();
        if (err.get() != null) throw new AssertionError("writer failed", err.get());

        long count = 0;
        Iterator<Map.Entry<Long, String>> it = m.entryIterator();
        List<Long> keys = new ArrayList<>();
        while (it.hasNext()) {
            Map.Entry<Long, String> e = it.next();
            String v = e.getValue();
            assertTrue("garbage value at " + e.getKey() + ": " + v, "foo".equals(v) || "bar".equals(v));
            keys.add(e.getKey());
            count++;
        }
        assertEquals(M + 1, count);
        for (long i = 0; i <= M; i++) assertTrue("missing key " + i, m.containsKey(i));
        store.verify();
    }

    // ported from mapdb2 IssuesTest.issue581: many threads each put a key then immediately
    // read-your-write it back via containsKey. Guards the root-swing happens-before.
    @Test public void issue581_concurrent_put_then_containsKey() throws Exception {
        final BTreeMap<Long, Long> m = longMap(8);
        final int THREADS = 8;
        final int PER = 1500;
        final AtomicReference<Throwable> err = new AtomicReference<>();
        Thread[] ts = new Thread[THREADS];
        for (int t = 0; t < THREADS; t++) {
            final long base = (long) t * PER;
            ts[t] = new Thread(() -> {
                try {
                    for (long i = 0; i < PER; i++) {
                        long k = base + i;
                        m.put(k, k);
                        if (!m.containsKey(k))
                            throw new AssertionError("read-your-write failed for key " + k);
                    }
                } catch (Throwable e) { err.set(e); }
            });
        }
        for (Thread t : ts) t.start();
        for (Thread t : ts) t.join();
        if (err.get() != null) throw new AssertionError(err.get());
        assertEquals((long) THREADS * PER, m.sizeLong());
        store.verify();
    }

    // ported from mapdb3 BTreeMapTest.testUnicodeCharacterKeyInsertion: non-ASCII keys
    // round-trip (UTF-8 key encoding + comparator).
    @Test public void unicode_keys() throws Exception {
        BTreeMap<String, String> m = stringMap(6);
        String[] keys = {"À", "é", "中文", "😀emoji", "plain", "ÀÀ"};
        for (String k : keys) m.put(k, "v:" + k);
        for (String k : keys) {
            assertTrue(m.containsKey(k));
            assertEquals("v:" + k, m.get(k));
        }
        assertEquals(keys.length, m.sizeLong());
        store.verify();
    }
}
