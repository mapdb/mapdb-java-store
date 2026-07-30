package org.mapdb.btree;

import org.junit.Test;
import org.mapdb.ser.LongFormat;
import org.mapdb.store.StoreDirect;

import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Fast smoke coverage for {@link BufferTreeMap}; the deep TCK lives in BufferTreeMapTCK. */
public class BufferTreeSmokeTest {

    private static BufferTreeMap<Long, Long> map(int maxNodeSize, int bufferBytes) {
        return BufferTreeMap.create(new StoreDirect(false),
                LongFormat.INSTANCE, LongFormat.INSTANCE, maxNodeSize, bufferBytes);
    }

    @Test public void putGetRemove() {
        BufferTreeMap<Long, Long> m = map(8, 64);
        assertNull(m.get(1L));
        m.put(1L, 100L);
        assertEquals((Long) 100L, m.get(1L));
        m.put(1L, 200L); // LWW overwrite in buffer
        assertEquals((Long) 200L, m.get(1L));
        m.remove(1L);
        assertNull(m.get(1L));
        assertFalse(m.containsKey(1L));
        m.put(1L, 300L); // re-insert after tombstone
        assertEquals((Long) 300L, m.get(1L));
    }

    @Test public void fuzzAgainstTreeMap() {
        BufferTreeMap<Long, Long> m = map(8, 96); // tiny node+buffer: constant flush/split churn
        TreeMap<Long, Long> oracle = new TreeMap<>();
        Random rnd = new Random(42);
        for (int i = 0; i < 200_000; i++) {
            long k = rnd.nextInt(5_000);
            if (rnd.nextInt(100) < 65) {
                long v = rnd.nextLong();
                m.put(k, v);
                oracle.put(k, v);
            } else {
                m.remove(k);
                oracle.remove(k);
            }
            if ((i & 8191) == 8191) {
                // spot gets + full ordered comparison
                for (int probe = 0; probe < 50; probe++) {
                    long pk = rnd.nextInt(5_000);
                    assertEquals("get(" + pk + ") @op " + i, oracle.get(pk), m.get(pk));
                }
                assertOrderedEquals(oracle, m);
            }
        }
        m.flushAll();
        assertOrderedEquals(oracle, m);
        assertEquals(oracle.size(), m.sizeLong());
    }

    private static void assertOrderedEquals(TreeMap<Long, Long> oracle, BufferTreeMap<Long, Long> m) {
        Iterator<Map.Entry<Long, Long>> a = oracle.entrySet().iterator();
        Iterator<Map.Entry<Long, Long>> b = m.entryIterator();
        while (a.hasNext()) {
            assertTrue("map iterator ended early", b.hasNext());
            Map.Entry<Long, Long> ea = a.next(), eb = b.next();
            assertEquals(ea.getKey(), eb.getKey());
            assertEquals(ea.getValue(), eb.getValue());
        }
        assertFalse("map iterator has extra entries", b.hasNext());
    }
}
