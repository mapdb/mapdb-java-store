package org.mapdb.htree;

import org.junit.Test;
import org.mapdb.hash.Hasher64;
import org.mapdb.hash.Hashers;
import org.mapdb.ser.Serializers;
import org.mapdb.store.Store;
import org.mapdb.store.StoreByteArray;
import org.mapdb.store.StoreOnHeap;

import java.util.HashMap;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Smoke coverage for {@link HTreeMap48} basics; the full store-dialect TCK lives in
 *  HTreeMap48TCK. */
public class HTreeMap48SmokeTest {

    @Test
    public void fuzzWithLong64HasherOnBothStoreKinds() {
        for (Store store : new Store[]{new StoreOnHeap(), new StoreByteArray()}) {
            HTreeMap48<Long, Long> m = HTreeMap48.create(store,
                    Serializers.LONG, Serializers.LONG, Hashers.LONG64);
            HashMap<Long, Long> oracle = new HashMap<>();
            Random rnd = new Random(4848);
            for (int i = 0; i < 30_000; i++) {
                long k = rnd.nextInt(8_000);
                int roll = rnd.nextInt(100);
                if (roll < 50) {
                    long v = rnd.nextLong();
                    assertEquals(oracle.put(k, v), m.put(k, v));
                } else if (roll < 75) {
                    assertEquals(oracle.remove(k), m.remove(k));
                } else {
                    assertEquals(oracle.get(k), m.get(k));
                }
            }
            assertEquals((long) oracle.size(), m.sizeLong());
            for (var e : oracle.entrySet()) assertEquals(e.getValue(), m.get(e.getKey()));
            m.clear();
            assertTrue(m.isEmpty());
            store.verify();
            store.close();
        }
    }

    /** Oversized concShift must be rejected up front: int shifts wrap
     *  mod 32, so e.g. 32+4*4==48 would pass the partition rule and then silently
     *  alias all 2^32 "segments" to segment 0. The cap is 12 (4096 segments). */
    @Test
    public void oversizedConcShiftRejected() {
        Store store = new StoreOnHeap();
        int[][] bad = {{32, 4, 4}, {41, 7, 1}, {13, 5, 7}, {-1, 7, 7}};
        for (int[] g : bad) {
            try {
                HTreeMap48.create(store, Serializers.LONG, Serializers.LONG,
                        g[0], g[1], g[2], null);
                throw new AssertionError("accepted concShift=" + g[0]);
            } catch (IllegalArgumentException expected) { /* ok */ }
        }
        store.close();
    }

    /** Identity hasher steers the full 48-bit budget: keys shifted to the top 48 bits
     *  land in the exact segment/index encoded in the key, including a deeper-than-
     *  32-bit index (bit 40) that a 32-bit map could never reach. */
    @Test
    public void deep48BitIndexResolves() {
        Store store = new StoreOnHeap();
        Hasher64<Long> identity = (k, seed) -> k << 16; // top-48 slice == k
        HTreeMap48<Long, Long> m = HTreeMap48.create(store,
                Serializers.LONG, Serializers.LONG, 6, 6, 7, 0L, identity);

        long[] indices = {0, 1, 1L << 6, 1L << 12, 1L << 40, (1L << 42) - 1, // segment 0
                (3L << 42) | 5, (3L << 42) | (1L << 41)};                    // segment 3
        for (long idx : indices) assertNull(m.put(idx, idx + 7));
        for (long idx : indices) assertEquals(Long.valueOf(idx + 7), m.get(idx));
        assertNull(m.get(2L));
        assertEquals(indices.length, m.sizeLong());
        for (long idx : indices) assertEquals(Long.valueOf(idx + 7), m.remove(idx));
        assertTrue(m.isEmpty());
        store.verify();
        store.close();
    }
}
