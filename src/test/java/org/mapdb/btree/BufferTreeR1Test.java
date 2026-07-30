package org.mapdb.btree;

import org.junit.Test;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;
import org.mapdb.ser.GroupFormat;
import org.mapdb.ser.LongFormat;
import org.mapdb.ser.Serializer;
import org.mapdb.store.StoreByteArray;
import org.mapdb.store.StoreDelta;
import org.mapdb.store.StoreDirect;

import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * R1-specific behavior tests for {@link BufferTreeMap} on the {@link org.mapdb.format.BufferedPageFormat}
 * op-tail: true newest-first reverse point read (LWW), the byte-side bounded range cursor that does NOT
 * materialize the leaf base, inline consolidation on append refusal, and range results vs a TreeMap oracle.
 */
public class BufferTreeR1Test {

    // ---- 1. newest-first reverse point read is last-writer-wins (PUT/DELETE) ----
    @Test public void reverseLwwPointRead() {
        BufferTreeMap<Long, Long> m = BufferTreeMap.create(new StoreDirect(false),
                LongFormat.INSTANCE, LongFormat.INSTANCE, 8, 512);
        m.putOnly(5L, 10L);
        m.putOnly(5L, 20L);
        assertEquals((Long) 20L, m.get(5L));
        m.removeOnly(5L);
        assertNull(m.get(5L));
        m.putOnly(5L, 30L);
        assertEquals((Long) 30L, m.get(5L));
        // interleave other keys so the op tail has several frames to reverse-scan past
        for (long k = 100; k < 140; k++) m.putOnly(k, k);
        m.putOnly(5L, 40L);
        assertEquals((Long) 40L, m.get(5L));
        assertNull(m.get(999L));
    }

    // ---- 2. bounded range read answers a single-leaf map WITHOUT materializing the base ----
    @Test public void rangeReadNoBaseMaterialization() {
        CountingLongFormat keyF = new CountingLongFormat();
        CountingLongFormat valF = new CountingLongFormat();
        // large node/buffer so 12 keys stay one leaf; flushAll folds ops into the leaf base.
        BufferTreeMap<Long, Long> m = BufferTreeMap.create(new StoreDirect(false), keyF, valF, 64, 4096, 4096);
        for (long k = 0; k < 12; k++) m.putOnly(k, k * 100);
        m.flushAll(); // all entries now live in the consolidated leaf BASE (empty op tail)

        keyF.reset();
        valF.reset();
        TreeMap<Long, Long> got = new TreeMap<>();
        for (Iterator<Map.Entry<Long, Long>> it = m.entryIterator(3L, true, 8L, true); it.hasNext(); ) {
            Map.Entry<Long, Long> e = it.next();
            got.put(e.getKey(), e.getValue());
        }
        // correctness
        TreeMap<Long, Long> oracle = new TreeMap<>();
        for (long k = 3; k <= 8; k++) oracle.put(k, k * 100);
        assertEquals(oracle, got);
        // PROOF: the range read used byte-side binaryGet only — the key/value GROUPS were never
        // deserialized (materialized) as a whole during the query.
        assertEquals("key group must not be materialized on a range read", 0, keyF.groupDeserialize);
        assertEquals("value group must not be materialized on a range read", 0, valF.groupDeserialize);
        assertTrue("byte-side base cursor was used (binaryGet)", keyF.binaryGets > 0 && valF.binaryGets > 0);
    }

    // ---- 3. descending byte-side range on a single leaf ----
    @Test public void descendingByteRange() {
        BufferTreeMap<Long, Long> m = BufferTreeMap.create(new StoreDirect(false),
                LongFormat.INSTANCE, LongFormat.INSTANCE, 64, 4096, 4096);
        for (long k = 0; k < 10; k++) m.putOnly(k, k);
        m.putOnly(4L, 444L); // shadow via the log (unflushed)
        m.removeOnly(6L);
        StringBuilder sb = new StringBuilder();
        for (Iterator<Map.Entry<Long, Long>> it = m.descendingEntryIterator(2L, true, 7L, true); it.hasNext(); ) {
            Map.Entry<Long, Long> e = it.next();
            sb.append(e.getKey()).append('=').append(e.getValue()).append(' ');
        }
        assertEquals("7=7 5=5 4=444 3=3 2=2 ", sb.toString());
    }

    // ---- 4. inline consolidation on append refusal keeps data intact (tiny buffers → flush/split) ----
    @Test public void consolidationOnRefusal() {
        for (StoreDelta store : new StoreDelta[]{new StoreByteArray(), new StoreDirect(false)}) {
            // leafHeadroom 0 + tiny dir buffer → nearly every append is REFUSED, forcing inline
            // flush/consolidate/split on the hot path.
            BufferTreeMap<Long, Long> m = BufferTreeMap.create(store, LongFormat.INSTANCE, LongFormat.INSTANCE, 4, 48, 0);
            TreeMap<Long, Long> oracle = new TreeMap<>();
            Random rnd = new Random(42);
            for (int i = 0; i < 3000; i++) {
                long k = rnd.nextInt(1500);
                if (rnd.nextInt(4) == 0) { oracle.remove(k); m.remove(k); }
                else { long v = rnd.nextLong(); oracle.put(k, v); m.put(k, v); }
            }
            assertEquals(oracle.size(), (int) m.sizeLong());
            for (Map.Entry<Long, Long> e : oracle.entrySet()) assertEquals(e.getValue(), m.get(e.getKey()));
            // pruned range read across a multi-level tree matches the oracle
            assertRangeMatches(oracle, m, 200L, 800L);
            assertRangeMatches(oracle, m, null, 100L);
            assertRangeMatches(oracle, m, 1400L, null);
        }
    }

    // ---- 5. pruned range == oracle across many bound shapes (multi-level tree) ----
    @Test public void prunedRangeMatchesOracleManyBounds() {
        BufferTreeMap<Long, Long> m = BufferTreeMap.create(new StoreDirect(false),
                LongFormat.INSTANCE, LongFormat.INSTANCE, 6, 96);
        TreeMap<Long, Long> oracle = new TreeMap<>();
        Random rnd = new Random(7);
        for (int i = 0; i < 2000; i++) { long k = rnd.nextInt(1000); long v = rnd.nextLong(); oracle.put(k, v); m.put(k, v); }
        long[] bounds = {-5, 0, 1, 250, 499, 500, 501, 750, 999, 1000};
        for (long lo : bounds) {
            for (long hi : bounds) {
                if (lo > hi) continue;
                for (boolean li : new boolean[]{true, false}) {
                    for (boolean hii : new boolean[]{true, false}) {
                        assertRangeExact(oracle, m, lo, li, hi, hii);
                    }
                }
            }
        }
    }

    private static void assertRangeMatches(TreeMap<Long, Long> oracle, BufferTreeMap<Long, Long> m, Long lo, Long hi) {
        TreeMap<Long, Long> exp = new TreeMap<>();
        for (Map.Entry<Long, Long> e : oracle.entrySet()) {
            if (lo != null && e.getKey() < lo) continue;
            if (hi != null && e.getKey() > hi) continue;
            exp.put(e.getKey(), e.getValue());
        }
        TreeMap<Long, Long> got = new TreeMap<>();
        for (Iterator<Map.Entry<Long, Long>> it = m.entryIterator(lo, true, hi, true); it.hasNext(); ) {
            Map.Entry<Long, Long> e = it.next();
            got.put(e.getKey(), e.getValue());
        }
        assertEquals(exp, got);
    }

    private static void assertRangeExact(TreeMap<Long, Long> oracle, BufferTreeMap<Long, Long> m,
                                         long lo, boolean li, long hi, boolean hii) {
        java.util.NavigableMap<Long, Long> sub = oracle.subMap(lo, li, hi, hii);
        // ascending
        Iterator<Map.Entry<Long, Long>> exp = sub.entrySet().iterator();
        Iterator<Map.Entry<Long, Long>> got = m.entryIterator(lo, li, hi, hii);
        while (exp.hasNext()) {
            assertTrue("map short on [" + lo + li + "," + hi + hii + "]", got.hasNext());
            assertEquals(exp.next(), got.next());
        }
        assertFalse("map long", got.hasNext());
        // descending
        Iterator<Map.Entry<Long, Long>> dexp = sub.descendingMap().entrySet().iterator();
        Iterator<Map.Entry<Long, Long>> dgot = m.descendingEntryIterator(lo, li, hi, hii);
        while (dexp.hasNext()) {
            assertTrue(dgot.hasNext());
            assertEquals(dexp.next(), dgot.next());
        }
        assertFalse(dgot.hasNext());
    }

    /** LongFormat that counts whole-group deserialize (materialization) and byte-side binaryGet. */
    private static final class CountingLongFormat implements GroupFormat<Long> {
        final GroupFormat<Long> d = LongFormat.INSTANCE;
        int groupDeserialize = 0, binaryGets = 0;

        void reset() { groupDeserialize = 0; binaryGets = 0; }

        @Override public Serializer<Long> element() { return d.element(); }
        @Override public Object empty() { return d.empty(); }
        @Override public int size(Object group) { return d.size(group); }
        @Override public Long get(Object group, int pos) { return d.get(group, pos); }
        @Override public int search(Object group, Long key) { return d.search(group, key); }
        @Override public int compare(Long a, Long b) { return d.compare(a, b); }
        @Override public Object insert(Object group, int pos, Long v) { return d.insert(group, pos, v); }
        @Override public Object set(Object group, int pos, Long v) { return d.set(group, pos, v); }
        @Override public Object delete(Object group, int pos) { return d.delete(group, pos); }
        @Override public Object copyRange(Object group, int from, int to) { return d.copyRange(group, from, to); }
        @Override public Object fromArray(Object[] values) { return d.fromArray(values); }
        @Override public void serialize(DataOutput2 out, Object group) { d.serialize(out, group); }
        @Override public Object deserialize(DataInput2 in, int size) { groupDeserialize++; return d.deserialize(in, size); }
        @Override public boolean supportsBinary() { return true; }
        @Override public int binarySearch(Long key, DataInput2 in, int size) { return d.binarySearch(key, in, size); }
        @Override public Long binaryGet(DataInput2 in, int size, int pos) { binaryGets++; return d.binaryGet(in, size, pos); }
    }
}
