package org.mapdb.sortedtable;

import org.junit.Test;
import org.mapdb.DBException;
import org.mapdb.ser.LongFormat;
import org.mapdb.store.Store;
import org.mapdb.store.StoreDirect;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Correctness of {@link SortedTableMap} vs a control {@link TreeMap}. Runs over an in-memory
 * {@link StoreDirect} (byte-resident records → the {@code onBytes} push-down path); the
 * {@link SortedTableMapOnHeapTest} subclass re-runs everything over a heap store (the
 * {@code onObject} path). Small page sizes are used deliberately to exercise page boundaries.
 */
public class SortedTableMapTest {

    /** Overridden by the on-heap subclass. */
    protected Store newStore() { return new StoreDirect(); }

    private SortedTableMap<Long, Long> build(int entriesPerPage, TreeMap<Long, Long> src) {
        Store store = newStore();
        SortedTableMap.Sink<Long, Long> sink =
                SortedTableMap.createFromSink(store, LongFormat.INSTANCE, LongFormat.INSTANCE, entriesPerPage);
        for (Map.Entry<Long, Long> e : src.entrySet()) sink.put(e.getKey(), e.getValue());
        return sink.create();
    }

    private static TreeMap<Long, Long> seq(int n) {
        TreeMap<Long, Long> t = new TreeMap<>();
        for (int i = 0; i < n; i++) t.put((long) i * 2, (long) i * 100); // even keys 0,2,4,...
        return t;
    }

    // ------------------------------------------------------------------ basic

    @Test public void emptyMap() {
        SortedTableMap<Long, Long> m = build(4, new TreeMap<>());
        assertTrue(m.isEmpty());
        assertEquals(0, m.size());
        assertEquals(0L, m.sizeLong());
        assertNull(m.get(1L));
        assertFalse(m.containsKey(1L));
        assertFalse(m.entryIterator().hasNext());
        assertNull(m.firstEntry());
        assertNull(m.lastEntry());
        try { m.firstKey(); fail(); } catch (NoSuchElementException expected) {}
        try { m.lastKey(); fail(); } catch (NoSuchElementException expected) {}
    }

    @Test public void singleEntry() {
        TreeMap<Long, Long> t = new TreeMap<>();
        t.put(5L, 50L);
        SortedTableMap<Long, Long> m = build(4, t);
        assertEquals(1, m.size());
        assertEquals(Long.valueOf(50L), m.get(5L));
        assertNull(m.get(4L));
        assertNull(m.get(6L));
        assertEquals(Long.valueOf(5L), m.firstKey());
        assertEquals(Long.valueOf(5L), m.lastKey());
        assertEquals(Long.valueOf(5L), m.floorKey(6L));
        assertEquals(Long.valueOf(5L), m.ceilingKey(4L));
        assertNull(m.lowerKey(5L));
        assertNull(m.higherKey(5L));
    }

    @Test public void getAcrossPageBoundaries() {
        // page size 4; 25 entries => 7 pages (last partial). Probe every gap and edge.
        TreeMap<Long, Long> t = seq(25);
        for (int eps : new int[]{1, 3, 4, 5, 8, 25, 26}) {
            SortedTableMap<Long, Long> m = build(eps, t);
            assertEquals(t.size(), m.size());
            for (long k = -2; k <= 52; k++) {
                assertEquals("eps=" + eps + " k=" + k, t.get(k), m.get(k));
                assertEquals("eps=" + eps + " k=" + k, t.containsKey(k), m.containsKey(k));
            }
            assertEquals(t.firstKey(), m.firstKey());
            assertEquals(t.lastKey(), m.lastKey());
        }
    }

    @Test public void rejectsNonAscending() {
        Store store = newStore();
        SortedTableMap.Sink<Long, Long> sink =
                SortedTableMap.createFromSink(store, LongFormat.INSTANCE, LongFormat.INSTANCE, 4);
        sink.put(1L, 1L);
        sink.put(5L, 5L);
        try { sink.put(5L, 9L); fail("duplicate must be rejected"); } catch (DBException.NotSorted expected) {}
        try { sink.put(3L, 9L); fail("descending must be rejected"); } catch (DBException.NotSorted expected) {}
    }

    // ------------------------------------------------------------------ navigation

    @Test public void navigationVsTreeMap() {
        TreeMap<Long, Long> t = seq(50); // keys 0..98 even
        SortedTableMap<Long, Long> m = build(4, t);
        for (long k = -2; k <= 100; k++) {
            assertEquals("floor " + k, t.floorEntry(k), m.floorEntry(k));
            assertEquals("ceiling " + k, t.ceilingEntry(k), m.ceilingEntry(k));
            assertEquals("lower " + k, t.lowerEntry(k), m.lowerEntry(k));
            assertEquals("higher " + k, t.higherEntry(k), m.higherEntry(k));
            assertEquals("floorKey " + k, t.floorKey(k), m.floorKey(k));
            assertEquals("ceilingKey " + k, t.ceilingKey(k), m.ceilingKey(k));
            assertEquals("lowerKey " + k, t.lowerKey(k), m.lowerKey(k));
            assertEquals("higherKey " + k, t.higherKey(k), m.higherKey(k));
        }
        assertEquals(t.firstEntry(), m.firstEntry());
        assertEquals(t.lastEntry(), m.lastEntry());
    }

    // ------------------------------------------------------------------ iteration

    private static <K, V> List<Map.Entry<K, V>> drain(Iterator<Map.Entry<K, V>> it) {
        List<Map.Entry<K, V>> out = new ArrayList<>();
        while (it.hasNext()) out.add(it.next());
        return out;
    }

    @Test public void forwardAndBackwardIteration() {
        for (int n : new int[]{0, 1, 4, 5, 8, 9, 17}) {
            TreeMap<Long, Long> t = seq(n);
            SortedTableMap<Long, Long> m = build(4, t);
            // ascending
            assertEquals(new ArrayList<>(t.entrySet()), drain(m.entryIterator()));
            // descending via descendingMap
            List<Map.Entry<Long, Long>> desc = drain(m.descendingMap().entrySet().iterator());
            List<Map.Entry<Long, Long>> expDesc = drain(t.descendingMap().entrySet().iterator());
            assertEquals(expDesc, desc);
        }
    }

    @Test public void boundedSubMapsAllInclusivity() {
        TreeMap<Long, Long> t = seq(20); // 0..38 even, page size 4
        SortedTableMap<Long, Long> m = build(4, t);
        long[] probes = {-1, 0, 1, 7, 8, 15, 16, 38, 39};
        for (long lo : probes) {
            for (long hi : probes) {
                if (lo > hi) continue;
                for (boolean li : new boolean[]{true, false}) {
                    for (boolean hin : new boolean[]{true, false}) {
                        NavigableMap<Long, Long> exp = t.subMap(lo, li, hi, hin);
                        NavigableMap<Long, Long> got = m.subMap(lo, li, hi, hin);
                        String msg = "sub " + lo + li + hi + hin;
                        assertEquals(msg, new ArrayList<>(exp.entrySet()), drain(got.entrySet().iterator()));
                        // descending over same bounds
                        assertEquals(msg + " desc",
                                drain(exp.descendingMap().entrySet().iterator()),
                                drain(got.descendingMap().entrySet().iterator()));
                        assertEquals(msg + " size", exp.size(), got.size());
                    }
                }
            }
        }
    }

    @Test public void headTailMaps() {
        TreeMap<Long, Long> t = seq(15);
        SortedTableMap<Long, Long> m = build(4, t);
        for (long k = -1; k <= 30; k++) {
            for (boolean inc : new boolean[]{true, false}) {
                assertEquals("head " + k + inc,
                        new ArrayList<>(t.headMap(k, inc).entrySet()),
                        drain(m.headMap(k, inc).entrySet().iterator()));
                assertEquals("tail " + k + inc,
                        new ArrayList<>(t.tailMap(k, inc).entrySet()),
                        drain(m.tailMap(k, inc).entrySet().iterator()));
            }
        }
    }

    @Test public void descendingSubMap() {
        TreeMap<Long, Long> t = seq(30);
        SortedTableMap<Long, Long> m = build(5, t);
        NavigableMap<Long, Long> tv = t.descendingMap().subMap(50L, true, 10L, false);
        NavigableMap<Long, Long> mv = m.descendingMap().subMap(50L, true, 10L, false);
        assertEquals(new ArrayList<>(tv.entrySet()), drain(mv.entrySet().iterator()));
        assertEquals(tv.firstKey(), mv.firstKey());
        assertEquals(tv.lastKey(), mv.lastKey());
    }

    // ------------------------------------------------------------------ immutability

    @Test public void mutatorsThrow() {
        SortedTableMap<Long, Long> m = build(4, seq(10));
        assertThrowsUOE(() -> m.put(1L, 1L));
        assertThrowsUOE(() -> m.remove(0L));
        assertThrowsUOE(() -> m.clear());
        assertThrowsUOE(() -> m.pollFirstEntry());
        assertThrowsUOE(() -> m.pollLastEntry());
        assertThrowsUOE(() -> m.putAll(new TreeMap<>(seq(2))));
        // through views (advance first so remove() reaches the adapter, not IllegalStateException)
        assertThrowsUOE(() -> { Iterator<Map.Entry<Long, Long>> it = m.entrySet().iterator(); it.next(); it.remove(); });
        assertThrowsUOE(() -> m.descendingMap().put(1L, 1L));
        assertThrowsUOE(() -> m.subMap(0L, 4L).clear());
    }

    private interface Run { void run(); }
    private static void assertThrowsUOE(Run r) {
        try { r.run(); fail("expected UnsupportedOperationException"); }
        catch (UnsupportedOperationException expected) {}
    }

    // ------------------------------------------------------------------ fuzz

    @Test public void fuzzVsTreeMap() {
        Random rnd = new Random(42);
        TreeMap<Long, Long> t = new TreeMap<>();
        int n = 5000;
        long k = -1000;
        for (int i = 0; i < n; i++) {
            k += 1 + rnd.nextInt(5); // strictly ascending, irregular gaps
            t.put(k, rnd.nextLong());
        }
        SortedTableMap<Long, Long> m = build(7, t);

        assertEquals(t.size(), m.size());
        assertEquals(new ArrayList<>(t.entrySet()), drain(m.entryIterator()));
        assertEquals(drain(t.descendingMap().entrySet().iterator()),
                drain(m.descendingMap().entrySet().iterator()));
        assertEquals(t.firstKey(), m.firstKey());
        assertEquals(t.lastKey(), m.lastKey());

        long minK = t.firstKey(), maxK = t.lastKey();
        for (int i = 0; i < 3000; i++) {
            long q = minK - 10 + (long) (rnd.nextDouble() * (maxK - minK + 20));
            assertEquals("get " + q, t.get(q), m.get(q));
            assertEquals("floor " + q, t.floorEntry(q), m.floorEntry(q));
            assertEquals("ceiling " + q, t.ceilingEntry(q), m.ceilingEntry(q));
            assertEquals("lower " + q, t.lowerEntry(q), m.lowerEntry(q));
            assertEquals("higher " + q, t.higherEntry(q), m.higherEntry(q));
        }
        // random bounded ranges
        for (int i = 0; i < 300; i++) {
            long a = minK - 5 + (long) (rnd.nextDouble() * (maxK - minK + 10));
            long b = a + (long) (rnd.nextDouble() * (maxK - minK) / 4);
            boolean ai = rnd.nextBoolean(), bi = rnd.nextBoolean();
            if (a > b) { long tmp = a; a = b; b = tmp; }
            assertEquals("range " + a + "," + b,
                    new ArrayList<>(t.subMap(a, ai, b, bi).entrySet()),
                    drain(m.subMap(a, ai, b, bi).entrySet().iterator()));
        }
    }
}
