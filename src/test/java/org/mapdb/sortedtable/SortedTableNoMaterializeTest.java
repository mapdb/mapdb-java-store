package org.mapdb.sortedtable;

import org.junit.Test;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;
import org.mapdb.ser.GroupFormat;
import org.mapdb.ser.LongFormat;
import org.mapdb.ser.Serializer;
import org.mapdb.store.Store;
import org.mapdb.store.StoreDirect;

import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * A byte-side {@code get} must answer from the serialized page bytes via
 * {@link GroupFormat#binarySearch}/{@link GroupFormat#binaryGet} and must NOT deserialize the
 * whole page ({@link GroupFormat#deserialize}). Instrumented delegating formats over a
 * {@link StoreDirect} (byte-resident) prove it.
 */
public class SortedTableNoMaterializeTest {

    /** Delegates to {@link LongFormat}, counting the calls that matter. */
    static final class CountingLongFormat implements GroupFormat<Long> {
        final LongFormat d = LongFormat.INSTANCE;
        int deserializeCount, binarySearchCount, binaryGetCount;

        void reset() { deserializeCount = binarySearchCount = binaryGetCount = 0; }

        @Override public Serializer<Long> element() { return d.element(); }
        @Override public Object empty() { return d.empty(); }
        @Override public int size(Object group) { return d.size(group); }
        @Override public Long get(Object group, int pos) { return d.get(group, pos); }
        @Override public int search(Object group, Long key) { return d.search(group, key); }
        @Override public Object insert(Object g, int p, Long v) { return d.insert(g, p, v); }
        @Override public Object set(Object g, int p, Long v) { return d.set(g, p, v); }
        @Override public Object delete(Object g, int p) { return d.delete(g, p); }
        @Override public Object copyRange(Object g, int f, int t) { return d.copyRange(g, f, t); }
        @Override public Object fromArray(Object[] v) { return d.fromArray(v); }
        @Override public void serialize(DataOutput2 out, Object g) { d.serialize(out, g); }
        @Override public Object deserialize(DataInput2 in, int size) { deserializeCount++; return d.deserialize(in, size); }
        @Override public boolean supportsBinary() { return true; }
        @Override public int binarySearch(Long key, DataInput2 in, int size) { binarySearchCount++; return d.binarySearch(key, in, size); }
        @Override public Long binaryGet(DataInput2 in, int size, int pos) { binaryGetCount++; return d.binaryGet(in, size, pos); }
    }

    @Test public void getDoesNotMaterializePages() {
        CountingLongFormat kf = new CountingLongFormat();
        CountingLongFormat vf = new CountingLongFormat();

        TreeMap<Long, Long> t = new TreeMap<>();
        for (long i = 0; i < 400; i++) t.put(i, i * 10);

        Store store = new StoreDirect();
        SortedTableMap.Sink<Long, Long> sink = SortedTableMap.createFromSink(store, kf, vf, 16);
        for (var e : t.entrySet()) sink.put(e.getKey(), e.getValue());
        SortedTableMap<Long, Long> m = SortedTableMap.open(store, sink.create().headerRecid(), kf, vf);

        // ignore the one-time directory materialization at open
        kf.reset();
        vf.reset();

        for (long i = 0; i < 400; i++) assertEquals(Long.valueOf(i * 10), m.get(i));
        assertEquals("also probe absent keys", null, m.get(1000L));

        // page bytes were binary-searched, never deserialized
        assertEquals("no key page materialization", 0, kf.deserializeCount);
        assertEquals("no value page materialization", 0, vf.deserializeCount);
        assertTrue("keys binary-searched", kf.binarySearchCount >= 400);
        assertTrue("values binary-got", vf.binaryGetCount >= 400);
    }
}
