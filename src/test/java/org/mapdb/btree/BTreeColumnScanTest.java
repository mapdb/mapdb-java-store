package org.mapdb.btree;

import org.junit.Test;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;
import org.mapdb.ser.ColumnarValueFormat;
import org.mapdb.ser.LongFormat;
import org.mapdb.ser.Serializer;
import org.mapdb.store.RecordRead;
import org.mapdb.store.Store;
import org.mapdb.store.StoreByteArray;
import org.mapdb.store.StoreDirect;
import org.mapdb.store.StoreOnHeap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.TreeMap;
import java.util.function.BiConsumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mapdb.ser.ColumnarValueFormat.ColumnType.BYTE;
import static org.mapdb.ser.ColumnarValueFormat.ColumnType.INT;
import static org.mapdb.ser.ColumnarValueFormat.ColumnType.LONG;

/**
 * Covers {@link BTreeMap#forEachValueColumn}: the narrow columnar single-column scan API.
 * Proves (a) the serialized byte path reads ONLY the requested column's bytes (a byte-counting
 * store), (b) results match ordinary {@code get(key)[column]} across store dialects, bounds and
 * inclusivity, and (c) the guard rails (non-columnar format / bad column / null consumer).
 */
public class BTreeColumnScanTest {

    // schema whose scanned column (BYTE) is a tiny fraction of the 33-byte row
    private static final ColumnarValueFormat WIDE =
            ColumnarValueFormat.of(LONG, LONG, LONG, LONG, BYTE); // rowWidth = 33
    private static final ColumnarValueFormat NARROW =
            ColumnarValueFormat.of(LONG, LONG, INT);              // rowWidth = 20

    private static Object[] wideRow(long k) {
        return new Object[]{ k, k * 3, ~k, k << 1, (byte) k };
    }

    private static Object[] narrowRow(Random rnd, long k) {
        return new Object[]{ k, rnd.nextLong(), rnd.nextInt() };
    }

    // ---- (a) byte path reads only the requested column -----------------------------

    @Test
    public void byteStore_readsOnlyRequestedColumnBytes() {
        CountingByteStore store = new CountingByteStore();
        int n = 40;                         // fits a single root leaf (maxNodeSize 64)
        BTreeMap<Long, Object[]> m = BTreeMap.create(store, LongFormat.INSTANCE, WIDE, 64);
        for (long k = 0; k < n; k++) m.put(k, wideRow(k));

        List<Map.Entry<Long, Object>> got = new ArrayList<>();
        store.bytesRead = 0;                // measure ONLY the scan's leaf reads
        m.forEachValueColumn(null, true, null, true, 4, (k, v) -> got.add(entry(k, v)));

        // correctness: every key with its BYTE column
        assertEquals(n, got.size());
        for (int i = 0; i < n; i++) {
            assertEquals(Long.valueOf(i), got.get(i).getKey());
            assertEquals((byte) i, got.get(i).getValue());
        }

        long keyBytes = (long) n * 8;                 // n LONG keys, decoded for output
        long colBytes = (long) n * 1;                 // BYTE column: one byte per row
        long fullValueGroup = (long) n * WIDE.rowWidth();
        assertTrue("scan (" + store.bytesRead + ") must read far below key+whole-value-group ("
                        + (keyBytes + fullValueGroup) + ")",
                store.bytesRead < keyBytes + fullValueGroup);
        assertTrue("scan (" + store.bytesRead + ") must read only header + keys + ONE column ("
                        + (keyBytes + colBytes) + " + small header)",
                store.bytesRead <= keyBytes + colBytes + 16);
        store.close();
    }

    // ---- (b) results match get()[column] across dialects, bounds, inclusivity -------

    @Test public void directStore_matchesControl()  { matchesControl(new StoreDirect(), 2000, 16); }
    @Test public void byteArrayStore_matchesControl(){ matchesControl(new StoreByteArray(), 1500, 12); }
    @Test public void heapStore_matchesControl()     { matchesControl(new StoreOnHeap(), 800, 8); }

    private void matchesControl(Store store, int n, int maxNodeSize) {
        Random rnd = new Random(0xC0FFEEL ^ n);
        BTreeMap<Long, Object[]> m = BTreeMap.create(store, LongFormat.INSTANCE, NARROW, maxNodeSize);
        TreeMap<Long, Object[]> control = new TreeMap<>();
        for (int i = 0; i < n; i++) {
            long k = i * 3L;                                   // gaps -> absent probe keys exist
            Object[] row = narrowRow(rnd, k);
            m.put(k, row);
            control.put(k, row);
        }

        long lastKey = control.lastKey();
        long[] bounds = { Long.MIN_VALUE, -1, 0, 3, 4, n / 2L * 3, lastKey, lastKey + 1 };
        for (int col = 0; col < NARROW.columnCount(); col++) {
            // open range
            assertScanMatches(m, control, col, null, true, null, true);
            for (long lo : bounds) {
                for (long hi : bounds) {
                    if (lo > hi) continue;
                    for (boolean li : new boolean[]{true, false}) {
                        for (boolean hj : new boolean[]{true, false}) {
                            assertScanMatches(m, control, col, lo, li, hi, hj);
                        }
                    }
                }
            }
            // half-open on each side
            assertScanMatches(m, control, col, null, true, lastKey, false);
            assertScanMatches(m, control, col, 0L, false, null, true);
        }
        store.close();
    }

    private void assertScanMatches(BTreeMap<Long, Object[]> m, TreeMap<Long, Object[]> control,
                                   int col, Long lo, boolean li, Long hi, boolean hj) {
        List<Map.Entry<Long, Object>> got = new ArrayList<>();
        m.forEachValueColumn(lo, li, hi, hj, col, (k, v) -> got.add(entry(k, v)));

        List<Map.Entry<Long, Object>> exp = new ArrayList<>();
        for (Map.Entry<Long, Object[]> e : control.entrySet()) {
            long k = e.getKey();
            if (lo != null) { int c = Long.compare(k, lo); if (c < 0 || (c == 0 && !li)) continue; }
            if (hi != null) { int c = Long.compare(k, hi); if (c > 0 || (c == 0 && !hj)) continue; }
            exp.add(entry(k, e.getValue()[col]));
        }

        String label = "col=" + col + " lo=" + lo + "/" + li + " hi=" + hi + "/" + hj;
        assertEquals(label + " size", exp.size(), got.size());
        for (int i = 0; i < exp.size(); i++) {
            assertEquals(label + " key@" + i, exp.get(i).getKey(), got.get(i).getKey());
            assertEquals(label + " val@" + i, exp.get(i).getValue(), got.get(i).getValue());
            if (i > 0) assertTrue(label + " ascending", got.get(i - 1).getKey() < got.get(i).getKey());
        }
    }

    // ---- (c) guard rails -----------------------------------------------------------

    @Test
    public void nonColumnarValueFormat_throws() {
        StoreOnHeap store = new StoreOnHeap();
        BTreeMap<Long, Long> m = BTreeMap.create(store, LongFormat.INSTANCE, LongFormat.INSTANCE, 8);
        m.put(1L, 10L);
        try {
            m.forEachValueColumn(null, true, null, true, 0, (k, v) -> fail("must not emit"));
            fail("expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) { /* ok */ }
        store.close();
    }

    @Test
    public void invalidColumn_and_nullConsumer_throw() {
        StoreOnHeap store = new StoreOnHeap();
        BTreeMap<Long, Object[]> m = BTreeMap.create(store, LongFormat.INSTANCE, NARROW, 8);
        m.put(1L, narrowRow(new Random(1), 1L));
        for (int badCol : new int[]{-1, 3}) {
            try { m.forEachValueColumn(null, true, null, true, badCol, (k, v) -> { }); fail("col " + badCol); }
            catch (IndexOutOfBoundsException e) { /* ok */ }
        }
        try { m.forEachValueColumn(null, true, null, true, 0, null); fail("null consumer"); }
        catch (NullPointerException e) { /* ok */ }
        store.close();
    }

    @Test
    public void emptyMap_scanEmitsNothing() {
        StoreDirect store = new StoreDirect();
        BTreeMap<Long, Object[]> m = BTreeMap.create(store, LongFormat.INSTANCE, NARROW, 8);
        m.forEachValueColumn(null, true, null, true, 1, (k, v) -> fail("empty map must not emit"));
        store.close();
    }

    // ---- helpers -------------------------------------------------------------------

    private static Map.Entry<Long, Object> entry(Long k, Object v) {
        return new AbstractMapEntry(k, v);
    }

    private static final class AbstractMapEntry implements Map.Entry<Long, Object> {
        final Long k; final Object v;
        AbstractMapEntry(Long k, Object v) { this.k = k; this.v = v; }
        @Override public Long getKey() { return k; }
        @Override public Object getValue() { return v; }
        @Override public Object setValue(Object value) { throw new UnsupportedOperationException(); }
    }

    /**
     * Minimal single-threaded {@link Store} that keeps one {@code byte[]} per record and hands the
     * push-down {@link Store#read} a {@link CountingInput}, tallying bytes actually read (seeks are
     * free) — so a test can prove {@link BTreeMap#forEachValueColumn} touches only one column.
     */
    private static final class CountingByteStore implements Store {
        private final HashMap<Long, byte[]> recs = new HashMap<>();
        private long seq = 1;              // recid 0 is the universal "no link" sentinel
        long bytesRead;
        private boolean closed;

        @Override public long preallocate() { long r = seq++; recs.put(r, null); return r; }

        @Override public <R> long put(R record, Serializer<R> serializer) {
            long r = seq++;
            recs.put(r, serialize(record, serializer));
            return r;
        }

        @Override public <R> void update(long recid, R record, Serializer<R> serializer) {
            recs.put(recid, serialize(record, serializer));
        }

        @Override public <R> R get(long recid, Serializer<R> serializer) {
            byte[] b = recs.get(recid);
            return b == null ? null : serializer.deserialize(new DataInput2.ByteArray(b, 0), b.length);
        }

        @Override public long read(long recid, RecordRead action) {
            byte[] b = recs.get(recid);
            if (b == null) return action.onNull();
            CountingInput in = new CountingInput(b);
            long r = action.onBytes(in, b.length);
            bytesRead += in.bytesRead;
            return r;
        }

        @Override public <R> boolean compareAndSwap(long recid, R expected, R next, Serializer<R> ser) {
            R cur = get(recid, ser);
            if (!ser.equals(cur, expected)) return false;
            update(recid, next, ser);
            return true;
        }

        @Override public <R> void delete(long recid, Serializer<R> serializer) { recs.remove(recid); }
        @Override public void commit() { }
        @Override public void close() { closed = true; }
        @Override public boolean isClosed() { return closed; }
        @Override public void verify() { }
        @Override public PrimitiveIterator.OfLong getAllRecids() {
            return recs.keySet().stream().mapToLong(Long::longValue).iterator();
        }

        private static <R> byte[] serialize(R record, Serializer<R> serializer) {
            DataOutput2 out = new DataOutput2();
            serializer.serialize(out, record);
            return out.copyBytes();
        }
    }

    /** DataInput2 that tallies bytes actually read; pos() seeks are free (like the columnar proof). */
    private static final class CountingInput extends DataInput2 {
        final byte[] buf;
        int pos;
        long bytesRead;
        CountingInput(byte[] buf) { this.buf = buf; }
        @Override public int pos() { return pos; }
        @Override public void pos(int pos) { this.pos = pos; }
        @Override public byte readByte() { bytesRead++; return buf[pos++]; }
        @Override public void readFully(byte[] b, int off, int len) {
            System.arraycopy(buf, pos, b, off, len); pos += len; bytesRead += len;
        }
        @Override public void skipBytes(int n) { pos += n; }
    }
}
