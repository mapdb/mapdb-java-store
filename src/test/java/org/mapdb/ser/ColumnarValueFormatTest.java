package org.mapdb.ser;

import org.junit.Test;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;
import org.mapdb.ser.ColumnarValueFormat.ColumnType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mapdb.ser.ColumnarValueFormat.ColumnType.BYTE;
import static org.mapdb.ser.ColumnarValueFormat.ColumnType.INT;
import static org.mapdb.ser.ColumnarValueFormat.ColumnType.LONG;
import static org.mapdb.ser.ColumnarValueFormat.ColumnType.SHORT;

/**
 * Binds the {@link GroupFormat} TCK to {@link ColumnarValueFormat} (schema {LONG,LONG,INT},
 * column 0 = the sort key so rows order lexicographically by v), then adds columnar-specific
 * coverage: column-major wire layout, single-column {@link ColumnarValueFormat#columnCursor}
 * scans vs the object side across schemas that include SHORT/BYTE, a CountingInput proof that a
 * column scan does NOT materialize the whole group, and edge groups.
 */
public class ColumnarValueFormatTest extends GroupFormatTCK<Object[]> {

    private static final long SENT = 0x600DF00DCAFEBABEL;

    private final ColumnarValueFormat fmt = ColumnarValueFormat.of(LONG, LONG, INT);

    @Override protected GroupFormat<Object[]> format() { return fmt; }

    /** col0 = v (the lexicographic sort key), col1/col2 arbitrary but in-domain. */
    @Override protected Object[] gen(long v) {
        return new Object[]{ v, (v * 2654435761L) ^ 0x5DEECE66DL, (int) (v & 0x7FFFFFFFL) };
    }

    @Override protected Object view(Object[] v) { return Arrays.deepToString(v); }

    // ---- basic capability + accessors --------------------------------------------

    @Test
    public void supportsBinaryAndRangeCursor() {
        assertTrue(fmt.supportsBinary());
        assertTrue(fmt.supportsRangeCursor());
        assertEquals(3, fmt.columnCount());
        assertEquals(LONG, fmt.columnType(0));
        assertEquals(INT, fmt.columnType(2));
        assertEquals(8 + 8 + 4, fmt.rowWidth());
    }

    @Test
    public void factory_rejectsEmptyOrNullSchema() {
        try { ColumnarValueFormat.of(); fail(); } catch (IllegalArgumentException e) { /* ok */ }
        try { ColumnarValueFormat.of((ColumnType) null); fail(); } catch (IllegalArgumentException e) { /* ok */ }
    }

    // ---- wire layout: exactly n*rowWidth, column-major -----------------------------

    @Test
    public void wire_isExactlyRowWidthTimesN_andColumnMajor() {
        int n = 5;
        Object[] rows = new Object[n];
        for (int i = 0; i < n; i++) rows[i] = gen((i + 1) * 10L);
        Object g = fmt.fromArray(rows);
        DataOutput2 out = new DataOutput2();
        fmt.serialize(out, g);
        assertEquals("column-major group is exactly n*rowWidth bytes", n * fmt.rowWidth(), out.pos);

        // column 0 (LONG) is the first n*8 bytes: read them directly and match get(i)[0]
        DataInput2 in = new DataInput2.ByteArray(out.copyBytes(), 0);
        for (int i = 0; i < n; i++) assertEquals(fmt.get(g, i)[0], in.readLong());
    }

    // ---- order coherence: compare == search == binarySearch ------------------------

    @Test
    public void order_compare_search_binarySearch_agree_onEqualPrefixRows() {
        // rows share col0/col1 and differ only in col2 → exercises multi-column lexicographic order
        ColumnarValueFormat f = ColumnarValueFormat.of(LONG, INT);
        Object[][] pool = {
                {1L, -5}, {1L, 0}, {1L, 7},
                {2L, Integer.MIN_VALUE}, {2L, Integer.MAX_VALUE},
                {5L, 3},
        };
        Object g = f.fromArray(pool);
        DataOutput2 out = new DataOutput2();
        f.serialize(out, g);
        out.writeLong(SENT);
        byte[] bytes = out.copyBytes();

        List<Object[]> probes = new ArrayList<>(Arrays.asList(pool));
        probes.add(new Object[]{0L, 0});          // below all
        probes.add(new Object[]{1L, 5});          // gap within col0==1
        probes.add(new Object[]{2L, 0});          // gap within col0==2
        probes.add(new Object[]{9L, 0});          // above all
        for (Object[] probe : probes) {
            int obj = f.search(g, probe);
            DataInput2 in = new DataInput2.ByteArray(bytes, 0);
            assertEquals("binarySearch vs search for " + Arrays.toString(probe),
                    obj, f.binarySearch(probe, in, pool.length));
            assertEquals("input at group end", SENT, in.readLong());
        }
    }

    /** col0 spans negative..positive: locks the SIGNED (not unsigned byte memcmp) decode/compare. */
    @Test
    public void order_signed_firstColumnNegative() {
        ColumnarValueFormat f = ColumnarValueFormat.of(LONG, INT);
        Object[][] pool = { // strictly ascending under signed lexicographic order
                {Long.MIN_VALUE, 0}, {-1000L, 5}, {-1L, Integer.MIN_VALUE}, {-1L, 7},
                {0L, -3}, {0L, 4}, {1L, 0}, {Long.MAX_VALUE, Integer.MAX_VALUE},
        };
        for (int i = 1; i < pool.length; i++)
            assertTrue("pool must be sorted at " + i, f.compare(pool[i - 1], pool[i]) < 0);
        Object g = f.fromArray(pool);
        DataOutput2 out = new DataOutput2();
        f.serialize(out, g);
        out.writeLong(SENT);
        byte[] bytes = out.copyBytes();

        List<Object[]> probes = new ArrayList<>(Arrays.asList(pool));
        probes.add(new Object[]{Long.MIN_VALUE, -1});   // below all
        probes.add(new Object[]{-500L, 0});             // between negatives
        probes.add(new Object[]{-1L, 0});               // gap within col0==-1
        probes.add(new Object[]{2L, 0});                // above all
        for (Object[] probe : probes) {
            int obj = f.search(g, probe);
            DataInput2 in = new DataInput2.ByteArray(bytes, 0);
            assertEquals("binarySearch vs search for " + Arrays.toString(probe),
                    obj, f.binarySearch(probe, in, pool.length));
            assertEquals(SENT, in.readLong());
        }
        // column-0 scan returns the negative sort keys in order
        DataInput2 in = new DataInput2.ByteArray(bytes, 0);
        GroupCursor<Object> c = f.columnCursor(in, pool.length, 0, 0, pool.length);
        for (Object[] row : pool) { assertTrue(c.next()); assertEquals(row[0], c.value()); }
        assertFalse(c.next());
        assertEquals(SENT, in.readLong());
    }

    // ---- column scan == object side, across schemas incl. SHORT/BYTE ---------------

    private static final ColumnarValueFormat WIDE = ColumnarValueFormat.of(LONG, INT, SHORT, BYTE);

    private static Object[] randRow(ColumnarValueFormat f, Random rnd) {
        Object[] row = new Object[f.columnCount()];
        for (int c = 0; c < f.columnCount(); c++) {
            switch (f.columnType(c)) {
                case LONG:  row[c] = rnd.nextLong(); break;
                case INT:   row[c] = rnd.nextInt(); break;
                case SHORT: row[c] = (short) rnd.nextInt(); break;
                case BYTE:  row[c] = (byte) rnd.nextInt(); break;
                default: throw new AssertionError();
            }
        }
        return row;
    }

    private static int[][] scanRanges(int n, Random rnd) {
        if (n == 0) return new int[][]{{0, 0}};
        return new int[][]{
                {0, n}, {0, n / 2}, {n / 2, n},
                {n / 2, n / 2}, {n, n},
                {rnd.nextInt(n), 0}, // fixed below into a valid [lo,hi]
        };
    }

    @Test
    public void columnCursor_matchesObjectSide_allColumnsAndRanges() {
        ColumnarValueFormat f = WIDE;
        Random rnd = new Random(0xC01DBEEFL);
        for (int round = 0; round < 50; round++) {
            int n = rnd.nextInt(60);
            Object[] rows = new Object[n];
            for (int i = 0; i < n; i++) rows[i] = randRow(f, rnd);
            Object g = f.fromArray(rows);
            DataOutput2 out = new DataOutput2();
            f.serialize(out, g);
            out.writeLong(SENT);
            byte[] bytes = out.copyBytes();

            for (int col = 0; col < f.columnCount(); col++) {
                for (int[] r0 : scanRanges(n, rnd)) {
                    int from = Math.min(r0[0], r0[1]), to = Math.max(r0[0], r0[1]);
                    DataInput2 in = new DataInput2.ByteArray(bytes, 0);
                    GroupCursor<Object> c = f.columnCursor(in, n, col, from, to);
                    int exp = from;
                    while (c.next()) {
                        assertEquals(exp, c.index());
                        assertEquals("col " + col + " row " + exp, f.get(g, exp)[col], c.value());
                        exp++;
                    }
                    assertEquals(to, exp);
                    assertFalse(c.next());
                    assertEquals("column cursor leaves input at group end", SENT, in.readLong());
                }
            }
        }
    }

    @Test
    public void columnCursor_invalidArgs_throw() {
        ColumnarValueFormat f = WIDE;
        Object[] rows = { randRow(f, new Random(1)), randRow(f, new Random(2)) };
        DataOutput2 out = new DataOutput2();
        f.serialize(out, f.fromArray(rows));
        byte[] bytes = out.copyBytes();
        for (int badCol : new int[]{-1, 4}) {
            try { f.columnCursor(new DataInput2.ByteArray(bytes, 0), 2, badCol, 0, 2); fail(); }
            catch (IndexOutOfBoundsException e) { /* ok */ }
        }
        for (int[] bad : new int[][]{{-1, 1}, {1, 0}, {0, 3}}) {
            try { f.columnCursor(new DataInput2.ByteArray(bytes, 0), 2, 0, bad[0], bad[1]); fail(); }
            catch (IndexOutOfBoundsException e) { /* ok */ }
        }
    }

    // ---- no-materialization proof: a column scan reads only that column's bytes ----

    @Test
    public void columnCursor_doesNotMaterializeWholeGroup() {
        ColumnarValueFormat f = WIDE; // rowWidth = 8+4+2+1 = 15
        int n = 2000;
        Object[] rows = new Object[n];
        Random rnd = new Random(7);
        for (int i = 0; i < n; i++) rows[i] = randRow(f, rnd);
        DataOutput2 out = new DataOutput2();
        f.serialize(out, f.fromArray(rows));
        byte[] bytes = out.copyBytes();
        int groupBytes = n * f.rowWidth();

        // scan the BYTE column (width 1): reads exactly n bytes, far below the whole 15n group
        CountingInput in = new CountingInput(bytes, 0);
        GroupCursor<Object> c = f.columnCursor(in, n, 3, 0, n); // col 3 == BYTE
        long cells = 0;
        while (c.next()) cells++;
        assertEquals(n, cells);
        assertEquals("BYTE-column scan reads exactly one byte per row", (long) n, in.bytesRead);
        assertTrue("column scan (" + in.bytesRead + ") must be far below whole group (" + groupBytes + ")",
                in.bytesRead < groupBytes / 2);

        // the LONG column (width 8) reads 8n, still well under 15n
        CountingInput in2 = new CountingInput(bytes, 0);
        GroupCursor<Object> c2 = f.columnCursor(in2, n, 0, 0, n);
        while (c2.next()) { /* drain */ }
        assertEquals((long) n * 8, in2.bytesRead);
        assertTrue(in2.bytesRead < groupBytes);
    }

    // ---- edge groups ---------------------------------------------------------------

    @Test
    public void edge_emptyGroup_cursorsReturnImmediatelyAtGroupEnd() {
        ColumnarValueFormat f = WIDE;
        Object g = f.empty();
        assertEquals(0, f.size(g));
        DataOutput2 out = new DataOutput2();
        f.serialize(out, g);
        assertEquals("empty columnar group is 0 bytes", 0, out.pos);
        out.writeLong(SENT);
        byte[] b = out.copyBytes();

        DataInput2 in = new DataInput2.ByteArray(b, 0);
        GroupCursor<Object[]> rc = f.rangeCursor(in, 0, 0, 0);
        assertFalse(rc.next());
        assertEquals(SENT, in.readLong());

        in = new DataInput2.ByteArray(b, 0);
        GroupCursor<Object> cc = f.columnCursor(in, 0, 2, 0, 0);
        assertFalse(cc.next());
        assertEquals(SENT, in.readLong());
    }

    @Test
    public void edge_singleRow_singleColumn() {
        ColumnarValueFormat f = ColumnarValueFormat.of(LONG);
        Object g = f.fromArray(new Object[]{ new Object[]{42L} });
        assertEquals(1, f.size(g));
        assertArrayEquals(new Object[]{42L}, f.get(g, 0));

        DataOutput2 out = new DataOutput2();
        f.serialize(out, g);
        out.writeLong(SENT);
        byte[] b = out.copyBytes();

        // point get + binarySearch from bytes
        DataInput2 in = new DataInput2.ByteArray(b, 0);
        assertArrayEquals(new Object[]{42L}, f.binaryGet(in, 1, 0));
        assertEquals(SENT, in.readLong());
        in = new DataInput2.ByteArray(b, 0);
        assertEquals(0, f.binarySearch(new Object[]{42L}, in, 1));
        assertEquals(SENT, in.readLong());
        in = new DataInput2.ByteArray(b, 0);
        assertEquals(-1, f.binarySearch(new Object[]{7L}, in, 1)); // below → insertion 0
        assertEquals(SENT, in.readLong());

        // column cursor over the sole column
        in = new DataInput2.ByteArray(b, 0);
        GroupCursor<Object> c = f.columnCursor(in, 1, 0, 0, 1);
        assertTrue(c.next());
        assertEquals(42L, c.value());
        assertFalse(c.next());
        assertEquals(SENT, in.readLong());
    }

    // ---- fuzz: columnar build round-trips vs a materialized control -----------------

    @Test
    public void fuzz_serializeRoundTrip_vsMaterializedControl() {
        ColumnarValueFormat f = WIDE;
        Random rnd = new Random(0x5CA1AB1EL);
        for (int round = 0; round < 60; round++) {
            int n = rnd.nextInt(80);
            Object[] control = new Object[n];               // the materialized rows (control)
            for (int i = 0; i < n; i++) control[i] = randRow(f, rnd);
            Object g = f.fromArray(control);

            DataOutput2 out = new DataOutput2();
            f.serialize(out, g);
            assertEquals(n * f.rowWidth(), out.pos);
            byte[] bytes = out.copyBytes();

            // object-side deserialize matches control
            Object back = f.deserialize(new DataInput2.ByteArray(bytes, 0), n);
            for (int i = 0; i < n; i++) assertArrayEquals("row " + i, (Object[]) control[i], f.get(back, i));

            // byte-side binaryGet matches control at every position
            for (int i = 0; i < n; i++) {
                DataInput2 in = new DataInput2.ByteArray(bytes, 0);
                assertArrayEquals("binaryGet row " + i, (Object[]) control[i], f.binaryGet(in, n, i));
            }
        }
    }

    /** DataInput2 that tallies bytes actually read (seeks via pos() are free). */
    private static final class CountingInput extends DataInput2 {
        final byte[] buf;
        int pos;
        long bytesRead;
        CountingInput(byte[] buf, int pos) { this.buf = buf; this.pos = pos; }
        @Override public int pos() { return pos; }
        @Override public void pos(int pos) { this.pos = pos; }
        @Override public byte readByte() { bytesRead++; return buf[pos++]; }
        @Override public void readFully(byte[] b, int off, int len) {
            System.arraycopy(buf, pos, b, off, len); pos += len; bytesRead += len;
        }
        @Override public void skipBytes(int n) { pos += n; }
    }
}
