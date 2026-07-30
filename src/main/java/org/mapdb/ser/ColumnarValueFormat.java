package org.mapdb.ser;

import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.util.Arrays;
import java.util.Comparator;

/**
 * Columnar value {@link GroupFormat}: stores a group of fixed-arity records COLUMN-BY-COLUMN so
 * that a scan over one column reads only that column's contiguous bytes (Arrow-style,
 * scan-friendly). This is a VALUE format — value groups are stored in KEY order, not value-sorted —
 * but it still exposes a coherent total order (lexicographic across columns, column 0 most
 * significant) so point {@link #get}, object-side {@link #search} and byte-side
 * {@link #binarySearch} all agree and it passes the {@link GroupFormat} TCK.
 *
 * <h3>Columns</h3>
 * Each column is a FIXED-WIDTH integral primitive ({@link ColumnType}); the schema is fixed at
 * construction (like {@link TupleFormat}) and is NOT written on the wire. Variable-width columns
 * (byte[]/String, needing per-column offset tables) are a separate, deferred design.
 *
 * <h3>Wire layout (column-major)</h3>
 * For {@code n} rows ({@code n} supplied externally by the node header) and columns of width
 * {@code w0,w1,..}:
 * <pre>
 *   [ col0 : n*w0 bytes ][ col1 : n*w1 bytes ] ... [ col(C-1) : n*w(C-1) bytes ]
 * </pre>
 * Each cell is big-endian. With {@code cumWidth[c] = Σ_{j&lt;c} w_j} and {@code rowWidth = Σ w_j}:
 * <ul>
 *   <li>cell {@code (row i, col c)} lives at {@code start + n*cumWidth[c] + i*w_c},</li>
 *   <li>the group ends at {@code start + n*rowWidth}.</li>
 * </ul>
 * Byte-side operations DECODE each probed cell and compare it with the same signed per-column
 * order as {@link #compare} (raw unsigned byte memcmp would disagree for negative integral values).
 *
 * <h3>Scans</h3>
 * {@link #columnCursor} walks one column's contiguous run — reading only {@code n*w_c} bytes, never
 * the whole {@code n*rowWidth} group (proven by a CountingInput test). {@link #rangeCursor} walks
 * whole rows. Both leave the input at group end on exhaustion.
 */
public final class ColumnarValueFormat implements GroupFormat<Object[]> {

    /** Fixed-width integral column type: big-endian on the wire, signed order. */
    public enum ColumnType {
        LONG(8), INT(4), SHORT(2), BYTE(1);
        private final int width;
        ColumnType(int width) { this.width = width; }
        public int width() { return width; }
    }

    private final ColumnType[] schema;
    private final int[] cumWidth; // length C+1; cumWidth[c] = bytes of columns before c; cumWidth[C] = rowWidth
    private final int rowWidth;
    private final RowSerializer rowSer = new RowSerializer();

    private ColumnarValueFormat(ColumnType[] schema) {
        this.schema = schema;
        this.cumWidth = new int[schema.length + 1];
        for (int c = 0; c < schema.length; c++) cumWidth[c + 1] = cumWidth[c] + schema[c].width();
        this.rowWidth = cumWidth[schema.length];
    }

    /** Build a columnar value format over the given fixed-width columns (arity = length, &gt;=1). */
    public static ColumnarValueFormat of(ColumnType... columns) {
        if (columns == null || columns.length == 0)
            throw new IllegalArgumentException("columnar schema needs >= 1 column");
        for (ColumnType c : columns) if (c == null) throw new IllegalArgumentException("null column type");
        return new ColumnarValueFormat(columns.clone());
    }

    public int columnCount() { return schema.length; }
    public ColumnType columnType(int col) { return schema[col]; }
    public int rowWidth() { return rowWidth; }

    // ---- cell codec (fixed-width big-endian, signed order) ----

    private static void writeCell(DataOutput2 out, ColumnType t, Object v) {
        switch (t) {
            case LONG:  out.writeLong(((Number) v).longValue()); break;
            case INT:   out.writeInt(((Number) v).intValue()); break;
            case SHORT: { int s = ((Number) v).shortValue() & 0xFFFF; out.writeByte(s >>> 8); out.writeByte(s); break; }
            case BYTE:  out.writeByte(((Number) v).byteValue()); break;
            default: throw new AssertionError();
        }
    }

    private static Object readCell(DataInput2 in, ColumnType t) {
        switch (t) {
            case LONG:  return in.readLong();
            case INT:   return in.readInt();
            case SHORT: { int hi = in.readByte() & 0xFF, lo = in.readByte() & 0xFF; return (short) ((hi << 8) | lo); }
            case BYTE:  return in.readByte();
            default: throw new AssertionError();
        }
    }

    private static int compareCell(ColumnType t, Object a, Object b) {
        switch (t) {
            case LONG:  return Long.compare(((Number) a).longValue(), ((Number) b).longValue());
            case INT:   return Integer.compare(((Number) a).intValue(), ((Number) b).intValue());
            case SHORT: return Short.compare(((Number) a).shortValue(), ((Number) b).shortValue());
            case BYTE:  return Byte.compare(((Number) a).byteValue(), ((Number) b).byteValue());
            default: throw new AssertionError();
        }
    }

    // ---- checked seek math (a torn/oversize node must fail fast, never overrun) ----

    private static int seek(int start, long add) {
        long p = (long) start + add;
        if (add < 0 || p < 0 || p > Integer.MAX_VALUE)
            throw new IllegalStateException("columnar seek overflow: start=" + start + " add=" + add);
        return (int) p;
    }

    private int cellOffset(int start, int size, int col, int row) {
        return seek(start, (long) size * cumWidth[col] + (long) row * schema[col].width());
    }

    private int groupEnd(int start, int size) { return seek(start, (long) size * rowWidth); }

    // ---- object side (group == Object[][] rows; defensively cloned at boundaries) ----

    private static Object[][] rows(Object group) { return (Object[][]) group; }

    private void checkRow(Object[] row) {
        if (row.length != schema.length)
            throw new IllegalArgumentException("row arity " + row.length + " != schema " + schema.length);
        for (int c = 0; c < row.length; c++) {
            if (row[c] == null)
                throw new IllegalArgumentException(
                        "null cell at column " + c + ": fixed-width primitive columns are non-null");
        }
    }

    /** Query probes must be full-arity too: an over-long key would silently prefix-match. */
    private void checkProbe(Object[] key) {
        if (key.length != schema.length)
            throw new IllegalArgumentException("probe arity " + key.length + " != schema " + schema.length);
    }

    @Override public Serializer<Object[]> element() { return rowSer; }

    @Override public int compare(Object[] a, Object[] b) {
        checkProbe(a);
        checkProbe(b);
        for (int c = 0; c < schema.length; c++) {
            int cmp = compareCell(schema[c], a[c], b[c]);
            if (cmp != 0) return cmp;
        }
        return 0;
    }

    @Override public Comparator<Object[]> comparator() { return this::compare; }

    @Override public Object empty() { return new Object[0][]; }

    @Override public int size(Object group) { return rows(group).length; }

    @Override public Object[] get(Object group, int pos) { return rows(group)[pos].clone(); }

    @Override public int search(Object group, Object[] key) {
        checkProbe(key); // fail fast even when the group is empty (compare() would not run)
        Object[][] g = rows(group);
        int lo = 0, hi = g.length - 1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int c = compare(g[mid], key);
            if (c == 0) return mid;
            else if (c < 0) lo = mid + 1;
            else hi = mid - 1;
        }
        return -(lo + 1);
    }

    @Override public Object insert(Object group, int pos, Object[] newValue) {
        checkRow(newValue);
        Object[][] g = rows(group);
        Object[][] r = new Object[g.length + 1][];
        System.arraycopy(g, 0, r, 0, pos);
        r[pos] = newValue.clone();
        System.arraycopy(g, pos, r, pos + 1, g.length - pos);
        return r;
    }

    @Override public Object set(Object group, int pos, Object[] newValue) {
        checkRow(newValue);
        Object[][] r = rows(group).clone();
        r[pos] = newValue.clone();
        return r;
    }

    @Override public Object delete(Object group, int pos) {
        Object[][] g = rows(group);
        Object[][] r = new Object[g.length - 1][];
        System.arraycopy(g, 0, r, 0, pos);
        System.arraycopy(g, pos + 1, r, pos, g.length - pos - 1);
        return r;
    }

    @Override public Object copyRange(Object group, int from, int to) {
        return Arrays.copyOfRange(rows(group), from, to);
    }

    @Override public Object fromArray(Object[] values) {
        Object[][] r = new Object[values.length][];
        for (int i = 0; i < values.length; i++) {
            Object[] row = (Object[]) values[i];
            checkRow(row);
            r[i] = row.clone();
        }
        return r;
    }

    // ---- wire (column-major) ----

    @Override public void serialize(DataOutput2 out, Object group) {
        Object[][] g = rows(group);
        for (int c = 0; c < schema.length; c++) {
            ColumnType t = schema[c];
            for (Object[] row : g) writeCell(out, t, row[c]);
        }
    }

    @Override public Object deserialize(DataInput2 in, int size) {
        Object[][] r = new Object[size][];
        for (int i = 0; i < size; i++) r[i] = new Object[schema.length];
        for (int c = 0; c < schema.length; c++) {
            ColumnType t = schema[c];
            for (int i = 0; i < size; i++) r[i][c] = readCell(in, t);
        }
        return r;
    }

    // ---- byte side ----

    @Override public boolean supportsBinary() { return true; }

    @Override public Object[] binaryGet(DataInput2 in, int size, int pos) {
        int start = in.pos();
        Object[] row = new Object[schema.length];
        for (int c = 0; c < schema.length; c++) {
            in.pos(cellOffset(start, size, c, pos));
            row[c] = readCell(in, schema[c]);
        }
        in.pos(groupEnd(start, size));
        return row;
    }

    @Override public int binarySearch(Object[] key, DataInput2 in, int size) {
        checkProbe(key);
        int start = in.pos();
        int lo = 0, hi = size - 1, found = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int c = compareRowAt(in, start, size, mid, key);
            if (c == 0) { found = mid; break; }
            else if (c < 0) lo = mid + 1;
            else hi = mid - 1;
        }
        in.pos(groupEnd(start, size));
        return found >= 0 ? found : -(lo + 1);
    }

    /** Compare stored row {@code row} against the probe, column-by-column, with early exit. */
    private int compareRowAt(DataInput2 in, int start, int size, int row, Object[] key) {
        for (int c = 0; c < schema.length; c++) {
            in.pos(cellOffset(start, size, c, row));
            Object cell = readCell(in, schema[c]);
            int cmp = compareCell(schema[c], cell, key[c]);
            if (cmp != 0) return cmp;
        }
        return 0;
    }

    @Override public GroupCursor<Object[]> rangeCursor(DataInput2 in, int size, int from, int to) {
        if (from < 0 || from > to || to > size)
            throw new IndexOutOfBoundsException("from=" + from + " to=" + to + " size=" + size);
        final int start = in.pos();
        return new GroupCursor<Object[]>() {
            int idx = from - 1;
            Object[] cur;
            boolean exhausted;

            @Override public boolean next() {
                if (exhausted) return false;
                idx++;
                if (idx >= to) {
                    exhausted = true;
                    cur = null;
                    in.pos(groupEnd(start, size));
                    return false;
                }
                Object[] row = new Object[schema.length];
                for (int c = 0; c < schema.length; c++) {
                    in.pos(cellOffset(start, size, c, idx));
                    row[c] = readCell(in, schema[c]);
                }
                cur = row;
                return true;
            }

            @Override public int index() { return idx; }

            @Override public Object[] value() { return cur; }
        };
    }

    /**
     * Cursor over ONE column's values for rows {@code [from, to)}: reads only that column's
     * contiguous byte run ({@code n*w_col} bytes), never the whole {@code n*rowWidth} group — the
     * columnar scan win. On exhaustion the input is left at group end (so it composes with byte-side
     * parsing of following fields).
     */
    public GroupCursor<Object> columnCursor(DataInput2 in, int size, int col, int from, int to) {
        if (col < 0 || col >= schema.length)
            throw new IndexOutOfBoundsException("col=" + col + " columns=" + schema.length);
        if (from < 0 || from > to || to > size)
            throw new IndexOutOfBoundsException("from=" + from + " to=" + to + " size=" + size);
        final int start = in.pos();
        final ColumnType t = schema[col];
        return new GroupCursor<Object>() {
            int idx = from - 1;
            Object cur;
            boolean exhausted;

            @Override public boolean next() {
                if (exhausted) return false;
                idx++;
                if (idx >= to) {
                    exhausted = true;
                    cur = null;
                    in.pos(groupEnd(start, size));
                    return false;
                }
                in.pos(cellOffset(start, size, col, idx));
                cur = readCell(in, t);
                return true;
            }

            @Override public int index() { return idx; }

            @Override public Object value() { return cur; }
        };
    }

    /** Standalone single-row codec (row-major fixed-width cells): the {@link #element()} serializer. */
    private final class RowSerializer implements Serializer<Object[]> {
        @Override public void serialize(DataOutput2 out, Object[] value) {
            checkRow(value);
            for (int c = 0; c < schema.length; c++) writeCell(out, schema[c], value[c]);
        }

        @Override public Object[] deserialize(DataInput2 in, int size) {
            Object[] row = new Object[schema.length];
            for (int c = 0; c < schema.length; c++) row[c] = readCell(in, schema[c]);
            return row;
        }

        @Override public int fixedSize() { return rowWidth; }

        @Override public int compare(Object[] a, Object[] b) { return ColumnarValueFormat.this.compare(a, b); }

        @Override public boolean equals(Object[] a, Object[] b) {
            checkProbe(a); // consistent with compare(): malformed arity is a caller bug, not "unequal"
            checkProbe(b);
            for (int c = 0; c < schema.length; c++)
                if (compareCell(schema[c], a[c], b[c]) != 0) return false;
            return true;
        }

        @Override public boolean equalsBySerializedBytes() { return true; } // fixed-width big-endian is canonical
        @Override public boolean naturalOrder() { return false; }           // rows are not natural-Comparable
    }
}
