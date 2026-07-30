package org.mapdb.ser;

import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.util.Arrays;

/**
 * Group format for Long values backed by long[] (no boxing in the group) and a
 * fixed 8-byte wire stride, giving O(log n) true binary search over serialized
 * bytes via seeking.
 */
public final class LongFormat implements GroupFormat<Long> {

    public static final LongFormat INSTANCE = new LongFormat();

    private LongFormat() {}

    @Override public Serializer<Long> element() { return Serializers.LONG; }

    @Override public Object empty() { return new long[0]; }

    @Override public int size(Object group) { return ((long[]) group).length; }

    @Override public Long get(Object group, int pos) { return ((long[]) group)[pos]; }

    @Override public int search(Object group, Long key) {
        return Arrays.binarySearch((long[]) group, key);
    }

    @Override public Object insert(Object group, int pos, Long newValue) {
        long[] g = (long[]) group;
        long[] r = new long[g.length + 1];
        System.arraycopy(g, 0, r, 0, pos);
        r[pos] = newValue;
        System.arraycopy(g, pos, r, pos + 1, g.length - pos);
        return r;
    }

    @Override public Object set(Object group, int pos, Long newValue) {
        long[] r = ((long[]) group).clone();
        r[pos] = newValue;
        return r;
    }

    @Override public Object delete(Object group, int pos) {
        long[] g = (long[]) group;
        long[] r = new long[g.length - 1];
        System.arraycopy(g, 0, r, 0, pos);
        System.arraycopy(g, pos + 1, r, pos, g.length - pos - 1);
        return r;
    }

    @Override public Object copyRange(Object group, int from, int to) {
        return Arrays.copyOfRange((long[]) group, from, to);
    }

    @Override public Object fromArray(Object[] values) {
        long[] r = new long[values.length];
        for (int i = 0; i < r.length; i++) r[i] = (Long) values[i];
        return r;
    }

    @Override public void serialize(DataOutput2 out, Object group) {
        for (long v : (long[]) group) out.writeLong(v);
    }

    @Override public Object deserialize(DataInput2 in, int size) {
        long[] r = new long[size];
        for (int i = 0; i < size; i++) r[i] = in.readLong();
        return r;
    }

    @Override public boolean supportsBinary() { return true; }

    @Override public int binarySearch(Long key, DataInput2 in, int size) {
        int start = in.pos();
        long k = key;
        int lo = 0, hi = size - 1, found = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            in.pos(start + mid * 8);
            long v = in.readLong();
            if (v == k) { found = mid; break; }
            else if (v < k) lo = mid + 1;
            else hi = mid - 1;
        }
        in.pos(start + size * 8);
        return found >= 0 ? found : -(lo + 1);
    }

    @Override public Long binaryGet(DataInput2 in, int size, int pos) {
        int start = in.pos();
        in.pos(start + pos * 8);
        long v = in.readLong();
        in.pos(start + size * 8);
        return v;
    }
}
