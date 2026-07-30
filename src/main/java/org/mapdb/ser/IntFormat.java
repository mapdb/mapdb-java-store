package org.mapdb.ser;

import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.util.Arrays;

/**
 * Group format for Integer values backed by int[] (no boxing in the group) and a
 * fixed 4-byte wire stride, giving O(log n) true binary search over serialized
 * bytes via seeking — the 32-bit mirror of {@link LongFormat}.
 *
 * Wire layout for a group of {@code n} elements ({@code n} supplied externally):
 * <pre>
 *   int32 v[0..n-1]     big-endian, ascending (signed int order)
 * </pre>
 * Order is signed {@link Integer#compare}, identical on the object and byte side.
 */
public final class IntFormat implements GroupFormat<Integer> {

    public static final IntFormat INSTANCE = new IntFormat();

    private IntFormat() {}

    @Override public Serializer<Integer> element() { return Serializers.INTEGER; }

    @Override public Object empty() { return new int[0]; }

    @Override public int size(Object group) { return ((int[]) group).length; }

    @Override public Integer get(Object group, int pos) { return ((int[]) group)[pos]; }

    @Override public int search(Object group, Integer key) {
        return Arrays.binarySearch((int[]) group, key);
    }

    @Override public Object insert(Object group, int pos, Integer newValue) {
        int[] g = (int[]) group;
        int[] r = new int[g.length + 1];
        System.arraycopy(g, 0, r, 0, pos);
        r[pos] = newValue;
        System.arraycopy(g, pos, r, pos + 1, g.length - pos);
        return r;
    }

    @Override public Object set(Object group, int pos, Integer newValue) {
        int[] r = ((int[]) group).clone();
        r[pos] = newValue;
        return r;
    }

    @Override public Object delete(Object group, int pos) {
        int[] g = (int[]) group;
        int[] r = new int[g.length - 1];
        System.arraycopy(g, 0, r, 0, pos);
        System.arraycopy(g, pos + 1, r, pos, g.length - pos - 1);
        return r;
    }

    @Override public Object copyRange(Object group, int from, int to) {
        return Arrays.copyOfRange((int[]) group, from, to);
    }

    @Override public Object fromArray(Object[] values) {
        int[] r = new int[values.length];
        for (int i = 0; i < r.length; i++) r[i] = (Integer) values[i];
        return r;
    }

    @Override public void serialize(DataOutput2 out, Object group) {
        for (int v : (int[]) group) out.writeInt(v);
    }

    @Override public Object deserialize(DataInput2 in, int size) {
        int[] r = new int[size];
        for (int i = 0; i < size; i++) r[i] = in.readInt();
        return r;
    }

    @Override public boolean supportsBinary() { return true; }

    @Override public int binarySearch(Integer key, DataInput2 in, int size) {
        int start = in.pos();
        int k = key;
        int lo = 0, hi = size - 1, found = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            in.pos(start + mid * 4);
            int v = in.readInt();
            if (v == k) { found = mid; break; }
            else if (v < k) lo = mid + 1;
            else hi = mid - 1;
        }
        in.pos(start + size * 4);
        return found >= 0 ? found : -(lo + 1);
    }

    @Override public Integer binaryGet(DataInput2 in, int size, int pos) {
        int start = in.pos();
        in.pos(start + pos * 4);
        int v = in.readInt();
        in.pos(start + size * 4);
        return v;
    }
}
