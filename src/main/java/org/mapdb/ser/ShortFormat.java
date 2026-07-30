package org.mapdb.ser;

import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.util.Arrays;

/**
 * Group format for Short values backed by short[] (no boxing in the group) and a
 * fixed 2-byte wire stride, giving O(log n) true binary search over serialized
 * bytes via seeking — the 16-bit mirror of {@link IntFormat}.
 *
 * Wire layout for a group of {@code n} elements ({@code n} supplied externally):
 * <pre>
 *   int16 v[0..n-1]     big-endian, ascending (signed short order)
 * </pre>
 * Order is signed {@link Short#compare}, identical on the object and byte side. The
 * stored bytes are read back as a SIGN-EXTENDED 16-bit value and compared as signed
 * ints, so the negative half sorts before the non-negative half — unlike
 * {@link CharFormat}, whose 16-bit values are unsigned.
 */
public final class ShortFormat implements GroupFormat<Short> {

    public static final ShortFormat INSTANCE = new ShortFormat();

    private ShortFormat() {}

    @Override public Serializer<Short> element() { return Serializers.SHORT; }

    @Override public Object empty() { return new short[0]; }

    @Override public int size(Object group) { return ((short[]) group).length; }

    @Override public Short get(Object group, int pos) { return ((short[]) group)[pos]; }

    @Override public int search(Object group, Short key) {
        return Arrays.binarySearch((short[]) group, key);
    }

    @Override public Object insert(Object group, int pos, Short newValue) {
        short[] g = (short[]) group;
        short[] r = new short[g.length + 1];
        System.arraycopy(g, 0, r, 0, pos);
        r[pos] = newValue;
        System.arraycopy(g, pos, r, pos + 1, g.length - pos);
        return r;
    }

    @Override public Object set(Object group, int pos, Short newValue) {
        short[] r = ((short[]) group).clone();
        r[pos] = newValue;
        return r;
    }

    @Override public Object delete(Object group, int pos) {
        short[] g = (short[]) group;
        short[] r = new short[g.length - 1];
        System.arraycopy(g, 0, r, 0, pos);
        System.arraycopy(g, pos + 1, r, pos, g.length - pos - 1);
        return r;
    }

    @Override public Object copyRange(Object group, int from, int to) {
        return Arrays.copyOfRange((short[]) group, from, to);
    }

    @Override public Object fromArray(Object[] values) {
        short[] r = new short[values.length];
        for (int i = 0; i < r.length; i++) r[i] = (Short) values[i];
        return r;
    }

    @Override public void serialize(DataOutput2 out, Object group) {
        for (short v : (short[]) group) {
            out.writeByte(v >>> 8);
            out.writeByte(v);
        }
    }

    @Override public Object deserialize(DataInput2 in, int size) {
        short[] r = new short[size];
        for (int i = 0; i < size; i++) {
            int hi = in.readUnsignedByte();
            int lo = in.readUnsignedByte();
            r[i] = (short) ((hi << 8) | lo);
        }
        return r;
    }

    @Override public boolean supportsBinary() { return true; }

    @Override public int binarySearch(Short key, DataInput2 in, int size) {
        int start = in.pos();
        int k = key; // sign-extended to int; signed compare
        int lo = 0, hi = size - 1, found = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            in.pos(start + mid * 2);
            int v = (short) ((in.readUnsignedByte() << 8) | in.readUnsignedByte()); // sign-extended
            if (v == k) { found = mid; break; }
            else if (v < k) lo = mid + 1;
            else hi = mid - 1;
        }
        in.pos(start + size * 2);
        return found >= 0 ? found : -(lo + 1);
    }

    @Override public Short binaryGet(DataInput2 in, int size, int pos) {
        int start = in.pos();
        in.pos(start + pos * 2);
        short v = (short) ((in.readUnsignedByte() << 8) | in.readUnsignedByte());
        in.pos(start + size * 2);
        return v;
    }
}
