package org.mapdb.ser;

import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.util.Arrays;

/**
 * Group format for Character values backed by char[] (no boxing in the group) and a
 * fixed 2-byte wire stride, giving O(log n) true binary search over serialized bytes
 * via seeking — the unsigned 16-bit sibling of {@link ShortFormat}.
 *
 * Wire layout for a group of {@code n} elements ({@code n} supplied externally):
 * <pre>
 *   uint16 v[0..n-1]    big-endian, ascending (unsigned char order)
 * </pre>
 * {@code char} is an UNSIGNED 16-bit type: its natural order ({@link Character#compare},
 * i.e. plain unsigned 0..65535) equals the big-endian 2-byte wire order DIRECTLY, with
 * NO sign flip. This differs from {@link ShortFormat}, whose signed 16-bit values put the
 * negative half first. On the byte side we read each pair as a zero-extended value in
 * {@code 0..65535} and compare as ints, exactly reproducing {@code Character.compare}.
 */
public final class CharFormat implements GroupFormat<Character> {

    public static final CharFormat INSTANCE = new CharFormat();

    private CharFormat() {}

    @Override public Serializer<Character> element() { return Serializers.CHAR; }

    @Override public Object empty() { return new char[0]; }

    @Override public int size(Object group) { return ((char[]) group).length; }

    @Override public Character get(Object group, int pos) { return ((char[]) group)[pos]; }

    @Override public int search(Object group, Character key) {
        return Arrays.binarySearch((char[]) group, key);
    }

    @Override public Object insert(Object group, int pos, Character newValue) {
        char[] g = (char[]) group;
        char[] r = new char[g.length + 1];
        System.arraycopy(g, 0, r, 0, pos);
        r[pos] = newValue;
        System.arraycopy(g, pos, r, pos + 1, g.length - pos);
        return r;
    }

    @Override public Object set(Object group, int pos, Character newValue) {
        char[] r = ((char[]) group).clone();
        r[pos] = newValue;
        return r;
    }

    @Override public Object delete(Object group, int pos) {
        char[] g = (char[]) group;
        char[] r = new char[g.length - 1];
        System.arraycopy(g, 0, r, 0, pos);
        System.arraycopy(g, pos + 1, r, pos, g.length - pos - 1);
        return r;
    }

    @Override public Object copyRange(Object group, int from, int to) {
        return Arrays.copyOfRange((char[]) group, from, to);
    }

    @Override public Object fromArray(Object[] values) {
        char[] r = new char[values.length];
        for (int i = 0; i < r.length; i++) r[i] = (Character) values[i];
        return r;
    }

    @Override public void serialize(DataOutput2 out, Object group) {
        for (char v : (char[]) group) {
            out.writeByte(v >>> 8);
            out.writeByte(v);
        }
    }

    @Override public Object deserialize(DataInput2 in, int size) {
        char[] r = new char[size];
        for (int i = 0; i < size; i++) {
            int hi = in.readUnsignedByte();
            int lo = in.readUnsignedByte();
            r[i] = (char) ((hi << 8) | lo);
        }
        return r;
    }

    @Override public boolean supportsBinary() { return true; }

    @Override public int binarySearch(Character key, DataInput2 in, int size) {
        int start = in.pos();
        int k = key; // 0..65535, unsigned
        int lo = 0, hi = size - 1, found = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            in.pos(start + mid * 2);
            int v = (in.readUnsignedByte() << 8) | in.readUnsignedByte(); // zero-extended, 0..65535
            if (v == k) { found = mid; break; }
            else if (v < k) lo = mid + 1;
            else hi = mid - 1;
        }
        in.pos(start + size * 2);
        return found >= 0 ? found : -(lo + 1);
    }

    @Override public Character binaryGet(DataInput2 in, int size, int pos) {
        int start = in.pos();
        in.pos(start + pos * 2);
        char v = (char) ((in.readUnsignedByte() << 8) | in.readUnsignedByte());
        in.pos(start + size * 2);
        return v;
    }
}
