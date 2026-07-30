package org.mapdb.ser;

import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.util.Arrays;

/**
 * Binary-capable group format for {@code byte[]} keys.
 * Group is {@code byte[][]}; the byte side binary-searches the serialized form by
 * comparing the probe against stored bytes IN PLACE — zero allocation on the probe
 * path (only {@link #binaryGet} allocates, for its return value).
 *
 * Wire layout for a group of {@code n} elements ({@code n} supplied externally by
 * the node header, exactly like {@link StringGroupFormat}):
 * <pre>
 *   int32 blobLen              total byte length of all elements
 *   int32 off[0..n-1]          start offset of element i within the blob
 *   byte  blob[blobLen]        concatenated bytes; element i spans
 *                              [off[i], (i+1&lt;n ? off[i+1] : blobLen))
 * </pre>
 *
 * ORDER: UNSIGNED lexicographic ({@code memcmp}, {@link Arrays#compareUnsigned}),
 * on BOTH sides — {@code element()} is {@link Serializers#BYTE_ARRAY_UNSIGNED}, not
 * BYTE_ARRAY, whose signed {@code Arrays.compare} order would disagree with the
 * in-place byte comparison the whole format exists for (see the constant's javadoc).
 * The invariant {@code element().compare == search order == binarySearch order}
 * is asserted by the format test battery.
 */
public final class ByteArrayFormat implements GroupFormat<byte[]> {

    public static final ByteArrayFormat INSTANCE = new ByteArrayFormat();

    private static final byte[][] EMPTY = new byte[0][];

    private ByteArrayFormat() {}

    @Override public Serializer<byte[]> element() { return Serializers.BYTE_ARRAY_UNSIGNED; }

    // ---- object side ----

    @Override public Object empty() { return EMPTY; }

    @Override public int size(Object group) { return ((byte[][]) group).length; }

    @Override public byte[] get(Object group, int pos) { return ((byte[][]) group)[pos]; }

    @Override public int search(Object group, byte[] key) {
        byte[][] g = (byte[][]) group;
        int lo = 0, hi = g.length - 1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int c = Arrays.compareUnsigned(g[mid], key);
            if (c == 0) return mid;
            else if (c < 0) lo = mid + 1;
            else hi = mid - 1;
        }
        return -(lo + 1);
    }

    @Override public Object insert(Object group, int pos, byte[] newValue) {
        byte[][] g = (byte[][]) group;
        byte[][] r = new byte[g.length + 1][];
        System.arraycopy(g, 0, r, 0, pos);
        r[pos] = newValue;
        System.arraycopy(g, pos, r, pos + 1, g.length - pos);
        return r;
    }

    @Override public Object set(Object group, int pos, byte[] newValue) {
        byte[][] r = ((byte[][]) group).clone();
        r[pos] = newValue;
        return r;
    }

    @Override public Object delete(Object group, int pos) {
        byte[][] g = (byte[][]) group;
        byte[][] r = new byte[g.length - 1][];
        System.arraycopy(g, 0, r, 0, pos);
        System.arraycopy(g, pos + 1, r, pos, g.length - pos - 1);
        return r;
    }

    @Override public Object copyRange(Object group, int from, int to) {
        return Arrays.copyOfRange((byte[][]) group, from, to);
    }

    @Override public Object fromArray(Object[] values) {
        byte[][] r = new byte[values.length][];
        for (int i = 0; i < r.length; i++) r[i] = (byte[]) values[i];
        return r;
    }

    // ---- wire ----

    @Override public void serialize(DataOutput2 out, Object group) {
        byte[][] g = (byte[][]) group;
        int blobLen = 0;
        for (byte[] b : g) blobLen += b.length;
        out.writeInt(blobLen);
        int off = 0;
        for (byte[] b : g) {
            out.writeInt(off);
            off += b.length;
        }
        for (byte[] b : g) out.write(b);
    }

    @Override public Object deserialize(DataInput2 in, int size) {
        int blobLen = in.readInt();
        int[] off = new int[size];
        for (int i = 0; i < size; i++) off[i] = in.readInt();
        byte[] blob = new byte[blobLen];
        in.readFully(blob);
        byte[][] r = new byte[size][];
        for (int i = 0; i < size; i++) {
            int s = off[i];
            int e = (i + 1 < size) ? off[i + 1] : blobLen;
            r[i] = Arrays.copyOfRange(blob, s, e);
        }
        return r;
    }

    // ---- byte side ----

    @Override public boolean supportsBinary() { return true; }

    @Override public int binarySearch(byte[] key, DataInput2 in, int size) {
        int start = in.pos();
        int blobLen = in.readInt();
        if (blobLen < 0) throw new IllegalStateException("corrupt byte[] group blobLen");
        int offBase = start + 4;
        int blobBase = offBase + size * 4;
        int lo = 0, hi = size - 1, found = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int c = compareStoredTo(in, offBase, blobBase, blobLen, size, mid, key);
            if (c == 0) { found = mid; break; }
            else if (c < 0) lo = mid + 1;
            else hi = mid - 1;
        }
        in.pos(blobBase + blobLen); // leave input at group end
        return found >= 0 ? found : -(lo + 1);
    }

    @Override public byte[] binaryGet(DataInput2 in, int size, int pos) {
        int start = in.pos();
        int blobLen = in.readInt();
        if (blobLen < 0) throw new IllegalStateException("corrupt byte[] group blobLen");
        int offBase = start + 4;
        int blobBase = offBase + size * 4;
        in.pos(offBase + pos * 4);
        int s = in.readInt();
        int e = (pos + 1 < size) ? in.readInt() : blobLen;
        if (s < 0 || e < s || e > blobLen) throw new IllegalStateException("corrupt byte[] group offsets");
        byte[] b = new byte[e - s];
        in.pos(blobBase + s);
        in.readFully(b);
        in.pos(blobBase + blobLen); // leave input at group end
        return b;
    }

    /**
     * Unsigned-lexicographic compare of stored element {@code idx} against the probe,
     * sign convention {@code stored - probe}, comparing in place — no allocation.
     */
    private static int compareStoredTo(DataInput2 in, int offBase, int blobBase, int blobLen,
                                       int size, int idx, byte[] probe) {
        in.pos(offBase + idx * 4);
        int s = in.readInt();
        int e;
        if (idx + 1 < size) e = in.readInt();
        else e = blobLen;
        // serialize() writes 0 <= off[i] <= off[i+1] <= blobLen; anything else is a torn
        // optimistic read or corruption — fail fast, never read from it
        if (s < 0 || e < s || e > blobLen) throw new IllegalStateException("corrupt byte[] group offsets");
        int storedLen = e - s;
        in.pos(blobBase + s);
        int n = Math.min(storedLen, probe.length);
        for (int i = 0; i < n; i++) {
            int c = (in.readByte() & 0xFF) - (probe[i] & 0xFF);
            if (c != 0) return c;
        }
        return storedLen - probe.length;
    }
}
