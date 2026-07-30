package org.mapdb.ser;

import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Binary-capable group format for {@code String}.
 *
 * The generic {@link ObjectArrayFormat} declares {@code supportsBinary()==false}, so a
 * point read has to deserialize the whole key group — all ~maxNodeSize strings — on
 * every lookup (measured: 13 KB allocated per String get on StoreDirect). This format
 * serializes the group as a length-prefixed blob with a FIXED-WIDTH per-element offset
 * index, so a read can binary-search directly over the serialized bytes: seek to the
 * offset of element {@code mid}, decode only that one, and never materialize the group.
 * Reads touch O(log n) elements instead of n.
 *
 * Wire layout for a group of {@code n} elements ({@code n} is supplied externally by the
 * node header, exactly like {@link LongFormat}):
 * <pre>
 *   int32 blobLen              total UTF-8 byte length of all elements
 *   int32 off[0..n-1]          start offset of element i within the blob
 *   byte  blob[blobLen]        concatenated UTF-8 bytes; element i spans
 *                              [off[i], (i+1&lt;n ? off[i+1] : blobLen))
 * </pre>
 * The fixed-width offset table makes {@code off[mid]} addressable at
 * {@code groupStart + 4 + mid*4} with no decode loop — the positional-access capability
 * of §5 generalized from fixed stride to variable width. Elements are stored whole (each
 * is its own restart point, R7 with interval K=1: no cross-element prefix compression),
 * and ordering is {@link String#compareTo}, matched exactly on the byte side by
 * {@link Utf8#compareUtf8} comparing the stored UTF-8 in place — the probe path of
 * {@link #binarySearch} allocates nothing; only {@link #binaryGet} materializes a String.
 */
public final class StringGroupFormat implements GroupFormat<String> {

    public static final StringGroupFormat INSTANCE = new StringGroupFormat();

    private static final String[] EMPTY = new String[0];

    private StringGroupFormat() {}

    @Override public Serializer<String> element() { return Serializers.STRING; }

    // ---- object side ----

    @Override public Object empty() { return EMPTY; }

    @Override public int size(Object group) { return ((String[]) group).length; }

    @Override public String get(Object group, int pos) { return ((String[]) group)[pos]; }

    @Override public int search(Object group, String key) {
        String[] g = (String[]) group;
        int lo = 0, hi = g.length - 1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int c = g[mid].compareTo(key);
            if (c == 0) return mid;
            else if (c < 0) lo = mid + 1;
            else hi = mid - 1;
        }
        return -(lo + 1);
    }

    @Override public Object insert(Object group, int pos, String newValue) {
        String[] g = (String[]) group;
        String[] r = new String[g.length + 1];
        System.arraycopy(g, 0, r, 0, pos);
        r[pos] = newValue;
        System.arraycopy(g, pos, r, pos + 1, g.length - pos);
        return r;
    }

    @Override public Object set(Object group, int pos, String newValue) {
        String[] r = ((String[]) group).clone();
        r[pos] = newValue;
        return r;
    }

    @Override public Object delete(Object group, int pos) {
        String[] g = (String[]) group;
        String[] r = new String[g.length - 1];
        System.arraycopy(g, 0, r, 0, pos);
        System.arraycopy(g, pos + 1, r, pos, g.length - pos - 1);
        return r;
    }

    @Override public Object copyRange(Object group, int from, int to) {
        return Arrays.copyOfRange((String[]) group, from, to);
    }

    @Override public Object fromArray(Object[] values) {
        String[] r = new String[values.length];
        for (int i = 0; i < r.length; i++) r[i] = (String) values[i];
        return r;
    }

    // ---- wire ----

    @Override public void serialize(DataOutput2 out, Object group) {
        String[] g = (String[]) group;
        int n = g.length;
        byte[][] enc = new byte[n][];
        int blobLen = 0;
        for (int i = 0; i < n; i++) {
            enc[i] = g[i].getBytes(StandardCharsets.UTF_8);
            blobLen += enc[i].length;
        }
        out.writeInt(blobLen);
        int off = 0;
        for (int i = 0; i < n; i++) {
            out.writeInt(off);
            off += enc[i].length;
        }
        for (int i = 0; i < n; i++) out.write(enc[i]);
    }

    @Override public Object deserialize(DataInput2 in, int size) {
        int blobLen = in.readInt();
        int[] off = new int[size];
        for (int i = 0; i < size; i++) off[i] = in.readInt();
        byte[] blob = new byte[blobLen];
        in.readFully(blob);
        String[] r = new String[size];
        for (int i = 0; i < size; i++) {
            int s = off[i];
            int e = (i + 1 < size) ? off[i + 1] : blobLen;
            r[i] = new String(blob, s, e - s, StandardCharsets.UTF_8);
        }
        return r;
    }

    // ---- byte side ----

    @Override public boolean supportsBinary() { return true; }

    @Override public int binarySearch(String key, DataInput2 in, int size) {
        int start = in.pos();
        int blobLen = in.readInt();
        if (blobLen < 0) throw new IllegalStateException("corrupt string group blobLen");
        int offBase = start + 4;
        int blobBase = offBase + size * 4;
        int lo = 0, hi = size - 1, found = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            in.pos(offBase + mid * 4);
            int s = in.readInt();
            int e = (mid + 1 < size) ? in.readInt() : blobLen;
            // serialize() writes 0 <= off[i] <= off[i+1] <= blobLen; anything else is a torn
            // optimistic read or corruption — fail fast, never read from it
            if (s < 0 || e < s || e > blobLen) throw new IllegalStateException("corrupt string group offsets");
            in.pos(blobBase + s);
            int c = Utf8.compareUtf8(in, e - s, key); // in place: no byte[] copy, no String
            if (c == 0) { found = mid; break; }
            else if (c < 0) lo = mid + 1;
            else hi = mid - 1;
        }
        in.pos(blobBase + blobLen); // leave input at group end
        return found >= 0 ? found : -(lo + 1);
    }

    @Override public String binaryGet(DataInput2 in, int size, int pos) {
        int start = in.pos();
        int blobLen = in.readInt();
        if (blobLen < 0) throw new IllegalStateException("corrupt string group blobLen");
        int offBase = start + 4;
        int blobBase = offBase + size * 4;
        String s = keyAt(in, offBase, blobBase, blobLen, size, pos);
        in.pos(blobBase + blobLen); // leave input at group end
        return s;
    }

    /** Decode element {@code idx} by seeking its offset in the fixed-width index. */
    private static String keyAt(DataInput2 in, int offBase, int blobBase, int blobLen, int size, int idx) {
        in.pos(offBase + idx * 4);
        int s = in.readInt();
        int e;
        if (idx + 1 < size) {
            in.pos(offBase + (idx + 1) * 4);
            e = in.readInt();
        } else {
            e = blobLen;
        }
        // serialize() writes 0 <= off[i] <= off[i+1] <= blobLen; anything else is a torn
        // optimistic read or corruption — fail fast, never allocate from it
        if (s < 0 || e < s || e > blobLen) throw new IllegalStateException("corrupt string group offsets");
        byte[] b = new byte[e - s];
        in.pos(blobBase + s);
        in.readFully(b);
        return new String(b, StandardCharsets.UTF_8);
    }
}
