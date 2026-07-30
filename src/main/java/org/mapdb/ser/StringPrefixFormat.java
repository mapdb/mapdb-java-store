package org.mapdb.ser;

import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Front-coded group format for {@code String} — the LevelDB/RocksDB block style:
 * shared-prefix compression with periodic RESTART points
 * so the byte side keeps O(log n) navigation without materializing the group.
 *
 * Every entry is encoded as {@code [packInt sharedPrefixLen][packInt suffixLen]
 * [suffix utf8]} where {@code sharedPrefixLen} is the byte length shared with the
 * PREVIOUS entry's UTF-8 form. Entry {@code i} with {@code i % K == 0} is a
 * restart: {@code sharedPrefixLen} is forced to 0, so it is decodable without
 * history and addressable through the restart offset table.
 *
 * Wire layout for a group of {@code n} elements ({@code n} supplied externally
 * by the node header; {@code nRestarts = ceil(n/K)} follows from it):
 * <pre>
 *   int32 blobLen                     total byte length of all encoded entries
 *   int32 restartOff[0..nRestarts-1]  blob offset of entry r*K (fixed-width, so
 *                                     restart r is addressable with no decode loop)
 *   byte  blob[blobLen]               entries: packInt(shared) packInt(suffixLen) suffix
 * </pre>
 *
 * RESTART_INTERVAL K=16 trades compression against lookup cost: within an
 * interval a reader decodes sequentially (≤ K entries), so larger K compresses
 * better (fewer full keys) but scans longer; 16 is the LevelDB default and gives
 * two intervals at the house maxNodeSize of 32. Sorted groups share long
 * prefixes by construction — that is the compression this format exists for.
 *
 * Order is {@link String#compareTo}, matched on the byte side by
 * {@link Utf8#compareUtf8} (UTF-16 code-unit order, NOT raw byte order — see the
 * helper's javadoc): {@link #binarySearch} binary-searches the restart entries
 * in place (allocation-free), then rolls forward through at most K entries of
 * one interval, reconstructing incrementally into a scratch buffer — the whole
 * group is never materialized.
 *
 * Torn-read discipline: blobLen/offsets/sharedLen/suffixLen are
 * all clamped (restart shared must be 0, shared ≤ previous length, suffix must
 * fit the blob) — garbage fails fast, allocations stay bounded by the blob, and
 * every loop is bounded by n or K, never by decoded data.
 */
public final class StringPrefixFormat implements GroupFormat<String> {

    public static final StringPrefixFormat INSTANCE = new StringPrefixFormat();

    /** Restart every K entries; see class javadoc for the trade. */
    static final int RESTART_INTERVAL = 16;

    private static final String[] EMPTY = new String[0];

    private StringPrefixFormat() {}

    @Override public Serializer<String> element() { return Serializers.STRING; }

    // ---- object side (identical shape to StringGroupFormat) ----

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
        int nRest = (n + RESTART_INTERVAL - 1) / RESTART_INTERVAL;
        int[] restOff = new int[nRest];
        DataOutput2 blob = new DataOutput2(Math.max(16, n * 8));
        byte[] prev = null;
        for (int i = 0; i < n; i++) {
            byte[] enc = g[i].getBytes(StandardCharsets.UTF_8);
            int shared;
            if (i % RESTART_INTERVAL == 0) {
                restOff[i / RESTART_INTERVAL] = blob.pos;
                shared = 0;
            } else {
                shared = commonPrefixLen(prev, enc);
            }
            blob.packInt(shared);
            blob.packInt(enc.length - shared);
            blob.write(enc, shared, enc.length - shared);
            prev = enc;
        }
        out.writeInt(blob.pos);
        for (int off : restOff) out.writeInt(off);
        out.write(blob.buf, 0, blob.pos);
    }

    private static int commonPrefixLen(byte[] a, byte[] b) {
        int n = Math.min(a.length, b.length);
        int i = 0;
        while (i < n && a[i] == b[i]) i++;
        return i;
    }

    @Override public Object deserialize(DataInput2 in, int size) {
        int blobLen = in.readInt();
        int nRest = (size + RESTART_INTERVAL - 1) / RESTART_INTERVAL;
        in.skipBytes(nRest * 4); // sequential decode does not need the restart table
        String[] r = new String[size];
        byte[] cur = new byte[64];
        int curLen = 0;
        for (int i = 0; i < size; i++) {
            int shared = in.unpackInt();
            int suffixLen = in.unpackInt();
            int newLen = shared + suffixLen;
            if (cur.length < newLen) cur = Arrays.copyOf(cur, Math.max(newLen, cur.length * 2));
            in.readFully(cur, shared, suffixLen);
            curLen = newLen;
            r[i] = new String(cur, 0, curLen, StandardCharsets.UTF_8);
        }
        return r;
    }

    // ---- byte side ----

    @Override public boolean supportsBinary() { return true; }

    @Override public int binarySearch(String key, DataInput2 in, int size) {
        int start = in.pos();
        int blobLen = in.readInt();
        if (blobLen < 0) throw new IllegalStateException("corrupt prefix group blobLen");
        int nRest = (size + RESTART_INTERVAL - 1) / RESTART_INTERVAL;
        int restBase = start + 4;
        int blobBase = restBase + nRest * 4;
        int end = blobBase + blobLen;

        // 1. binary search the restarts for the RIGHTMOST restart entry <= key,
        //    comparing the stored UTF-8 in place (restarts have shared == 0)
        int lo = 0, hi = nRest - 1, r = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int c = compareRestart(in, restBase, blobBase, blobLen, mid, key);
            if (c <= 0) { r = mid; lo = mid + 1; }
            else hi = mid - 1;
        }
        if (r < 0) { // key sorts below the first entry
            in.pos(end);
            return -1;
        }

        // 2. roll forward through interval r (<= K entries), reconstructing incrementally
        int first = r * RESTART_INTERVAL;
        int limit = Math.min(first + RESTART_INTERVAL, size);
        seekRestart(in, restBase, blobBase, blobLen, r);
        Scratch s = new Scratch();
        int result = -(limit + 1); // key above the whole interval (and below restart r+1, if any)
        for (int i = first; i < limit; i++) {
            readEntry(in, s, end, i == first);
            int c = Utf8.compareUtf8(new DataInput2.ByteArray(s.buf, 0), s.len, key);
            if (c == 0) { result = i; break; }
            if (c > 0) { result = -(i + 1); break; }
        }
        in.pos(end);
        return result;
    }

    @Override public String binaryGet(DataInput2 in, int size, int pos) {
        int start = in.pos();
        int blobLen = in.readInt();
        if (blobLen < 0) throw new IllegalStateException("corrupt prefix group blobLen");
        int nRest = (size + RESTART_INTERVAL - 1) / RESTART_INTERVAL;
        int restBase = start + 4;
        int blobBase = restBase + nRest * 4;
        int end = blobBase + blobLen;

        int r = pos / RESTART_INTERVAL;
        seekRestart(in, restBase, blobBase, blobLen, r);
        Scratch s = new Scratch();
        int first = r * RESTART_INTERVAL;
        for (int i = first; i <= pos; i++) readEntry(in, s, end, i == first);
        in.pos(end);
        return new String(s.buf, 0, s.len, StandardCharsets.UTF_8);
    }

    /** Reconstruction buffer for one interval walk (method-local; the format itself is stateless). */
    private static final class Scratch {
        byte[] buf = new byte[64];
        int len;
    }

    /** Position {@code in} at restart {@code r}'s entry, validating the offset. */
    private static void seekRestart(DataInput2 in, int restBase, int blobBase, int blobLen, int r) {
        in.pos(restBase + r * 4);
        int off = in.readInt();
        if (off < 0 || off > blobLen) throw new IllegalStateException("corrupt prefix group restart offset");
        in.pos(blobBase + off);
    }

    /**
     * Decode one entry into {@code s} ({@code s} holds the previous entry's bytes on
     * entry; restart entries must carry shared == 0). Every length is clamped:
     * garbage throws, allocation is bounded by the blob.
     */
    private static void readEntry(DataInput2 in, Scratch s, int end, boolean restart) {
        int shared = in.unpackInt();
        if (shared < 0 || (restart ? shared != 0 : shared > s.len))
            throw new IllegalStateException("corrupt prefix group sharedLen");
        // subtraction form: in.pos() + suffixLen could overflow int on garbage
        int suffixLen = in.unpackInt();
        if (suffixLen < 0 || suffixLen > end - in.pos())
            throw new IllegalStateException("corrupt prefix group suffixLen");
        int newLen = shared + suffixLen; // bounded: shared <= s.len, suffixLen <= blob remainder
        if (s.buf.length < newLen) s.buf = Arrays.copyOf(s.buf, Math.max(newLen, s.buf.length * 2));
        in.readFully(s.buf, shared, suffixLen);
        s.len = newLen;
    }

    /** Compare restart entry {@code r} against {@code key} in place — no copy, no String. */
    private static int compareRestart(DataInput2 in, int restBase, int blobBase, int blobLen,
                                      int r, String key) {
        seekRestart(in, restBase, blobBase, blobLen, r);
        int shared = in.unpackInt();
        if (shared != 0) throw new IllegalStateException("corrupt prefix group restart sharedLen");
        // subtraction form: in.pos() + len could overflow int on garbage
        int len = in.unpackInt();
        if (len < 0 || len > blobBase + blobLen - in.pos())
            throw new IllegalStateException("corrupt prefix group restart suffixLen");
        return Utf8.compareUtf8(in, len, key);
    }
}
