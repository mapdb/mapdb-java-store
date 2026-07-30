package org.mapdb.ser;

import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.util.Arrays;

/**
 * Delta-packed group format for Long values. Object side
 * is identical to {@link LongFormat} (long[], no boxing); the wire trades
 * LongFormat's fixed 8-byte stride for packed zigzag deltas:
 *
 * <pre>
 *   packLong(zigzag(k[0]))                 first element, absolute
 *   packLong(zigzag(k[i] - k[i-1]))        i = 1..n-1
 * </pre>
 *
 * Intended for KEY groups: sorted keys have small non-negative deltas that pack
 * into 1–2 bytes each (vs 8). Values gain nothing (arbitrary deltas ≈ 10 packed
 * bytes) — keep {@link LongFormat} there. Zigzag rather than assuming
 * sortedness: positional ops (insert/set/fromArray) do not guarantee ascending
 * input, and a negative or overflow-wrapped delta still round-trips exactly
 * (two's-complement accumulate wraps the same way), so the format never corrupts
 * on non-sorted groups — it just packs them poorly.
 *
 * The byte side has no random access, so {@link #binarySearch} is a SEQUENTIAL
 * decode with early exit on {@code v >= key}. Groups are capped by maxNodeSize
 * (≈32): a linear walk over ~2-byte entries touches ≤ ~64 contiguous bytes —
 * the hypothesis (benched in BufferTreeScaleIT via -Dbuffertree.keyFormat) is
 * that this cache-friendly walk competes with LongFormat's stride-8 seeks while
 * shrinking directory records. Torn bytes: the walk decodes at
 * most {@code size} packed longs, each terminated by the physical buffer at the
 * latest (a missing terminator throws from the buffer bound) — like LongFormat's
 * fixed-stride reads, torn bytes may yield nonsense VALUES rather than an
 * exception, which the store's optimistic-read validation discards; nothing here
 * allocates from decoded data or loops beyond {@code size}.
 */
public final class LongDeltaFormat implements GroupFormat<Long> {

    public static final LongDeltaFormat INSTANCE = new LongDeltaFormat();

    private LongDeltaFormat() {}

    private static long zigzag(long v) { return (v << 1) ^ (v >> 63); }

    private static long unzigzag(long v) { return (v >>> 1) ^ -(v & 1); }

    @Override public Serializer<Long> element() { return Serializers.LONG; }

    // ---- object side: identical to LongFormat ----

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

    // ---- wire ----

    @Override public void serialize(DataOutput2 out, Object group) {
        long prev = 0;
        for (long v : (long[]) group) {
            out.packLong(zigzag(v - prev));
            prev = v;
        }
    }

    @Override public Object deserialize(DataInput2 in, int size) {
        long[] r = new long[size];
        long v = 0;
        for (int i = 0; i < size; i++) {
            v += unzigzag(in.unpackLong());
            r[i] = v;
        }
        return r;
    }

    // ---- byte side: sequential decode, early exit ----

    @Override public boolean supportsBinary() { return true; }

    @Override public int binarySearch(Long key, DataInput2 in, int size) {
        long k = key;
        long v = 0;
        for (int i = 0; i < size; i++) {
            v += unzigzag(in.unpackLong());
            if (v >= k) {
                in.unpackLongSkip(size - i - 1); // leave input at group end
                return v == k ? i : -(i + 1);
            }
        }
        return -(size + 1);
    }

    @Override public Long binaryGet(DataInput2 in, int size, int pos) {
        long v = 0;
        for (int i = 0; i <= pos; i++) v += unzigzag(in.unpackLong());
        in.unpackLongSkip(size - pos - 1); // leave input at group end
        return v;
    }

    /**
     * Single forward pass (O(n)) instead of the interface default's per-element re-decode
     * (O(n²) for a delta layout): the cursor decodes the zigzag stream once, accumulating, and
     * on exhaustion drains any remaining deltas so the input is left at group end.
     */
    @Override public GroupCursor<Long> rangeCursor(DataInput2 in, int size, int from, int to) {
        if (from < 0 || from > to || to > size)
            throw new IndexOutOfBoundsException("from=" + from + " to=" + to + " size=" + size);
        return new GroupCursor<Long>() {
            int idx = from - 1;
            int decoded = 0;   // number of elements consumed from the stream so far
            long acc = 0;      // running value; after decoding element k, acc == element[k]
            Long cur;
            boolean exhausted;

            private void consumeTo(int count) { // decode forward until `decoded == count`
                while (decoded < count) { acc += unzigzag(in.unpackLong()); decoded++; }
            }

            @Override public boolean next() {
                if (exhausted) return false;
                idx++;
                if (idx >= to) {
                    exhausted = true;
                    cur = null;
                    consumeTo(size); // drain to group end (forward-only; input was never reset)
                    return false;
                }
                consumeTo(idx + 1);  // decode elements up to and including idx
                cur = acc;
                return true;
            }

            @Override public int index() { return idx; }

            @Override public Long value() { return cur; }
        };
    }
}
