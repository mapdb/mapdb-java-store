package org.mapdb.ser;

import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.util.Arrays;

/**
 * Delta-packed group format for Integer values — the 32-bit mirror of
 * {@link LongDeltaFormat}. Object side is identical to
 * {@link IntFormat} (int[], no boxing in the group); the wire trades IntFormat's
 * fixed 4-byte stride for packed zigzag deltas:
 *
 * <pre>
 *   packInt(zigzag(k[0]))                  first element, absolute
 *   packInt(zigzag(k[i] - k[i-1]))         i = 1..n-1
 * </pre>
 *
 * Intended for KEY groups: sorted keys have small non-negative deltas that pack
 * into 1–2 bytes each (vs 4). Values gain nothing (arbitrary deltas ≈ 5 packed
 * bytes) — keep {@link IntFormat} there. Zigzag rather than assuming sortedness:
 * positional ops (insert/set/fromArray) do not guarantee ascending input, and a
 * negative or overflow-wrapped delta still round-trips exactly (two's-complement
 * accumulate wraps the same way, mod 2^32), so the format never corrupts on
 * non-sorted groups — it just packs them poorly.
 *
 * The byte side has no random access, so {@link #binarySearch} is a SEQUENTIAL
 * decode with early exit on {@code v >= key}. Groups are capped by maxNodeSize
 * (≈32): a linear walk over ~2-byte entries touches ≤ ~64 contiguous bytes — the
 * same cache-friendly walk as {@link LongDeltaFormat} while shrinking directory
 * records. Torn bytes: the walk decodes at most {@code size}
 * packed ints, each terminated by the physical buffer at the latest (a missing
 * terminator throws from the buffer bound) — like IntFormat's fixed-stride reads,
 * torn bytes may yield nonsense VALUES rather than an exception, which the store's
 * optimistic-read validation discards; nothing here allocates from decoded data
 * or loops beyond {@code size}.
 */
public final class IntDeltaFormat implements GroupFormat<Integer> {

    public static final IntDeltaFormat INSTANCE = new IntDeltaFormat();

    private IntDeltaFormat() {}

    private static int zigzag(int v) { return (v << 1) ^ (v >> 31); }

    private static int unzigzag(int v) { return (v >>> 1) ^ -(v & 1); }

    @Override public Serializer<Integer> element() { return Serializers.INTEGER; }

    // ---- object side: identical to IntFormat ----

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

    // ---- wire ----

    @Override public void serialize(DataOutput2 out, Object group) {
        int prev = 0;
        for (int v : (int[]) group) {
            out.packInt(zigzag(v - prev));
            prev = v;
        }
    }

    @Override public Object deserialize(DataInput2 in, int size) {
        int[] r = new int[size];
        int v = 0;
        for (int i = 0; i < size; i++) {
            v += unzigzag(in.unpackInt());
            r[i] = v;
        }
        return r;
    }

    // ---- byte side: sequential decode, early exit ----

    @Override public boolean supportsBinary() { return true; }

    @Override public int binarySearch(Integer key, DataInput2 in, int size) {
        int k = key;
        int v = 0;
        for (int i = 0; i < size; i++) {
            v += unzigzag(in.unpackInt());
            if (v >= k) {
                in.unpackLongSkip(size - i - 1); // leave input at group end
                return v == k ? i : -(i + 1);
            }
        }
        return -(size + 1);
    }

    @Override public Integer binaryGet(DataInput2 in, int size, int pos) {
        int v = 0;
        for (int i = 0; i <= pos; i++) v += unzigzag(in.unpackInt());
        in.unpackLongSkip(size - pos - 1); // leave input at group end
        return v;
    }
}
