package org.mapdb.ser;

import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.util.UUID;

/**
 * Group format for {@link UUID} values backed by a {@code long[]} of length
 * {@code 2*count} — consecutive {@code (mostSigBits, leastSigBits)} pairs, no per-element
 * boxing — and a fixed 16-byte wire stride, giving O(log n) true binary search over
 * serialized bytes via seeking.
 *
 * Wire layout for a group of {@code n} elements ({@code n} supplied externally):
 * <pre>
 *   for each element: int64 mostSigBits, int64 leastSigBits   (both big-endian)
 * </pre>
 *
 * <h3>Order</h3>
 * The order is exactly {@link UUID#compareTo(UUID)}: compare {@code mostSigBits} as a
 * SIGNED 64-bit long, and on a tie compare {@code leastSigBits} as a SIGNED 64-bit long.
 * Because each half is written as a big-endian signed long (identical to
 * {@link LongFormat}'s encoding) and read back with {@link DataInput2#readLong()} (signed),
 * comparing (msb, then lsb) as signed longs on the byte side reproduces
 * {@code UUID.compareTo} byte-for-byte. This is a SIGNED comparison on each half: a UUID
 * whose msb has the high bit set (e.g. {@code ffff...}) sorts BELOW one whose msb high bit
 * is clear (e.g. {@code 0000...}) — matching the JDK, NOT unsigned/lexicographic byte order.
 * The materialized {@link #search} and the byte-side {@link #binarySearch} use this one
 * comparison, so they never diverge.
 */
public final class UUIDFormat implements GroupFormat<UUID> {

    public static final UUIDFormat INSTANCE = new UUIDFormat();

    private UUIDFormat() {}

    /** Signed (msb, then lsb) comparison — exactly {@link UUID#compareTo}. */
    private static int cmp(long aMsb, long aLsb, long bMsb, long bLsb) {
        int c = Long.compare(aMsb, bMsb);
        return c != 0 ? c : Long.compare(aLsb, bLsb);
    }

    @Override public Serializer<UUID> element() { return Serializers.UUID; }

    @Override public Object empty() { return new long[0]; }

    @Override public int size(Object group) { return ((long[]) group).length >>> 1; }

    @Override public UUID get(Object group, int pos) {
        long[] g = (long[]) group;
        return new UUID(g[2 * pos], g[2 * pos + 1]);
    }

    @Override public int search(Object group, UUID key) {
        long[] g = (long[]) group;
        long kMsb = key.getMostSignificantBits(), kLsb = key.getLeastSignificantBits();
        int lo = 0, hi = (g.length >>> 1) - 1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int c = cmp(g[2 * mid], g[2 * mid + 1], kMsb, kLsb);
            if (c < 0) lo = mid + 1;
            else if (c > 0) hi = mid - 1;
            else return mid;
        }
        return -(lo + 1);
    }

    @Override public Object insert(Object group, int pos, UUID newValue) {
        long[] g = (long[]) group;
        long[] r = new long[g.length + 2];
        System.arraycopy(g, 0, r, 0, 2 * pos);
        r[2 * pos] = newValue.getMostSignificantBits();
        r[2 * pos + 1] = newValue.getLeastSignificantBits();
        System.arraycopy(g, 2 * pos, r, 2 * pos + 2, g.length - 2 * pos);
        return r;
    }

    @Override public Object set(Object group, int pos, UUID newValue) {
        long[] r = ((long[]) group).clone();
        r[2 * pos] = newValue.getMostSignificantBits();
        r[2 * pos + 1] = newValue.getLeastSignificantBits();
        return r;
    }

    @Override public Object delete(Object group, int pos) {
        long[] g = (long[]) group;
        long[] r = new long[g.length - 2];
        System.arraycopy(g, 0, r, 0, 2 * pos);
        System.arraycopy(g, 2 * pos + 2, r, 2 * pos, g.length - 2 * pos - 2);
        return r;
    }

    @Override public Object copyRange(Object group, int from, int to) {
        return java.util.Arrays.copyOfRange((long[]) group, 2 * from, 2 * to);
    }

    @Override public Object fromArray(Object[] values) {
        long[] r = new long[values.length * 2];
        for (int i = 0; i < values.length; i++) {
            UUID u = (UUID) values[i];
            r[2 * i] = u.getMostSignificantBits();
            r[2 * i + 1] = u.getLeastSignificantBits();
        }
        return r;
    }

    @Override public void serialize(DataOutput2 out, Object group) {
        for (long v : (long[]) group) out.writeLong(v);
    }

    @Override public Object deserialize(DataInput2 in, int size) {
        long[] r = new long[size * 2];
        for (int i = 0; i < r.length; i++) r[i] = in.readLong();
        return r;
    }

    @Override public boolean supportsBinary() { return true; }

    @Override public int binarySearch(UUID key, DataInput2 in, int size) {
        int start = in.pos();
        long kMsb = key.getMostSignificantBits(), kLsb = key.getLeastSignificantBits();
        int lo = 0, hi = size - 1, found = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            in.pos(start + mid * 16);
            long msb = in.readLong();
            long lsb = in.readLong();
            int c = cmp(msb, lsb, kMsb, kLsb);
            if (c == 0) { found = mid; break; }
            else if (c < 0) lo = mid + 1;
            else hi = mid - 1;
        }
        in.pos(start + size * 16);
        return found >= 0 ? found : -(lo + 1);
    }

    @Override public UUID binaryGet(DataInput2 in, int size, int pos) {
        int start = in.pos();
        in.pos(start + pos * 16);
        long msb = in.readLong();
        long lsb = in.readLong();
        in.pos(start + size * 16);
        return new UUID(msb, lsb);
    }
}
