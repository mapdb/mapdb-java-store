package org.mapdb.ser;

import org.junit.Test;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Binds the {@link GroupFormat} TCK to {@link LongDeltaFormat}. */
public class LongDeltaFormatTest extends GroupFormatTCK<Long> {

    private final LongDeltaFormat fmt = LongDeltaFormat.INSTANCE;

    @Override protected GroupFormat<Long> format() { return fmt; }

    @Override protected Long gen(long v) { return v; }

    @Test
    public void supportsBinaryIsTrue() {
        assertTrue(fmt.supportsBinary());
    }

    /** Same under-the-covers pin as LongFormatTest: the group is a long[]. */
    @Test
    public void representationIsLongArray() {
        assertTrue(fmt.empty() instanceof long[]);
        Object g = fmt.fromArray(new Object[]{1L, 2L, 3L});
        assertTrue(g instanceof long[]);
    }

    private long[] roundTrip(long[] values) {
        Object g = values.clone();
        DataOutput2 out = new DataOutput2();
        fmt.serialize(out, g);
        return (long[]) fmt.deserialize(new DataInput2.ByteArray(out.copyBytes(), 0), values.length);
    }

    /**
     * Zigzag is there so NON-SORTED input never corrupts (positional ops don't
     * guarantee sortedness): negative deltas and even overflow-wrapped deltas
     * (MAX_VALUE next to MIN_VALUE) must round-trip exactly.
     */
    @Test
    public void nonSortedAndExtremeValues_roundTripExactly() {
        long[][] cases = {
                {5, 3, 100, -7, 0},                                    // descending / mixed
                {Long.MAX_VALUE, Long.MIN_VALUE, 0, Long.MAX_VALUE},   // overflow-wrapped deltas
                {Long.MIN_VALUE, -5, 0, 7, Long.MAX_VALUE},            // sorted with extremes
                {-1, -1, -1},                                          // repeats (zero deltas)
                {0},
                {},
        };
        for (long[] c : cases) {
            assertArrayEquals(c, roundTrip(c));
        }
    }

    /** Sequential binarySearch/binaryGet agree with the object side on sorted groups with negatives. */
    @Test
    public void binaryEquivalence_negativeSortedGroup() {
        long[] group = {Long.MIN_VALUE, -1_000_000, -1, 0, 1, 42, Long.MAX_VALUE};
        Object g = group.clone();
        DataOutput2 out = new DataOutput2();
        fmt.serialize(out, g);
        out.writeLong(0x51515151L); // sentinel
        byte[] bytes = out.copyBytes();
        long[] probes = {Long.MIN_VALUE, Long.MIN_VALUE + 1, -1_000_001, -1_000_000, -2, -1,
                0, 1, 2, 41, 42, 43, Long.MAX_VALUE - 1, Long.MAX_VALUE};
        for (long probe : probes) {
            DataInput2 in = new DataInput2.ByteArray(bytes, 0);
            assertEquals("probe " + probe, fmt.search(g, probe), fmt.binarySearch(probe, in, group.length));
            assertEquals(0x51515151L, in.readLong());
        }
        for (int pos = 0; pos < group.length; pos++) {
            DataInput2 in = new DataInput2.ByteArray(bytes, 0);
            assertEquals((Long) group[pos], fmt.binaryGet(in, group.length, pos));
            assertEquals(0x51515151L, in.readLong());
        }
    }

    /** The size argument: sorted ascending keys pack far below LongFormat's 8 bytes/key. */
    @Test
    public void sortedKeysPackSmall() {
        long[] keys = new long[32];
        for (int i = 0; i < 32; i++) keys[i] = 1_000_000 + i * 17; // dense ascending, small deltas
        DataOutput2 delta = new DataOutput2(), fixed = new DataOutput2();
        fmt.serialize(delta, keys.clone());
        LongFormat.INSTANCE.serialize(fixed, keys.clone());
        assertEquals(32 * 8, fixed.pos);
        assertTrue("delta wire (" + delta.pos + "B) must be well under fixed (" + fixed.pos + "B)",
                delta.pos < fixed.pos / 3);
    }
}
