package org.mapdb.ser;

import org.junit.Test;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Binds the {@link GroupFormat} TCK to {@link IntDeltaFormat}. */
public class IntDeltaFormatTest extends GroupFormatTCK<Integer> {

    private final IntDeltaFormat fmt = IntDeltaFormat.INSTANCE;

    @Override protected GroupFormat<Integer> format() { return fmt; }

    @Override protected Integer gen(long v) { return (int) v; }

    @Test
    public void supportsBinaryIsTrue() {
        assertTrue(fmt.supportsBinary());
    }

    /** Same under-the-covers pin as IntFormatTest: the group is an int[]. */
    @Test
    public void representationIsIntArray() {
        assertTrue(fmt.empty() instanceof int[]);
        Object g = fmt.fromArray(new Object[]{1, 2, 3});
        assertTrue(g instanceof int[]);
    }

    private int[] roundTrip(int[] values) {
        Object g = values.clone();
        DataOutput2 out = new DataOutput2();
        fmt.serialize(out, g);
        return (int[]) fmt.deserialize(new DataInput2.ByteArray(out.copyBytes(), 0), values.length);
    }

    /**
     * Zigzag is there so NON-SORTED input never corrupts (positional ops don't
     * guarantee sortedness): negative deltas and even overflow-wrapped deltas
     * (MAX_VALUE next to MIN_VALUE) must round-trip exactly.
     */
    @Test
    public void nonSortedAndExtremeValues_roundTripExactly() {
        int[][] cases = {
                {5, 3, 100, -7, 0},                                          // descending / mixed
                {Integer.MAX_VALUE, Integer.MIN_VALUE, 0, Integer.MAX_VALUE}, // overflow-wrapped deltas
                {Integer.MIN_VALUE, -5, 0, 7, Integer.MAX_VALUE},            // sorted with extremes
                {-1, -1, -1},                                               // repeats (zero deltas)
                {0},
                {},
        };
        for (int[] c : cases) {
            assertArrayEquals(c, roundTrip(c));
        }
    }

    /** Sequential binarySearch/binaryGet agree with the object side on sorted groups with negatives. */
    @Test
    public void binaryEquivalence_negativeSortedGroup() {
        int[] group = {Integer.MIN_VALUE, -1_000_000, -1, 0, 1, 42, Integer.MAX_VALUE};
        Object g = group.clone();
        DataOutput2 out = new DataOutput2();
        fmt.serialize(out, g);
        out.writeLong(0x51515151L); // sentinel
        byte[] bytes = out.copyBytes();
        int[] probes = {Integer.MIN_VALUE, Integer.MIN_VALUE + 1, -1_000_001, -1_000_000, -2, -1,
                0, 1, 2, 41, 42, 43, Integer.MAX_VALUE - 1, Integer.MAX_VALUE};
        for (int probe : probes) {
            DataInput2 in = new DataInput2.ByteArray(bytes, 0);
            assertEquals("probe " + probe, fmt.search(g, probe), fmt.binarySearch(probe, in, group.length));
            assertEquals(0x51515151L, in.readLong());
        }
        for (int pos = 0; pos < group.length; pos++) {
            DataInput2 in = new DataInput2.ByteArray(bytes, 0);
            assertEquals((Integer) group[pos], fmt.binaryGet(in, group.length, pos));
            assertEquals(0x51515151L, in.readLong());
        }
    }

    /** The size argument: sorted ascending keys pack far below IntFormat's 4 bytes/key. */
    @Test
    public void sortedKeysPackSmall() {
        int[] keys = new int[32];
        for (int i = 0; i < 32; i++) keys[i] = 1_000_000 + i * 17; // dense ascending, small deltas
        DataOutput2 delta = new DataOutput2(), fixed = new DataOutput2();
        fmt.serialize(delta, keys.clone());
        IntFormat.INSTANCE.serialize(fixed, keys.clone());
        assertEquals(32 * 4, fixed.pos);
        assertTrue("delta wire (" + delta.pos + "B) must be well under fixed (" + fixed.pos + "B)",
                delta.pos < fixed.pos / 2);
    }
}
