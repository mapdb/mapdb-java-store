package org.mapdb.ser;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

/** Binds the {@link GroupFormat} TCK to {@link IntFormat}. */
public class IntFormatTest extends GroupFormatTCK<Integer> {

    @Override protected GroupFormat<Integer> format() { return IntFormat.INSTANCE; }

    @Override protected Integer gen(long v) { return Math.toIntExact(v); }

    @Test
    public void supportsBinaryIsTrue() {
        assertTrue(IntFormat.INSTANCE.supportsBinary());
    }

    /**
     * The ONE place we look under the covers: documents that IntFormat's opaque
     * group is an {@code int[]} (no boxing in the group). All other tests treat
     * the group as opaque.
     */
    @Test
    public void representationIsIntArray() {
        assertTrue(IntFormat.INSTANCE.empty() instanceof int[]);
        Object g = IntFormat.INSTANCE.fromArray(new Object[]{1, 2, 3});
        assertTrue(g instanceof int[]);
        int[] raw = (int[]) g;
        assertTrue(raw.length == 3 && raw[0] == 1 && raw[2] == 3);
    }
}
