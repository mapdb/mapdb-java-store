package org.mapdb.ser;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

/** Binds the {@link GroupFormat} TCK to {@link LongFormat}. */
public class LongFormatTest extends GroupFormatTCK<Long> {

    @Override protected GroupFormat<Long> format() { return LongFormat.INSTANCE; }

    @Override protected Long gen(long v) { return v; }

    @Test
    public void supportsBinaryIsTrue() {
        assertTrue(LongFormat.INSTANCE.supportsBinary());
    }

    /**
     * The ONE place we look under the covers: documents that LongFormat's opaque
     * group is a {@code long[]} (no boxing in the group). All other tests treat
     * the group as opaque.
     */
    @Test
    public void representationIsLongArray() {
        assertTrue(LongFormat.INSTANCE.empty() instanceof long[]);
        Object g = LongFormat.INSTANCE.fromArray(new Object[]{1L, 2L, 3L});
        assertTrue(g instanceof long[]);
        long[] raw = (long[]) g;
        assertTrue(raw.length == 3 && raw[0] == 1L && raw[2] == 3L);
    }
}
