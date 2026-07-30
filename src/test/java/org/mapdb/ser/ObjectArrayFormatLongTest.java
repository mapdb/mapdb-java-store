package org.mapdb.ser;

import org.junit.Test;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Binds the {@link GroupFormat} TCK to {@link ObjectArrayFormat} over LONG, and
 * additionally proves the generic format agrees with the specialized
 * {@link LongFormat} on identical data (same object-side answers, same wire bytes).
 */
public class ObjectArrayFormatLongTest extends GroupFormatTCK<Long> {

    private final GroupFormat<Long> fmt = new ObjectArrayFormat<>(Serializers.LONG);

    @Override protected GroupFormat<Long> format() { return fmt; }

    @Override protected Long gen(long v) { return v; }

    @Test
    public void supportsBinaryIsFalse() {
        assertFalse(fmt.supportsBinary());
    }

    @Test
    public void agreesWithLongFormat_searchAndSizeAndGet() {
        LongFormat lf = LongFormat.INSTANCE;
        for (int n : new int[]{0, 1, 2, 3, 64, 500}) {
            Object[] vals = new Object[n];
            for (int i = 0; i < n; i++) vals[i] = (i + 1) * 10L;
            Object og = fmt.fromArray(vals);
            Object lg = lf.fromArray(vals);
            assertEquals(lf.size(lg), fmt.size(og));
            for (int i = 0; i < n; i++) assertEquals(lf.get(lg, i), fmt.get(og, i));
            // present + absent search agreement
            for (int i = 0; i < n; i++) {
                Long key = (i + 1) * 10L;
                assertEquals(lf.search(lg, key), fmt.search(og, key));
            }
            for (int i = 0; i <= n; i++) {
                Long key = i * 10L + 5L; // in every gap incl. below-first and above-last
                assertEquals("absent key " + key, lf.search(lg, key), fmt.search(og, key));
            }
        }
    }

    @Test
    public void agreesWithLongFormat_wireBytesIdentical() {
        LongFormat lf = LongFormat.INSTANCE;
        for (int n : new int[]{0, 1, 5, 200}) {
            Object[] vals = new Object[n];
            for (int i = 0; i < n; i++) vals[i] = (long) i * 7 - 3;
            Object og = fmt.fromArray(vals);
            Object lg = lf.fromArray(vals);
            DataOutput2 o1 = new DataOutput2();
            DataOutput2 o2 = new DataOutput2();
            fmt.serialize(o1, og);
            lf.serialize(o2, lg);
            assertArrayEquals("generic and specialized wire bytes must match for size " + n,
                    o2.copyBytes(), o1.copyBytes());
            // and the generic format can read back bytes written by LongFormat
            DataInput2 in = new DataInput2.ByteArray(o2.copyBytes(), 0);
            Object back = fmt.deserialize(in, n);
            for (int i = 0; i < n; i++) assertEquals(vals[i], fmt.get(back, i));
        }
    }
}
