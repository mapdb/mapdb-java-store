package org.mapdb.ser;

import org.junit.Test;

import static org.junit.Assert.assertFalse;

/** Binds the {@link GroupFormat} TCK to {@link ObjectArrayFormat} over STRING. */
public class ObjectArrayFormatStringTest extends GroupFormatTCK<String> {

    private final GroupFormat<String> fmt = new ObjectArrayFormat<>(Serializers.STRING);

    @Override protected GroupFormat<String> format() { return fmt; }

    /** Zero-padded 20 digits so lexicographic order == numeric order for v >= 0. */
    @Override protected String gen(long v) { return String.format("%020d", v); }

    @Test
    public void supportsBinaryIsFalse() {
        assertFalse(fmt.supportsBinary());
    }
}
