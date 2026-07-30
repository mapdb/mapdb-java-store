package org.mapdb.ported;

import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;
import org.mapdb.ser.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;

/**
 * Shared helpers for the ported historical-test suites (mapdb1/2/3 → mapdb5).
 * Public-API only.
 */
final class Ported {

    private Ported() {}

    /**
     * Largest plain-record content mapdb5 StoreDirect accepts: IndexVal.MAX_CAPACITY - 4.
     * MAX_CAPACITY = 0xFFFD * 16 (~1 MiB-48); minus the 4-byte used-length header.
     */
    static final int MAX_CONTENT = 0xFFFD * 16 - 4; // 1_048_524

    /**
     * Size-driven raw byte[] serializer (mapdb3 {@code BYTE_ARRAY_NOSIZE} analogue).
     * Content == value; deserialize consumes exactly {@code size} bytes. Needed for
     * byte-exact boundary/large-record assertions (a length-prefixed serializer would
     * mask the true stored size).
     */
    static final Serializer<byte[]> RAW = new Serializer<>() {
        @Override public void serialize(DataOutput2 out, byte[] value) { out.write(value); }

        @Override public byte[] deserialize(DataInput2 in, int size) {
            byte[] b = new byte[size];
            in.readFully(b);
            return b;
        }

        @Override public boolean equals(byte[] a, byte[] b) { return Arrays.equals(a, b); }

        @Override public int compare(byte[] a, byte[] b) { return Arrays.compare(a, b); }
    };

    /**
     * Zero-byte-emitting String serializer (mapdb2 EngineTest.zero_size_serializer):
     * "" serializes to 0 bytes, giving a genuine 0-length LIVE record — distinct from
     * a null record and never produced by the length-prefixed {@code Serializers.STRING}.
     */
    static final Serializer<String> ZERO_STR = new Serializer<>() {
        @Override public void serialize(DataOutput2 out, String value) {
            if (!value.isEmpty()) out.write(value.getBytes(StandardCharsets.UTF_8));
        }

        @Override public String deserialize(DataInput2 in, int size) {
            if (size == 0) return "";
            byte[] b = new byte[size];
            in.readFully(b);
            return new String(b, StandardCharsets.UTF_8);
        }
    };

    static byte[] bytes(int size, long seed) {
        byte[] b = new byte[size];
        new Random(seed).nextBytes(b);
        return b;
    }
}
