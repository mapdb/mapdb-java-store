package org.mapdb.store;

import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;
import org.mapdb.ser.Serializer;

import java.util.Arrays;

/**
 * Shared test fixtures for the Store/Delta/WAL suites.
 *
 * The delta capability is only exercisable byte-exactly with a size-driven
 * ("no-size") serializer: {@link #RAW} writes its bytes verbatim and
 * deserialize() consumes exactly the {@code size} bytes the store reports
 * (base ++ deltas). This engine's {@code Serializers.BYTE_ARRAY}
 * is self-delimiting (length-prefixed) so it hides the appended region — it is
 * the wrong tool for merged-content assertions. RAW is the mapdb3
 * {@code BYTE_ARRAY_NOSIZE} analogue that the delta contract's merged-logical-value
 * semantics imply.
 */
final class Fixtures {

    private Fixtures() {}

    /** Size-driven raw byte[] serializer: content == value, deserialize reads all {@code size} bytes. */
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

    /** Self-describing content: [slot:int][version:int] then a deterministic fill of {@code extra} bytes. */
    static byte[] payload(int slot, int version, int extra) {
        byte[] b = new byte[8 + Math.max(0, extra)];
        b[0] = (byte) (slot >>> 24); b[1] = (byte) (slot >>> 16); b[2] = (byte) (slot >>> 8); b[3] = (byte) slot;
        b[4] = (byte) (version >>> 24); b[5] = (byte) (version >>> 16); b[6] = (byte) (version >>> 8); b[7] = (byte) version;
        for (int i = 8; i < b.length; i++) b[i] = (byte) (slot * 31 + version * 7 + i);
        return b;
    }

    /** Copies record content out during the read call (legal per A1). Verifies base ++ deltas byte-exactly. */
    static final class Capture implements RecordRead {
        byte[] bytes;
        boolean nullSeen;

        @Override public long onBytes(DataInput2 in, int size) {
            bytes = new byte[size];
            in.readFully(bytes);
            nullSeen = false;
            return size;
        }

        @Override public long onNull() {
            bytes = null;
            nullSeen = true;
            return -1;
        }
    }
}
