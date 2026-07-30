package org.mapdb.ser;

import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * A typed component of a composite key handled by {@link TupleFormat}, providing an
 * ORDER-PRESERVING (memcomparable) per-component codec: the unsigned byte order of a
 * component's encoding equals its {@link #compare logical order}, so a concatenation of
 * component encodings is memcomparable at the whole-tuple level.
 *
 * <h3>Encodings</h3>
 * <ul>
 *   <li>{@link #INT}/{@link #LONG}: fixed-width big-endian with the SIGN BIT FLIPPED
 *       ({@code v ^ MIN_VALUE}). This maps the signed range onto the unsigned range
 *       monotonically ({@code MIN_VALUE -> 0x00..}, {@code MAX_VALUE -> 0xFF..}), so
 *       big-endian unsigned byte compare equals signed integer order. Fixed width makes
 *       the component self-delimiting.</li>
 *   <li>{@link #STRING}/{@link #BYTES}: variable length, encoded with the
 *       escaped-terminated codec — each {@code 0x00} payload byte becomes {@code 0x00 0xFF},
 *       every other byte is copied verbatim, and a {@code 0x00 0x00} terminator is appended.
 *       This is order-preserving under unsigned compare and prefix-free (the only unescaped
 *       {@code 0x00 0x00} is the terminator), so a variable-length component can be followed
 *       by further components without the boundary corrupting order.</li>
 * </ul>
 *
 * STRING order is UTF-8 unsigned byte order (Unicode code-point order), matching the
 * encoding. This deliberately differs from {@link String#compareTo} (UTF-16 code-unit
 * order) for supplementary characters — {@link StringGroupFormat} preserves the latter
 * via {@link Utf8}; a memcomparable tuple key uses the former, the natural order of its
 * bytes. For BMP-only strings the two coincide.
 */
public enum TupleComponent {

    /** Signed 32-bit int, 4-byte big-endian, sign bit flipped. */
    INT {
        @Override void encode(DataOutput2 out, Object value) {
            out.writeInt(((Integer) value) ^ 0x80000000);
        }
        @Override Object decode(DataInput2 in, int end) {
            if (in.pos() + 4 > end) throw new IllegalStateException("corrupt tuple: truncated int component");
            return in.readInt() ^ 0x80000000;
        }
        @Override int compare(Object a, Object b) { return Integer.compare((Integer) a, (Integer) b); }
        @Override boolean equalTo(Object a, Object b) { return ((Integer) a).intValue() == ((Integer) b).intValue(); }
    },

    /** Signed 64-bit long, 8-byte big-endian, sign bit flipped. */
    LONG {
        @Override void encode(DataOutput2 out, Object value) {
            out.writeLong(((Long) value) ^ 0x8000000000000000L);
        }
        @Override Object decode(DataInput2 in, int end) {
            if (in.pos() + 8 > end) throw new IllegalStateException("corrupt tuple: truncated long component");
            return in.readLong() ^ 0x8000000000000000L;
        }
        @Override int compare(Object a, Object b) { return Long.compare((Long) a, (Long) b); }
        @Override boolean equalTo(Object a, Object b) { return ((Long) a).longValue() == ((Long) b).longValue(); }
    },

    /** UTF-8 string, escaped-terminated; UTF-8 unsigned (code-point) order. */
    STRING {
        @Override void encode(DataOutput2 out, Object value) {
            writeEscaped(out, ((String) value).getBytes(StandardCharsets.UTF_8));
        }
        @Override Object decode(DataInput2 in, int end) {
            return new String(readEscaped(in, end), StandardCharsets.UTF_8);
        }
        @Override int compare(Object a, Object b) {
            return Arrays.compareUnsigned(
                    ((String) a).getBytes(StandardCharsets.UTF_8),
                    ((String) b).getBytes(StandardCharsets.UTF_8));
        }
        @Override boolean equalTo(Object a, Object b) { return a.equals(b); }
    },

    /** Raw byte[], escaped-terminated; unsigned lexicographic order. */
    BYTES {
        @Override void encode(DataOutput2 out, Object value) {
            writeEscaped(out, (byte[]) value);
        }
        @Override Object decode(DataInput2 in, int end) {
            return readEscaped(in, end);
        }
        @Override int compare(Object a, Object b) { return Arrays.compareUnsigned((byte[]) a, (byte[]) b); }
        @Override boolean equalTo(Object a, Object b) { return Arrays.equals((byte[]) a, (byte[]) b); }
    };

    /** Append the memcomparable encoding of {@code value} to {@code out}. */
    abstract void encode(DataOutput2 out, Object value);

    /**
     * Read exactly one component from {@code in}, which must not advance past {@code end}
     * (the exclusive end of the tuple's encoded bytes). Throws on torn/corrupt input.
     */
    abstract Object decode(DataInput2 in, int end);

    /** Logical order of this component, equal to the unsigned byte order of {@link #encode}. */
    abstract int compare(Object a, Object b);

    /** Logical equality (value-based). */
    abstract boolean equalTo(Object a, Object b);

    // ---- escaped-terminated codec for variable-length components ----

    private static void writeEscaped(DataOutput2 out, byte[] payload) {
        for (byte value : payload) {
            if (value == 0x00) { out.writeByte(0x00); out.writeByte(0xFF); }
            else out.writeByte(value);
        }
        out.writeByte(0x00); // terminator 0x00 0x00 (never produced by an escaped 0x00, which is 0x00 0xFF)
        out.writeByte(0x00);
    }

    private static byte[] readEscaped(DataInput2 in, int end) {
        byte[] buf = new byte[16];
        int len = 0;
        while (true) {
            if (in.pos() >= end) throw new IllegalStateException("corrupt tuple: unterminated component");
            int b = in.readByte() & 0xFF;
            if (b == 0x00) {
                if (in.pos() >= end) throw new IllegalStateException("corrupt tuple: dangling escape");
                int b2 = in.readByte() & 0xFF;
                if (b2 == 0x00) break;                 // terminator
                if (b2 != 0xFF) throw new IllegalStateException("corrupt tuple: bad escape 0x00 " + b2);
                b = 0x00;                              // 0x00 0xFF -> literal 0x00
            }
            if (len == buf.length) buf = Arrays.copyOf(buf, buf.length * 2);
            buf[len++] = (byte) b;
        }
        return len == buf.length ? buf : Arrays.copyOf(buf, len);
    }
}
