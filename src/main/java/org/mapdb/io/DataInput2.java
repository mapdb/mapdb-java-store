package org.mapdb.io;

/**
 * Positioned read cursor over record bytes. Handed to formats/actions by the store;
 * valid only for the duration of the call. Seekable ({@link #pos(int)})
 * so fixed-stride formats can binary-search serialized bytes in place.
 *
 * Packed-long wire format (mapdb lineage): 7 bits per byte, most significant group first,
 * the terminating byte has bit 0x80 set. {@link #unpackLongSkip(int)} skips whole packed
 * values by scanning for terminator bytes without decoding.
 */
public abstract class DataInput2 {

    public abstract int pos();

    public abstract void pos(int pos);

    public abstract byte readByte();

    public abstract void readFully(byte[] b, int off, int len);

    public final void readFully(byte[] b) { readFully(b, 0, b.length); }

    public abstract void skipBytes(int n);

    public final int readUnsignedByte() { return readByte() & 0xFF; }

    public long readLong() {
        long r = 0;
        for (int i = 0; i < 8; i++) r = (r << 8) | readUnsignedByte();
        return r;
    }

    public int readInt() {
        int r = 0;
        for (int i = 0; i < 4; i++) r = (r << 8) | readUnsignedByte();
        return r;
    }

    public long unpackLong() {
        long ret = 0;
        byte v;
        do {
            v = readByte();
            ret = (ret << 7) | (v & 0x7F);
        } while ((v & 0x80) == 0);
        return ret;
    }

    public int unpackInt() {
        return (int) unpackLong();
    }

    /** Skip {@code count} packed longs without decoding them. */
    public void unpackLongSkip(int count) {
        while (count > 0) {
            if ((readByte() & 0x80) != 0) count--;
        }
    }

    /**
     * Compare the next {@code expected.length} bytes against {@code expected} without
     * copying. The position ALWAYS advances by {@code expected.length}, match or not
     * (callers scanning framed entries rely on that). Caller must ensure the bytes
     * are in range, exactly as with {@link #readFully}.
     */
    public boolean matchBytes(byte[] expected) {
        int p = pos();
        boolean match = true;
        for (int i = 0; i < expected.length && match; i++) {
            if (readByte() != expected[i]) match = false;
        }
        pos(p + expected.length);
        return match;
    }

    /** View over a byte[]; shares the array, never copies. */
    public static final class ByteArray extends DataInput2 {
        public final byte[] buf;
        public int pos;

        public ByteArray(byte[] buf, int pos) {
            this.buf = buf;
            this.pos = pos;
        }

        @Override public int pos() { return pos; }
        @Override public void pos(int pos) { this.pos = pos; }

        @Override public byte readByte() { return buf[pos++]; }

        @Override public void readFully(byte[] b, int off, int len) {
            System.arraycopy(buf, pos, b, off, len);
            pos += len;
        }

        @Override public void skipBytes(int n) { pos += n; }

        @Override public boolean matchBytes(byte[] expected) {
            int p = pos;
            pos = p + expected.length;
            return java.util.Arrays.equals(buf, p, pos, expected, 0, expected.length);
        }
    }

    /** View over a ByteBuffer using absolute gets; shares the buffer, never copies. */
    public static final class ByteBuf extends DataInput2 {
        public final java.nio.ByteBuffer buf;
        public int pos;

        public ByteBuf(java.nio.ByteBuffer buf, int pos) {
            this.buf = buf;
            this.pos = pos;
        }

        @Override public int pos() { return pos; }
        @Override public void pos(int pos) { this.pos = pos; }

        @Override public byte readByte() { return buf.get(pos++); }

        @Override public void readFully(byte[] b, int off, int len) {
            java.nio.ByteBuffer dup = buf.duplicate();
            dup.position(pos);
            dup.get(b, off, len);
            pos += len;
        }

        @Override public void skipBytes(int n) { pos += n; }

        @Override public long readLong() {
            long r = buf.getLong(pos);
            pos += 8;
            return r;
        }

        @Override public int readInt() {
            int r = buf.getInt(pos);
            pos += 4;
            return r;
        }

        @Override public boolean matchBytes(byte[] expected) {
            int p = pos;
            int len = expected.length;
            pos = p + len;
            int i = 0;
            for (; i + 8 <= len; i += 8) { // word-at-a-time: getLong is intrinsic
                if (buf.getLong(p + i) != (long) LONG_BE.get(expected, i)) return false;
            }
            for (; i < len; i++) {
                if (buf.get(p + i) != expected[i]) return false;
            }
            return true;
        }
    }

    /** Big-endian long view over byte[], for word-at-a-time compares. */
    private static final java.lang.invoke.VarHandle LONG_BE =
            java.lang.invoke.MethodHandles.byteArrayViewVarHandle(long[].class, java.nio.ByteOrder.BIG_ENDIAN);
}
