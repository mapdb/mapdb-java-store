package org.mapdb.io;

import java.util.Arrays;

/**
 * Growable output buffer for serialization. Takes an initial-capacity hint
 * (mapdb3's fixed 128-byte default was a known copy source).
 */
public final class DataOutput2 {

    public byte[] buf;
    public int pos;

    public DataOutput2() { this(128); }

    public DataOutput2(int sizeHint) {
        buf = new byte[Math.max(16, sizeHint)];
    }

    public void ensureAvail(int n) {
        int need = pos + n;
        if (need > buf.length) {
            int newSize = Math.max(need, buf.length * 2);
            buf = Arrays.copyOf(buf, newSize);
        }
    }

    public void writeByte(int v) {
        ensureAvail(1);
        buf[pos++] = (byte) v;
    }

    public void write(byte[] b, int off, int len) {
        ensureAvail(len);
        System.arraycopy(b, off, buf, pos, len);
        pos += len;
    }

    public void write(byte[] b) { write(b, 0, b.length); }

    public void writeInt(int v) {
        ensureAvail(4);
        buf[pos++] = (byte) (v >>> 24);
        buf[pos++] = (byte) (v >>> 16);
        buf[pos++] = (byte) (v >>> 8);
        buf[pos++] = (byte) v;
    }

    public void writeLong(long v) {
        ensureAvail(8);
        for (int shift = 56; shift >= 0; shift -= 8) buf[pos++] = (byte) (v >>> shift);
    }

    /** Packed long, see {@link DataInput2} for wire format. Value must be non-negative. */
    public void packLong(long value) {
        ensureAvail(10);
        int shift = 63 - Long.numberOfLeadingZeros(value);
        shift -= shift % 7;
        while (shift != 0) {
            buf[pos++] = (byte) ((value >>> shift) & 0x7F);
            shift -= 7;
        }
        buf[pos++] = (byte) ((value & 0x7F) | 0x80);
    }

    public void packInt(int value) { packLong(value & 0xFFFFFFFFL); }

    /** Copy of the written bytes, exact length. */
    public byte[] copyBytes() { return Arrays.copyOf(buf, pos); }
}
