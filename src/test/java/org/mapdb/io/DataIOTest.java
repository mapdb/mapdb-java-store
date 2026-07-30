package org.mapdb.io;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the byte IO layer: {@link DataOutput2} (growable output, packed-long
 * wire format) and {@link DataInput2} (seekable views over byte[] and ByteBuffer).
 */
public class DataIOTest {

    // ---- helpers -------------------------------------------------------------

    private static DataInput2.ByteArray inArray(byte[] b, int pos) {
        return new DataInput2.ByteArray(b, pos);
    }

    private static DataInput2.ByteBuf inHeapBuf(byte[] b, int pos) {
        ByteBuffer bb = ByteBuffer.allocate(b.length);
        bb.put(b);
        bb.clear();
        return new DataInput2.ByteBuf(bb, pos);
    }

    private static DataInput2.ByteBuf inDirectBuf(byte[] b, int pos) {
        ByteBuffer bb = ByteBuffer.allocateDirect(b.length);
        bb.put(b);
        bb.clear();
        return new DataInput2.ByteBuf(bb, pos);
    }

    private static long packUnpack(long value) {
        DataOutput2 out = new DataOutput2();
        out.packLong(value);
        DataInput2 in = inArray(out.copyBytes(), 0);
        long r = in.unpackLong();
        // the input must be positioned exactly at the end of the packed value
        assertEquals("packLong consumed wrong byte count for " + value, out.pos, in.pos());
        return r;
    }

    // ---- packLong / unpackLong ----------------------------------------------

    @Test
    public void packLong_fixedValues() {
        long[] vals = {0, 1, 127, 128, 16383, 16384, Long.MAX_VALUE};
        for (long v : vals) {
            assertEquals(v, packUnpack(v));
        }
    }

    @Test
    public void packLong_powersOfTwoAndNeighbours() {
        for (int i = 0; i <= 62; i++) {
            long p = 1L << i;
            assertEquals(p, packUnpack(p));
            long minus = p - 1;
            if (minus >= 0) assertEquals(minus, packUnpack(minus));
            long plus = p + 1;
            if (plus >= 0) assertEquals(plus, packUnpack(plus));
        }
    }

    @Test
    public void packLong_random() {
        Random rnd = new Random(0xCAFEBABEL);
        for (int i = 0; i < 10_000; i++) {
            long v = rnd.nextLong() & Long.MAX_VALUE; // non-negative
            assertEquals(v, packUnpack(v));
        }
    }

    @Test
    public void packLong_zeroIsSingleByte() {
        DataOutput2 out = new DataOutput2();
        out.packLong(0);
        assertEquals("packLong(0) must emit a single terminator byte", 1, out.pos);
    }

    // ---- packInt / unpackInt -------------------------------------------------

    @Test
    public void packInt_nonNegativeRoundTrip() {
        int[] vals = {0, 1, 127, 128, 16383, 16384, 65535, Integer.MAX_VALUE};
        for (int v : vals) {
            DataOutput2 out = new DataOutput2();
            out.packInt(v);
            DataInput2 in = inArray(out.copyBytes(), 0);
            assertEquals(v, in.unpackInt());
        }
    }

    @Test
    public void packInt_negativeRoundTripsThroughUnsignedLongPath() {
        // packInt masks with 0xFFFFFFFFL, so a negative int is packed as its unsigned
        // 32-bit value (a full 5-byte packed long). unpackInt() truncates the decoded
        // long back to int, restoring the original negative value.
        int[] vals = {-1, -2, Integer.MIN_VALUE, -12345678};
        for (int v : vals) {
            DataOutput2 out = new DataOutput2();
            out.packInt(v);
            byte[] bytes = out.copyBytes();
            DataInput2 in = inArray(bytes, 0);
            // document: the decoded long is the unsigned 32-bit value...
            long asLong = new DataInput2.ByteArray(bytes, 0).unpackLong();
            assertEquals((v & 0xFFFFFFFFL), asLong);
            // ...and unpackInt truncates it back to the negative int
            assertEquals(v, in.unpackInt());
        }
    }

    // ---- unpackLongSkip ------------------------------------------------------

    @Test
    public void unpackLongSkip_skipsExactlyN() {
        Random rnd = new Random(42);
        int n = 100;
        long[] vals = new long[n];
        DataOutput2 out = new DataOutput2();
        for (int i = 0; i < n; i++) {
            vals[i] = rnd.nextLong() & Long.MAX_VALUE;
            out.packLong(vals[i]);
        }
        long sentinel = 0x1234_5678_9ABCL;
        out.packLong(sentinel);

        DataInput2 in = inArray(out.copyBytes(), 0);
        in.unpackLongSkip(n);
        assertEquals("after skipping N values the next read must be the sentinel",
                sentinel, in.unpackLong());
    }

    @Test
    public void unpackLongSkip_partialThenRead() {
        DataOutput2 out = new DataOutput2();
        for (int i = 0; i < 10; i++) out.packLong(i);
        DataInput2 in = inArray(out.copyBytes(), 0);
        in.unpackLongSkip(3);
        assertEquals(3, in.unpackLong());
        in.unpackLongSkip(2);
        assertEquals(6, in.unpackLong());
    }

    // ---- writeLong / readLong / writeInt / readInt ---------------------------

    private static final long[] LONGS = {
            0L, 1L, -1L, 2L, -2L, 127L, -128L, 1234567890123L, -1234567890123L,
            Long.MAX_VALUE, Long.MIN_VALUE
    };
    private static final int[] INTS = {
            0, 1, -1, 2, -2, 127, -128, 65535, 1234567, -1234567,
            Integer.MAX_VALUE, Integer.MIN_VALUE
    };

    @Test
    public void writeReadLong_allInputImpls() {
        DataOutput2 out = new DataOutput2();
        for (long v : LONGS) out.writeLong(v);
        byte[] bytes = out.copyBytes();
        for (DataInput2 in : new DataInput2[]{inArray(bytes, 0), inHeapBuf(bytes, 0), inDirectBuf(bytes, 0)}) {
            for (long v : LONGS) assertEquals(v, in.readLong());
        }
    }

    @Test
    public void writeReadInt_allInputImpls() {
        DataOutput2 out = new DataOutput2();
        for (int v : INTS) out.writeInt(v);
        byte[] bytes = out.copyBytes();
        for (DataInput2 in : new DataInput2[]{inArray(bytes, 0), inHeapBuf(bytes, 0), inDirectBuf(bytes, 0)}) {
            for (int v : INTS) assertEquals(v, in.readInt());
        }
    }

    @Test
    public void readByteAndUnsignedByte() {
        DataOutput2 out = new DataOutput2();
        out.writeByte(0x7F);
        out.writeByte(0x80);
        out.writeByte(0xFF);
        byte[] bytes = out.copyBytes();
        for (DataInput2 in : new DataInput2[]{inArray(bytes, 0), inHeapBuf(bytes, 0), inDirectBuf(bytes, 0)}) {
            assertEquals((byte) 0x7F, in.readByte());
            assertEquals(0x80, in.readUnsignedByte());
            assertEquals(0xFF, in.readUnsignedByte());
        }
    }

    // ---- pos() / pos(int) seeking -------------------------------------------

    @Test
    public void seeking_allInputImpls() {
        DataOutput2 out = new DataOutput2();
        for (long v : LONGS) out.writeLong(v);
        byte[] bytes = out.copyBytes();
        for (DataInput2 in : new DataInput2[]{inArray(bytes, 0), inHeapBuf(bytes, 0), inDirectBuf(bytes, 0)}) {
            // read the 4th long by seeking
            in.pos(3 * 8);
            assertEquals(3 * 8, in.pos());
            assertEquals(LONGS[3], in.readLong());
            assertEquals(4 * 8, in.pos());
            // jump backwards to the first
            in.pos(0);
            assertEquals(LONGS[0], in.readLong());
            // jump to the last
            in.pos((LONGS.length - 1) * 8);
            assertEquals(LONGS[LONGS.length - 1], in.readLong());
        }
    }

    @Test
    public void skipBytes_allInputImpls() {
        DataOutput2 out = new DataOutput2();
        for (long v : LONGS) out.writeLong(v);
        byte[] bytes = out.copyBytes();
        for (DataInput2 in : new DataInput2[]{inArray(bytes, 0), inHeapBuf(bytes, 0), inDirectBuf(bytes, 0)}) {
            in.skipBytes(16);
            assertEquals(16, in.pos());
            assertEquals(LONGS[2], in.readLong());
        }
    }

    // ---- readFully bulk reads ------------------------------------------------

    @Test
    public void readFully_allInputImpls() {
        byte[] payload = new byte[777];
        new Random(7).nextBytes(payload);
        DataOutput2 out = new DataOutput2();
        out.writeByte(0xAB); // leading byte so payload starts at offset 1
        out.write(payload);
        out.writeByte(0xCD);
        byte[] bytes = out.copyBytes();

        for (DataInput2 in : new DataInput2[]{inArray(bytes, 0), inHeapBuf(bytes, 0), inDirectBuf(bytes, 0)}) {
            assertEquals(0xAB, in.readUnsignedByte());
            byte[] got = new byte[payload.length];
            in.readFully(got);
            assertArrayEquals(payload, got);
            assertEquals(0xCD, in.readUnsignedByte());
        }
    }

    @Test
    public void readFully_withOffsetAndLen() {
        byte[] payload = new byte[100];
        new Random(9).nextBytes(payload);
        DataOutput2 out = new DataOutput2();
        out.write(payload);
        byte[] bytes = out.copyBytes();
        for (DataInput2 in : new DataInput2[]{inArray(bytes, 0), inHeapBuf(bytes, 0), inDirectBuf(bytes, 0)}) {
            byte[] got = new byte[120];
            in.readFully(got, 10, 100);
            for (int i = 0; i < 100; i++) assertEquals(payload[i], got[10 + i]);
            assertEquals(100, in.pos());
        }
    }

    // ---- DataOutput2 growth --------------------------------------------------

    @Test
    public void growth_startSmallWrite1MB() {
        DataOutput2 out = new DataOutput2(16);
        assertEquals(16, out.buf.length);
        int n = 1 << 20; // 1 MB
        byte[] data = new byte[n];
        new Random(123).nextBytes(data);
        out.write(data);
        assertEquals(n, out.pos);
        assertTrue("buffer must have grown to hold 1MB", out.buf.length >= n);
        assertArrayEquals(data, out.copyBytes());
    }

    @Test
    public void ensureAvail_doublingBehavior() {
        DataOutput2 out = new DataOutput2(16);
        assertEquals(16, out.buf.length);
        // fill to 16, still 16
        for (int i = 0; i < 16; i++) out.writeByte(i);
        assertEquals(16, out.buf.length);
        // 17th byte: need=17 > 16 -> newSize = max(17, 32) = 32
        out.writeByte(0);
        assertEquals(32, out.buf.length);
        // fill to 32, then 33rd byte -> newSize = max(33, 64) = 64
        for (int i = 17; i < 32; i++) out.writeByte(i);
        assertEquals(32, out.buf.length);
        out.writeByte(0);
        assertEquals(64, out.buf.length);
    }

    @Test
    public void ensureAvail_largeSingleRequestUsesExactNeed() {
        DataOutput2 out = new DataOutput2(16);
        out.ensureAvail(1000); // need=1000 > 32 -> newSize = max(1000, 32) = 1000
        assertEquals(1000, out.buf.length);
    }

    @Test
    public void copyBytes_exactLength() {
        DataOutput2 out = new DataOutput2(1024);
        for (int i = 0; i < 300; i++) out.writeByte(i);
        byte[] copy = out.copyBytes();
        assertEquals(300, copy.length);
        for (int i = 0; i < 300; i++) assertEquals((byte) i, copy[i]);
    }

    // ---- ByteBuf view at a non-zero offset inside a page ---------------------

    @Test
    public void byteBufView_recordInsideAPageAtNonZeroOffset() {
        int pageSize = 4096;
        int recordOffset = 1000;

        // build the record bytes
        DataOutput2 rec = new DataOutput2();
        rec.writeLong(Long.MIN_VALUE);
        rec.writeInt(-42);
        rec.packLong(123456789L);
        byte[] recBytes = rec.copyBytes();

        // fill a page with garbage, then splice the record at recordOffset
        byte[] page = new byte[pageSize];
        new Random(555).nextBytes(page);
        System.arraycopy(recBytes, 0, page, recordOffset, recBytes.length);

        for (DataInput2 in : new DataInput2[]{inHeapBuf(page, recordOffset), inDirectBuf(page, recordOffset),
                inArray(page, recordOffset)}) {
            assertEquals(Long.MIN_VALUE, in.readLong());
            assertEquals(-42, in.readInt());
            assertEquals(123456789L, in.unpackLong());
            assertEquals(recordOffset + recBytes.length, in.pos());
        }
    }

    @Test
    public void byteBufView_absoluteReadsIgnoreBufferPosition() {
        // ByteBuf uses absolute gets, so the buffer's own cursor position is
        // irrelevant. (Note: absolute gets DO validate against the buffer limit,
        // so the caller must give a buffer whose limit covers the record — the
        // limit is left at capacity here, matching how a page volume is handed over.)
        DataOutput2 rec = new DataOutput2();
        rec.writeLong(0xDEADBEEFCAFEL);
        byte[] recBytes = rec.copyBytes();
        byte[] page = new byte[512];
        System.arraycopy(recBytes, 0, page, 200, recBytes.length);

        ByteBuffer bb = ByteBuffer.allocate(512);
        bb.put(page);
        bb.position(10); // misleading cursor; absolute reads must ignore it
        DataInput2.ByteBuf in = new DataInput2.ByteBuf(bb, 200);
        assertEquals(0xDEADBEEFCAFEL, in.readLong());
        assertEquals(208, in.pos());
    }

    @Test
    public void unpackLong_terminatorDetection() {
        // sanity: a value whose low group is 0 still terminates correctly
        for (long v : new long[]{0, 128, 128L * 128, 0x80808080L}) {
            assertEquals(v, packUnpack(v));
        }
    }

    // ---- matchBytes -----------------------------------------------------------

    private interface InFactory { DataInput2 make(byte[] b, int pos); }

    @Test
    public void matchBytes_matchMismatchAndPositionContract() {
        byte[] data = {9, 9, 1, 2, 3, 4, 5, 42};
        InFactory[] factories = {DataIOTest::inArray, DataIOTest::inHeapBuf, DataIOTest::inDirectBuf};
        byte[][] probes = {
                {1, 2, 3, 4, 5},   // match
                {1, 2, 3, 4, 6},   // last byte differs
                {0, 2, 3, 4, 5},   // first byte differs
                {1, 2, 9, 4, 5},   // middle byte differs
        };
        boolean[] expect = {true, false, false, false};
        for (InFactory f : factories) {
            for (int i = 0; i < probes.length; i++) {
                DataInput2 in = f.make(data, 2);
                assertEquals("probe " + i, expect[i], in.matchBytes(probes[i]));
                // pos advances by expected.length, match or not
                assertEquals("pos after probe " + i, 7, in.pos());
                assertEquals(42, in.readByte());
            }
            // empty probe: trivially matches, no movement
            DataInput2 in = f.make(data, 2);
            assertTrue(in.matchBytes(new byte[0]));
            assertEquals(2, in.pos());
        }
    }
}
