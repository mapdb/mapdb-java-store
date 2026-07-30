package org.mapdb.ser;

import org.junit.Test;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Binds the {@link GroupFormat} TCK to {@link ShortFormat}, plus signed-boundary fuzz. */
public class ShortFormatTest extends GroupFormatTCK<Short> {

    @Override protected GroupFormat<Short> format() { return ShortFormat.INSTANCE; }

    // short domain: keep every TCK value in [Short.MIN, Short.MAX].
    @Override protected Short gen(long v) { return (short) Math.toIntExact(v); }

    @Override protected long setProbeValue() { return 30_000L; }

    @Override protected long fuzzBound() { return 30_000L; }

    @Test
    public void supportsBinaryIsTrue() {
        assertTrue(ShortFormat.INSTANCE.supportsBinary());
    }

    @Test
    public void representationIsShortArray() {
        assertTrue(ShortFormat.INSTANCE.empty() instanceof short[]);
        Object g = ShortFormat.INSTANCE.fromArray(new Object[]{(short) 1, (short) 2, (short) 3});
        assertTrue(g instanceof short[]);
        short[] raw = (short[]) g;
        assertTrue(raw.length == 3 && raw[0] == 1 && raw[2] == 3);
    }

    /**
     * Signed-order fuzz: random groups including the sign boundaries (MIN/MAX/-1/0/1),
     * byte-side {@link GroupFormat#binarySearch} must agree with the object side AND with an
     * independent JDK control ({@code Collections.binarySearch} over natural {@code Short}
     * order, which is signed). Catches any signed/unsigned wire-order mismatch.
     */
    @Test
    public void signedOrder_fuzz_bytesideMatchesJdkControl() {
        Random rnd = new Random(0x5401);
        ShortFormat f = ShortFormat.INSTANCE;
        short[] boundaries = {Short.MIN_VALUE, (short) (Short.MIN_VALUE + 1), -256, -1, 0, 1, 255, 256,
                (short) (Short.MAX_VALUE - 1), Short.MAX_VALUE};
        for (int round = 0; round < 200; round++) {
            TreeSet<Short> ctrl = new TreeSet<>();
            for (short b : boundaries) if (rnd.nextBoolean()) ctrl.add(b);
            int extra = rnd.nextInt(40);
            for (int i = 0; i < extra; i++) ctrl.add((short) rnd.nextInt(0x10000));
            List<Short> sorted = new ArrayList<>(ctrl);
            Object[] arr = sorted.toArray();
            Object g = f.fromArray(arr);
            int n = sorted.size();

            byte[] bytes = serialize(f, g);

            List<Short> probes = new ArrayList<>(sorted);
            for (short b : boundaries) probes.add(b);
            for (int i = 0; i < 30; i++) probes.add((short) rnd.nextInt(0x10000));

            for (short probe : probes) {
                int control = Collections.binarySearch(sorted, probe);
                assertEquals("object search vs JDK control", control, f.search(g, probe));
                DataInput2 in = new DataInput2.ByteArray(bytes, 0);
                assertEquals("byteside vs JDK control probe=" + probe, control, f.binarySearch(probe, in, n));
                assertEquals("input at group end", n * 2, in.pos());
            }
        }
    }

    /**
     * No-materialization proof: byte-side binarySearch on a large group must read only
     * O(log n) strides, never scan the whole group.
     */
    @Test
    public void binarySearch_doesNotMaterialize() {
        ShortFormat f = ShortFormat.INSTANCE;
        int n = 4096;
        Object[] arr = new Object[n];
        for (int i = 0; i < n; i++) arr[i] = (short) (i - 2048); // spans the sign boundary
        Object g = f.fromArray(arr);
        byte[] bytes = serialize(f, g);
        Counting in = new Counting(bytes);
        int r = f.binarySearch((short) 7, in, n);
        assertEquals(f.search(g, (short) 7), r);
        // log2(4096)=12 probes * 2 bytes = 24; allow generous slack, but far below n*2=8192.
        assertTrue("read too many bytes: " + in.bytesRead, in.bytesRead <= 64);
    }

    private static byte[] serialize(GroupFormat<?> f, Object g) {
        DataOutput2 out = new DataOutput2();
        f.serialize(out, g);
        return out.copyBytes();
    }

    /** Counts every byte fetched, so a materializing search would be caught. */
    static final class Counting extends DataInput2 {
        final byte[] buf;
        int pos, bytesRead;
        Counting(byte[] b) { this.buf = b; }
        @Override public int pos() { return pos; }
        @Override public void pos(int pos) { this.pos = pos; }
        @Override public byte readByte() { bytesRead++; return buf[pos++]; }
        @Override public void readFully(byte[] b, int off, int len) {
            System.arraycopy(buf, pos, b, off, len); pos += len; bytesRead += len;
        }
        @Override public void skipBytes(int n) { pos += n; }
    }
}
