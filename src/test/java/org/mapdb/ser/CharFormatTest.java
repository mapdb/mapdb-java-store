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

/** Binds the {@link GroupFormat} TCK to {@link CharFormat}, plus unsigned-order fuzz. */
public class CharFormatTest extends GroupFormatTCK<Character> {

    @Override protected GroupFormat<Character> format() { return CharFormat.INSTANCE; }

    // char domain: 0..65535 unsigned; keep TCK values inside it.
    @Override protected Character gen(long v) { return (char) Math.toIntExact(v); }

    @Override protected long setProbeValue() { return 60_000L; }

    @Override protected long fuzzBound() { return 60_000L; }

    @Test
    public void supportsBinaryIsTrue() {
        assertTrue(CharFormat.INSTANCE.supportsBinary());
    }

    @Test
    public void representationIsCharArray() {
        assertTrue(CharFormat.INSTANCE.empty() instanceof char[]);
        Object g = CharFormat.INSTANCE.fromArray(new Object[]{'a', 'b', 'c'});
        assertTrue(g instanceof char[]);
        char[] raw = (char[]) g;
        assertTrue(raw.length == 3 && raw[0] == 'a' && raw[2] == 'c');
    }

    /**
     * Unsigned-order fuzz across the 0x8000 boundary — the exact point where a SIGNED
     * 16-bit read would mis-order. byte-side {@link GroupFormat#binarySearch} must agree
     * with the object side AND with a JDK control ({@code Collections.binarySearch} over
     * natural {@code Character} order, which is unsigned).
     */
    @Test
    public void unsignedOrder_fuzz_bytesideMatchesJdkControl() {
        Random rnd = new Random(0xC402);
        CharFormat f = CharFormat.INSTANCE;
        char[] boundaries = {0, 1, 0x7FFF, 0x8000, 0x8001, 0xFFFE, 0xFFFF, 'A', 0xD800, 0xDFFF};
        for (int round = 0; round < 200; round++) {
            TreeSet<Character> ctrl = new TreeSet<>();
            for (char b : boundaries) if (rnd.nextBoolean()) ctrl.add(b);
            int extra = rnd.nextInt(40);
            for (int i = 0; i < extra; i++) ctrl.add((char) rnd.nextInt(0x10000));
            List<Character> sorted = new ArrayList<>(ctrl);
            Object[] arr = sorted.toArray();
            Object g = f.fromArray(arr);
            int n = sorted.size();

            byte[] bytes = serialize(f, g);

            List<Character> probes = new ArrayList<>(sorted);
            for (char b : boundaries) probes.add(b);
            for (int i = 0; i < 30; i++) probes.add((char) rnd.nextInt(0x10000));

            for (char probe : probes) {
                int control = Collections.binarySearch(sorted, probe);
                assertEquals("object search vs JDK control", control, f.search(g, probe));
                DataInput2 in = new DataInput2.ByteArray(bytes, 0);
                assertEquals("byteside vs JDK control probe=" + (int) probe, control, f.binarySearch(probe, in, n));
                assertEquals("input at group end", n * 2, in.pos());
            }
        }
    }

    @Test
    public void binarySearch_doesNotMaterialize() {
        CharFormat f = CharFormat.INSTANCE;
        int n = 4096;
        Object[] arr = new Object[n];
        for (int i = 0; i < n; i++) arr[i] = (char) (i * 16); // spread across 0..65535
        Object g = f.fromArray(arr);
        byte[] bytes = serialize(f, g);
        Counting in = new Counting(bytes);
        int r = f.binarySearch((char) (7 * 16), in, n);
        assertEquals(f.search(g, (char) (7 * 16)), r);
        assertTrue("read too many bytes: " + in.bytesRead, in.bytesRead <= 64);
    }

    private static byte[] serialize(GroupFormat<?> f, Object g) {
        DataOutput2 out = new DataOutput2();
        f.serialize(out, g);
        return out.copyBytes();
    }

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
