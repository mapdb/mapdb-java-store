package org.mapdb.ser;

import org.junit.Test;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Binds the {@link GroupFormat} TCK to {@link UUIDFormat}, plus signed-long-pair order fuzz. */
public class UUIDFormatTest extends GroupFormatTCK<UUID> {

    @Override protected GroupFormat<UUID> format() { return UUIDFormat.INSTANCE; }

    /** Order-preserving for non-negative v: msb fixed at 0, lsb = v (positive ⇒ signed-increasing). */
    @Override protected UUID gen(long v) { return new UUID(0, v); }

    @Test
    public void supportsBinaryIsTrue() {
        assertTrue(UUIDFormat.INSTANCE.supportsBinary());
    }

    @Test
    public void representationIsLongPairArray() {
        assertTrue(UUIDFormat.INSTANCE.empty() instanceof long[]);
        UUID a = new UUID(1, 2), b = new UUID(3, 4);
        Object g = UUIDFormat.INSTANCE.fromArray(new Object[]{a, b});
        assertTrue(g instanceof long[]);
        long[] raw = (long[]) g;
        assertEquals(4, raw.length);
        assertTrue(raw[0] == 1 && raw[1] == 2 && raw[2] == 3 && raw[3] == 4);
        assertEquals(2, UUIDFormat.INSTANCE.size(g));
    }

    /**
     * The key correctness test: insert N random UUIDs spanning signed-long boundaries on
     * BOTH halves, sort a control {@code TreeSet<UUID>} (which uses {@link UUID#compareTo},
     * i.e. signed msb then signed lsb), build the group, and assert byte-side binarySearch
     * finds every present key at its sorted index and reports the correct insertion point
     * for absent keys — via an independent JDK control ({@code Collections.binarySearch}).
     * Any signed/unsigned or msb/lsb ordering mismatch surfaces here.
     */
    @Test
    public void signedLongPairOrder_fuzz_bytesideMatchesTreeSet() {
        Random rnd = new Random(0x0DDBA11);
        UUIDFormat f = UUIDFormat.INSTANCE;
        UUID[] boundaries = {
                new UUID(0, 0),
                new UUID(-1, -1),                                   // all ones
                new UUID(Long.MIN_VALUE, 0),
                new UUID(Long.MAX_VALUE, -1),
                new UUID(0, Long.MIN_VALUE),
                new UUID(0, Long.MAX_VALUE),
                new UUID(5, Long.MIN_VALUE),                        // msb tie, lsb differs
                new UUID(5, Long.MAX_VALUE),
                new UUID(5, 0),
                new UUID(-1, 0),
                new UUID(Long.MIN_VALUE, Long.MAX_VALUE),
                new UUID(Long.MAX_VALUE, Long.MIN_VALUE),
        };
        for (int round = 0; round < 300; round++) {
            TreeSet<UUID> ctrl = new TreeSet<>();
            for (UUID u : boundaries) if (rnd.nextBoolean()) ctrl.add(u);
            int extra = rnd.nextInt(50);
            for (int i = 0; i < extra; i++) ctrl.add(randomUuid(rnd));
            List<UUID> sorted = new ArrayList<>(ctrl);
            Object g = f.fromArray(sorted.toArray());
            int n = sorted.size();

            DataOutput2 out = new DataOutput2();
            f.serialize(out, g);
            byte[] bytes = out.copyBytes();

            List<UUID> probes = new ArrayList<>(sorted);            // hits
            for (UUID u : boundaries) probes.add(u);                 // present-or-absent boundaries
            for (int i = 0; i < 40; i++) probes.add(randomUuid(rnd)); // mostly misses

            for (UUID probe : probes) {
                int control = Collections.binarySearch(sorted, probe); // JDK control, UUID natural order
                assertEquals("object search vs TreeSet control", control, f.search(g, probe));
                DataInput2 in = new DataInput2.ByteArray(bytes, 0);
                assertEquals("byteside vs TreeSet control probe=" + probe, control, f.binarySearch(probe, in, n));
                assertEquals("input at group end", n * 16, in.pos());
            }
        }
    }

    @Test
    public void binarySearch_doesNotMaterialize() {
        UUIDFormat f = UUIDFormat.INSTANCE;
        int n = 4096;
        TreeSet<UUID> ctrl = new TreeSet<>();
        Random rnd = new Random(7);
        while (ctrl.size() < n) ctrl.add(randomUuid(rnd));
        List<UUID> sorted = new ArrayList<>(ctrl);
        Object g = f.fromArray(sorted.toArray());
        DataOutput2 out = new DataOutput2();
        f.serialize(out, g);
        Counting in = new Counting(out.copyBytes());
        UUID key = sorted.get(1234);
        int r = f.binarySearch(key, in, n);
        assertEquals(1234, r);
        // ~12 probes * 16 bytes = 192; far below n*16 = 65536.
        assertTrue("read too many bytes: " + in.bytesRead, in.bytesRead <= 512);
    }

    private static UUID randomUuid(Random rnd) {
        // full 128-bit range incl. negative halves (NOT UUID.randomUUID, which fixes version bits)
        return new UUID(rnd.nextLong(), rnd.nextLong());
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
