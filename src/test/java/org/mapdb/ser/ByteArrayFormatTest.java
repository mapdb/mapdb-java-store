package org.mapdb.ser;

import org.junit.Test;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Binds the {@link GroupFormat} TCK to {@link ByteArrayFormat} and pins its UNSIGNED order. */
public class ByteArrayFormatTest extends GroupFormatTCK<byte[]> {

    @Override protected GroupFormat<byte[]> format() { return ByteArrayFormat.INSTANCE; }

    /** Zero-padded 20 ASCII digits: lexicographic (signed==unsigned) order == numeric order. */
    @Override protected byte[] gen(long v) {
        return String.format("%020d", v).getBytes(StandardCharsets.US_ASCII);
    }

    @Override protected Object view(byte[] v) { return Arrays.toString(v); }

    @Test
    public void supportsBinaryIsTrue() {
        assertTrue(ByteArrayFormat.INSTANCE.supportsBinary());
    }

    // ---- the order-consistency invariant ---------------
    // element().compare == object-side search order == byte-side binarySearch order,
    // exercised with sign-boundary bytes where UNSIGNED differs from signed order.

    /** Pool sorted by UNSIGNED order; under signed Arrays.compare {0x80}/{0xFF} would sort first. */
    private static byte[][] unsignedSortedPool() {
        return new byte[][]{
                {},
                {0x00},
                {0x00, (byte) 0xFF},
                {0x01},
                {0x7F},
                {0x7F, 0x00},
                {(byte) 0x80},          // signed: negative, would sort before {0x00}
                {(byte) 0x80, 0x01},
                {(byte) 0xFF},          // signed: most negative single byte
                {(byte) 0xFF, (byte) 0xFF},
        };
    }

    @Test
    public void elementOrder_isUnsigned() {
        Serializer<byte[]> e = ByteArrayFormat.INSTANCE.element();
        assertTrue("element must be BYTE_ARRAY_UNSIGNED", e == Serializers.BYTE_ARRAY_UNSIGNED);
        byte[][] pool = unsignedSortedPool();
        for (int i = 0; i < pool.length; i++) {
            for (int j = 0; j < pool.length; j++) {
                assertEquals("compare(" + Arrays.toString(pool[i]) + "," + Arrays.toString(pool[j]) + ")",
                        Integer.signum(Arrays.compareUnsigned(pool[i], pool[j])),
                        Integer.signum(e.compare(pool[i], pool[j])));
            }
        }
        // sanity: the divergence from signed BYTE_ARRAY order is real for this pool
        assertTrue(Serializers.BYTE_ARRAY.compare(new byte[]{(byte) 0xFF}, new byte[]{0x01}) < 0);
        assertTrue(Serializers.BYTE_ARRAY_UNSIGNED.compare(new byte[]{(byte) 0xFF}, new byte[]{0x01}) > 0);
    }

    @Test
    public void orderInvariant_elementCompare_search_binarySearch_agree() {
        ByteArrayFormat f = ByteArrayFormat.INSTANCE;
        byte[][] pool = unsignedSortedPool();
        // pool must be strictly ascending in the format's declared order
        for (int i = 1; i < pool.length; i++) {
            assertTrue(f.element().compare(pool[i - 1], pool[i]) < 0);
        }
        Object g = f.fromArray(pool);
        DataOutput2 out = new DataOutput2();
        f.serialize(out, g);
        byte[] bytes = out.copyBytes();

        List<byte[]> probes = new ArrayList<>(Arrays.asList(pool));
        probes.add(new byte[]{0x40});
        probes.add(new byte[]{(byte) 0x80, 0x00});
        probes.add(new byte[]{(byte) 0xC0});
        probes.add(new byte[]{(byte) 0xFF, (byte) 0xFF, 0x00}); // above all
        for (byte[] probe : probes) {
            int obj = f.search(g, probe);
            // reference: linear scan by element().compare
            int expected = -(pool.length + 1);
            for (int i = 0; i < pool.length; i++) {
                int c = f.element().compare(pool[i], probe);
                if (c == 0) { expected = i; break; }
                if (c > 0) { expected = -(i + 1); break; }
            }
            assertEquals("search vs element().compare for " + Arrays.toString(probe), expected, obj);
            DataInput2 in = new DataInput2.ByteArray(bytes, 0);
            assertEquals("binarySearch vs search for " + Arrays.toString(probe),
                    obj, f.binarySearch(probe, in, pool.length));
            assertEquals("input at group end", bytes.length, in.pos());
        }
    }

    /** Randomized equivalence with HIGH bytes (the TCK's gen is ASCII-only). */
    @Test
    public void binaryEquivalence_randomHighBytes() {
        ByteArrayFormat f = ByteArrayFormat.INSTANCE;
        Random rnd = new Random(0xB17E5);
        for (int round = 0; round < 40; round++) {
            int n = rnd.nextInt(40);
            TreeSet<byte[]> set = new TreeSet<>(Arrays::compareUnsigned);
            while (set.size() < n) {
                byte[] b = new byte[rnd.nextInt(12)]; // includes empty
                rnd.nextBytes(b);
                set.add(b);
            }
            byte[][] pool = set.toArray(new byte[0][]);
            Object g = f.fromArray(pool);
            DataOutput2 out = new DataOutput2();
            f.serialize(out, g);
            byte[] bytes = out.copyBytes();

            List<byte[]> probes = new ArrayList<>(Arrays.asList(pool));
            for (int p = 0; p < 15; p++) {
                byte[] b = new byte[rnd.nextInt(12)];
                rnd.nextBytes(b);
                probes.add(b);
            }
            for (byte[] probe : probes) {
                DataInput2 in = new DataInput2.ByteArray(bytes, 0);
                assertEquals(f.search(g, probe), f.binarySearch(probe, in, n));
                assertEquals(bytes.length, in.pos());
            }
            for (int pos = 0; pos < n; pos++) {
                DataInput2 in = new DataInput2.ByteArray(bytes, 0);
                assertEquals(Arrays.toString(pool[pos]), Arrays.toString(f.binaryGet(in, n, pos)));
                assertEquals(bytes.length, in.pos());
            }
        }
    }
}
