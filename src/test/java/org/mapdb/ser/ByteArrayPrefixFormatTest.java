package org.mapdb.ser;

import org.junit.Test;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Binds the {@link GroupFormat} TCK to {@link ByteArrayPrefixFormat} (front-coding,
 * R7) and pins its UNSIGNED order, matching {@link ByteArrayFormat} exactly. Adds
 * prefix-specific coverage: restart-boundary sizes, shared-prefix-heavy corpora,
 * unsigned high-byte order, a TreeMap fuzz control, and byte-side non-materialization.
 */
public class ByteArrayPrefixFormatTest extends GroupFormatTCK<byte[]> {

    private static final int K = ByteArrayPrefixFormat.RESTART_INTERVAL;
    private final ByteArrayPrefixFormat fmt = ByteArrayPrefixFormat.INSTANCE;

    @Override protected GroupFormat<byte[]> format() { return fmt; }

    /** Zero-padded 20 ASCII digits: lexicographic (signed==unsigned) order == numeric AND heavy shared prefixes. */
    @Override protected byte[] gen(long v) {
        return String.format("%020d", v).getBytes(StandardCharsets.US_ASCII);
    }

    @Override protected Object view(byte[] v) { return Arrays.toString(v); }

    @Test
    public void supportsBinaryIsTrue() {
        assertTrue(fmt.supportsBinary());
    }

    // ---- order invariant with sign-boundary bytes (UNSIGNED != signed) -----------

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
    public void elementOrder_isUnsigned_sameAsByteArrayFormat() {
        Serializer<byte[]> e = fmt.element();
        assertTrue("element must be BYTE_ARRAY_UNSIGNED", e == Serializers.BYTE_ARRAY_UNSIGNED);
        assertTrue("element must match ByteArrayFormat's", e == ByteArrayFormat.INSTANCE.element());
        byte[][] pool = unsignedSortedPool();
        for (byte[] a : pool)
            for (byte[] b : pool)
                assertEquals("compare(" + Arrays.toString(a) + "," + Arrays.toString(b) + ")",
                        Integer.signum(Arrays.compareUnsigned(a, b)),
                        Integer.signum(e.compare(a, b)));
    }

    @Test
    public void comparator_isNonNullUnsigned() {
        // byte[] is NOT natural-Comparable order, so the format must advertise a comparator.
        java.util.Comparator<byte[]> c = fmt.comparator();
        assertTrue("comparator must be non-null for byte[] order", c != null);
        byte[][] pool = unsignedSortedPool();
        for (byte[] a : pool)
            for (byte[] b : pool)
                assertEquals(Integer.signum(Arrays.compareUnsigned(a, b)), Integer.signum(c.compare(a, b)));
    }

    @Test
    public void orderInvariant_elementCompare_search_binarySearch_agree() {
        byte[][] pool = unsignedSortedPool();
        for (int i = 1; i < pool.length; i++)
            assertTrue(fmt.element().compare(pool[i - 1], pool[i]) < 0);
        Object g = fmt.fromArray(pool);
        DataOutput2 out = new DataOutput2();
        fmt.serialize(out, g);
        out.writeLong(0x600DF00DL);
        byte[] bytes = out.copyBytes();

        List<byte[]> probes = new ArrayList<>(Arrays.asList(pool));
        probes.add(new byte[]{0x40});
        probes.add(new byte[]{(byte) 0x80, 0x00});
        probes.add(new byte[]{(byte) 0xC0});
        probes.add(new byte[]{(byte) 0xFF, (byte) 0xFF, 0x00}); // above all
        for (byte[] probe : probes) {
            int obj = fmt.search(g, probe);
            int expected = -(pool.length + 1);
            for (int i = 0; i < pool.length; i++) {
                int c = fmt.element().compare(pool[i], probe);
                if (c == 0) { expected = i; break; }
                if (c > 0) { expected = -(i + 1); break; }
            }
            assertEquals("search vs element().compare for " + Arrays.toString(probe), expected, obj);
            DataInput2 in = new DataInput2.ByteArray(bytes, 0);
            assertEquals("binarySearch vs search for " + Arrays.toString(probe),
                    obj, fmt.binarySearch(probe, in, pool.length));
            assertEquals("input at group end", 0x600DF00DL, in.readLong());
        }
    }

    // ---- byte-side == object-side agreement harness ------------------------------

    private void assertBinaryMatchesObject(byte[][] group, List<byte[]> extraProbes) {
        Object g = fmt.fromArray(group);
        DataOutput2 out = new DataOutput2();
        fmt.serialize(out, g);
        out.writeLong(0x600DF00DL); // sentinel: binary ops must stop exactly at group end
        byte[] bytes = out.copyBytes();
        List<byte[]> probes = new ArrayList<>(Arrays.asList(group));
        probes.addAll(extraProbes);
        for (byte[] probe : probes) {
            DataInput2 in = new DataInput2.ByteArray(bytes, 0);
            assertEquals("n=" + group.length + " probe " + Arrays.toString(probe),
                    fmt.search(g, probe), fmt.binarySearch(probe, in, group.length));
            assertEquals(0x600DF00DL, in.readLong());
        }
        for (int pos = 0; pos < group.length; pos++) {
            DataInput2 in = new DataInput2.ByteArray(bytes, 0);
            assertEquals("n=" + group.length + " pos " + pos,
                    Arrays.toString(group[pos]), Arrays.toString(fmt.binaryGet(in, group.length, pos)));
            assertEquals(0x600DF00DL, in.readLong());
        }
    }

    private static byte[] ascii(String s) { return s.getBytes(StandardCharsets.US_ASCII); }

    /** Shared-prefix-heavy corpus (path-like keys). */
    private static byte[][] prefixHeavyCorpus(int n, long seed) {
        TreeSet<byte[]> s = new TreeSet<>(Arrays::compareUnsigned);
        Random rnd = new Random(seed);
        String[] roots = {"/home/user/project/src/main/java/org/example/module",
                "/home/user/project/src/test/java/org/example/module",
                "/var/lib/app/data/segment/", "com.example.service.handler."};
        while (s.size() < n) {
            s.add(ascii(roots[rnd.nextInt(roots.length)] + rnd.nextInt(1000) + "/leaf" + rnd.nextInt(10000)));
        }
        return s.toArray(new byte[0][]);
    }

    /** K-boundary sizes with a shared-prefix-heavy corpus. */
    @Test
    public void binaryEquivalence_atRestartBoundaries() {
        for (int n : new int[]{1, 2, K - 1, K, K + 1, 2 * K - 1, 2 * K, 2 * K + 1, 3 * K, 5 * K}) {
            byte[][] group = prefixHeavyCorpus(n, 99);
            List<byte[]> probes = new ArrayList<>();
            probes.add(new byte[0]);                     // below all (empty sorts first)
            probes.add(ascii("~~~~"));                   // above all
            for (byte[] key : group) {
                byte[] above = Arrays.copyOf(key, key.length + 1); // shares full prefix, sorts just above
                above[key.length] = 0x01;
                probes.add(above);
                if (key.length > 0) probes.add(Arrays.copyOf(key, key.length - 1)); // strict prefix, just below
            }
            assertBinaryMatchesObject(group, probes);
        }
    }

    /** Empty, single, and at-restart-boundary special groups. */
    @Test
    public void binaryEquivalence_edgeGroups() {
        assertBinaryMatchesObject(new byte[0][], List.of(new byte[0], ascii("x")));
        assertBinaryMatchesObject(new byte[][]{ascii("only")},
                List.of(new byte[0], ascii("only"), ascii("onlz"), ascii("onl"), ascii("zzz")));
        // group containing the empty key (front-coding shared==0 edge)
        assertBinaryMatchesObject(new byte[][]{{}, {0x00}, {0x00, 0x00}, {0x01}},
                List.of(new byte[0], new byte[]{0x00}, new byte[]{0x02}));
    }

    /** Randomized equivalence with HIGH bytes (unsigned order), fuzzed vs the object side. */
    @Test
    public void binaryEquivalence_randomHighBytes() {
        Random rnd = new Random(0xBA9E5);
        for (int round = 0; round < 60; round++) {
            int n = rnd.nextInt(3 * K + 5);
            TreeSet<byte[]> set = new TreeSet<>(Arrays::compareUnsigned);
            while (set.size() < n) {
                byte[] b = new byte[rnd.nextInt(14)]; // includes empty
                rnd.nextBytes(b);
                set.add(b);
            }
            byte[][] pool = set.toArray(new byte[0][]);
            List<byte[]> probes = new ArrayList<>();
            for (int p = 0; p < 25; p++) {
                byte[] b = new byte[rnd.nextInt(14)];
                rnd.nextBytes(b);
                probes.add(b);
            }
            assertBinaryMatchesObject(pool, probes);
        }
    }

    /**
     * Fuzz the whole search contract against an independent control: a TreeMap with an
     * unsigned comparator. Verifies present/absent/-(insertionPoint+1) all coherent
     * across object side, byte side, and the control.
     */
    @Test
    public void fuzz_vsTreeMapControl() {
        Random rnd = new Random(0x5EED);
        for (int round = 0; round < 100; round++) {
            int n = rnd.nextInt(2 * K + 3);
            TreeMap<byte[], Integer> ctrl = new TreeMap<>(Arrays::compareUnsigned);
            // shared-prefix-heavy keys to stress front-coding
            byte[] prefix = new byte[rnd.nextInt(6)];
            rnd.nextBytes(prefix);
            while (ctrl.size() < n) {
                byte[] suffix = new byte[rnd.nextInt(8)];
                rnd.nextBytes(suffix);
                byte[] key = new byte[prefix.length + suffix.length];
                System.arraycopy(prefix, 0, key, 0, prefix.length);
                System.arraycopy(suffix, 0, key, prefix.length, suffix.length);
                ctrl.putIfAbsent(key, ctrl.size());
            }
            byte[][] pool = ctrl.keySet().toArray(new byte[0][]);
            Object g = fmt.fromArray(pool);
            DataOutput2 out = new DataOutput2();
            fmt.serialize(out, g);
            byte[] bytes = out.copyBytes();

            List<byte[]> probes = new ArrayList<>(Arrays.asList(pool));
            for (int p = 0; p < 20; p++) {
                byte[] b = new byte[rnd.nextInt(10)];
                rnd.nextBytes(b);
                probes.add(b);
            }
            for (byte[] probe : probes) {
                // control's expected binary-search result index
                int idx = indexInSorted(pool, probe);
                int obj = fmt.search(g, probe);
                assertEquals("object vs control", idx, obj);
                DataInput2 in = new DataInput2.ByteArray(bytes, 0);
                assertEquals("byte vs control", idx, fmt.binarySearch(probe, in, pool.length));
                assertEquals(bytes.length, in.pos());
            }
            for (int pos = 0; pos < pool.length; pos++) {
                DataInput2 in = new DataInput2.ByteArray(bytes, 0);
                assertEquals(Arrays.toString(pool[pos]), Arrays.toString(fmt.binaryGet(in, pool.length, pos)));
                assertEquals(bytes.length, in.pos());
            }
        }
    }

    /** Reference binary-search encoding over an unsigned-sorted array. */
    private static int indexInSorted(byte[][] sorted, byte[] key) {
        int lo = 0, hi = sorted.length - 1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int c = Arrays.compareUnsigned(sorted[mid], key);
            if (c == 0) return mid;
            else if (c < 0) lo = mid + 1;
            else hi = mid - 1;
        }
        return -(lo + 1);
    }

    /** The point of R7: front-coded wire must be genuinely smaller on prefix-heavy data. */
    @Test
    public void wireSmallerThanByteArrayFormat_onPrefixHeavyCorpus() {
        byte[][] group = prefixHeavyCorpus(32, 5); // one node's worth
        DataOutput2 pref = new DataOutput2(), whole = new DataOutput2();
        fmt.serialize(pref, fmt.fromArray(group));
        ByteArrayFormat.INSTANCE.serialize(whole, ByteArrayFormat.INSTANCE.fromArray(group));
        System.out.println("[ByteArrayPrefixFormat] path corpus n=32: front-coded " + pref.pos
                + " B vs whole-byte[] " + whole.pos + " B ("
                + Math.round(100.0 * pref.pos / whole.pos) + "%)");
        assertTrue("front-coded (" + pref.pos + ") must beat whole-byte[] (" + whole.pos + ")",
                pref.pos < whole.pos * 3 / 4);
    }

    /**
     * Byte-side search must NOT materialize the whole group. We verify indirectly by
     * proving the roll-forward is bounded: an instrumented DataInput2 that counts total
     * bytes read during a single binarySearch reads far fewer than the whole blob for a
     * large group (it touches only the restart table + one interval).
     */
    @Test
    public void binarySearch_doesNotMaterializeWholeGroup() {
        byte[][] group = prefixHeavyCorpus(20 * K, 3); // 320 keys, 20 restarts
        DataOutput2 out = new DataOutput2();
        fmt.serialize(out, fmt.fromArray(group));
        byte[] bytes = out.copyBytes();
        int blobLen = new DataInput2.ByteArray(bytes, 0).readInt();

        CountingInput in = new CountingInput(bytes, 0);
        fmt.binarySearch(group[group.length / 2], in, group.length); // a present key mid-group
        // one binary search touches log2(20) restarts + one <=K interval, never the whole blob
        assertTrue("byte-side search read " + in.bytesRead + " of blob " + blobLen
                        + " -- must not materialize the whole group",
                in.bytesRead < blobLen / 2);
    }

    /** DataInput2 that tallies readByte/readFully volume. */
    private static final class CountingInput extends DataInput2 {
        final byte[] buf;
        int pos;
        long bytesRead;
        CountingInput(byte[] buf, int pos) { this.buf = buf; this.pos = pos; }
        @Override public int pos() { return pos; }
        @Override public void pos(int pos) { this.pos = pos; }
        @Override public byte readByte() { bytesRead++; return buf[pos++]; }
        @Override public void readFully(byte[] b, int off, int len) {
            System.arraycopy(buf, pos, b, off, len); pos += len; bytesRead += len;
        }
        @Override public void skipBytes(int n) { pos += n; }
    }

    // ---- torn-byte clamps --------------------------------------------------------

    private byte[] serialized(byte[][] group) {
        DataOutput2 out = new DataOutput2();
        fmt.serialize(out, fmt.fromArray(group));
        return out.copyBytes();
    }

    @Test
    public void tornBytes_failFast() {
        byte[][] group = prefixHeavyCorpus(2 * K, 4);
        byte[] good = serialized(group);

        // negative blobLen (top bit of the first int) -- fails before touching any entry
        byte[] b = good.clone();
        b[0] = (byte) 0xFF;
        assertCorrupt(b, group.length, ascii("/home"));

        // restart offset beyond blobLen (second restart entry in the table, bytes 8..11).
        // Probe the MAX key so the restart binary search seeks restart[1] and hits the offset.
        byte[] maxKey = group[group.length - 1];
        b = good.clone();
        b[8] = 0x7F; b[9] = (byte) 0xFF; b[10] = (byte) 0xFF; b[11] = (byte) 0xFF;
        assertCorrupt(b, group.length, maxKey);

        // truncated tail: a probe above all keys walks the last interval into the
        // missing bytes -- must surface as a runtime exception, never a silent overrun
        b = Arrays.copyOf(good, good.length - 5);
        assertCorruptOrOutOfBounds(b, group.length, ascii("~~~~"));
    }

    private void assertCorrupt(byte[] bytes, int n, byte[] probe) {
        try {
            fmt.binarySearch(probe, new DataInput2.ByteArray(bytes, 0), n);
            fail("expected IllegalStateException");
        } catch (IllegalStateException expected) { /* ok */ }
    }

    private void assertCorruptOrOutOfBounds(byte[] bytes, int n, byte[] probe) {
        try {
            fmt.binarySearch(probe, new DataInput2.ByteArray(bytes, 0), n);
            fail("expected a runtime exception");
        } catch (IllegalStateException | IndexOutOfBoundsException expected) { /* ok */ }
    }
}
