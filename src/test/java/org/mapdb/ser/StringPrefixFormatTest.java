package org.mapdb.ser;

import org.junit.Test;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Binds the {@link GroupFormat} TCK to {@link StringPrefixFormat} (front-coding, R7). */
public class StringPrefixFormatTest extends GroupFormatTCK<String> {

    private static final int K = StringPrefixFormat.RESTART_INTERVAL;

    private final StringPrefixFormat fmt = StringPrefixFormat.INSTANCE;

    @Override protected GroupFormat<String> format() { return fmt; }

    /** Zero-padded 20 digits: numeric order == compareTo order AND heavy shared prefixes. */
    @Override protected String gen(long v) { return String.format("%020d", v); }

    @Test
    public void supportsBinaryIsTrue() {
        assertTrue(fmt.supportsBinary());
    }

    // ---- corpora ---------------------------------------------------------------

    private static String[] urlCorpus(int n) {
        TreeSet<String> s = new TreeSet<>();
        Random rnd = new Random(7);
        String[] hosts = {"https://api.example.com/v2/users/", "https://api.example.com/v2/orders/",
                "https://cdn.example.com/assets/img/", "/usr/local/lib/app/modules/"};
        while (s.size() < n) {
            s.add(hosts[rnd.nextInt(hosts.length)] + Integer.toHexString(rnd.nextInt(1 << 20)) + "/detail");
        }
        return s.toArray(new String[0]);
    }

    private static String[] unicodeCorpus() {
        String u10000 = new String(Character.toChars(0x10000));
        String emoji = new String(Character.toChars(0x1F600));
        TreeSet<String> s = new TreeSet<>();
        s.addAll(List.of("", "a", "ab", "b", "é", "你", "你好", "｡",
                u10000, emoji, emoji + "a",
                "prefix", "prefixa", "prefix｡", "prefix" + emoji, "prefix" + u10000,
                "path/to/file", "path/to/file2", "path/to/other"));
        return s.toArray(new String[0]);
    }

    private void assertBinaryMatchesObject(String[] group, List<String> extraProbes) {
        Object g = fmt.fromArray(group);
        DataOutput2 out = new DataOutput2();
        fmt.serialize(out, g);
        out.writeLong(0x600DF00DL); // sentinel: binary ops must stop exactly at group end
        byte[] bytes = out.copyBytes();
        List<String> probes = new ArrayList<>(List.of(group));
        probes.addAll(extraProbes);
        for (String probe : probes) {
            DataInput2 in = new DataInput2.ByteArray(bytes, 0);
            assertEquals("n=" + group.length + " probe \"" + probe + "\"",
                    fmt.search(g, probe), fmt.binarySearch(probe, in, group.length));
            assertEquals(0x600DF00DL, in.readLong());
        }
        for (int pos = 0; pos < group.length; pos++) {
            DataInput2 in = new DataInput2.ByteArray(bytes, 0);
            assertEquals("n=" + group.length + " pos " + pos, group[pos], fmt.binaryGet(in, group.length, pos));
            assertEquals(0x600DF00DL, in.readLong());
        }
    }

    /** K-boundary sizes: n = 1, K-1, K, K+1, 2K-1, 2K, 2K+1 (single/partial/full intervals). */
    @Test
    public void binaryEquivalence_atRestartBoundaries() {
        for (int n : new int[]{1, 2, K - 1, K, K + 1, 2 * K - 1, 2 * K, 2 * K + 1, 3 * K}) {
            String[] group = urlCorpus(n);
            List<String> probes = new ArrayList<>();
            probes.add("");                                  // below all
            probes.add("zzzz");                              // above all
            for (String s : group) {
                probes.add(s + "!");                         // just above each (shares full prefix)
                probes.add(s.substring(0, s.length() - 1));  // just below (strict prefix)
            }
            assertBinaryMatchesObject(group, probes);
        }
    }

    @Test
    public void binaryEquivalence_unicodeCorpus() {
        String[] group = unicodeCorpus();
        String emoji = new String(Character.toChars(0x1F600));
        assertBinaryMatchesObject(group,
                List.of("c", "prefix!", "｡｡", emoji + emoji, "path/to", "path/to/file3"));
    }

    @Test
    public void binaryEquivalence_filePathCorpus() {
        TreeSet<String> s = new TreeSet<>();
        Random rnd = new Random(21);
        while (s.size() < 100) {
            s.add("/home/user/project/src/main/java/org/example/module"
                    + rnd.nextInt(10) + "/Class" + rnd.nextInt(1000) + ".java");
        }
        String[] group = s.toArray(new String[0]);
        assertBinaryMatchesObject(group, List.of("/home/user/project", "/z", "/a"));
    }

    /** The point of R7: front-coded wire must be genuinely smaller on prefix-heavy data. */
    @Test
    public void wireSmallerThanStringGroupFormat_onPrefixHeavyCorpus() {
        String[] group = urlCorpus(32); // one node's worth
        DataOutput2 pref = new DataOutput2(), whole = new DataOutput2();
        fmt.serialize(pref, fmt.fromArray(group));
        StringGroupFormat.INSTANCE.serialize(whole, StringGroupFormat.INSTANCE.fromArray(group));
        System.out.println("[StringPrefixFormat] URL corpus n=32: front-coded " + pref.pos
                + " B vs whole-string " + whole.pos + " B ("
                + Math.round(100.0 * pref.pos / whole.pos) + "%)");
        assertTrue("front-coded (" + pref.pos + ") must beat whole-string (" + whole.pos + ")",
                pref.pos < whole.pos * 2 / 3);
    }

    // ---- torn-byte clamps -------------------------------------------------------

    private byte[] serialized(String[] group) {
        DataOutput2 out = new DataOutput2();
        fmt.serialize(out, fmt.fromArray(group));
        return out.copyBytes();
    }

    @Test
    public void tornBytes_failFast() {
        String[] group = urlCorpus(2 * K);
        byte[] good = serialized(group);

        // negative blobLen
        byte[] b = good.clone();
        b[0] = (byte) 0xFF;
        assertCorrupt(b, group.length);

        // restart offset beyond blobLen (second restart entry in the table)
        b = good.clone();
        b[8] = 0x7F; b[9] = (byte) 0xFF; b[10] = (byte) 0xFF; b[11] = (byte) 0xFF;
        assertCorrupt(b, group.length);

        // truncated record: a probe above all keys walks the last interval into the
        // missing tail -- must surface as a runtime exception, never a silent overrun
        b = java.util.Arrays.copyOf(good, good.length - 5);
        assertCorruptOrOutOfBounds(b, group.length, "zzzz");
    }

    private void assertCorrupt(byte[] bytes, int n) {
        try {
            fmt.binarySearch("https://api.example.com/v2/", new DataInput2.ByteArray(bytes, 0), n);
            fail("expected IllegalStateException");
        } catch (IllegalStateException expected) { /* ok */ }
    }

    private void assertCorruptOrOutOfBounds(byte[] bytes, int n, String probe) {
        try {
            fmt.binarySearch(probe, new DataInput2.ByteArray(bytes, 0), n);
            fail("expected a runtime exception");
        } catch (IllegalStateException | IndexOutOfBoundsException expected) { /* ok */ }
    }
}
