package org.mapdb.ser;

import org.junit.Test;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Binds the {@link GroupFormat} TCK to the binary {@link StringGroupFormat}. */
public class StringGroupFormatTest extends GroupFormatTCK<String> {

    private final GroupFormat<String> fmt = StringGroupFormat.INSTANCE;

    @Override protected GroupFormat<String> format() { return fmt; }

    /** Zero-padded 20 digits so lexicographic order == numeric order for v >= 0. */
    @Override protected String gen(long v) { return String.format("%020d", v); }

    @Test
    public void supportsBinaryIsTrue() {
        assertTrue(fmt.supportsBinary());
    }

    /**
     * binary==object equivalence over a UNICODE corpus whose order distinguishes
     * UTF-8 (code-point) from UTF-16 (compareTo) order — supplementary characters
     * vs high-BMP forms like U+FF61 — plus CJK, Latin-1, shared prefixes, empty.
     * The TCK's gen is ASCII-only, so this coverage lives here.
     */
    @Test
    public void binaryEquivalence_unicodeCorpus() {
        String u10000 = new String(Character.toChars(0x10000));
        String emoji = new String(Character.toChars(0x1F600));
        TreeSet<String> sorted = new TreeSet<>(); // natural order == compareTo
        sorted.addAll(List.of(
                "", "a", "ab", "b", "é", "ÿ", "你", "你好", "好",
                "｡", u10000, emoji, emoji + "a",
                "prefix", "prefixa", "prefix｡", "prefix" + emoji, "prefix" + u10000));
        String[] group = sorted.toArray(new String[0]);
        Object g = fmt.fromArray(group);
        DataOutput2 out = new DataOutput2();
        fmt.serialize(out, g);
        byte[] bytes = out.copyBytes();

        List<String> probes = new ArrayList<>(sorted);
        probes.add("c");
        probes.add("prefix!");
        probes.add("｡｡");
        probes.add(u10000 + u10000);
        probes.add(emoji + emoji);
        for (String probe : probes) {
            DataInput2 in = new DataInput2.ByteArray(bytes, 0);
            assertEquals("probe \"" + probe + "\"",
                    fmt.search(g, probe), fmt.binarySearch(probe, in, group.length));
            assertEquals("input at group end", bytes.length, in.pos());
        }
        for (int pos = 0; pos < group.length; pos++) {
            DataInput2 in = new DataInput2.ByteArray(bytes, 0);
            assertEquals(group[pos], fmt.binaryGet(in, group.length, pos));
            assertEquals(bytes.length, in.pos());
        }
    }
}
