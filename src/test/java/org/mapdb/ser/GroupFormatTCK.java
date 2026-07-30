package org.mapdb.ser;

import org.junit.Test;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Contract test kit (TCK) for {@link GroupFormat}. Concrete subclasses bind a
 * format instance and an order-preserving element generator; every test here
 * exercises the interface contract without ever casting the opaque group object.
 */
public abstract class GroupFormatTCK<A> {

    /** Format under test. */
    protected abstract GroupFormat<A> format();

    /**
     * Order-preserving element generator: for non-negative {@code v1 < v2},
     * {@code element().compare(gen(v1), gen(v2)) < 0}. Distinct inputs map to
     * distinct elements.
     */
    protected abstract A gen(long v);

    /**
     * Assertion view of an element: must have value-based equals/toString.
     * Identity for most element types; override for types whose Object.equals is
     * identity (byte[] → {@code Arrays.toString}).
     */
    protected Object view(A v) { return v; }

    /**
     * A large, in-domain value used by {@link #set_copyOnWrite} as the replacement key.
     * Default fits Long/Int; narrow-domain formats (short/char) override it to a value
     * that still fits their type. Need not be sorted relative to the group (set doesn't
     * require ordering), only distinct and representable.
     */
    protected long setProbeValue() { return 999999L; }

    /**
     * Exclusive upper bound for the randomized {@link #binaryEquivalence_randomGroupsAndProbes}
     * fuzz: distinct present keys are drawn from {@code [1, bound)} and the "above all" probe
     * is {@code gen(bound + 100)}, so {@code gen(bound + 100)} must be representable in the
     * element type. Default fits Long/Int; narrow-domain formats (short/char) override it.
     */
    protected long fuzzBound() { return 1_000_000_000L; }

    // ---- helpers -------------------------------------------------------------

    private Comparator<A> cmp() {
        GroupFormat<A> f = format();
        return (x, y) -> f.element().compare(x, y);
    }

    private List<Object> views(List<A> l) {
        List<Object> r = new ArrayList<>(l.size());
        for (A v : l) r.add(view(v));
        return r;
    }

    /** Present, sorted, distinct keys spaced by 10 starting at 10. */
    private Object[] presentValues(int n) {
        Object[] a = new Object[n];
        for (int i = 0; i < n; i++) a[i] = gen((i + 1) * 10L);
        return a;
    }

    private Object group(int n) {
        return format().fromArray(presentValues(n));
    }

    private List<A> contents(Object group) {
        GroupFormat<A> f = format();
        int s = f.size(group);
        List<A> l = new ArrayList<>(s);
        for (int i = 0; i < s; i++) l.add(f.get(group, i));
        return l;
    }

    /** Keys that are absent from a group of size n: below all, in every gap, above all. */
    private List<A> absentKeys(int n) {
        List<A> l = new ArrayList<>();
        l.add(gen(5));                        // below the first present key (10)
        for (int i = 0; i < n; i++) l.add(gen((i + 1) * 10L + 5)); // in the gap after key i
        return l;
    }

    // ==== object side =========================================================

    @Test
    public void empty_hasSizeZeroAndRoundTrips() {
        GroupFormat<A> f = format();
        Object e = f.empty();
        assertEquals(0, f.size(e));

        DataOutput2 out = new DataOutput2();
        f.serialize(out, e);
        DataInput2 in = new DataInput2.ByteArray(out.copyBytes(), 0);
        Object back = f.deserialize(in, 0);
        assertEquals(0, f.size(back));
    }

    @Test
    public void fromArray_sizeAndGetRoundTrip() {
        GroupFormat<A> f = format();
        for (int n : new int[]{0, 1, 2, 3, 64, 200}) {
            Object[] vals = presentValues(n);
            Object g = f.fromArray(vals);
            assertEquals(n, f.size(g));
            for (int i = 0; i < n; i++) {
                @SuppressWarnings("unchecked")
                A expected = (A) vals[i];
                assertEquals(view(expected), view(f.get(g, i)));
            }
        }
    }

    @Test
    public void insert_headMiddleTail_copyOnWrite() {
        GroupFormat<A> f = format();
        for (int n : new int[]{0, 1, 2, 3, 10, 64}) {
            for (int pos : new int[]{0, n / 2, n}) {
                Object g = group(n);
                List<A> before = contents(g);
                A nv = gen(7);
                Object g2 = f.insert(g, pos, nv);
                // original untouched
                assertEquals("insert must not mutate the original group", views(before), views(contents(g)));
                assertNotSame(g, g2);
                // new group has the value inserted at pos
                assertEquals(n + 1, f.size(g2));
                List<A> expected = new ArrayList<>(before);
                expected.add(pos, nv);
                assertEquals(views(expected), views(contents(g2)));
            }
        }
    }

    @Test
    public void set_copyOnWrite() {
        GroupFormat<A> f = format();
        for (int n : new int[]{1, 2, 3, 10, 64}) {
            for (int pos : new int[]{0, n / 2, n - 1}) {
                Object g = group(n);
                List<A> before = contents(g);
                A nv = gen(setProbeValue());
                Object g2 = f.set(g, pos, nv);
                assertEquals("set must not mutate the original group", views(before), views(contents(g)));
                assertNotSame(g, g2);
                List<A> expected = new ArrayList<>(before);
                expected.set(pos, nv);
                assertEquals(views(expected), views(contents(g2)));
            }
        }
    }

    @Test
    public void delete_headMiddleTail_copyOnWrite() {
        GroupFormat<A> f = format();
        for (int n : new int[]{1, 2, 3, 10, 64}) {
            for (int pos : new int[]{0, n / 2, n - 1}) {
                Object g = group(n);
                List<A> before = contents(g);
                Object g2 = f.delete(g, pos);
                assertEquals("delete must not mutate the original group", views(before), views(contents(g)));
                assertNotSame(g, g2);
                assertEquals(n - 1, f.size(g2));
                List<A> expected = new ArrayList<>(before);
                expected.remove(pos);
                assertEquals(views(expected), views(contents(g2)));
            }
        }
    }

    @Test
    public void copyRange_variants_copyOnWrite() {
        GroupFormat<A> f = format();
        for (int n : new int[]{0, 1, 2, 3, 10, 64}) {
            Object g = group(n);
            List<A> before = contents(g);
            int[][] ranges = {
                    {0, n},              // full
                    {0, n / 2},          // prefix
                    {n / 2, n},          // suffix
                    {n > 0 ? n / 2 : 0, n > 0 ? n / 2 + 1 : 0}, // single (or empty when n==0)
                    {n / 2, n / 2},      // empty range
            };
            for (int[] r : ranges) {
                Object g2 = f.copyRange(g, r[0], r[1]);
                assertEquals("copyRange must not mutate the original group", views(before), views(contents(g)));
                assertNotSame(g, g2);
                assertEquals(views(before.subList(r[0], r[1])), views(contents(g2)));
            }
        }
    }

    // ==== search ==============================================================

    @Test
    public void search_presentAndAbsent_matchesReferenceBinarySearch() {
        GroupFormat<A> f = format();
        for (int n : new int[]{0, 1, 2, 3, 64, 1000}) {
            Object g = group(n);
            List<A> list = contents(g);
            // present keys: found at their index
            for (int i = 0; i < n; i++) {
                A key = list.get(i);
                int expected = Collections.binarySearch(list, key, cmp());
                assertEquals("present key at " + i, expected, f.search(g, key));
                assertEquals("present key must be found at its index", i, f.search(g, key));
            }
            // absent keys: -(insertionPoint+1), agreeing with reference
            for (A key : absentKeys(n)) {
                int expected = Collections.binarySearch(list, key, cmp());
                assertTrue("absent key must yield negative", expected < 0);
                assertEquals("absent key search", expected, f.search(g, key));
            }
        }
    }

    // ==== serialize / deserialize =============================================

    @Test
    public void serializeDeserialize_preservesOrderAndContent() {
        GroupFormat<A> f = format();
        for (int n = 0; n <= 200; n++) {
            Object g = group(n);
            List<A> before = contents(g);
            DataOutput2 out = new DataOutput2();
            f.serialize(out, g);
            byte[] bytes = out.copyBytes();
            for (DataInput2 in : inputs(bytes, 0)) {
                Object back = f.deserialize(in, n);
                assertEquals("size " + n, views(before), views(contents(back)));
            }
        }
    }

    // ==== byte side ===========================================================

    @Test
    public void binaryUnsupported_throwsWhenNotSupported() {
        GroupFormat<A> f = format();
        if (f.supportsBinary()) return; // covered by the binary contract tests below
        Object g = group(4);
        DataOutput2 out = new DataOutput2();
        f.serialize(out, g);
        DataInput2 in = new DataInput2.ByteArray(out.copyBytes(), 0);
        try {
            f.binarySearch(gen(10), in, 4);
            fail("binarySearch must throw when supportsBinary()==false");
        } catch (UnsupportedOperationException expected) { /* ok */ }
        try {
            f.binaryGet(in, 4, 0);
            fail("binaryGet must throw when supportsBinary()==false");
        } catch (UnsupportedOperationException expected) { /* ok */ }
    }

    @Test
    public void binarySearch_matchesObjectSide_andLeavesInputAtGroupEnd() {
        GroupFormat<A> f = format();
        if (!f.supportsBinary()) return;
        long sentinel = 0x0102030405060708L;
        for (int n : new int[]{0, 1, 2, 7, 64, 1000}) {
            Object g = group(n);
            List<A> list = contents(g);
            for (int offset : new int[]{0, 1000}) {
                Framed fr = frame(f, g, offset, sentinel);
                for (InFactory mk : IN_FACTORIES) {
                    // present keys
                    for (int i = 0; i < n; i++) {
                        A key = list.get(i);
                        DataInput2 in = mk.make(fr.bytes, fr.groupStart);
                        int r = f.binarySearch(key, in, n);
                        assertEquals("present binarySearch idx", f.search(g, key), r);
                        assertEquals("binarySearch must leave input at group end",
                                sentinel, in.readLong());
                    }
                    // absent keys
                    for (A key : absentKeys(n)) {
                        DataInput2 in = mk.make(fr.bytes, fr.groupStart);
                        int r = f.binarySearch(key, in, n);
                        assertEquals("absent binarySearch", f.search(g, key), r);
                        assertEquals(sentinel, in.readLong());
                    }
                }
            }
        }
    }

    @Test
    public void binaryGet_matchesObjectSide_andLeavesInputAtGroupEnd() {
        GroupFormat<A> f = format();
        if (!f.supportsBinary()) return;
        long sentinel = 0x1122334455667788L;
        for (int n : new int[]{1, 2, 7, 64, 1000}) {
            Object g = group(n);
            for (int offset : new int[]{0, 1000}) {
                Framed fr = frame(f, g, offset, sentinel);
                for (InFactory mk : IN_FACTORIES) {
                    for (int pos = 0; pos < n; pos++) {
                        DataInput2 in = mk.make(fr.bytes, fr.groupStart);
                        A got = f.binaryGet(in, n, pos);
                        assertEquals("binaryGet pos " + pos, view(f.get(g, pos)), view(got));
                        assertEquals("binaryGet must leave input at group end",
                                sentinel, in.readLong());
                    }
                }
            }
        }
    }

    /**
     * Randomized binary==object equivalence property: for random sorted groups and
     * random probes (hits, misses, below-all/above-all boundaries, empty group),
     * {@code binarySearch(bytes) == search(group)} and {@code binaryGet == get},
     * with the input left exactly at group end every time.
     */
    @Test
    public void binaryEquivalence_randomGroupsAndProbes() {
        GroupFormat<A> f = format();
        if (!f.supportsBinary()) return;
        Random rnd = new Random(0xF0F0);
        long sentinel = 0x5A5A5A5A5A5A5A5AL;
        final long bound = fuzzBound(); // fits every element domain incl. int; narrow types override
        for (int round = 0; round < 60; round++) {
            int n = round == 0 ? 0 : rnd.nextInt(65); // round 0: empty group
            java.util.TreeSet<Long> vs = new java.util.TreeSet<>();
            while (vs.size() < n) vs.add((long) rnd.nextInt((int) bound) + 1);
            Object[] arr = new Object[n];
            int i = 0;
            for (long v : vs) arr[i++] = gen(v);
            Object g = f.fromArray(arr);
            Framed fr = frame(f, g, rnd.nextInt(3) * 7, sentinel);

            List<A> probes = new ArrayList<>(contents(g));       // every hit
            for (int p = 0; p < 20; p++) probes.add(gen(rnd.nextInt((int) bound) + 1)); // random (mostly misses)
            probes.add(gen(0));                                   // below all
            probes.add(gen(bound + 100));                         // above all

            for (A probe : probes) {
                int expected = f.search(g, probe);
                for (InFactory mk : IN_FACTORIES) {
                    DataInput2 in = mk.make(fr.bytes, fr.groupStart);
                    assertEquals("binarySearch equivalence n=" + n + " probe=" + probe,
                            expected, f.binarySearch(probe, in, n));
                    assertEquals("input must be at group end", sentinel, in.readLong());
                }
            }
            for (int pos = 0; pos < n; pos++) {
                for (InFactory mk : IN_FACTORIES) {
                    DataInput2 in = mk.make(fr.bytes, fr.groupStart);
                    assertEquals("binaryGet equivalence pos=" + pos,
                            view(f.get(g, pos)), view(f.binaryGet(in, n, pos)));
                    assertEquals("input must be at group end", sentinel, in.readLong());
                }
            }
        }
    }

    // ==== byte side: range cursor =============================================

    /** Ranges to exercise for a group of size n: full, prefix, suffix, single, empty, tail-empty. */
    private int[][] cursorRanges(int n) {
        if (n == 0) return new int[][]{{0, 0}};
        return new int[][]{
                {0, n},                  // full
                {0, n / 2},              // prefix
                {n / 2, n},              // suffix
                {n / 2, n / 2 + 1},      // single
                {n / 2, n / 2},          // empty in the middle
                {n, n},                  // empty at the tail
        };
    }

    @Test
    public void rangeCursor_unsupported_throwsWhenNotSupported() {
        GroupFormat<A> f = format();
        if (f.supportsRangeCursor()) return;
        Object g = group(4);
        DataOutput2 out = new DataOutput2();
        f.serialize(out, g);
        DataInput2 in = new DataInput2.ByteArray(out.copyBytes(), 0);
        try {
            f.rangeCursor(in, 4, 0, 4);
            fail("rangeCursor must throw when supportsRangeCursor()==false");
        } catch (UnsupportedOperationException expected) { /* ok */ }
    }

    @Test
    public void rangeCursor_invalidRange_throws() {
        GroupFormat<A> f = format();
        if (!f.supportsRangeCursor()) return;
        Object g = group(4);
        DataOutput2 out = new DataOutput2();
        f.serialize(out, g);
        byte[] bytes = out.copyBytes();
        for (int[] bad : new int[][]{{-1, 2}, {2, 1}, {0, 5}, {3, 5}}) {
            try {
                f.rangeCursor(new DataInput2.ByteArray(bytes, 0), 4, bad[0], bad[1]);
                fail("rangeCursor must reject range " + bad[0] + ".." + bad[1]);
            } catch (IndexOutOfBoundsException expected) { /* ok */ }
        }
    }

    /**
     * The range cursor yields exactly {@code get(pos)} for {@code pos} in {@code [from,to)}, in
     * order, with the right absolute {@code index()}; and after exhaustion the input is left at
     * group END for EVERY size (including the empty group) so following fields still parse.
     */
    @Test
    public void rangeCursor_matchesObjectSide_andLeavesInputAtGroupEnd() {
        GroupFormat<A> f = format();
        if (!f.supportsRangeCursor()) return;
        long sentinel = 0x0BADC0DE0BADC0DEL;
        for (int n : new int[]{0, 1, 2, 7, 64, 300}) {
            Object g = group(n);
            List<A> list = contents(g);
            for (int[] r : cursorRanges(n)) {
              for (int offset : new int[]{0, 1000}) {           // guard the captured start = in.pos()
                for (InFactory mk : IN_FACTORIES) {
                    Framed fr = frame(f, g, offset, sentinel);
                    DataInput2 in = mk.make(fr.bytes, fr.groupStart);
                    GroupCursor<A> c = f.rangeCursor(in, n, r[0], r[1]);
                    int exp = r[0];
                    while (c.next()) {
                        assertEquals("cursor index", exp, c.index());
                        assertEquals("cursor value at " + exp, view(list.get(exp)), view(c.value()));
                        exp++;
                    }
                    assertEquals("cursor must visit the whole range " + r[0] + ".." + r[1], r[1], exp);
                    assertFalse("next() stays false after exhaustion", c.next());
                    assertEquals("input at group end after exhaustion (n=" + n + ")", sentinel, in.readLong());
                }
              }
            }
        }
    }

    // ---- byte-side framing scaffolding --------------------------------------

    private static final class Framed {
        final byte[] bytes;
        final int groupStart;
        Framed(byte[] bytes, int groupStart) { this.bytes = bytes; this.groupStart = groupStart; }
    }

    /** Lay out: [offset garbage bytes][serialized group][sentinel long]. */
    private Framed frame(GroupFormat<A> f, Object group, int offset, long sentinel) {
        DataOutput2 whole = new DataOutput2();
        for (int i = 0; i < offset; i++) whole.writeByte(0xAB);
        int groupStart = whole.pos;
        f.serialize(whole, group);
        whole.writeLong(sentinel);
        return new Framed(whole.copyBytes(), groupStart);
    }

    private interface InFactory { DataInput2 make(byte[] b, int pos); }

    private static final InFactory[] IN_FACTORIES = {
            DataInput2.ByteArray::new,
            (b, pos) -> {
                ByteBuffer bb = ByteBuffer.allocate(b.length);
                bb.put(b); bb.clear();
                return new DataInput2.ByteBuf(bb, pos);
            },
            (b, pos) -> {
                ByteBuffer bb = ByteBuffer.allocateDirect(b.length);
                bb.put(b); bb.clear();
                return new DataInput2.ByteBuf(bb, pos);
            },
    };

    private static DataInput2[] inputs(byte[] b, int pos) {
        DataInput2[] r = new DataInput2[IN_FACTORIES.length];
        for (int i = 0; i < r.length; i++) r[i] = IN_FACTORIES[i].make(b, pos);
        return r;
    }
}
