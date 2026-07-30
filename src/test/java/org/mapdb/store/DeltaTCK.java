package org.mapdb.store;

import org.junit.After;
import org.junit.Test;
import org.mapdb.DBException;

import java.util.ArrayList;
import java.util.List;
import java.util.PrimitiveIterator;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Delta-capability TCK for {@link StoreDelta} implementers. Exercises the
 * record content model: content == base ++ deltas, capacity refusal, and
 * merged-logical-value CAS. Uses {@link Fixtures#RAW} (size-driven) so appended bytes are
 * observable — length-prefixed serializers self-delimit and hide the delta region.
 */
public abstract class DeltaTCK {

    private final List<StoreDelta> opened = new ArrayList<>();

    protected abstract StoreDelta createStore();

    protected final StoreDelta openStore() {
        StoreDelta s = createStore();
        opened.add(s);
        return s;
    }

    @After public void tearDown() {
        try {
            for (StoreDelta s : opened) {
                try { if (!s.isClosed()) s.close(); } catch (Throwable ignore) {}
            }
            opened.clear();
        } finally {
            cleanup();
        }
    }

    /** Subclass hook for post-close resource cleanup (temp files). See {@link StoreTCK#cleanup()}. */
    protected void cleanup() { }

    // ---------- helpers ----------

    private static void assertGetVoid(Runnable r) {
        try {
            r.run();
            fail("expected DBException.GetVoid");
        } catch (DBException.GetVoid expected) { /* ok */ }
    }

    /** Full record content via read() (merged base ++ deltas), or null for null-content records. */
    private static byte[] content(StoreDelta s, long recid) {
        Fixtures.Capture c = new Fixtures.Capture();
        s.read(recid, c);
        return c.bytes;
    }

    private static byte[] concat(byte[]... parts) {
        int n = 0;
        for (byte[] p : parts) n += p.length;
        byte[] out = new byte[n];
        int off = 0;
        for (byte[] p : parts) { System.arraycopy(p, 0, out, off, p.length); off += p.length; }
        return out;
    }

    private static boolean contains(StoreDelta s, long recid) {
        PrimitiveIterator.OfLong it = s.getAllRecids();
        while (it.hasNext()) if (it.nextLong() == recid) return true;
        return false;
    }

    // ================= cases =================

    @Test public void append_grows_content_byte_exactly() {
        StoreDelta s = openStore();
        byte[] base = Fixtures.payload(1, 1, 4);      // 12 bytes
        long r = s.put(base, Fixtures.RAW);
        s.updateWithHeadroom(r, base, Fixtures.RAW, 64);
        s.verify();

        byte[] d1 = {10, 11, 12};
        byte[] d2 = {20, 21};
        byte[] d3 = {30};
        assertEquals(base.length + 3, s.append(r, d1, 0, d1.length));
        assertEquals(base.length + 5, s.append(r, d2, 0, d2.length));
        assertEquals(base.length + 6, s.append(r, d3, 0, d3.length));
        s.verify();

        assertArrayEquals(concat(base, d1, d2, d3), content(s, r));
        assertArrayEquals(concat(base, d1, d2, d3), s.get(r, Fixtures.RAW));
    }

    @Test public void refused_exactly_at_capacity_boundary() {
        StoreDelta s = openStore();
        byte[] base = Fixtures.payload(2, 1, 0);       // 8 bytes
        long r = s.put(base, Fixtures.RAW);
        s.updateWithHeadroom(r, base, Fixtures.RAW, 40);
        s.commit();                                     // WAL: establish capacity (no-op for others)

        long capRem = s.capacityRemaining(r);
        assertTrue("headroom must be honoured", capRem >= 40);
        byte[] fill = new byte[(int) capRem];
        for (int i = 0; i < fill.length; i++) fill[i] = (byte) (i + 1);
        assertEquals(base.length + capRem, s.append(r, fill, 0, fill.length));
        assertEquals("at boundary", 0, s.capacityRemaining(r));

        byte[] merged = concat(base, fill);
        assertEquals(StoreDelta.REFUSED, s.append(r, new byte[]{99}, 0, 1));
        assertEquals("content unchanged after REFUSED", 0, s.capacityRemaining(r));
        assertArrayEquals("content unchanged after REFUSED", merged, content(s, r));
        s.verify();
    }

    @Test public void append_on_prealloc_establishes_delta_only() {
        StoreDelta s = openStore();
        long r = s.preallocate();
        assertNull("P record reads null", content(s, r));
        assertTrue("P excluded from getAllRecids", !contains(s, r));

        byte[] d = Fixtures.payload(3, 1, 6);          // 14 bytes
        assertEquals(d.length, s.append(r, d, 0, d.length));  // content == deltas only
        s.verify();

        assertArrayEquals(d, content(s, r));
        assertArrayEquals(d, s.get(r, Fixtures.RAW));
        assertTrue("append established a live record", contains(s, r));

        s.commit();                                     // survives commit (WAL); no-op others
        assertArrayEquals(d, s.get(r, Fixtures.RAW));
        assertTrue(contains(s, r));
        s.verify();
    }

    @Test public void update_resets_appended_region() {
        StoreDelta s = openStore();
        byte[] base = Fixtures.payload(4, 1, 0);
        long r = s.put(base, Fixtures.RAW);
        s.updateWithHeadroom(r, base, Fixtures.RAW, 32);
        byte[] extra = {7, 7, 7, 7};
        s.append(r, extra, 0, extra.length);
        assertArrayEquals(concat(base, extra), content(s, r));

        byte[] base2 = Fixtures.payload(4, 2, 3);
        s.update(r, base2, Fixtures.RAW);
        assertArrayEquals("appended region must reset on update", base2, content(s, r));
        assertArrayEquals(base2, s.get(r, Fixtures.RAW));
        s.verify();
    }

    @Test public void updateWithHeadroom_guarantees_appendable() {
        StoreDelta s = openStore();
        int H = 48;
        byte[] base = Fixtures.payload(5, 1, 2);
        long r = s.put(base, Fixtures.RAW);
        s.updateWithHeadroom(r, base, Fixtures.RAW, H);
        assertTrue("capacityRemaining >= headroom", s.capacityRemaining(r) >= H);

        byte[] block = new byte[H];
        for (int i = 0; i < H; i++) block[i] = (byte) i;
        long sz = s.append(r, block, 0, H);
        assertTrue("headroom bytes must be immediately appendable (not REFUSED)", sz != StoreDelta.REFUSED);
        assertEquals(base.length + H, sz);
        assertArrayEquals(concat(base, block), content(s, r));
        s.verify();
    }

    @Test public void delete_after_appends_getvoid_everywhere() {
        StoreDelta s = openStore();
        byte[] base = Fixtures.payload(6, 1, 0);
        long r = s.put(base, Fixtures.RAW);
        s.updateWithHeadroom(r, base, Fixtures.RAW, 32);
        s.append(r, new byte[]{1, 2, 3}, 0, 3);
        s.delete(r, Fixtures.RAW);
        s.verify();

        assertGetVoid(() -> s.get(r, Fixtures.RAW));
        assertGetVoid(() -> s.read(r, new Fixtures.Capture()));
        assertGetVoid(() -> s.append(r, new byte[]{1}, 0, 1));
        assertGetVoid(() -> s.capacityRemaining(r));
        assertGetVoid(() -> s.update(r, base, Fixtures.RAW));
        assertGetVoid(() -> s.updateWithHeadroom(r, base, Fixtures.RAW, 8));
        assertGetVoid(() -> s.compareAndSwap(r, base, base, Fixtures.RAW));
        s.verify();
    }

    @Test public void zero_length_append_is_noop_returning_size() {
        StoreDelta s = openStore();
        byte[] base = Fixtures.payload(7, 1, 5);
        long r = s.put(base, Fixtures.RAW);
        long sz = s.append(r, new byte[0], 0, 0);
        assertEquals("zero-length append returns current size", base.length, sz);
        assertArrayEquals("content unchanged", base, content(s, r));
        // zero-length append using a non-zero offset into a buffer is still a no-op
        assertEquals(base.length, s.append(r, new byte[]{9, 9, 9}, 2, 0));
        assertArrayEquals(base, content(s, r));
        s.verify();
    }

    /**
     * A zero-length append on an ALREADY-COMMITTED record is a no-op returning the current
     * size — the shape-agnostic half of the contract. (Whether a zero-length append
     * establishes a preallocated record, and how it behaves on an oversize/linked record,
     * are implementation-specific and pinned in the StoreDirect/StoreWAL suites.)
     */
    @Test public void zero_length_append_after_commit_is_noop() {
        StoreDelta s = openStore();
        byte[] base = Fixtures.payload(9, 1, 16);
        long r = s.put(base, Fixtures.RAW);
        s.commit();
        assertEquals("zero-length append returns current size", base.length,
                s.append(r, new byte[0], 0, 0));
        s.commit();                                    // must not trip the commit-apply assertion
        assertArrayEquals("content untouched", base, s.get(r, Fixtures.RAW));
        s.verify();
    }

    @Test public void cas_after_appends_compares_merged_value() {
        StoreDelta s = openStore();
        byte[] base = Fixtures.payload(8, 1, 0);
        long r = s.put(base, Fixtures.RAW);
        s.updateWithHeadroom(r, base, Fixtures.RAW, 64);
        byte[] d1 = {40, 41, 42};
        byte[] d2 = {50, 51};
        s.append(r, d1, 0, d1.length);
        s.append(r, d2, 0, d2.length);
        byte[] merged = concat(base, d1, d2);
        assertArrayEquals(merged, content(s, r));

        // CAS against the base-only image must FAIL: the record's logical value is the merge.
        org.junit.Assert.assertFalse("CAS vs pre-append image must fail",
                s.compareAndSwap(r, base, Fixtures.payload(8, 9, 0), Fixtures.RAW));
        // CAS against the merged image must SUCCEED (merged logical value).
        byte[] replacement = Fixtures.payload(8, 2, 4);
        assertTrue("CAS vs merged image must succeed",
                s.compareAndSwap(r, merged, replacement, Fixtures.RAW));
        assertArrayEquals(replacement, content(s, r));
        s.verify();
    }
}
