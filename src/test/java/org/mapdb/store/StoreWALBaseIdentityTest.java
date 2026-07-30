package org.mapdb.store;

import org.junit.After;
import org.junit.Test;
import org.mapdb.DBException;
import org.mapdb.io.DataOutput2;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * WAL v2 base identity: every {@code T_APPEND} carries the LSN of the image it extends, and
 * replay is one ordered pass over the transition table (no per-recid replay
 * floor, no lookahead pass).
 *
 * <p>The hand-built logs below are the point of this file. A delta whose base image is no
 * longer in the log is exactly what segment cleaning will produce — the shape that
 * the deleted floor design got wrong twice — so the decisions it forces are pinned here
 * BEFORE the mechanism that produces it exists, against crafted images rather than against a
 * cleaner that could be wrong in the same direction as the reader.
 */
public class StoreWALBaseIdentityTest {

    private final List<File> files = new ArrayList<>();

    private File newFile(String tag) {
        try {
            File f = Files.createTempFile("mapdb5-wal-base-" + tag, ".wal").toFile();
            f.delete();
            files.add(f);
            return f;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @After public void cleanup() {
        for (File f : files) {
            f.delete();
            org.mapdb.store.WalTestKit.deleteStore(f);
        }
        files.clear();
    }

    // ---------------------------------------------------------------- log building

    private static byte[] recordEntry(long recid, long cap, byte[] content) {
        DataOutput2 o = new DataOutput2(64);
        o.writeByte(2);
        o.packLong(recid);
        o.packLong(cap);
        if (content == null) {
            o.packLong(0);
        } else {
            o.packLong(content.length + 1);
            o.write(content);
        }
        return java.util.Arrays.copyOf(o.buf, o.pos);
    }

    /** {@code T_APPEND := tag | recid | packLong(lsn - baseLsn) | len | bytes}. */
    private static byte[] appendEntry(long recid, long baseDelta, byte[] data) {
        DataOutput2 o = new DataOutput2(64);
        o.writeByte(3);
        o.packLong(recid);
        o.packLong(baseDelta);
        o.packLong(data.length);
        o.write(data);
        return java.util.Arrays.copyOf(o.buf, o.pos);
    }

    private static byte[] preallocEntry(long recid) {
        DataOutput2 o = new DataOutput2(16);
        o.writeByte(1);
        o.packLong(recid);
        return java.util.Arrays.copyOf(o.buf, o.pos);
    }

    private static byte[] deleteEntry(long recid) {
        DataOutput2 o = new DataOutput2(16);
        o.writeByte(4);
        o.packLong(recid);
        return java.util.Arrays.copyOf(o.buf, o.pos);
    }

    /** A section that has not been placed yet: its CRCs depend on where it lands (§6.2). */
    private record Sec(char tag, long lsn, byte[] body) { }

    private static Sec section(char tag, long lsn, byte[]... entries) throws IOException {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        for (byte[] e : entries) body.write(e);
        return new Sec(tag, lsn, body.toByteArray());
    }

    /**
     * Lays the fixture's sections out and, when they start above LSN 1, <b>authorizes that start
     * the way a writer would</b>.
     *
     * <p>Most of these fixtures begin around LSN 10 so that an append can cite a base BELOW the
     * start of the log — the stranded-delta case §4.2 exists for. A lone segment 1 holding LSN 10
     * is not a reachable image: LSNs 1–9 would have had to live in that same segment, and a
     * segment's prefix cannot be removed. What legitimately produces a log starting at LSN 10 is a
     * <em>clean</em>: it retires the segments below and records {@code logStartLsn} in its mark. So
     * the fixture is written as segment 2 (stating LSN 10 in its header) plus segment 3 holding a
     * mark that retires the absent segment 1 and attests the log begins at 10.
     *
     * <p>Offsets are computed here rather than by the caller because both CRCs are domain-separated
     * by {@code segmentHeader || be64(sectionOffset)} — a section image is only valid at the exact
     * place it was built for.
     */
    private void writeLog(File f, Sec... sections) throws IOException {
        long first = sections.length == 0 ? 1 : sections[0].lsn();
        if (first == 1) {
            writeOneSegment(f, 1, 1, sections);
            return;
        }
        writeOneSegment(f, 2, first, sections);
        long markLsn = sections[sections.length - 1].lsn() + 1;
        byte[] body = new byte[16];
        WalSegmentSet.putBe64(body, 0, 1);        // retires segment 1, which is absent
        WalSegmentSet.putBe64(body, 8, first);    // and says the log now begins where we start
        writeOneSegment(f, 3, markLsn, new Sec('K', markLsn, body));
    }

    private void writeOneSegment(File f, long seq, long firstLsn, Sec... sections) throws IOException {
        byte[] segHeader = WalTestKit.segmentHeader(seq, firstLsn);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(segHeader);
        int off = WalTestKit.SEG_HDR;
        for (Sec sec : sections) {
            byte[] image = WalTestKit.section(segHeader, off, sec.tag(), sec.lsn(), sec.body());
            out.write(image);
            off += image.length;
        }
        WalTestKit.write(WalTestKit.segment(f, seq), out.toByteArray());
    }

    /** 16-aligned capacity that fits {@code len} content bytes plus the 4-byte length word. */
    private static long cap(int len) { return (4L + len + 15) & ~15L; }

    // ---------------------------------------------------------------- the writer side

    @Test public void append_carries_its_base_lsn_as_a_delta_against_its_own_section() throws IOException {
        File f = newFile("stamp");
        byte[] base = Fixtures.payload(1, 1, 8);
        byte[] delta = Fixtures.payload(2, 1, 4);
        long recid;
        StoreWAL s = new StoreWAL(f);
        try {
            recid = s.put(base, Fixtures.RAW);
            s.updateWithHeadroom(recid, base, Fixtures.RAW, 64); // room to append into
            s.commit();                                          // LSN 1: the base image
            s.append(recid, delta, 0, delta.length);
            s.commit();                                          // LSN 2: the delta
        } finally {
            s.close();
        }
        byte[] all = WalTestKit.read(WalTestKit.onlySegment(f));
        // section 2's body: [T_APPEND][recid][baseDelta][len][bytes]
        int sec2 = WalTestKit.sectionOffset(all, 1);
        int p = sec2 + WalTestKit.SEC_HDR;
        assertEquals("T_APPEND", 3, all[p++] & 0xFF);
        long[] r = new long[1];
        p = unpack(all, p, r);
        assertEquals(recid, r[0]);
        p = unpack(all, p, r);
        assertEquals("base is the immediately preceding section", 1, r[0]);
        p = unpack(all, p, r);
        assertEquals(delta.length, r[0]);
    }

    /**
     * The identities must survive a reopen: the writer reads {@code contentBaseLsn} to stamp,
     * and after recovery that table exists only because replay rebuilt it while applying.
     */
    @Test public void append_after_reopen_stamps_the_base_replay_rebuilt() throws IOException {
        File f = newFile("reopen");
        byte[] base = Fixtures.payload(1, 1, 8);
        byte[] d1 = Fixtures.payload(2, 1, 4);
        byte[] d2 = Fixtures.payload(3, 1, 4);
        long recid;
        StoreWAL s = new StoreWAL(f);
        try {
            recid = s.put(base, Fixtures.RAW);
            s.updateWithHeadroom(recid, base, Fixtures.RAW, 64);
            s.commit();
            s.append(recid, d1, 0, d1.length);
            s.commit();
        } finally {
            s.close();
        }
        StoreWAL s2 = new StoreWAL(f);
        try {
            s2.append(recid, d2, 0, d2.length);
            s2.commit();
        } finally {
            s2.close();
        }
        StoreWAL s3 = new StoreWAL(f);
        try {
            assertArrayEquals(concat(base, d1, d2), s3.get(recid, Fixtures.RAW));
            s3.verify();
        } finally {
            s3.close();
        }
    }

    /**
     * <b>The checkpoint interplay.</b> {@code checkpointLocked} re-homes every record at the
     * snapshot's LSN, so the base a later append cites must be the SNAPSHOT's LSN, not the
     * pre-checkpoint one. Miss it and the reopen after that append is refused by the skip
     * audit — on a crash-free, entirely legitimate history.
     */
    @Test public void append_after_a_checkpoint_cites_the_snapshot_as_its_base() throws IOException {
        File f = newFile("ckpt");
        byte[] base = Fixtures.payload(1, 1, 8);
        byte[] delta = Fixtures.payload(2, 1, 4);
        long recid;
        StoreWAL s = new StoreWAL(f);
        try {
            recid = s.put(base, Fixtures.RAW);
            s.updateWithHeadroom(recid, base, Fixtures.RAW, 64);
            s.commit();
            s.checkpoint();  // the whole store is re-homed at the snapshot LSN
            s.append(recid, delta, 0, delta.length);
            s.commit();
        } finally {
            s.close();
        }
        StoreWAL s2 = new StoreWAL(f);
        try {
            assertArrayEquals(concat(base, delta), s2.get(recid, Fixtures.RAW));
            s2.verify();
        } finally {
            s2.close();
        }
    }

    /** Same, but for a record the checkpoint re-homes as PREALLOCATED: no content base at all. */
    @Test public void checkpoint_of_a_preallocated_record_leaves_no_content_base() throws IOException {
        File f = newFile("ckpt-p");
        byte[] v = Fixtures.payload(1, 1, 8);
        byte[] delta = Fixtures.payload(2, 1, 4);
        long recid;
        StoreWAL s = new StoreWAL(f);
        try {
            recid = s.preallocate();
            s.commit();
            s.checkpoint();
            s.updateWithHeadroom(recid, v, Fixtures.RAW, 64); // first content: a full image
            s.commit();
            s.append(recid, delta, 0, delta.length);          // now a delta on top of it
            s.commit();
        } finally {
            s.close();
        }
        StoreWAL s2 = new StoreWAL(f);
        try {
            assertArrayEquals(concat(v, delta), s2.get(recid, Fixtures.RAW));
            s2.verify();
        } finally {
            s2.close();
        }
    }

    @Test public void repeated_checkpoints_between_appends_stay_replayable() throws IOException {
        File f = newFile("ckpt-many");
        byte[] base = Fixtures.payload(1, 1, 8);
        long recid;
        StoreWAL s = new StoreWAL(f);
        byte[] expected = base;
        try {
            recid = s.put(base, Fixtures.RAW);
            s.updateWithHeadroom(recid, base, Fixtures.RAW, 256);
            s.commit();
            for (int i = 0; i < 5; i++) {
                byte[] d = Fixtures.payload(10 + i, 1, 4);
                s.append(recid, d, 0, d.length);
                s.commit();
                expected = concat(expected, d);
                s.checkpoint();
            }
        } finally {
            s.close();
        }
        StoreWAL s2 = new StoreWAL(f);
        try {
            assertArrayEquals(expected, s2.get(recid, Fixtures.RAW));
            s2.verify();
        } finally {
            s2.close();
        }
    }

    // ---------------------------------------------------------------- the replay table

    /**
     * §4.1's no-crash loss case, which the replay floor got wrong: the base was legitimately
     * discarded and a later image already contains those bytes. The delta is skipped, the
     * image clears the skip, the store opens.
     */
    @Test public void stranded_append_is_skipped_when_a_later_image_supersedes_it() throws IOException {
        File f = newFile("stranded");
        byte[] whole = Fixtures.payload(7, 1, 12); // base ++ delta, as a cleaner would copy it
        writeLog(f,
                section('S', 10, appendEntry(1, 5, Fixtures.payload(2, 1, 4))),
                section('C', 11, recordEntry(1, cap(whole.length), whole)));
        StoreWAL s = new StoreWAL(f);
        try {
            assertArrayEquals(whole, s.get(1, Fixtures.RAW));
            s.verify();
        } finally {
            s.close();
        }
    }

    /** The dead-recid history: the delete no-ops on a void recid and discharges the skip. */
    @Test public void stranded_append_followed_by_a_delete_opens_with_the_recid_void() throws IOException {
        File f = newFile("dead");
        writeLog(f,
                section('S', 10, appendEntry(1, 5, Fixtures.payload(2, 1, 4))),
                section('S', 11, deleteEntry(1)));
        StoreWAL s = new StoreWAL(f);
        try {
            try {
                s.get(1, Fixtures.RAW);
                fail("expected GetVoid: the record was deleted");
            } catch (DBException.GetVoid expected) { /* ok */ }
            s.verify();
        } finally {
            s.close();
        }
    }

    /**
     * The audit. A skip that nothing supersedes means the log depends on sections that are
     * gone — refuse the open rather than return a record missing acknowledged bytes. This is
     * the loudness the floor provided, restored WITHOUT letting the mechanism decide anything.
     */
    @Test public void a_skip_that_is_never_superseded_refuses_the_open() throws IOException {
        File f = newFile("audit");
        writeLog(f, section('S', 10, appendEntry(1, 5, Fixtures.payload(2, 1, 4))));
        try {
            new StoreWAL(f).close();
            fail("expected DataCorruption from the deferred skip audit");
        } catch (DBException.DataCorruption expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("skipped"));
        }
    }

    /** A prealloc is self-contained: it discharges a skip even though it restores no content. */
    @Test public void a_prealloc_supersedes_a_skipped_append() throws IOException {
        File f = newFile("audit-p");
        writeLog(f,
                section('S', 10, appendEntry(1, 5, Fixtures.payload(2, 1, 4))),
                section('S', 11, preallocEntry(1)));
        StoreWAL s = new StoreWAL(f);
        try {
            assertEquals(null, s.get(1, Fixtures.RAW)); // preallocated: present, no content
            s.verify();
        } finally {
            s.close();
        }
    }

    /**
     * Recid reuse. An old incarnation's stranded delta cites the OLD base LSN, which can never
     * equal a new incarnation's — exact identity is strictly stronger than the floor's
     * comparison, which only asked "is this below the newest self-contained entry?".
     */
    @Test public void a_delta_from_an_older_incarnation_never_applies_to_the_new_one() throws IOException {
        File f = newFile("reuse");
        byte[] a = Fixtures.payload(1, 1, 8);
        byte[] b = Fixtures.payload(9, 1, 8);
        writeLog(f,
                section('S', 10, recordEntry(1, cap(a.length), a)),
                section('S', 11, appendEntry(1, 6, Fixtures.payload(2, 1, 4))), // cites a base at LSN 5
                section('S', 12, recordEntry(1, cap(b.length), b)));
        StoreWAL s = new StoreWAL(f);
        try {
            assertArrayEquals("the stale delta must not be in the record", b, s.get(1, Fixtures.RAW));
            s.verify();
        } finally {
            s.close();
        }
    }

    /**
     * Cited base ABOVE the applied one: an image for this recid, between the two, is missing.
     * Defence in depth over the LSN density rule — the section LSNs here are consecutive, so
     * density sees nothing wrong; what is absent is a section's worth of THIS recid's history.
     */
    @Test public void a_base_above_the_applied_content_base_is_corruption() throws IOException {
        File f = newFile("gap");
        byte[] a = Fixtures.payload(1, 1, 8);
        byte[] other = Fixtures.payload(5, 1, 8);
        writeLog(f,
                section('S', 10, recordEntry(1, cap(a.length), a)),
                section('S', 11, recordEntry(2, cap(other.length), other)), // unrelated recid
                section('S', 12, appendEntry(1, 1, Fixtures.payload(2, 1, 4)))); // base 11, applied 10
        try {
            new StoreWAL(f).close();
            fail("expected DataCorruption: the cited base is not the applied one");
        } catch (DBException.DataCorruption expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("missing"));
        }
    }

    /** The decoder rules, checked before any mutation. */
    @Test public void a_zero_base_delta_is_rejected() throws IOException {
        File f = newFile("delta0");
        byte[] a = Fixtures.payload(1, 1, 8);
        writeLog(f,
                section('S', 10, recordEntry(1, cap(a.length), a)),
                section('S', 11, appendEntry(1, 0, Fixtures.payload(2, 1, 4))));
        try {
            new StoreWAL(f).close();
            fail("expected DataCorruption: an append may not cite its own section");
        } catch (DBException.DataCorruption expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("base delta"));
        }
    }

    @Test public void a_base_delta_below_the_first_lsn_is_rejected() throws IOException {
        File f = newFile("delta-neg");
        writeLog(f, section('S', 3, appendEntry(1, 5, Fixtures.payload(2, 1, 4)))); // base would be -2
        try {
            new StoreWAL(f).close();
            fail("expected DataCorruption: base LSN below 1");
        } catch (DBException.DataCorruption expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("base delta"));
        }
    }

    @Test public void two_entries_for_one_recid_in_one_section_is_corruption() throws IOException {
        File f = newFile("dup");
        byte[] a = Fixtures.payload(1, 1, 8);
        writeLog(f, section('S', 10,
                recordEntry(1, cap(a.length), a),
                appendEntry(1, 5, Fixtures.payload(2, 1, 4))));
        try {
            new StoreWAL(f).close();
            fail("expected DataCorruption: one entry per recid per section");
        } catch (DBException.DataCorruption expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("two WAL entries"));
        }
    }

    /**
     * {@code walPrealloc} no-ops on any set slot, so applying it over a content-live record
     * would leave the record there while the identities describe a preallocated one. Reject it
     * instead of silently diverging — ports must not be free to choose here.
     */
    @Test public void prealloc_over_a_content_live_record_is_corruption() throws IOException {
        File f = newFile("prealloc-live");
        byte[] a = Fixtures.payload(1, 1, 8);
        writeLog(f,
                section('S', 10, recordEntry(1, cap(a.length), a)),
                section('S', 11, preallocEntry(1)));
        try {
            new StoreWAL(f).close();
            fail("expected DataCorruption: PREALLOC over a content-live record");
        } catch (DBException.DataCorruption expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("content-live"));
        }
    }

    /**
     * A null-content record is neither void, nor P, nor content-live — the state the
     * review found the table did not cover. It is legal to prealloc over: the entry is
     * self-contained, applying it is a no-op in the engine, and the identities move as for any
     * other state-only entry.
     */
    @Test public void prealloc_over_a_null_content_record_is_accepted() throws IOException {
        File f = newFile("prealloc-null");
        writeLog(f,
                section('S', 10, recordEntry(1, 0, null)),
                section('S', 11, preallocEntry(1)));
        StoreWAL s = new StoreWAL(f);
        try {
            assertEquals(null, s.get(1, Fixtures.RAW));
            s.verify();
        } finally {
            s.close();
        }
    }

    /**
     * A null {@code T_RECORD} CLEARS the content base — it does not merely decline to set one.
     * Otherwise a stale base survives into a state where append is not valid, and a delta
     * citing it would apply to the wrong image.
     */
    @Test public void a_null_record_clears_the_content_base_so_a_stale_delta_is_skipped() throws IOException {
        File f = newFile("null-clears");
        byte[] a = Fixtures.payload(1, 1, 8);
        byte[] b = Fixtures.payload(9, 1, 8);
        writeLog(f,
                section('S', 10, recordEntry(1, cap(a.length), a)),
                section('S', 11, recordEntry(1, 0, null)),                          // content base cleared
                section('S', 12, appendEntry(1, 2, Fixtures.payload(2, 1, 4))),     // cites the dead base@10
                section('S', 13, recordEntry(1, cap(b.length), b)));                // discharges the skip
        StoreWAL s = new StoreWAL(f);
        try {
            assertArrayEquals(b, s.get(1, Fixtures.RAW));
            s.verify();
        } finally {
            s.close();
        }
    }

    /** A superseded image is RE-APPLIED, not skipped: walPut is idempotent, so ordering suffices. */
    @Test public void a_superseded_image_is_reapplied_in_order() throws IOException {
        File f = newFile("supersede");
        byte[] a = Fixtures.payload(1, 1, 8);
        byte[] b = Fixtures.payload(9, 1, 8);
        byte[] d = Fixtures.payload(2, 1, 4);
        writeLog(f,
                section('S', 10, recordEntry(1, cap(a.length) + 16, a)),
                section('S', 11, recordEntry(1, cap(b.length) + 16, b)),
                section('S', 12, appendEntry(1, 1, d))); // extends b, the newest image
        StoreWAL s = new StoreWAL(f);
        try {
            assertArrayEquals(concat(b, d), s.get(1, Fixtures.RAW));
            s.verify();
        } finally {
            s.close();
        }
    }

    // ---------------------------------------------------------------- helpers

    private static int secBodyLen(byte[] all, int secStart) {
        return (int) ByteBuffer.wrap(all).getLong(secStart + 9);
    }

    private static int unpack(byte[] a, int p, long[] out) {
        long ret = 0;
        int v;
        do { v = a[p++] & 0xFF; ret = (ret << 7) | (v & 0x7F); } while ((v & 0x80) == 0);
        out[0] = ret;
        return p;
    }

    private static byte[] concat(byte[]... parts) {
        int n = 0;
        for (byte[] p : parts) n += p.length;
        byte[] r = new byte[n];
        int at = 0;
        for (byte[] p : parts) {
            System.arraycopy(p, 0, r, at, p.length);
            at += p.length;
        }
        return r;
    }
}
