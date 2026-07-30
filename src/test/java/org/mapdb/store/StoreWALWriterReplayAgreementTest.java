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
 * <b>Everything the writer acknowledges must replay.</b> A commit that returns normally is a
 * promise; a reopen that then refuses the log breaks it just as badly as losing the bytes,
 * because the store is unopenable rather than merely stale.
 *
 * <p>Each case here is a place where the writer and the recovery decoder disagreed about what
 * is legal.
 */
public class StoreWALWriterReplayAgreementTest {

    private final List<File> files = new ArrayList<>();

    private File newFile(String tag) {
        try {
            File f = Files.createTempFile("mapdb5-wal-agree-" + tag, ".wal").toFile();
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

    /**
     * A rejected {@code append} — bad offset, bad length, null array — must leave the
     * transaction exactly as it found it. It used to leave an empty staged entry behind
     * (staging happens before the arguments are used), which the classifier then emitted as
     * {@code T_PREALLOC} over a still content-live record: the NEXT commit is acknowledged and
     * the reopen after it refuses the whole log.
     */
    @Test public void an_append_with_bad_arguments_stages_nothing() throws IOException {
        for (String kind : new String[]{"offset", "length", "range"}) {
            File f = newFile("badargs-" + kind);
            byte[] v = Fixtures.payload(1, 1, 8);
            long recid;
            StoreWAL s = new StoreWAL(f);
            try {
                recid = s.put(v, Fixtures.RAW);
                s.commit();
                byte[] data = new byte[4];
                try {
                    switch (kind) {
                        case "offset" -> s.append(recid, data, -1, 1);
                        case "length" -> s.append(recid, data, 0, -1);
                        default -> s.append(recid, data, 2, 99);
                    }
                    fail("expected an argument error for " + kind);
                } catch (RuntimeException expected) { /* the contract violation the caller made */ }
                s.commit(); // the application catches the bad call and commits its other work
            } finally {
                s.close();
            }
            StoreWAL s2 = new StoreWAL(f);
            try {
                assertArrayEquals("reopen after a rejected append (" + kind + ")",
                        v, s2.get(recid, Fixtures.RAW));
                s2.verify();
            } finally {
                s2.close();
            }
        }
    }

    /**
     * The capacity straddle. A staged base reports unlimited capacity, so an append may push a
     * merged record to exactly the plain-record maximum; adding the requested headroom then
     * overflows it. The writer encoded that as {@code cap = 0} — the code for "oversize, store
     * it linked" — but the content itself still FITS a plain record, so the decoder rejected
     * {@code cap = 0} as garbage and the acknowledged commit became an unopenable log.
     *
     * <p>Headroom is a hint; the record is what was promised. Clamping the capacity to the
     * plain maximum keeps the record plain and keeps the capacity exact, which is what a later
     * {@code T_APPEND} needs (§5.2: never a recomputed exact fit).
     */
    @Test public void a_merged_record_at_the_capacity_boundary_replays() throws IOException {
        File f = newFile("straddle");
        int baseLen = IndexVal.MAX_CAPACITY - 4 - 100;
        byte[] base = Fixtures.payload(1, 1, baseLen / 8 * 8 == baseLen ? baseLen / 8 : baseLen / 8 + 1);
        base = java.util.Arrays.copyOf(base, baseLen);
        byte[] tail = Fixtures.payload(2, 1, 13);
        tail = java.util.Arrays.copyOf(tail, 100);
        long recid;
        StoreWAL s = new StoreWAL(f);
        try {
            recid = s.put(Fixtures.payload(1, 1, 4), Fixtures.RAW);
            s.commit();
            s.updateWithHeadroom(recid, base, Fixtures.RAW, 64); // accepted: base+headroom fits
            long r = s.append(recid, tail, 0, tail.length);       // staged base: unlimited
            assertTrue("the append must be admitted", r != StoreDelta.REFUSED);
            s.commit();                                           // acknowledged
        } finally {
            s.close();
        }
        byte[] expected = new byte[baseLen + tail.length];
        System.arraycopy(base, 0, expected, 0, baseLen);
        System.arraycopy(tail, 0, expected, baseLen, tail.length);
        StoreWAL s2 = new StoreWAL(f);
        try {
            assertArrayEquals(expected, s2.get(recid, Fixtures.RAW));
            s2.verify();
        } finally {
            s2.close();
        }
    }

    /**
     * The v1 rule — "a nonempty file too short to hold a header is not an empty log" — is
     * <b>superseded by table H</b>, and the replacement is sharper. Under v2 the short file is a
     * <em>segment</em>, and its POSITION decides: a short highest segment is create-crash
     * residue (H2), because W2 writes the header before the directory entry is fsynced and
     * nothing above it can exist yet; the same bytes anywhere else are corruption, since
     * something above it proves its create completed once.
     *
     * <p>The silent-loss concern the v1 rule addressed is now N6's job: a v1 log is a file named
     * {@code <db>.wal}, and its mere presence refuses the open.
     */
    @Test public void a_short_segment_is_residue_when_highest_and_corruption_when_not() throws IOException {
        File f = newFile("short-high");
        byte[] a = Fixtures.payload(1, 1, 8);
        writeLog(f, section('S', 1, recordEntry(1, cap(a.length), a)));
        WalTestKit.write(WalTestKit.segment(f, 2), new byte[]{'M', 'D', 'B', '5', '.'});
        StoreWAL s = new StoreWAL(f);
        try {
            assertArrayEquals(a, s.get(1, Fixtures.RAW));
            assertEquals("residue unlinked at R2", 1, WalTestKit.segments(f).length);
            s.verify();
        } finally {
            s.close();
        }

        File g = newFile("short-low");
        writeLog(g, section('S', 1, recordEntry(1, cap(a.length), a)));
        // move the real segment up to 2 and leave a short one at 1
        WalTestKit.write(WalTestKit.segment(g, 2), rehomed(WalTestKit.read(WalTestKit.segment(g, 1)), 2));
        WalTestKit.write(WalTestKit.segment(g, 1), new byte[]{'M', 'D', 'B', '5', '.'});
        try {
            new StoreWAL(g).close();
            fail("expected DataCorruption for a short NON-highest segment");
        } catch (DBException.DataCorruption expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("highest"));
        }
    }

    /**
     * Rebuilds a one-section segment image under a new sequence number. A straight byte copy
     * would not do: both the segment header and every section CRC bind the segment identity,
     * which is the whole point of the §6.2 domain.
     */
    private static byte[] rehomed(byte[] seg, long newSeq) {
        byte[] header = WalTestKit.segmentHeader(newSeq, WalTestKit.lsnOf(seg, WalTestKit.SEG_HDR));
        int off = WalTestKit.SEG_HDR;
        byte[] body = java.util.Arrays.copyOfRange(seg, off + WalTestKit.SEC_HDR,
                WalTestKit.sectionEnd(seg, off));
        return WalTestKit.concat(header, WalTestKit.section(header, off,
                WalTestKit.tagOf(seg, off), WalTestKit.lsnOf(seg, off), body));
    }

    /**
     * Section LSNs are dense by construction — one per committed section, one per checkpoint,
     * and a rolled-back reservation burns none — so recovery requires them CONSECUTIVE, not
     * merely increasing (§6.1, Q5 S9). A gap means a section that was written and acknowledged
     * is no longer there, which is exactly the silent case a mere ordering check cannot see.
     */
    @Test public void a_gap_in_the_section_lsns_is_corruption() throws IOException {
        File f = newFile("density");
        byte[] a = Fixtures.payload(1, 1, 8);
        byte[] b = Fixtures.payload(9, 1, 8);
        writeLog(f,
                section('S', 1, recordEntry(1, cap(a.length), a)),
                section('S', 3, recordEntry(2, cap(b.length), b))); // LSN 2 is missing
        try {
            new StoreWAL(f).close();
            fail("expected DataCorruption for a non-consecutive section LSN");
        } catch (DBException.DataCorruption expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("consecutive"));
        }
    }

    /** The first section of a checkpointed log legitimately starts above 1. */
    @Test public void density_is_relative_to_the_first_retained_section() throws IOException {
        File f = newFile("density-ckpt");
        byte[] a = Fixtures.payload(1, 1, 8);
        byte[] b = Fixtures.payload(9, 1, 8);
        writeLog(f,
                section('C', 17, recordEntry(1, cap(a.length), a)),
                section('S', 18, recordEntry(2, cap(b.length), b)));
        StoreWAL s = new StoreWAL(f);
        try {
            assertArrayEquals(a, s.get(1, Fixtures.RAW));
            assertArrayEquals(b, s.get(2, Fixtures.RAW));
            s.verify();
        } finally {
            s.close();
        }
    }

    // ---------------------------------------------------------------- log building

    private static byte[] recordEntry(long recid, long cap, byte[] content) {
        DataOutput2 o = new DataOutput2(64);
        o.writeByte(2);
        o.packLong(recid);
        o.packLong(cap);
        o.packLong(content.length + 1);
        o.write(content);
        return java.util.Arrays.copyOf(o.buf, o.pos);
    }

    private static long cap(int len) { return (4L + len + 15) & ~15L; }

    /** A section that has not been placed yet: its CRCs depend on where it lands (§6.2). */
    private record Sec(char tag, long lsn, byte[] body) { }

    private static Sec section(char tag, long lsn, byte[]... entries) throws IOException {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        for (byte[] e : entries) body.write(e);
        return new Sec(tag, lsn, body.toByteArray());
    }

    /**
     * Lays the sections out, computing each one's CRC domain from its offset. A fixture starting
     * above LSN 1 is written as a POST-CLEAN log — segment 2 stating that start in its header, plus
     * segment 3 holding the mark that retires the absent segment 1 and attests the start — because
     * that is the only thing that legitimately produces one, and v3 checks the stated start rather
     * than inferring it.
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
        WalSegmentSet.putBe64(body, 0, 1);
        WalSegmentSet.putBe64(body, 8, first);
        writeOneSegment(f, 3, markLsn, new Sec('K', markLsn, body));
    }

    private void writeOneSegment(File f, long seq, long firstLsn, Sec... sections) throws IOException {
        byte[] segHeader = WalTestKit.segmentHeader(seq, firstLsn);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(segHeader);
        int off = WalTestKit.SEG_HDR;
        for (Sec s : sections) {
            byte[] image = WalTestKit.section(segHeader, off, s.tag(), s.lsn(), s.body());
            out.write(image);
            off += image.length;
        }
        WalTestKit.write(WalTestKit.segment(f, seq), out.toByteArray());
    }
}
