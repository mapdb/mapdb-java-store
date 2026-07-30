package org.mapdb.store;

import org.junit.After;
import org.junit.Test;
import org.mapdb.DBException;
import org.mapdb.io.DataOutput2;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The WAL <b>segment set</b>: namespace (N), segment header (H), sections across a segment
 * boundary (S), and clean-mark/unlink (K) rules, plus the writer obligations that are about
 * files rather than bytes (W2, W3, W6, W7). The letter/number labels are the stable rule
 * names used throughout {@link StoreWAL} and {@link WalSegmentSet}; this suite is where each
 * one is pinned.
 *
 * <p><b>Why so many hand-built images.</b> The expensive part of R2 to port is this state
 * machine, not the codec, and most of its rows are states no conforming writer
 * ever produces — a crash mid-unlink, a mark attesting bytes that never landed, a segment
 * copied under a new name. They exist on disk only after a crash, so they can only be tested
 * from crafted images. Each test below names the row it pins.
 */
public class StoreWALSegmentSetTest {

    private final List<File> files = new ArrayList<>();

    private File newFile(String tag) {
        try {
            File f = Files.createTempFile("mapdb5-wal-seg-" + tag, ".wal").toFile();
            f.delete();
            files.add(f);
            return f;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @After public void cleanup() {
        for (File f : files) WalTestKit.deleteStore(f);
        files.clear();
    }

    // ---------------------------------------------------------------- oracle

    private static TreeMap<Long, byte[]> snapshot(Store s) {
        TreeMap<Long, byte[]> snap = new TreeMap<>();
        PrimitiveIterator.OfLong it = s.getAllRecids();
        while (it.hasNext()) {
            long r = it.nextLong();
            snap.put(r, s.get(r, Fixtures.RAW));
        }
        return snap;
    }

    private static void assertState(Store s, TreeMap<Long, byte[]> snap) {
        TreeMap<Long, byte[]> actual = snapshot(s);
        assertEquals("recid set differs", snap.keySet(), actual.keySet());
        for (Long r : snap.keySet()) {
            assertArrayEquals("content differs at recid=" + r, snap.get(r), actual.get(r));
        }
    }

    // ---------------------------------------------------------------- image building

    private static byte[] recordEntry(long recid, byte[] content) {
        DataOutput2 o = new DataOutput2(64);
        o.writeByte(2);
        o.packLong(recid);
        o.packLong((4L + content.length + 15) & ~15L);
        o.packLong(content.length + 1);
        o.write(content);
        return java.util.Arrays.copyOf(o.buf, o.pos);
    }

    private record Sec(char tag, long lsn, byte[] body) { }

    private static Sec put(long recid, long lsn, byte[] content) {
        return new Sec('S', lsn, recordEntry(recid, content));
    }

    private static Sec image(long recid, long lsn, byte[] content) {
        return new Sec('C', lsn, recordEntry(recid, content));
    }

    /**
     * Builds a complete segment image, checksumming each section for the offset it lands at and
     * stating in the header the LSN of its first section — which is what a conforming writer
     * records and what R4 checks against the mark and the neighbouring segments.
     */
    private static byte[] segmentImage(long seq, Sec... sections) throws IOException {
        return segmentImage(seq, sections.length == 0 ? 1 : sections[0].lsn(), sections);
    }

    /** As above, but stating {@code firstLsn} explicitly — for empty segments and for surgery. */
    private static byte[] segmentImage(long seq, long firstLsn, Sec... sections) throws IOException {
        byte[] header = WalTestKit.segmentHeader(seq, firstLsn);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(header);
        int off = WalTestKit.SEG_HDR;
        for (Sec s : sections) {
            byte[] img = s.tag() == 'K'
                    ? WalTestKit.mark(header, off, s.lsn(), WalSegmentSet.be64(s.body(), 0),
                            WalSegmentSet.be64(s.body(), 8))
                    : WalTestKit.section(header, off, s.tag(), s.lsn(), s.body());
            out.write(img);
            off += img.length;
        }
        return out.toByteArray();
    }

    /**
     * A clean mark retiring everything at or below {@code through} and attesting that the retained
     * log then begins at {@code logStartLsn} — the LSN of the image that superseded those segments.
     */
    private static Sec mark(long lsn, long through, long logStartLsn) {
        byte[] body = new byte[16];
        WalSegmentSet.putBe64(body, 0, through);
        WalSegmentSet.putBe64(body, 8, logStartLsn);
        return new Sec('K', lsn, body);
    }

    private void writeSegment(File base, long seq, Sec... sections) throws IOException {
        WalTestKit.write(WalTestKit.segment(base, seq), segmentImage(seq, sections));
    }

    // ================= W2/W3: rollover =================

    /**
     * W3. The writer seals the active segment and opens the next only at a SECTION BOUNDARY, so
     * a non-final segment ends with zero trailing bytes — which is what lets recovery call any
     * tear in a non-final segment corruption without a lookahead. Verified structurally: every
     * non-final segment parses to exactly its own length.
     */
    @Test public void rollover_leaves_every_sealed_segment_ending_on_a_section_boundary() {
        File f = newFile("roll");
        TreeMap<Long, byte[]> oracle;
        StoreWAL s = new StoreWAL(f);
        try {
            s.setSegmentBytes(1024);
            s.setMinLogBytes(0); // cleaning would retire what we want to inspect
            oracle = churn(s, 0x5EED, 40);
        } finally {
            s.close();
        }
        File[] segs = WalTestKit.segments(f);
        assertTrue("expected several segments, got " + segs.length, segs.length >= 3);
        for (int i = 0; i < segs.length - 1; i++) {
            byte[] img = WalTestKit.read(segs[i]);
            int end = WalTestKit.sectionOffset(img, WalTestKit.sectionCount(img));
            assertEquals("sealed segment " + segs[i].getName() + " has trailing bytes",
                    img.length, end);
        }
        StoreWAL s2 = new StoreWAL(f);
        try {
            assertState(s2, oracle);
            s2.verify();
        } finally {
            s2.close();
        }
    }

    /** LSNs are global across the segment set and stay exactly consecutive over a rollover (S9). */
    @Test public void section_lsns_stay_consecutive_across_a_rollover() {
        File f = newFile("lsn-roll");
        StoreWAL s = new StoreWAL(f);
        try {
            s.setSegmentBytes(1024);
            s.setMinLogBytes(0);
            churn(s, 0xABBA, 30);
        } finally {
            s.close();
        }
        long expect = 1;
        for (File seg : WalTestKit.segments(f)) {
            byte[] img = WalTestKit.read(seg);
            int n = WalTestKit.sectionCount(img);
            for (int i = 0; i < n; i++) {
                int off = WalTestKit.sectionOffset(img, i);
                assertEquals("LSN density broken in " + seg.getName(), expect++, WalTestKit.lsnOf(img, off));
            }
        }
        assertTrue("expected many sections", expect > 30);
    }

    // ================= C9: truncation sweep across a segment boundary =================

    /**
     * C9. The surviving-commit oracle must hold across FILES, not just offsets: a torn tail is
     * tolerated only in the highest segment, and everything in the sealed segments below it is
     * already durable.
     */
    @Test public void truncation_sweep_across_a_segment_boundary() throws IOException {
        File src = newFile("c9-src");
        List<long[]> where = new ArrayList<>();   // {segmentSeq, segmentLen} after each commit
        List<TreeMap<Long, byte[]>> snaps = new ArrayList<>();
        StoreWAL s = new StoreWAL(src);
        try {
            s.setSegmentBytes(512);
            s.setMinLogBytes(0);
            Random rnd = new Random(0xC9);
            List<Long> live = new ArrayList<>();
            for (int c = 0; c < 25; c++) {
                if (live.isEmpty() || rnd.nextInt(3) == 0)
                    live.add(s.put(Fixtures.payload(live.size(), c, 40), Fixtures.RAW));
                else
                    s.update(live.get(rnd.nextInt(live.size())), Fixtures.payload(c, c, 40), Fixtures.RAW);
                s.commit();
                File[] segs = WalTestKit.segments(src);
                File active = segs[segs.length - 1];
                where.add(new long[]{seqOf(active), active.length()});
                snaps.add(snapshot(s));
            }
        } finally {
            s.close();
        }
        File[] segs = WalTestKit.segments(src);
        assertTrue("need a multi-segment log", segs.length >= 3);
        File active = segs[segs.length - 1];
        long activeSeq = seqOf(active);
        byte[] activeBytes = WalTestKit.read(active);

        File copy = newFile("c9-copy");
        for (int t = WalTestKit.SEG_HDR; t <= activeBytes.length; t++) {
            WalTestKit.deleteStore(copy);
            for (File seg : segs) {
                long seq = seqOf(seg);
                byte[] img = seq == activeSeq ? java.util.Arrays.copyOf(activeBytes, t) : WalTestKit.read(seg);
                WalTestKit.write(WalTestKit.segment(copy, seq), img);
            }
            TreeMap<Long, byte[]> expect = new TreeMap<>();
            for (int i = 0; i < where.size(); i++) {
                long[] w = where.get(i);
                if (w[0] < activeSeq || (w[0] == activeSeq && w[1] <= t)) expect = snaps.get(i);
            }
            StoreWAL rs = null;
            try {
                rs = new StoreWAL(copy);
                rs.verify();
                assertState(rs, expect);
            } catch (RuntimeException e) {
                fail("truncation of the active segment at " + t + " threw " + e);
            } finally {
                if (rs != null) rs.close();
            }
        }
    }

    // ================= C6/C12: where a tear is tolerated =================

    /** C6. A tear in a NON-final segment is corruption: something follows it, so W3 was violated. */
    @Test public void torn_tail_in_a_non_final_segment_is_corruption() throws IOException {
        File f = newFile("c6");
        writeSegment(f, 1, put(1, 1, Fixtures.payload(1, 1, 8)));
        writeSegment(f, 2, put(2, 2, Fixtures.payload(2, 1, 8)));
        File seg1 = WalTestKit.segment(f, 1);
        byte[] img = WalTestKit.read(seg1);
        WalTestKit.write(seg1, java.util.Arrays.copyOf(img, img.length - 3)); // cut mid-section
        try {
            new StoreWAL(f).close();
            fail("expected DataCorruption for a tear in a sealed segment");
        } catch (DBException.DataCorruption expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("non-final"));
        }
    }

    /** C12/H2. A torn 28-byte header on the HIGHEST segment is an ordinary create crash. */
    @Test public void torn_segment_header_on_the_highest_segment_is_residue() throws IOException {
        File f = newFile("c12");
        byte[] v = Fixtures.payload(1, 1, 8);
        writeSegment(f, 1, put(1, 1, v));
        byte[] halfHeader = java.util.Arrays.copyOf(WalTestKit.segmentHeader(2), 14);
        WalTestKit.write(WalTestKit.segment(f, 2), halfHeader);
        StoreWAL s = new StoreWAL(f);
        try {
            assertArrayEquals(v, s.get(1, Fixtures.RAW));
            assertEquals("residue removed at R2", 1, WalTestKit.segments(f).length);
            s.verify();
        } finally {
            s.close();
        }
    }

    /**
     * H3, non-highest. The same bytes below the top are corruption, not residue: create-crash
     * residue is by construction always highest, because nothing above it exists yet.
     */
    @Test public void torn_segment_header_below_the_highest_segment_is_corruption() throws IOException {
        File f = newFile("h3-low");
        writeSegment(f, 2, put(1, 1, Fixtures.payload(1, 1, 8)));
        byte[] hdr = WalTestKit.segmentHeader(1);
        hdr[3] ^= 0xFF; // CRC no longer matches
        WalTestKit.write(WalTestKit.segment(f, 1), hdr);
        try {
            new StoreWAL(f).close();
            fail("expected DataCorruption for a damaged non-highest segment header");
        } catch (DBException.DataCorruption expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("highest"));
        }
    }

    /** N5/H7. A header whose sequence disagrees with its name is a copied or renamed segment. */
    @Test public void header_sequence_disagreeing_with_the_name_is_corruption() throws IOException {
        File f = newFile("h7");
        byte[] img = segmentImage(1, put(1, 1, Fixtures.payload(1, 1, 8)));
        WalTestKit.write(WalTestKit.segment(f, 5), img); // segment 1's bytes under name 5
        try {
            new StoreWAL(f).close();
            fail("expected DataCorruption for a header/name mismatch");
        } catch (DBException.DataCorruption expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("sequence"));
        }
    }

    // ================= C7: the CRC domain binds segment AND offset =================

    /**
     * C7, half one. A section byte-copied from another SEGMENT fails its CRCs, because both are
     * computed over {@code segmentHeader || be64(offset) || bytes} — the 28 header bytes are used
     * verbatim as an identity string.
     */
    @Test public void section_byte_copied_from_another_segment_is_rejected() throws IOException {
        File good = newFile("c7-seg-ok");
        writeSegment(good, 1, put(1, 1, Fixtures.payload(1, 1, 8)));
        writeSegment(good, 2, put(2, 2, Fixtures.payload(2, 1, 8)));
        writeSegment(good, 3, put(3, 3, Fixtures.payload(3, 1, 8)));
        StoreWAL ok = new StoreWAL(good);   // the twin: proves the shape is otherwise legal
        try {
            assertEquals(3, snapshot(ok).size());
        } finally {
            ok.close();
        }

        File f = newFile("c7-seg");
        writeSegment(f, 1, put(1, 1, Fixtures.payload(1, 1, 8)));
        writeSegment(f, 3, put(3, 3, Fixtures.payload(3, 1, 8)));
        // segment 2's only section is a section built for SEGMENT 1's identity
        byte[] header1 = WalTestKit.segmentHeader(1);
        byte[] stolen = WalTestKit.section(header1, WalTestKit.SEG_HDR, 'S', 2,
                recordEntry(2, Fixtures.payload(2, 1, 8)));
        WalTestKit.write(WalTestKit.segment(f, 2),
                WalTestKit.concat(WalTestKit.segmentHeader(2), stolen));
        try {
            new StoreWAL(f).close();
            fail("expected the copied section to fail its CRC");
        } catch (DBException.DataCorruption expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("non-final"));
        }
    }

    /**
     * C7, half two. The same section copied to a different OFFSET of its own segment also fails
     * — which the segment identity alone would not have caught.
     */
    @Test public void section_byte_copied_to_another_offset_is_rejected() throws IOException {
        File f = newFile("c7-off");
        byte[] header1 = WalTestKit.segmentHeader(1);
        byte[] first = WalTestKit.section(header1, WalTestKit.SEG_HDR, 'S', 1,
                recordEntry(1, Fixtures.payload(1, 1, 8)));
        // a section that would be perfectly valid — right LSN, right segment — at offset 28,
        // placed at the offset AFTER the first section instead
        byte[] misplaced = WalTestKit.section(header1, WalTestKit.SEG_HDR, 'S', 2,
                recordEntry(2, Fixtures.payload(2, 1, 8)));
        WalTestKit.write(WalTestKit.segment(f, 1), WalTestKit.concat(header1, first, misplaced));
        writeSegment(f, 2, put(3, 3, Fixtures.payload(3, 1, 8)));   // makes segment 1 non-final
        try {
            new StoreWAL(f).close();
            fail("expected the misplaced section to fail its CRC");
        } catch (DBException.DataCorruption expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("non-final"));
        }
    }

    // ================= C4/C5/K: the clean mark =================

    /**
     * C4. A crash between the cleaner's forced {@code 'C'} sections and its {@code 'K'} leaves
     * the old segment present with its images duplicated above. Both replay, the state is
     * identical, and cleaning simply re-runs — the whole crash window is state-preserving.
     */
    @Test public void images_without_a_mark_replay_alongside_the_segment_they_supersede() throws IOException {
        File f = newFile("c4");
        byte[] v1 = Fixtures.payload(1, 1, 8);
        byte[] v2 = Fixtures.payload(2, 1, 8);
        writeSegment(f, 1, put(1, 1, v1), put(2, 2, v2));
        writeSegment(f, 2, image(1, 3, v1), image(2, 4, v2)); // no 'K': the crash window
        StoreWAL s = new StoreWAL(f);
        try {
            assertArrayEquals(v1, s.get(1, Fixtures.RAW));
            assertArrayEquals(v2, s.get(2, Fixtures.RAW));
            assertEquals("nothing authorized removal, so nothing is removed",
                    2, WalTestKit.segments(f).length);
            s.verify();
        } finally {
            s.close();
        }
    }

    /**
     * C5/K5. A crash between the forced {@code 'K'} and the unlink leaves the authorized
     * segments present; the next open removes them. The state is identical either way — the
     * mark authorizes garbage collection and nothing else, and is NEVER a replay filter.
     */
    @Test public void a_forced_mark_whose_unlink_was_lost_is_completed_at_open() throws IOException {
        File f = newFile("c5");
        byte[] v1 = Fixtures.payload(1, 1, 8);
        byte[] v2 = Fixtures.payload(2, 1, 8);
        writeSegment(f, 1, put(1, 1, v1), put(2, 2, v2));
        writeSegment(f, 2, image(1, 3, v1), image(2, 4, v2), mark(5, 1, 3));
        StoreWAL s = new StoreWAL(f);
        try {
            assertArrayEquals(v1, s.get(1, Fixtures.RAW));
            assertArrayEquals(v2, s.get(2, Fixtures.RAW));
            File[] segs = WalTestKit.segments(f);
            assertEquals("the authorized segment is unlinked at R5", 1, segs.length);
            assertEquals(WalTestKit.segment(f, 2).getName(), segs[0].getName());
            s.verify();
        } finally {
            s.close();
        }
    }

    /** K4. A mark cannot authorize removing itself; CRC-valid means writer defect or forgery. */
    @Test public void a_mark_authorizing_its_own_segment_is_corruption() throws IOException {
        File f = newFile("k4");
        writeSegment(f, 1, put(1, 1, Fixtures.payload(1, 1, 8)));
        writeSegment(f, 2, image(1, 2, Fixtures.payload(1, 1, 8)), mark(3, 2, 2));
        try {
            new StoreWAL(f).close();
            fail("expected DataCorruption for a self-authorizing mark");
        } catch (DBException.DataCorruption expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("including itself"));
        }
    }

    /**
     * K9/N3. A crash mid-unlink can expose ANY subset of the authorized segments, not merely a
     * prefix: syscall order says nothing about the order removals persist before the directory
     * fsync. So an interior gap AT OR BELOW the mark is a legitimate crash image.
     */
    @Test public void an_interior_gap_at_or_below_the_mark_is_legal() throws IOException {
        File f = newFile("n3-ok");
        byte[] v = Fixtures.payload(1, 1, 8);
        writeSegment(f, 1, put(1, 1, v));                        // segment 2 was already removed
        writeSegment(f, 3, image(1, 3, v), mark(4, 2, 3));
        StoreWAL s = new StoreWAL(f);
        try {
            assertArrayEquals(v, s.get(1, Fixtures.RAW));
            assertEquals(1, WalTestKit.segments(f).length);
            s.verify();
        } finally {
            s.close();
        }
    }

    /**
     * N3, corrected: a missing sequence number above the mark is adjudicated by the <b>LSN
     * witness</b>, not by its existence. A segment that vanished after holding an accepted section
     * necessarily leaves an LSN hole, so THAT is corruption.
     */
    @Test public void a_gap_above_the_mark_with_an_lsn_hole_is_corruption() throws IOException {
        File f = newFile("n3-bad");
        writeSegment(f, 1, put(1, 1, Fixtures.payload(1, 1, 8)));
        writeSegment(f, 3, put(2, 3, Fixtures.payload(2, 1, 8)));   // LSN 2 is gone with segment 2
        try {
            new StoreWAL(f).close();
            fail("expected DataCorruption for a gap that swallowed an LSN");
        } catch (DBException.DataCorruption expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("are gone"));
        }
    }

    /**
     * The other half of the corrected N3, and the reason it had to be corrected: an interior
     * namespace gap whose LSN chain is EXACTLY CONSECUTIVE across it is legal, because Q5's own
     * W6 + R2 manufacture one. This is the disk state
     * {@link #a_removed_orphans_sequence_number_is_not_reused} produces; under the literal rule
     * ("any interior gap above the mark is corruption") the store bricks permanently with every
     * acknowledged commit intact.
     */
    @Test public void a_gap_above_the_mark_whose_lsn_chain_is_dense_is_legal() throws IOException {
        File f = newFile("n3-ok");
        byte[] v1 = Fixtures.payload(1, 1, 8);
        byte[] v2 = Fixtures.payload(2, 1, 8);
        writeSegment(f, 1, put(1, 1, v1));
        writeSegment(f, 3, put(2, 2, v2));      // seq 2 was residue: burnt by W6, never a section
        StoreWAL s = new StoreWAL(f);
        try {
            assertArrayEquals(v1, s.get(1, Fixtures.RAW));
            assertArrayEquals(v2, s.get(2, Fixtures.RAW));
            s.verify();
        } finally {
            s.close();
        }
    }

    /** S9 across a boundary: the LSN space is global over the retained set. */
    @Test public void an_lsn_gap_across_a_segment_boundary_is_corruption() throws IOException {
        File f = newFile("s9-cross");
        writeSegment(f, 1, put(1, 1, Fixtures.payload(1, 1, 8)));
        writeSegment(f, 2, put(2, 9, Fixtures.payload(2, 1, 8))); // LSNs 2..8 are missing
        try {
            new StoreWAL(f).close();
            fail("expected DataCorruption for an LSN gap across segments");
        } catch (DBException.DataCorruption expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("are gone"));
        }
    }

    /**
     * H8, non-highest. A valid but EMPTY segment below the active one is retained, not swept.
     * W7's post-truncation rotate is a conforming producer of one, and unlinking it would CREATE
     * an interior gap — so an ordinary torn-tail recovery would poison the store permanently:
     * truncate-to-empty, rotate, next open unlinks the empty segment, third open fails N3
     * forever, with nothing wrong in the store's history.
     */
    @Test public void a_valid_empty_non_highest_segment_is_retained() throws IOException {
        File f = newFile("h8");
        byte[] v1 = Fixtures.payload(1, 1, 8);
        byte[] v2 = Fixtures.payload(2, 1, 8);
        writeSegment(f, 1, put(1, 1, v1));
        // empty, but stating the LSN its first section WOULD have held — which is exactly what
        // separates W7's legitimately empty rotate target from a segment whose sections vanished
        WalTestKit.write(WalTestKit.segment(f, 2), WalTestKit.segmentHeader(2, 2));
        writeSegment(f, 3, put(2, 2, v2));
        StoreWAL s = new StoreWAL(f);
        try {
            assertArrayEquals(v1, s.get(1, Fixtures.RAW));
            assertArrayEquals(v2, s.get(2, Fixtures.RAW));
            assertEquals("the empty segment must survive", 3, WalTestKit.segments(f).length);
            s.verify();
        } finally {
            s.close();
        }
    }

    /**
     * The full W7 sequence, twice over, which is the history that bricked the v1 draft:
     * torn tail → truncate to empty → rotate → reopen → reopen. The empty segment 1 must still
     * be there, and the third open must not fail N3.
     */
    @Test public void truncate_to_empty_then_rotate_survives_repeated_reopens() {
        File f = newFile("w7");
        byte[] v;
        StoreWAL s = new StoreWAL(f);
        try {
            v = Fixtures.payload(1, 1, 8);
            s.put(v, Fixtures.RAW);
            s.commit();
        } finally {
            s.close();
        }
        // tear the ONLY section, so the valid prefix is empty
        File seg1 = WalTestKit.segment(f, 1);
        byte[] img = WalTestKit.read(seg1);
        img[img.length - 1] ^= 0x5A;
        WalTestKit.write(seg1, img);

        for (int open = 0; open < 3; open++) {
            StoreWAL r = new StoreWAL(f);
            try {
                assertTrue("nothing committed survives the tear", snapshot(r).isEmpty());
                r.verify();
            } finally {
                r.close();
            }
            assertEquals("open " + open + ": segment 1 truncated to its header",
                    WalTestKit.SEG_HDR, WalTestKit.segment(f, 1).length());
            assertEquals("open " + open + ": exactly one rotate", 2, WalTestKit.segments(f).length);
        }
    }

    // ================= W6: sequence numbers are never reused =================

    /**
     * W6. {@code nextSeq} is one above the highest sequence appearing in any enumerated NAME,
     * sampled before recovery unlinks anything — residue included. A stale directory entry can
     * then never alias a segment a later create reuses.
     */
    @Test public void a_removed_orphans_sequence_number_is_not_reused() throws IOException {
        File f = newFile("w6");
        writeSegment(f, 1, put(1, 1, Fixtures.payload(1, 1, 8)));
        WalTestKit.write(WalTestKit.segment(f, 2), new byte[0]); // H1 residue, highest
        StoreWAL s = new StoreWAL(f);
        try {
            assertEquals("residue unlinked", 1, WalTestKit.segments(f).length);
            s.setSegmentBytes(WalTestKit.SEG_HDR + WalTestKit.SEC_HDR); // force a rollover
            s.put(Fixtures.payload(2, 1, 8), Fixtures.RAW);
            s.commit();
            assertNotNull("the successor must NOT reuse the orphan's sequence number",
                    WalTestKit.segment(f, 3));
            assertTrue("segment 3 expected, got " + java.util.Arrays.toString(WalTestKit.segments(f)),
                    WalTestKit.segment(f, 3).exists());
        } finally {
            s.close();
        }
        // AND THE STORE MUST STILL OPEN. Stopping at the disk state above is what let the
        // permanent brick hide: W6 and R2 have just manufactured the interior gap {1, 3} that
        // N3's literal wording calls corruption, so every later open used to fail on a history
        // containing one ordinary crash and nothing else.
        StoreWAL s2 = new StoreWAL(f);
        try {
            assertArrayEquals(Fixtures.payload(1, 1, 8), s2.get(1, Fixtures.RAW));
            assertArrayEquals(Fixtures.payload(2, 1, 8), s2.get(2, Fixtures.RAW));
            s2.verify();
        } finally {
            s2.close();
        }
    }

    /**
     * The gap at the MARK BOUNDARY. A chain seeded inside the retained set has
     * no predecessor for its first segment, so a segment vanishing directly above the mark — while
     * the superseded segment below it is still present to witness the hole — used to open silently
     * and then unlink that witness at R5, destroying the evidence on a corrupt image.
     */
    @Test public void a_segment_vanishing_at_the_mark_boundary_is_corruption() throws IOException {
        File f = newFile("mark-boundary");
        writeSegment(f, 1, put(1, 1, Fixtures.payload(1, 1, 8)));
        // segment 2 carried LSN 2 — acknowledged commits ABOVE the mark — and is absent
        writeSegment(f, 3, put(3, 3, Fixtures.payload(3, 1, 8)), mark(4, 1, 2));
        // Refused by the floor, which now runs unconditionally — the retained set begins with an
        // 'S', so the image the mark claims to have superseded segment 2 with does not exist.
        expectCorruption(f, "the clean mark attests it begins at");
        // and the refusal must not have destroyed the witness (Q5 §2.1: no mutation on refusal)
        assertTrue("segment 1 is the evidence and must survive the refusal",
                WalTestKit.segment(f, 1).exists());
    }

    /**
     * N4. A store directory is allowed to contain other things. Only the exact pattern —
     * prefix, then 16 LOWERCASE hex digits with a non-negative {@code int64} value, on a regular
     * file — is a segment; everything else is ignored rather than rejected. Enumeration is
     * case-sensitive even on a case-insensitive filesystem, and a name whose value is ≥ 2⁶³ is
     * never parsed.
     */
    @Test public void names_that_are_not_segments_are_ignored() throws IOException {
        File f = newFile("n4");
        byte[] v = Fixtures.payload(1, 1, 8);
        writeSegment(f, 1, put(1, 1, v));
        File dir = f.getAbsoluteFile().getParentFile();
        String prefix = f.getAbsoluteFile().getName() + ".wal.";
        List<File> junk = List.of(
                new File(dir, prefix + "000000000000000"),        // 15 digits
                new File(dir, prefix + "00000000000000001"),      // 17 digits
                new File(dir, prefix + "00000000000000AB"),       // uppercase hex
                new File(dir, prefix + "000000000000zzzz"),       // not hex
                new File(dir, prefix + "ffffffffffffffff"),       // >= 2^63: negative as int64
                new File(dir, f.getAbsoluteFile().getName() + ".wal.ckpt"));
        for (File j : junk) WalTestKit.write(j, new byte[]{1, 2, 3});
        File asDir = new File(dir, prefix + "0000000000000009");
        assertTrue(asDir.mkdir());
        try {
            StoreWAL s = new StoreWAL(f);
            try {
                assertArrayEquals(v, s.get(1, Fixtures.RAW));
                s.verify();
            } finally {
                s.close();
            }
            for (File j : junk) assertTrue("must not be touched: " + j.getName(), j.exists());
            assertTrue("a directory matching the pattern is not a segment", asDir.isDirectory());
        } finally {
            for (File j : junk) j.delete();
            asDir.delete();
        }
    }

    /**
     * S8. A {@code 'K'} body is exactly {@code cleanedThroughSeq int64}, nothing else. CRC-valid
     * with the wrong length means a writer defect, not rot, and it is rejected BEFORE any
     * mutation — a mark is the one section that authorizes deleting data.
     */
    @Test public void a_clean_mark_with_the_wrong_body_length_is_corruption() throws IOException {
        File f = newFile("s8");
        writeSegment(f, 1, put(1, 1, Fixtures.payload(1, 1, 8)));
        byte[] header = WalTestKit.segmentHeader(2);
        byte[] bad = WalTestKit.section(header, WalTestKit.SEG_HDR, 'K', 2, new byte[]{0, 0, 0, 1});
        WalTestKit.write(WalTestKit.segment(f, 2), WalTestKit.concat(header, bad));
        try {
            new StoreWAL(f).close();
            fail("expected DataCorruption for a malformed clean mark");
        } catch (DBException.DataCorruption expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("clean mark body"));
        }
    }

    // ================= W9: a failed write never leaves an open store =================

    /**
     * W9. After a failed or partial section write the writer must not append into that segment
     * again — it fails the store CLOSED. The v1 code threw without closing, so the caller's retry
     * wrote a complete, forced, ACKNOWLEDGED section after the garbage at {@code lastLsn + 1},
     * while the suspect-header lookahead demands exactly {@code lastLsn + 2} — and the next open
     * truncated at the garbage, discarding the acknowledged commit.
     *
     * <p>Driven here through rollover's directory fsync (W2), the one failure point reachable
     * without a channel-level fault injector. What it pins is the DISPOSITION: the store is
     * closed, later calls are refused, and reopen recovers every acknowledged commit.
     */
    @Test public void a_failed_rollover_fails_the_store_closed() {
        File f = newFile("w9");
        byte[] acked = Fixtures.payload(1, 1, 8);
        long r;
        StoreWAL s = new StoreWAL(f);
        try {
            s.setSegmentBytes(WalTestKit.SEG_HDR + WalTestKit.SEC_HDR); // roll on the next commit
            s.setMinLogBytes(0);
            r = s.put(acked, Fixtures.RAW);
            s.commit();                                                  // acknowledged
            StoreWAL.testSetDirectorySync(dir -> {
                throw new IOException("forced directory fsync failure");
            });
            try {
                s.put(Fixtures.payload(2, 1, 8), Fixtures.RAW);
                s.commit();
                fail("expected the rollover to fail the commit");
            } catch (DBException expected) {
                assertTrue(expected.getMessage(), expected.getMessage().contains("store closed"));
            }
            assertTrue("a failed write must not leave an open store", s.isClosed());
            try {
                s.commit();
                fail("closed WAL accepted a retry");
            } catch (DBException.StoreClosed expected) { /* ok */ }
        } finally {
            StoreWAL.testSetDirectorySync(null);
            if (!s.isClosed()) s.close();
        }

        StoreWAL s2 = new StoreWAL(f);
        try {
            assertArrayEquals("the acknowledged commit must survive", acked, s2.get(r, Fixtures.RAW));
            s2.verify();
        } finally {
            s2.close();
        }
    }

    // ================= §3.1: one process at a time =================

    /** A writable open takes an exclusive store lock and holds it for the store's lifetime. */
    @Test public void a_second_writable_open_is_refused_while_the_first_holds_the_lock() {
        File f = newFile("lock");
        StoreWAL s = new StoreWAL(f);
        try {
            new StoreWAL(f).close();
            fail("expected the second open to be refused");
        } catch (DBException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("already open"));
        } finally {
            s.close();
        }
        new StoreWAL(f).close(); // the lock is released with the store
    }

    // ================= R-RO: read-only opens =================

    /**
     * R-RO. A read-only open runs R0, R1, R3, R4 and R6 and computes {@code nextLsn}/{@code
     * nextSeq} by the same rules WITHOUT acting on them, so it recovers an identical record map
     * while leaving residue and superseded segments on disk for the next writable open.
     *
     * <p>It DOES create {@code <db>.lock} — §3.1's shared lock is what stops a later writer
     * recovering under a reader's feet, and the file is the only way to hold one. Asserted
     * explicitly, because {@link #fileSizes} enumerates only segments and so cannot see it: an
     * unasserted side effect in a test named "mutates nothing" is how that claim goes stale.
     */
    @Test public void a_read_only_open_recovers_the_same_state_and_mutates_nothing() throws IOException {
        File f = newFile("ro");
        byte[] v1 = Fixtures.payload(1, 1, 8);
        byte[] v2 = Fixtures.payload(2, 1, 8);
        writeSegment(f, 1, put(1, 1, v1), put(2, 2, v2));
        writeSegment(f, 2, image(1, 3, v1), image(2, 4, v2), mark(5, 1, 3));
        WalTestKit.write(WalTestKit.segment(f, 3), new byte[0]); // residue a writable open sweeps

        long[] before = fileSizes(f);
        File lock = new File(f.getPath() + ".lock");
        assertTrue("precondition: no lock file yet", !lock.exists() || lock.delete());
        StoreWAL ro = StoreWAL.openReadOnly(f);
        try {
            assertArrayEquals(v1, ro.get(1, Fixtures.RAW));
            assertArrayEquals(v2, ro.get(2, Fixtures.RAW));
            ro.verify();
            try {
                ro.put(Fixtures.payload(9, 9, 4), Fixtures.RAW);
                fail("a read-only store accepted a write");
            } catch (DBException expected) {
                assertTrue(expected.getMessage(), expected.getMessage().contains("read-only"));
            }
        } finally {
            ro.close();
        }
        assertArrayEquals("a read-only open must not touch the SEGMENT namespace",
                before, fileSizes(f));
        assertTrue("a read-only open takes a shared lock, so it creates <db>.lock", lock.exists());

        StoreWAL rw = new StoreWAL(f);
        try {
            assertArrayEquals(v1, rw.get(1, Fixtures.RAW));
            assertEquals("the writable open does the collecting", 1, WalTestKit.segments(f).length);
        } finally {
            rw.close();
        }
    }

    /**
     * §4.1 reserves sequence 0 for "no clean mark", so no conforming writer can create it. It is
     * refused at R1, where the reason can be stated, rather than falling through to R4 and being
     * refused there as an accident of the retained set coming out empty.
     */
    @Test public void sequence_zero_is_refused_with_its_own_reason() throws IOException {
        File f = newFile("seq0");
        writeSegment(f, 0, put(1, 1, Fixtures.payload(1, 1, 8)));
        writeSegment(f, 1, put(2, 2, Fixtures.payload(2, 1, 8)));
        try {
            new StoreWAL(f).close();
            fail("expected DataCorruption for a segment at sequence 0");
        } catch (DBException.DataCorruption expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("reserved"));
        }
    }

    // ================= R4's floor: the retained log must be anchored at its bottom =================
    //
    // N3 and S9 pin the gaps BETWEEN retained segments and the density ACROSS them — every check
    // has a predecessor to compare against, so the LOWEST retained segment and the LOWEST retained
    // LSN were pinned by nothing. Three images passed every other rule with an acknowledged commit
    // gone. Found by StoreWALSegmentEventEnumeratorTest; kept here as named counterexamples,
    // because a rule with no counterexample beside it is the first thing a port drops.

    /**
     * A log whose first segment was unlinked with nothing authorizing it must be refused — caught
     * by the LSN anchor, not by the segment number, since a legitimate fresh store may itself start
     * above {@code FIRST_SEQ} after a crashed first create.
     */
    @Test public void the_lowest_segment_may_not_simply_be_absent() {
        File f = newFile("floor-gone");
        StoreWAL s = oneSectionPerSegment(f);
        try {
            s.put(Fixtures.payload(1, 1, 8), Fixtures.RAW);
            s.commit();
            s.put(Fixtures.payload(2, 1, 8), Fixtures.RAW);
            s.commit();
        } finally {
            s.close();
        }
        assertTrue(WalTestKit.segment(f, 1).delete());
        expectCorruption(f, "an unmarked log must begin at LSN 1");
    }

    /**
     * The same hole one layer down: emptying the first segment leaves the SEQUENCE run intact and
     * the LSN chain internally dense — H8 skips an empty segment — while LSN 1 is gone.
     */
    @Test public void the_lowest_segment_may_not_be_emptied_either() throws IOException {
        File f = newFile("floor-empty");
        StoreWAL s = oneSectionPerSegment(f);
        try {
            s.put(Fixtures.payload(1, 1, 8), Fixtures.RAW);
            s.commit();
            s.put(Fixtures.payload(2, 1, 8), Fixtures.RAW);
            s.commit();
        } finally {
            s.close();
        }
        truncateTo(f, 1, WalTestKit.SEG_HDR);
        expectCorruption(f, "sections between them are gone");
    }

    /**
     * The failure §6.1 invented S9 to catch, in the one place S9 cannot see: a clean whose
     * {@code 'C'} sections vanished WHOLLY. The mark survives and authorizes the already-completed
     * removal of everything below, so without the floor the store opens EMPTY.
     */
    @Test public void a_clean_mark_whose_image_vanished_must_not_open_an_empty_store()
            throws IOException {
        File f = newFile("floor-image");
        long r;
        StoreWAL s = oneSectionPerSegment(f);
        try {
            r = s.put(Fixtures.payload(1, 1, 8), Fixtures.RAW);
            s.commit();
            s.checkpoint();
        } finally {
            s.close();
        }
        // the checkpoint left {imageSeq: 'C', imageSeq+1: 'K'} and unlinked everything below
        File[] segs = WalTestKit.segments(f);
        assertEquals("expected the 'C' and 'K' segments", 2, segs.length);
        assertEquals('C', WalTestKit.tagOf(WalTestKit.read(segs[0]), WalTestKit.SEG_HDR));
        truncateTo(f, seqOf(segs[0]), WalTestKit.SEG_HDR);
        expectCorruption(f, "sections between them are gone");
        // and the record really was the only thing at stake
        assertTrue("the vanished image held the store's only copy of recid " + r, r > 0);
    }

    /**
     * The legal shapes the floor must NOT refuse. W7's truncate-then-rotate is a conforming
     * producer of an empty NON-FINAL segment (H8), and K3 makes a torn-away {@code 'K'} legal —
     * there the {@code 'C'} image alone anchors the log, which is why the floor's escape hatch is
     * the image rather than the mark.
     */
    @Test public void an_empty_interior_segment_is_legal_but_an_erased_mark_is_not()
            throws IOException {
        // (a) W7: an empty interior segment, with the LSN chain continuous across it
        File a = newFile("floor-ok-w7");
        long r1;
        StoreWAL sa = oneSectionPerSegment(a);
        try {
            r1 = sa.put(Fixtures.payload(1, 1, 8), Fixtures.RAW);
            sa.commit();
            sa.put(Fixtures.payload(2, 1, 8), Fixtures.RAW);
            sa.commit();
        } finally {
            sa.close();
        }
        byte[] seg2 = WalTestKit.read(WalTestKit.segment(a, 2));
        truncateTo(a, 2, seg2.length - 3);            // tear it: recovery empties and rotates
        byte[] expected1 = Fixtures.payload(1, 1, 8);
        StoreWAL sb = oneSectionPerSegment(a);
        try {
            assertArrayEquals(expected1, sb.get(r1, Fixtures.RAW));
            sb.put(Fixtures.payload(3, 1, 8), Fixtures.RAW);
            sb.commit();
            sb.verify();
        } finally {
            sb.close();
        }
        StoreWAL sc = new StoreWAL(a);                // {1: lsn 1, 2: empty, 3: lsn 2}
        try {
            assertArrayEquals(expected1, sc.get(r1, Fixtures.RAW));
            sc.verify();
        } finally {
            sc.close();
        }

        // (b) A mark erased AFTER its unlink persisted is refused, and that is the right answer.
        // v2 accepted it, on the reasoning that the 'C' image alone reconstructs the store — which
        // is true, but the image is not reachable: the 'K' is forced BEFORE the unlink (W5), so a
        // crash cannot lose the mark while keeping its effect. Accepting it means accepting an
        // unmarked log that begins at an arbitrary LSN, which is precisely the silent-loss case
        // (delete the lowest segment of a fresh log and it looks identical). The explicit
        // logStartLsn makes the two indistinguishable-under-v2 cases distinguishable, and this one
        // lands on the refusing side.
        File b = newFile("floor-k3");
        StoreWAL sd = oneSectionPerSegment(b);
        try {
            sd.put(Fixtures.payload(1, 1, 8), Fixtures.RAW);
            sd.commit();
            sd.checkpoint();
        } finally {
            sd.close();
        }
        File[] segs = WalTestKit.segments(b);
        assertEquals(2, segs.length);
        truncateTo(b, seqOf(segs[1]), WalTestKit.SEG_HDR);   // empty the 'K' segment
        expectCorruption(b, "an unmarked log must begin at LSN 1");
    }

    // ================= the chain anchor =================
    /**
     * The same loss where the FLOOR cannot see it: the retained set does begin with a {@code 'C'}
     * image, so only the LSN chain can catch that segment 2 is gone. This is why the anchor is keyed
     * on the mark's own segment rather than on the retained set's immediate predecessor — keyed on
     * the latter, position 2 is absent, there is no anchor at all, the {@code 'C'} arm satisfies the
     * floor, and the store opens with segment 2's acknowledged commits silently missing. A mark that
     * under-collects like this is legal (K3), so the image is reachable rather than doctored.
     */
    @Test public void a_segment_lost_above_an_under_collecting_mark_is_corruption() throws IOException {
        File f = newFile("anchor-undercollect");
        byte[] v1 = Fixtures.payload(1, 1, 8);
        writeSegment(f, 1, put(1, 1, v1));
        // segment 2 carried LSN 2 and is gone; the mark only attests 1, so 2 is still retained
        writeSegment(f, 3, image(1, 3, v1), mark(4, 1, 2));
        expectCorruption(f, "the clean mark attests it begins at");
        assertTrue("the witness must survive the refusal", WalTestKit.segment(f, 1).exists());
    }



    /**
     * <b>F1.</b> Damage confined to a segment BELOW the mark must not refuse the open, even when a
     * clean lower neighbour survives beneath it. Q5 R4: those segments are superseded and about to
     * be deleted, so "throwing on it would brick a store over bytes nobody will read".
     *
     * <p>The revision this pins searched downward for the highest usable anchor and so walked
     * straight past the damaged segment onto segment 1, then demanded the retained set continue
     * from segment 1's stale {@code lastLsn} and refused the store <b>permanently</b>. All three
     * flavours of below-mark damage failed identically.
     */
    @Test public void damage_below_the_mark_never_refuses_the_open() throws IOException {
        byte[] v1 = Fixtures.payload(1, 1, 8);
        byte[] v2 = Fixtures.payload(2, 1, 8);
        for (int flavour = 0; flavour < 3; flavour++) {
            File f = newFile("f1-" + flavour);
            // an ordinary crash-mid-unlink image: the mark in 4 retires 1 and 2, the image is in 3
            writeSegment(f, 1, put(1, 1, v1));
            writeSegment(f, 2, put(2, 2, v2));
            writeSegment(f, 3, image(1, 3, v1), image(2, 4, v2));
            writeSegment(f, 4, mark(5, 2, 3));
            byte[] seg2 = WalTestKit.read(WalTestKit.segment(f, 2));
            byte[] damaged = switch (flavour) {
                case 0 -> rotBody(seg2);                                   // held: body CRC
                case 1 -> java.util.Arrays.copyOf(seg2, seg2.length - 3);  // held: torn non-final
                default -> java.util.Arrays.copyOf(seg2, WalTestKit.SEG_HDR); // valid-empty
            };
            WalTestKit.write(WalTestKit.segment(f, 2), damaged);
            StoreWAL s = new StoreWAL(f);
            try {
                assertArrayEquals("flavour " + flavour, v1, s.get(1, Fixtures.RAW));
                assertArrayEquals("flavour " + flavour, v2, s.get(2, Fixtures.RAW));
                s.verify();
            } finally {
                s.close();
            }
        }
    }

    /**
     * <b>F2.</b> The floor runs even when a chain anchor exists. A {@code 'K'} with no {@code 'C'}
     * image behind it, whose superseded segments are still present, satisfies the LSN chain — their
     * data lies below the mark, so no LSN is missing — while violating the mark-implies-image
     * contract. Skipping the floor there opened the store, replayed only the retained set, and let
     * R5 <b>unlink the only copy of both records</b>.
     */
    @Test public void a_mark_with_no_image_behind_it_is_refused_even_with_an_anchor()
            throws IOException {
        File f = newFile("f2");
        writeSegment(f, 1, put(1, 1, Fixtures.payload(1, 1, 8)));
        writeSegment(f, 2, put(2, 2, Fixtures.payload(2, 1, 8)));
        // segment 3 holds a plain 'S' and then a mark retiring 1 and 2 — nothing images them
        writeSegment(f, 3, put(3, 3, Fixtures.payload(3, 1, 8)), mark(4, 2, 2));
        expectCorruption(f, "the clean mark attests it begins at");
        // and the refusal must not have destroyed the data it was protecting
        assertTrue("segment 1 must survive the refusal", WalTestKit.segment(f, 1).exists());
        assertTrue("segment 2 must survive the refusal", WalTestKit.segment(f, 2).exists());
    }

    /** Flips a byte inside the first section's body, so the section fails its body CRC. */
    private static byte[] rotBody(byte[] seg) {
        byte[] out = seg.clone();
        out[WalTestKit.SEG_HDR + WalTestKit.SEC_HDR] ^= 0x5A;
        return out;
    }

    /**
     * Recovery must not hold one file descriptor per segment. Nothing reads a
     * segment after recovery — the record map lives in the memory-backed inner store, and only the
     * active segment is appended to — while the log is allowed to reach roughly twice the live data
     * size. At 64 MiB per segment a large store therefore meant thousands of open descriptors
     * against a default {@code ulimit -n} of 1024: a legitimate store failing to open with
     * {@code EMFILE}, and an attacker-supplied directory of valid 28-byte segments able to force it.
     */
    @Test public void recovery_does_not_hold_a_descriptor_per_segment() {
        File f = newFile("fd-bound");
        StoreWAL s = new StoreWAL(f);
        try {
            s.setSegmentBytes(WalTestKit.SEG_HDR + WalTestKit.SEC_HDR);   // one section per segment
            s.setMinLogBytes(0);
            for (int i = 0; i < 40; i++) {
                s.put(Fixtures.payload(i, 1, 8), Fixtures.RAW);
                s.commit();
            }
        } finally {
            s.close();
        }
        assertTrue("expected many segments", WalTestKit.segments(f).length >= 30);
        StoreWAL s2 = new StoreWAL(f);
        try {
            s2.verify();
            assertEquals("only the active segment may hold a channel after recovery",
                    1, s2.openSegmentChannelsForTest());
        } finally {
            s2.close();
        }
        // and a read-only open, which never writes, may hold none at all
        StoreWAL ro = StoreWAL.openReadOnly(f);
        try {
            assertTrue("a read-only open holds at most one channel",
                    ro.openSegmentChannelsForTest() <= 1);
        } finally {
            ro.close();
        }
    }

    // ================= a hostile namespace =================
    //
    // Sequence numbers are attacker-visible 16-hex file names, and nothing in this suite used to
    // feed recovery a doctored one. That is how an O(numeric gap) walk survived unnoticed for so
    // long: it was correct on every image a writer produces, and every image the tests built was one.

    /**
     * A wildly high sequence number must cost recovery nothing beyond the file
     * count. The revision this pins walked the numeric gap one sequence number at a time, so a
     * name at 2³³ held the open ~3 s and a name at 2⁴⁰ would hold it for minutes — under the store
     * lock, with no verdict and no exception, which is a denial of service rather than a refusal.
     *
     * <p>The timeout is deliberately enormous relative to the real cost (milliseconds): it is here
     * to catch a reintroduced numeric walk, not to measure anything.
     */
    @Test(timeout = 20_000) public void a_wildly_high_sequence_number_does_not_stall_recovery()
            throws IOException {
        File f = newFile("hostile-seq");
        byte[] v1 = Fixtures.payload(1, 1, 8);
        writeSegment(f, 1, put(1, 1, v1));
        // a valid segment, at a name ~10^12 above its neighbour, carrying the mark that retires it
        long far = 1L << 40;
        writeSegment(f, far, image(1, 2, v1), mark(3, 1, 2));
        StoreWAL s = new StoreWAL(f);
        try {
            assertArrayEquals(v1, s.get(1, Fixtures.RAW));
            s.verify();
        } finally {
            s.close();
        }
    }

    /**
     * A name at {@code Long.MAX_VALUE} leaves no room for {@code nextSeq}, and W6 forbids reusing
     * any number. Refused explicitly rather than wrapping to a negative sequence — which would
     * alias a future create onto an existing name, the one thing W6 exists to prevent.
     */
    @Test public void a_sequence_number_at_the_maximum_is_refused_rather_than_wrapping()
            throws IOException {
        File f = newFile("hostile-max");
        writeSegment(f, 1, put(1, 1, Fixtures.payload(1, 1, 8)));
        writeSegment(f, Long.MAX_VALUE, put(2, 2, Fixtures.payload(2, 1, 8)));
        try {
            new StoreWAL(f).close();
            fail("expected a refusal for a sequence number with no successor");
        } catch (DBException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("overflow"));
        }
    }

    /**
     * A mark attesting a {@code cleanedThroughSeq} far above every other segment retires them all
     * from the retained set. It is K4-valid (the mark's own segment outranks what it retires), so
     * the refusal has to come from the floor: nothing images the data being retired.
     */
    @Test public void a_mark_retiring_everything_below_a_hostile_gap_is_refused() throws IOException {
        File f = newFile("hostile-mark");
        writeSegment(f, 1, put(1, 1, Fixtures.payload(1, 1, 8)));
        writeSegment(f, 1L << 40, put(2, 2, Fixtures.payload(2, 1, 8)), mark(3, 1L << 39, 3));
        expectCorruption(f, "the clean mark attests it begins at");
        assertTrue("the refusal must not have deleted the evidence",
                WalTestKit.segment(f, 1).exists());
    }

    // ---------------------------------------------------------------- helpers

    /** One section per segment: the smallest legal size, so every commit rolls over. */
    private static StoreWAL oneSectionPerSegment(File f) {
        StoreWAL s = new StoreWAL(f);
        s.setMinLogBytes(0);
        s.setSegmentBytes(WalSegmentSet.SEG_HDR + StoreWAL.SEC_HDR);
        return s;
    }

    private static void truncateTo(File base, long seq, int len) throws IOException {
        File seg = WalTestKit.segment(base, seq);
        WalTestKit.write(seg, java.util.Arrays.copyOf(WalTestKit.read(seg), len));
    }

    private static void expectCorruption(File base, String messageFragment) {
        try {
            new StoreWAL(base).close();
            fail("expected DataCorruption containing \"" + messageFragment + "\"");
        } catch (DBException.DataCorruption expected) {
            assertTrue(expected.getMessage(),
                    expected.getMessage().contains(messageFragment));
        }
    }

    private static long[] fileSizes(File base) {
        File[] segs = WalTestKit.segments(base);
        long[] out = new long[segs.length * 2];
        for (int i = 0; i < segs.length; i++) {
            out[2 * i] = seqOf(segs[i]);
            out[2 * i + 1] = segs[i].length();
        }
        return out;
    }

    private static long seqOf(File segment) {
        String name = segment.getName();
        return Long.parseUnsignedLong(name.substring(name.length() - 16), 16);
    }

    /** Commits {@code n} small transactions over a growing record set; returns the final state. */
    private static TreeMap<Long, byte[]> churn(StoreWAL s, long seed, int n) {
        Random rnd = new Random(seed);
        List<Long> live = new ArrayList<>();
        for (int c = 0; c < n; c++) {
            if (live.isEmpty() || rnd.nextInt(3) == 0)
                live.add(s.put(Fixtures.payload(live.size(), c, 60), Fixtures.RAW));
            else
                s.update(live.get(rnd.nextInt(live.size())), Fixtures.payload(c, c, 60), Fixtures.RAW);
            s.commit();
        }
        return snapshot(s);
    }
}
