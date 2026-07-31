package org.mapdb.store;

import org.junit.After;
import org.junit.Test;
import org.mapdb.DBException;
import org.mapdb.TmpFiles;
import org.mapdb.io.DataOutput2;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Two recovery behaviours that the code decides and the comments do not: the {@code lsn == 0}
 * sentinel edge, and the reduction that picks {@code markLogStartLsn} out of several {@code 'K'}
 * marks. Both are <b>frozen</b> — this suite exists to pin them as they are, not to argue they
 * are the nicest reading, because rust-store and zig-store are being ported against this
 * implementation and a port that "fixes" either one diverges on images this one accepts.
 *
 * <h2>The lsn == 0 sentinel</h2>
 *
 * <p>{@code Segment.firstLsn}/{@code lastLsn} start at 0 and 0 doubles as "no section seen yet"
 * ({@link StoreWAL#scanSegment}). A conforming writer never mints LSN 0 — {@code nextLsn} starts
 * at 1 — so the ambiguity is unreachable from the writer. It is reachable from a crafted or
 * doctored image, and there a CRC-valid section holding LSN 0 is <b>accepted</b>. Precisely:
 *
 * <ul>
 * <li>S2 and S9 both sit under {@code if (seg.lastLsn != 0)}, so a whole LEADING RUN of LSN-0
 *     sections is admitted, not merely one;</li>
 * <li>such a section never claims {@code firstLsn}. The next nonzero section claims it, and R4's
 *     self, floor and chain equalities then apply to THAT section normally;</li>
 * <li>a segment holding nothing but LSN-0 sections is "empty" to R4's chain rule, which
 *     therefore reads its successor's start off its <em>stated</em> {@code firstLsn};</li>
 * <li>an accepted LSN 0 does set the scan-local lookahead anchor to 0. Only the CROSS-SEGMENT
 *     carry is preserved, and only because {@link StoreWAL#pass1} advances it just for a segment
 *     that ended with {@code lastLsn != 0}.</li>
 * </ul>
 *
 * <p>Not pinned here, because it is not observable: whether an implementation "counts" the LSN-0
 * section in the retained maximum. A 0 contributes nothing to a maximum initialised at 0, and the
 * one branch that could tell them apart — R7's {@code headerFirstLsn(retained[0]) - 1} fallback —
 * is reachable only in an unmarked log, where R4's floor has already forced that header to state
 * 1. The two readings agree by arithmetic on every image that survives R4.
 *
 * <h2>The mark reduction</h2>
 *
 * <p>{@code cleanedThroughSeq} is the maximum over every valid mark in the whole segment set, but
 * {@code markLogStartLsn} is assigned inside {@link StoreWAL#scanSegment} against that
 * <b>segment's own</b> running maximum, which restarts at every segment, and only on a
 * <b>strict</b> increase. So the value R4's floor check uses is the logStartLsn of the first mark
 * attaining the strict per-segment maximum, taken from the last segment that held any valid mark
 * — neither "the newest mark's" (the comment at the assignment) nor "the globally greatest
 * mark's". The tests below fix all four readings apart from each other.
 *
 * <p>A mark in a segment that ends up SUPERSEDED can never decide the final value: any later
 * accepted mark overwrites the field and strictly dominates the removal boundary, and the last
 * segment holding an accepted mark is always retained (K4). There is nothing there to pin.
 *
 * <p>Written for the WAL v3 port workstream (todo/store-wal3, slice J0). Test-only: nothing in
 * {@code src/main} changes, and both behaviours are hereby the reference the ports must match.
 */
public class StoreWALFrozenEdgeTest {

    private final List<File> files = new ArrayList<>();

    private File newFile(String tag) {
        try {
            File f = TmpFiles.tempFile("mapdb-wal-frozen-" + tag, ".wal");
            f.delete();
            files.add(f);
            return f;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @After public void cleanup() {
        for (File f : files) TmpFiles.delete(f);
        files.clear();
    }

    // ---------------------------------------------------------------- image building
    // Same recipe as StoreWALSegmentSetTest: one section per Sec, checksummed for the offset it
    // lands at, the header stating the LSN of the first section.

    private static byte[] recordEntry(long recid, byte[] content) {
        DataOutput2 o = new DataOutput2(64);
        o.writeByte(2);
        o.packLong(recid);
        o.packLong((4L + content.length + 15) & ~15L);
        o.packLong(content.length + 1);
        o.write(content);
        return Arrays.copyOf(o.buf, o.pos);
    }

    private record Sec(char tag, long lsn, byte[] body) { }

    private static Sec put(long recid, long lsn, byte[] content) {
        return new Sec('S', lsn, recordEntry(recid, content));
    }

    private static Sec image(long recid, long lsn, byte[] content) {
        return new Sec('C', lsn, recordEntry(recid, content));
    }

    private static Sec mark(long lsn, long through, long logStartLsn) {
        byte[] body = new byte[16];
        WalSegmentSet.putBe64(body, 0, through);
        WalSegmentSet.putBe64(body, 8, logStartLsn);
        return new Sec('K', lsn, body);
    }

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

    private void writeSegment(File base, long seq, long firstLsn, Sec... sections) throws IOException {
        WalTestKit.write(WalTestKit.segment(base, seq), segmentImage(seq, firstLsn, sections));
    }

    private static void expectCorruption(File base, String messageFragment) {
        try {
            new StoreWAL(base).close();
            fail("expected DataCorruption containing \"" + messageFragment + "\"");
        } catch (DBException.DataCorruption expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains(messageFragment));
        }
    }

    /** {seq, length} of every segment — the oracle for "the refusal mutated nothing". */
    private static long[] fileSet(File base) {
        File[] segs = WalTestKit.segments(base);
        long[] out = new long[segs.length * 2];
        for (int i = 0; i < segs.length; i++) {
            String name = segs[i].getName();
            out[2 * i] = Long.parseUnsignedLong(name.substring(name.length() - 16), 16);
            out[2 * i + 1] = segs[i].length();
        }
        return out;
    }

    private static long[] seqs(File base) {
        File[] segs = WalTestKit.segments(base);
        long[] out = new long[segs.length];
        for (int i = 0; i < segs.length; i++) {
            String name = segs[i].getName();
            out[i] = Long.parseUnsignedLong(name.substring(name.length() - 16), 16);
        }
        return out;
    }

    /** LSN of the last whole section of a segment — where the writer continued after recovery. */
    private static long lastSectionLsn(File base, long seq) throws IOException {
        long[][] rows = WalTestKit.sectionHeaders(WalTestKit.segment(base, seq));
        assertTrue("no section in segment " + seq, rows.length > 0);
        return rows[rows.length - 1][1];
    }

    // ================= the lsn == 0 sentinel =================

    /**
     * A CRC-valid section holding LSN 0 at the head of a segment is accepted and replayed. It
     * does not claim {@code firstLsn}: the LSN-1 section that follows does, and R4 then compares
     * THAT against the header's stated 1 as it would for any segment. What the leading zero
     * switches off is only pass 1's density pair (S2/S9), which is gated on
     * {@code lastLsn != 0}.
     */
    @Test public void an_lsn_zero_section_at_the_head_of_a_segment_is_accepted_and_replayed()
            throws IOException {
        File f = newFile("lsn0-head");
        byte[] v0 = Fixtures.payload(7, 1, 8);
        byte[] v1 = Fixtures.payload(8, 1, 8);
        writeSegment(f, 1, 1, put(1, 0, v0), put(2, 1, v1));
        StoreWAL s = new StoreWAL(f);
        try {
            assertArrayEquals("the crafted LSN-0 section is replayed like any other",
                    v0, s.get(1, Fixtures.RAW));
            assertArrayEquals(v1, s.get(2, Fixtures.RAW));
            s.verify();
            // R7 counted from the LSN-1 section: the next commit takes LSN 2.
            s.put(Fixtures.payload(9, 1, 8), Fixtures.RAW);
            s.commit();
            assertEquals(2, lastSectionLsn(f, 1));
        } finally {
            s.close();
        }
    }

    /**
     * The density checks are off at the head, but the header contract is not: the first nonzero
     * section becomes {@code firstLsn}, so R4's self check still demands it equal what the header
     * states. A leading zero does not license an arbitrary follower.
     */
    @Test public void the_section_after_a_leading_zero_still_answers_to_the_header()
            throws IOException {
        File f = newFile("lsn0-self");
        writeSegment(f, 1, 1, put(1, 0, Fixtures.payload(7, 8, 8)), put(2, 7, Fixtures.payload(8, 8, 8)));
        expectCorruption(f, "its first section is 7");
    }

    /**
     * Both density checks sit under the same {@code lastLsn != 0} guard, so the acceptance is not
     * limited to one section: an entire leading RUN of LSN-0 sections is admitted, each replayed.
     * A port that sets a "have seen a section" flag instead of relying on the 0 sentinel starts
     * enforcing density after the first one and refuses this image.
     */
    @Test public void a_leading_run_of_lsn_zero_sections_is_accepted() throws IOException {
        File f = newFile("lsn0-run");
        byte[] a = Fixtures.payload(7, 9, 8);
        byte[] b = Fixtures.payload(8, 9, 8);
        byte[] c = Fixtures.payload(9, 9, 8);
        writeSegment(f, 1, 1, put(1, 0, a), put(2, 0, b), put(3, 1, c));
        StoreWAL s = new StoreWAL(f);
        try {
            assertArrayEquals(a, s.get(1, Fixtures.RAW));
            assertArrayEquals(b, s.get(2, Fixtures.RAW));
            assertArrayEquals(c, s.get(3, Fixtures.RAW));
            s.verify();
        } finally {
            s.close();
        }
    }

    /**
     * The same section as the ONLY one in the store: accepted, applied, and the log resumes at
     * LSN 1 — R7 has no nonzero LSN over the retained set and falls back to the lowest segment's
     * stated {@code firstLsn - 1}. (An implementation that instead counted the LSN-0 section and
     * resumed at {@code 0 + 1} lands on the same number; the numbers cannot separate those two
     * readings, here or anywhere — see the class javadoc. What this pins is acceptance, replay,
     * the resumption point, and that the resulting image reopens.)
     */
    @Test public void a_lone_lsn_zero_section_is_accepted_and_the_log_resumes_at_1() throws IOException {
        File f = newFile("lsn0-lone");
        byte[] v0 = Fixtures.payload(7, 2, 8);
        byte[] v1 = Fixtures.payload(9, 2, 8);
        writeSegment(f, 1, 1, put(1, 0, v0));
        long recid;
        StoreWAL s = new StoreWAL(f);
        try {
            assertArrayEquals(v0, s.get(1, Fixtures.RAW));
            s.verify();
            recid = s.put(v1, Fixtures.RAW);
            s.commit();
            assertEquals(1, lastSectionLsn(f, 1));
        } finally {
            s.close();
        }
        // The resulting image — an LSN-0 section followed by LSN 1 — must reopen: the sentinel is
        // stable across reopens, not a one-shot acceptance.
        StoreWAL again = new StoreWAL(f);
        try {
            assertArrayEquals(v0, again.get(1, Fixtures.RAW));
            assertArrayEquals(v1, again.get(recid, Fixtures.RAW));
            again.verify();
        } finally {
            again.close();
        }
    }

    /**
     * The edge is confined to the head of a segment. Once any real section precedes it,
     * {@code lastLsn != 0} and S2 sees LSN 0 as "does not follow" — corruption, held and thrown
     * because the segment is retained.
     */
    @Test public void an_lsn_zero_section_after_a_real_one_is_corruption() throws IOException {
        File f = newFile("lsn0-tail");
        writeSegment(f, 1, 1, put(1, 1, Fixtures.payload(7, 3, 8)), put(2, 0, Fixtures.payload(8, 3, 8)));
        long[] before = fileSet(f);
        expectCorruption(f, "does not follow");
        assertArrayEquals("a refusal at R4 mutates nothing", before, fileSet(f));
    }

    /**
     * R4's chain rule reads a segment that holds no nonzero LSN as EMPTY, whatever it actually
     * contains: its successor's stated start is compared with {@code headerFirstLsn(prev)}, not
     * with {@code prev.lastLsn + 1}. And the middle segment's own self check is skipped, because
     * that check is gated on {@code firstLsn != 0} rather than on the segment being empty — a
     * port transcribing "s nonempty AND s.firstLsn != stated" refuses this image.
     */
    @Test public void an_lsn_zero_only_segment_chains_by_its_stated_start() throws IOException {
        File f = newFile("lsn0-chain");
        byte[] a = Fixtures.payload(1, 10, 8);
        byte[] b = Fixtures.payload(2, 10, 8);
        byte[] c = Fixtures.payload(3, 10, 8);
        writeSegment(f, 1, 1, put(1, 1, a));
        writeSegment(f, 2, 2, put(2, 0, b));
        writeSegment(f, 3, 2, put(3, 2, c));
        StoreWAL s = new StoreWAL(f);
        try {
            assertArrayEquals(a, s.get(1, Fixtures.RAW));
            assertArrayEquals(b, s.get(2, Fixtures.RAW));
            assertArrayEquals(c, s.get(3, Fixtures.RAW));
            s.verify();
        } finally {
            s.close();
        }
    }

    /**
     * The mirror: a successor stating {@code prev.lastLsn + 1} over the zero-only segment — which
     * is what an implementation counting LSN 0 as a real last LSN would demand — is refused,
     * because the chain wants the predecessor's STATED start.
     */
    @Test public void an_lsn_zero_only_segment_does_not_advance_the_chain() throws IOException {
        File f = newFile("lsn0-chain-bad");
        writeSegment(f, 1, 1, put(1, 1, Fixtures.payload(1, 11, 8)));
        writeSegment(f, 2, 2, put(2, 0, Fixtures.payload(2, 11, 8)));
        writeSegment(f, 3, 1, put(3, 1, Fixtures.payload(3, 11, 8)));
        expectCorruption(f, "accounts for LSNs up to 1");
    }

    /**
     * The cross-segment half of the sentinel. The suspect-header lookahead is anchored on the
     * previous segment's last accepted LSN, and {@link StoreWAL#pass1} carries that anchor
     * forward only when the segment it just scanned ended with {@code lastLsn != 0}. So a
     * segment holding nothing but an LSN-0 section does NOT reset the anchor to 0: the damaged
     * header in the segment above is still judged against LSN 1, its follower carries exactly
     * {@code 1 + 2}, and the tear is classified as mid-log corruption rather than a torn tail.
     *
     * <p>Had the anchor been erased, the same image would open — the follower's LSN 3 does not
     * equal the {@code 0 + 2} an erased anchor demands — truncating a CRC-valid section away.
     */
    @Test public void an_lsn_zero_only_segment_does_not_erase_the_cross_segment_anchor()
            throws IOException {
        File f = newFile("lsn0-anchor");
        writeSegment(f, 1, 1, put(1, 1, Fixtures.payload(7, 4, 8)));
        writeSegment(f, 2, 2, put(2, 0, Fixtures.payload(8, 4, 8)));
        // The active segment: a section whose header CRC no longer matches (its bodyLen still
        // frames the next one), followed by a wholly valid section carrying anchor + 2.
        byte[] seg3 = segmentImage(3, 2, put(3, 2, Fixtures.payload(9, 4, 8)),
                put(4, 3, Fixtures.payload(10, 4, 8)));
        seg3[WalTestKit.SEG_HDR + 17] ^= 0x40;                  // hdrCrc byte: S3 without resealing
        WalTestKit.write(WalTestKit.segment(f, 3), seg3);
        long[] before = fileSet(f);
        expectCorruption(f, "mid-log corruption: section header damaged");
        assertArrayEquals("a refusal at R4 mutates nothing", before, fileSet(f));
    }

    /**
     * The other half of the anchor rule, and the reason "invisible" is the wrong word for it:
     * WITHIN a segment an accepted LSN 0 does move the lookahead anchor — to 0. The damaged
     * header below it is therefore judged against {@code 0 + 2}, which the following section
     * carries, so the image is mid-log corruption. A port that filtered the zero out of its
     * local anchor would keep 1, demand LSN 3, find 2, and truncate the tail away instead.
     */
    @Test public void an_accepted_lsn_zero_does_move_the_scan_local_anchor() throws IOException {
        File f = newFile("lsn0-local-anchor");
        writeSegment(f, 1, 1, put(1, 1, Fixtures.payload(7, 12, 8)));
        byte[] seg2 = segmentImage(2, 2, put(2, 0, Fixtures.payload(8, 12, 8)),
                put(3, 9, Fixtures.payload(9, 12, 8)),
                put(4, 2, Fixtures.payload(10, 12, 8)));
        // Damage the MIDDLE section's header CRC; the LSN-0 section before it has already been
        // accepted, so the anchor the lookahead uses is 0 and the section after it carries 2.
        int second = WalTestKit.sectionOffset(seg2, 1);
        seg2[second + 17] ^= 0x40;
        WalTestKit.write(WalTestKit.segment(f, 2), seg2);
        expectCorruption(f, "mid-log corruption: section header damaged");
    }

    // ================= the mark reduction =================

    /**
     * K2/R4 floor. Two marks in two segments: the greater {@code cleanedThroughSeq} is in the
     * LOWER segment, and the segment above holds a mark authorizing less. {@code cleanedThroughSeq}
     * is the global maximum (2 — segments 1 and 2 are unlinked), while {@code markLogStartLsn} is
     * the LAST scanned segment's mark value (3, from the mark that only authorized through 1). The
     * floor therefore expects the retained log to begin at LSN 3, which is what segment 3's header
     * states, and the open succeeds.
     *
     * <p>Under "the globally greatest mark decides" the floor would expect LSN 2 and refuse this
     * image; under "the newest mark decides" it would agree here and disagree in
     * {@link #within_one_segment_the_greatest_mark_wins_not_the_newest}.
     */
    @Test public void the_log_start_comes_from_the_last_segment_holding_a_mark() throws IOException {
        File f = newFile("mark-last-seg");
        byte[] v1 = Fixtures.payload(1, 5, 8);
        byte[] v2 = Fixtures.payload(2, 5, 8);
        writeSegment(f, 1, 1, put(1, 1, v1));
        writeSegment(f, 2, 2, put(1, 2, v2));
        writeSegment(f, 3, 3, image(1, 3, v2), mark(4, 2, 2));   // greater through, logStart 2
        writeSegment(f, 4, 5, mark(5, 1, 3));                    // lesser through, logStart 3
        StoreWAL s = new StoreWAL(f);
        try {
            assertArrayEquals(v2, s.get(1, Fixtures.RAW));
            assertArrayEquals("cleanedThroughSeq is the GLOBAL maximum: 1 and 2 are unlinked",
                    new long[]{3, 4}, seqs(f));
            s.verify();
        } finally {
            s.close();
        }
    }

    /**
     * The same image through a read-only open, which runs R3/R4/R6 unchanged and suppresses only
     * the mutations: the identical reduction and floor, no unlink at all. A port whose read-only
     * path is a separate scanner diverges here.
     */
    @Test public void a_read_only_open_takes_the_same_reduction_and_unlinks_nothing()
            throws IOException {
        File f = newFile("mark-ro");
        byte[] v1 = Fixtures.payload(1, 13, 8);
        byte[] v2 = Fixtures.payload(2, 13, 8);
        writeSegment(f, 1, 1, put(1, 1, v1));
        writeSegment(f, 2, 2, put(1, 2, v2));
        writeSegment(f, 3, 3, image(1, 3, v2), mark(4, 2, 2));
        writeSegment(f, 4, 5, mark(5, 1, 3));
        long[] before = fileSet(f);
        StoreWAL ro = StoreWAL.openReadOnly(f);
        try {
            assertArrayEquals(v2, ro.get(1, Fixtures.RAW));
            assertArrayEquals("R5 is suppressed: the superseded segments stay on disk",
                    before, fileSet(f));
        } finally {
            ro.close();
        }
    }

    /**
     * The mirror of {@link #the_log_start_comes_from_the_last_segment_holding_a_mark}, and the one
     * that rules out "the globally greatest mark decides": the two logStartLsn values are swapped,
     * so the last segment's mark now attests a start of 2 while segment 3 states 3. The open is
     * refused — the value that lost the global {@code through} comparison is nonetheless the one
     * the floor uses.
     */
    @Test public void the_greatest_marks_log_start_does_not_win() throws IOException {
        File f = newFile("mark-greatest-loses");
        byte[] v1 = Fixtures.payload(1, 6, 8);
        byte[] v2 = Fixtures.payload(2, 6, 8);
        writeSegment(f, 1, 1, put(1, 1, v1));
        writeSegment(f, 2, 2, put(1, 2, v2));
        writeSegment(f, 3, 3, image(1, 3, v2), mark(4, 2, 3));   // greater through, logStart 3
        writeSegment(f, 4, 5, mark(5, 1, 2));                    // lesser through, logStart 2
        expectCorruption(f, "the clean mark attests it begins at 2");
        assertArrayEquals("R4 refuses before R5: nothing was unlinked",
                new long[]{1, 2, 3, 4}, seqs(f));
    }

    /**
     * Within one segment the running maximum is over {@code cleanedThroughSeq}, so a later mark
     * authorizing LESS does not move {@code markLogStartLsn} — "always the NEWEST mark's" holds
     * only among marks that raise the maximum. The floor expects LSN 3, from the first mark.
     */
    @Test public void within_one_segment_the_greatest_mark_wins_not_the_newest() throws IOException {
        File f = newFile("mark-in-seg");
        byte[] v1 = Fixtures.payload(1, 7, 8);
        byte[] v2 = Fixtures.payload(2, 7, 8);
        writeSegment(f, 1, 1, put(1, 1, v1));
        writeSegment(f, 2, 2, put(1, 2, v2));
        writeSegment(f, 3, 3, image(1, 3, v2), mark(4, 2, 3), mark(5, 1, 5));
        StoreWAL s = new StoreWAL(f);
        try {
            assertArrayEquals(v2, s.get(1, Fixtures.RAW));
            assertArrayEquals(new long[]{3}, seqs(f));
            s.verify();
        } finally {
            s.close();
        }
    }

    /**
     * The direction the previous test cannot see: a later mark that RAISES the segment's maximum
     * does move {@code markLogStartLsn}. Without this, "keep the first mark of each segment"
     * agrees with the real reduction on every other image in this file — the greater mark happens
     * to come first in all of them.
     */
    @Test public void a_later_greater_mark_in_the_same_segment_moves_the_log_start()
            throws IOException {
        File f = newFile("mark-raise");
        byte[] v1 = Fixtures.payload(1, 14, 8);
        byte[] v2 = Fixtures.payload(2, 14, 8);
        writeSegment(f, 1, 1, put(1, 1, v1));
        writeSegment(f, 2, 2, put(1, 2, v2));
        writeSegment(f, 3, 3, image(1, 3, v2), mark(4, 1, 4), mark(5, 2, 3));
        StoreWAL s = new StoreWAL(f);
        try {
            assertArrayEquals(v2, s.get(1, Fixtures.RAW));
            assertArrayEquals("the second mark's through=2 decided the removal boundary too",
                    new long[]{3}, seqs(f));
            s.verify();
        } finally {
            s.close();
        }
    }

    /**
     * The same image with segment 3 stating the FIRST mark's logStartLsn. Both readings refuse
     * it, so the pin is the message: Java's floor names the second mark's 3, while a port keeping
     * the first mark's 4 would pass the floor and then refuse for an unrelated reason (its own
     * first section not matching the header).
     */
    @Test public void the_floor_names_the_later_greater_marks_log_start() throws IOException {
        File f = newFile("mark-raise-msg");
        byte[] v1 = Fixtures.payload(1, 15, 8);
        byte[] v2 = Fixtures.payload(2, 15, 8);
        writeSegment(f, 1, 1, put(1, 1, v1));
        writeSegment(f, 2, 2, put(1, 2, v2));
        writeSegment(f, 3, 4, image(1, 3, v2), mark(4, 1, 4), mark(5, 2, 3));
        expectCorruption(f, "the clean mark attests it begins at 3");
    }

    /**
     * The comparison is a STRICT increase, so an equal {@code cleanedThroughSeq} does not
     * displace the mark already holding the segment's maximum: the floor keeps the FIRST of the
     * two. A port written with {@code >=} passes every other mark test here and fails this one.
     */
    @Test public void an_equal_mark_does_not_displace_the_first_one() throws IOException {
        File f = newFile("mark-tie");
        byte[] v1 = Fixtures.payload(1, 16, 8);
        byte[] v2 = Fixtures.payload(2, 16, 8);
        writeSegment(f, 1, 1, put(1, 1, v1));
        writeSegment(f, 2, 2, put(1, 2, v2));
        writeSegment(f, 3, 3, image(1, 3, v2), mark(4, 2, 3), mark(5, 2, 5));
        StoreWAL s = new StoreWAL(f);
        try {
            assertArrayEquals(v2, s.get(1, Fixtures.RAW));
            assertArrayEquals(new long[]{3}, seqs(f));
            s.verify();
        } finally {
            s.close();
        }
    }
}
