package org.mapdb.store;

import org.junit.After;
import org.junit.Test;
import org.mapdb.TmpFiles;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * <b>Tier 2 of the Q5 conformance suite (§7): the writer's obligations, OBSERVED and INJECTED.</b>
 *
 * <p>W1, W2, W3, W4, W5 and P11 are all claims of the form "operation X does not happen before
 * operation Y completed". None of them leaves a trace in the resulting bytes — a log written in the
 * wrong order and one written in the right order are byte-identical, and differ only in which
 * crashes they survive. So until this class they were held by <b>structural argument alone</b>: the
 * calls appear in the right order in the source, and nothing checked that they still did. That left
 * them the top unproven surface, and the incremental cleaner made it more pressing still by giving
 * P11 a <em>second</em> writer.
 *
 * <p>The instrument is {@link StoreWAL.WalIo}, a seam every durability-relevant file operation
 * reports to. A run of a realistic history yields a trace, and {@link #checkWriterObligations} is
 * an oracle over that trace. The trace is not a summary the writer produces about itself: the
 * events are emitted at the call sites, immediately before the syscalls, so a reordering of those
 * calls reorders the trace by construction.
 *
 * <p><b>What this covers, and what it deliberately does not.</b>
 *
 * <ul>
 * <li><b>Covered — ordering:</b> the rules above, over a history that spans several segments, a
 *     partial cleaning retirement, a whole-log {@code checkpoint()}, a close and a reopen.</li>
 * <li><b>Covered — failure:</b> the seam also THROWS. Every operation of a canonical workload is
 *     failed in turn, and every run must reopen with at least the commits it acknowledged before
 *     the fault. That is W9's oracle rather than its trace shape: a partial section write must
 *     fail the store closed, and the next open must truncate and rotate past it (W7) rather than
 *     reuse the torn segment's checksum domain.</li>
 * <li><b>Not covered here — power loss.</b> This seam makes a syscall fail; it does not make
 *     already-written bytes vanish. Torn tails at every offset, non-prefix unlink subsets and
 *     create-crash residue are {@link StoreWALSegmentEventEnumeratorTest}'s subject and stay
 *     there.</li>
 * <li><b>Not covered here — recovery's own unlinks.</b> R2 (residue) and R5 (acting on a mark that
 *     was already durable when the store was opened) are authorized by bytes that predate the
 *     trace, so W5 cannot be judged from it. Every event is tagged with whether it happened inside
 *     an open, and W5 is asserted only over the writer's own unlinks. The recovery half is the
 *     tier-1 fixtures' oracle.</li>
 * </ul>
 *
 * <p><b>Non-vacuity, in two forms.</b> The writer is correct, so a passing oracle proves nothing on
 * its own — an oracle that asserted nothing would pass identically.
 *
 * <p>First, in the source: the {@code the_oracle_rejects_*} tests feed it traces from the same real
 * history with one event removed or retyped, and require it to reject each, naming the rule.
 *
 * <p>Second, and this is the one that matters, <b>each rule was verified by breaking the REAL
 * writer</b> and confirming this class fails — the house rule's "revert the fix and confirm the
 * test fails", with the ordering constraint standing in for the fix. All four edits are one line,
 * change no bytes the log contains, and break no other test in the suite except by way of this one:
 *
 * <ul>
 * <li><b>W1</b> — {@code appendSection} skips {@code force(false)} for ordinary {@code 'S'}
 *     sections, batching them under the next force. Caught: <em>"a section starts at 113 while
 *     bytes [36, 113) of this segment are unforced"</em>.</li>
 * <li><b>W2</b> — {@code createSegment} does the directory fsync before the header force, so the
 *     entry becomes durable before the identity it names. Caught at the first append into it.</li>
 * <li><b>W3</b> — {@code rollover} seals with {@code force(false)} instead of {@code force(true)}.
 *     Caught at the successor's create.</li>
 * <li><b>W5</b> — {@code finishCycleLocked} unlinks before appending the mark rather than after.
 *     Caught: <em>"a segment was unlinked with no forced mark to authorize it"</em>.</li>
 * </ul>
 *
 * <p>In addition {@link Obligations#checked} counts how many times each rule actually fired on the
 * real trace, and the history test asserts every count is positive — a rule that never applied
 * would otherwise read exactly like a rule that passed.
 */
public class StoreWALWriterFaultTest {

    /** One section per segment: every append rolls over, so a short history is a long log. */
    private static final long ONE_SECTION_PER_SEGMENT = WalSegmentSet.SEG_HDR + StoreWAL.SEC_HDR;

    private final List<File> files = new ArrayList<>();

    private File newFile() {
        try {
            File f = TmpFiles.tempFile("mapdb-wal-tier2", ".wal");
            f.delete();
            files.add(f);
            return f;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @After public void cleanup() {
        StoreWAL.testSetWalIo(null);
        for (File f : files) TmpFiles.delete(f);
        files.clear();
    }

    // ================= the recorder =================

    /**
     * One recorded operation.
     *
     * @param duringOpen set by the test around each constructor call, because recovery performs
     *                   some of the same operations under an authority — durable bytes written by
     *                   a previous run — that the trace cannot see.
     * @param markThrough for the {@code FORCE_DATA} of a {@code 'K'} section only: the
     *                    {@code targetSeq} read out of the mark's body <b>at the moment of the
     *                    force</b>. It cannot be read afterwards — the segment holding the mark is
     *                    itself retired by a later cycle, so the file is gone by the time the
     *                    oracle runs. Reading it here also keeps the oracle's authority right: the
     *                    number checked is the one on the device, not one the writer reported.
     */
    record Rec(StoreWAL.WalIoEvent e, boolean duringOpen, Long markThrough) {
        StoreWAL.WalOpKind kind() { return e.kind(); }
        long seq() { return e.seq(); }
        long off() { return e.off(); }
        long len() { return e.len(); }
        int tag() { return e.tag(); }
    }

    /** Records every event; never injects. Installed for the whole of a history. */
    static final class Trace implements StoreWAL.WalIo {
        final List<Rec> recs = new ArrayList<>();
        final File base;
        boolean duringOpen;
        private long pendingMarkOff = -1, pendingMarkSeq = -1;

        Trace(File base) { this.base = base; }

        @Override public void before(StoreWAL.WalIoEvent e) {
            Long through = null;
            if (e.kind() == StoreWAL.WalOpKind.SEC_HEADER && e.tag() == 'K') {
                pendingMarkOff = e.off();
                pendingMarkSeq = e.seq();
            } else if (e.kind() == StoreWAL.WalOpKind.FORCE_DATA && e.tag() == 'K'
                    && pendingMarkSeq == e.seq() && pendingMarkOff >= 0) {
                through = readMarkThrough(base, pendingMarkSeq, pendingMarkOff);
                pendingMarkOff = -1;
                pendingMarkSeq = -1;
            }
            recs.add(new Rec(e, duringOpen, through));
        }

        /** Runs {@code body} with {@code duringOpen} set, which is what brackets an open. */
        <T> T opening(java.util.function.Supplier<T> body) {
            duringOpen = true;
            try {
                return body.get();
            } finally {
                duringOpen = false;
            }
        }
    }

    // ================= the oracle =================

    /** Per-segment durability state, reconstructed from the trace as the writer's own would be. */
    private static final class Seg {
        /** Highest byte offset written; {@code -1} until this trace observes the segment. */
        long writtenTo = -1;
        /** Highest byte offset a force has made durable. */
        long forcedTo = -1;
        StoreWAL.WalOpKind lastForce;
        /** True when this trace saw the segment created, rather than inheriting it from disk. */
        boolean createdHere;
        /** W2: the segment's directory entry is durable, so sections in it may be acknowledged. */
        boolean dirDurable;
        /** A later segment was created, so this one is sealed and must never be appended to again. */
        boolean sealed;
        boolean unlinked;
    }

    /** The rules the oracle enforces, and how many times each one actually applied. */
    enum Rule { W1_FORCE_BEFORE_SUCCESSOR, W2_DIR_ENTRY_DURABLE, W3_SEALED_WITH_FULL_FORCE,
        W5_UNLINK_AFTER_FORCED_MARK, P11_MARK_ATTESTS_ONLY_FORCED_BYTES, FORCE_FLAVOR }

    static final class Obligations {
        final EnumMap<Rule, Integer> checked = new EnumMap<>(Rule.class);
        void hit(Rule r) { checked.merge(r, 1, Integer::sum); }
        int count(Rule r) { return checked.getOrDefault(r, 0); }
    }

    /**
     * Walks a trace and asserts every writer obligation it can see. Throws {@link AssertionError}
     * naming the rule and the event index on the first violation.
     *
     * <p>Segments this trace did not create are <b>inherited</b>: their bytes came off disk and are
     * durable by definition, so their state is seeded at the first event that mentions them. That
     * makes a handful of checks trivially true rather than wrong, and is why {@link Obligations}
     * counts hits — a rule whose count is zero was not checked, whatever the run reported.
     */
    static Obligations checkWriterObligations(Trace t, File base) {
        Obligations obs = new Obligations();
        Map<Long, Seg> segs = new LinkedHashMap<>();
        Long active = null;
        Long awaitingDirSync = null;
        long forcedMarkThrough = -1;     // targetSeq of the newest mark whose force completed

        for (int i = 0; i < t.recs.size(); i++) {
            Rec r = t.recs.get(i);
            String at = "event " + i + " " + r.e() + ": ";
            switch (r.kind()) {
                case CREATE -> {
                    if (active != null) {
                        Seg p = segs.get(active);
                        // W3 — rollover happens only after the sealed segment's last section is
                        // forced with a SIZE-persisting force. Its whole load is that a non-final
                        // segment ends exactly at a section boundary with zero trailing bytes,
                        // which is what lets §5.3 call any tear there corruption without a
                        // lookahead. A force(false) here would leave that to the platform.
                        if (p.lastForce != null) {
                            assertEquals(at + "W3: the segment a rollover seals must be forced with"
                                            + " force(true), not a data-only sync",
                                    StoreWAL.WalOpKind.FORCE_FULL, p.lastForce);
                            assertEquals(at + "W3: the sealed segment has unforced bytes past "
                                            + p.forcedTo,
                                    p.writtenTo, p.forcedTo);
                            obs.hit(Rule.W3_SEALED_WITH_FULL_FORCE);
                        }
                        p.sealed = true;
                    }
                    Seg s = new Seg();
                    s.createdHere = true;
                    s.writtenTo = 0;
                    s.forcedTo = 0;
                    segs.put(r.seq(), s);
                    active = r.seq();
                    awaitingDirSync = null;   // set once the header force completes
                }
                case SEG_HEADER -> {
                    Seg s = seg(segs, r);
                    assertTrue(at + "W2: the header must be written to a segment this run created",
                            s.createdHere);
                    s.writtenTo = r.off() + r.len();
                }
                case FORCE_FULL -> {
                    Seg s = seg(segs, r);
                    s.forcedTo = Math.max(s.forcedTo, r.off());
                    s.writtenTo = Math.max(s.writtenTo, r.off());
                    s.lastForce = StoreWAL.WalOpKind.FORCE_FULL;
                    // W2 — the segment's identity is durable; its directory entry is not until the
                    // next directory fsync, and nothing in it may be acknowledged before that.
                    if (s.createdHere && !s.dirDurable) awaitingDirSync = r.seq();
                }
                case DIRSYNC -> {
                    if (awaitingDirSync != null) {
                        segs.get(awaitingDirSync).dirDurable = true;
                        awaitingDirSync = null;
                        obs.hit(Rule.W2_DIR_ENTRY_DURABLE);
                    }
                }
                case SEC_HEADER -> {
                    Seg s = seg(segs, r);
                    assertTrue(at + "a section was appended to a SEALED segment; W3's"
                            + " zero-trailing-bytes guarantee does not survive that", !s.sealed);
                    assertTrue(at + "a section was appended to an UNLINKED segment", !s.unlinked);
                    if (s.createdHere) {
                        // W2 — without the directory fsync the whole segment can vanish on a crash,
                        // taking acknowledged commits with it.
                        assertTrue(at + "W2: a section was appended before the segment's directory"
                                + " entry was made durable", s.dirDurable);
                    }
                    // W1 (and W4, which is W1 scoped to the cleaner's 'C' sections — the same
                    // check, since both are "the predecessor's force completed"). §6.1's mid-log
                    // rot inference is sound only under this: batching sections under one fsync
                    // lets writeback reorder, so a crash could leave section k torn and k+1
                    // durable, and recovery would refuse an ordinary torn tail.
                    assertTrue(at + "W1: a section starts at " + r.off() + " while bytes ["
                                    + s.forcedTo + ", " + r.off() + ") of this segment are unforced",
                            s.forcedTo >= r.off());
                    obs.hit(Rule.W1_FORCE_BEFORE_SUCCESSOR);
                    if (r.tag() == 'K') {
                        // P11 — no 'K' before its attested bytes are forced. The mark authorizes
                        // deleting segments whose contents were re-emitted above; if any of those
                        // re-emitted bytes is still unforced anywhere in the log, a crash can leave
                        // a mark attesting bytes that vanished. Stated over EVERY segment, not just
                        // the one being written, because a cycle that rolled over mid-re-emission
                        // left its earlier images in a segment it no longer appends to.
                        for (Map.Entry<Long, Seg> en : segs.entrySet()) {
                            Seg o = en.getValue();
                            if (o.unlinked || o.writtenTo < 0) continue;
                            assertTrue(at + "P11: the mark attests segment " + en.getKey()
                                            + ", whose bytes [" + o.forcedTo + ", " + o.writtenTo
                                            + ") were never forced",
                                    o.forcedTo >= o.writtenTo);
                        }
                        obs.hit(Rule.P11_MARK_ATTESTS_ONLY_FORCED_BYTES);
                    }
                    s.writtenTo = Math.max(s.writtenTo, r.off() + r.len());
                }
                case SEC_BODY -> {
                    Seg s = seg(segs, r);
                    s.writtenTo = Math.max(s.writtenTo, r.off() + r.len());
                }
                case FORCE_DATA -> {
                    Seg s = seg(segs, r);
                    // The force-flavor rule, the other half of W2/W3: force(false) is a DATA sync
                    // and is legal exactly where the file's SIZE is not itself the payload. It is
                    // therefore the right flavor for an ordinary append and the wrong one for a
                    // create or a rollover seal — both of which this oracle requires to be
                    // FORCE_FULL, so an inverted pair fails on the other side.
                    assertEquals(at + "a data sync must cover exactly the bytes just written",
                            s.writtenTo, r.off());
                    obs.hit(Rule.FORCE_FLAVOR);
                    s.forcedTo = Math.max(s.forcedTo, r.off());
                    s.lastForce = StoreWAL.WalOpKind.FORCE_DATA;
                    // The mark is durable once this force completes, so it may now authorize an
                    // unlink. Its target came off the FILE at record time (see Rec#markThrough):
                    // the number that matters is the one recovery would read, not one the writer
                    // reported about itself.
                    if (r.markThrough() != null) forcedMarkThrough = r.markThrough();
                }
                case TRUNCATE -> {
                    Seg s = seg(segs, r);
                    s.writtenTo = r.off();
                    s.forcedTo = Math.min(s.forcedTo, r.off());
                }
                case UNLINK -> {
                    Seg s = seg(segs, r);
                    if (!r.duringOpen()) {
                        // W5 — unlink only after the 'K' that authorizes it is forced. A failed
                        // unlink is a leak the next open retries; it is never permission to advance
                        // an unproven mark. Scoped to the writer's own unlinks: recovery's are
                        // authorized by a mark that was durable before this trace began.
                        assertTrue(at + "W5: a segment was unlinked with no forced mark to"
                                + " authorize it", forcedMarkThrough >= 0);
                        assertTrue(at + "W5: segment " + r.seq() + " was unlinked, but the newest"
                                        + " forced mark only authorizes through " + forcedMarkThrough,
                                r.seq() <= forcedMarkThrough);
                        obs.hit(Rule.W5_UNLINK_AFTER_FORCED_MARK);
                    }
                    s.unlinked = true;
                }
            }
        }
        return obs;
    }

    /**
     * The state of a segment this trace has already seen, or a freshly seeded one for a segment it
     * INHERITED from disk. An inherited segment's existing bytes are durable — they were forced by
     * whatever run wrote them — so it is seeded as fully forced up to the offset of the event that
     * first mentions it.
     */
    private static Seg seg(Map<Long, Seg> segs, Rec r) {
        Seg s = segs.get(r.seq());
        if (s == null) {
            s = new Seg();
            s.writtenTo = r.off();
            s.forcedTo = r.off();
            s.lastForce = StoreWAL.WalOpKind.FORCE_FULL;
            s.dirDurable = true;
            segs.put(r.seq(), s);
        }
        return s;
    }

    /** Reads {@code targetSeq} out of the mark's durable body — 8 big-endian bytes at its start. */
    private static long readMarkThrough(File base, long seq, long markOff) {
        File f = WalTestKit.segment(base, seq);
        try (FileChannel ch = FileChannel.open(f.toPath(), StandardOpenOption.READ)) {
            ByteBuffer b = ByteBuffer.allocate(8);
            long p = markOff + StoreWAL.SEC_HDR;
            while (b.hasRemaining()) {
                int n = ch.read(b, p);
                if (n < 0) throw new IOException("mark body short read at " + p + " of " + f);
                p += n;
            }
            return b.getLong(0);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // ================= the history under observation =================

    /**
     * Segments large enough to hold SEVERAL sections. This matters more than it looks: at
     * {@link #ONE_SECTION_PER_SEGMENT} — the size every other cleaner test uses — no segment ever
     * holds a second section, so W1's "the predecessor's force completed" is satisfied by
     * {@code forcedTo == SEG_HDR == off} at every single append and the rule is never really
     * asked. The first draft of this class ran at the minimum size and its W1 mutation had no
     * victim to choose, which is how the gap was found.
     */
    private static final long SEVERAL_SECTIONS_PER_SEGMENT = 512;

    /**
     * A history chosen to reach every event kind: segments holding several sections (so W1 is
     * asked a real question) AND several rollovers (so W3 is), a PARTIAL cleaning retirement (so a
     * mark and an unlink appear while ordinary sections survive above them), a whole-log
     * {@code checkpoint()}, a close, a hand-torn tail, and a reopen whose recovery truncates and
     * rotates.
     */
    private Trace runHistory(File f) {
        Trace t = new Trace(f);
        StoreWAL.testSetWalIo(t);
        StoreWAL s = t.opening(() -> new StoreWAL(f));
        long b;
        try {
            s.setMinLogBytes(0);
            s.setSegmentBytes(SEVERAL_SECTIONS_PER_SEGMENT);
            long a = s.put(Fixtures.payload(1, 1, 40), Fixtures.RAW);
            s.commit();
            b = s.put(Fixtures.payload(2, 2, 40), Fixtures.RAW);
            s.commit();
            for (int i = 0; i < 12; i++) {
                s.update(b, Fixtures.payload(2, 9 + i, 40), Fixtures.RAW);
                s.commit();
                s.update(a, Fixtures.payload(1, 7 + i, 40), Fixtures.RAW);
                s.commit();
            }
            assertTrue("the history must span several segments",
                    WalTestKit.segments(f).length >= 3);
            assertTrue("a cleaning cycle must run, or W5 is never reached",
                    s.testCleanOldestSegment());
            s.update(b, Fixtures.payload(2, 11, 40), Fixtures.RAW);
            s.commit();
            s.checkpoint();
        } finally {
            s.close();
        }
        // A torn tail, so the reopen exercises R7's truncate-force-rotate (W7) — the only path that
        // emits TRUNCATE, and the one whose force flavor W3's argument also depends on.
        File[] all = WalTestKit.segments(f);
        File active = all[all.length - 1];
        WalTestKit.write(active, WalTestKit.concat(WalTestKit.read(active), new byte[7]));

        StoreWAL s2 = t.opening(() -> new StoreWAL(f));
        try {
            s2.setMinLogBytes(0);
            s2.setSegmentBytes(SEVERAL_SECTIONS_PER_SEGMENT);
            s2.put(Fixtures.payload(3, 3, 40), Fixtures.RAW);
            s2.commit();
            s2.update(b, Fixtures.payload(4, 4, 40), Fixtures.RAW);
            s2.commit();
        } finally {
            s2.close();
        }
        StoreWAL.testSetWalIo(null);
        return t;
    }

    /**
     * The headline: on a history that exercises all of it, the writer obeys W1, W2, W3, W4, W5 and
     * P11, and each of those rules actually APPLIED at least once. The second half is not
     * ceremony — an oracle whose rules never fire passes exactly as loudly as one whose rules hold.
     */
    @Test public void the_writer_obligations_hold_over_a_realistic_history() {
        File f = newFile();
        Trace t = runHistory(f);

        EnumSet<StoreWAL.WalOpKind> seen = EnumSet.noneOf(StoreWAL.WalOpKind.class);
        int marks = 0, nonFirstSections = 0;
        for (Rec r : t.recs) {
            seen.add(r.kind());
            if (r.kind() != StoreWAL.WalOpKind.SEC_HEADER) continue;
            if (r.tag() == 'K') marks++;
            if (r.off() > WalSegmentSet.SEG_HDR) nonFirstSections++;
        }
        assertEquals("the history did not reach every operation the seam reports",
                EnumSet.allOf(StoreWAL.WalOpKind.class), seen);
        assertTrue("no clean mark was written, so W5 and P11 were never reachable", marks > 0);
        // W1 is about a section following ANOTHER SECTION in the same segment. One section per
        // segment satisfies it vacuously at every append, so the count is asserted rather than the
        // rule's hit count, which cannot tell the two apart.
        assertTrue("every section in this history was the first in its segment, so W1 was satisfied"
                + " vacuously at every append", nonFirstSections > 0);

        Obligations obs = checkWriterObligations(t, f);
        for (Rule rule : Rule.values()) {
            assertTrue("rule " + rule + " never applied on this history, so it did not pass — it"
                    + " was not checked", obs.count(rule) > 0);
        }
    }

    // ================= non-vacuity: the oracle must be able to fail =================

    /**
     * W1 broken: a section's force is removed, so the next section into the same segment starts
     * over unforced bytes. This is the batching-under-one-fsync shape §6.1's mid-log-rot inference
     * cannot survive.
     */
    @Test public void the_oracle_rejects_a_trace_that_batches_two_sections_under_one_force() {
        File f = newFile();
        Trace t = runHistory(f);
        int victim = -1;
        for (int i = 0; i < t.recs.size() - 1 && victim < 0; i++) {
            if (t.recs.get(i).kind() != StoreWAL.WalOpKind.FORCE_DATA) continue;
            // only a force with a further section into the SAME segment after it makes W1 bite
            for (int j = i + 1; j < t.recs.size(); j++) {
                Rec r = t.recs.get(j);
                if (r.kind() == StoreWAL.WalOpKind.SEC_HEADER && r.seq() == t.recs.get(i).seq()) {
                    victim = i;
                    break;
                }
                if (r.kind() == StoreWAL.WalOpKind.CREATE) break;
            }
        }
        assertTrue("no force with a later same-segment section exists in this history", victim >= 0);
        assertRejects("W1", drop(t, victim), f);
    }

    /**
     * W2 broken: the directory fsync that follows a segment create is removed, so the first section
     * in that segment is acknowledged while the segment's directory entry may still vanish, taking
     * the acknowledged commit with it.
     */
    @Test public void the_oracle_rejects_a_trace_that_appends_before_the_directory_entry_is_durable() {
        File f = newFile();
        Trace t = runHistory(f);
        int victim = -1;
        for (int i = 1; i < t.recs.size() && victim < 0; i++) {
            if (t.recs.get(i).kind() == StoreWAL.WalOpKind.DIRSYNC
                    && t.recs.get(i - 1).kind() == StoreWAL.WalOpKind.FORCE_FULL
                    && !t.recs.get(i).duringOpen()) victim = i;
        }
        assertTrue("no post-create directory fsync outside an open in this history", victim >= 0);
        assertRejects("W2", drop(t, victim), f);
    }

    /**
     * W3 broken: the size-persisting seal before a rollover becomes a data-only sync. The bytes are
     * identical; what changes is whether the sealed segment's tail extent survives a crash, and
     * with it whether a tear in a non-final segment means corruption or an ordinary torn tail.
     */
    @Test public void the_oracle_rejects_a_trace_that_seals_a_segment_with_a_data_only_sync() {
        File f = newFile();
        Trace t = runHistory(f);
        int victim = -1;
        for (int i = 1; i < t.recs.size() && victim < 0; i++) {
            if (t.recs.get(i).kind() == StoreWAL.WalOpKind.CREATE
                    && t.recs.get(i - 1).kind() == StoreWAL.WalOpKind.FORCE_FULL
                    && t.recs.get(i - 1).seq() != t.recs.get(i).seq()) victim = i - 1;
        }
        assertTrue("no rollover seal in this history", victim >= 0);
        List<Rec> mutated = new ArrayList<>(t.recs);
        Rec seal = mutated.get(victim);
        mutated.set(victim, new Rec(new StoreWAL.WalIoEvent(StoreWAL.WalOpKind.FORCE_DATA,
                seal.seq(), seal.off(), seal.len(), seal.tag()), seal.duringOpen(),
                seal.markThrough()));
        assertRejects("W3", mutated, f);
    }

    /**
     * <b>P11 is NOT independently pinnable here, and this test records that rather than hiding
     * it.</b> Dropping the force of the last {@code 'C'} image the mark attests does make the
     * oracle reject — but for <b>W1</b>, not for P11.
     *
     * <p>That is not a flaw in the mutation, it is a property of the rules: <b>W1 ∧ W3 imply
     * P11</b> over any trace. W1 says a segment has no unforced bytes when the next section into
     * it starts, so the mark's own segment is clean by the time the mark is written; W3 says a
     * segment is fully forced with a size-persisting force before it is sealed, so every OTHER
     * segment is clean too. There is no reachable trace in which some segment holds unforced bytes
     * at the moment a mark is written and both other rules hold — every mutation that creates one
     * trips W1 or W3 first.
     *
     * <p><b>So why keep the P11 assertion?</b> Because the implication is an ARGUMENT, and it is
     * the class of argument this whole file exists to stop relying on. It holds only while
     * {@code appendSection} is the sole writer of bytes into a segment; a port — or a future
     * cleaner that wrote images through a bulk path — breaks the implication without touching W1
     * or W3, and then P11 is the only rule left standing between a mark and the bytes it attests.
     * It costs one loop over the segment table per mark. The honest statement is that it is
     * redundant TODAY, checked anyway, and unproven by mutation; per the house rule, this javadoc
     * is where that is said.
     */
    @Test public void p11_is_implied_by_W1_and_W3_and_this_records_which_rule_actually_bites() {
        File f = newFile();
        Trace t = runHistory(f);
        int mark = -1;
        for (int i = 0; i < t.recs.size() && mark < 0; i++) {
            Rec r = t.recs.get(i);
            if (r.kind() == StoreWAL.WalOpKind.SEC_HEADER && r.tag() == 'K') mark = i;
        }
        assertTrue("no mark in this history", mark >= 0);
        // the force immediately preceding the mark made the last re-emitted image durable
        int force = -1;
        for (int i = mark - 1; i >= 0 && force < 0; i--) {
            StoreWAL.WalOpKind k = t.recs.get(i).kind();
            if (k == StoreWAL.WalOpKind.FORCE_DATA || k == StoreWAL.WalOpKind.FORCE_FULL) force = i;
        }
        assertTrue("no force precedes the mark", force >= 0);
        assertRejectsForAnyOf("dropping the last force before a mark",
                drop(t, force), f, "W1", "W2", "W3");

        // And the rule DID apply on the unmutated trace — it is redundant, not skipped.
        assertTrue("P11 never applied, so its redundancy was not even demonstrated",
                checkWriterObligations(t, f).count(Rule.P11_MARK_ATTESTS_ONLY_FORCED_BYTES) > 0);
    }

    /**
     * W5 broken: the unlink moves ahead of the mark's force. The mark's bytes are in the page cache
     * either way, so the log looks identical; what changes is that a crash in the window destroys
     * the segments while the mark that authorized it never reached the device — which is exactly
     * the "acknowledged commits vanish" outcome the rule exists to prevent.
     */
    @Test public void the_oracle_rejects_a_trace_that_unlinks_before_the_mark_is_forced() {
        File f = newFile();
        Trace t = runHistory(f);
        int unlink = -1;
        for (int i = 0; i < t.recs.size() && unlink < 0; i++) {
            Rec r = t.recs.get(i);
            if (r.kind() == StoreWAL.WalOpKind.UNLINK && !r.duringOpen()) unlink = i;
        }
        assertTrue("no writer unlink in this history", unlink >= 0);
        int markForce = -1;
        for (int i = unlink - 1; i >= 0 && markForce < 0; i--) {
            if (t.recs.get(i).kind() == StoreWAL.WalOpKind.FORCE_DATA
                    && t.recs.get(i).tag() == 'K') markForce = i;
        }
        assertTrue("the unlink is not preceded by the mark's force", markForce >= 0);
        assertRejects("W5", drop(t, markForce), f);
    }

    // ================= the fault sweep: every operation, failed in turn =================

    /**
     * Fails the {@code failAt}-th operation and records everything, so the run that fails and the
     * run that recovers are both observable. {@code failAt < 0} injects nothing.
     */
    static final class Injector implements StoreWAL.WalIo {
        final int failAt;
        final List<StoreWAL.WalIoEvent> recs = new ArrayList<>();
        int n;
        StoreWAL.WalIoEvent failed;

        Injector(int failAt) { this.failAt = failAt; }

        @Override public void before(StoreWAL.WalIoEvent e) throws IOException {
            recs.add(e);
            if (n++ != failAt) return;
            failed = e;
            throw new IOException("injected tier-2 fault at event " + failAt + ": " + e);
        }
    }

    /**
     * What one run of {@link #faultWorkload} acknowledged before it stopped.
     *
     * @param failedClosed the store's own {@link StoreWAL#isClosed()} at the moment the injected
     *                     fault propagated out. {@code null} when no fault fired, or when it fired
     *                     inside the constructor and there is no store object to ask.
     */
    private record Ack(Map<Long, byte[]> committed, boolean reachedEnd, Boolean failedClosed) { }

    /**
     * The workload the sweep replays. Deterministic: the same recids in the same order every run,
     * so the fault index is the only variable. Short on purpose — one section per segment makes it
     * a multi-segment log and a cleaning cycle in a dozen operations.
     *
     * <p>A record is added to {@code committed} only once {@link StoreWAL#commit} has RETURNED,
     * which is the acknowledgement rule. A commit whose durability point was reached but whose
     * force then failed is not acknowledged, and may or may not survive — the oracle below requires
     * only that what WAS acknowledged does.
     */
    private Ack faultWorkload(File f, Injector inj, long segmentBytes) {
        StoreWAL.testSetWalIo(inj);
        Map<Long, byte[]> ack = new LinkedHashMap<>();
        boolean reachedEnd = false;
        Boolean failedClosed = null;
        StoreWAL s = null;
        try {
            s = new StoreWAL(f);
            s.setMinLogBytes(0);
            s.setSegmentBytes(segmentBytes);
            for (int i = 0; i < 4; i++) {
                byte[] v = Fixtures.payload(i + 1, i + 1, 40);
                long r = s.put(v, Fixtures.RAW);
                s.commit();
                ack.put(r, v);
            }
            s.testCleanOldestSegment();
            byte[] v = Fixtures.payload(9, 9, 40);
            long r = s.put(v, Fixtures.RAW);
            s.commit();
            ack.put(r, v);
            reachedEnd = true;
        } catch (org.mapdb.DBException expectedWhenInjecting) {
            // Only DBException is swallowed, and only while injecting. Anything else — an
            // AssertionError from the store's own invariants above all — must reach the runner:
            // a sweep that caught Error would turn every broken invariant into a silent pass.
            if (inj.failAt < 0) throw expectedWhenInjecting;
            // Sampled HERE, before close(), or the answer would be "closed" for every run.
            if (s != null) failedClosed = s.isClosed();
        } finally {
            if (s != null) {
                try {
                    s.close();
                } catch (RuntimeException alreadyFailedClosed) { }
            }
            StoreWAL.testSetWalIo(null);
        }
        return new Ack(ack, reachedEnd, failedClosed);
    }

    /**
     * <b>The sweep.</b> Fail each operation of the workload in turn and require, every time, that
     * reopening the store yields <em>at least</em> every commit that was acknowledged before the
     * fault, with the store passing its own {@code verify()}.
     *
     * <p><b>"At least", not "exactly", and the difference is the specification's.</b> A commit
     * whose section reached the file but whose force then failed is not acknowledged — {@code
     * commit()} threw — yet its bytes are complete and valid, so recovery is entitled to replay it.
     * Requiring equality would demand the writer un-write bytes it had already handed to the
     * platform, which nothing can do. What must never happen is the other direction, and that is
     * what is asserted.
     *
     * <p>The sweep also asserts the store <b>fails CLOSED</b>: after an injected write fault the
     * store must not be usable. That is W9's first clause, and the reason it exists is a latent v1
     * defect — the old code threw without closing, so the caller's retry wrote a complete, forced,
     * <em>acknowledged</em> section after the garbage, and the next open discarded both.
     */
    @Test public void every_injected_io_fault_preserves_every_acknowledged_commit() {
        File probe = newFile();
        Injector survey = new Injector(-1);
        Ack full = faultWorkload(probe, survey, ONE_SECTION_PER_SEGMENT);
        assertTrue("the survey run must complete, or the sweep has nothing to sweep",
                full.reachedEnd());
        int operations = survey.recs.size();
        assertTrue("the workload is too short to be a sweep: " + operations, operations >= 20);

        int faultsFired = 0, storesReopened = 0, wroteAndStayedOpen = 0;
        for (int at = 0; at < operations; at++) {
            File f = newFile();
            Injector inj = new Injector(at);
            Ack ack = faultWorkload(f, inj, ONE_SECTION_PER_SEGMENT);
            assertNotNull("the workload is not deterministic: event " + at + " existed in the"
                    + " survey run but not in the injected one", inj.failed);
            faultsFired++;
            // W9's first clause. A fault while WRITING must leave the store unusable: the v1 code
            // threw without closing, and the caller's retry then wrote a complete, forced,
            // acknowledged section after the garbage. Scoped to the write kinds — a failed unlink
            // or directory sync is a leak the next open retries, not a reason to close the store,
            // and asserting over those would demand the wrong behaviour.
            if (ack.failedClosed() != null && isWrite(inj.failed.kind()) && !ack.failedClosed())
                wroteAndStayedOpen++;

            StoreWAL re = new StoreWAL(f);
            try {
                for (Map.Entry<Long, byte[]> e : ack.committed().entrySet()) {
                    byte[] got;
                    try {
                        got = re.get(e.getKey(), Fixtures.RAW);
                    } catch (org.mapdb.DBException gone) {
                        throw new AssertionError("fault at " + at + " (" + inj.failed + ") LOST the"
                                + " acknowledged commit at recid " + e.getKey(), gone);
                    }
                    assertNotNull("fault at " + at + " (" + inj.failed + ") lost acknowledged recid "
                            + e.getKey(), got);
                    assertArrayEquals("fault at " + at + " (" + inj.failed + ") changed"
                            + " acknowledged recid " + e.getKey(), e.getValue(), got);
                }
                re.verify();
                storesReopened++;
            } finally {
                re.close();
            }
        }
        assertTrue("no injected fault ever fired, so this swept nothing", faultsFired > 0);
        assertEquals("every fault that fired must leave a reopenable store",
                faultsFired, storesReopened);
        assertEquals("W9: a failed section write left the store OPEN, so a caller could retry into"
                + " a segment holding partial bytes", 0, wroteAndStayedOpen);
    }

    /** The operations that put bytes into a segment, as opposed to naming or syncing files. */
    private static boolean isWrite(StoreWAL.WalOpKind k) {
        return k == StoreWAL.WalOpKind.SEC_HEADER || k == StoreWAL.WalOpKind.SEC_BODY
                || k == StoreWAL.WalOpKind.FORCE_DATA;
    }

    /**
     * <b>W9, the clause the trace alone cannot show.</b> A fault between a section's header and its
     * body leaves PARTIAL bytes at the write position — the one state W9 forbids appending after.
     * The writer must fail the store closed, and the next open must truncate to the last durable
     * section and <b>rotate</b> (W7) rather than reuse the torn segment's checksum domain.
     *
     * <p>Without this the v1 defect returns: a retry writes a complete, forced, acknowledged
     * section after the garbage at {@code lastLsn + 1}, while the suspect-header lookahead demands
     * {@code lastLsn + 2}, so the next open classifies a torn tail and truncates at the garbage,
     * discarding the acknowledged commit and everything above it.
     */
    @Test public void a_partial_section_write_fails_closed_and_the_next_open_rotates_past_it() {
        File probe = newFile();
        Injector survey = new Injector(-1);
        faultWorkload(probe, survey, SEVERAL_SECTIONS_PER_SEGMENT);   // survey only
        int at = -1;
        for (int i = 0; i < survey.recs.size() && at < 0; i++) {
            // A SEC_BODY fault: the section header reaches the file, its body does not. It must be
            // a NON-FIRST section of its segment, so the truncation leaves a durable prefix behind
            // — a torn FIRST section truncates the segment to empty, which is H8's path and says
            // nothing about reusing a checksum domain. This is why the test runs at
            // SEVERAL_SECTIONS_PER_SEGMENT: at the minimum size no such section exists, and the
            // first draft of this test asserted its way into finding that out.
            if (survey.recs.get(i).kind() == StoreWAL.WalOpKind.SEC_BODY
                    && survey.recs.get(i).off() > WalSegmentSet.SEG_HDR + StoreWAL.SEC_HDR) at = i;
        }
        assertTrue("no non-first section body write in this workload", at >= 0);

        File f = newFile();
        Injector inj = new Injector(at);
        Ack ack = faultWorkload(f, inj, SEVERAL_SECTIONS_PER_SEGMENT);
        assertNotNull("the fault did not fire", inj.failed);
        long tornSeq = inj.failed.seq();

        // the store failed closed, so nothing was appended after the partial bytes
        int writesAfterFault = 0;
        boolean past = false;
        for (StoreWAL.WalIoEvent e : inj.recs) {
            if (e == inj.failed) { past = true; continue; }
            if (past && e.seq() == tornSeq && e.kind() == StoreWAL.WalOpKind.SEC_HEADER)
                writesAfterFault++;
        }
        assertEquals("W9: the writer appended into a segment holding partial bytes", 0,
                writesAfterFault);

        // and the next open truncates and ROTATES rather than appending into that segment
        Injector after = new Injector(-1);
        StoreWAL.testSetWalIo(after);
        StoreWAL re = new StoreWAL(f);
        try {
            for (Map.Entry<Long, byte[]> e : ack.committed().entrySet())
                assertArrayEquals("the partial write cost an acknowledged commit at recid "
                        + e.getKey(), e.getValue(), re.get(e.getKey(), Fixtures.RAW));
            re.setMinLogBytes(0);
            re.setSegmentBytes(SEVERAL_SECTIONS_PER_SEGMENT);
            re.put(Fixtures.payload(7, 7, 40), Fixtures.RAW);
            re.commit();
            re.verify();
        } finally {
            re.close();
            StoreWAL.testSetWalIo(null);
        }
        boolean truncated = false, appendedIntoTorn = false;
        for (StoreWAL.WalIoEvent e : after.recs) {
            if (e.kind() == StoreWAL.WalOpKind.TRUNCATE && e.seq() == tornSeq) truncated = true;
            if (e.kind() == StoreWAL.WalOpKind.SEC_HEADER && e.seq() == tornSeq)
                appendedIntoTorn = true;
        }
        assertTrue("R7 did not truncate the torn segment, so this workload never reached the state"
                + " W7 is about", truncated);
        assertTrue("W7: the reopen appended into the segment it had just truncated, reusing that"
                + " segment's checksum domain", !appendedIntoTorn);
    }

    // ---------- helpers for the mutation tests ----------

    private static List<Rec> drop(Trace t, int index) {
        List<Rec> out = new ArrayList<>(t.recs);
        out.remove(index);
        return out;
    }

    /**
     * Feeds a mutated trace to the oracle and requires it to reject. The failure message must name
     * the rule, so a mutation that trips a <em>different</em> rule does not count as the check
     * under test having worked.
     */
    private void assertRejects(String rule, List<Rec> mutated, File base) {
        assertRejectsForAnyOf(rule, mutated, base, rule);
    }

    /**
     * As {@link #assertRejects}, for a mutation whose violation is caught by one of several rules.
     * Used only where the rules genuinely overlap, and the test that uses it says which and why.
     */
    private void assertRejectsForAnyOf(String what, List<Rec> mutated, File base, String... rules) {
        Trace t = new Trace(base);
        t.recs.addAll(mutated);
        try {
            checkWriterObligations(t, base);
        } catch (AssertionError expected) {
            assertNotNull(expected.getMessage());
            for (String rule : rules) {
                if (expected.getMessage().contains(rule + ":")) return;
            }
            fail("the oracle rejected the mutated trace, but for " + expected.getMessage()
                    + " rather than for any of " + String.join("/", rules));
        }
        fail("the oracle accepted " + what + ", so it is vacuous there");
    }
}
