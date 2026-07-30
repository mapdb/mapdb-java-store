package org.mapdb.store;

import org.junit.After;
import org.junit.Test;
import org.mapdb.DBException;
import org.mapdb.TmpFiles;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * <b>The Q5 segment-event gate.</b> {@link StoreWALSmallModelEnumeratorTest} enumerates API
 * histories against an oracle but, by its own admission (item 5 of its "what it cannot find"
 * list), is blind to everything the segment set added: rollover, the {@code 'K'} clean mark,
 * unlink, and the crash points between them. Both confirmed defects lived in that
 * enumerator's blind spots, and the one real defect the segment-set work found — pass 1 carrying
 * {@code lastLsn} across a segment boundary — lives in exactly this one. So the events get
 * enumerated too.
 *
 * <p>The method: run a short API history with {@code segmentBytes} at its <b>minimum</b>, so every
 * section lands in its own segment and an ordinary history becomes a multi-segment one; then
 * <b>perturb the resulting directory</b> into crash images a conforming writer could have left,
 * and assert what Q5 says each must do. The alphabet is deliberately <em>commit-dense</em>
 * (most ops mutate and commit in one step): what this test needs from a history is many committed
 * sections, because a section is a segment here, and segments are the subject.
 *
 * <h2>The five phases</h2>
 * <ol>
 * <li><b>A — plain.</b> The history itself, one section per segment. Covers cross-segment
 *     {@code T_APPEND} bases, cross-segment LSN density, W3 rollover, W7's truncate-then-rotate
 *     and checkpoint-over-many-segments. No perturbation.</li>
 * <li><b>B — torn tail (C9/C12).</b> Truncate the HIGHEST segment. Every result must OPEN — a
 *     torn tail is an ordinary crash artifact, never corruption — and must recover either the
 *     newest committed state or the one before it: a tear may cost the last section and must
 *     never cost two. {@link #the_full_byte_by_byte_truncation_sweep_of_the_active_segment} does
 *     every offset over fixed histories; the sweep here samples the boundaries.</li>
 * <li><b>C — crash mid-unlink (K5/K9/N3, the legal interior gap).</b> Reconstruct the image a
 *     crash between a forced {@code 'K'} and the completion of its unlink leaves — the retired
 *     segments put back — then delete an ARBITRARY SUBSET of them, prefix or not, because syscall
 *     order says nothing about the order removals persist. Every one must open and recover the
 *     final committed state EXACTLY (Q5 §2.3, retained-set equivalence).</li>
 * <li><b>D — the negatives.</b> A sequence gap ABOVE the mark, and any truncation of a non-final
 *     retained segment, must be REFUSED. These keep B and C from being vacuous: a recovery that
 *     accepted everything would pass A, B and C and fail only here. The sweep asserts a floor on
 *     how many refusals it actually observed, for the same reason.</li>
 * <li><b>E — create-crash residue, then ordinary operation (W6/R2/N3).</b> Plant residue at the
 *     next sequence number, reopen so R2 sweeps it and W6 burns that number, then <b>keep
 *     operating</b> and reopen twice more. This is the case a reviewer had to find
 *     by hand precisely because this sweep used to stop at reopen-and-check.</li>
 * </ol>
 *
 * <p><b>What this still cannot find</b>, so the bound is not mistaken for completeness: bit rot
 * inside a section body (the CRC-domain tests in {@link StoreWALSegmentSetTest} carry that),
 * durable-write faults below the channel (no injector exists), the capacity straddle, and
 * anything needing more than {@value #MAX_RECIDS} recids or depth {@value #DEFAULT_DEPTH}.
 */
public class StoreWALSegmentEventEnumeratorTest {

    private static final int DEFAULT_DEPTH = 3;
    /** Records live at once. Two is enough for delete-then-reuse across a segment boundary. */
    private static final int MAX_RECIDS = 2;
    /** One section per segment: the smallest legal value, so every append rolls over. */
    private static final long ONE_SECTION_PER_SEGMENT = WalSegmentSet.SEG_HDR + StoreWAL.SEC_HDR;
    /** Cap on phase C's subset enumeration, so a long history cannot explode it. */
    private static final int MAX_RETIRED_SUBSETS = 32;

    private final List<File> files = new ArrayList<>();
    private int histories, images, refusals;

    @After public void cleanup() {
        for (File f : files) wipe(f);
        files.clear();
    }

    // ---------------------------------------------------------------- the oracle

    /**
     * Committed snapshots in order, oldest first. Phase B's oracle is "the last or the one before
     * it", which is why the whole sequence is kept rather than a single state.
     */
    private static final class Model {
        final Map<Long, byte[]> visible = new LinkedHashMap<>();
        final List<Map<Long, byte[]>> committedHistory = new ArrayList<>();

        Model() { committedHistory.add(new LinkedHashMap<>()); }

        Map<Long, byte[]> committed() {
            return committedHistory.get(committedHistory.size() - 1);
        }

        /**
         * The states a torn tail may legitimately land on: the newest, or the one before it.
         *
         * <p>Over <b>distinct</b> states, not over commits. A commit whose transaction staged
         * nothing — or whose only op was a capacity-REFUSED append — writes no section at all, so
         * commits and sections are not in bijection; counting commits made this oracle reject a
         * correct recovery. Distinct states and sections <em>are</em> in the right relation: the
         * highest segment holds exactly one section here, and one section is at most one state
         * change ({@code 'C'} and {@code 'K'} change none).
         */
        List<Map<Long, byte[]>> tolerableAfterTear() {
            List<Map<Long, byte[]>> distinct = distinctStates();
            List<Map<Long, byte[]>> out = new ArrayList<>();
            out.add(distinct.get(distinct.size() - 1));
            if (distinct.size() >= 2) out.add(distinct.get(distinct.size() - 2));
            return out;
        }

        private List<Map<Long, byte[]>> distinctStates() {
            List<Map<Long, byte[]>> out = new ArrayList<>();
            for (Map<Long, byte[]> s : committedHistory) {
                if (out.isEmpty() || !sameState(out.get(out.size() - 1), s)) out.add(s);
            }
            return out;
        }

        private static boolean sameState(Map<Long, byte[]> a, Map<Long, byte[]> b) {
            if (!a.keySet().equals(b.keySet())) return false;
            for (Map.Entry<Long, byte[]> e : a.entrySet()) {
                if (!Arrays.equals(e.getValue(), b.get(e.getKey()))) return false;
            }
            return true;
        }

        void commit() { committedHistory.add(copyOf(visible)); }

        /** Rollback and a reopen agree: both discard everything not committed. */
        void discardStaged() {
            visible.clear();
            visible.putAll(copyOf(committed()));
        }

        static Map<Long, byte[]> copyOf(Map<Long, byte[]> m) {
            Map<Long, byte[]> c = new LinkedHashMap<>();
            for (Map.Entry<Long, byte[]> e : m.entrySet())
                c.put(e.getKey(), e.getValue() == null ? null : e.getValue().clone());
            return c;
        }
    }

    // ---------------------------------------------------------------- the sweep

    @Test public void every_segment_crash_image_of_every_short_history_recovers_as_Q5_says()
            throws IOException {
        int depth = Integer.getInteger("mapdb.segenum.depth", DEFAULT_DEPTH);
        long t0 = System.nanoTime();
        explore(new ArrayList<>(), depth);
        long ms = (System.nanoTime() - t0) / 1_000_000;
        System.out.printf("segment-event enumerator: depth %d, ≤%d recids, %d histories,"
                        + " %d crash images, %d refusals asserted, %d ms%n",
                depth, MAX_RECIDS, histories, images, refusals, ms);
        // a sweep that silently checked nothing would pass every assertion inside it
        if (histories < 50) fail("enumeration collapsed to " + histories + " histories");
        if (images < 300) fail("perturbation collapsed to " + images + " crash images");
        if (refusals < 20) fail("only " + refusals + " refusals asserted: phase D went vacuous");
    }

    private void explore(List<String> prefix, int remaining) throws IOException {
        checkHistory(prefix);
        if (remaining == 0) return;
        for (String op : alphabetAfter(prefix)) {
            List<String> next = new ArrayList<>(prefix);
            next.add(op);
            explore(next, remaining - 1);
        }
    }

    /**
     * The applicable alphabet for the state a prefix reaches, recomputed by replaying it — the
     * sweep is over histories, not over op-code tuples. Ops are commit-dense: {@code putC} is
     * put-then-commit, and only {@code putStaged} deliberately leaves a transaction open, because
     * the cleaner's {@code stagedCreated} filter is a rule that needs one.
     */
    private List<String> alphabetAfter(List<String> prefix) throws IOException {
        Model m = new Model();
        File f = newFile();
        StoreWAL s = open(f);
        try {
            boolean staged = false;
            for (String op : prefix) {
                s = apply(s, m, f, op);
                staged = op.equals("putStaged")
                        || (staged && (op.equals("checkpoint") || op.equals("clean")));
            }
            List<String> ops = new ArrayList<>();
            if (m.visible.size() < MAX_RECIDS) {
                ops.add("putC");
                ops.add("putStaged");
            }
            for (int i = 0; i < m.visible.size(); i++) {
                ops.add("updateC" + i);
                ops.add("appendC" + i);
                ops.add("deleteC" + i);
            }
            ops.add("checkpoint");
            // The INCREMENTAL cleaner, which `checkpoint` cannot reach: a whole-log
            // clean retires everything in one cycle and re-emits the entire store, so it never
            // produces the shape this op does — a mark that retires ONE segment, above a retained
            // set that still contains ordinary 'S' sections and older marks. Only applicable when
            // something sits below the active segment, which at one section per segment means
            // after the first commit.
            if (WalTestKit.segments(f).length >= 2) ops.add("clean");
            // Rollback only after a staged put: it is the one op that must move NEITHER identity
            // (§5.3) and must leave no section behind, so its interaction with the segment set is
            // "nothing happened" — which is exactly the kind of claim worth enumerating rather
            // than asserting.
            if (staged) ops.add("rollback");
            return ops;
        } finally {
            close(s);
            wipe(f);
        }
    }

    /** Runs one history, then puts its on-disk image through all four phases. */
    private void checkHistory(List<String> history) throws IOException {
        Model m = new Model();
        File f = newFile();
        // Phase C needs the pre-unlink image, which exists only mid-checkpoint. Snapshotting every
        // segment just before each checkpoint and restoring the retired ones afterwards
        // reconstructs it exactly — the writer is not instrumented at all.
        Map<Long, byte[]> preCheckpoint = new LinkedHashMap<>();
        StoreWAL s = open(f);
        try {
            for (String op : history) {
                if (op.equals("checkpoint") || op.equals("clean"))
                    preCheckpoint = snapshotSegments(f);
                try {
                    s = apply(s, m, f, op);
                } catch (RuntimeException e) {
                    throw new AssertionError("history " + history + " failed at " + op, e);
                }
            }
            close(s);

            // ---- phase A: the plain multi-segment image
            s = open(f);
            check(s, m.committed(), history, "phase A reopen");
            s.verify();
            close(s);
            histories++;

            byte[][] image = readSegments(f);
            long[] seqs = segmentSeqs(f);
            if (image.length == 0) return;

            phaseB(f, history, m, image, seqs, boundarySamples(image[image.length - 1].length),
                    preCheckpoint);
            phaseC(f, history, m, image, seqs, preCheckpoint);
            phaseC2(f, history, m, image, seqs, preCheckpoint);
            phaseD(f, history, image, seqs);
            phaseE(f, history, m, image, seqs);
        } finally {
            close(s);
            wipe(f);
        }
    }

    // ---------------------------------------------------------------- phase B: torn tail

    /**
     * The offsets where a tear changes CLASS rather than merely degree: an empty segment (H8), a
     * partial section header (S3), a complete header with a partial body (S5), and the untouched
     * whole. Between those, one more byte lost is the same rule again — which is what the
     * full-sweep test exists to confirm rather than assume.
     */
    private static List<Integer> boundarySamples(int len) {
        Set<Integer> at = new LinkedHashSet<>();
        int hdr = WalSegmentSet.SEG_HDR;
        for (int c : new int[]{hdr, hdr + 1, hdr + StoreWAL.SEC_HDR - 1, hdr + StoreWAL.SEC_HDR,
                hdr + StoreWAL.SEC_HDR + 1, len - 1, len}) {
            if (c >= hdr && c <= len) at.add(c);
        }
        return new ArrayList<>(at);
    }

    private void phaseB(File f, List<String> history, Model m, byte[][] image, long[] seqs,
                        List<Integer> lengths, Map<Long, byte[]> preCleaning) throws IOException {
        int top = image.length - 1;
        // A truncation that erases the NEWEST clean mark, when that mark's removals have already
        // persisted, does not produce a reachable image: the 'K' is forced before the unlink (W5), so
        // a crash cannot lose the mark while keeping its effect. Such an image must be REFUSED, not
        // opened — accepting it means accepting a log whose stated start nothing accounts for, which
        // is byte-identical to deleting the lowest segment of a healthy one. So those lengths are
        // adjudicated separately.
        //
        // An older surviving mark does NOT rescue the image, and the condition used to require that
        // there be none ("the log's only mark"). That was written when every clean retired the whole
        // log, so a mark never outlived the cycle after it. The incremental cleaner retires one
        // segment at a time, so older marks routinely survive — and an older mark's logStartLsn
        // describes a log the newer cycle has since shortened, so recovering under it reports
        // exactly the "sections below it are gone" it is supposed to. Requiring a lone mark here
        // would have demanded that a store open on an image its own writer cannot produce.
        //
        // "Its removals persisted" is judged against the segments the modeled cleaning event
        // actually removed — the pre-cleaning snapshot phase C already keeps — never by looking for
        // holes in the sequence NUMBERS. W6 burns numbers on create-crash residue, so a numeric gap
        // is not evidence of an unlink; and a range loop over sequence values is work proportional
        // to an attacker-supplied 64-bit name. Both are on the R4 postmortem's do-not-reintroduce
        // list, and an oracle that inferred removal from numeric density would go wrong the moment
        // this sweep combines burnt numbers with a reconstructed pre-unlink image.
        long mark = newestMark(image);
        boolean markRemovalsPersisted = mark > 0 && anyRemoved(preCleaning, seqs, mark);
        for (int len : lengths) {
            restore(f, image, seqs);
            WalTestKit.write(WalTestKit.segment(f, seqs[top]), Arrays.copyOf(image[top], len));
            String at = "phase B truncate segment " + seqs[top] + " to " + len
                    + " (of " + image[top].length + ")";
            if (markRemovalsPersisted && newestMark(readSegments(f)) != mark) {
                expectRefusal(f, history, at + " erases the mark that authorized completed removals");
                continue;
            }
            StoreWAL s;
            try {
                s = open(f);
            } catch (DBException e) {
                throw new AssertionError("history " + history + ": " + at
                        + " must open (a torn tail is not corruption): " + e.getMessage(), e);
            }
            try {
                checkOneOf(s, m.tolerableAfterTear(), history, at);
                s.verify();
            } finally {
                close(s);
            }
            images++;
        }
    }

    // ---------------------------------------------------------------- phase C: crash mid-unlink

    private void phaseC(File f, List<String> history, Model m, byte[][] image, long[] seqs,
                        Map<Long, byte[]> preCheckpoint) throws IOException {
        if (preCheckpoint.isEmpty()) return;
        long mark = newestMark(image);
        if (mark <= 0) return;
        List<Long> retired = new ArrayList<>();
        for (Map.Entry<Long, byte[]> e : preCheckpoint.entrySet()) {
            if (e.getKey() <= mark) retired.add(e.getKey());
        }
        if (retired.isEmpty()) return;
        int n = Math.min(retired.size(), 5);
        int subsets = Math.min(1 << n, MAX_RETIRED_SUBSETS);
        for (int bits = 0; bits < subsets; bits++) {
            restore(f, image, seqs);
            // put every retired segment back, then remove the ones this subset says crashed away
            for (int i = 0; i < n; i++) {
                long seq = retired.get(i);
                if ((bits & (1 << i)) == 0)
                    WalTestKit.write(WalTestKit.segment(f, seq), preCheckpoint.get(seq));
            }
            String at = "phase C mark=" + mark + " retired-still-present-bits=~" + bits;
            StoreWAL s;
            try {
                s = open(f);
            } catch (DBException e) {
                throw new AssertionError("history " + history + ": " + at
                        + " is a legal crash-mid-unlink image and must open: " + e.getMessage(), e);
            }
            try {
                check(s, m.committed(), history, at);
                s.verify();
            } finally {
                close(s);
            }
            images++;
        }
    }

    /**
     * <b>Phase C2 — damage a retired-but-present segment.</b> Phase C only ever <em>deletes</em>
     * retired segments, and that gap is where a defect lived: a superseded segment that is
     * still present but unusable — rotted, torn, or truncated to a bare header — must be as
     * irrelevant as an absent one, because Q5 R4 says damage below the mark is "bytes nobody will
     * read". An earlier revision instead let such a segment divert the chain anchor downward onto a
     * stale predecessor and then refused the whole store, permanently, on a legal K5 image.
     *
     * <p>So every retired-but-restored segment gets each damage flavour in turn, and every result
     * must still open and recover the final committed state exactly.
     */
    private void phaseC2(File f, List<String> history, Model m, byte[][] image, long[] seqs,
                         Map<Long, byte[]> preCheckpoint) throws IOException {
        if (preCheckpoint.isEmpty()) return;
        long mark = newestMark(image);
        if (mark <= 0) return;
        for (Map.Entry<Long, byte[]> retired : preCheckpoint.entrySet()) {
            if (retired.getKey() > mark) continue;
            byte[] whole = retired.getValue();
            if (whole.length <= WalSegmentSet.SEG_HDR) continue;   // already empty: nothing to damage
            for (int flavour = 0; flavour < 3; flavour++) {
                restore(f, image, seqs);
                // put every retired segment back, so a CLEAN lower neighbour survives below the
                // damaged one — that adjacency is what made the old anchor search go wrong
                for (Map.Entry<Long, byte[]> e : preCheckpoint.entrySet()) {
                    if (e.getKey() <= mark)
                        WalTestKit.write(WalTestKit.segment(f, e.getKey()), e.getValue());
                }
                byte[] damaged = switch (flavour) {
                    case 0 -> rotOneBodyByte(whole);                                  // held: body CRC
                    case 1 -> Arrays.copyOf(whole, whole.length - 3);                 // held: torn
                    default -> Arrays.copyOf(whole, WalSegmentSet.SEG_HDR);           // valid-empty
                };
                WalTestKit.write(WalTestKit.segment(f, retired.getKey()), damaged);
                String at = "phase C2 damage flavour " + flavour + " on retired segment "
                        + retired.getKey() + " (mark " + mark + ")";
                StoreWAL s;
                try {
                    s = open(f);
                } catch (DBException e) {
                    throw new AssertionError("history " + history + ": " + at
                            + " — damage BELOW the mark is irrelevant and must not refuse the open: "
                            + e.getMessage(), e);
                }
                try {
                    check(s, m.committed(), history, at);
                    s.verify();
                } finally {
                    close(s);
                }
                images++;
            }
        }
    }

    /** True when every sequence number at or below {@code mark} is still on disk. */
    /**
     * Did a modeled cleaning event's unlink actually take effect? True when some segment that
     * existed before it, and that the mark authorized removing, is absent from the final image.
     * Over the SEGMENTS observed, never over the sequence-number range between them.
     */
    private static boolean anyRemoved(Map<Long, byte[]> preCleaning, long[] seqs, long mark) {
        java.util.HashSet<Long> present = new java.util.HashSet<>();
        for (long q : seqs) present.add(q);
        for (Long before : preCleaning.keySet()) {
            if (before <= mark && !present.contains(before)) return true;
        }
        return false;
    }

    /** Flips a byte inside the first section's body, producing a body-CRC held verdict. */
    private static byte[] rotOneBodyByte(byte[] seg) {
        byte[] out = seg.clone();
        int off = WalSegmentSet.SEG_HDR + StoreWAL.SEC_HDR;
        if (off < out.length) out[off] ^= 0x5A;
        return out;
    }

    // ---------------------------------------------------------------- phase D: the negatives

    private void phaseD(File f, List<String> history, byte[][] image, long[] seqs)
            throws IOException {
        long mark = newestMark(image);
        // D1: remove ANY non-highest segment above the mark -> refuse. An interior one leaves a
        // sequence gap (N3); the LOWEST retained one leaves no gap at all, and catching it is the
        // whole point of R4's floor check — nothing but a mark may authorize a segment's absence.
        // The highest is excluded because losing it is an indistinguishable lost tail.
        for (int i = 0; i < image.length - 1; i++) {
            if (seqs[i] <= mark) continue;
            restore(f, image, seqs);
            assertTrue(WalTestKit.segment(f, seqs[i]).delete());
            expectRefusal(f, history, "phase D1 gap: removed segment " + seqs[i]);
        }
        // D2: truncate a NON-FINAL retained segment. W3 guarantees it ends exactly at a section
        // boundary, so a short read there is corruption (S6/S5/S3); truncated to empty instead,
        // its section's LSN vanishes and R4's cross-segment density check must catch the hole.
        for (int i = 0; i < image.length - 1; i++) {
            if (seqs[i] <= mark) continue;
            if (image[i].length <= WalSegmentSet.SEG_HDR) continue;   // already empty (H8): legal
            for (int len : new int[]{WalSegmentSet.SEG_HDR, image[i].length - 1}) {
                if (len < WalSegmentSet.SEG_HDR || len >= image[i].length) continue;
                restore(f, image, seqs);
                WalTestKit.write(WalTestKit.segment(f, seqs[i]), Arrays.copyOf(image[i], len));
                expectRefusal(f, history,
                        "phase D2 torn non-final: segment " + seqs[i] + " truncated to " + len);
            }
        }
    }

    private void expectRefusal(File f, List<String> history, String at) {
        StoreWAL s = null;
        try {
            s = open(f);
            fail("history " + history + ": " + at + " must be refused, but the store opened");
        } catch (DBException.DataCorruption expected) {
            refusals++;
        } finally {
            close(s);
        }
        images++;
    }

    // ---------------------------------------------------------------- phase E: residue, then keep going

    /**
     * <b>Phase E — create-crash residue, and then ordinary operation.</b> This is the shape of
     * the case found by a reviewer BY HAND because the sweep stopped at
     * reopen-and-check and never kept operating afterwards.
     *
     * <p>R2 removes create-crash residue and W6 burns its sequence number, so the next rollover
     * creates a segment ABOVE the burnt one and the namespace legitimately contains a gap. Under
     * the literal N3 ("an interior gap above the mark is corruption") the store then refused to
     * open <em>forever</em>, on a history containing one ordinary crash and nothing else. The
     * assertion that catches it is not "does it reopen" but <b>"does it still reopen after being
     * used again"</b> — so this phase reopens, commits, closes, and reopens twice more.
     */
    private void phaseE(File f, List<String> history, Model m, byte[][] image, long[] seqs)
            throws IOException {
        for (byte[] residue : RESIDUE_SHAPES) {
            restore(f, image, seqs);
            long residueSeq = seqs[seqs.length - 1] + 1;
            WalTestKit.write(WalTestKit.segment(f, residueSeq), residue);
            String at = "phase E residue(" + residue.length + "B) at seq " + residueSeq;

            Map<Long, byte[]> expected = Model.copyOf(m.committed());
            StoreWAL s = null;
            try {
                s = open(f);                                    // R2 sweeps it, W6 burns the seq
                check(s, expected, history, at + " after the sweep");
                // Two commits, so a rollover happens whether or not the active segment was empty:
                // that is what puts a live segment above the burnt number and opens the gap.
                for (int i = 0; i < 2; i++) {
                    byte[] v = value(90 + i, "e");
                    expected.put(s.put(v, Fixtures.RAW), v);
                    s.commit();
                }
            } finally {
                close(s);
            }
            // W6 itself: the burnt number must never come back. Without this the phase would
            // pass even if W6's residue accounting were deleted — the successor would then REUSE
            // residueSeq, no gap would form, and both reopens below would succeed. Reuse is the
            // whole thing W6 forbids, because a stale directory entry can alias a later create.
            assertTrue(at + ": the residue's sequence number must never be reused",
                    !WalTestKit.segment(f, residueSeq).exists());
            long highest = 0;
            for (long seq : segmentSeqs(f)) highest = Math.max(highest, seq);
            assertTrue(at + ": the successor must land ABOVE the burnt number, got " + highest,
                    highest > residueSeq);

            // And the gap must not brick the store — not now, and not on any later open. A single
            // reopen was exactly what the W6 test was missing.
            for (int open = 0; open < 2; open++) {
                StoreWAL s2;
                try {
                    s2 = open(f);
                } catch (DBException e) {
                    throw new AssertionError("history " + history + ": " + at
                            + " — reopen #" + (open + 1) + " must succeed; a burnt residue sequence"
                            + " number is a legal namespace gap: " + e.getMessage(), e);
                }
                try {
                    check(s2, expected, history, at + " reopen #" + (open + 1));
                    s2.verify();
                } finally {
                    close(s2);
                }
                images++;
            }
        }
    }

    /**
     * The create-crash residue shapes of table H, all on the highest sequence number: a
     * zero-length file (H1), a header shorter than 28 bytes (H2), and a full-length header whose
     * CRC does not check out (H3). All three are ordinary crash artifacts that R2 must sweep.
     */
    private static final byte[][] RESIDUE_SHAPES = {
            new byte[0],
            new byte[WalSegmentSet.SEG_HDR - 5],
            corruptedHeader(),
    };

    private static byte[] corruptedHeader() {
        byte[] h = WalTestKit.segmentHeader(1);   // seq is irrelevant: the CRC is what fails
        h[3] ^= 0x5A;                             // flip a magic byte, leave headerCrc stale
        return h;
    }

    // ---------------------------------------------------------------- the full sweep

    /**
     * Phase B's sampling claims that between the class boundaries the rule does not change. This
     * asserts it instead, byte by byte, over histories chosen to cover the shapes: a plain
     * multi-segment log, one whose tail is a {@code 'K'} mark, and one holding a cross-segment
     * {@code T_APPEND} delta whose base image lives in an earlier segment.
     */
    @Test public void the_full_byte_by_byte_truncation_sweep_of_the_active_segment()
            throws IOException {
        List<List<String>> shapes = List.of(
                List.of("putC", "updateC0", "updateC0"),
                List.of("putC", "updateC0", "checkpoint"),
                List.of("putC", "appendC0", "appendC0"),
                List.of("putC", "putC", "deleteC1"),
                List.of("putC", "checkpoint", "appendC0"),
                List.of("putC", "putStaged", "checkpoint"));
        int total = 0;
        for (List<String> shape : shapes) {
            Model m = new Model();
            File f = newFile();
            Map<Long, byte[]> preCleaning = new LinkedHashMap<>();
            StoreWAL s = open(f);
            try {
                for (String op : shape) {
                    if (op.equals("checkpoint") || op.equals("clean"))
                        preCleaning = snapshotSegments(f);
                    s = apply(s, m, f, op);
                }
            } finally {
                close(s);
            }
            byte[][] image = readSegments(f);
            long[] seqs = segmentSeqs(f);
            int top = image.length - 1;
            List<Integer> all = new ArrayList<>();
            for (int len = WalSegmentSet.SEG_HDR; len <= image[top].length; len++) all.add(len);
            phaseB(f, shape, m, image, seqs, all, preCleaning);
            total += all.size();
            wipe(f);
        }
        assertTrue("the sweep must cover a real range of offsets, got " + total, total >= 60);
    }

    // ---------------------------------------------------------------- image helpers

    private static long[] segmentSeqs(File base) {
        File[] fs = WalTestKit.segments(base);
        long[] seqs = new long[fs.length];
        for (int i = 0; i < fs.length; i++) seqs[i] = seqOf(fs[i]);
        return seqs;
    }

    private static long seqOf(File segment) {
        String n = segment.getName();
        return Long.parseUnsignedLong(n.substring(n.length() - 16), 16);
    }

    private static byte[][] readSegments(File base) {
        File[] fs = WalTestKit.segments(base);
        byte[][] out = new byte[fs.length][];
        for (int i = 0; i < fs.length; i++) out[i] = WalTestKit.read(fs[i]);
        return out;
    }

    private static Map<Long, byte[]> snapshotSegments(File base) {
        Map<Long, byte[]> out = new LinkedHashMap<>();
        for (File s : WalTestKit.segments(base)) out.put(seqOf(s), WalTestKit.read(s));
        return out;
    }

    /** Resets the directory to exactly {@code image}, removing whatever a previous probe left. */
    private static void restore(File base, byte[][] image, long[] seqs) {
        for (File s : WalTestKit.segments(base)) s.delete();
        new File(base.getPath() + ".lock").delete();
        for (int i = 0; i < image.length; i++)
            WalTestKit.write(WalTestKit.segment(base, seqs[i]), image[i]);
    }

    /**
     * The highest {@code cleanedThroughSeq} attested by a {@code 'K'} in this image, or 0 when
     * there is none. Deliberately a separate, dumb reimplementation of the scan: sharing
     * {@link StoreWAL}'s would make the oracle agree with the code under test by construction.
     */
    private static long newestMark(byte[][] image) {
        long mark = 0;
        for (byte[] seg : image) {
            int count = WalTestKit.sectionCount(seg);
            for (int i = 0; i < count; i++) {
                int off = WalTestKit.sectionOffset(seg, i);
                if (WalTestKit.tagOf(seg, off) != 'K') continue;
                if (WalTestKit.bodyLen(seg, off) != 16) continue;
                mark = Math.max(mark, WalSegmentSet.be64(seg, off + StoreWAL.SEC_HDR));
            }
        }
        return mark;
    }

    // ---------------------------------------------------------------- history execution

    private StoreWAL open(File f) {
        StoreWAL s = new StoreWAL(f);
        s.setMinLogBytes(0);            // cleaning happens only when a history says so
        s.setSegmentBytes(ONE_SECTION_PER_SEGMENT);
        return s;
    }

    private StoreWAL apply(StoreWAL s, Model m, File f, String op) {
        switch (op) {
            case "putC", "putStaged" -> {
                byte[] v = value(m.visible.size(), "p");
                m.visible.put(s.put(v, Fixtures.RAW), v);
                if (op.equals("putC")) {
                    s.commit();
                    m.commit();
                }
            }
            case "checkpoint" -> s.checkpoint();
            case "clean" -> s.testCleanOldestSegment();
            case "rollback" -> {
                s.rollback();
                m.discardStaged();
            }
            default -> {
                int idx = op.charAt(op.length() - 1) - '0';
                long r = recidAt(m, idx);
                if (op.startsWith("updateC")) {
                    // The ordinal keeps every committed state DISTINCT. With a fixed value per
                    // recid, [putC, updateC0, updateC0] produced two identical committed states,
                    // distinctStates() collapsed them, and "one distinct state back" then reached
                    // TWO commits back — so a recovery losing two sections would have passed
                    // phase B on exactly those histories.
                    byte[] v = value(idx, "u" + m.committedHistory.size());
                    s.updateWithHeadroom(r, v, Fixtures.RAW, 64);
                    m.visible.put(r, v);
                } else if (op.startsWith("appendC")) {
                    byte[] d = value(idx, "a");
                    if (s.append(r, d, 0, d.length) != StoreDelta.REFUSED) {
                        byte[] cur = m.visible.get(r);
                        m.visible.put(r, cur == null ? d.clone() : concat(cur, d));
                    }
                } else if (op.startsWith("deleteC")) {
                    s.delete(r, Fixtures.RAW);
                    m.visible.remove(r);
                } else {
                    throw new AssertionError("unknown op " + op);
                }
                s.commit();
                m.commit();
            }
        }
        return s;
    }

    // ---------------------------------------------------------------- assertions

    private void check(StoreWAL s, Map<Long, byte[]> expected, List<String> history, String at) {
        String why = mismatch(s, expected);
        if (why != null) throw new AssertionError("history " + history + " at " + at + ": " + why);
    }

    /** Phase B's weaker oracle: one of the tolerable states, and nothing else. */
    private void checkOneOf(StoreWAL s, List<Map<Long, byte[]>> tolerable,
                            List<String> history, String at) {
        List<String> whys = new ArrayList<>();
        for (Map<Long, byte[]> candidate : tolerable) {
            String why = mismatch(s, candidate);
            if (why == null) return;
            whys.add(why);
        }
        throw new AssertionError("history " + history + " at " + at
                + ": the recovered state matches neither the newest committed state nor the one"
                + " before it " + whys);
    }

    /** Null when the store's visible state equals {@code expected}, else why not. */
    private static String mismatch(StoreWAL s, Map<Long, byte[]> expected) {
        for (Map.Entry<Long, byte[]> e : expected.entrySet()) {
            byte[] got;
            try {
                got = s.get(e.getKey(), Fixtures.RAW);
            } catch (DBException.GetVoid v) {
                return "recid " + e.getKey() + " vanished";
            }
            if (e.getValue() == null) {
                if (got != null) return "recid " + e.getKey() + " should have null content";
            } else if (!Arrays.equals(e.getValue(), got)) {
                return "recid " + e.getKey() + " content differs";
            }
        }
        long max = 0;
        for (long r : expected.keySet()) max = Math.max(max, r);
        for (long r = 1; r <= max + 2; r++) {
            if (expected.containsKey(r)) continue;
            try {
                s.get(r, Fixtures.RAW);
                return "recid " + r + " should be void";
            } catch (DBException.GetVoid ok) { /* as expected */ }
        }
        return null;
    }

    // ---------------------------------------------------------------- small helpers

    private static long recidAt(Model m, int idx) {
        int i = 0;
        for (long r : m.visible.keySet()) {
            if (i++ == idx) return r;
        }
        throw new AssertionError("no recid at index " + idx);
    }

    private static byte[] value(int seed, String kind) {
        return (kind + seed + "-0123456789").getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] r = new byte[a.length + b.length];
        System.arraycopy(a, 0, r, 0, a.length);
        System.arraycopy(b, 0, r, a.length, b.length);
        return r;
    }

    private static void close(StoreWAL s) {
        if (s == null) return;
        try {
            s.close();
        } catch (RuntimeException ignored) { /* already closed / failed closed */ }
    }

    private File newFile() throws IOException {
        File f = TmpFiles.tempFile("mapdb-segenum", ".wal");
        f.delete();
        files.add(f);
        return f;
    }

    private static void wipe(File f) {
        f.delete();
        WalTestKit.deleteStore(f);
    }

    /** Guards the sweep against silently losing an op, exactly as the §4.2 enumerator does. */
    @Test public void the_alphabet_is_what_the_sweep_claims_it_is() throws IOException {
        assertEquals(List.of("putC", "putStaged", "checkpoint"),
                alphabetAfter(new ArrayList<>()));
        // `clean` appears only once something sits below the active segment. One committed
        // section does not put it there: at one section per segment the roll happens on the NEXT
        // append, so after `putC` there is still a single segment holding it.
        assertEquals(List.of("putC", "putStaged", "updateC0", "appendC0", "deleteC0", "checkpoint"),
                alphabetAfter(new ArrayList<>(List.of("putC"))));
        assertEquals(List.of("putC", "putStaged", "updateC0", "appendC0", "deleteC0", "checkpoint",
                        "clean"),
                alphabetAfter(new ArrayList<>(List.of("putC", "updateC0"))));
        // rollback appears only where there is something staged to roll back; putStaged writes
        // no section, so there is still nothing to clean
        assertEquals(List.of("putC", "putStaged", "updateC0", "appendC0", "deleteC0", "checkpoint",
                        "rollback"),
                alphabetAfter(new ArrayList<>(List.of("putStaged"))));
    }

    /** One section per segment is the premise of the whole sweep; assert it, do not assume it. */
    @Test public void the_minimum_segment_size_really_does_put_one_section_per_segment()
            throws IOException {
        File f = newFile();
        StoreWAL s = open(f);
        try {
            for (int i = 0; i < 4; i++) {
                s.put(Fixtures.payload(i, 1, 8), Fixtures.RAW);
                s.commit();
            }
        } finally {
            close(s);
        }
        File[] segs = WalTestKit.segments(f);
        assertEquals("four commits must land in four segments", 4, segs.length);
        for (File seg : segs) {
            assertEquals("segment " + seg.getName() + " must hold exactly one section",
                    1, WalTestKit.sectionCount(WalTestKit.read(seg)));
        }
    }
}
