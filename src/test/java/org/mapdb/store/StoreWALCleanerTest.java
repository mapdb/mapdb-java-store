package org.mapdb.store;

import org.junit.After;
import org.junit.Test;
import org.mapdb.DBException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The INCREMENTAL cleaner: retire the oldest segment by re-emitting only what that
 * segment still owns, in ticks bounded by a {@link MaintenanceBudget}. Crash-matrix rows C4, C5,
 * C10, C11, C13 and C14, plus W10.
 *
 * <p>{@link StoreWALCheckpointTest} drives the same machinery with its budget set to "everything"
 * — one cycle retiring the whole log — and that arrangement hides most of what matters here. A
 * whole-log clean re-emits every record, so it can never under-re-emit; it retires the segment
 * holding the previous mark, so an older mark never survives; and it leaves a retained set that is
 * a checkpoint image with nothing else in it. <b>Every test below is about a PARTIAL
 * retirement</b>, which is the state the step-2 stand-in could not produce and the one all of R4
 * has to accept.
 *
 * <p>Segments are held at their minimum size throughout, so "the oldest segment" is a single
 * section and a handful of commits is a multi-segment log.
 */
public class StoreWALCleanerTest {

    /** One section per segment: the smallest legal value, so every append rolls over. */
    private static final long ONE_SECTION_PER_SEGMENT = WalSegmentSet.SEG_HDR + StoreWAL.SEC_HDR;

    private final List<File> files = new ArrayList<>();

    private File newFile() {
        try {
            File f = Files.createTempFile("mapdb5-wal-cleaner", ".wal").toFile();
            f.delete();
            files.add(f);
            return f;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @After public void cleanup() {
        StoreWAL.testSetDirectorySync(null);
        StoreWAL.testDropRecidFromPublish = 0;
        for (File f : files) WalTestKit.deleteStore(f);
        files.clear();
    }

    private StoreWAL open(File f) {
        StoreWAL s = new StoreWAL(f);
        s.setMinLogBytes(0);                 // cleaning happens only where a test asks for it
        s.setSegmentBytes(ONE_SECTION_PER_SEGMENT);
        return s;
    }

    private static TreeMap<Long, byte[]> snapshot(Store s) {
        TreeMap<Long, byte[]> snap = new TreeMap<>();
        var it = s.getAllRecids();
        while (it.hasNext()) {
            long r = it.nextLong();
            snap.put(r, s.get(r, Fixtures.RAW));
        }
        return snap;
    }

    private static void assertState(Store s, Map<Long, byte[]> expected, String what) {
        TreeMap<Long, byte[]> actual = snapshot(s);
        assertEquals(what + ": recid set differs", expected.keySet(), actual.keySet());
        for (Long r : expected.keySet())
            assertArrayEquals(what + ": content differs at recid=" + r, expected.get(r), actual.get(r));
    }

    /** Reopens {@code f} and asserts the committed state survived exactly. */
    private void assertReopensAs(File f, Map<Long, byte[]> expected, String what) {
        StoreWAL s = new StoreWAL(f);
        try {
            assertState(s, expected, what);
            s.verify();
        } finally {
            s.close();
        }
    }

    // ================= the shape the incremental cleaner exists for =================

    /**
     * A partial retirement leaves a log that is not a checkpoint image: ordinary {@code 'S'}
     * sections stay above the retired segment, alongside the cleaner's {@code 'C'} and {@code 'K'}.
     */
    @Test public void retiring_the_oldest_segment_preserves_state_and_leaves_a_mixed_log() {
        File f = newFile();
        Map<Long, byte[]> oracle;
        StoreWAL s = open(f);
        try {
            byte[] va = Fixtures.payload(1, 1, 40);
            long a = s.put(va, Fixtures.RAW);
            s.commit();                                   // seg 1: 'S' — a's only image
            long b = s.put(Fixtures.payload(2, 2, 40), Fixtures.RAW);
            s.commit();                                   // seg 2: 'S'
            s.update(b, Fixtures.payload(2, 9, 40), Fixtures.RAW);
            s.commit();                                   // seg 3: 'S'
            oracle = snapshot(s);

            long lowestBefore = lowestSeq(f);
            assertTrue("history must span several segments", WalTestKit.segments(f).length >= 3);
            assertTrue("a cycle must be available", s.testCleanOldestSegment());
            assertTrue("the oldest segment must be gone", lowestSeq(f) > lowestBefore);

            assertState(s, oracle, "in process");
            // a's only self-contained entry WAS in the retired segment, so it had to be re-emitted;
            // b's was not, so its plain commit section is still there.
            assertTrue("the cleaner must have re-emitted a", hasTag(f, 'C'));
            assertTrue("the retained log still holds plain commit sections", hasTag(f, 'S'));
            assertTrue("and the mark the cycle wrote", hasTag(f, 'K'));
            assertArrayEquals(va, s.get(a, Fixtures.RAW));
        } finally {
            s.close();
        }
        assertReopensAs(f, oracle, "after reopen");
    }

    /**
     * C10 — write a base, seal the segment, append a delta above it, then retire the segment the
     * base lived in. The re-emitted image must carry base+delta, because the delta's own base is
     * about to stop existing. This is the §4.1 data-loss case: the v1 draft's C10 would have passed
     * while shipping it.
     */
    @Test public void a_base_in_the_retired_segment_is_re_emitted_with_its_delta_folded_in() {
        File f = newFile();
        Map<Long, byte[]> oracle;
        StoreWAL s = open(f);
        try {
            byte[] base = Fixtures.payload(3, 3, 32);
            long r = s.put(base, Fixtures.RAW);
            // put sizes the record exactly, so an append would be REFUSED for want of capacity;
            // the headroom is what makes this a DELTA history rather than a rewrite one.
            s.updateWithHeadroom(r, base, Fixtures.RAW, 64);
            s.commit();                                   // seg 1: 'S' holding r's base
            s.put(Fixtures.payload(4, 4, 16), Fixtures.RAW);
            s.commit();                                   // seg 2: seals seg 1
            byte[] delta = Fixtures.payload(5, 5, 8);
            s.append(r, delta, 0, delta.length);
            s.commit();                                   // seg 3: 'T_APPEND' citing the base in seg 1
            oracle = snapshot(s);
            byte[] merged = concat(base, delta);
            assertArrayEquals(merged, oracle.get(r));

            assertTrue(s.testCleanOldestSegment());        // retires seg 1 — the base's home
            assertState(s, oracle, "in process");
        } finally {
            s.close();
        }
        // The reopened log holds a stranded T_APPEND (its base is gone, so replay SKIPS it) and
        // the 'C' image that supersedes it. Getting either half wrong shows up here.
        assertReopensAs(f, oracle, "after reopen");
    }

    /** C11 — a cycle sliced by a tiny budget and resumed under a different one converges. */
    @Test public void a_cycle_resumed_under_a_different_budget_converges_to_the_same_state() {
        File f = newFile();
        Map<Long, byte[]> oracle;
        StoreWAL s = open(f);
        try {
            // Six records in ONE transaction: one section, one segment, six candidates when it is
            // retired — so a one-record budget really does stop the cycle mid-publish.
            for (int i = 0; i < 6; i++) s.put(Fixtures.payload(i, i, 48), Fixtures.RAW);
            s.commit();                                   // seg 1: 'S' with six T_RECORDs
            s.put(Fixtures.payload(9, 9, 16), Fixtures.RAW);
            s.commit();                                   // seg 2: seals seg 1
            oracle = snapshot(s);

            assertTrue(s.testStartCleanCycle());
            MaintenanceBudget oneRecord = new MaintenanceBudget(0, 1, 0, 0, true);
            MaintenanceBudget oneByte = new MaintenanceBudget(0, 0, 1, 0, true);
            int ticks = 0;
            while (s.testCleaning()) {
                s.testCleanTick(ticks % 2 == 0 ? oneRecord : oneByte);
                if (++ticks > 10_000) fail("cycle did not converge in " + ticks + " ticks");
            }
            assertTrue("a sliced cycle must take several ticks, took " + ticks, ticks > 3);
            assertState(s, oracle, "in process");
        } finally {
            s.close();
        }
        assertReopensAs(f, oracle, "after reopen");
    }

    /**
     * C13 — a commit landing between a cycle's candidate selection and its publish must not be
     * overwritten by a stale image. The cleaner would append that image at a HIGHER LSN than the
     * commit, so replay would resurrect the superseded value; the recheck against
     * {@code stateLsn} at publish time is what prevents it.
     */
    @Test public void an_update_racing_the_re_emission_is_not_resurrected() {
        File f = newFile();
        Map<Long, byte[]> oracle;
        StoreWAL s = open(f);
        try {
            byte[] stale = Fixtures.payload(1, 1, 40);
            long r = s.put(stale, Fixtures.RAW);
            s.commit();                                   // seg 1: r's only image
            s.put(Fixtures.payload(2, 2, 16), Fixtures.RAW);
            s.commit();                                   // seg 2: seals seg 1

            assertTrue(s.testStartCleanCycle());           // r is selected as a candidate
            byte[] fresh = Fixtures.payload(1, 7, 40);
            s.update(r, fresh, Fixtures.RAW);
            s.commit();                                   // r is re-homed above the boundary
            oracle = snapshot(s);

            while (s.testCleaning()) s.testCleanTick(new MaintenanceBudget(0, 0, 0, 0, true));
            assertArrayEquals("the stale image must not have been published",
                    fresh, s.get(r, Fixtures.RAW));
        } finally {
            s.close();
        }
        assertReopensAs(f, oracle, "after reopen");
    }

    /** C13's other half: a delete racing the re-emission must not bring the record back. */
    @Test public void a_delete_racing_the_re_emission_is_not_resurrected() {
        File f = newFile();
        Map<Long, byte[]> oracle;
        StoreWAL s = open(f);
        try {
            long r = s.put(Fixtures.payload(1, 1, 40), Fixtures.RAW);
            s.commit();
            s.put(Fixtures.payload(2, 2, 16), Fixtures.RAW);
            s.commit();

            assertTrue(s.testStartCleanCycle());
            s.delete(r, Fixtures.RAW);
            s.commit();                                   // stateLsn[r] is now ABSENT
            oracle = snapshot(s);
            assertFalse("precondition: r is gone", oracle.containsKey(r));

            while (s.testCleaning()) s.testCleanTick(new MaintenanceBudget(0, 0, 0, 0, true));
            assertFalse("a deleted recid must not be re-emitted", snapshot(s).containsKey(r));
        } finally {
            s.close();
        }
        assertReopensAs(f, oracle, "after reopen");
    }

    // ================= W10: verify before marking =================

    /**
     * W10, proved non-vacuous. An under-re-emission — the class §4.2's skip audit structurally
     * cannot see, because a record wholly contained in the retiring range leaves no entry to skip
     * — must be caught BEFORE the mark, while the evidence still exists. The injector drops one
     * recid from the publish loop, which is exactly that defect.
     */
    @Test public void W10_refuses_to_mark_when_a_record_was_not_re_emitted() {
        File f = newFile();
        Map<Long, byte[]> oracle;
        StoreWAL s = open(f);
        try {
            byte[] va = Fixtures.payload(1, 1, 40);
            long a = s.put(va, Fixtures.RAW);
            s.commit();                                   // seg 1: a's only image
            s.put(Fixtures.payload(2, 2, 16), Fixtures.RAW);
            s.commit();                                   // seg 2: seals seg 1
            oracle = snapshot(s);

            StoreWAL.testDropRecidFromPublish = a;
            try {
                s.testCleanOldestSegment();
                fail("W10 must refuse a cycle that dropped recid " + a);
            } catch (DBException expected) {
                assertTrue("message must name the recid and the boundary: " + expected.getMessage(),
                        expected.getMessage().contains("recid " + a));
            } finally {
                StoreWAL.testDropRecidFromPublish = 0;
            }

            // Nothing was destroyed: the mark was never written, so the segment holding a is
            // still there and the store is intact. That is the whole point of verifying FIRST.
            assertEquals("the retiring segment must still exist", 1, lowestSeq(f));

            // And a RETRY must refuse again. The scan cursor advances before the visitor runs, so
            // without a rewind the next tick would resume past the entry that refused, find the
            // remainder clean, and write the mark — turning the refusal into the silent loss it
            // exists to prevent.
            StoreWAL.testDropRecidFromPublish = a;
            try {
                for (int i = 0; i < 20 && s.testCleaning(); i++)
                    s.testCleanTick(new MaintenanceBudget(0, 0, 0, 0, true));
                fail("a retried cycle must refuse again, not resume past the failure");
            } catch (DBException expected) {
                assertTrue(expected.getMessage().contains("recid " + a));
            } finally {
                StoreWAL.testDropRecidFromPublish = 0;
            }
            assertEquals("still nothing unlinked", 1, lowestSeq(f));
            assertState(s, oracle, "after a refused cycle");
        } finally {
            s.close();
        }
        assertReopensAs(f, oracle, "after reopen");
    }

    /**
     * Q5 §7's conformance row verbatim: "a doctored cleaner that drops a {@code T_PREALLOC} — the
     * mark must not be written and the segment must not be unlinked". A preallocation is the case
     * W10 exists for and the one the {@code recState} predicate could not express: it carries no
     * content, so the content base says nothing about it, and losing it makes an acknowledged
     * {@code preallocate()} void on reopen — after which {@code rebuildFreeRecids} hands the recid
     * out again and a later allocation collides with it.
     */
    @Test public void W10_fires_when_the_dropped_entry_is_a_prealloc() {
        File f = newFile();
        StoreWAL s = open(f);
        try {
            long p = s.preallocate();
            s.commit();                                   // seg 1: 'T_PREALLOC', p's only entry
            s.put(Fixtures.payload(2, 2, 16), Fixtures.RAW);
            s.commit();                                   // seg 2: seals seg 1
            long segmentsBefore = WalTestKit.segments(f).length;

            StoreWAL.testDropRecidFromPublish = p;
            try {
                s.testCleanOldestSegment();
                fail("W10 must refuse a cycle that dropped the prealloc at recid " + p);
            } catch (DBException expected) {
                assertTrue(expected.getMessage().contains("recid " + p));
            } finally {
                StoreWAL.testDropRecidFromPublish = 0;
            }
            assertEquals("the segment must not be unlinked",
                    segmentsBefore, WalTestKit.segments(f).length);
            assertEquals("nor may the mark have been written", 1, lowestSeq(f));
        } finally {
            s.close();
        }
    }

    /**
     * Q5 §7: "prealloc-and-null survive cleaning". Both are self-contained states with no content,
     * so they are exactly what a cleaner keyed on content images would drop — and the damage is
     * silent until the recid is reissued.
     */
    @Test public void prealloc_and_null_records_survive_an_incremental_retirement() {
        File f = newFile();
        long p, n;
        StoreWAL s = open(f);
        try {
            p = s.preallocate();
            n = s.put(null, Fixtures.RAW);
            s.commit();                                   // seg 1: their only entries
            s.put(Fixtures.payload(2, 2, 16), Fixtures.RAW);
            s.commit();                                   // seg 2: seals seg 1
            assertTrue(s.testCleanOldestSegment());
        } finally {
            s.close();
        }
        StoreWAL s2 = new StoreWAL(f);
        try {
            assertNull("the null-content record must still be null", s2.get(n, Fixtures.RAW));
            assertNull("the preallocated recid must still read as null", s2.get(p, Fixtures.RAW));
            // The real damage of a dropped prealloc is silent: the recid falls back onto the free
            // list and the next allocation collides with it.
            for (int i = 0; i < 4; i++) {
                long fresh = s2.preallocate();
                assertNotEquals("a surviving recid was handed out again", p, fresh);
                assertNotEquals("a surviving recid was handed out again", n, fresh);
            }
            s2.verify();
        } finally {
            s2.close();
        }
    }

    /**
     * Q5 §7: "headroom, clean, append, reopen" — the re-emitted image must carry the record's
     * EXACT committed capacity, not a recomputed exact fit. Get that wrong and the append below
     * comes back {@code REFUSED} on a history that was legal before the clean.
     */
    @Test public void a_re_emitted_image_keeps_the_exact_capacity_an_append_needs() {
        File f = newFile();
        Map<Long, byte[]> oracle;
        StoreWAL s = open(f);
        try {
            byte[] base = Fixtures.payload(1, 1, 24);
            long r = s.put(base, Fixtures.RAW);
            s.updateWithHeadroom(r, base, Fixtures.RAW, 128);
            s.commit();                                   // seg 1: r, with room to grow
            s.put(Fixtures.payload(2, 2, 16), Fixtures.RAW);
            s.commit();                                   // seg 2: seals seg 1

            assertTrue(s.testCleanOldestSegment());        // r is re-emitted from seg 1

            byte[] delta = Fixtures.payload(3, 3, 8);
            assertNotEquals("the re-emitted capacity lost the headroom",
                    StoreDelta.REFUSED, s.append(r, delta, 0, delta.length));
            s.commit();
            oracle = snapshot(s);
            assertArrayEquals(concat(base, delta), oracle.get(r));
        } finally {
            s.close();
        }
        assertReopensAs(f, oracle, "after reopen");
    }

    /** And with the injector off, the same history cleans — so the test above is not tautological. */
    @Test public void the_same_history_cleans_cleanly_without_the_injector() {
        File f = newFile();
        Map<Long, byte[]> oracle;
        StoreWAL s = open(f);
        try {
            long a = s.put(Fixtures.payload(1, 1, 40), Fixtures.RAW);
            s.commit();
            s.put(Fixtures.payload(2, 2, 16), Fixtures.RAW);
            s.commit();
            oracle = snapshot(s);
            assertTrue(s.testCleanOldestSegment());
            assertState(s, oracle, "in process");
        } finally {
            s.close();
        }
        assertReopensAs(f, oracle, "after reopen");
    }

    /**
     * <b>A commit pays ONE bounded slice, and the bound has to be small enough to be one.</b>
     *
     * <p>Step 4's measurement found the {@code FOREGROUND_BUDGET} constant set to
     * {@code (4096 records, 8 MiB, maxNanos = 0)} — a "bound" larger than the whole log on the
     * benchmark, with the time field, the one that names the quantity step 3 exists to bound,
     * switched off entirely. The incremental cleaner was consequently <em>worse</em> on every
     * pause percentile than the whole-store checkpoint it replaced: 326 commits over 1 ms against
     * the checkpoint's 70, on a 350k-commit run. Six review rounds went past it because it is a
     * value, not a rule.
     *
     * <p>This pins the byte half <b>deterministically</b>, by sampling {@link
     * StoreWAL#cleanerBytesWritten()} around each commit rather than by timing anything — a
     * wall-clock assertion would be flaky under CI load and would pin the machine, not the policy.
     * The workload makes the retiring segment ~100% live (every record is written once and never
     * updated, so nothing in it is garbage), which is what forces a tick to have far more
     * re-emission available than one slice may take; a garbage-heavy log would pass with any
     * budget, since there would be nothing to re-emit.
     *
     * <p><b>What this does NOT pin</b>, stated rather than left to be overread: the {@code
     * maxNanos} half is <em>argued</em>, not pinned. Its value cannot be asserted without either
     * timing the commit path or reading the constant back, and the first is flaky while the second
     * only restates the source. What is pinned is that a single commit's cleaning cannot re-emit
     * an unbounded slice of a live segment, which is the failure that was actually measured.
     */
    @Test public void a_commits_cleaning_slice_is_bounded_well_below_a_segment() {
        File f = newFile();
        // Big segments and an all-live population, so one retiring segment offers MEGABYTES of
        // re-emission to any tick willing to take it.
        final int recordBytes = 4096;
        final int records = 2_000;              // ~8 MiB of live data, ~2 segments' worth
        StoreWAL s = new StoreWAL(f);
        try {
            s.setSegmentBytes(4L << 20);
            s.setMinLogBytes(8L << 20);
            s.setSpaceAmplification(1);
            for (int i = 0; i < records; i++) {
                s.put(Fixtures.payload(i, i & 0x7f, recordBytes), Fixtures.RAW);
                if ((i + 1) % 100 == 0) s.commit();
            }
            s.commit();

            // Now push the log past the trigger, and watch what each commit is charged for
            // cleaning. ONE record is rewritten over and over, so the log grows without the live
            // set growing: the new bytes are garbage, the OLD segments are not. The rewrite has to
            // be big enough to outrun the trigger, which is `max(8 MiB, 1 × liveDataBytes)` and so
            // sits just ABOVE the live set — a small probe never reaches it (that is what the
            // vacuity guard below caught first time round).
            long probe = s.put(Fixtures.payload(-1, 1, recordBytes), Fixtures.RAW);
            s.commit();

            long worstSlice = 0;
            int cleaningCommits = 0;
            for (int i = 0; i < 2_500; i++) {
                s.update(probe, Fixtures.payload(-1, i & 0x7f, recordBytes), Fixtures.RAW);
                long before = s.cleanerBytesWritten();
                s.commit();
                long slice = s.cleanerBytesWritten() - before;
                if (slice > 0) cleaningCommits++;
                worstSlice = Math.max(worstSlice, slice);
            }

            // Non-vacuity: the cleaner must actually have run, or the bound below is about nothing.
            assertTrue("no commit did any cleaning — the trigger never fired, so this test is "
                    + "vacuous rather than passing", cleaningCommits > 0);

            // The budget allows 512 KiB per tick; one oversize unit may overshoot it, so the
            // assertion is at 1 MiB. The shipped-before value (8 MiB) let a tick take a whole
            // 4 MiB segment's live content at once, which is what this separates from.
            assertTrue("a single commit re-emitted " + worstSlice + " bytes of cleaning; a "
                            + "commit's slice must stay well under a segment (" + (4 << 20) + ")",
                    worstSlice <= (1 << 20));
        } finally {
            s.close();
        }
    }

    // ================= crash images around a cycle =================

    /**
     * C4 — a crash between the cleaner's forced images and its {@code 'K'}. The retiring segment is
     * still there and no mark authorizes its removal, so replay covers both it and the images that
     * duplicate it; the state must be identical and cleaning simply re-runs.
     */
    @Test public void a_crash_between_the_images_and_the_mark_replays_to_the_same_state() {
        File f = newFile();
        Map<Long, byte[]> oracle;
        Map<Long, byte[]> before;
        StoreWAL s = open(f);
        try {
            long a = s.put(Fixtures.payload(1, 1, 40), Fixtures.RAW);
            s.commit();
            s.put(Fixtures.payload(2, 2, 24), Fixtures.RAW);
            s.commit();
            oracle = snapshot(s);
            before = segmentImage(f);
            assertTrue(s.testCleanOldestSegment());
        } finally {
            s.close();
        }
        // Reconstruct the pre-mark image: the retired segments put back, and every 'K' the cycle
        // wrote cut away. What remains is exactly "the images are forced, the mark is not".
        Map<Long, byte[]> crashed = segmentImage(f);
        crashed.putAll(before);
        stripMarks(crashed);
        restore(f, crashed);
        assertReopensAs(f, oracle, "crash before the mark");

        // …and cleaning re-runs from that image without complaint.
        StoreWAL s2 = open(f);
        try {
            assertTrue(s2.testCleanOldestSegment());
            assertState(s2, oracle, "after re-running the cycle");
        } finally {
            s2.close();
        }
        assertReopensAs(f, oracle, "after the re-run");
    }

    /**
     * C5 — a crash between the forced {@code 'K'} and the completion of its unlink, over an
     * ARBITRARY subset of the retired segments: syscall order says nothing about the order removals
     * persist, so an interior gap is a legitimate image (K5/K9).
     */
    @Test public void a_crash_mid_unlink_leaves_orphans_that_the_next_open_retires() {
        File f = newFile();
        Map<Long, byte[]> oracle;
        Map<Long, byte[]> before;
        StoreWAL s = open(f);
        try {
            for (int i = 0; i < 4; i++) {
                s.put(Fixtures.payload(i, i, 32), Fixtures.RAW);
                s.commit();
            }
            oracle = snapshot(s);
            before = segmentImage(f);
            s.checkpoint();                               // retires everything below the fresh segment
        } finally {
            s.close();
        }
        Map<Long, byte[]> after = segmentImage(f);
        List<Long> retired = new ArrayList<>(before.keySet());
        retired.removeAll(after.keySet());
        assertTrue("the checkpoint must have retired something", retired.size() >= 2);

        for (int subset = 1; subset < (1 << retired.size()); subset++) {
            Map<Long, byte[]> image = new LinkedHashMap<>(after);
            for (int i = 0; i < retired.size(); i++) {
                if ((subset & (1 << i)) != 0) image.put(retired.get(i), before.get(retired.get(i)));
            }
            restore(f, image);
            assertReopensAs(f, oracle, "crash mid-unlink, surviving orphan subset " + subset);
            assertEquals("the next open must retire every orphan the mark authorized",
                    after.keySet(), segmentImage(f).keySet());
        }
    }

    /**
     * C14 — a crash at every point the cleaner reaches between ticks: after a segment create, after
     * each forced {@code 'C'}, after a rollover, after the {@code 'K'}, and after the unlink and
     * its directory fsync. Every one must recover the last commit exactly.
     *
     * <p>One record per tick makes each tick one section, so the snapshots land on section
     * boundaries — which is where a crash can actually leave the log. The one point NOT sampled
     * this way is between the mark and the unlink, because both happen inside the closing tick;
     * that is C5 above.
     */
    @Test public void a_crash_after_every_cleaner_tick_recovers_the_last_commit() {
        File f = newFile();
        Map<Long, byte[]> oracle;
        List<Map<Long, byte[]>> perTick = new ArrayList<>();
        StoreWAL s = open(f);
        try {
            for (int i = 0; i < 5; i++) s.put(Fixtures.payload(i, i, 40), Fixtures.RAW);
            s.commit();
            s.put(Fixtures.payload(9, 9, 16), Fixtures.RAW);
            s.commit();
            oracle = snapshot(s);

            assertTrue(s.testStartCleanCycle());
            perTick.add(segmentImage(f));
            MaintenanceBudget oneRecord = new MaintenanceBudget(0, 1, 0, 0, true);
            while (s.testCleaning()) {
                s.testCleanTick(oneRecord);
                perTick.add(segmentImage(f));
            }
        } finally {
            s.close();
        }
        assertTrue("the cycle must have taken several ticks", perTick.size() > 3);
        for (int i = 0; i < perTick.size(); i++) {
            restore(f, perTick.get(i));
            assertReopensAs(f, oracle, "crash after cleaner tick " + i + " of " + perTick.size());
        }
    }

    // ================= budget honouring =================

    /** A no-fsync budget must not half-write a cycle: every unit ends in a forced section. */
    @Test public void a_budget_that_forbids_fsync_does_nothing() {
        File f = newFile();
        StoreWAL s = open(f);
        try {
            for (int i = 0; i < 3; i++) {
                s.put(Fixtures.payload(i, i, 32), Fixtures.RAW);
                s.commit();
            }
            // An OPEN cycle, so a -1 here can only mean "the budget forbade it" and not "nothing
            // was due" — the trigger does not fire on a store this small (see cleaningDue).
            assertTrue(s.testStartCleanCycle());
            long before = WalTestKit.logBytes(f);
            assertEquals(-1, s.maintenanceCleanStep(new MaintenanceBudget(0, 0, 0, 0, false)));
            assertEquals("nothing may be written", before, WalTestKit.logBytes(f));
            assertTrue("and the cycle must be left exactly as it was", s.testCleaning());
        } finally {
            s.close();
        }
    }

    /** C15 — maintenance defers behind a delta transaction's LSN reservation, never interleaves. */
    @Test public void maintenance_defers_while_a_delta_transaction_holds_a_reservation() {
        File f = newFile();
        StoreWAL s = open(f);
        try {
            long r = s.put(new byte[64], Fixtures.RAW);
            s.commit();
            s.put(new byte[64], Fixtures.RAW);
            s.commit();
            assertTrue(s.testStartCleanCycle());

            s.append(r, (out, lsn) -> out.write(new byte[8]));   // takes the reservation
            assertEquals("deferred, not interleaved", -1,
                    s.maintenanceCleanStep(MaintenanceBudget.defaultBudget()));
            s.commit();
            assertTrue("and runs once the reservation is released",
                    s.maintenanceCleanStep(MaintenanceBudget.defaultBudget()) >= 0);
        } finally {
            s.close();
        }
    }

    // ================= the background task, end to end =================

    /**
     * The R6 rewiring, exercised through the real task with <b>no test-opened cycle</b>. Every other
     * test here calls {@code testStartCleanCycle} first, and that blind spot hid a defect that made
     * the whole rewiring inert: {@code maintenanceCleanStep} checked the trigger but never STARTED
     * a cycle, so it fell through to a tick with nothing to advance, returned zero bytes, and the
     * executor read that as progress-with-more-to-come and re-ran it immediately — a maintenance
     * thread spinning on a core while the log grew untouched.
     *
     * <p>Driven by calling {@link WalCleanerTask#run} directly rather than through
     * {@link MaintenanceExecutor}, so the assertions are about what the task returns rather than
     * about thread timing.
     */
    @Test public void the_background_task_retires_a_segment_on_its_own() {
        File f = newFile();
        StoreWAL s = new StoreWAL(f);
        byte[] last;
        try {
            s.setMinLogBytes(0);                          // no inline cleaning while we build
            s.setSegmentBytes(256 << 10);
            byte[] v = Fixtures.payload(1, 0, 60_000);
            long r = s.put(v, Fixtures.RAW);
            s.commit();
            last = v;
            for (int i = 1; i <= 80; i++) {
                last = Fixtures.payload(1, i, 60_000);
                s.update(r, last, Fixtures.RAW);
                s.commit();
            }
            long grown = WalTestKit.logBytes(f);
            int segmentsBefore = WalTestKit.segments(f).length;
            assertTrue("precondition: the log must exceed the store footprint",
                    grown > s.getCurrentSize());

            s.setMinLogBytes(1);
            s.setSpaceAmplification(1);                   // now the trigger is live
            assertFalse("precondition: no cycle may be open", s.testCleaning());

            WalCleanerTask task = new WalCleanerTask(s);
            MaintenanceResult res = null;
            int ticks = 0;
            while (ticks++ < 5_000) {
                res = task.run(MaintenanceBudget.defaultBudget());
                if (!res.madeProgress) break;
            }
            assertFalse("the task must reach a quiet state, not spin", res.madeProgress);
            assertTrue("the task must have retired segments on its own",
                    WalTestKit.segments(f).length < segmentsBefore);
            assertTrue("and shrunk the log", WalTestKit.logBytes(f) < grown);
            assertTrue("and reported what it cost", s.cleanerBytesWritten() > 0);
            assertTrue("and what it reclaimed", s.cleanerBytesRetired() > 0);
            assertArrayEquals("with the committed state intact", last, s.get(r, Fixtures.RAW));

            // A run with nothing left to do must report NO progress, or the executor never sleeps.
            assertFalse("an idle run must not claim progress",
                    task.run(MaintenanceBudget.defaultBudget()).madeProgress);

            // Note the floor is deliberately NOT asserted clear here: an episode outlives a quiet
            // trigger, so that a workload hovering around the target does not pay a fresh seal
            // every few commits. What must not outlive its episode is the floor across a
            // CONFIGURATION change, which `changing_a_cleaning_setting_clears_the_episodes_floor`
            // pins, and across a completed episode, which `endEpisodeLocked` handles.
        } finally {
            s.close();
        }
        StoreWAL s2 = new StoreWAL(f);
        try {
            assertArrayEquals(last, s2.get(1, Fixtures.RAW));
            s2.verify();
        } finally {
            s2.close();
        }
    }

    /**
     * Changing either cleaning setting must take the episode's floor with it, not just the
     * exhaustion latch. The floor is the previous episode's high-water mark, so leaving it behind
     * makes the very next attempt give up immediately on "lowest >= floor" — raising the target
     * would then not resume cleaning at all.
     */
    @Test public void changing_a_cleaning_setting_clears_the_episodes_floor() {
        File f = newFile();
        StoreWAL s = new StoreWAL(f);
        try {
            s.setSegmentBytes(256 << 10);
            s.setMinLogBytes(0);
            long r = s.put(Fixtures.payload(1, 0, 60_000), Fixtures.RAW);
            s.commit();
            for (int i = 1; i <= 80; i++) {
                s.update(r, Fixtures.payload(1, i, 60_000), Fixtures.RAW);
                s.commit();
            }
            s.setMinLogBytes(1);
            s.setSpaceAmplification(1);

            // One tiny tick: enough to open an episode (and so set its floor) without finishing it.
            assertTrue(s.maintenanceCleanStep(new MaintenanceBudget(0, 1, 0, 0, true)) >= 0);
            assertNotEquals("precondition: an episode must be in progress", 0, s.testCleanFloorSeq());

            s.setSpaceAmplification(2);
            assertEquals("the floor must go with the target", 0, s.testCleanFloorSeq());
            s.setMinLogBytes(2);
            assertEquals("both setters, not just one", 0, s.testCleanFloorSeq());
        } finally {
            s.close();
        }
    }

    /**
     * The tick's work unit is an ENTRY, not a section. A section may be arbitrarily large — a
     * rollover happens only at a section boundary, so one commit can exceed {@code segmentBytes} on
     * its own — so a per-section unit would have held the WAL write lock for an unbounded time,
     * which is the pause step 3 exists to remove.
     */
    @Test public void a_budget_of_one_record_advances_by_one_entry_inside_a_huge_section() {
        final int records = 200;
        File f = newFile();
        StoreWAL s = open(f);
        try {
            for (int i = 0; i < records; i++) s.put(Fixtures.payload(i, i, 24), Fixtures.RAW);
            s.commit();                                   // ONE section holding 200 entries
            s.put(Fixtures.payload(999, 999, 8), Fixtures.RAW);
            s.commit();                                   // seals it
            Map<Long, byte[]> oracle = snapshot(s);

            assertTrue(s.testStartCleanCycle());
            MaintenanceBudget oneRecord = new MaintenanceBudget(0, 1, 0, 0, true);
            int ticks = 0;
            while (s.testCleaning()) {
                s.testCleanTick(oneRecord);
                if (++ticks > 10_000) fail("cycle did not converge");
            }
            // Publish walks all 200 entries and W10 walks them again, one per tick. A per-SECTION
            // unit would have finished the same cycle in a handful.
            assertTrue("a one-record budget must not swallow a whole section, took " + ticks,
                    ticks >= 2 * records);
            assertState(s, oracle, "in process");
        } finally {
            s.close();
        }
    }

    /**
     * Steady state: once the log is compacted, further commits must not each pay to rewrite it.
     * The foreground loop stops at the segment that was active when it began, and reaching that
     * floor while the trigger is still live is LATCHED rather than retried — otherwise every
     * subsequent commit pays an O(live store) clean and the whole-store pause returns by the back
     * door.
     */
    @Test public void steady_state_commits_do_not_re_clean_the_whole_log() {
        File f = newFile();
        StoreWAL s = new StoreWAL(f);
        try {
            s.setSegmentBytes(256 << 10);
            s.setMinLogBytes(1);
            s.setSpaceAmplification(1);                   // as aggressive as the setter allows
            long r = s.put(Fixtures.payload(1, 0, 60_000), Fixtures.RAW);
            s.commit();
            for (int i = 1; i <= 60; i++) {
                s.update(r, Fixtures.payload(1, i, 60_000), Fixtures.RAW);
                s.commit();
            }
            long writtenAfterWarmup = s.cleanerBytesWritten();

            // 40 more commits of ~60 KiB each. Cleaning may legitimately re-emit and mark, but it
            // must stay proportional to the traffic — not to the store size once per commit.
            for (int i = 61; i <= 100; i++) {
                s.update(r, Fixtures.payload(1, i, 60_000), Fixtures.RAW);
                s.commit();
            }
            long cleanerCost = s.cleanerBytesWritten() - writtenAfterWarmup;
            long traffic = 40L * 60_000;
            assertTrue("cleaning cost " + cleanerCost + " bytes for " + traffic
                            + " bytes of traffic: the whole-log rewrite is back",
                    cleanerCost < 4 * traffic);
            assertFalse("nothing here is an unachievable target", s.cleaningExhausted());
        } finally {
            s.close();
        }
    }

    /**
     * The cached log size must equal the brute-force sum at every point — across appends,
     * rollovers, a partial retirement and a whole-log clean. It is consulted on every commit, so
     * it is cached; a drifting counter would mis-trigger cleaning in either direction, silently.
     */
    @Test public void logBytes_stays_exact_across_rollover_and_cleaning() {
        File f = newFile();
        StoreWAL s = open(f);
        try {
            assertLogBytesExact(s, "fresh store");
            for (int i = 0; i < 8; i++) {
                s.put(Fixtures.payload(i, i, 32), Fixtures.RAW);
                s.commit();
                assertLogBytesExact(s, "after commit " + i);
            }
            assertTrue(s.testCleanOldestSegment());
            assertLogBytesExact(s, "after a partial retirement");
            assertTrue(s.testCleanOldestSegment());
            assertLogBytesExact(s, "after a second retirement");
            s.checkpoint();
            assertLogBytesExact(s, "after a whole-log clean");
            s.put(Fixtures.payload(9, 9, 32), Fixtures.RAW);
            s.commit();
            assertLogBytesExact(s, "after cleaning, then committing again");
        } finally {
            s.close();
        }
        StoreWAL s2 = new StoreWAL(f);
        try {
            assertLogBytesExact(s2, "after reopen");
        } finally {
            s2.close();
        }
    }

    private static void assertLogBytesExact(StoreWAL s, String what) {
        assertEquals("cached log size drifted " + what,
                s.testLogBytesExact(), s.testLogBytes());
    }

    /**
     * The episode floor must be a segment the episode itself created. Using the PRE-EXISTING active
     * segment declared exhaustion while that segment still held mostly superseded sections — a log
     * an explicit {@code checkpoint()} then compacted by a large factor, so the target was never
     * unachievable and the cleaner had simply declined to finish.
     */
    @Test public void an_episode_does_not_declare_exhaustion_with_a_reclaimable_segment_left() {
        File f = newFile();
        StoreWAL s = new StoreWAL(f);
        try {
            s.setSegmentBytes(4 << 20);
            s.setMinLogBytes(0);
            long r = s.put(Fixtures.payload(1, 0, 60_000), Fixtures.RAW);
            s.commit();
            for (int i = 1; i <= 111; i++) {
                s.update(r, Fixtures.payload(1, i, 60_000), Fixtures.RAW);
                s.commit();
            }
            s.setMinLogBytes(1);
            s.setSpaceAmplification(1);

            WalCleanerTask task = new WalCleanerTask(s);
            int ticks = 0;
            while (ticks++ < 20_000 && task.run(MaintenanceBudget.defaultBudget()).madeProgress) { }
            long automatic = WalTestKit.logBytes(f);

            // The honest test of "unachievable": if an explicit full clean can shrink the same log
            // materially, the automatic cleaner stopped early and the latch was a false positive.
            s.checkpoint();
            long explicit = WalTestKit.logBytes(f);
            assertTrue("automatic cleaning left " + automatic + " bytes that checkpoint() reduced to "
                            + explicit + ": the episode gave up with reclaimable log left",
                    automatic < 4 * Math.max(explicit, 1));
            assertFalse("and it must not have called that exhaustion", s.cleaningExhausted());
        } finally {
            s.close();
        }
    }

    /**
     * A pure-garbage workload — one record overwritten forever — must stay bounded near its target,
     * must never declare itself unachievable, and must not churn cleaning EPISODES.
     *
     * <p>The last of those is the one with teeth. An episode seals the active segment to establish
     * its floor, so an episode that ends on every dip below the target pays a create and a directory
     * fsync every few commits: on this workload that was 75 episodes in 220 commits before the floor
     * was made to outlive a quiet trigger, and 7 after.
     *
     * <p><b>What this does NOT pin</b>, stated so the coverage is not overread: an earlier finding —
     * that latching on "the episode reached its floor" suppresses cleaning after a SUCCESSFUL
     * episode — needs an episode long enough that traffic arriving above its floor keeps the log
     * over target when it completes, and on this small denominator a completed episode almost
     * always lands under target and never latches either way. That case is
     * {@link #a_successful_episode_is_not_an_unachievable_ratio()}.
     */
    @Test public void a_pure_garbage_workload_stays_bounded_without_churning_episodes() {
        File f = newFile();
        StoreWAL s = new StoreWAL(f);
        try {
            s.setSegmentBytes(128 << 10);
            s.setMinLogBytes(0);
            long r = s.put(Fixtures.payload(1, 0, 60_000), Fixtures.RAW);
            s.commit();
            for (int i = 1; i <= 40; i++) {
                s.update(r, Fixtures.payload(1, i, 60_000), Fixtures.RAW);
                s.commit();
            }
            s.setMinLogBytes(1);
            s.setSpaceAmplification(1);
            long target = s.getCurrentSize();          // factor 1, and minLogBytes is out of the way

            long worst = 0;
            boolean everExhausted = false;
            int episodes = 0;
            boolean inEpisode = s.testCleanFloorSeq() != 0;
            final int commits = 220;
            for (int i = 41; i <= 40 + commits; i++) {
                s.update(r, Fixtures.payload(1, i, 60_000), Fixtures.RAW);
                s.commit();
                worst = Math.max(worst, WalTestKit.logBytes(f));
                everExhausted |= s.cleaningExhausted();
                boolean now = s.testCleanFloorSeq() != 0;
                if (now && !inEpisode) episodes++;
                inEpisode = now;
            }
            assertFalse("a workload that is pure garbage is never 'unachievable'", everExhausted);
            assertTrue("log high-water " + worst + " against a target of " + target,
                    worst < 4 * target);
            assertTrue("an episode seals a segment, so " + episodes + " episodes in " + commits
                            + " commits is churn: the floor must outlive a quiet trigger",
                    episodes < commits / 8);
        } finally {
            s.close();
        }
    }

    /**
     * An episode that COMPLETES while the trigger is still live is a success, not a terminal. It
     * says the whole log was rewritten once and that traffic arrived while it ran — which is what
     * the NEXT episode is for. Latching there instead, as "reached its floor" did, reports a ratio
     * cleaning is meeting as unachievable and, worse, suppresses the inline cleaning P7 owes a
     * writer above the hard ceiling.
     *
     * <p>The workload is engineered to reach exactly that state, which the single-record one
     * cannot: a log an order of magnitude over its target at the start, so an episode spans many
     * segments and therefore many commits, and enough garbage per commit that the log is still due
     * when the episode ends. The counter proves it got there — nine of ten episodes here end with
     * the trigger live. The cleaner meanwhile retires four orders of magnitude more than it
     * re-emits, so "unachievable" is not a defensible reading of this history under any policy.
     */
    @Test public void a_successful_episode_is_not_an_unachievable_ratio() {
        final int recs = 30, size = 30_000, perCommit = 10, warm = 12, commits = 120;
        File f = newFile();
        StoreWAL s = new StoreWAL(f);
        try {
            s.setSegmentBytes(256 << 10);
            s.setMinLogBytes(0);
            long[] r = new long[recs];
            for (int i = 0; i < recs; i++) r[i] = s.put(Fixtures.payload(i, 0, size), Fixtures.RAW);
            s.commit();
            for (int w = 1; w <= warm; w++) {                   // ~11 MB of log against ~2 MB live
                for (int i = 0; i < recs; i++) s.update(r[i], Fixtures.payload(i, w, size), Fixtures.RAW);
                s.commit();
            }
            s.setMinLogBytes(1);
            s.setSpaceAmplification(1);

            boolean everExhausted = false, inEpisode = s.testCleanFloorSeq() != 0;
            int endedWhileDue = 0;
            for (int c = 1; c <= commits; c++) {
                for (int k = 0; k < perCommit; k++) {
                    int i = (c * perCommit + k) % recs;
                    s.update(r[i], Fixtures.payload(i, warm + c, size), Fixtures.RAW);
                }
                s.commit();
                everExhausted |= s.cleaningExhausted();
                boolean now = s.testCleanFloorSeq() != 0;
                if (!now && inEpisode && WalTestKit.logBytes(f) > s.getCurrentSize()) endedWhileDue++;
                inEpisode = now;
            }
            assertFalse("the cleaner retired " + s.cleanerBytesRetired() + " bytes to re-emit "
                            + s.cleanerBytesWritten() + ": that ratio is being MET, not missed",
                    everExhausted);
            assertTrue("no episode ended with the trigger still live, so this workload never"
                            + " reached the state under test", endedWhileDue > 0);
        } finally {
            s.close();
        }
    }

    /**
     * The futility latch releases on EITHER side of the ratio. Waiting only for the log to grow
     * wedges the other direction: a large delete drops the live footprint and makes the images
     * already in the log reclaimable <em>without the log moving at all</em>, so a store that deleted
     * most of its data and then went quiet would stay latched forever.
     *
     * <p>It must also NOT release on any drop, which is what forced a threshold: the target is the
     * inner store's footprint and one ordinary 60 KB update moves it by ~160 bytes as the allocator
     * reuses extents. The first half of this test is that commit; a latch that releases there is not
     * a latch.
     *
     * <p>The latch is armed through a hook, because a genuinely futile episode is not reachable from
     * this API — see {@code testArmFutility}. What is pinned here is the release rule.
     */
    @Test public void a_material_target_drop_releases_the_futility_latch() {
        final int recs = 20, size = 60_000;
        File f = newFile();
        StoreWAL s = new StoreWAL(f);
        try {
            s.setSegmentBytes(128 << 10);
            s.setMinLogBytes(0);
            long[] r = new long[recs];
            for (int i = 0; i < recs; i++) r[i] = s.put(Fixtures.payload(i, 0, size), Fixtures.RAW);
            s.commit();
            for (int w = 1; w <= 6; w++) {
                for (int i = 0; i < recs; i++) s.update(r[i], Fixtures.payload(i, w, size), Fixtures.RAW);
                s.commit();
            }
            s.setMinLogBytes(1);
            s.setSpaceAmplification(1);
            assertTrue("the trigger must be live or a latch means nothing",
                    WalTestKit.logBytes(f) > s.testCleaningTarget());

            // A large re-emitted count keeps the staleness rule out of this test: what is under
            // test here is the TARGET rule, and `a_state_only_churn_releases_the_futility_latch`
            // covers the other one.
            s.testArmFutility(1_000);
            assertTrue("the hook must arm through the real path", s.cleaningExhausted());

            s.update(r[0], Fixtures.payload(0, 99, size), Fixtures.RAW);
            s.commit();
            assertTrue("allocator jitter is not a smaller store: the latch must survive an ordinary"
                            + " commit", s.cleaningExhausted());

            long before = WalTestKit.logBytes(f);
            for (int i = 1; i < recs; i++) s.delete(r[i], Fixtures.RAW);
            s.commit();                                 // the log GREW; the store shrank by ~40%
            assertFalse("a delete makes the log reclaimable without the log moving: the latch must"
                            + " release on the target, not only on growth", s.cleaningExhausted());

            s.update(r[0], Fixtures.payload(0, 100, size), Fixtures.RAW);
            s.commit();
            assertTrue("released, so cleaning must actually run: log was " + before + ", now "
                            + WalTestKit.logBytes(f), WalTestKit.logBytes(f) < before);
        } finally {
            s.close();
        }
    }

    /**
     * MINIMUM-SIZE segments must not put the cleaner on a treadmill. A cycle retires one segment and
     * pays for one mark, and at the minimum segment size the mark costs more than the segment holds:
     * retiring ~61 bytes while appending ~107 means one-at-a-time cleaning <em>grows</em> the log
     * forever, on a log a single wide pass collapses. Worse, the episode that would notice never
     * completes — 20,000 segments at one per cycle — so no terminal is ever reached and
     * {@code cleaningExhausted()} stays false while the log runs away.
     *
     * <p>Measured on the code before the fix: segments 20,000 → 22,400 over 1,200 commits, the log
     * up 170 KB, and the cleaner retiring 76,673 file bytes for 82,673 written — a net LOSS that the
     * old section-byte accounting still reported as progress. After: the segment count is flat and
     * the cleaner gains about 30% of what it writes.
     *
     * <p><b>Two further fixes are argued, not pinned</b>, for the same reachability reason
     * that keeps the latch itself out of reach: the width cap is a construction bound
     * ({@code Math.min(..., CYCLE_WIDTH_CAP)}), and the terminal's qualification — a cycle must be as
     * wide as the range its episode STARTED with, not merely as wide as what is left — only bites on
     * a futile episode, which this API cannot produce.
     *
     * <p>This test pins four of the five parts of that fix — reverting the file-byte accounting, the
     * width doubling, or halving-instead-of-resetting each fails it, and reverting the staleness rule
     * fails {@link #a_state_only_churn_releases_the_futility_latch()}. <b>The one it does not pin is
     * the "worth its writes" threshold</b>: with the width search in place, cycles here gain either
     * ~30% or nothing, so replacing {@code gain > written/8} with {@code gain > 0} passes. The
     * threshold guards the case where the WIDEST cycle gains epsilon — the same disease at maximum
     * width — and that is argued, not pinned.
     */
    @Test public void minimum_size_segments_do_not_put_cleaning_on_a_treadmill() {
        final int build = 20_000, drive = 600;
        File f = newFile();
        StoreWAL s = new StoreWAL(f);
        try {
            s.setSegmentBytes(ONE_SECTION_PER_SEGMENT);
            s.setMinLogBytes(0);                       // no cleaning while building
            for (int i = 0; i < build; i++) { s.preallocate(); s.commit(); }

            s.setMinLogBytes(1);
            s.setSpaceAmplification(1);
            long target = s.testCleaningTarget();
            long logBefore = WalTestKit.logBytes(f);
            int segsBefore = WalTestKit.segments(f).length;
            assertTrue("the trigger must be live: log " + logBefore + " target " + target,
                    logBefore > target);

            for (int i = 0; i < drive; i++) { s.preallocate(); s.commit(); }

            long gained = s.cleanerBytesRetired() - s.cleanerBytesWritten();
            int segsAfter = WalTestKit.segments(f).length;
            assertTrue("cleaning must pay for itself: retired " + s.cleanerBytesRetired()
                            + " for " + s.cleanerBytesWritten() + " written",
                    gained > s.cleanerBytesWritten() / 8);
            assertTrue("the segment count must not climb with the commit count: " + segsBefore
                            + " -> " + segsAfter + " over " + drive + " commits",
                    segsAfter - segsBefore < drive / 4);
            assertFalse("cleaning is working here, so nothing is unachievable",
                    s.cleaningExhausted());
        } finally {
            s.close();
        }
    }

    /**
     * A latch is a proof about the log as it stood, and commits invalidate proofs. A mass delete of
     * records that own no data extent obsoletes every image in the log while moving neither the log's
     * size nor the target — so neither the growth rule nor the target rule can see it, and the store
     * would stay latched on a log that is now entirely garbage. The third release rule is the
     * store's own state-change count: retry once per live-set's worth of commits, which bounds the
     * wasted work to one pass per that many.
     */
    @Test public void a_state_only_churn_releases_the_futility_latch() {
        final int recs = 24;
        File f = newFile();
        StoreWAL s = new StoreWAL(f);
        try {
            s.setSegmentBytes(128 << 10);
            s.setMinLogBytes(0);
            long[] r = new long[recs];
            for (int i = 0; i < recs; i++) r[i] = s.put(Fixtures.payload(i, 0, 60_000), Fixtures.RAW);
            s.commit();
            for (int w = 1; w <= 4; w++) {
                for (int i = 0; i < recs; i++) s.update(r[i], Fixtures.payload(i, w, 60_000), Fixtures.RAW);
                s.commit();
            }
            s.setMinLogBytes(1);
            s.setSpaceAmplification(1);
            assertTrue("the trigger must be live or a latch means nothing",
                    WalTestKit.logBytes(f) > s.testCleaningTarget());

            s.testArmFutility(recs);            // as if a whole-range episode had gained nothing
            assertTrue(s.cleaningExhausted());
            long logAtArming = WalTestKit.logBytes(f), targetAtArming = s.testCleaningTarget();

            // PREALLOCATE, one per commit: a self-contained entry that owns no data extent, so the
            // footprint does not fall and the log grows by a few dozen bytes rather than a target's
            // worth. Neither of the other two rules can fire on this traffic.
            for (int i = 0; i < recs; i++) { s.preallocate(); s.commit(); }
            assertTrue("this traffic must not trip the GROWTH rule: " + logAtArming + " -> "
                            + WalTestKit.logBytes(f) + " against a target of " + targetAtArming,
                    WalTestKit.logBytes(f) < logAtArming + targetAtArming);
            assertTrue("nor the TARGET rule: " + targetAtArming + " -> " + s.testCleaningTarget(),
                    s.testCleaningTarget() > targetAtArming - (targetAtArming >> 3));
            assertFalse("a live-set's worth of state changes must invalidate the proof",
                    s.cleaningExhausted());
        } finally {
            s.close();
        }
    }

    /**
     * A commit that crosses the trigger pays ONE bounded slice, not a whole pass. This method runs
     * inside commit's write-lock hold and cannot release it, so looping there is one uninterrupted
     * hold for the entire pass — the per-tick budget would bound an internal iteration while every
     * reader and writer waited for all of them.
     */
    @Test public void a_commit_crossing_the_trigger_pays_one_slice_not_the_whole_pass() {
        File f = newFile();
        StoreWAL s = new StoreWAL(f);
        try {
            s.setSegmentBytes(64 << 10);
            s.setMinLogBytes(0);
            // One record rewritten many times: the LIVE data stays flat while the log grows, and
            // at 64 KiB segments each 60 KiB section lands in its own segment. So a whole pass is
            // dozens of cycles, while the trigger (a ratio against the flat footprint) is crossed.
            long r = s.put(Fixtures.payload(1, 0, 60_000), Fixtures.RAW);
            s.commit();
            for (int i = 1; i <= 60; i++) {
                s.update(r, Fixtures.payload(1, i, 60_000), Fixtures.RAW);
                s.commit();
            }
            int segmentsBefore = WalTestKit.segments(f).length;
            assertTrue("precondition: many segments, was " + segmentsBefore, segmentsBefore >= 8);

            s.setMinLogBytes(1);
            s.setSpaceAmplification(1);
            // One commit crosses the trigger. It may seal, open a cycle and run ONE slice; it must
            // not retire the whole log. (The hard ceiling is far above: the log is nowhere near
            // twice its target, since the target is the ~2 MiB store footprint.)
            s.update(r, Fixtures.payload(1, 999, 60_000), Fixtures.RAW);
            s.commit();
            int segmentsAfter = WalTestKit.segments(f).length;
            assertTrue("one commit retired " + (segmentsBefore - segmentsAfter) + " segments of "
                            + segmentsBefore + ": that is a whole pass, not a slice",
                    segmentsBefore - segmentsAfter <= 2);
        } finally {
            s.close();
        }
    }

    // ================= helpers over the on-disk image =================

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] out = Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }

    private static long seqOf(File seg) {
        String name = seg.getName();
        return Long.parseUnsignedLong(name.substring(name.length() - 16), 16);
    }

    private static long lowestSeq(File f) { return seqOf(WalTestKit.segments(f)[0]); }

    /** seq -> bytes for every segment currently on disk. */
    private static Map<Long, byte[]> segmentImage(File f) {
        Map<Long, byte[]> image = new LinkedHashMap<>();
        for (File seg : WalTestKit.segments(f)) image.put(seqOf(seg), WalTestKit.read(seg));
        return image;
    }

    /** Makes the directory hold exactly {@code image}, and nothing else. */
    private static void restore(File f, Map<Long, byte[]> image) {
        for (File seg : WalTestKit.segments(f)) {
            if (!image.containsKey(seqOf(seg))) seg.delete();
        }
        for (Map.Entry<Long, byte[]> e : image.entrySet())
            WalTestKit.write(WalTestKit.segment(f, e.getKey()), e.getValue());
    }

    /** Truncates each segment at its first {@code 'K'}, leaving the images that precede it. */
    private static void stripMarks(Map<Long, byte[]> image) {
        for (Map.Entry<Long, byte[]> e : image.entrySet()) {
            byte[] seg = e.getValue();
            int count = WalTestKit.sectionCount(seg);
            for (int i = 0; i < count; i++) {
                int off = WalTestKit.sectionOffset(seg, i);
                if (WalTestKit.tagOf(seg, off) == 'K') {
                    e.setValue(Arrays.copyOf(seg, off));
                    break;
                }
            }
        }
    }

    private static boolean hasTag(File f, char tag) {
        for (File seg : WalTestKit.segments(f)) {
            byte[] bytes = WalTestKit.read(seg);
            int count = WalTestKit.sectionCount(bytes);
            for (int i = 0; i < count; i++) {
                if (WalTestKit.tagOf(bytes, WalTestKit.sectionOffset(bytes, i)) == tag) return true;
            }
        }
        return false;
    }
}
