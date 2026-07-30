package org.mapdb.store;

import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * <b>What the cleaner's two scans COST, as an invariant rather than as a stopwatch reading.</b>
 *
 * <p>Both phases of a cleaning cycle walk the retiring range: phase 1 to find candidates, phase 2
 * (W10) to verify them. The property that has to hold is that each walk reads the range a bounded
 * number of times — bounded by its BYTES, through {@link StoreWAL#SCAN_BUF}-sized windows — and in
 * particular that its cost does not scale with the number of SECTIONS the range happens to be cut
 * into. The two are wildly different numbers for a real log: a store committed one operation at a
 * time writes one section per commit, so a 34 MB log held ~33 500 of them.
 *
 * <p>That invariant was false. {@code WalIn.reset} dropped the read window, and the scan called it
 * at every section boundary — once for the separately-read section header, once more entering the
 * body — so the walk issued <em>two syscalls per section</em> and re-read bytes it already held.
 * Measured on the {@code checkpoint()} benchmark, the two scans made ~148 000 reads to walk 34 MB
 * of log, against the ~16 000 the byte count calls for; {@code checkpoint()} spent essentially all
 * of its time there.
 *
 * <h2>Why this is a read COUNT and not a duration</h2>
 *
 * A timing assertion on a shared machine measures the machine. The count is exact, deterministic
 * and reproducible, it is the quantity the defect was actually about, and it fails by an order of
 * magnitude rather than by a margin — so it needs no tolerance tuned to any particular host. This
 * is the same move tier 2 made for the ordering rules: a property no artifact records needs an
 * instrument built for it, which is what {@link StoreWAL.ReadCount} is.
 *
 * <h2>Non-vacuity</h2>
 *
 * Verified by reverting the fix — restoring {@code reset(bodyStart, c.bodyEnd)} at the section
 * boundary and the separate positional read of the section header. Both tests then fail, by an
 * order of magnitude rather than by a margin: {@code reads=6000} against a bound of 301 for the
 * whole-log clean, {@code reads=1218} against 220 for the budgeted partial one.
 *
 * <p>The revert check also earned its keep here rather than merely confirming what was expected.
 * On its first draft the second test <em>passed</em> with the fix reverted: left at the default
 * segment size the whole log lands in one segment, so there is nothing below the active one to
 * retire, the cycle never opens and a scan that never runs reads nothing. A test that measures a
 * cost is vacuous unless something checks that the cost was incurred, which is why it now asserts
 * that the cycle opened, that it took more than one tick, and that the reads are non-zero.
 */
public class StoreWALCleanerScanCostTest {

    private final List<File> files = new ArrayList<>();

    private File newFile() {
        try {
            File f = Files.createTempFile("mapdb-wal-scancost", ".wal").toFile();
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

    /**
     * A log of many small sections is read by the byte, not by the section.
     *
     * <p>The workload is one commit per record, which is the shape that makes the two numbers
     * diverge: every commit seals a section, so sections ≈ commits while the range's byte size
     * stays small. {@code checkpoint()} then retires all of it in one cycle, so both scans walk the
     * whole range exactly once each and the reads they issue are attributable with no sampling.
     */
    @Test
    public void the_two_scans_read_the_range_by_the_byte_and_not_by_the_section() {
        final int sections = 3_000;
        File f = newFile();
        StoreWAL s = new StoreWAL(f);
        try {
            s.setMinLogBytes(0);          // no automatic cleaning: the checkpoint below is the only one
            for (int i = 0; i < sections; i++) {
                s.put(Fixtures.payload(i, i, 16), Fixtures.RAW);
                s.commit();               // one section per commit — the point of the workload
            }
            long rangeBytes = s.testLogBytes();
            long before = s.testCleanerScanReads();
            s.checkpoint();
            long reads = s.testCleanerScanReads() - before;

            // Two passes over the range, in SCAN_BUF windows, with slack for the windows a seek
            // past the end of one legitimately drops and for the segment boundaries.
            long byBytes = 3 * (2 * rangeBytes / StoreWAL.SCAN_BUF) + 64;
            String detail = " (sections=" + sections + " rangeBytes=" + rangeBytes
                    + " reads=" + reads + " byBytes bound=" + byBytes + ")";

            // Same guard as the partial-clean test below, for the same reason: `reads < sections`
            // is satisfied by a scan that never ran.
            assertTrue("the checkpoint scanned nothing" + detail, reads > 0);
            assertTrue("the cleaner's scans read the range by the byte, not once per section"
                    + detail, reads <= byBytes);
            // Stated separately and deliberately redundantly: whatever the byte bound works out to
            // on some future default, the walk must not be per-section. This is the assertion that
            // names the defect.
            assertTrue("the cleaner's scans issued at least one read per section" + detail,
                    reads < sections);
        } finally {
            s.close();
        }
    }

    /**
     * The same bound holds for a PARTIAL retirement driven in small budgeted ticks — the path a
     * commit actually pays for, and the one where a per-section read count is charged to latency
     * rather than to an explicit {@code checkpoint()}.
     *
     * <p>Resumption is what makes this worth asserting separately: a tick that stops mid-range
     * leaves a cursor behind, and a resumed scan that re-dropped its window on every entry would
     * satisfy the test above (which never stops) while still reading per section here.
     */
    @Test
    public void a_budgeted_partial_clean_reads_the_range_by_the_byte_too() {
        final int sections = 2_000;
        File f = newFile();
        StoreWAL s = new StoreWAL(f);
        try {
            s.setMinLogBytes(0);
            // Several segments, so there IS something below the active one to retire. Left at the
            // default this test is silently vacuous: the whole log lands in one segment,
            // testStartCleanCycle refuses, and a scan that never runs reads nothing and passes.
            // (Caught by the revert check, which the first test failed and this one did not.)
            s.setSegmentBytes(32 << 10);
            for (int i = 0; i < sections; i++) {
                s.put(Fixtures.payload(i, i, 16), Fixtures.RAW);
                s.commit();
            }
            long logBytes = s.testLogBytes();
            long before = s.testCleanerScanReads();
            assertTrue("nothing below the active segment: this test would prove nothing",
                    s.testStartCleanCycle());
            // 64 records per tick, so the range is walked across dozens of separate ticks and
            // every resumption point is exercised.
            MaintenanceBudget small = new MaintenanceBudget(0, 64, 0, 0, true);
            int ticks = 0;
            while (s.testCleaning() && ticks++ < 100_000) s.testCleanTick(small);
            long reads = s.testCleanerScanReads() - before;

            // The retiring range is ONE segment, so the whole log is a generous over-estimate of
            // its bytes — which only makes the bound easier to satisfy and the failure below more
            // damning.
            long byBytes = 3 * (2 * logBytes / StoreWAL.SCAN_BUF) + 64;
            String detail = " (sections=" + sections + " logBytes=" + logBytes
                    + " reads=" + reads + " ticks=" + ticks + ")";
            assertTrue("the cycle closed without scanning anything" + detail, reads > 0);
            assertTrue("a budgeted partial clean must be resumable without re-reading" + detail,
                    ticks > 1);
            assertTrue("a budgeted partial clean read the range by the byte" + detail,
                    reads <= byBytes);
        } finally {
            s.close();
        }
    }
}
