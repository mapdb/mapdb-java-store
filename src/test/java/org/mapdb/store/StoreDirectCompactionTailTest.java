package org.mapdb.store;

import org.junit.After;
import org.junit.Test;
import org.mapdb.TmpFiles;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Compaction TAIL: index-page relocation and linked/oversize-chunk
 * relocation folded into the bounded incremental compactor. Covers correctness + reopen for both
 * new move kinds, crash-refuses-reopen for both, and bad-plan -> no-op (validators degrade a
 * corrupt plan instead of writing corruption). verify() is the double-use / lost-extent oracle.
 */
public class StoreDirectCompactionTailTest {

    static final long P = 1L << 20;
    private final List<File> files = new ArrayList<>();

    private File newFile() throws IOException {
        File f = TmpFiles.tempFile("mapdb-compact-tail", ".db");
        f.delete();
        files.add(f);
        return f;
    }

    @After public void cleanup() {
        StoreDirect.testCompactCrashHook = null;
        StoreDirect.testTamperMode = 0;
        for (File f : files) TmpFiles.delete(f);
        files.clear();
    }

    private static void assertGetVoid(Runnable r) {
        try { r.run(); fail("expected DBException.GetVoid"); }
        catch (org.mapdb.DBException.GetVoid expected) { /* ok */ }
    }

    // ---------- shared constructions ----------

    /**
     * Store whose TOP page is a relocatable index page (ordinal 0, addressed by the
     * header-checksum-protected ZERO_PAGE_LINK) with >=2 fully-free data pages below it. Returns the
     * recids: [dataRecid(D3, on a retained page), slotRecid(65529, whose index slot lives on the
     * moved index page)].
     */
    private long[] buildTopIndexPage(StoreDirect s, byte[] d3, byte[] small) {
        long r1 = s.put(Fixtures.payload(1, 1, 1_000_000), Fixtures.RAW); // page1
        long r2 = s.put(Fixtures.payload(2, 1, 1_000_000), Fixtures.RAW); // page2
        long r3 = s.put(d3, Fixtures.RAW);                                // page3
        for (int i = 0; i < 65525; i++) s.put(null, Fixtures.RAW);        // fill recids 4..65528
        long rs = s.put(small, Fixtures.RAW);                             // recid 65529 -> allocates index page (top)
        assertEquals("index page must be the single top page", 1, s.testIndexPages().length);
        s.delete(r1, Fixtures.RAW); // free page1
        s.delete(r2, Fixtures.RAW); // free page2
        return new long[]{r3, rs};
    }

    /**
     * Store with a ~2.5 MiB linked (3-chunk) record occupying the trailing pages and two fully-free
     * data pages below it. Returns the linked recid.
     */
    private long buildTrailingLinked(StoreDirect s, byte[] big) {
        long r1 = s.put(Fixtures.payload(1, 1, 1_000_000), Fixtures.RAW); // page1
        long r2 = s.put(Fixtures.payload(2, 1, 1_000_000), Fixtures.RAW); // page2
        long rl = s.put(big, Fixtures.RAW);                               // linked: chunks on pages 3,4,5
        s.delete(r1, Fixtures.RAW); // free page1
        s.delete(r2, Fixtures.RAW); // free page2
        return rl;
    }

    // ---------- index-page relocation ----------

    @Test public void index_page_move_correctness_and_reopen() throws IOException {
        File f = newFile();
        byte[] d3 = Fixtures.payload(3, 1, 1_000_000);
        byte[] small = Fixtures.payload(7, 7, 100);
        StoreDirect s = new StoreDirect(f);
        try {
            long[] r = buildTopIndexPage(s, d3, small);
            long r3 = r[0], rs = r[1];
            long fullTail = s.testFileTail();
            long oldIndexPage = s.testIndexPages()[0];
            s.verify();

            long reclaimed = s.compactIncremental();
            s.verify();
            assertTrue("relocated the index page", s.testIndexPageMovesApplied > 0);
            assertTrue("reclaimed trailing pages", reclaimed > 0);
            assertTrue("file tail shrank", s.testFileTail() < fullTail);
            assertTrue("index page moved to a lower offset", s.testIndexPages()[0] < oldIndexPage);
            // slot on the moved index page still resolves to identical content
            assertArrayEquals(small, s.get(rs, Fixtures.RAW));
            assertArrayEquals(d3, s.get(r3, Fixtures.RAW));
        } finally {
            s.close();
        }
        StoreDirect re = new StoreDirect(f);
        try {
            re.verify();
            assertArrayEquals(small, re.get(65529, Fixtures.RAW));
            assertArrayEquals(d3, re.get(3, Fixtures.RAW));
        } finally {
            re.close();
        }
    }

    // ---------- relocating MULTIPLE adjacent index pages in one step ----------

    @Test public void multi_adjacent_index_page_move_and_reopen() throws IOException {
        File f = newFile();
        StoreDirect s = new StoreDirect(f);
        try {
            List<Long> big = new ArrayList<>();
            for (int i = 0; i < 5; i++) big.add(s.put(Fixtures.payload(i, 1, 1_000_000), Fixtures.RAW)); // pages1..5
            for (int i = 0; i < 4; i++) s.delete(big.get(i), Fixtures.RAW); // free pages1..4; keep page5
            long keep = big.get(4);
            // force TWO index pages (ordinals 0 and 1) as the top two pages
            s.testEnsureIndexCapacity(65528 + 131070 + 1);
            assertEquals("two index pages", 2, s.testIndexPages().length);
            long[] oldPages = s.testIndexPages();
            long fullTail = s.testFileTail();
            s.verify();

            int reclaimed = s.compactStep(2); // drop both index pages in one step
            s.verify();
            assertEquals("relocated both index pages", 2, s.testIndexPageMovesApplied);
            assertEquals(2, reclaimed);
            assertTrue(s.testFileTail() < fullTail);
            long[] newPages = s.testIndexPages();
            assertEquals(2, newPages.length);
            assertTrue("both index pages moved lower", newPages[0] < oldPages[0] && newPages[1] < oldPages[1]);
            assertArrayEquals(Fixtures.payload(4, 1, 1_000_000), s.get(keep, Fixtures.RAW));
        } finally {
            s.close();
        }
        StoreDirect re = new StoreDirect(f);
        try { re.verify(); } finally { re.close(); }
    }

    // ---------- linked/oversize chunk relocation ----------

    @Test public void linked_chunk_move_correctness_and_reopen() throws IOException {
        File f = newFile();
        byte[] big = Fixtures.payload(9, 9, 2_500_000);
        StoreDirect s = new StoreDirect(f);
        final long rl;
        try {
            rl = buildTrailingLinked(s, big);
            long fullTail = s.testFileTail();
            s.verify();

            long reclaimed = s.compactIncremental();
            s.verify();
            assertTrue("relocated >=1 linked chunk", s.testLinkedMovesApplied > 0);
            assertTrue("reclaimed pages", reclaimed > 0);
            assertTrue("file tail shrank", s.testFileTail() < fullTail);
            // content identical and linked/oversize semantics preserved (never re-planed to plain)
            assertArrayEquals(big, s.get(rl, Fixtures.RAW));
            assertEquals("linked records take no appends", 0, s.capacityRemaining(rl));
            assertEquals("append on a linked record is refused", StoreDelta.REFUSED,
                    s.append(rl, new byte[]{1}, 0, 1));
            s.verify();
        } finally {
            s.close();
        }
        StoreDirect re = new StoreDirect(f);
        try {
            re.verify();
            assertArrayEquals(big, re.get(rl, Fixtures.RAW));
        } finally {
            re.close();
        }
    }

    // ---------- crash mid index-page move refuses reopen ----------

    @Test public void crash_during_index_page_move_refuses_reopen() throws IOException {
        File f = newFile();
        StoreDirect s = new StoreDirect(f);
        boolean crashed = false;
        try {
            buildTopIndexPage(s, Fixtures.payload(3, 1, 1_000_000), Fixtures.payload(7, 7, 100));
            s.commit(); // clean, valid on-disk state before the faulted step
            s.verify();

            StoreDirect.testCompactCrashHook = () -> { throw new RuntimeException("simulated crash"); };
            try {
                s.compactStep(1);
                fail("expected simulated crash (index-page move)");
            } catch (RuntimeException expected) {
                crashed = "simulated crash".equals(expected.getMessage());
                if (!crashed) throw expected;
            } finally {
                StoreDirect.testCompactCrashHook = null;
            }
            assertRefusesReopen(f);
        } finally {
            StoreDirect.testCompactCrashHook = null;
        }
        assertTrue(crashed);
    }

    // ---------- crash mid linked-chunk move refuses reopen ----------

    @Test public void crash_during_linked_move_refuses_reopen() throws IOException {
        File f = newFile();
        StoreDirect s = new StoreDirect(f);
        boolean crashed = false;
        try {
            buildTrailingLinked(s, Fixtures.payload(9, 9, 2_500_000));
            s.commit();
            s.verify();

            StoreDirect.testCompactCrashHook = () -> { throw new RuntimeException("simulated crash"); };
            try {
                s.compactStep(64); // relocates head+middle chunks, then crashes mid-step
                fail("expected simulated crash (linked-chunk move)");
            } catch (RuntimeException expected) {
                crashed = "simulated crash".equals(expected.getMessage());
                if (!crashed) throw expected;
            } finally {
                StoreDirect.testCompactCrashHook = null;
            }
            assertRefusesReopen(f);
        } finally {
            StoreDirect.testCompactCrashHook = null;
        }
        assertTrue(crashed);
    }

    private void assertRefusesReopen(File f) throws IOException {
        File snap = newFile();
        Files.write(snap.toPath(), Files.readAllBytes(f.toPath()));
        try {
            new StoreDirect(snap).close();
            fail("expected reopen of a mid-compaction crash to refuse");
        } catch (org.mapdb.DBException.DataCorruption expected) { /* refused: correct */ }
    }

    // ---------- a bad plan degrades to a NO-OP (never corruption) ----------

    @Test public void bad_plan_degrades_to_noop_universal() throws IOException {
        // Universal tamper injects an out-of-range free extent, so EVERY plan attempt fails
        // validateTiling -> compactStep can only return 0, leaving the store byte-identical.
        assertBadPlanNoop(1, /*linked*/ false);
    }

    @Test public void bad_plan_degrades_to_noop_index() throws IOException {
        assertBadPlanNoop(2, /*linked*/ false); // un-aligned index target -> validateIndexPagesAfterMove
    }

    @Test public void bad_plan_degrades_to_noop_linked() throws IOException {
        assertBadPlanNoop(3, /*linked*/ true);  // un-moved linked chunk -> validateLinkedChainsAfterMove
    }

    /**
     * With {@code tamperMode} active, EVERY plan that would relocate the tampered move kind is
     * rejected by the pre-write validators (that kind is never applied and the store never
     * corrupts); the store may still make unrelated progress. {@code universal} additionally
     * asserts a strict 0-page no-op (mode 1 rejects all plans). After clearing the tamper the store
     * compacts correctly and the previously-blocked move kind is applied.
     */
    private void assertBadPlanNoop(int tamperMode, boolean linked) throws IOException {
        boolean universal = tamperMode == 1;
        File f = newFile();
        StoreDirect s = new StoreDirect(f);
        try {
            byte[] probeVal;
            long probe;
            if (linked) {
                byte[] big = Fixtures.payload(9, 9, 2_500_000);
                probe = buildTrailingLinked(s, big);
                probeVal = big;
            } else {
                long[] r = buildTopIndexPage(s, Fixtures.payload(3, 1, 1_000_000), Fixtures.payload(7, 7, 100));
                probe = r[1];
                probeVal = Fixtures.payload(7, 7, 100);
            }
            s.verify();
            long tailBefore = s.testFileTail();
            long linkedBefore = s.testLinkedMovesApplied;
            long idxBefore = s.testIndexPageMovesApplied;

            StoreDirect.testTamperMode = tamperMode;
            int reclaimed = 0, n, guard = 0;
            while ((n = s.compactStep(linked ? 64 : 1)) > 0) {
                reclaimed += n;
                s.verify();                                        // never corrupts, even mid-tamper
                assertArrayEquals(probeVal, s.get(probe, Fixtures.RAW));
                if (++guard > 50) fail("compactStep did not terminate under tamper");
            }
            StoreDirect.testTamperMode = 0;

            // the TAMPERED move kind was never applied (its plan was always degraded to a no-op)
            if (linked) assertEquals("linked move never applied under tamper",
                    linkedBefore, s.testLinkedMovesApplied);
            else assertEquals("index move never applied under tamper",
                    idxBefore, s.testIndexPageMovesApplied);
            if (universal) {
                assertEquals("universal tamper is a strict no-op", 0, reclaimed);
                assertEquals("file tail unchanged", tailBefore, s.testFileTail());
            }
            s.verify();
            assertArrayEquals(probeVal, s.get(probe, Fixtures.RAW));

            // once the tamper is cleared the store compacts correctly and applies the blocked kind
            s.compactIncremental();
            s.verify();
            if (linked) assertTrue("linked move applied after clearing tamper",
                    s.testLinkedMovesApplied > linkedBefore);
            else assertTrue("index move applied after clearing tamper",
                    s.testIndexPageMovesApplied > idxBefore);
            assertArrayEquals(probeVal, s.get(probe, Fixtures.RAW));
        } finally {
            StoreDirect.testTamperMode = 0;
            s.close();
        }
        StoreDirect re = new StoreDirect(f);
        try { re.verify(); } finally { re.close(); } // no dirty partial state persisted
    }

    // ---------- verify()-oracle fuzz across seeds, WITH linked/oversize records ----------

    @Test public void verify_oracle_fuzz_multi_seed_with_linked() throws IOException {
        long base = Long.getLong("tailfuzz.seed", 0xA5A5_1234L);
        long totalLinkedMoves = 0;
        for (int seedIdx = 0; seedIdx < 4; seedIdx++) {
            long seed = base + seedIdx * 0x9E37_79B9L;
            totalLinkedMoves += runLinkedFuzz(seed);
        }
        assertTrue("linked-chunk relocation must be exercised across the fuzz seeds", totalLinkedMoves > 0);
    }

    private long runLinkedFuzz(long seed) throws IOException {
        File f = newFile();
        java.util.Random rnd = new java.util.Random(seed);
        java.util.Map<Long, byte[]> ref = new java.util.HashMap<>();
        StoreDirect s = new StoreDirect(f);
        try {
            for (int op = 0; op < 2500; op++) {
                int kind = rnd.nextInt(100);
                if (kind < 45 || ref.isEmpty()) {
                    byte[] v = rnd.nextInt(20) == 0 ? null : bytes(pickSizeLinked(rnd), op);
                    ref.put(s.put(v, Fixtures.RAW), v);
                } else if (kind < 58) {
                    long r = pickRecid(ref, rnd);
                    byte[] v = rnd.nextInt(20) == 0 ? null : bytes(pickSizeLinked(rnd), op ^ 0x77);
                    s.update(r, v, Fixtures.RAW);
                    ref.put(r, v);
                } else if (kind < 80) {
                    long r = pickRecid(ref, rnd);
                    s.delete(r, Fixtures.RAW);
                    ref.remove(r);
                } else {
                    long r = pickRecid(ref, rnd);
                    assertArrayEquals("get op " + op, ref.get(r), s.get(r, Fixtures.RAW));
                }
                if (op % 150 == 149) {
                    s.compactStep(1 + rnd.nextInt(4));
                    s.verify();
                    checkAll(s, ref);
                }
                if (op == 1500) {
                    s.compactIncremental();
                    s.verify();
                    s.close();
                    s = new StoreDirect(f);
                    s.verify();
                    checkAll(s, ref);
                }
            }
            s.compactIncremental();
            s.verify();
            checkAll(s, ref);
            long moves = s.testLinkedMovesApplied;
            s.close();
            s = new StoreDirect(f);
            s.verify();
            checkAll(s, ref);
            return moves;
        } finally {
            try { if (!s.isClosed()) s.close(); } catch (Throwable ignore) { }
        }
    }

    // ---------- concurrent reads (incl. large LINKED reads) stay consistent while compacting ----------

    @Test public void concurrent_reads_consistent_with_linked_while_compacting() throws Exception {
        File f = newFile();
        StoreDirect s = new StoreDirect(f);
        try {
            java.util.Map<Long, byte[]> permanent = new java.util.HashMap<>();
            for (int i = 0; i < 60; i++) {
                byte[] v = Fixtures.payload(i, 3, 300 + (i % 4) * 2000);
                permanent.put(s.put(v, Fixtures.RAW), v);
            }
            // a handful of permanent LINKED (oversize) records that will be relocated by compaction
            for (int i = 0; i < 4; i++) {
                byte[] v = Fixtures.payload(1000 + i, 5, 1_200_000 + i * 200_000);
                permanent.put(s.put(v, Fixtures.RAW), v);
            }
            List<Long> permKeys = new ArrayList<>(permanent.keySet());

            final int READERS = 4;
            Thread[] readers = new Thread[READERS];
            final java.util.concurrent.atomic.AtomicBoolean stop = new java.util.concurrent.atomic.AtomicBoolean();
            final java.util.concurrent.ConcurrentLinkedQueue<Throwable> errs = new java.util.concurrent.ConcurrentLinkedQueue<>();
            for (int t = 0; t < READERS; t++) {
                readers[t] = new Thread(() -> {
                    java.util.Random r = new java.util.Random(Thread.currentThread().getId());
                    while (!stop.get()) {
                        long recid = permKeys.get(r.nextInt(permKeys.size()));
                        try {
                            byte[] got = s.get(recid, Fixtures.RAW);
                            if (!Arrays.equals(permanent.get(recid), got))
                                errs.add(new AssertionError("torn read recid=" + recid));
                        } catch (Throwable ex) {
                            errs.add(ex);
                        }
                    }
                });
                readers[t].start();
            }

            java.util.Random rnd = new java.util.Random(0xC0FFEE);
            for (int round = 0; round < 30 && errs.isEmpty(); round++) {
                List<Long> junk = new ArrayList<>();
                for (int i = 0; i < 20; i++) junk.add(s.put(Fixtures.payload(round, i, 700_000), Fixtures.RAW));
                for (int i = 0; i < 3; i++) junk.add(s.put(Fixtures.payload(round, 90 + i, 1_300_000), Fixtures.RAW));
                for (long j : junk) s.delete(j, Fixtures.RAW);
                while (s.compactStep(3) > 0) { /* drain */ }
            }
            stop.set(true);
            for (Thread rd : readers) rd.join();
            s.verify();
            if (!errs.isEmpty()) throw new AssertionError("reader error", errs.peek());
            for (var e : permanent.entrySet())
                assertArrayEquals(e.getValue(), s.get(e.getKey(), Fixtures.RAW));
        } finally {
            s.close();
        }
    }

    private static void checkAll(StoreDirect s, java.util.Map<Long, byte[]> ref) {
        for (var e : ref.entrySet())
            assertArrayEquals("content mismatch recid=" + e.getKey(), e.getValue(), s.get(e.getKey(), Fixtures.RAW));
    }

    private static Long pickRecid(java.util.Map<Long, byte[]> ref, java.util.Random rnd) {
        int idx = rnd.nextInt(ref.size());
        for (Long r : ref.keySet()) if (idx-- == 0) return r;
        throw new AssertionError("unreachable");
    }

    private static int pickSizeLinked(java.util.Random rnd) {
        int r = rnd.nextInt(100);
        if (r < 70) return rnd.nextInt(2000);                 // small plain
        if (r < 90) return 2000 + rnd.nextInt(300_000);       // medium plain
        return 1_100_000 + rnd.nextInt(2_000_000);            // LINKED (content > MAX_CAPACITY)
    }

    private static byte[] bytes(int size, long seed) {
        byte[] b = new byte[size];
        long st = seed * 0x9E3779B97F4A7C15L + 1;
        for (int i = 0; i < size; i++) {
            st ^= st << 13; st ^= st >>> 7; st ^= st << 17;
            b[i] = (byte) st;
        }
        return b;
    }
}
