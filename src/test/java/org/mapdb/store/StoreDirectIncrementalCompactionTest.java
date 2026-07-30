package org.mapdb.store;

import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Incremental (bounded, structure-aware) compaction for StoreDirect.
 * Property: after any sequence of compactStep()/compactIncremental() calls the store
 * holds EXACTLY the same logical content (every live recid resolves to identical bytes,
 * deleted recids stay void), the on-volume invariants hold (verify()), reclaimed pages
 * shrink the file, and the store reopens cleanly. verify() is the double-use / lost-extent
 * oracle.
 */
public class StoreDirectIncrementalCompactionTest {

    private final List<File> files = new ArrayList<>();

    private File newFile() throws IOException {
        File f = Files.createTempFile("mapdb5-incr-compact", ".db").toFile();
        f.delete();
        files.add(f);
        return f;
    }

    @After public void cleanup() {
        for (File f : files) f.delete();
        files.clear();
    }

    private static void assertGetVoid(Runnable r) {
        try {
            r.run();
            fail("expected DBException.GetVoid");
        } catch (org.mapdb.DBException.GetVoid expected) { /* ok */ }
    }

    // ---------- reclamation of trailing free pages ----------

    @Test public void reclaims_trailing_free_pages_and_preserves_live_data() throws IOException {
        File f = newFile();
        StoreDirect s = new StoreDirect(f);
        try {
            // fill several pages with ~100 KB records, keep a couple of low live records
            byte[] keepA = Fixtures.payload(1, 1, 90_000);
            byte[] keepB = Fixtures.payload(2, 1, 90_000);
            long a = s.put(keepA, Fixtures.RAW);
            long b = s.put(keepB, Fixtures.RAW);
            List<Long> doomed = new ArrayList<>();
            for (int i = 0; i < 60; i++) doomed.add(s.put(Fixtures.payload(100 + i, 1, 100_000), Fixtures.RAW));
            long fullTail = s.testFileTail();
            for (long r : doomed) s.delete(r, Fixtures.RAW);
            s.verify();

            long reclaimed = s.compactIncremental();
            s.verify();
            assertTrue("should reclaim trailing pages", reclaimed > 0);
            assertTrue("file tail should shrink", s.testFileTail() < fullTail);
            assertArrayEquals(keepA, s.get(a, Fixtures.RAW));
            assertArrayEquals(keepB, s.get(b, Fixtures.RAW));
            for (long r : doomed) assertGetVoid(() -> s.get(r, Fixtures.RAW));
        } finally {
            s.close();
        }

        StoreDirect re = new StoreDirect(f);
        try {
            re.verify();
            assertEquals("close truncates to logical tail", f.length(), re.testFileTail());
        } finally {
            re.close();
        }
    }

    // ---------- bounded steps make identical progress to a full compact ----------

    @Test public void stepwise_reaches_same_content_as_full_compact() throws IOException {
        File f = newFile();
        StoreDirect s = new StoreDirect(f);
        try {
            Map<Long, byte[]> ref = new HashMap<>();
            Random rnd = new Random(0xBEEF);
            for (int i = 0; i < 200; i++) {
                byte[] v = Fixtures.payload(i, 1, rnd.nextInt(40_000));
                ref.put(s.put(v, Fixtures.RAW), v);
            }
            // delete ~half at random to fragment the store
            List<Long> keys = new ArrayList<>(ref.keySet());
            for (Long k : keys) if (rnd.nextBoolean()) { s.delete(k, Fixtures.RAW); ref.remove(k); }
            s.verify();

            int steps = 0;
            while (s.compactStep(1) > 0) {
                s.verify();
                for (var e : ref.entrySet())
                    assertArrayEquals("content after step " + steps, e.getValue(), s.get(e.getKey(), Fixtures.RAW));
                if (++steps > 10_000) fail("compactStep did not terminate");
            }
            s.verify();
            for (var e : ref.entrySet())
                assertArrayEquals(e.getValue(), s.get(e.getKey(), Fixtures.RAW));
        } finally {
            s.close();
        }
    }

    // ---------- capacity + append headroom survive relocation ----------

    @Test public void relocation_preserves_capacity_and_append() throws IOException {
        File f = newFile();
        StoreDirect s = new StoreDirect(f);
        try {
            // create trailing garbage to be reclaimed
            List<Long> doomed = new ArrayList<>();
            for (int i = 0; i < 40; i++) doomed.add(s.put(Fixtures.payload(i, 1, 120_000), Fixtures.RAW));
            byte[] base = Fixtures.payload(500, 1, 200);
            long rec = s.put(base, Fixtures.RAW);
            s.updateWithHeadroom(rec, base, Fixtures.RAW, 4096); // provision appendable capacity
            for (long r : doomed) s.delete(r, Fixtures.RAW);
            long capBefore = s.capacityRemaining(rec);
            assertTrue(capBefore >= 4096);

            s.compactIncremental();
            s.verify();

            assertArrayEquals(base, s.get(rec, Fixtures.RAW));
            assertEquals("capacity preserved across relocation", capBefore, s.capacityRemaining(rec));
            byte[] extra = Fixtures.payload(500, 2, 1000);
            assertEquals(base.length + extra.length, s.append(rec, extra, 0, extra.length));
            byte[] merged = new byte[base.length + extra.length];
            System.arraycopy(base, 0, merged, 0, base.length);
            System.arraycopy(extra, 0, merged, base.length, extra.length);
            assertArrayEquals(merged, s.get(rec, Fixtures.RAW));
            s.verify();
        } finally {
            s.close();
        }
    }

    // ---------- deleted recids are reused after incremental compaction ----------

    @Test public void reuses_freed_recids_after_incremental_compact() throws IOException {
        File f = newFile();
        StoreDirect s = new StoreDirect(f);
        try {
            long a = s.put(Fixtures.payload(1, 1, 50_000), Fixtures.RAW);
            List<Long> doomed = new ArrayList<>();
            for (int i = 0; i < 40; i++) doomed.add(s.put(Fixtures.payload(i, 1, 120_000), Fixtures.RAW));
            long firstDoomed = doomed.get(0);
            for (long r : doomed) s.delete(r, Fixtures.RAW);
            s.compactIncremental();
            s.verify();
            // free-recid stack rebuilt: a freed recid is handed back out
            long reused = s.put(Fixtures.payload(9, 9, 10), Fixtures.RAW);
            assertTrue("freed recid reused", doomed.contains(reused));
            assertArrayEquals(Fixtures.payload(1, 1, 50_000), s.get(a, Fixtures.RAW));
            s.verify();
        } finally {
            s.close();
        }
    }

    // ---------- crash mid-step reopens as a refusal (never a torn accepted state) ----------

    @Test public void crash_during_compaction_refuses_reopen() throws IOException {
        File f = newFile();
        StoreDirect s = new StoreDirect(f);
        boolean crashed = false;
        try {
            long a = s.put(Fixtures.payload(1, 1, 80_000), Fixtures.RAW);
            List<Long> doomed = new ArrayList<>();
            for (int i = 0; i < 40; i++) doomed.add(s.put(Fixtures.payload(i, 1, 120_000), Fixtures.RAW));
            for (long r : doomed) s.delete(r, Fixtures.RAW);
            s.commit(); // clean, valid on-disk state before the (faulted) step
            s.verify();

            StoreDirect.testCompactCrashHook = () -> { throw new RuntimeException("simulated crash"); };
            try {
                s.compactStep(64);
                fail("expected simulated crash");
            } catch (RuntimeException expected) {
                crashed = "simulated crash".equals(expected.getMessage());
                if (!crashed) throw expected;
            } finally {
                StoreDirect.testCompactCrashHook = null;
            }

            // snapshot the on-disk bytes as they stand mid-step (the barrier synced an invalid
            // checksum), then confirm a fresh open of that snapshot REFUSES rather than accepting
            // a half-relocated store.
            File snap = newFile();
            Files.write(snap.toPath(), Files.readAllBytes(f.toPath()));
            try {
                new StoreDirect(snap).close();
                fail("expected reopen of a mid-compaction crash to refuse");
            } catch (org.mapdb.DBException.DataCorruption expected) { /* refused: correct */ }

            // the faulted LIVE store is poisoned: it refuses further use, and an ordinary
            // close() releases resources WITHOUT resealing the half-relocated state as clean
            // (the pre-poison bug: close() stamped a valid checksum and reopen accepted a
            // store whose verify() then failed with overlapping extents).
            assertTrue(s.isClosed());
            s.close(); // must not stamp
            try {
                new StoreDirect(f).close();
                fail("expected reopen after fault+close to refuse");
            } catch (org.mapdb.DBException.DataCorruption expected) { /* refused: correct */ }
        } finally {
            StoreDirect.testCompactCrashHook = null;
        }
        assertTrue(crashed);
    }

    // ---------- concurrent reads stay consistent across relocation steps ----------

    @Test public void concurrent_reads_consistent_while_compacting() throws Exception {
        File f = newFile();
        StoreDirect s = new StoreDirect(f); // thread-safe by default
        try {
            // permanent live records the readers continuously validate (they are never deleted,
            // but ARE relocated by compaction as the churn frees trailing pages around them)
            Map<Long, byte[]> permanent = new HashMap<>();
            for (int i = 0; i < 200; i++) {
                byte[] v = Fixtures.payload(i, 7, 200 + (i % 5) * 3000);
                permanent.put(s.put(v, Fixtures.RAW), v);
            }
            List<Long> permKeys = new ArrayList<>(permanent.keySet());

            final int READERS = 4;
            Thread[] readers = new Thread[READERS];
            final java.util.concurrent.atomic.AtomicBoolean stop = new java.util.concurrent.atomic.AtomicBoolean();
            final java.util.concurrent.ConcurrentLinkedQueue<Throwable> errs = new java.util.concurrent.ConcurrentLinkedQueue<>();
            for (int t = 0; t < READERS; t++) {
                readers[t] = new Thread(() -> {
                    Random r = new Random(Thread.currentThread().getId());
                    while (!stop.get()) {
                        long recid = permKeys.get(r.nextInt(permKeys.size()));
                        try {
                            byte[] got = s.get(recid, Fixtures.RAW);
                            if (!java.util.Arrays.equals(permanent.get(recid), got))
                                errs.add(new AssertionError("torn read recid=" + recid));
                        } catch (Throwable ex) {
                            errs.add(ex);
                        }
                    }
                });
                readers[t].start();
            }

            Random rnd = new Random(0x5EED);
            for (int round = 0; round < 40 && errs.isEmpty(); round++) {
                List<Long> junk = new ArrayList<>();
                for (int i = 0; i < 40; i++) junk.add(s.put(Fixtures.payload(round, i, 60_000), Fixtures.RAW));
                for (long j : junk) s.delete(j, Fixtures.RAW);
                while (s.compactStep(2) > 0) { /* drain */ }
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

    // ---------- fuzz: interleave mutations, incremental compaction, verify + reopen ----------

    @Test public void fuzz_incremental_compact_with_reopen() throws IOException {
        long seed = Long.getLong("incrfuzz.seed", 0x1CE_C0FFEEL);
        System.out.println("StoreDirectIncrementalCompactionTest seed=" + Long.toHexString(seed));
        File f = newFile();
        Random rnd = new Random(seed);
        Map<Long, byte[]> ref = new HashMap<>();
        StoreDirect s = new StoreDirect(f);
        try {
            for (int op = 0; op < 4000; op++) {
                int kind = rnd.nextInt(100);
                if (kind < 45 || ref.isEmpty()) {
                    byte[] v = rnd.nextInt(15) == 0 ? null : bytes(pickSize(rnd), op);
                    ref.put(s.put(v, Fixtures.RAW), v);
                } else if (kind < 60) {
                    long r = pickRecid(ref, rnd);
                    byte[] v = rnd.nextInt(15) == 0 ? null : bytes(pickSize(rnd), op ^ 0x9999);
                    s.update(r, v, Fixtures.RAW);
                    ref.put(r, v);
                } else if (kind < 80) {
                    long r = pickRecid(ref, rnd);
                    s.delete(r, Fixtures.RAW);
                    ref.remove(r);
                } else {
                    long r = pickRecid(ref, rnd);
                    assertArrayEquals("get mismatch op " + op, ref.get(r), s.get(r, Fixtures.RAW));
                }
                if (op % 200 == 199) {
                    int budget = 1 + rnd.nextInt(4);
                    s.compactStep(budget);
                    s.verify();
                    checkAll(s, ref, op);
                }
                if (op == 1300 || op == 2600) {
                    s.compactIncremental();
                    s.verify();
                    s.close();
                    s = new StoreDirect(f);
                    s.verify();
                    checkAll(s, ref, op);
                }
            }
            s.compactIncremental();
            s.verify();
            checkAll(s, ref, -1);
            s.close();
            s = new StoreDirect(f);
            s.verify();
            checkAll(s, ref, -2);
        } finally {
            try { if (!s.isClosed()) s.close(); } catch (Throwable ignore) { }
        }
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

    private static int pickSize(Random rnd) {
        int r = rnd.nextInt(100);
        if (r < 65) return rnd.nextInt(500);
        if (r < 92) return 500 + rnd.nextInt(60_000);
        return 200_000 + rnd.nextInt(700_000); // large plain records spanning most of a page
    }

    private static Long pickRecid(Map<Long, byte[]> ref, Random rnd) {
        int idx = rnd.nextInt(ref.size());
        for (Long r : ref.keySet()) if (idx-- == 0) return r;
        throw new AssertionError("unreachable");
    }

    private static void checkAll(StoreDirect s, Map<Long, byte[]> ref, int op) {
        for (var e : ref.entrySet())
            assertArrayEquals("content mismatch recid=" + e.getKey() + " op " + op,
                    e.getValue(), s.get(e.getKey(), Fixtures.RAW));
    }
}
