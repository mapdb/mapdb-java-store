package org.mapdb.store;

import org.junit.After;
import org.junit.Test;
import org.mapdb.DBException;
import org.mapdb.TmpFiles;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * WAL crash-recovery harness. Property: recovery always yields exactly the
 * state at the last successful commit. Drives a seeded workload with periodic commits,
 * snapshots an oracle at each commit, then verifies: reopen-as-is; truncation at every
 * byte offset; last-section CRC corruption; rollback discards staged mutations; appends
 * survive commit+reopen.
 */
public class StoreWALCrashTest {

    private final List<File> files = new ArrayList<>();

    private File newFile(String tag) {
        try {
            File f = TmpFiles.tempFile("mapdb-wal-crash-" + tag, ".wal");
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

    // ---------- oracle ----------

    /** Live (non-prealloc) records: recid -> content bytes (null for null-content records). */
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

    private final List<Long> commitLens = new ArrayList<>();
    private final List<TreeMap<Long, byte[]>> commitSnaps = new ArrayList<>();

    /** Builds a committed workload on {@code f}, recording file length + oracle snapshot per commit. */
    private void buildWorkload(File f, long seed) {
        commitLens.clear();
        commitSnaps.clear();
        Random rnd = new Random(seed);
        StoreWAL s = new StoreWAL(f);
        List<Long> live = new ArrayList<>();
        int version = 1;
        for (int c = 0; c < 8; c++) {
            int ops = 3 + rnd.nextInt(4);
            for (int i = 0; i < ops; i++) {
                int pick = live.isEmpty() ? 0 : rnd.nextInt(5);
                switch (pick) {
                    case 0 -> {
                        long r = s.put(Fixtures.payload(live.size(), version++, rnd.nextInt(8)), Fixtures.RAW);
                        live.add(r);
                    }
                    case 1 -> {
                        long r = live.get(rnd.nextInt(live.size()));
                        s.updateWithHeadroom(r, Fixtures.payload((int) (r & 0x7F), version++, rnd.nextInt(8)), Fixtures.RAW, 32);
                    }
                    case 2 -> {
                        long r = live.get(rnd.nextInt(live.size()));
                        s.updateWithHeadroom(r, Fixtures.payload((int) (r & 0x7F), version++, 2), Fixtures.RAW, 16);
                        s.append(r, new byte[]{(byte) version, (byte) i, (byte) c}, 0, 3);
                    }
                    case 3 -> {
                        if (live.size() > 1) {
                            long r = live.remove(rnd.nextInt(live.size()));
                            s.delete(r, Fixtures.RAW);
                        }
                    }
                    default -> s.preallocate(); // stays P (excluded from snapshot)
                }
            }
            s.commit();
            s.verify();
            commitLens.add(WalTestKit.onlySegment(f).length());
            commitSnaps.add(snapshot(s));
        }
        // uncommitted tail: staged, never written to file
        if (!live.isEmpty()) s.update(live.get(0), Fixtures.payload(0, 999, 4), Fixtures.RAW);
        s.put(Fixtures.payload(77, 777, 3), Fixtures.RAW);
        s.close();
    }

    private TreeMap<Long, byte[]> survivingSnapshot(long truncLen) {
        TreeMap<Long, byte[]> best = new TreeMap<>();
        for (int i = 0; i < commitLens.size(); i++) {
            if (commitLens.get(i) <= truncLen) best = commitSnaps.get(i);
        }
        return best;
    }

    // ================= (a) reopen as-is =================

    @Test public void reopen_asIs_equals_last_committed_snapshot() {
        long seed = 0xC0FFEEL;
        File f = newFile("asis");
        buildWorkload(f, seed);
        TreeMap<Long, byte[]> last = commitSnaps.get(commitSnaps.size() - 1);

        StoreWAL s = new StoreWAL(f);
        try {
            s.verify();
            assertState(s, last); // uncommitted tail must NOT be visible
        } finally {
            s.close();
        }
    }

    // ================= (b) truncation at every byte offset =================

    @Test public void truncation_at_every_offset_recovers_surviving_commit() throws IOException {
        long seed = 0x1234ABCDL;
        File src = newFile("trunc-src");
        buildWorkload(src, seed);
        byte[] all = WalTestKit.read(WalTestKit.onlySegment(src));
        assertTrue("need >=2 commits", commitSnaps.size() >= 2);
        // System.out.println("truncation sweep: fileLen=" + all.length + " seed=" + seed);

        // The bytes go into a segment with the SAME sequence number: every section CRC binds
        // the segment header (§6.2), so the image is only valid under the name it was written
        // for. Truncating below the 28-byte header makes it a torn create (H1/H2) — residue on
        // the highest segment, which recovers as the empty store the oracle expects.
        File copy = newFile("trunc-copy");
        for (int t = 0; t <= all.length; t++) {
            WalTestKit.deleteStore(copy);
            WalTestKit.write(WalTestKit.segment(copy, 1), java.util.Arrays.copyOf(all, t));
            StoreWAL s = null;
            try {
                s = new StoreWAL(copy); // must never throw on a torn tail
                s.verify();
                assertState(s, survivingSnapshot(t));
            } catch (RuntimeException e) {
                fail("truncation at offset " + t + " threw " + e);
            } finally {
                if (s != null) s.close();
            }
        }
    }

    // ================= (c) CRC corruption of last section =================

    @Test public void crc_flip_in_last_section_discards_that_section() throws IOException {
        long seed = 0x55AA55AAL;
        File f = newFile("crc");
        buildWorkload(f, seed);
        byte[] all = WalTestKit.read(WalTestKit.onlySegment(f));
        assertTrue(commitSnaps.size() >= 2);

        // last 4 bytes are the last section's stored CRC; corrupt one of them
        byte[] corrupt = all.clone();
        corrupt[corrupt.length - 1] ^= 0xFF;
        File cf = newFile("crc-copy");
        WalTestKit.write(WalTestKit.segment(cf, 1), corrupt);

        StoreWAL s = new StoreWAL(cf);
        try {
            s.verify();
            // last section must not apply -> state is the second-to-last commit
            assertState(s, commitSnaps.get(commitSnaps.size() - 2));
        } finally {
            s.close();
        }
    }

    // ================= rollback =================

    @Test public void rollback_discards_staged_mutations_including_appends() {
        File f = newFile("rollback");
        StoreWAL s = new StoreWAL(f);
        try {
            byte[] base = Fixtures.payload(1, 1, 4);
            long r = s.put(base, Fixtures.RAW);
            s.updateWithHeadroom(r, base, Fixtures.RAW, 32);
            s.commit();
            TreeMap<Long, byte[]> committed = snapshot(s);

            // stage: an update, an append, and a brand-new record
            s.update(r, Fixtures.payload(1, 2, 4), Fixtures.RAW);
            s.append(r, new byte[]{9, 9, 9}, 0, 3);
            long r2 = s.put(Fixtures.payload(2, 1, 0), Fixtures.RAW);
            assertArrayEquals(Fixtures.payload(2, 1, 0), s.get(r2, Fixtures.RAW)); // visible through same handle

            s.rollback();
            s.verify();

            // ops after rollback see pre-tx (committed) state
            assertState(s, committed);
            assertArrayEquals(base, s.get(r, Fixtures.RAW));
            assertGetVoid(() -> s.get(r2, Fixtures.RAW)); // created record gone
            PrimitiveIterator.OfLong it = s.getAllRecids();
            boolean hasR2 = false;
            while (it.hasNext()) if (it.nextLong() == r2) hasR2 = true;
            assertFalse("rolled-back record must not appear", hasR2);
        } finally {
            s.close();
        }
    }

    // ================= appends survive commit + reopen =================

    @Test public void reopen_preserves_appends_committed_via_append() {
        File f = newFile("append-persist");
        byte[] base = Fixtures.payload(5, 1, 0);
        byte[] d1 = {41, 42, 43};
        byte[] d2 = {51, 52};
        StoreWAL s = new StoreWAL(f);
        long r;
        try {
            r = s.preallocate();
            s.commit();
            s.updateWithHeadroom(r, base, Fixtures.RAW, 64);
            s.commit();
            s.append(r, d1, 0, d1.length);
            s.append(r, d2, 0, d2.length);
            s.commit();
            s.verify();
        } finally {
            s.close();
        }

        StoreWAL s2 = new StoreWAL(f);
        try {
            s2.verify();
            byte[] expect = new byte[base.length + d1.length + d2.length];
            System.arraycopy(base, 0, expect, 0, base.length);
            System.arraycopy(d1, 0, expect, base.length, d1.length);
            System.arraycopy(d2, 0, expect, base.length + d1.length, d2.length);
            assertArrayEquals(expect, s2.get(r, Fixtures.RAW));
        } finally {
            s2.close();
        }
    }

    private static void assertGetVoid(Runnable run) {
        try {
            run.run();
            fail("expected GetVoid");
        } catch (DBException.GetVoid expected) { /* ok */ }
    }
}
