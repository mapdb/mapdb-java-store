package org.mapdb.store;

import org.junit.After;
import org.junit.Test;
import org.mapdb.DBException;

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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Log-compaction invariants for {@link StoreWAL}. Under v2 a checkpoint is a CLEANING round —
 * roll to a fresh segment, write the whole committed store as one {@code 'C'} image, mark, and
 * unlink what the mark authorizes — so the old {@code .ckpt} temp file and its {@code ATOMIC_MOVE}
 * commit point are gone. What must not change is that it is TRANSPARENT: same visible state
 * before and after, across reopen, with capacities (append/REFUSED semantics), preallocated
 * recids, null records and staged (uncommitted) mutations all preserved.
 */
public class StoreWALCheckpointTest {

    private final List<File> files = new ArrayList<>();

    private File newFile(String tag) {
        try {
            File f = Files.createTempFile("mapdb5-wal-ckpt-" + tag, ".wal").toFile();
            f.delete();
            files.add(f);
            return f;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @After public void cleanup() {
        StoreWAL.testSetDirectorySync(null);
        for (File f : files) WalTestKit.deleteStore(f);
        files.clear();
    }

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

    /** Random committed workload; returns the oracle snapshot at the last commit. */
    private static TreeMap<Long, byte[]> workload(StoreWAL s, long seed, int commits) {
        Random rnd = new Random(seed);
        List<Long> live = new ArrayList<>();
        int version = 1;
        for (int c = 0; c < commits; c++) {
            int ops = 3 + rnd.nextInt(4);
            for (int i = 0; i < ops; i++) {
                int pick = live.isEmpty() ? 0 : rnd.nextInt(5);
                switch (pick) {
                    case 0 -> live.add(s.put(Fixtures.payload(live.size(), version++, rnd.nextInt(16)), Fixtures.RAW));
                    case 1 -> s.updateWithHeadroom(live.get(rnd.nextInt(live.size())),
                            Fixtures.payload(c, version++, rnd.nextInt(16)), Fixtures.RAW, 32);
                    case 2 -> {
                        long r = live.get(rnd.nextInt(live.size()));
                        s.updateWithHeadroom(r, Fixtures.payload(c, version++, 2), Fixtures.RAW, 16);
                        s.append(r, new byte[]{(byte) version, (byte) i}, 0, 2);
                    }
                    case 3 -> {
                        if (live.size() > 1) s.delete(live.remove(rnd.nextInt(live.size())), Fixtures.RAW);
                    }
                    default -> s.preallocate();
                }
            }
            s.commit();
        }
        return snapshot(s);
    }

    // ================= checkpoint is state-transparent =================

    @Test public void checkpoint_preserves_state_in_process_and_across_reopen() {
        File f = newFile("basic");
        StoreWAL s = new StoreWAL(f);
        TreeMap<Long, byte[]> oracle;
        try {
            oracle = workload(s, 0xBEEF, 6);
            s.checkpoint();
            s.verify();
            assertState(s, oracle); // in-process: checkpoint changes nothing visible

            // store stays fully operational after the channel swap
            long extra = s.put(Fixtures.payload(9, 9, 5), Fixtures.RAW);
            s.commit();
            oracle.put(extra, Fixtures.payload(9, 9, 5));
            assertState(s, oracle);
        } finally {
            s.close();
        }

        StoreWAL s2 = new StoreWAL(f);
        try {
            s2.verify();
            assertState(s2, oracle);
        } finally {
            s2.close();
        }
        assertEquals("cleaning retires every segment below its image",
                1, WalTestKit.segments(f).length);
    }

    @Test public void checkpoint_shrinks_log_to_live_data() {
        File f = newFile("shrink");
        StoreWAL s = new StoreWAL(f);
        byte[] last = null;
        try {
            long r = s.put(new byte[1024], Fixtures.RAW);
            s.commit();
            for (int i = 0; i < 200; i++) {
                last = Fixtures.payload(i, i, 1016);
                s.update(r, last, Fixtures.RAW);
                s.commit();
            }
            long before = WalTestKit.logBytes(f);
            assertTrue("log should have grown, len=" + before, before > 100_000);
            s.checkpoint();
            long after = WalTestKit.logBytes(f);
            assertTrue("log should compact to ~1 record, len=" + after, after < 5_000 && after > 0);
            assertArrayEquals(last, s.get(r, Fixtures.RAW));

            StoreWAL s2 = null;
            s.close();
            try {
                s2 = new StoreWAL(f);
                assertArrayEquals(last, s2.get(r, Fixtures.RAW));
                s2.verify();
            } finally {
                if (s2 != null) s2.close();
            }
        } finally {
            if (!s.isClosed()) s.close();
        }
    }

    @Test public void checkpoint_of_empty_store_and_double_checkpoint() {
        File f = newFile("empty");
        StoreWAL s = new StoreWAL(f);
        try {
            s.checkpoint();
            // Cleaning an empty store writes NOTHING. The store opened with one empty segment;
            // there is nothing below it to retire, so there is no cycle, no image and no mark
            // (a 'K' attesting cleanedThroughSeq 0 is corruption by S8 anyway). Step 2's
            // whole-store stand-in wrote an empty 'C' here and then, on the second call, rolled
            // and retired segment 1 — two sections and a segment burnt to represent nothing.
            s.checkpoint();
            assertEquals("only the segment the store opened with", 1, WalTestKit.segments(f).length);
            assertEquals("nothing but the segment header", (long) WalTestKit.SEG_HDR,
                    WalTestKit.logBytes(f));
            assertFalse(s.getAllRecids().hasNext());
        } finally {
            s.close();
        }
        StoreWAL s2 = new StoreWAL(f);
        try {
            assertFalse(s2.getAllRecids().hasNext());
            s2.verify();
        } finally {
            s2.close();
        }
    }

    // ================= capacity / delta semantics survive =================

    @Test public void checkpoint_preserves_capacity_and_append_refusal() {
        File f = newFile("cap");
        byte[] base = Fixtures.payload(1, 1, 0); // 8 bytes
        long r;
        StoreWAL s = new StoreWAL(f);
        try {
            r = s.put(base, Fixtures.RAW);
            s.updateWithHeadroom(r, base, Fixtures.RAW, 32); // cap = roundUp16(4+8+32) = 48
            s.commit();
            assertEquals(36, s.capacityRemaining(r));
            s.checkpoint();
            assertEquals(36, s.capacityRemaining(r));
        } finally {
            s.close();
        }

        StoreWAL s2 = new StoreWAL(f);
        try {
            assertEquals("capacity must survive checkpoint + reopen", 36, s2.capacityRemaining(r));
            byte[] fill = new byte[36];
            assertEquals(8 + 36, s2.append(r, fill, 0, 36)); // exactly fits
            assertEquals(StoreDelta.REFUSED, s2.append(r, new byte[]{1}, 0, 1)); // beyond cap
            s2.commit();
        } finally {
            s2.close();
        }
    }

    @Test public void checkpoint_preserves_prealloc_null_and_deleted_recids() {
        File f = newFile("states");
        long nullRec, preRec, delRec;
        StoreWAL s = new StoreWAL(f);
        try {
            nullRec = s.put(null, Fixtures.RAW);   // null-content record: visible, content null
            preRec = s.preallocate();               // P state: invisible until filled
            delRec = s.put(Fixtures.payload(3, 3, 4), Fixtures.RAW);
            s.commit();
            s.delete(delRec, Fixtures.RAW);
            s.commit();
            s.checkpoint();
        } finally {
            s.close();
        }

        StoreWAL s2 = new StoreWAL(f);
        try {
            TreeMap<Long, byte[]> snap = snapshot(s2);
            assertTrue("null record visible", snap.containsKey(nullRec));
            assertNull(snap.get(nullRec));
            assertFalse("prealloc excluded from getAllRecids", snap.containsKey(preRec));
            assertFalse("deleted recid gone", snap.containsKey(delRec));
            try {
                s2.get(delRec, Fixtures.RAW);
                fail("expected GetVoid");
            } catch (DBException.GetVoid expected) { /* ok */ }

            // the preallocated recid survived as P: filling it works
            byte[] fillData = Fixtures.payload(7, 7, 3);
            s2.update(preRec, fillData, Fixtures.RAW);
            s2.commit();
            assertArrayEquals(fillData, s2.get(preRec, Fixtures.RAW));
            s2.verify();
        } finally {
            s2.close();
        }
    }

    // ================= staged mutations vs checkpoint =================

    @Test public void checkpoint_leaves_staged_mutations_intact() {
        File f = newFile("staged");
        StoreWAL s = new StoreWAL(f);
        try {
            byte[] committed = Fixtures.payload(1, 1, 8);
            long r = s.put(committed, Fixtures.RAW);
            s.commit();
            TreeMap<Long, byte[]> committedSnap = snapshot(s);

            // stage but do not commit
            byte[] stagedVal = Fixtures.payload(1, 2, 8);
            s.updateWithHeadroom(r, stagedVal, Fixtures.RAW, 16);
            s.append(r, new byte[]{7, 7}, 0, 2);
            long r2 = s.put(Fixtures.payload(2, 1, 4), Fixtures.RAW);

            s.checkpoint(); // snapshot holds only committed state; staging is memory-only

            byte[] merged = new byte[stagedVal.length + 2];
            System.arraycopy(stagedVal, 0, merged, 0, stagedVal.length);
            merged[stagedVal.length] = 7;
            merged[stagedVal.length + 1] = 7;
            assertArrayEquals("staged update+append still visible", merged, s.get(r, Fixtures.RAW));
            assertArrayEquals(Fixtures.payload(2, 1, 4), s.get(r2, Fixtures.RAW));

            s.rollback(); // staged mutations discard cleanly after the checkpoint
            assertState(s, committedSnap);
        } finally {
            s.close();
        }
    }

    @Test public void staged_commit_after_checkpoint_is_durable() {
        File f = newFile("staged-commit");
        long r;
        byte[] val = Fixtures.payload(4, 4, 12);
        StoreWAL s = new StoreWAL(f);
        try {
            r = s.put(Fixtures.payload(4, 3, 12), Fixtures.RAW);
            s.commit();
            s.update(r, val, Fixtures.RAW); // staged
            s.checkpoint();
            s.commit();                      // appended after the snapshot section
        } finally {
            s.close();
        }
        StoreWAL s2 = new StoreWAL(f);
        try {
            assertArrayEquals(val, s2.get(r, Fixtures.RAW));
            s2.verify();
        } finally {
            s2.close();
        }
    }

    // ================= commits after checkpoint + torn tails =================

    @Test public void torn_tail_after_cleaning_recovers_surviving_state() throws IOException {
        File f = newFile("torn-src");
        StoreWAL s = new StoreWAL(f);
        List<Long> lens = new ArrayList<>();
        List<TreeMap<Long, byte[]>> snaps = new ArrayList<>();
        try {
            workload(s, 0xA11CE, 4);
            s.checkpoint();
            lens.add(WalTestKit.onlySegment(f).length());
            snaps.add(snapshot(s));
            for (int c = 0; c < 2; c++) {
                long r = s.put(Fixtures.payload(50 + c, c, 8), Fixtures.RAW);
                s.append(r, new byte[]{(byte) c}, 0, 1);
                s.commit();
                lens.add(WalTestKit.onlySegment(f).length());
                snaps.add(snapshot(s));
            }
        } finally {
            s.close();
        }

        File src = WalTestKit.onlySegment(f);
        long seq = seqOf(src);
        byte[] all = Files.readAllBytes(src.toPath());
        File copy = newFile("torn-copy");
        // Sweep every truncation point from the clean mark to EOF: replay must land exactly on
        // the newest wholly-surviving section. The bytes are copied into a segment with the SAME
        // sequence number, because every section CRC binds the segment header (§6.2) — the whole
        // image would fail its checksums under any other name.
        for (long t = lens.get(0); t <= all.length; t++) {
            for (File old : WalTestKit.segments(copy)) old.delete();
            WalTestKit.write(WalTestKit.segment(copy, seq), java.util.Arrays.copyOf(all, (int) t));
            TreeMap<Long, byte[]> expect = snaps.get(0);
            for (int i = 0; i < lens.size(); i++) {
                if (lens.get(i) <= t) expect = snaps.get(i);
            }
            StoreWAL rs = null;
            try {
                rs = new StoreWAL(copy);
                rs.verify();
                assertState(rs, expect);
            } catch (RuntimeException e) {
                fail("truncation at " + t + " threw " + e);
            } finally {
                if (rs != null) rs.close();
            }
        }
    }

    private static long seqOf(File segment) {
        String name = segment.getName();
        return Long.parseUnsignedLong(name.substring(name.length() - 16), 16);
    }

    // ================= cleaning fails closed rather than half-done =================

    /**
     * The directory fsync is not optional bookkeeping: W2 makes it part of the acknowledgement
     * rule (a commit is durable when its section is forced AND the directory entry of the
     * segment holding it is durable). Cleaning rolls to a fresh segment, so a failing directory
     * fsync means the new segment may vanish — the store must fail CLOSED rather than carry on
     * writing into a name that might not exist after a crash.
     */
    @Test public void cleaning_fails_closed_when_directory_fsync_fails() {
        File f = newFile("dirsync-fail");
        TreeMap<Long, byte[]> oracle;
        StoreWAL s = new StoreWAL(f);
        try {
            oracle = workload(s, 0x515E, 4);
            StoreWAL.testSetDirectorySync(dir -> {
                throw new IOException("forced directory fsync failure");
            });
            try {
                s.compact();
                fail("expected cleaning to fail");
            } catch (DBException expected) {
                assertTrue(expected.getMessage(), expected.getMessage().contains("cleaning failed"));
            }
            assertTrue("a half-published clean must fail the store closed", s.isClosed());
            try {
                long r = s.put(Fixtures.payload(9, 9, 9), Fixtures.RAW);
                s.commit();
                fail("closed WAL accepted a post-failure commit, recid=" + r);
            } catch (DBException.StoreClosed expected) { /* ok */ }
        } finally {
            StoreWAL.testSetDirectorySync(null);
            if (!s.isClosed()) s.close();
        }

        StoreWAL reopened = new StoreWAL(f);
        try {
            assertState(reopened, oracle);
        } finally {
            reopened.close();
        }
    }

    @Test public void new_wal_open_fails_if_directory_fsync_fails() {
        File f = newFile("create-dirsync-fail");
        StoreWAL.testSetDirectorySync(dir -> {
            throw new IOException("forced directory fsync failure");
        });
        try {
            StoreWAL s = new StoreWAL(f);
            s.close();
            fail("expected WAL open failure");
        } catch (DBException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("WAL open failed"));
        } finally {
            StoreWAL.testSetDirectorySync(null);
        }
    }


    // ================= automatic checkpointing =================

    /**
     * The trigger is a RATIO now — {@code log > max(minLogBytes, factor × store footprint)} — so a
     * bound has to be expressed against the footprint rather than against a byte constant. One
     * repeatedly-updated 60 KiB record keeps the footprint flat while the log grows fast, which is
     * the shape that makes the ratio bite; {@code setMinLogBytes(1024)} takes the floor out of the
     * way so the amplification term is what is actually under test.
     */
    @Test public void auto_cleaning_keeps_the_log_within_its_space_amplification_target() {
        File f = newFile("auto");
        StoreWAL s = new StoreWAL(f);
        byte[] last = null;
        long r;
        // A record written once and never touched again: its only self-contained entry sits in
        // the oldest segment, so every retirement of that segment MUST re-emit it. Without it the
        // workload is pure garbage and the cleaner would never exercise a publish at all.
        byte[] stable = Fixtures.payload(7, 7, 4_000);
        long keep;
        try {
            s.setMinLogBytes(1024);
            s.setSpaceAmplification(1);
            s.setSegmentBytes(1 << 20);
            keep = s.put(stable, Fixtures.RAW);
            r = s.put(new byte[60_000], Fixtures.RAW);
            s.commit();
            long maxLen = 0;
            long written = 0;
            for (int i = 0; i < 200; i++) {
                last = Fixtures.payload(i, i, 60_000);
                s.update(r, last, Fixtures.RAW);
                s.commit();
                written += 60_000;
                maxLen = Math.max(maxLen, WalTestKit.logBytes(f));
            }
            long ceiling = 4 * s.getCurrentSize();
            assertTrue("log must stay within the target (max seen " + maxLen + ", ceiling "
                    + ceiling + ")", maxLen < ceiling);
            assertTrue("cleaning must have retired history", WalTestKit.logBytes(f) < written);
            assertTrue("the cleaner must report what it retired", s.cleanerBytesRetired() > 0);
            assertTrue("the cleaner must have re-emitted the untouched record",
                    s.cleanerBytesWritten() > stable.length);
            assertArrayEquals(last, s.get(r, Fixtures.RAW));
            assertArrayEquals("an untouched record survives every retirement",
                    stable, s.get(keep, Fixtures.RAW));
        } finally {
            s.close();
        }

        StoreWAL s2 = new StoreWAL(f);
        try {
            assertArrayEquals(last, s2.get(r, Fixtures.RAW));
            assertArrayEquals(stable, s2.get(keep, Fixtures.RAW));
            s2.verify();
        } finally {
            s2.close();
        }
    }

    @Test public void auto_checkpoint_disabled_grows_log() {
        File f = newFile("auto-off");
        StoreWAL s = new StoreWAL(f);
        try {
            s.setMinLogBytes(0); // disabled
            long r = s.put(new byte[200], Fixtures.RAW);
            s.commit();
            for (int i = 0; i < 30; i++) {
                s.update(r, Fixtures.payload(i, i, 192), Fixtures.RAW);
                s.commit();
            }
            assertTrue("without cleaning the log keeps history", WalTestKit.logBytes(f) > 30 * 200);
        } finally {
            s.close();
        }
    }
}
