package org.mapdb.store;

import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** StoreDirect physical compaction: live records are copied densely and free lists rebuilt. */
public class StoreDirectCompactionTest {

    private final List<File> files = new ArrayList<>();

    private File newFile() throws IOException {
        File f = Files.createTempFile("mapdb-compact", ".db").toFile();
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

    @Test public void direct_compact_reclaims_deleted_plain_and_linked_pages() throws IOException {
        File f = newFile();
        byte[] liveSmall = Fixtures.payload(1, 1, 321);
        byte[] liveLinked = Fixtures.payload(2, 1, 2_200_000);
        long rDead1, rLiveSmall, rDead2, rLiveLinked, beforeTail;

        StoreDirect s = new StoreDirect(f);
        try {
            rDead1 = s.put(Fixtures.payload(3, 1, 2_700_000), Fixtures.RAW);
            rLiveSmall = s.put(liveSmall, Fixtures.RAW);
            rDead2 = s.put(Fixtures.payload(4, 1, 2_900_000), Fixtures.RAW);
            rLiveLinked = s.put(liveLinked, Fixtures.RAW);
            s.delete(rDead1, Fixtures.RAW);
            s.delete(rDead2, Fixtures.RAW);
            beforeTail = s.testFileTail();

            s.compact();

            assertTrue("compaction should shrink logical file tail", s.testFileTail() < beforeTail);
            assertArrayEquals(liveSmall, s.get(rLiveSmall, Fixtures.RAW));
            assertArrayEquals(liveLinked, s.get(rLiveLinked, Fixtures.RAW));
            assertGetVoid(() -> s.get(rDead1, Fixtures.RAW));
            assertGetVoid(() -> s.get(rDead2, Fixtures.RAW));
            s.verify();
        } finally {
            s.close();
        }

        StoreDirect reopened = new StoreDirect(f);
        try {
            assertEquals("close keeps compacted physical length", f.length(), reopened.testFileTail());
        } finally {
            reopened.close();
        }
    }

    @Test public void compact_preserves_null_prealloc_and_append_headroom() {
        StoreDirect s = new StoreDirect();
        try {
            long nullRec = s.put(null, Fixtures.RAW);
            long pre = s.preallocate();
            byte[] base = Fixtures.payload(5, 1, 100);
            long rec = s.put(base, Fixtures.RAW);
            s.updateWithHeadroom(rec, base, Fixtures.RAW, 512);
            long capBefore = s.capacityRemaining(rec);

            s.compact();

            assertNull(s.get(nullRec, Fixtures.RAW));
            assertNull(s.get(pre, Fixtures.RAW));
            assertFalse("preallocated recids stay excluded", containsRecid(s, pre));
            assertArrayEquals(base, s.get(rec, Fixtures.RAW));
            assertEquals("plain record capacity must survive relocation",
                    capBefore, s.capacityRemaining(rec));
            byte[] extra = Fixtures.payload(5, 2, 128);
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

    @Test public void compacted_file_reopens_and_reuses_deleted_recids() throws IOException {
        File f = newFile();
        long deleted, live, reused;
        byte[] payload = Fixtures.payload(6, 1, 10_000);
        StoreDirect s = new StoreDirect(f);
        try {
            deleted = s.put(Fixtures.payload(6, 2, 80_000), Fixtures.RAW);
            live = s.put(payload, Fixtures.RAW);
            s.delete(deleted, Fixtures.RAW);
            s.compact();
            s.close();
        } finally {
            if (!s.isClosed()) s.close();
        }

        StoreDirect reopened = new StoreDirect(f);
        try {
            assertArrayEquals(payload, reopened.get(live, Fixtures.RAW));
            assertGetVoid(() -> reopened.get(deleted, Fixtures.RAW));
            reused = reopened.put(Fixtures.payload(6, 3, 123), Fixtures.RAW);
            assertEquals("free recid stack rebuilt by compaction", deleted, reused);
            reopened.verify();
        } finally {
            reopened.close();
        }
    }

    /**
     * A throw mid-write-back leaves compact() with no rollback; the store must be poisoned so
     * a subsequent ordinary close() cannot stamp a valid checksum over the half-rebuilt state
     * (which would reopen "clean" with every not-yet-rewritten record silently gone).
     */
    @Test public void compact_faulted_midway_poisons_store_and_reopen_refuses() throws IOException {
        File f = newFile();
        StoreDirect s = new StoreDirect(f);
        try {
            for (int i = 0; i < 20; i++) s.put(Fixtures.payload(i, 1, 10_000), Fixtures.RAW);
            s.commit();

            int[] calls = {0};
            StoreDirect.testCompactCrashHook = () -> {
                if (++calls[0] == 8) throw new RuntimeException("simulated compact fault");
            };
            try {
                s.compact();
                fail("expected simulated compact fault");
            } catch (RuntimeException expected) {
                assertEquals("simulated compact fault", expected.getMessage());
            } finally {
                StoreDirect.testCompactCrashHook = null;
            }

            // poisoned: further use refused, and close() must not seal the half-rebuilt state
            assertTrue(s.isClosed());
            s.close(); // no-op, must not stamp
            try {
                new StoreDirect(f).close();
                fail("expected reopen of a faulted compact() to refuse");
            } catch (org.mapdb.DBException.DataCorruption expected) { /* refused: correct */ }
        } finally {
            StoreDirect.testCompactCrashHook = null;
        }
    }

    /**
     * compact() rebuilds in place over reused extents with no rollback, so BEFORE its first
     * volume mutation the on-disk header checksum must already be durably inverted (the same
     * step-0 barrier as executeCompaction). Otherwise a power loss that writes data pages back
     * but not the header page reopens FALSE-CLEAN under the last commit's checksum, over
     * clobbered extents. Simulated here: snapshot the data pages mid-rebuild via the crash
     * hook, pair them with the commit-time header page (= "header page never hit disk") plus
     * the checksum word as the barrier left it, and reopen — it must be refused.
     */
    @Test public void compact_inverts_ondisk_checksum_before_first_mutation() throws IOException {
        File f = newFile();
        StoreDirect s = new StoreDirect(f);
        try {
            List<Long> doomed = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                s.put(Fixtures.payload(i, 1, 10_000), Fixtures.RAW);
                doomed.add(s.put(Fixtures.payload(i, 2, 40_000), Fixtures.RAW));
            }
            for (long r : doomed) s.delete(r, Fixtures.RAW);
            s.commit(); // clean, valid on-disk state before compaction starts

            byte[] headerAtCommit = new byte[(int) StoreDirect.PAGE_SIZE];
            System.arraycopy(Files.readAllBytes(f.toPath()), 0, headerAtCommit, 0, headerAtCommit.length);
            int validChecksum = java.nio.ByteBuffer.wrap(headerAtCommit)
                    .getInt((int) StoreDirect.O_HEAD_CHECKSUM);

            // mid-rebuild, capture the volume as a crash image (several records already rewritten)
            byte[][] crashImage = {null};
            int[] calls = {0};
            StoreDirect.testCompactCrashHook = () -> {
                if (++calls[0] == 8) {
                    try {
                        crashImage[0] = Files.readAllBytes(f.toPath());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            };
            try {
                s.compact();
            } finally {
                StoreDirect.testCompactCrashHook = null;
            }
            assertTrue("crash hook must have fired mid-rebuild", crashImage[0] != null);

            // the ONLY checksum write between commit and the hook is the barrier's, and the
            // barrier syncs it: the mid-rebuild on-disk checksum must already be the inversion
            int checksumMidRebuild = java.nio.ByteBuffer.wrap(crashImage[0])
                    .getInt((int) StoreDirect.O_HEAD_CHECKSUM);
            assertEquals("pre-mutation barrier must durably invert the header checksum",
                    ~validChecksum, checksumMidRebuild);

            // power-loss image: mid-rebuild data pages + commit-time header page, with the
            // checksum word as the (durably synced) barrier left it
            System.arraycopy(headerAtCommit, 0, crashImage[0], 0, headerAtCommit.length);
            java.nio.ByteBuffer.wrap(crashImage[0])
                    .putInt((int) StoreDirect.O_HEAD_CHECKSUM, checksumMidRebuild);
            File crash = newFile();
            Files.write(crash.toPath(), crashImage[0]);
            try {
                new StoreDirect(crash).close();
                fail("expected reopen of a mid-compact power-loss image to refuse");
            } catch (org.mapdb.DBException.DataCorruption expected) { /* refused: correct */ }
        } finally {
            StoreDirect.testCompactCrashHook = null;
            s.close();
        }
    }

    private static boolean containsRecid(StoreDirect s, long recid) {
        java.util.PrimitiveIterator.OfLong it = s.getAllRecids();
        while (it.hasNext()) if (it.nextLong() == recid) return true;
        return false;
    }
}
