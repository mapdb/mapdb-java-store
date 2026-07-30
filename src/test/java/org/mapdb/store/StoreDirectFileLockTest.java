package org.mapdb.store;

import org.junit.After;
import org.junit.Test;
import org.mapdb.DBException;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The exclusive {@code <db>.lock} open lock on the file-backed {@link StoreDirect}: one live
 * store per base pathname, enforced against other processes AND against a second handle in this
 * JVM. StoreDirect has no crash recovery — a second writer's allocations simply overwrite the
 * first's, and the survivor's header checksum still validates — so an admitted second opener is
 * silent corruption, not a detected one.
 *
 * <p>The lock pathname and conventions are {@code WalSegmentSet}'s ({@code <db>.lock},
 * {@code tryLock}, {@link DBException} on refusal), deliberately THE SAME pathname: the invariant
 * is one store per base file, not one store per engine, so StoreDirect and StoreWAL exclude each
 * other over the same base. Pinned by {@link #wal_and_direct_exclude_each_other_over_one_base}.
 */
public class StoreDirectFileLockTest {

    private final List<File> files = new ArrayList<>();

    private File newFile() throws IOException {
        File f = Files.createTempFile("mapdb-lock", ".db").toFile();
        f.delete();
        files.add(f);
        return f;
    }

    private static File lockFileOf(File f) {
        return new File(f.getAbsoluteFile().getPath() + ".lock");
    }

    @After public void cleanup() {
        for (File f : files) {
            f.delete();
            lockFileOf(f).delete();
            File dir = f.getAbsoluteFile().getParentFile();
            String prefix = f.getAbsoluteFile().getName() + ".wal.";
            String[] names = dir == null ? null : dir.list();
            if (names != null)
                for (String n : names) if (n.startsWith(prefix)) new File(dir, n).delete();
        }
        files.clear();
    }

    // ---------- refusal while held ----------

    @Test public void second_open_in_same_jvm_is_refused() throws IOException {
        File f = newFile();
        StoreDirect s = new StoreDirect(f);
        try {
            s.put(1L, org.mapdb.ser.Serializers.LONG);
            s.commit();
            try {
                new StoreDirect(f).close();
                fail("expected the second open of a live store file to be refused");
            } catch (DBException expected) {
                // FileLock is JVM-wide, so the second tryLock throws OverlappingFileLockException
                // rather than returning null; the refusal has to look the same to the caller.
                assertTrue(expected.getMessage(), expected.getMessage().contains("already open in this JVM"));
            }
            // refused, and the LIVE store is untouched by the failed attempt
            assertEquals(Long.valueOf(1L), s.get(1, org.mapdb.ser.Serializers.LONG));
            s.verify();
        } finally {
            s.close();
        }
    }

    /** A non-canonical spelling of the same path is the same lock: the pathname is absolutized. */
    @Test public void relative_and_absolute_spellings_collide() throws IOException {
        File f = newFile();
        StoreDirect s = new StoreDirect(f);
        try {
            File dotted = new File(f.getParentFile(), "." + File.separator + f.getName());
            try {
                new StoreDirect(dotted).close();
                fail("expected ./<name> to be recognized as the same store file");
            } catch (DBException expected) {
                assertTrue(expected.getMessage(), expected.getMessage().contains("already open in this JVM"));
            }
        } finally {
            s.close();
        }
    }

    // ---------- release on close ----------

    @Test public void close_releases_the_lock() throws IOException {
        File f = newFile();
        for (int i = 0; i < 3; i++) {
            StoreDirect s = new StoreDirect(f);
            s.put((long) i, org.mapdb.ser.Serializers.LONG);
            s.commit();
            s.close();
        }
        StoreDirect s = new StoreDirect(f);
        try {
            s.verify();
        } finally {
            s.close();
        }
        assertLockAvailable(f);
    }

    /** close() is idempotent and a second call must not double-release or re-take anything. */
    @Test public void double_close_then_reopen() throws IOException {
        File f = newFile();
        StoreDirect s = new StoreDirect(f);
        s.close();
        s.close();
        assertLockAvailable(f);
        new StoreDirect(f).close();
    }

    /**
     * The lock file is a marker that is created once and never unlinked. Deleting it while a
     * holder is alive would let the next open create a FRESH inode and lock that instead, so two
     * writers would each believe they were alone — the classic unlink race. {@code WalSegmentSet}
     * leaves it behind for the same reason, and only DBMaker's explicit
     * {@code fileDeleteAfterOpen/Close} removes it, along with the store itself.
     */
    @Test public void lock_file_is_created_and_never_deleted() throws IOException {
        File f = newFile();
        File lockFile = lockFileOf(f);
        assertFalse(lockFile.exists());
        StoreDirect s = new StoreDirect(f);
        assertTrue("open creates <db>.lock", lockFile.exists());
        s.close();
        assertTrue("close releases the lock but keeps the marker file", lockFile.exists());
        assertEquals("the lock file is a zero-byte marker", 0L, lockFile.length());
    }

    // ---------- no lock leaked by a failed open ----------

    @Test public void failed_open_bad_magic_does_not_leak_the_lock() throws IOException {
        File f = newFile();
        byte[] garbage = new byte[(int) StoreDirect.PAGE_SIZE]; // >= the header page, so it is read
        for (int i = 0; i < garbage.length; i++) garbage[i] = (byte) (i * 31 + 7);
        Files.write(f.toPath(), garbage);
        try {
            new StoreDirect(f).close();
            fail("expected bad magic to be refused");
        } catch (DBException.DataCorruption expected) { /* refused: correct */ }
        // The constructor threw, so no reference exists and close() can never run: the release
        // has to have happened on the way out, or the pathname stays locked until JVM exit.
        assertLockAvailable(f);
    }

    @Test public void failed_open_short_file_does_not_leak_the_lock() throws IOException {
        File f = newFile();
        Files.write(f.toPath(), new byte[16]); // non-empty but smaller than the header page
        try {
            new StoreDirect(f).close();
            fail("expected a file smaller than the header page to be refused");
        } catch (DBException.DataCorruption expected) { /* refused: correct */ }
        assertLockAvailable(f);
        // and the pathname is reusable for real once the junk is gone
        f.delete();
        new StoreDirect(f).close();
    }

    /** A refused open must not leave the NEXT open refused either (the leak's visible symptom). */
    @Test public void repeated_failed_opens_stay_refusals_not_lock_errors() throws IOException {
        File f = newFile();
        Files.write(f.toPath(), new byte[16]);
        for (int i = 0; i < 3; i++) {
            try {
                new StoreDirect(f).close();
                fail("expected refusal #" + i);
            } catch (DBException.DataCorruption expected) { /* the same refusal every time */ }
        }
    }

    /** Proves the lock is free by taking it the same way the store does. */
    private static void assertLockAvailable(File f) throws IOException {
        File lockFile = lockFileOf(f);
        if (!lockFile.exists()) return; // never created (or deleted): nothing can be held
        try (FileChannel ch = FileChannel.open(lockFile.toPath(), StandardOpenOption.CREATE,
                StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            FileLock l = ch.tryLock(0, Long.MAX_VALUE, false);
            assertNotNull("the store lock on " + lockFile + " is still held", l);
            l.release();
        }
    }

    // ---------- one store per base file, whichever engine ----------

    /**
     * StoreDirect and StoreWAL share the {@code <db>.lock} pathname ON PURPOSE. Both engines own
     * the base pathname exclusively — StoreWAL checkpoints rewrite {@code <db>} wholesale — so
     * letting a StoreDirect writer in beside a StoreWAL one (in either order) would corrupt the
     * store just as surely as two StoreDirects would. StoreWAL takes the lock exactly once, in
     * {@code WalSegmentSet}: its inner {@link StoreDirect} is memory-backed and takes none, so
     * there is no self-collision to work around.
     */
    @Test public void wal_and_direct_exclude_each_other_over_one_base() throws IOException {
        File f = newFile();
        StoreDirect direct = new StoreDirect(f);
        try {
            try {
                new StoreWAL(f).close();
                fail("expected StoreWAL to be refused while StoreDirect holds the base file");
            } catch (DBException expected) {
                assertTrue(expected.getMessage(), expected.getMessage().contains("already open in this JVM"));
            }
        } finally {
            direct.close();
        }

        File g = newFile();
        StoreWAL wal = new StoreWAL(g);
        try {
            try {
                new StoreDirect(g).close();
                fail("expected StoreDirect to be refused while StoreWAL holds the base file");
            } catch (DBException expected) {
                assertTrue(expected.getMessage(), expected.getMessage().contains("already open in this JVM"));
            }
        } finally {
            wal.close();
        }
        // both released: each pathname reopens with its own engine
        new StoreDirect(g).close();
    }

    // ---------- memory-backed stores take no lock ----------

    @Test public void memory_stores_are_unlocked_and_unaffected() {
        StoreDirect a = new StoreDirect(false);
        StoreDirect b = new StoreDirect(false);
        StoreDirect c = new StoreDirect(true, false);
        try {
            // No pathname, nothing to contend on: any number of them coexist.
            a.put(1L, org.mapdb.ser.Serializers.LONG);
            b.put(2L, org.mapdb.ser.Serializers.LONG);
            c.put(3L, org.mapdb.ser.Serializers.LONG);
            assertEquals(Long.valueOf(1L), a.get(1, org.mapdb.ser.Serializers.LONG));
            assertEquals(Long.valueOf(2L), b.get(1, org.mapdb.ser.Serializers.LONG));
        } finally {
            a.close();
            b.close();
            c.close();
        }
    }

    // ---------- compaction runs under the held lock ----------

    /**
     * {@code executeCompaction} rebuilds the volume IN PLACE — no temp file, no rename — so the
     * lock is neither dropped nor re-established across it. What this pins is that the lock is
     * still held afterwards (a compaction that closed/reopened the volume would have dropped it)
     * and that the store still reopens once released.
     */
    @Test public void compaction_keeps_the_lock_held() throws IOException {
        File f = newFile();
        StoreDirect s = new StoreDirect(f);
        try {
            List<Long> doomed = new ArrayList<>();
            for (int i = 0; i < 40; i++) doomed.add(s.put(Fixtures.payload(i, 1, 120_000), Fixtures.RAW));
            long keep = s.put(Fixtures.payload(99, 1, 4_000), Fixtures.RAW);
            for (long r : doomed) s.delete(r, Fixtures.RAW);
            s.commit();
            s.compact();
            s.verify();
            try {
                new StoreDirect(f).close();
                fail("expected the store to still be locked after compaction");
            } catch (DBException expected) {
                assertTrue(expected.getMessage(), expected.getMessage().contains("already open in this JVM"));
            }
            assertTrue(s.get(keep, Fixtures.RAW).length > 0);
        } finally {
            s.close();
        }
        assertLockAvailable(f);
        new StoreDirect(f).close();
    }

    /**
     * A compaction fault POISONS the store: it is already {@code closed}, and the caller's
     * {@code close()} must release resources without resealing the half-rebuilt volume as clean.
     * The lock has to come back on that path too — otherwise a faulted store locks its pathname
     * out for the rest of the JVM's life and the repair reopen can never even be attempted.
     */
    @Test public void poisoned_close_after_a_faulted_compaction_releases_the_lock() throws IOException {
        File f = newFile();
        StoreDirect s = new StoreDirect(f);
        try {
            List<Long> doomed = new ArrayList<>();
            for (int i = 0; i < 40; i++) doomed.add(s.put(Fixtures.payload(i, 1, 120_000), Fixtures.RAW));
            for (long r : doomed) s.delete(r, Fixtures.RAW);
            s.commit();
            StoreDirect.testCompactCrashHook = () -> { throw new RuntimeException("simulated crash"); };
            try {
                s.compactStep(64);
                fail("expected the simulated compaction crash");
            } catch (RuntimeException expected) {
                if (!"simulated crash".equals(expected.getMessage())) throw expected;
            } finally {
                StoreDirect.testCompactCrashHook = null;
            }
            assertTrue("a faulted compaction poisons and closes the store", s.isClosed());
            // Still HELD: the fault closed the store but nobody has called close() yet, so the
            // release below is the one that matters — not something that already happened.
            try {
                new StoreDirect(f).close();
                fail("expected a poisoned-but-not-closed store to keep holding the lock");
            } catch (DBException expected) {
                assertTrue(expected.getMessage(), expected.getMessage().contains("already open in this JVM"));
            }
        } finally {
            StoreDirect.testCompactCrashHook = null;
            s.close();
        }
        assertLockAvailable(f);
        // The lock is back, and close() still did NOT reseal the poisoned volume: the reopen it
        // permits is a refusal, which is the outcome the release exists to make reachable.
        try {
            new StoreDirect(f).close();
            fail("expected the poisoned store to be refused on reopen");
        } catch (DBException.DataCorruption expected) { /* refused: correct */ }
        assertLockAvailable(f);
    }

    // ---------- through the DB facade ----------

    /**
     * {@code DBMaker.readOnly()} is a LOGICAL wrapper ({@code StoreReadOnlyWrapper}) over a
     * volume that is still mapped read-write, so it takes the same exclusive lock as any other
     * open — and its close() has to give it back through the wrapper.
     */
    @Test public void db_readonly_open_is_exclusive_and_releases_on_close() throws IOException {
        File f = newFile();
        org.mapdb.db.DB db = org.mapdb.db.DBMaker.fileDB(f).make();
        db.hashMap("m", org.mapdb.ser.Serializers.LONG, org.mapdb.ser.Serializers.STRING)
                .create().put(1L, "one");
        db.commit();
        db.close();

        org.mapdb.db.DB ro = org.mapdb.db.DBMaker.fileDB(f).readOnly().make();
        try {
            try {
                new StoreDirect(f).close();
                fail("expected a read-only DB to still hold the base file exclusively");
            } catch (DBException expected) {
                assertTrue(expected.getMessage(), expected.getMessage().contains("already open in this JVM"));
            }
        } finally {
            ro.close();
        }
        assertLockAvailable(f);
    }

    // ---------- true cross-process refusal ----------

    /**
     * The in-JVM cases above never exercise the OS lock at all: {@code tryLock} short-circuits on
     * {@code OverlappingFileLockException} before the kernel is asked. Only a second PROCESS
     * proves the lock is actually visible across processes, which is the whole point of it, so
     * this forks a JVM on the same classpath and has it try the open.
     */
    @Test(timeout = 300_000) public void second_open_from_another_process_is_refused() throws Exception {
        File f = newFile();
        StoreDirect s = new StoreDirect(f);
        String whileHeld;
        try {
            s.put(1L, org.mapdb.ser.Serializers.LONG);
            s.commit();
            whileHeld = runChildOpen(f);
        } finally {
            s.close();
        }
        assertEquals("a second OS process must be refused while the store is open",
                "REFUSED", whileHeld.split(":", 2)[0]);
        assertTrue(whileHeld, whileHeld.contains("locked by another process"));
        // and the same process succeeds once the lock is released
        assertEquals("OK", runChildOpen(f).split(":", 2)[0]);
    }

    /** Runs {@link LockProbe} in a fresh JVM against {@code f}; returns its one-line verdict. */
    private static String runChildOpen(File f) throws Exception {
        File javaBin = new File(new File(System.getProperty("java.home"), "bin"), "java");
        // Output to a FILE, not a pipe: reading a pipe to EOF before waitFor() would block
        // forever on a child that wedges with stdout still open, and the timeout below would
        // never get a chance to fire — a silent CI hang instead of a failed test.
        File out = File.createTempFile("mapdb-lockprobe", ".out");
        ProcessBuilder pb = new ProcessBuilder(javaBin.getPath(), "-cp",
                System.getProperty("java.class.path"), LockProbe.class.getName(), f.getPath());
        pb.redirectErrorStream(true);
        pb.redirectOutput(out);
        Process p = pb.start();
        String text;
        try {
            if (!p.waitFor(120, TimeUnit.SECONDS)) fail("the lock probe process did not finish");
            text = new String(Files.readAllBytes(out.toPath()), StandardCharsets.UTF_8).trim();
        } finally {
            p.destroyForcibly();
            out.delete();
        }
        String verdict = null;
        for (String line : text.split("\n")) {
            String t = line.trim();
            if (t.startsWith("REFUSED") || t.startsWith("OK") || t.startsWith("OTHER")) verdict = t;
        }
        assertNotNull("no verdict from the lock probe; output was:\n" + text, verdict);
        return verdict;
    }

    /**
     * Child-JVM entry point: opens the store at {@code argv[0]} and prints a one-line verdict.
     * A separate process is the only way to exercise the kernel-level {@code FileLock}.
     */
    public static final class LockProbe {
        public static void main(String[] argv) {
            try {
                new StoreDirect(new File(argv[0])).close();
                System.out.println("OK");
            } catch (DBException e) {
                String msg = String.valueOf(e.getMessage());
                System.out.println(msg.contains("locked by another process")
                        ? "REFUSED:" + msg : "OTHER:" + e.getClass().getName() + ":" + msg);
            } catch (Throwable e) {
                System.out.println("OTHER:" + e.getClass().getName() + ":" + e.getMessage());
            }
        }
    }
}
