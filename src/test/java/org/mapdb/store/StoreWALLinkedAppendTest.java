package org.mapdb.store;

import org.junit.After;
import org.junit.Test;
import org.mapdb.TmpFiles;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Regression: a zero-length {@code append()} on a COMMITTED oversize/linked record used to
 * crash the writer.
 *
 * <p>The path was: {@code StoreWAL.append} only enforces refusal when no base is staged, and
 * its check was {@code s.appendsLen + len > capRem} — with {@code capRem == 0} for a linked
 * record and {@code len == 0} that is {@code 0 > 0}, i.e. false. So the empty append was
 * staged, {@code commit()} classified the recid as {@code T_APPEND}, and the apply loop hit
 * {@code inner.append(...) == REFUSED} (linked records refuse every append) and threw
 * {@code AssertionError("commit append refused")} — a writer-side crash on a legal,
 * contract-defined no-op.
 *
 * <p>The fix stages nothing for a zero-length append, in both StoreWAL and StoreDirect, so
 * the no-op is shape-independent. {@code DeltaTCK.zero_length_append_after_commit_is_noop}
 * covers the shared half; the linked shape is pinned here and in
 * {@code StoreDirectLinkedTest.zero_length_append_on_linked_is_noop}.
 */
public class StoreWALLinkedAppendTest {

    /** Comfortably past the plain-record ceiling, so the record is stored as a linked chain. */
    private static final int LINKED_SIZE = IndexVal.MAX_CAPACITY + 100;

    private final List<File> files = new ArrayList<>();

    private File newFile(String tag) {
        try {
            File f = TmpFiles.tempFile("mapdb-wal-linked-" + tag, ".wal");
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

    private static byte[] bytes(int n, int seed) {
        byte[] b = new byte[n];
        for (int i = 0; i < n; i++) b[i] = (byte) (i * 31 + seed);
        return b;
    }

    @Test public void zero_length_append_on_committed_linked_record_does_not_crash_commit() {
        File f = newFile("zero");
        byte[] v = bytes(LINKED_SIZE, 7);
        StoreWAL s = new StoreWAL(f, false, true);
        try {
            long r = s.put(v, Fixtures.RAW);
            s.commit();                       // record is now LIVE and linked in inner
            assertEquals("linked records expose no append capacity", 0, s.capacityRemaining(r));

            assertEquals("zero-length append returns current size", v.length,
                    s.append(r, new byte[0], 0, 0));
            assertEquals("zero-length append at a non-zero offset is also a no-op", v.length,
                    s.append(r, new byte[]{9, 9, 9}, 2, 0));

            s.commit();                       // used to throw AssertionError("commit append refused")
            assertArrayEquals("content untouched", v, s.get(r, Fixtures.RAW));
            s.verify();
        } finally {
            s.close();
        }

        // and the log still replays to the same state
        StoreWAL s2 = new StoreWAL(f, false, true);
        try {
            long r = s2.getAllRecids().nextLong();
            assertArrayEquals("survives reopen", v, s2.get(r, Fixtures.RAW));
            s2.verify();
        } finally {
            s2.close();
        }
    }

    /**
     * A zero-length append must not leave an empty staged entry behind. It used to:
     * {@code stagedForWrite} inserts a {@code Staged} for any untouched recid, so a
     * contract-defined no-op turned into a {@code T_PREALLOC} section at commit — a WAL
     * write, and an LSN burned, for an operation that changed nothing. (Content was
     * unchanged either way, which is why the other tests never caught it.)
     */
    @Test public void zero_length_append_writes_no_wal_section() {
        File f = newFile("nosection");
        StoreWAL s = new StoreWAL(f, false, true);
        try {
            byte[] v = bytes(64, 3);
            long r = s.put(v, Fixtures.RAW);
            s.commit();
            long lenAfterCommit = f.length();

            assertEquals(v.length, s.append(r, new byte[0], 0, 0));
            s.commit();                    // nothing staged => commit must be a no-op

            assertEquals("a zero-length append must not grow the log",
                    lenAfterCommit, f.length());
            assertArrayEquals(v, s.get(r, Fixtures.RAW));
            s.verify();
        } finally {
            s.close();
        }
    }

    @Test public void non_empty_append_on_committed_linked_record_is_still_refused() {
        File f = newFile("refuse");
        StoreWAL s = new StoreWAL(f, false, true);
        try {
            byte[] v = bytes(LINKED_SIZE, 11);
            long r = s.put(v, Fixtures.RAW);
            s.commit();
            assertEquals(StoreDelta.REFUSED, s.append(r, new byte[]{1}, 0, 1));
            s.commit();
            assertArrayEquals("content untouched by refused append", v, s.get(r, Fixtures.RAW));
            s.verify();
        } finally {
            s.close();
        }
    }
}
