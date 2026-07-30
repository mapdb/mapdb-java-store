package org.mapdb.ported;

import org.junit.Test;
import org.mapdb.DBException;
import org.mapdb.TmpFiles;
import org.mapdb.store.StoreDirect;
import org.mapdb.store.StoreWAL;

import java.io.File;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

/**
 * The ~1 MiB plain-record boundary. Records up to MAX_CONTENT are
 * single plain records; records ABOVE it now round-trip as LINKED chunk chains
 * (the mapdb3 linked-record mechanism), so
 * DBException.RecordTooLarge no longer fires for oversize content. It still
 * fires where the plain-capacity contract would otherwise silently break:
 * updateWithHeadroom whose headroom cannot be honoured (see CapacityOverflowTest —
 * linked records refuse all appends, so going linked would void the guarantee).
 */
public class PortedRecordTooLargeTest {

    @Test public void direct_max_size_roundtrips() {
        StoreDirect s = new StoreDirect();
        try {
            byte[] max = Ported.bytes(Ported.MAX_CONTENT, 1);
            long r = s.put(max, Ported.RAW);
            assertArrayEquals(max, s.get(r, Ported.RAW));
            s.verify();
            // update to the exact max also fits
            byte[] max2 = Ported.bytes(Ported.MAX_CONTENT, 2);
            s.update(r, max2, Ported.RAW);
            assertArrayEquals(max2, s.get(r, Ported.RAW));
            s.verify();
        } finally { s.close(); }
    }

    @Test public void direct_put_over_cap_roundtrips_as_linked() {
        StoreDirect s = new StoreDirect();
        try {
            byte[] over = Ported.bytes(Ported.MAX_CONTENT + 1, 3);
            long r = s.put(over, Ported.RAW);
            assertArrayEquals(over, s.get(r, Ported.RAW));
            s.verify();
            byte[] big = Ported.bytes(3 * Ported.MAX_CONTENT + 100, 4);
            s.update(r, big, Ported.RAW);
            assertArrayEquals(big, s.get(r, Ported.RAW));
            s.verify();
            s.delete(r, Ported.RAW);
            s.verify();
        } finally { s.close(); }
    }

    // Crossing the boundary in both directions on one recid must relocate cleanly.
    @Test public void direct_update_across_cap_boundary() {
        StoreDirect s = new StoreDirect();
        try {
            byte[] small = Ported.bytes(1000, 4);
            long r = s.put(small, Ported.RAW);
            byte[] over = Ported.bytes(Ported.MAX_CONTENT + 17, 5);
            s.update(r, over, Ported.RAW);
            assertArrayEquals(over, s.get(r, Ported.RAW));
            s.verify();
            s.update(r, small, Ported.RAW);
            assertArrayEquals(small, s.get(r, Ported.RAW));
            s.verify();
        } finally { s.close(); }
    }

    // Headroom that cannot be honoured still throws — BEFORE any mutation.
    @Test public void direct_unsatisfiable_headroom_throws_and_leaves_old_intact() {
        StoreDirect s = new StoreDirect();
        try {
            byte[] small = Ported.bytes(1000, 6);
            long r = s.put(small, Ported.RAW);
            try {
                s.updateWithHeadroom(r, small, Ported.RAW, Ported.MAX_CONTENT);
                fail("expected RecordTooLarge");
            } catch (DBException.RecordTooLarge expected) { /* ok */ }
            assertArrayEquals("old value must survive a rejected update",
                    small, s.get(r, Ported.RAW));
            s.verify();
        } finally { s.close(); }
    }

    // WAL stores oversize merged records as linked chains at commit and replays them.
    @Test public void wal_over_cap_commits_and_survives_reopen() throws Exception {
        File f = TmpFiles.tempFile("mapdb-ported-toolarge", ".wal");
        f.delete();
        byte[] over = Ported.bytes(Ported.MAX_CONTENT + 1, 7);
        long r;
        StoreWAL s = new StoreWAL(f);
        try {
            r = s.put(over, Ported.RAW);
            s.commit();
            assertArrayEquals(over, s.get(r, Ported.RAW));
            s.verify();
        } finally {
            try { s.close(); } catch (Throwable ignore) {}
        }
        StoreWAL s2 = new StoreWAL(f);
        try {
            assertArrayEquals("oversize record must survive WAL replay", over, s2.get(r, Ported.RAW));
            s2.verify();
        } finally {
            try { s2.close(); } catch (Throwable ignore) {}
            f.delete();
            org.mapdb.store.WalTestKit.deleteStore(f);
        }
    }
}
