package org.mapdb.store;

import org.junit.After;
import org.junit.Test;
import org.mapdb.DBException;
import org.mapdb.TmpFiles;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

/**
 * Capacity arithmetic must never wrap an int: {@code updateWithHeadroom} with a huge
 * headroom used to compute {@code 4 + len + headroom} in int, go negative, pass the
 * old {@code checkSize} (which only rejected {@code > MAX_CAPACITY}) and could persist
 * a garbage capacity into the WAL / move the allocator's dataTail backwards. Both
 * stores must reject it up front with {@link DBException.RecordTooLarge}, and reject
 * negative headroom outright, leaving the store fully usable.
 */
public class CapacityOverflowTest {

    private File walFile;

    @After public void cleanup() {
        TmpFiles.delete(walFile);
    }

    @Test public void store_direct_rejects_headroom_overflow_and_stays_usable() {
        StoreDirect s = new StoreDirect(false);
        try {
            byte[] v = Fixtures.payload(1, 1, 8);
            long r = s.put(v, Fixtures.RAW);
            try {
                s.updateWithHeadroom(r, v, Fixtures.RAW, Integer.MAX_VALUE);
                fail("expected RecordTooLarge");
            } catch (DBException.RecordTooLarge expected) { /* ok */ }
            try {
                s.updateWithHeadroom(r, v, Fixtures.RAW, -1);
                fail("expected IllegalArgumentException");
            } catch (IllegalArgumentException expected) { /* ok */ }
            assertArrayEquals(v, s.get(r, Fixtures.RAW)); // untouched
            s.verify();
        } finally {
            s.close();
        }
    }

    @Test public void store_wal_rejects_headroom_overflow_before_logging() throws IOException {
        walFile = TmpFiles.tempFile("mapdb-cap-ovf", ".wal");
        walFile.delete();
        StoreWAL s = new StoreWAL(walFile);
        try {
            byte[] v = Fixtures.payload(1, 1, 8);
            long r = s.put(v, Fixtures.RAW);
            s.commit();
            try {
                s.updateWithHeadroom(r, v, Fixtures.RAW, Integer.MAX_VALUE - 4);
                fail("expected RecordTooLarge");
            } catch (DBException.RecordTooLarge expected) { /* ok */ }
            try {
                s.updateWithHeadroom(r, v, Fixtures.RAW, Integer.MIN_VALUE);
                fail("expected IllegalArgumentException");
            } catch (IllegalArgumentException expected) { /* ok */ }
            // nothing bad staged: commit + reopen keep the committed value
            s.update(r, Fixtures.payload(1, 2, 8), Fixtures.RAW);
            s.commit();
            s.verify();
        } finally {
            s.close();
        }

        StoreWAL s2 = new StoreWAL(walFile);
        try {
            assertArrayEquals(Fixtures.payload(1, 2, 8), s2.get(1, Fixtures.RAW));
            s2.verify();
        } finally {
            s2.close();
        }
    }
}
