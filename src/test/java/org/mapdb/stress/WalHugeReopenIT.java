package org.mapdb.stress;

import org.junit.After;
import org.junit.Test;
import org.mapdb.store.StoreWAL;
import org.mapdb.store.WalTestKit;

import java.io.File;

import static org.mapdb.stress.StressSupport.*;

/**
 * Over-2-GiB WAL reopen (the old milestone-1 ceiling: whole-log Files.readAllBytes was
 * capped at Integer.MAX_VALUE). Builds a log comfortably past 2 GiB out of ~0.9 MiB
 * records with cleaning DISABLED so the raw log size is what is tested, then reopens
 * (streaming replay) and verifies every record's self-validating payload.
 *
 * Under the v3 format the log is a SEGMENT SET ({@code <db>.wal.<16 hex>}), not one file,
 * so "log size" is the sum over {@link WalTestKit#logBytes} — the base file itself is
 * never written. The rollover threshold is pinned to {@code Long.MAX_VALUE} in both write
 * phases so each phase produces ONE segment past 2 GiB: replay must then read the channel
 * at per-file offsets above Integer.MAX_VALUE, which the default ~64 MiB segments never
 * would however large the log grew. (A single section BODY over 2 GiB is not constructible
 * by this writer — checkpoint images are chunked ~1 MiB and DataOutput2 is byte[]-backed —
 * so the int64 bodyLen stays covered by format headroom, not by this test.)
 *
 * Manual run (direct buffers keep the 2.3+ GiB volume off-heap):
 *   java -ea -Xmx2g -XX:MaxDirectMemorySize=8g \
 *     -cp target/test-classes:target/classes org.junit.runner.JUnitCore \
 *     org.mapdb.stress.WalHugeReopenIT
 * (or: mvn -P integration-tests verify -Dit.test=WalHugeReopenIT)
 */
public class WalHugeReopenIT {

    static final int RECSIZE = 900 * 1024;              // under the ~1 MiB record cap
    static final long TARGET = (1L << 31) + (200L << 20); // 2 GiB + 200 MiB of log
    /** Commits move the log in 64-record batches, so allow one whole batch of overshoot. */
    static final int MAX_RECS = (int) (TARGET / RECSIZE) + 128;

    private File walFile;

    @After public void cleanup() {
        if (walFile != null) WalTestKit.deleteStore(walFile);
    }

    @Test public void reopen_wal_over_2gib() throws Exception {
        walFile = File.createTempFile("stress-wal-huge", ".wal");
        walFile.delete();

        long[] recids = new long[MAX_RECS];
        int n = 0;

        StoreWAL store = new StoreWAL(walFile, true); // direct buffers: volume off-heap
        long t0 = System.nanoTime();
        try {
            store.setMinLogBytes(0); // cleaning off: every committed section is retained
            store.setSegmentBytes(Long.MAX_VALUE); // no rollover: one segment past 2 GiB
            while (WalTestKit.logBytes(walFile) < TARGET) {
                for (int i = 0; i < 64; i++) {
                    if (n == recids.length)
                        throw new AssertionError("log did not reach " + TARGET + " bytes after "
                                + n + " records; segment set holds " + WalTestKit.logBytes(walFile));
                    long recid = store.preallocate();
                    store.update(recid, payload(recid, 1, RECSIZE), RAW);
                    recids[n++] = recid;
                }
                store.commit();
            }
        } finally {
            store.close();
        }
        long logBytes = WalTestKit.logBytes(walFile);
        if (logBytes <= Integer.MAX_VALUE)
            throw new AssertionError("log not past 2 GiB: " + logBytes);
        if (WalTestKit.segments(walFile).length != 1)
            throw new AssertionError("rollover was pinned off, expected ONE over-2-GiB segment,"
                    + " got " + WalTestKit.segments(walFile).length);
        System.out.printf("[STRESS] huge WAL built: %,d records, %d segments, log=%.2f GiB in %.1fs%n",
                n, WalTestKit.segments(walFile).length,
                logBytes / 1024.0 / 1024 / 1024, (System.nanoTime() - t0) / 1e9);

        long t1 = System.nanoTime();
        StoreWAL reopened = new StoreWAL(walFile, true);
        double replaySec = (System.nanoTime() - t1) / 1e9;
        try {
            verifyAll(reopened, recids, n);
            reopened.verify();
            System.out.printf("[STRESS] huge WAL reopened: replay=%.2fs (%.1f MB/s), %,d records verified%n",
                    replaySec, logBytes / 1024.0 / 1024 / replaySec, n);
            // checkpoint: re-homes the whole committed store as ~1 MiB 'C' image sections and
            // retires every pre-existing segment. The live data is > 2 GiB, so the retained
            // image log must be too — and with rollover pinned off it is again ONE segment,
            // so the third open replays 'C' sections from past-2-GiB offsets as well.
            reopened.setSegmentBytes(Long.MAX_VALUE);
            long t2 = System.nanoTime();
            reopened.checkpoint();
            long imageBytes = WalTestKit.logBytes(walFile);
            System.out.printf("[STRESS] checkpoint: %.1fs, log=%.2f GiB in %d segments%n",
                    (System.nanoTime() - t2) / 1e9, imageBytes / 1024.0 / 1024 / 1024,
                    WalTestKit.segments(walFile).length);
            if (imageBytes <= Integer.MAX_VALUE)
                throw new AssertionError("checkpoint log unexpectedly small: " + imageBytes);
            if (WalTestKit.segments(walFile).length != 1)
                throw new AssertionError("rollover was pinned off, expected the checkpoint image"
                        + " in ONE over-2-GiB segment, got " + WalTestKit.segments(walFile).length);
        } finally {
            reopened.close();
        }

        long t3 = System.nanoTime();
        StoreWAL again = new StoreWAL(walFile, true); // replay of a > 2 GiB checkpoint image
        double replay2 = (System.nanoTime() - t3) / 1e9;
        try {
            verifyAll(again, recids, n);
            again.verify();
            System.out.printf("[STRESS] checkpoint image reopened: replay=%.2fs (%.1f MB/s)%n",
                    replay2, WalTestKit.logBytes(walFile) / 1024.0 / 1024 / replay2);
        } finally {
            again.close();
        }
    }

    private static void verifyAll(StoreWAL store, long[] recids, int n) {
        for (int i = 0; i < n; i++) {
            long recid = recids[i];
            byte[] got = store.get(recid, RAW);
            if (got == null || got.length != RECSIZE || getLong(got, 0) != recid)
                throw new AssertionError("bad payload after replay, recid=" + recid);
            if (getInt(got, 16) != crc32(got, RECSIZE))
                throw new AssertionError("CRC mismatch after replay, recid=" + recid);
        }
    }
}
