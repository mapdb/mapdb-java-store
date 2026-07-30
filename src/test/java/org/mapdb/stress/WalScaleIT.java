package org.mapdb.stress;

import org.junit.After;
import org.junit.Test;
import org.mapdb.store.StoreWAL;

import java.io.File;
import java.util.SplittableRandom;

import static org.mapdb.stress.StressSupport.*;

/**
 * WAL scale + recovery. Builds ~2 GB of committed data across 200 commits (~10 MB per
 * commit: new records + updates + appends), closes, then times the reopen (full replay
 * of every committed section) and prints replay throughput in MB/s. Records are
 * self-validating; 200,000 random ones are verified post-reopen.
 *
 * Full-scale (scale=1.0): 200 commits x 8300 new 1 KiB records => ~1.66M records.
 *
 * StoreWAL recovery is streaming (fixed-size window over the channel, long offsets), so
 * the old 2 GiB reopen ceiling (whole-log Files.readAllBytes) is gone. This IT stays at
 * ~1.7 GB of committed data so it fits the suite's heap-backed volume under -Xmx2g;
 * {@link WalHugeReopenIT} is the dedicated over-2-GiB reopen check (direct buffers,
 * needs -XX:MaxDirectMemorySize).
 */
public class WalScaleIT {

    static final int COMMITS   = 200;
    static final int NEW_PER   = 8300;
    static final int UPD_PER   = 1000;
    static final int APP_PER   = 500;
    static final int RECSIZE   = 1024;
    static final int APPEND    = 32;
    static final long VERIFY_N = 200_000L;

    private File walFile;

    @After public void cleanup() {
        if (walFile != null) org.mapdb.store.WalTestKit.deleteStore(walFile);
    }

    @Test public void walScaleReplay() throws Exception {
        int commits = scaled(COMMITS);
        int newPer = scaled(NEW_PER);
        int updPer = scaled(UPD_PER);
        int appPer = scaled(APP_PER);

        walFile = File.createTempFile("stress-wal-scale", ".wal");
        walFile.delete();

        long[] recids = new long[commits * newPer];
        int idx = 0;
        SplittableRandom rnd = new SplittableRandom(2025);

        StoreWAL store = new StoreWAL(walFile);
        long t0 = System.nanoTime();
        long created = 0, updated = 0, appended = 0;
        try {
            for (int c = 0; c < commits; c++) {
                for (int i = 0; i < newPer; i++) {
                    long recid = store.preallocate();
                    store.update(recid, payload(recid, 0, RECSIZE), RAW);
                    recids[idx++] = recid;
                    created++;
                }
                for (int i = 0; i < updPer && idx > 0; i++) {
                    long recid = recids[rnd.nextInt(idx)];
                    store.update(recid, payload(recid, c + 1, RECSIZE), RAW);
                    updated++;
                }
                byte[] chunk = new byte[APPEND];
                for (int i = 0; i < appPer && idx > 0; i++) {
                    long recid = recids[rnd.nextInt(idx)];
                    store.append(recid, chunk, 0, APPEND); // beyond base region; crc still valid
                    appended++;
                }
                store.commit();
            }
        } finally {
            store.close();
        }
        long tBuild = System.nanoTime() - t0;
        long fileBytes = org.mapdb.store.WalTestKit.logBytes(walFile); // v3: a segment set, not one file
        int total = idx;

        // reopen: full WAL replay
        long t1 = System.nanoTime();
        StoreWAL reopened = new StoreWAL(walFile);
        long tReplay = System.nanoTime() - t1;

        try {
            long t2 = System.nanoTime();
            long verifyN = scaledL(VERIFY_N);
            SplittableRandom vr = new SplittableRandom(77);
            for (long i = 0; i < verifyN; i++) {
                long recid = recids[vr.nextInt(total)];
                byte[] got = reopened.get(recid, RAW);
                if (got == null || getLong(got, 0) != recid)
                    throw new AssertionError("recid stamp mismatch, recid=" + recid);
                if (got.length < RECSIZE || getInt(got, 16) != crc32(got, RECSIZE))
                    throw new AssertionError("CRC mismatch after replay, recid=" + recid);
            }
            long tVerify = System.nanoTime() - t2;
            reopened.verify();

            double replaySec = tReplay / 1e9;
            double mb = fileBytes / (1024.0 * 1024.0);
            summary("WAL build (" + commits + " commits)", created + updated + appended, tBuild, fileBytes);
            System.out.printf("[STRESS]   WAL created=%,d updated=%,d appended=%,d  file=%.2f GB%n",
                created, updated, appended, fileBytes / 1e9);
            System.out.printf("[STRESS]   WAL REPLAY reopen=%.2fs  replay=%.1f MB/s  verify(%,d)=%.2fs%n",
                replaySec, mb / replaySec, verifyN, tVerify / 1e9);
        } finally {
            reopened.close();
        }
    }
}
