package org.mapdb.stress;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.mapdb.store.StoreDelta;
import org.mapdb.store.StoreDirect;

import java.util.PrimitiveIterator;
import java.util.SplittableRandom;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mapdb.stress.StressSupport.*;

/**
 * Raw {@link StoreDirect} scale. Each scenario is an independent @Test; run the whole
 * class, or one scenario per JVM with -Dstress.only=&lt;tag&gt;. Sizes scale with
 * -Dstress.scale.
 *
 * Full-scale sizes (scale=1.0):
 *   v1 heapSmall  : 200,000,000 x 16B  (~6.4 GB data)
 *   v2 directSmall: 100,000,000 x 16B  (~3.2 GB direct)   [needs -XX:MaxDirectMemorySize>=8g]
 *   v3 large      : 40,000 x (1 MiB-64) (~40 GB heap buffers)
 *   v4 churn      : 10,000,000 records, headroom 256, 10 append passes
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class StoreVolumeIT {

    static final long HEAP_SMALL_N   = 200_000_000L;
    static final long DIRECT_SMALL_N = 100_000_000L;
    static final long LARGE_N        = 40_000L;
    static final int  LARGE_PAYLOAD  = (1 << 20) - 64;   // ~1 MiB - 64
    static final long CHURN_N        = 10_000_000L;
    static final int  CHURN_HEADROOM = 256;
    static final int  CHURN_PASSES   = 10;
    static final int  APPEND_CHUNK   = 16;

    @Test public void v1_heapSmall() {
        requireTag("v1");
        smallRecords(false, scaledL(HEAP_SMALL_N), "StoreDirect(heap) small");
    }

    @Test public void v2_directSmall() {
        requireTag("v2");
        smallRecords(true, scaledL(DIRECT_SMALL_N), "StoreDirect(direct) small");
    }

    private void smallRecords(boolean direct, long n, String label) {
        StoreDirect store = new StoreDirect(direct);
        try {
            long t0 = System.nanoTime();
            for (long i = 1; i <= n; i++) {
                long recid = store.put(smallPayload(i), RAW);
                // single-threaded, no frees -> recids are dense 1..n
                if (recid != i) throw new AssertionError("expected dense recid " + i + " got " + recid);
            }
            long tPut = System.nanoTime() - t0;
            long bytes = n * 32L; // roundUp16(4+16)=32 capacity per record

            // spot-verify 1,000,000 random recids by self-describing content
            long verifyN = scaledL(1_000_000L);
            SplittableRandom rnd = new SplittableRandom(42);
            long t1 = System.nanoTime();
            for (long i = 0; i < verifyN; i++) {
                long recid = 1 + rnd.nextLong(n);
                byte[] got = store.get(recid, RAW);
                if (smallRecidOf(got) != recid)
                    throw new AssertionError("content recid " + smallRecidOf(got) + " != " + recid);
            }
            long tVer = System.nanoTime() - t1;

            // getAllRecids count
            long t2 = System.nanoTime();
            long count = 0;
            PrimitiveIterator.OfLong it = store.getAllRecids();
            while (it.hasNext()) { it.nextLong(); count++; }
            long tCnt = System.nanoTime() - t2;
            assertEquals("getAllRecids count", n, count);

            long t3 = System.nanoTime();
            store.verify();
            long tVerify = System.nanoTime() - t3;

            summary(label + " PUT", n, tPut, bytes);
            phase(label + " spot-verify", verifyN, tVer);
            phase(label + " getAllRecids(" + count + ")", count, tCnt);
            phase(label + " verify() [" + String.format("%.1fs", tVerify / 1e9) + "]", n, tVerify);
        } finally {
            store.close();
        }
        System.gc();
    }

    @Test public void v3_large() {
        requireTag("v3");
        long n = scaledL(LARGE_N);
        StoreDirect store = new StoreDirect(false); // heap buffers
        try {
            long[] recids = new long[(int) n];
            long t0 = System.nanoTime();
            for (int i = 0; i < n; i++) {
                long recid = store.preallocate();
                recids[i] = recid;
                store.update(recid, largePayload(recid, LARGE_PAYLOAD), RAW);
            }
            long tWrite = System.nanoTime() - t0;
            long bytes = n * (long) LARGE_PAYLOAD;

            // spot-verify 500 random ones byte-exactly
            long verifyN = scaledL(500L);
            SplittableRandom rnd = new SplittableRandom(7);
            long t1 = System.nanoTime();
            for (long i = 0; i < verifyN; i++) {
                long recid = recids[rnd.nextInt((int) n)];
                byte[] got = store.get(recid, RAW);
                if (!java.util.Arrays.equals(got, largePayload(recid, LARGE_PAYLOAD)))
                    throw new AssertionError("byte mismatch recid=" + recid);
            }
            long tVer = System.nanoTime() - t1;

            // delete half, verify(), re-put into freed space
            long t2 = System.nanoTime();
            for (int i = 0; i < n; i += 2) store.delete(recids[i], RAW);
            long tDel = System.nanoTime() - t2;
            store.verify();

            long heapAfterDelete = usedHeapMB();
            long t3 = System.nanoTime();
            for (int i = 0; i < n; i += 2) {
                long recid = store.preallocate();
                store.update(recid, largePayload(recid, LARGE_PAYLOAD), RAW);
            }
            long tReput = System.nanoTime() - t3;
            store.verify();

            summary("large WRITE", n, tWrite, bytes);
            phase("large spot-verify(byte-exact)", verifyN, tVer);
            phase("large delete-half", n / 2, tDel);
            phase("large re-put(reuse freed)", n / 2, tReput);
            System.out.printf("[STRESS]   large heapUsed after delete=%,d MB, after re-put=%,d MB%n",
                heapAfterDelete, usedHeapMB());
        } finally {
            store.close();
        }
        System.gc();
    }

    @Test public void v4_churn() {
        requireTag("v4");
        long n = scaledL(CHURN_N);
        StoreDirect store = new StoreDirect(false);
        try {
            byte[] base = new byte[16];
            long t0 = System.nanoTime();
            for (long i = 1; i <= n; i++) {
                long recid = store.preallocate();
                putLong(base, 0, recid);
                store.updateWithHeadroom(recid, base, RAW, CHURN_HEADROOM);
            }
            long tCreate = System.nanoTime() - t0;

            byte[] chunk = new byte[APPEND_CHUNK];
            long appends = 0;
            long t1 = System.nanoTime();
            for (int pass = 1; pass <= CHURN_PASSES; pass++) {
                long expected = 16 + (long) APPEND_CHUNK * pass;
                for (long recid = 1; recid <= n; recid++) {
                    long r = store.append(recid, chunk, 0, APPEND_CHUNK);
                    // headroom 256 fits all 10 passes; assert exact merged length every time
                    if (r != expected && r != StoreDelta.REFUSED)
                        throw new AssertionError("append len recid=" + recid + " got=" + r + " expected=" + expected);
                    if (r == expected) appends++;
                }
            }
            long tAppend = System.nanoTime() - t1;

            // verify merged length via read() action on a sample
            long sample = scaledL(200_000L);
            SplittableRandom rnd = new SplittableRandom(11);
            long expectedLen = 16 + (long) APPEND_CHUNK * CHURN_PASSES;
            for (long i = 0; i < sample; i++) {
                long recid = 1 + rnd.nextLong(n);
                long len = store.read(recid, LEN_ACTION);
                assertEquals("merged len recid=" + recid, expectedLen, len);
            }
            store.verify();

            summary("churn CREATE(headroom=" + CHURN_HEADROOM + ")", n, tCreate, n * 288L);
            phase("churn APPEND(" + CHURN_PASSES + " passes)", appends, tAppend);
            assertTrue("all appends fit in headroom", appends == n * CHURN_PASSES);
        } finally {
            store.close();
        }
        System.gc();
    }
}
