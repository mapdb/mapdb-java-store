package org.mapdb.stress;

import org.junit.After;
import org.junit.Test;
import org.mapdb.TmpFiles;
import org.mapdb.btree.BTreeMap;
import org.mapdb.ser.LongFormat;
import org.mapdb.store.Store;
import org.mapdb.store.StoreByteArray;
import org.mapdb.store.StoreDelta;
import org.mapdb.store.StoreDirect;
import org.mapdb.store.StoreWAL;
import org.mapdb.DBException;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.mapdb.stress.StressSupport.*;

/**
 * Concurrency hammer. Threads = 2x cores; each sub-test runs {@code stress.durationMs}
 * (default 30s). Content is self-validating (recid + version + CRC32); any CRC or recid
 * mismatch fails immediately (zero tolerance).
 *
 * Ownership contract: writers only mutate recids they currently own (recids handed to
 * them by preallocate/put are exclusively theirs), so single-writer-per-record holds even
 * across delete+recreate. Readers hit random recids across the whole pool and tolerate
 * only GetVoid / null (a concurrently deleted or momentarily-preallocated record).
 */
public class ConcurrencyStressIT {

    static final int  CORES = Runtime.getRuntime().availableProcessors();
    static final int  POOL  = scaled(1_000_000L);
    static final int  HEADROOM = 256;

    private File walFile;

    @After public void cleanup() {
        TmpFiles.delete(walFile);
    }

    // ---------------- 1. StoreDirect + StoreByteArray: mixed ops, whole-pool reads ----------------

    @Test public void directMixed() {
        StoreDirect s = new StoreDirect(false);
        try { mixedOps(s, "StoreDirect", 2 * CORES); } finally { s.close(); }
    }

    @Test public void byteArrayMixed() {
        StoreByteArray s = new StoreByteArray();
        try { mixedOps(s, "StoreByteArray", 2 * CORES); } finally { s.close(); }
    }

    private void mixedOps(StoreDelta store, String label, int nThreads) {
        // pre-create a dense pool of POOL live records, recids 1..POOL
        for (long i = 1; i <= POOL; i++) {
            long recid = store.preallocate();
            store.updateWithHeadroom(recid, payload(recid, 0), RAW, HEADROOM);
        }

        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicLong totalOps = new AtomicLong();
        long deadline = System.nanoTime() + durationMs() * 1_000_000L;
        Thread[] threads = new Thread[nThreads];
        int per = POOL / nThreads;

        for (int t = 0; t < nThreads; t++) {
            final int lo = 1 + t * per;
            final int hi = (t == nThreads - 1) ? POOL : lo + per - 1;
            final long seed = 1000 + t;
            threads[t] = new Thread(() -> {
                SplittableRandom rnd = new SplittableRandom(seed);
                ArrayList<Long> owned = new ArrayList<>();
                for (long r = lo; r <= hi; r++) owned.add(r);
                Validator val = new Validator();
                long ops = 0, ver = 1;
                try {
                    while (System.nanoTime() < deadline && failure.get() == null) {
                        int roll = rnd.nextInt(100);
                        if (roll < 70) {                       // READ across whole pool
                            long recid = 1 + rnd.nextInt(POOL);
                            val.expected = recid;
                            try { store.read(recid, val); }
                            catch (DBException.GetVoid ignore) { /* concurrently deleted */ }
                        } else {
                            int idx = rnd.nextInt(owned.size());
                            long recid = owned.get(idx);
                            int w = rnd.nextInt(100);
                            if (w < 55) {                      // UPDATE (bump version)
                                store.updateWithHeadroom(recid, payload(recid, ver++), RAW, HEADROOM);
                            } else if (w < 85) {               // APPEND 16B (REFUSED at tail is fine)
                                store.append(recid, new byte[APPEND16], 0, APPEND16);
                            } else {                           // DELETE + recreate: net-neutral,
                                store.delete(recid, RAW);      // pool size stays bounded, exercises
                                long nr = store.preallocate(); // recid/data free-list reuse across threads
                                store.updateWithHeadroom(nr, payload(nr, ver++), RAW, HEADROOM);
                                owned.set(idx, nr);            // replace in place -> owned size constant
                            }
                        }
                        ops++;
                    }
                } catch (Throwable e) {
                    failure.compareAndSet(null, e);
                }
                totalOps.addAndGet(ops);
            }, label + "-" + t);
        }
        runAll(threads);
        if (failure.get() != null) throw new AssertionError(label + " failed", failure.get());
        store.verify();
        summary(label + " mixed (" + nThreads + " thr, pool=" + POOL + ")",
            totalOps.get(), durationMs() * 1_000_000L, 0);
    }

    static final int APPEND16 = 16;

    // ---------------- 2. BTreeMap: single writer + concurrent readers ----------------

    @Test public void btreeReaders() {
        StoreDirect store = new StoreDirect(false);
        try {
            BTreeMap<Long, Long> map =
                BTreeMap.create(store, LongFormat.INSTANCE, LongFormat.INSTANCE, 32);
            final int keyspace = Math.max(1000, POOL * 2);
            final long MULT = 1_000_003L, GEN_MOD = 100_000L;

            // pre-populate half the keyspace
            for (int k = 0; k < keyspace; k += 2) map.put((long) k, k * MULT);

            AtomicReference<Throwable> failure = new AtomicReference<>();
            AtomicLong writes = new AtomicLong(), reads = new AtomicLong(), iters = new AtomicLong();
            long deadline = System.nanoTime() + durationMs() * 1_000_000L;
            int nReaders = Math.max(1, 2 * CORES - 1);
            Thread[] threads = new Thread[nReaders + 1];

            threads[0] = new Thread(() -> {              // single writer
                SplittableRandom rnd = new SplittableRandom(7);
                long gen = 0, ops = 0;
                try {
                    while (System.nanoTime() < deadline && failure.get() == null) {
                        int k = rnd.nextInt(keyspace);
                        if (rnd.nextInt(100) < 70) map.put((long) k, k * MULT + (gen++ % GEN_MOD));
                        else map.remove((long) k);
                        ops++;
                    }
                } catch (Throwable e) { failure.compareAndSet(null, e); }
                writes.addAndGet(ops);
            }, "btree-writer");

            for (int r = 0; r < nReaders; r++) {
                final long seed = 200 + r;
                threads[r + 1] = new Thread(() -> {
                    SplittableRandom rnd = new SplittableRandom(seed);
                    long rd = 0, itc = 0;
                    try {
                        while (System.nanoTime() < deadline && failure.get() == null) {
                            if ((rd & 8191) == 8191) {       // occasional full iteration
                                long prev = Long.MIN_VALUE;
                                Iterator<Map.Entry<Long, Long>> it = map.entryIterator();
                                while (it.hasNext()) {
                                    Map.Entry<Long, Long> e = it.next();
                                    long key = e.getKey();
                                    if (key <= prev) throw new AssertionError("iter not ascending " + prev + "->" + key);
                                    long g = e.getValue() - key * MULT;
                                    if (g < 0 || g >= GEN_MOD) throw new AssertionError("bad value key=" + key + " v=" + e.getValue());
                                    prev = key;
                                }
                                itc++;
                            } else {
                                long k = rnd.nextInt(keyspace);
                                Long v = map.get(k);
                                if (v != null) {
                                    long g = v - k * MULT;
                                    if (g < 0 || g >= GEN_MOD)
                                        throw new AssertionError("bad value key=" + k + " v=" + v);
                                }
                            }
                            rd++;
                        }
                    } catch (Throwable e) { failure.compareAndSet(null, e); }
                    reads.addAndGet(rd); iters.addAndGet(itc);
                }, "btree-reader-" + r);
            }

            runAll(threads);
            if (failure.get() != null) throw new AssertionError("btree readers failed", failure.get());
            summary("BTree 1writer+" + nReaders + "readers", writes.get() + reads.get(),
                durationMs() * 1_000_000L, 0);
            phase("  writer put/remove", writes.get(), durationMs() * 1_000_000L);
            phase("  readers get(+" + iters.get() + " iters)", reads.get(), durationMs() * 1_000_000L);
        } finally {
            store.close();
        }
    }

    // ---------------- 2b. BTreeMap: MANY writers + concurrent readers (Lehman-Yao) ----------------

    /**
     * Multi-writer hammer for the B-link write protocol: writers own disjoint key
     * slices (key % nWriters), so each writer's ops per key are sequential and it can
     * keep an exact local shadow map; readers validate the value relation and ordered
     * iteration while the tree grows from EMPTY under full write concurrency (racing
     * splits, root splits, fence-covered inserts after removes). After the run the
     * map must equal the union of all shadows exactly.
     */
    @Test public void btreeMultiWriterDirect() {
        StoreDirect store = new StoreDirect(false);
        try { btreeMultiWriter(store, "Direct"); } finally { store.close(); }
    }

    @Test public void btreeMultiWriterOnHeap() {
        org.mapdb.store.StoreOnHeap store = new org.mapdb.store.StoreOnHeap();
        try { btreeMultiWriter(store, "OnHeap"); } finally { store.close(); }
    }

    private void btreeMultiWriter(Store store, String label) {
        {
            BTreeMap<Long, Long> map =
                BTreeMap.create(store, LongFormat.INSTANCE, LongFormat.INSTANCE, 32);
            final int keyspace = Math.max(1000, POOL);
            final long MULT = 1_000_003L, GEN_MOD = 100_000L;
            final int nWriters = Math.max(2, CORES / 2);
            final int nReaders = Math.max(1, CORES);

            AtomicReference<Throwable> failure = new AtomicReference<>();
            AtomicLong writes = new AtomicLong(), reads = new AtomicLong(), iters = new AtomicLong();
            long deadline = System.nanoTime() + durationMs() * 1_000_000L;
            Thread[] threads = new Thread[nWriters + nReaders];
            @SuppressWarnings("unchecked")
            java.util.HashMap<Long, Long>[] shadows = new java.util.HashMap[nWriters];

            for (int w = 0; w < nWriters; w++) {
                final int self = w;
                final java.util.HashMap<Long, Long> shadow = new java.util.HashMap<>();
                shadows[w] = shadow;
                threads[w] = new Thread(() -> {
                    SplittableRandom rnd = new SplittableRandom(31 + self);
                    long gen = 0, ops = 0;
                    try {
                        while (System.nanoTime() < deadline && failure.get() == null) {
                            long k = rnd.nextInt(keyspace / nWriters) * (long) nWriters + self;
                            if (rnd.nextInt(100) < 70) {
                                long v = k * MULT + (gen++ % GEN_MOD);
                                Long old = map.put(k, v);
                                Long shOld = shadow.put(k, v);
                                if (!java.util.Objects.equals(old, shOld))
                                    throw new AssertionError("put returned " + old + " expected " + shOld + " key=" + k);
                            } else {
                                Long old = map.remove(k);
                                Long shOld = shadow.remove(k);
                                if (!java.util.Objects.equals(old, shOld))
                                    throw new AssertionError("remove returned " + old + " expected " + shOld + " key=" + k);
                            }
                            ops++;
                        }
                    } catch (Throwable e) { failure.compareAndSet(null, e); }
                    writes.addAndGet(ops);
                }, "btree-mwriter-" + w);
            }

            for (int r = 0; r < nReaders; r++) {
                final long seed = 700 + r;
                threads[nWriters + r] = new Thread(() -> {
                    SplittableRandom rnd = new SplittableRandom(seed);
                    long rd = 0, itc = 0;
                    try {
                        while (System.nanoTime() < deadline && failure.get() == null) {
                            if ((rd & 8191) == 8191) {       // occasional full iteration
                                long prev = Long.MIN_VALUE;
                                Iterator<Map.Entry<Long, Long>> it = map.entryIterator();
                                while (it.hasNext()) {
                                    Map.Entry<Long, Long> e = it.next();
                                    long key = e.getKey();
                                    if (key <= prev) throw new AssertionError("iter not ascending " + prev + "->" + key);
                                    long g = e.getValue() - key * MULT;
                                    if (g < 0 || g >= GEN_MOD) throw new AssertionError("bad value key=" + key + " v=" + e.getValue());
                                    prev = key;
                                }
                                itc++;
                            } else {
                                long k = rnd.nextInt(keyspace);
                                Long v = map.get(k);
                                if (v != null) {
                                    long g = v - k * MULT;
                                    if (g < 0 || g >= GEN_MOD)
                                        throw new AssertionError("bad value key=" + k + " v=" + v);
                                }
                            }
                            rd++;
                        }
                    } catch (Throwable e) { failure.compareAndSet(null, e); }
                    reads.addAndGet(rd); iters.addAndGet(itc);
                }, "btree-mreader-" + r);
            }

            runAll(threads);
            if (failure.get() != null) throw new AssertionError("btree multiwriter failed", failure.get());

            // quiescent check: map content == union of writer shadows, exactly
            java.util.HashMap<Long, Long> expected = new java.util.HashMap<>();
            for (var s : shadows) expected.putAll(s);
            long n = 0;
            Iterator<Map.Entry<Long, Long>> it = map.entryIterator();
            while (it.hasNext()) {
                Map.Entry<Long, Long> e = it.next();
                Long exp = expected.get(e.getKey());
                if (!e.getValue().equals(exp))
                    throw new AssertionError("final mismatch key=" + e.getKey() + " map=" + e.getValue() + " expected=" + exp);
                n++;
            }
            if (n != expected.size())
                throw new AssertionError("final size mismatch: iterated=" + n + " expected=" + expected.size());
            store.verify();
            summary("BTree/" + label + " " + nWriters + "writers+" + nReaders + "readers",
                writes.get() + reads.get(), durationMs() * 1_000_000L, 0);
            phase("  writers put/remove", writes.get(), durationMs() * 1_000_000L);
            phase("  readers get(+" + iters.get() + " iters)", reads.get(), durationMs() * 1_000_000L);
        }
    }

    // ---------------- 3. StoreWAL: readers on committed data + writer commit cycles ----------------

    @Test public void walReaders() throws Exception {
        walFile = TmpFiles.tempFile("stress-wal-conc", ".wal");
        walFile.delete();
        StoreWAL store = new StoreWAL(walFile);
        try {
            int m = Math.min(POOL, scaled(200_000L));
            long[] recids = new long[m];
            for (int i = 0; i < m; i++) recids[i] = store.put(payload(0, 0), RAW); // stamped below
            // stamp each record with its own recid, then commit the baseline
            for (int i = 0; i < m; i++) store.update(recids[i], payload(recids[i], 0), RAW);
            store.commit();

            AtomicReference<Throwable> failure = new AtomicReference<>();
            AtomicLong commits = new AtomicLong(), reads = new AtomicLong();
            long deadline = System.nanoTime() + durationMs() * 1_000_000L;
            Thread[] threads = new Thread[9];

            threads[0] = new Thread(() -> {              // writer: update + commit cycles
                SplittableRandom rnd = new SplittableRandom(3);
                long ver = 1, c = 0;
                try {
                    while (System.nanoTime() < deadline && failure.get() == null) {
                        int batch = 64;
                        for (int b = 0; b < batch; b++) {
                            long recid = recids[rnd.nextInt(m)];
                            store.update(recid, payload(recid, ver++), RAW);
                        }
                        store.commit();
                        c++;
                    }
                } catch (Throwable e) { failure.compareAndSet(null, e); }
                commits.addAndGet(c);
            }, "wal-writer");

            for (int r = 0; r < 8; r++) {
                final long seed = 500 + r;
                threads[r + 1] = new Thread(() -> {
                    SplittableRandom rnd = new SplittableRandom(seed);
                    Validator val = new Validator();
                    long rd = 0;
                    try {
                        while (System.nanoTime() < deadline && failure.get() == null) {
                            long recid = recids[rnd.nextInt(m)];
                            val.expected = recid;
                            store.read(recid, val);
                            rd++;
                        }
                    } catch (Throwable e) { failure.compareAndSet(null, e); }
                    reads.addAndGet(rd);
                }, "wal-reader-" + r);
            }

            runAll(threads);
            if (failure.get() != null) throw new AssertionError("wal readers failed", failure.get());
            store.verify();
            summary("StoreWAL 8readers+1committer (m=" + m + ")", reads.get() + commits.get(),
                durationMs() * 1_000_000L, 0);
            phase("  commits", commits.get(), durationMs() * 1_000_000L);
            phase("  reads", reads.get(), durationMs() * 1_000_000L);
        } finally {
            store.close();
        }
    }

    private static void runAll(Thread[] threads) {
        for (Thread t : threads) t.start();
        for (Thread t : threads) {
            try { t.join(); } catch (InterruptedException e) { throw new RuntimeException(e); }
        }
    }
}
