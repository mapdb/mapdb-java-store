package org.mapdb.stress;

import org.junit.Test;
import org.mapdb.btree.BufferTreeMap;
import org.mapdb.ser.LongFormat;
import org.mapdb.store.StoreDelta;

import java.util.Iterator;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.mapdb.stress.StressSupport.*;

/**
 * Concurrency hammer for {@link BufferTreeMap} — the write-optimized buffer tree
 * (Bε-tree) companion to BTreeMap. Ported from {@code ConcurrencyStressIT.btreeReaders}.
 *
 * BufferTreeMap contract exercised here:
 *  - SINGLE writer (put/remove take a map-global lock) — every test uses exactly one
 *    writer thread as the contract requires.
 *  - Readers (get/containsKey/entryIterator) are LOCK-FREE and run concurrently with
 *    the writer.
 *  - Writes are BLIND (put/remove return void); the writer self-validates by encoding
 *    (key, generation) into each value so any reader can check the value relation.
 *  - Iteration is weakly consistent.
 *
 * Self-validating content: value = key * MULT + gen, with 0 <= gen < GEN_MOD. Any read
 * of a live key must satisfy 0 <= value - key*MULT < GEN_MOD, and full iterations must
 * be strictly ascending. Any violation fails the test immediately (zero tolerance).
 */
public class BufferTreeStressIT {

    static final int  CORES    = Runtime.getRuntime().availableProcessors();
    static final int  POOL     = scaled(1_000_000L);
    static final long MULT     = 1_000_003L;
    static final long GEN_MOD  = 100_000L;
    static final int  MAX_NODE = 32;
    static final int  BUF_BYTES = 256;

    // ---------------- 1. BufferTreeMap: single writer + concurrent readers ----------------

    @Test public void bufferTreeReaders() {
        StoreDelta store = newBufferTreeStore();
        try {
            BufferTreeMap<Long, Long> map =
                BufferTreeMap.create(store, LongFormat.INSTANCE, LongFormat.INSTANCE, MAX_NODE, BUF_BYTES);
            final int keyspace = Math.max(1000, POOL * 2);

            // pre-populate half the keyspace
            for (int k = 0; k < keyspace; k += 2) map.put((long) k, k * MULT);

            AtomicReference<Throwable> failure = new AtomicReference<>();
            AtomicLong writes = new AtomicLong(), reads = new AtomicLong(), iters = new AtomicLong();
            long deadline = System.nanoTime() + durationMs() * 1_000_000L;
            int nReaders = Math.max(1, 2 * CORES - 1);
            Thread[] threads = new Thread[nReaders + 1];

            threads[0] = new Thread(() -> {              // single writer (contract: exactly one)
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
            }, "buftree-writer");

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
                }, "buftree-reader-" + r);
            }

            runAll(threads);
            if (failure.get() != null) throw new AssertionError("buffer tree readers failed", failure.get());
            summary("BufferTree 1writer+" + nReaders + "readers store=" + STORE_KIND, writes.get() + reads.get(),
                durationMs() * 1_000_000L, 0);
            phase("  writer put/remove", writes.get(), durationMs() * 1_000_000L);
            phase("  readers get(+" + iters.get() + " iters)", reads.get(), durationMs() * 1_000_000L);
        } finally {
            store.close();
        }
    }

    // ---------------- 2. BufferTreeMap: hammer then quiescent exact-equality vs shadow ----------------

    /**
     * Single writer keeps an exact local {@link TreeMap} shadow (possible precisely
     * because there is only one writer). Readers hammer the lock-free read path during
     * a shorter hammer phase; then ALL threads join and the map is compared EXACTLY
     * against the shadow — same size, same ordered (key,value) entries — both before
     * and after {@code flushAll()} drains every buffer to the leaves. Finally
     * {@code store.verify()} checks store-level invariants.
     */
    @Test public void bufferTreeQuiescentEquality() {
        StoreDelta store = newBufferTreeStore();
        try {
            BufferTreeMap<Long, Long> map =
                BufferTreeMap.create(store, LongFormat.INSTANCE, LongFormat.INSTANCE, MAX_NODE, BUF_BYTES);
            final int keyspace = Math.max(1000, POOL);
            final TreeMap<Long, Long> shadow = new TreeMap<>();

            AtomicReference<Throwable> failure = new AtomicReference<>();
            AtomicLong writes = new AtomicLong(), reads = new AtomicLong(), iters = new AtomicLong();
            // shorter hammer phase: this test does heavy post-join validation work
            long hammerMs = Math.max(1000, durationMs() / 2);
            long deadline = System.nanoTime() + hammerMs * 1_000_000L;
            int nReaders = Math.max(1, 2 * CORES - 1);
            Thread[] threads = new Thread[nReaders + 1];

            threads[0] = new Thread(() -> {              // single writer maintains exact shadow
                SplittableRandom rnd = new SplittableRandom(11);
                long gen = 0, ops = 0;
                try {
                    while (System.nanoTime() < deadline && failure.get() == null) {
                        long k = rnd.nextInt(keyspace);
                        if (rnd.nextInt(100) < 70) {
                            long v = k * MULT + (gen++ % GEN_MOD);
                            map.put(k, v);
                            shadow.put(k, v);
                        } else {
                            map.remove(k);
                            shadow.remove(k);
                        }
                        ops++;
                    }
                } catch (Throwable e) { failure.compareAndSet(null, e); }
                writes.addAndGet(ops);
            }, "buftree-qw-writer");

            for (int r = 0; r < nReaders; r++) {
                final long seed = 400 + r;
                threads[r + 1] = new Thread(() -> {
                    SplittableRandom rnd = new SplittableRandom(seed);
                    long rd = 0, itc = 0;
                    try {
                        while (System.nanoTime() < deadline && failure.get() == null) {
                            if ((rd & 8191) == 8191) {
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
                }, "buftree-qw-reader-" + r);
            }

            runAll(threads);
            if (failure.get() != null) throw new AssertionError("buffer tree quiescent hammer failed", failure.get());

            // ---- quiescent: exact equality against the shadow, before flush ----
            assertEquals(map, shadow, "before flushAll");
            // ---- drain all buffers to leaves, then compare again ----
            map.flushAll();
            assertEquals(map, shadow, "after flushAll");

            store.verify();
            summary("BufferTree quiescent 1w+" + nReaders + "r store=" + STORE_KIND + " (shadow=" + shadow.size() + ")",
                writes.get() + reads.get(), hammerMs * 1_000_000L, 0);
            phase("  writer put/remove", writes.get(), hammerMs * 1_000_000L);
            phase("  readers get(+" + iters.get() + " iters)", reads.get(), hammerMs * 1_000_000L);
        } finally {
            store.close();
        }
    }

    /** Exact ordered comparison of the map's entryIterator against a TreeMap shadow. */
    private static void assertEquals(BufferTreeMap<Long, Long> map, TreeMap<Long, Long> shadow, String when) {
        Iterator<Map.Entry<Long, Long>> mit = map.entryIterator();
        Iterator<Map.Entry<Long, Long>> sit = shadow.entrySet().iterator();
        long n = 0;
        while (mit.hasNext() && sit.hasNext()) {
            Map.Entry<Long, Long> me = mit.next();
            Map.Entry<Long, Long> se = sit.next();
            if (!me.getKey().equals(se.getKey()))
                throw new AssertionError("[" + when + "] key mismatch at pos " + n
                    + ": map=" + me.getKey() + " shadow=" + se.getKey());
            if (!me.getValue().equals(se.getValue()))
                throw new AssertionError("[" + when + "] value mismatch key=" + me.getKey()
                    + ": map=" + me.getValue() + " shadow=" + se.getValue());
            n++;
        }
        if (mit.hasNext()) {
            Map.Entry<Long, Long> extra = mit.next();
            throw new AssertionError("[" + when + "] map has extra entries beyond shadow size " + n
                + " (e.g. key=" + extra.getKey() + "), shadow.size=" + shadow.size());
        }
        if (sit.hasNext()) {
            Map.Entry<Long, Long> missing = sit.next();
            throw new AssertionError("[" + when + "] map missing entries; iterated " + n
                + " but shadow has " + shadow.size() + " (e.g. missing key=" + missing.getKey() + ")");
        }
        if (n != shadow.size())
            throw new AssertionError("[" + when + "] size mismatch: iterated=" + n + " shadow=" + shadow.size());
    }

    private static void runAll(Thread[] threads) {
        for (Thread t : threads) t.start();
        for (Thread t : threads) {
            try { t.join(); } catch (InterruptedException e) { throw new RuntimeException(e); }
        }
    }
}
