package org.mapdb.stress;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.mapdb.htree.HTreeMap;
import org.mapdb.ser.Serializers;
import org.mapdb.store.StoreDirect;

import java.util.Iterator;
import java.util.Map;
import java.util.SplittableRandom;

import static org.junit.Assert.assertEquals;
import static org.mapdb.stress.StressSupport.*;

/**
 * {@link HTreeMap} scale over {@link StoreDirect} (heap buffers) — the segmented hash
 * tree. Companion to {@link BTreeScaleIT}/{@link BufferTreeScaleIT}, same house pattern
 * (tag-gated, {@code stress.scale}, {@link StressSupport} reporting, a {@link #main}
 * entry with clean JVM flags for JFR profiling).
 *
 * HTreeMap is UNORDERED, so iteration checks count + per-entry value, never order.
 * Values are stored inline; value == key (Long) or (long)i (String) so any live read is
 * self-validating.
 *
 * Full-scale (scale=1.0), each phase on its own freshly-built map, StoreDirect(false):
 *   h1 longSeq  : Long-&gt;Long, 200,000,000 sequential inserts, 200,000,000 random gets,
 *                 full iteration (count + value check).
 *   h2 longRand : Long-&gt;Long, 200,000,000 shuffled inserts (different dir-tree growth),
 *                 100,000,000 random gets.
 *   h3 string   : String-&gt;Long, 50,000,000 entries, 50,000,000 random gets, iteration.
 *   h4 concRead : aggregate random-GET throughput at 1/8/CORES reader threads.
 *   h5 mixed    : 100,000,000-entry base load, then 200,000,000 blind put/remove
 *                 (70/30) split across 8 CONCURRENT writers (per-segment write locks)
 *                 interleaved with 4 validating reader threads (random gets +
 *                 periodic full iterations).
 *
 * Two ways to run:
 *  - JUnit (correctness / smoke, assertions on):
 *      mvn test -Dtest=HTreeScaleIT -Dstress.only=h1 -Dstress.scale=0.005
 *  - {@link #main} (full-scale perf, clean JVM flags, trivial to attach Java Flight
 *    Recorder). Args are the tags to run in order:
 *      java -Xmx70g -Xms70g -XX:+UseParallelGC -XX:+AlwaysPreTouch \
 *           -XX:StartFlightRecording=filename=h1.jfr,settings=profile,dumponexit=true \
 *           -Dstress.scale=1.0 -cp &lt;cp&gt; org.mapdb.stress.HTreeScaleIT h1
 *    Assertions are left off (no {@code -ea}) for production-shape numbers; the
 *    validation checks are plain method calls, so correctness still holds.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class HTreeScaleIT {

    /** Full-scale perf entry: each arg is a tag (h1/h2/h3/h4) run in sequence. */
    public static void main(String[] args) {
        if (args.length == 0) args = new String[]{"h1", "h2", "h3"};
        HTreeScaleIT t = new HTreeScaleIT();
        System.out.printf("[STRESS] HTreeScaleIT main: scale=%s cores=%d args=%s%n",
            System.getProperty("stress.scale", "1.0"), CORES, java.util.Arrays.toString(args));
        for (String tag : args) {
            System.setProperty("stress.only", tag);
            long t0 = System.nanoTime();
            switch (tag) {
                case "h1": t.h1_longSequential(); break;
                case "h2": t.h2_longRandom(); break;
                case "h3": t.h3_string(); break;
                case "h4": t.h4_concurrentReadScaling(); break;
                case "h5": t.h5_multiWriterMixed(); break;
                default: throw new IllegalArgumentException("unknown tag " + tag);
            }
            System.out.printf("[STRESS] tag %s done in %.1fs%n", tag, (System.nanoTime() - t0) / 1e9);
        }
    }

    static final long LONG_SEQ_N  = 200_000_000L;
    static final long LONG_SEQ_GET = 200_000_000L;
    static final long LONG_RAND_N = 200_000_000L;
    static final long LONG_RAND_GET = 100_000_000L;
    static final long STRING_N    = 50_000_000L;
    static final long STRING_GET  = 50_000_000L;
    static final long MIXED_LOAD_N = 100_000_000L;
    static final long MIXED_OPS    = 200_000_000L; // total across all writers

    static final int CORES = Runtime.getRuntime().availableProcessors();

    // ---------------- h1: sequential long load, gets, iteration ----------------

    @Test public void h1_longSequential() {
        requireTag("h1");
        long n = scaledL(LONG_SEQ_N);
        StoreDirect store = new StoreDirect(false);
        try {
            HTreeMap<Long, Long> map = HTreeMap.create(store, Serializers.LONG, Serializers.LONG);

            long t0 = System.nanoTime();
            for (long k = 0; k < n; k++) map.put(k, k);
            long tPut = System.nanoTime() - t0;

            long getN = scaledL(LONG_SEQ_GET);
            SplittableRandom rnd = new SplittableRandom(2024);
            long t1 = System.nanoTime();
            for (long i = 0; i < getN; i++) {
                long k = rnd.nextLong(n);
                Long v = map.get(k);
                if (v == null || v != k) throw new AssertionError("get(" + k + ")=" + v);
            }
            long tGet = System.nanoTime() - t1;

            long t2 = System.nanoTime();
            long count = iterateCount(map);
            long tIter = System.nanoTime() - t2;
            assertEquals("iteration count", n, count);

            summary("htree long-seq PUT", n, tPut, n * 16L);
            phase("htree long-seq random GET", getN, tGet);
            phase("htree long-seq ITER(" + count + ")", count, tIter);
        } finally {
            store.close();
        }
        System.gc();
    }

    // ---------------- h2: shuffled long load, gets ----------------

    @Test public void h2_longRandom() {
        requireTag("h2");
        int n = scaled(LONG_RAND_N);
        StoreDirect store = new StoreDirect(false);
        try {
            HTreeMap<Long, Long> map = HTreeMap.create(store, Serializers.LONG, Serializers.LONG);

            long[] keys = new long[n];
            for (int i = 0; i < n; i++) keys[i] = i;
            SplittableRandom rnd = new SplittableRandom(99);
            for (int i = n - 1; i > 0; i--) { // Fisher-Yates
                int j = rnd.nextInt(i + 1);
                long t = keys[i]; keys[i] = keys[j]; keys[j] = t;
            }

            long t0 = System.nanoTime();
            for (int i = 0; i < n; i++) map.put(keys[i], keys[i]);
            long tPut = System.nanoTime() - t0;
            keys = null; // release ~n*8 bytes before the read phase

            long getN = scaledL(LONG_RAND_GET);
            long t1 = System.nanoTime();
            for (long i = 0; i < getN; i++) {
                long k = rnd.nextInt(n);
                Long v = map.get(k);
                if (v == null || v != k) throw new AssertionError("get(" + k + ")=" + v);
            }
            long tGet = System.nanoTime() - t1;

            long t2 = System.nanoTime();
            long count = map.sizeLong();
            long tSize = System.nanoTime() - t2;
            assertEquals("size after load", (long) n, count);

            summary("htree long-rand PUT", n, tPut, n * 16L);
            phase("htree long-rand random GET", getN, tGet);
            phase("htree long-rand sizeLong(" + count + ")", count, tSize);
        } finally {
            store.close();
        }
        System.gc();
    }

    // ---------------- h3: string keys ----------------

    @Test public void h3_string() {
        requireTag("h3");
        int n = scaled(STRING_N);
        StoreDirect store = new StoreDirect(false);
        try {
            HTreeMap<String, Long> map = HTreeMap.create(store, Serializers.STRING, Serializers.LONG);

            long t0 = System.nanoTime();
            for (int i = 0; i < n; i++) map.put(key(i), (long) i);
            long tPut = System.nanoTime() - t0;

            long getN = scaledL(STRING_GET);
            SplittableRandom rnd = new SplittableRandom(555);
            long t1 = System.nanoTime();
            for (long i = 0; i < getN; i++) {
                int j = rnd.nextInt(n);
                Long v = map.get(key(j));
                if (v == null || v != j) throw new AssertionError("get(" + key(j) + ")=" + v);
            }
            long tGet = System.nanoTime() - t1;

            long t2 = System.nanoTime();
            long count = iterateCountStr(map);
            long tIter = System.nanoTime() - t2;
            assertEquals("iteration count", (long) n, count);

            summary("htree string PUT", n, tPut, n * 22L);
            phase("htree string random GET", getN, tGet);
            phase("htree string ITER(" + count + ")", count, tIter);
        } finally {
            store.close();
        }
        System.gc();
    }

    // ---------------- h4: concurrent read scaling (the lock-free-read benchmark) ----------------

    /**
     * The RIGHT benchmark for the leaf lock-free-read change: single-threaded h1 can't
     * show it (an uncontended readLock CAS is nearly free), so this loads a map once and
     * measures aggregate random-GET throughput at 1, 8, and {@code CORES} reader threads.
     * If the leaf read is lock-free (optimistic store.read), aggregate throughput scales
     * with threads; if it took the segment readLock CAS, concurrent readers would bounce
     * the store segments' lock cache lines and the curve would flatten. Each thread
     * self-validates (value == key). Uses {@code stress.durationMs} per thread-count phase.
     */
    @Test public void h4_concurrentReadScaling() {
        requireTag("h4");
        int n = scaled(LONG_RAND_N);
        long durMs = durationMs();
        StoreDirect store = new StoreDirect(false);
        try {
            HTreeMap<Long, Long> map = HTreeMap.create(store, Serializers.LONG, Serializers.LONG);
            long tl = System.nanoTime();
            for (long k = 0; k < n; k++) map.put(k, k);
            phase("htree concRead BASE-LOAD(" + n + ")", n, System.nanoTime() - tl);

            for (int threads : new int[]{1, 8, CORES}) {
                long ops = concurrentGets(map, n, threads, durMs);
                double sec = durMs / 1000.0;
                System.out.printf(
                    "[STRESS]   htree concRead t%-2d  ops=%,15d  wall=%6.2fs  ops/sec=%,14.0f%n",
                    threads, ops, sec, ops / sec);
            }
        } finally {
            store.close();
        }
        System.gc();
    }

    /** {@code threads} readers do random gets for {@code durMs}; returns total gets done. */
    private static long concurrentGets(HTreeMap<Long, Long> map, int n, int threads, long durMs) {
        java.util.concurrent.atomic.AtomicLong total = new java.util.concurrent.atomic.AtomicLong();
        java.util.concurrent.atomic.AtomicReference<Throwable> fail = new java.util.concurrent.atomic.AtomicReference<>();
        long deadline = System.nanoTime() + durMs * 1_000_000L;
        Thread[] ts = new Thread[threads];
        for (int t = 0; t < threads; t++) {
            final long seed = 1000 + t;
            ts[t] = new Thread(() -> {
                SplittableRandom rnd = new SplittableRandom(seed);
                long done = 0;
                try {
                    while (System.nanoTime() < deadline) {
                        for (int b = 0; b < 4096; b++) { // batch to amortize the clock read
                            long k = rnd.nextInt(n);
                            Long v = map.get(k);
                            if (v == null || v != k) throw new AssertionError("get(" + k + ")=" + v);
                        }
                        done += 4096;
                    }
                } catch (Throwable e) { fail.compareAndSet(null, e); }
                total.addAndGet(done);
            }, "htree-concread-" + t);
        }
        for (Thread th : ts) th.start();
        for (Thread th : ts) { try { th.join(); } catch (InterruptedException e) { throw new RuntimeException(e); } }
        if (fail.get() != null) throw new AssertionError("concurrent read failed", fail.get());
        return total.get();
    }

    // ---------------- h5: MULTI-WRITER mixed + concurrent validating readers ----------------

    /**
     * The multi-writer stress the porting agent flagged as missing: unlike bt2's
     * single-writer contract, HTreeMap allows CONCURRENT writers (per-segment write
     * locks serialize only same-segment mutations), so 8 writer threads run a blind
     * put/remove (70/30) mix over a shared keyspace while 4 readers validate random
     * gets and periodic full iterations against the self-validating value scheme
     * (value = key*MULT + gen, 0 &lt;= gen &lt; GEN_MOD — any live read must satisfy it,
     * regardless of which writer wrote last). Exercises segment write-lock contention,
     * lock-free leaf reads racing bucket rewrites, and dir split/collapse under load.
     */
    @Test public void h5_multiWriterMixed() {
        requireTag("h5");
        final long MULT = 1_000_000L, GEN_MOD = 100_000L;
        int n = scaled(MIXED_LOAD_N);
        long mixedOps = scaledL(MIXED_OPS);
        final int nWriters = 8, nReaders = 4;
        StoreDirect store = new StoreDirect(false);
        try {
            HTreeMap<Long, Long> map = HTreeMap.create(store, Serializers.LONG, Serializers.LONG);

            long tl = System.nanoTime();
            for (long k = 0; k < n; k++) map.put(k, k * MULT); // gen 0 base load
            phase("htree mixed BASE-LOAD(" + n + ")", n, System.nanoTime() - tl);

            final int keyspace = n;
            java.util.concurrent.atomic.AtomicReference<Throwable> failure =
                    new java.util.concurrent.atomic.AtomicReference<>();
            java.util.concurrent.atomic.AtomicInteger writersLeft =
                    new java.util.concurrent.atomic.AtomicInteger(nWriters);
            java.util.concurrent.atomic.AtomicLong reads = new java.util.concurrent.atomic.AtomicLong();
            java.util.concurrent.atomic.AtomicLong iters = new java.util.concurrent.atomic.AtomicLong();

            Thread[] writers = new Thread[nWriters];
            for (int w = 0; w < nWriters; w++) {
                final long seed = 7 + w;
                final long ops = mixedOps / nWriters;
                writers[w] = new Thread(() -> {
                    SplittableRandom rnd = new SplittableRandom(seed);
                    long gen = seed; // distinct gen streams per writer, all < GEN_MOD after mod
                    try {
                        for (long op = 0; op < ops && failure.get() == null; op++) {
                            long k = rnd.nextInt(keyspace);
                            if (rnd.nextInt(100) < 70) map.put(k, k * MULT + (gen++ % GEN_MOD));
                            else map.remove(k);
                        }
                    } catch (Throwable e) { failure.compareAndSet(null, e); }
                    finally { writersLeft.decrementAndGet(); }
                }, "htree-mixed-writer-" + w);
            }

            Thread[] readers = new Thread[nReaders];
            for (int r = 0; r < nReaders; r++) {
                final long seed = 200 + r;
                readers[r] = new Thread(() -> {
                    SplittableRandom rnd = new SplittableRandom(seed);
                    long rd = 0, itc = 0;
                    try {
                        while (writersLeft.get() > 0 && failure.get() == null) {
                            if ((rd & 65535) == 65535) { // occasional full iteration
                                Iterator<Map.Entry<Long, Long>> it = map.entryIterator();
                                while (it.hasNext()) {
                                    Map.Entry<Long, Long> e = it.next();
                                    long g = e.getValue() - e.getKey() * MULT;
                                    if (g < 0 || g >= GEN_MOD)
                                        throw new AssertionError("bad value key=" + e.getKey()
                                                + " v=" + e.getValue());
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
                    reads.addAndGet(rd);
                    iters.addAndGet(itc);
                }, "htree-mixed-reader-" + r);
            }

            long t0 = System.nanoTime();
            for (Thread t : writers) t.start();
            for (Thread t : readers) t.start();
            for (Thread t : writers) joinThread(t);
            for (Thread t : readers) joinThread(t);
            long tMix = System.nanoTime() - t0;
            if (failure.get() != null) throw new AssertionError("htree mixed failed", failure.get());

            // post-race validation: every surviving entry satisfies the value scheme,
            // iteration count agrees with sizeLong
            long count = 0;
            Iterator<Map.Entry<Long, Long>> it = map.entryIterator();
            while (it.hasNext()) {
                Map.Entry<Long, Long> e = it.next();
                long g = e.getValue() - e.getKey() * MULT;
                if (g < 0 || g >= GEN_MOD)
                    throw new AssertionError("post-race bad value key=" + e.getKey() + " v=" + e.getValue());
                count++;
            }
            assertEquals("final iteration vs sizeLong", map.sizeLong(), count);

            summary("htree mixed " + nWriters + " WRITERS put/remove(70/30)", mixedOps, tMix, 0);
            phase("htree mixed READERS get(+" + iters.get() + " iters)", reads.get(), tMix);
        } finally {
            store.close();
        }
        System.gc();
    }

    private static void joinThread(Thread t) {
        try { t.join(); } catch (InterruptedException e) { throw new RuntimeException(e); }
    }

    // ---------------- helpers ----------------

    private static String key(int i) { return String.format("key%09d", i); }

    private static long iterateCount(HTreeMap<Long, Long> map) {
        long count = 0;
        Iterator<Map.Entry<Long, Long>> it = map.entryIterator();
        while (it.hasNext()) {
            Map.Entry<Long, Long> e = it.next();
            if (!e.getValue().equals(e.getKey()))
                throw new AssertionError("value mismatch key=" + e.getKey() + " val=" + e.getValue());
            count++;
        }
        return count;
    }

    private static long iterateCountStr(HTreeMap<String, Long> map) {
        long count = 0;
        Iterator<Map.Entry<String, Long>> it = map.entryIterator();
        while (it.hasNext()) { it.next(); count++; }
        return count;
    }
}
