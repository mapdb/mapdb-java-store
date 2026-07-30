package org.mapdb.stress;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.mapdb.btree.BufferTreeMap;
import org.mapdb.ser.GroupFormat;
import org.mapdb.ser.LongDeltaFormat;
import org.mapdb.ser.LongFormat;
import org.mapdb.store.StoreDelta;

import java.util.Iterator;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.mapdb.stress.StressSupport.*;

/**
 * {@link BufferTreeMap} scale over a heap-slice store — the write-optimized
 * buffer tree (Bε-tree). Companion to {@link BTreeScaleIT}, same house pattern
 * (tag-gated, {@code stress.scale}, {@link StressSupport} reporting).
 *
 * Long-&gt;Long, maxNodeSize 32, bufferBytes 4096 (unless a knob is under test);
 * {@code -Dbuffertree.leafHeadroom=0|512|4096|...} sweeps leaf headroom
 * (default = {@link BufferTreeMap#DEFAULT_LEAF_HEADROOM}; 4096 reproduces the
 * old flat budget); {@code -Dbuffertree.store=direct|appendonly} selects the
 * store (segment-locked {@code StoreDirect} vs single-writer/wait-free-reader
 * {@code StoreAppendOnly}); {@code -Dbuffertree.keyFormat=long|longdelta}
 * selects the key group format (fixed 8-byte stride vs packed zigzag deltas).
 * Tags gate each phase so the normal suite never runs it (also excluded as {@code *IT}):
 * run one with e.g. {@code -Dtest=BufferTreeScaleIT -Dstress.only=bt1}.
 *
 * Self-validating content: value = key * MULT + gen with 0 &lt;= gen &lt; GEN_MOD, so any
 * live read satisfies {@code 0 <= value - key*MULT < GEN_MOD} and full iteration is
 * strictly ascending (like {@link BufferTreeStressIT}). The base load writes gen 0,
 * so an untouched key reads back exactly {@code key*MULT}.
 *
 * Full-scale (scale=1.0), each phase on its own freshly-built tree:
 *   bt1 randomLoad : 300,000,000 shuffled puts (single writer); size == count check.
 *   bt2 mixed      : build the load tree, then 100,000,000 blind put/remove (70/30)
 *                    by one writer interleaved with 4 validating reader threads
 *                    (random gets + periodic forEach sweeps).
 *   bt3 readAfter  : build the load tree, then 100,000,000 random gets, one full
 *                    forEach (entries/s), then flushAll and repeat the gets — the
 *                    buffered-read vs consolidated-read cost.
 *
 * Two ways to run:
 *  - JUnit (correctness / smoke, assertions on):
 *      mvn test -Dtest=BufferTreeScaleIT -Dstress.only=bt1 -Dstress.scale=0.005
 *  - {@link #main} (full-scale perf, clean JVM flags, no surefire argLine/-ea wrangling,
 *    trivial to attach Java Flight Recorder). Args are the tags to run in order:
 *      java -Xmx80g -Xms80g -XX:+UseParallelGC -XX:+AlwaysPreTouch \
 *           -XX:StartFlightRecording=filename=bt1.jfr,settings=profile,dumponexit=true \
 *           -Dstress.scale=1.0 -cp &lt;cp&gt; org.mapdb.stress.BufferTreeScaleIT bt1
 *    Assertions are simply left off (no {@code -ea}) for production-shape numbers; the
 *    {@code assertEquals}/relation checks are plain method calls, so correctness still holds.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class BufferTreeScaleIT {

    /** Full-scale perf entry: each arg is a tag (bt1/bt2/bt3) run in sequence. */
    public static void main(String[] args) {
        if (args.length == 0) args = new String[]{"bt1", "bt2", "bt3"};
        BufferTreeScaleIT t = new BufferTreeScaleIT();
        System.out.printf("[STRESS] BufferTreeScaleIT main: scale=%s cores=%d leafHeadroom=%d store=%s keyFormat=%s args=%s%n",
            System.getProperty("stress.scale", "1.0"), CORES, LEAF_HEADROOM, STORE_KIND, KEY_FORMAT,
            java.util.Arrays.toString(args));
        for (String tag : args) {
            System.setProperty("stress.only", tag);
            long t0 = System.nanoTime();
            switch (tag) {
                case "bt1": t.bt1_randomLoad(); break;
                case "bt2": t.bt2_mixed(); break;
                case "bt3": t.bt3_readAfter(); break;
                default: throw new IllegalArgumentException("unknown tag " + tag);
            }
            System.out.printf("[STRESS] tag %s done in %.1fs%n", tag, (System.nanoTime() - t0) / 1e9);
        }
    }

    static final long LOAD_N   = 300_000_000L;
    static final long MIXED_N  = 100_000_000L;
    static final long GET_N    = 100_000_000L;

    static final int  MAX_NODE  = 32;
    static final int  BUF_BYTES = 4096;
    /** Leaf append headroom; -Dbuffertree.leafHeadroom=0|512|4096|... sweeps it (4096 = the old flat budget). */
    static final int  LEAF_HEADROOM =
        Integer.getInteger("buffertree.leafHeadroom", BufferTreeMap.DEFAULT_LEAF_HEADROOM);
    /** Key group format; -Dbuffertree.keyFormat=long|longdelta (values always LongFormat). */
    static final String KEY_FORMAT = System.getProperty("buffertree.keyFormat", "long");

    static GroupFormat<Long> keyFormat() {
        switch (KEY_FORMAT) {
            case "long": return LongFormat.INSTANCE;
            case "longdelta": return LongDeltaFormat.INSTANCE;
            default: throw new IllegalArgumentException("unknown buffertree.keyFormat: " + KEY_FORMAT);
        }
    }
    static final long MULT      = 1_000_003L;
    static final long GEN_MOD   = 100_000L;

    static final int  CORES = Runtime.getRuntime().availableProcessors();

    /** Config suffix for sweep-compared labels, so pasted snippets are self-identifying. */
    private static String cfg() {
        return "(maxNode=" + MAX_NODE + ",buf=" + BUF_BYTES + ",leafHr=" + LEAF_HEADROOM
                + ",store=" + STORE_KIND + ",keyFmt=" + KEY_FORMAT + ")";
    }

    // ---------------- bt1: random load ----------------

    @Test public void bt1_randomLoad() {
        requireTag("bt1");
        int n = scaled(LOAD_N);
        StoreDelta store = newBufferTreeStore();
        try {
            BufferTreeMap<Long, Long> map =
                BufferTreeMap.create(store, keyFormat(), LongFormat.INSTANCE, MAX_NODE, BUF_BYTES, LEAF_HEADROOM);

            long t0 = System.nanoTime();
            loadShuffled(map, n, 2024);
            long tPut = System.nanoTime() - t0;

            long t1 = System.nanoTime();
            long count = map.sizeLong();
            long tSize = System.nanoTime() - t1;
            assertEquals("size after load", (long) n, count);

            System.gc(); // heapUsed below ≈ live tree+store, not load garbage (it is THE capacity metric for headroom sweeps)
            summary("buftree randomLoad PUT " + cfg(), n, tPut, n * 16L);
            phase("buftree randomLoad sizeLong(" + count + ")", count, tSize);
        } finally {
            store.close();
        }
        System.gc();
    }

    // ---------------- bt2: mixed writer + concurrent readers ----------------

    @Test public void bt2_mixed() {
        requireTag("bt2");
        int n = scaled(LOAD_N);
        long mixedOps = scaledL(MIXED_N);
        StoreDelta store = newBufferTreeStore();
        try {
            BufferTreeMap<Long, Long> map =
                BufferTreeMap.create(store, keyFormat(), LongFormat.INSTANCE, MAX_NODE, BUF_BYTES, LEAF_HEADROOM);

            long tl = System.nanoTime();
            loadShuffled(map, n, 2024);
            phase("buftree mixed BASE-LOAD(" + n + ")", n, System.nanoTime() - tl);

            final int keyspace = n;
            AtomicReference<Throwable> failure = new AtomicReference<>();
            AtomicBoolean writerDone = new AtomicBoolean(false);
            AtomicLong reads = new AtomicLong(), iters = new AtomicLong();
            final int nReaders = 4;
            Thread[] readers = new Thread[nReaders];

            Thread writer = new Thread(() -> {              // single writer (contract)
                SplittableRandom rnd = new SplittableRandom(7);
                long gen = 0;
                try {
                    for (long op = 0; op < mixedOps && failure.get() == null; op++) {
                        long k = rnd.nextInt(keyspace);
                        if (rnd.nextInt(100) < 70) map.put(k, k * MULT + (gen++ % GEN_MOD));
                        else map.remove(k);
                    }
                } catch (Throwable e) { failure.compareAndSet(null, e); }
                finally { writerDone.set(true); }
            }, "buftree-mixed-writer");

            for (int r = 0; r < nReaders; r++) {
                final long seed = 200 + r;
                readers[r] = new Thread(() -> {
                    SplittableRandom rnd = new SplittableRandom(seed);
                    long rd = 0, itc = 0;
                    try {
                        while (!writerDone.get() && failure.get() == null) {
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
                }, "buftree-mixed-reader-" + r);
            }

            long t0 = System.nanoTime();
            writer.start();
            for (Thread t : readers) t.start();
            join(writer);
            for (Thread t : readers) join(t);
            long tMix = System.nanoTime() - t0;
            if (failure.get() != null) throw new AssertionError("buffer tree mixed failed", failure.get());

            summary("buftree mixed WRITER put/remove(70/30) " + cfg(), mixedOps, tMix, 0);
            phase("buftree mixed READERS get(+" + iters.get() + " iters)", reads.get(), tMix);
        } finally {
            store.close();
        }
        System.gc();
    }

    // ---------------- bt3: read after load, buffered vs consolidated ----------------

    @Test public void bt3_readAfter() {
        requireTag("bt3");
        int n = scaled(LOAD_N);
        long getN = scaledL(GET_N);
        StoreDelta store = newBufferTreeStore();
        try {
            BufferTreeMap<Long, Long> map =
                BufferTreeMap.create(store, keyFormat(), LongFormat.INSTANCE, MAX_NODE, BUF_BYTES, LEAF_HEADROOM);

            long tl = System.nanoTime();
            loadShuffled(map, n, 2024);
            phase("buftree readAfter BASE-LOAD(" + n + ")", n, System.nanoTime() - tl);

            // gets over the buffered tree
            long t0 = System.nanoTime();
            gets(map, n, getN, 555);
            long tGetBuf = System.nanoTime() - t0;

            // one full forEach (weakly-consistent DFS merge)
            long t1 = System.nanoTime();
            long count = forEachAscending(map, n);
            long tIter = System.nanoTime() - t1;
            assertEquals("forEach count", (long) n, count);

            // drain all buffers to leaves, then repeat gets against consolidated leaves
            long t2 = System.nanoTime();
            map.flushAll();
            long tFlush = System.nanoTime() - t2;

            long t3 = System.nanoTime();
            gets(map, n, getN, 555);
            long tGetFlat = System.nanoTime() - t3;

            assertEquals("size after flush", (long) n, map.sizeLong());

            summary("buftree readAfter GET buffered " + cfg(), getN, tGetBuf, 0);
            phase("buftree readAfter forEach(" + count + ")", count, tIter);
            phase("buftree readAfter flushAll", n, tFlush);
            phase("buftree readAfter GET consolidated", getN, tGetFlat);
        } finally {
            store.close();
        }
        System.gc();
    }

    // ---------------- helpers ----------------

    /** Insert keys [0,n) in a seeded shuffled order; value = key*MULT (gen 0). */
    private static void loadShuffled(BufferTreeMap<Long, Long> map, int n, long seed) {
        long[] keys = new long[n];
        for (int i = 0; i < n; i++) keys[i] = i;
        SplittableRandom rnd = new SplittableRandom(seed);
        for (int i = n - 1; i > 0; i--) { // Fisher-Yates
            int j = rnd.nextInt(i + 1);
            long t = keys[i]; keys[i] = keys[j]; keys[j] = t;
        }
        for (int i = 0; i < n; i++) map.put(keys[i], keys[i] * MULT);
        // keys array (~n*8 bytes) is garbage now — let the read phases have the heap back
    }

    /** getN random gets over keyspace [0,n); untouched keys must read back exactly key*MULT. */
    private static void gets(BufferTreeMap<Long, Long> map, int n, long getN, long seed) {
        SplittableRandom rnd = new SplittableRandom(seed);
        for (long i = 0; i < getN; i++) {
            long k = rnd.nextInt(n);
            Long v = map.get(k);
            if (v == null || v != k * MULT) throw new AssertionError("get(" + k + ")=" + v);
        }
    }

    private static long forEachAscending(BufferTreeMap<Long, Long> map, int n) {
        long[] state = {0, Long.MIN_VALUE}; // count, prevKey
        map.forEach((k, v) -> {
            if (state[0] > 0 && k <= state[1])
                throw new AssertionError("forEach not ascending: " + state[1] + " -> " + k);
            long g = v - k * MULT;
            if (g < 0 || g >= GEN_MOD)
                throw new AssertionError("bad value key=" + k + " v=" + v);
            state[1] = k;
            state[0]++;
        });
        return state[0];
    }

    private static void join(Thread t) {
        try { t.join(); } catch (InterruptedException e) { throw new RuntimeException(e); }
    }
}
