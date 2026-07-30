package org.mapdb.stress;

import org.junit.Test;
import org.mapdb.TmpFiles;
import org.mapdb.btree.BufferTreeMap;
import org.mapdb.ser.LongFormat;
import org.mapdb.store.StoreAppendOnly;
import org.mapdb.store.StoreByteArray;
import org.mapdb.store.StoreDelta;
import org.mapdb.store.StoreDirect;
import org.mapdb.store.StoreWAL;

import java.io.File;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.mapdb.stress.StressSupport.durationMs;
import static org.mapdb.stress.StressSupport.phase;
import static org.mapdb.stress.StressSupport.scaled;
import static org.mapdb.stress.StressSupport.summary;

/**
 * HARDER, independent concurrency hammer for {@link BufferTreeMap} as a
 * {@link java.util.concurrent.ConcurrentNavigableMap}. Where
 * {@link BufferTreeNavigableStressIT} runs a SINGLE writer, this one runs
 * {@code CORES} writer threads (they serialize on the map-global writer lock —
 * exactly the contention this test wants: it stresses the lock + flush/split
 * machinery, and it verifies the CAS methods are truly atomic under that lock).
 *
 * <p>Runs on EVERY thread-safe store dialect (StoreDirect, StoreByteArray,
 * StoreAppendOnly, StoreWAL — all constructed {@code threadSafe=true}) via a
 * separate {@code @Test} per dialect so a failure is isolated.
 *
 * <h3>Key-space partition (all ranges disjoint)</h3>
 * <ul>
 *   <li><b>CANON</b> {@code [0,KS)} — put / putIfAbsent / replace / replace(k,ov,nv) /
 *       remove. Value convention {@code f(k)=k*2654435761L}: any value observed for such
 *       a key is absent or EXACTLY {@code f(k)} (a torn/foreign value fails instantly).
 *       Large {@code KS} with a small node + buffer forces deep trees, multi-level
 *       flushes and frequent k-way splits.</li>
 *   <li><b>POLL</b> {@code [KS,KS+PS)} — pre-filled with {@code f(k)}; writers
 *       {@code pollFirstEntry}/{@code pollLastEntry} on {@code subMap(POLL range)} and
 *       re-insert, so poll churn cannot touch any other range. Poll winners are
 *       value-checked on the returned pair.</li>
 *   <li><b>COUNTER</b> {@code [KS+PS,KS+PS+CN)} — a "no lost update" race: writers
 *       CAS-increment shared counters via {@code replace(k,old,new)} retry loops,
 *       counting successful increments per key. At quiescence each counter's stored
 *       value MUST equal the number of successful increments — no lost updates.
 *       (These hold monotone counter values, not {@code f(k)}, so the value oracle
 *       skips this range.)</li>
 *   <li><b>PIA</b> {@code [.. , +PN)} — a single-winner {@code putIfAbsent} race:
 *       writers race {@code putIfAbsent(k,f(k))}; EXACTLY one attempt per key may return
 *       null (win). A per-key winner tally must never exceed 1, and any present PIA key
 *       must have exactly one recorded winner. Value is {@code f(k)}, so the oracle
 *       covers it.</li>
 * </ul>
 *
 * <h3>Readers ({@code >= 2*CORES})</h3>
 * get / containsKey / floor / ceiling / lower / higher, full ascending
 * {@code entryIterator} scans, {@code descendingMap} scans, {@code subMap} range scans,
 * and {@code navigableKeySet} iteration. Every full/range scan asserts STRICT key
 * monotonicity (ascending strictly up, descending strictly down — never a duplicate or
 * out-of-order key) and the {@code f(k)} value oracle on every non-COUNTER key.
 *
 * <h3>Quiescence</h3>
 * iterate → snapshot; {@code flushAll()}; iterate again → IDENTICAL read surface;
 * counter oracle (no lost updates); PIA single-winner oracle; {@code store.verify()}.
 *
 * <p>{@code *IT} OPT-IN: {@code mvn -o test -Dtest=BufferTreeHardConcurrencyIT
 * -Dstress.durationMs=5000}. Scales with {@code -Dstress.scale}.
 */
public class BufferTreeHardConcurrencyIT {

    static final int CORES     = Runtime.getRuntime().availableProcessors();
    static final int MAX_NODE  = 8;    // tiny node => deep tree, constant splits
    static final int BUF_BYTES = 128;  // tiny buffer => constant multi-level flushes

    /** Knuth multiplicative hash; value is a pure function of the key. */
    static long f(long key) { return key * 2654435761L; }

    @Test public void hammerStoreDirect() {
        run("StoreDirect(threadSafe)", () -> new StoreDirect(false, true), null);
    }

    @Test public void hammerStoreByteArray() {
        run("StoreByteArray(threadSafe)", () -> new StoreByteArray(true), null);
    }

    @Test public void hammerStoreAppendOnly() {
        run("StoreAppendOnly(threadSafe)", () -> new StoreAppendOnly(false), null);
    }

    @Test public void hammerStoreWAL() throws Exception {
        File wal = TmpFiles.tempFile("buftree-hard-wal", ".wal");
        wal.delete();
        try {
            run("StoreWAL(threadSafe)", () -> new StoreWAL(wal, false, true), null);
        } finally {
            wal.delete();
            new File(wal.getPath() + ".0").delete();
        }
    }

    // ================= core hammer =================

    private void run(String dialect, Supplier<StoreDelta> storeFactory, Void unused) {
        StoreDelta store = storeFactory.get();
        try {
            BufferTreeMap<Long, Long> map =
                BufferTreeMap.create(store, LongFormat.INSTANCE, LongFormat.INSTANCE, MAX_NODE, BUF_BYTES);

            // ---- key-space layout (all disjoint) ----
            final int KS   = Math.max(4_000, scaled(200_000L)); // CANON range, forces splits
            final int PS   = Math.max(256, KS / 50);            // POLL range
            final int CN   = 64;                                // COUNTER keys (contended)
            final int PN   = 128;                               // PIA keys (single-winner)
            final long POLL_LO = KS,               POLL_HI = KS + PS;               // [lo, hi)
            final long CTR_LO  = KS + PS,           CTR_HI  = KS + PS + CN;
            final long PIA_LO  = KS + PS + CN,      PIA_HI  = KS + PS + CN + PN;

            // pre-populate: half of CANON (evens), all of POLL, all COUNTER=0
            for (long k = 0; k < KS; k += 2) map.put(k, f(k));
            for (long k = POLL_LO; k < POLL_HI; k++) map.put(k, f(k));
            for (long k = CTR_LO; k < CTR_HI; k++) map.put(k, 0L);

            // per-key oracles
            final AtomicLong[] ctrSuccess = new AtomicLong[CN];
            for (int i = 0; i < CN; i++) ctrSuccess[i] = new AtomicLong();
            final AtomicInteger[] piaWinners = new AtomicInteger[PN];
            for (int i = 0; i < PN; i++) piaWinners[i] = new AtomicInteger();

            final AtomicReference<Throwable> failure = new AtomicReference<>();
            final AtomicLong writes = new AtomicLong(), polls = new AtomicLong();
            final AtomicLong reads = new AtomicLong(), queries = new AtomicLong(), iters = new AtomicLong();
            final long deadline = System.nanoTime() + durationMs() * 1_000_000L;

            final int nWriters = Math.max(2, CORES);
            final int nReaders = Math.max(2, 2 * CORES);
            Thread[] threads = new Thread[nWriters + nReaders];

            // ---- writers (serialize on the map's writer lock) ----
            for (int w = 0; w < nWriters; w++) {
                final long seed = 100 + w;
                threads[w] = new Thread(() -> {
                    SplittableRandom rnd = new SplittableRandom(seed);
                    long ops = 0, pc = 0;
                    try {
                        while (System.nanoTime() < deadline && failure.get() == null) {
                            int pick = rnd.nextInt(100);
                            if (pick < 55) {                                   // CANON mutate
                                long k = rnd.nextInt(KS);
                                switch (rnd.nextInt(6)) {
                                    case 0: map.put(k, f(k)); break;
                                    case 1: map.putIfAbsent(k, f(k)); break;
                                    case 2: map.replace(k, f(k)); break;
                                    case 3: map.replace(k, f(k), f(k)); break;
                                    case 4: map.remove(k); break;
                                    default: map.removeOnly(k); break;
                                }
                            } else if (pick < 70) {                            // POLL churn (bounded subMap)
                                NavigableMap<Long, Long> sub = map.subMap(POLL_LO, true, POLL_HI - 1, true);
                                Map.Entry<Long, Long> e = (rnd.nextBoolean())
                                        ? sub.pollFirstEntry() : sub.pollLastEntry();
                                if (e != null) {
                                    checkKV(failure, "poll", e.getKey(), e.getValue());
                                    map.put(e.getKey(), f(e.getKey()));       // re-insert to keep range busy
                                    pc++;
                                }
                            } else if (pick < 85) {                            // COUNTER: no-lost-update CAS
                                int idx = rnd.nextInt(CN);
                                long k = CTR_LO + idx;
                                for (;;) {
                                    Long cur = map.get(k);
                                    if (cur == null) { fail(failure, "counter key vanished k=" + k); break; }
                                    if (map.replace(k, cur, cur + 1)) { ctrSuccess[idx].incrementAndGet(); break; }
                                }
                            } else {                                           // PIA: single-winner
                                int idx = rnd.nextInt(PN);
                                long k = PIA_LO + idx;
                                Long prev = map.putIfAbsent(k, f(k));
                                if (prev == null) piaWinners[idx].incrementAndGet();
                            }
                            ops++;
                        }
                    } catch (Throwable t) { failure.compareAndSet(null, t); }
                    writes.addAndGet(ops); polls.addAndGet(pc);
                }, "buftree-hard-writer-" + w);
            }

            // ---- readers: 1/3 are dedicated "scanners" that continuously exercise
            //      concurrent iteration (bounded ranges + periodic full asc/desc/keySet
            //      scans, all monotonicity-checked); the rest do point + navigation reads. ----
            for (int r = 0; r < nReaders; r++) {
                final long seed = 9000 + r;
                final boolean scanner = (r % 3 == 0);
                threads[nWriters + r] = new Thread(() -> {
                    SplittableRandom rnd = new SplittableRandom(seed);
                    long rd = 0, qc = 0, itc = 0;
                    try {
                        while (System.nanoTime() < deadline && failure.get() == null) {
                            if (scanner) {
                                long phase = itc % 12;
                                if (phase == 0) {                                // full ascending
                                    scanAscending(map, failure, CTR_LO, CTR_HI);
                                } else if (phase == 6) {                         // full descending
                                    scanDescending(map, failure, CTR_LO, CTR_HI);
                                } else if (phase == 3) {                         // navigableKeySet
                                    long prev = Long.MIN_VALUE;
                                    NavigableSet<Long> ks = map.navigableKeySet();
                                    for (Long key : ks) {
                                        if (key <= prev) fail(failure, "keySet not ascending " + prev + "->" + key);
                                        prev = key;
                                    }
                                } else {                                         // bounded subMap range
                                    long a = rnd.nextInt(KS), b = a + rnd.nextInt(1 + KS / 16);
                                    scanRange(map, failure, a, Math.min(KS, b), CTR_LO, CTR_HI);
                                }
                                itc++;
                            } else {
                                long q = rnd.nextInt(KS);
                                switch (rnd.nextInt(6)) {
                                    case 0: {
                                        Long v = map.get(q);
                                        if (v != null && v != f(q)) fail(failure, "get bad value key=" + q + " v=" + v);
                                        break;
                                    }
                                    case 1: map.containsKey(q); break;
                                    case 2: checkRel(failure, "floor",   map.floorEntry(q),   q, k -> k <= q, CTR_LO, CTR_HI); qc++; break;
                                    case 3: checkRel(failure, "ceiling", map.ceilingEntry(q), q, k -> k >= q, CTR_LO, CTR_HI); qc++; break;
                                    case 4: checkRel(failure, "higher",  map.higherEntry(q),  q, k -> k >  q, CTR_LO, CTR_HI); qc++; break;
                                    default:checkRel(failure, "lower",   map.lowerEntry(q),   q, k -> k <  q, CTR_LO, CTR_HI); qc++; break;
                                }
                            }
                            rd++;
                        }
                    } catch (Throwable t) { failure.compareAndSet(null, t); }
                    reads.addAndGet(rd); queries.addAndGet(qc); iters.addAndGet(itc);
                }, "buftree-hard-reader-" + r);
            }

            runAll(threads);
            if (failure.get() != null) throw new AssertionError("hard hammer failed [" + dialect + "]", failure.get());

            // ---- quiescent oracles ----
            // 1. ascending value+order correct, buffered vs flushed identical read surface
            java.util.TreeMap<Long, Long> before = snapshot(map);
            assertMonotoneAndValues(before, CTR_LO, CTR_HI, dialect + " before flush");
            map.flushAll();
            java.util.TreeMap<Long, Long> after = snapshot(map);
            assertMonotoneAndValues(after, CTR_LO, CTR_HI, dialect + " after flush");
            if (!before.equals(after))
                throw new AssertionError("[" + dialect + "] read surface changed across flushAll: "
                        + before.size() + " -> " + after.size());

            // 2. no lost updates on the counters
            for (int i = 0; i < CN; i++) {
                long k = CTR_LO + i;
                Long stored = map.get(k);
                long expect = ctrSuccess[i].get();
                if (stored == null || stored != expect)
                    throw new AssertionError("[" + dialect + "] LOST UPDATE counter k=" + k
                            + " stored=" + stored + " expectedSuccesses=" + expect);
            }

            // 3. single-winner putIfAbsent
            for (int i = 0; i < PN; i++) {
                long k = PIA_LO + i;
                int wins = piaWinners[i].get();
                if (wins > 1)
                    throw new AssertionError("[" + dialect + "] putIfAbsent MULTIPLE winners k=" + k + " wins=" + wins);
                boolean present = map.containsKey(k);
                if (present && wins != 1)
                    throw new AssertionError("[" + dialect + "] PIA key present but winners=" + wins + " k=" + k);
                if (present) {
                    Long v = map.get(k);
                    if (v == null || v != f(k))
                        throw new AssertionError("[" + dialect + "] PIA bad value k=" + k + " v=" + v);
                }
            }

            // 4. store invariants
            store.verify();

            long nanos = durationMs() * 1_000_000L;
            summary("BufferTreeHard " + nWriters + "w+" + nReaders + "r " + dialect,
                    writes.get() + reads.get(), nanos, 0);
            phase("  writers put/CAS/poll/remove", writes.get(),  nanos);
            phase("  poll winners",                polls.get(),   nanos);
            phase("  reader point reads",          reads.get(),   nanos);
            phase("  reader nav queries",          queries.get(), nanos);
            phase("  reader full/range scans",     iters.get(),   nanos);
        } finally {
            store.close();
        }
    }

    // ================= helpers =================

    private interface KeyRel { boolean ok(long k); }

    private static boolean isCounter(long k, long ctrLo, long ctrHi) { return k >= ctrLo && k < ctrHi; }

    private static void checkKV(AtomicReference<Throwable> failure, String name, long k, long v) {
        if (v != f(k)) fail(failure, name + " VALUE MISMATCH key=" + k + " got=" + v + " expected=" + f(k));
    }

    private static void checkRel(AtomicReference<Throwable> failure, String name, Map.Entry<Long, Long> e,
                                 long q, KeyRel rel, long ctrLo, long ctrHi) {
        if (e == null) return;
        long k = e.getKey(), v = e.getValue();
        if (!isCounter(k, ctrLo, ctrHi) && v != f(k))
            fail(failure, name + "Entry(" + q + ") VALUE MISMATCH key=" + k + " got=" + v + " expected=" + f(k));
        if (!rel.ok(k))
            fail(failure, name + "Entry(" + q + ") RELATION broken: key=" + k + " q=" + q);
    }

    private static void scanAscending(BufferTreeMap<Long, Long> map, AtomicReference<Throwable> failure,
                                      long ctrLo, long ctrHi) {
        long prev = Long.MIN_VALUE;
        Iterator<Map.Entry<Long, Long>> it = map.entryIterator();
        while (it.hasNext()) {
            Map.Entry<Long, Long> e = it.next();
            long k = e.getKey(), v = e.getValue();
            if (k <= prev) fail(failure, "asc not strictly increasing " + prev + "->" + k);
            if (!isCounter(k, ctrLo, ctrHi) && v != f(k)) fail(failure, "asc bad value key=" + k + " v=" + v);
            prev = k;
        }
    }

    private static void scanDescending(BufferTreeMap<Long, Long> map, AtomicReference<Throwable> failure,
                                       long ctrLo, long ctrHi) {
        long prev = Long.MAX_VALUE;
        NavigableMap<Long, Long> dm = map.descendingMap();
        for (Map.Entry<Long, Long> e : dm.entrySet()) {
            long k = e.getKey(), v = e.getValue();
            if (k >= prev) fail(failure, "desc not strictly decreasing " + prev + "->" + k);
            if (!isCounter(k, ctrLo, ctrHi) && v != f(k)) fail(failure, "desc bad value key=" + k + " v=" + v);
            prev = k;
        }
    }

    private static void scanRange(BufferTreeMap<Long, Long> map, AtomicReference<Throwable> failure,
                                  long lo, long hi, long ctrLo, long ctrHi) {
        if (hi <= lo) return;
        long prev = Long.MIN_VALUE;
        NavigableMap<Long, Long> sub = map.subMap(lo, true, hi, false);
        for (Map.Entry<Long, Long> e : sub.entrySet()) {
            long k = e.getKey(), v = e.getValue();
            if (k < lo || k >= hi) fail(failure, "range key out of bounds k=" + k + " [" + lo + "," + hi + ")");
            if (k <= prev) fail(failure, "range not strictly increasing " + prev + "->" + k);
            if (!isCounter(k, ctrLo, ctrHi) && v != f(k)) fail(failure, "range bad value key=" + k + " v=" + v);
            prev = k;
        }
    }

    private static java.util.TreeMap<Long, Long> snapshot(BufferTreeMap<Long, Long> map) {
        java.util.TreeMap<Long, Long> out = new java.util.TreeMap<>();
        Iterator<Map.Entry<Long, Long>> it = map.entryIterator();
        while (it.hasNext()) {
            Map.Entry<Long, Long> e = it.next();
            Long dup = out.put(e.getKey(), e.getValue());
            if (dup != null) throw new AssertionError("duplicate key in iteration: " + e.getKey());
        }
        return out;
    }

    private static void assertMonotoneAndValues(java.util.TreeMap<Long, Long> snap, long ctrLo, long ctrHi, String when) {
        long prev = Long.MIN_VALUE;
        for (Map.Entry<Long, Long> e : snap.entrySet()) {
            long k = e.getKey(), v = e.getValue();
            if (k <= prev) throw new AssertionError("[" + when + "] not ascending " + prev + "->" + k);
            if (!isCounter(k, ctrLo, ctrHi) && v != f(k))
                throw new AssertionError("[" + when + "] value mismatch key=" + k + " got=" + v + " expected=" + f(k));
            prev = k;
        }
    }

    private static void fail(AtomicReference<Throwable> failure, String msg) {
        AssertionError err = new AssertionError(msg);
        failure.compareAndSet(null, err);
        throw err;
    }

    private static void runAll(Thread[] threads) {
        for (Thread t : threads) t.start();
        for (Thread t : threads) {
            try { t.join(); } catch (InterruptedException e) { throw new RuntimeException(e); }
        }
    }
}
