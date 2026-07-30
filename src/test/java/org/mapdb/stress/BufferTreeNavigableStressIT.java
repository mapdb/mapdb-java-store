package org.mapdb.stress;

import org.junit.Test;
import org.mapdb.btree.BufferTreeMap;
import org.mapdb.ser.LongFormat;
import org.mapdb.store.StoreDelta;

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.mapdb.stress.StressSupport.*;

/**
 * Concurrency stress hammer for the NEW ConcurrentNavigableMap surface of
 * {@link BufferTreeMap} — the write-optimized single-writer buffer tree. Companion to
 * {@link BTreeNavigableStressIT}, adapted for BufferTreeMap's contract:
 *
 * <ul>
 *   <li><b>SINGLE writer</b>: exactly ONE writer thread (put/remove/CAS/poll all serialize on
 *       the map-global write lock). This map does NOT scale writes — the point of the test is
 *       reader safety + CAS/poll atomicity, not write throughput. Multiple writers are OUT OF
 *       SCOPE and never spawned.</li>
 *   <li><b>Lock-free readers</b>: many reader threads run get/containsKey/navigation queries
 *       and full iterations concurrently with the writer; weakly consistent, never torn.</li>
 * </ul>
 *
 * <h3>Value convention (integrity oracle)</h3>
 * Every write stores {@code value == f(key)} for the pure function
 * {@code f(key) = key * 2654435761L}. Because the value depends ONLY on the key, ANY value a
 * reader/poll ever observes for a key must be either absent or EXACTLY {@code f(key)} — never a
 * torn or foreign value — with zero bookkeeping and stable across insert/remove/re-insert. Every
 * navigation result additionally checks its key relation (floor ≤ q, ceiling ≥ q, higher &gt; q).
 * Any violation fails the run immediately (zero tolerance).
 *
 * <p>{@code *IT} => OPT-IN: run only via {@code -Dtest=BufferTreeNavigableStressIT
 * -Dstress.durationMs=...} (surefire runs {@code *Test} by default). Store selected by
 * {@code -Dbuffertree.store=direct|appendonly}.
 */
public class BufferTreeNavigableStressIT {

    static final int  CORES     = Runtime.getRuntime().availableProcessors();
    static final int  MAX_NODE  = 8;    // small node => deep tree, frequent splits/flushes
    static final int  BUF_BYTES = 128;  // small buffers => constant flush/split churn

    /** Knuth multiplicative hash; value is a pure function of the key. */
    static long f(long key) { return key * 2654435761L; }

    /**
     * One writer mixes putIfAbsent/replace/replace(k,ov,nv)/pollFirstEntry/pollLastEntry/remove;
     * {@code 2*CORES-1} readers hammer get/containsKey/floor/ceiling/higher/iteration/descending
     * scans. Every observed value must equal {@code f(key)}; every nav result must also satisfy
     * its key relation. Poll winners are value-checked on the returned (k,v) pair (poll is atomic
     * on the pair). At quiescence the map is iterated (ascending + value-correct), flushed, and
     * iterated again (identical read surface), then {@code store.verify()} gates store invariants.
     */
    @Test public void navPollHammerSingleWriter() {
        StoreDelta store = newBufferTreeStore();
        try {
            BufferTreeMap<Long, Long> map =
                BufferTreeMap.create(store, LongFormat.INSTANCE, LongFormat.INSTANCE, MAX_NODE, BUF_BYTES);
            final int keyspace = Math.max(2_000, scaled(500_000L));

            // pre-populate half the keyspace so readers/pollers have work at once
            for (long k = 0; k < keyspace; k += 2) map.put(k, f(k));

            final AtomicReference<Throwable> failure = new AtomicReference<>();
            final AtomicLong writes = new AtomicLong(), polls = new AtomicLong();
            final AtomicLong reads = new AtomicLong(), queries = new AtomicLong(), iters = new AtomicLong();
            long deadline = System.nanoTime() + durationMs() * 1_000_000L;
            int nReaders = Math.max(1, 2 * CORES - 1);
            Thread[] threads = new Thread[nReaders + 1];

            // ---- the single writer (contract: exactly one) ----
            threads[0] = new Thread(() -> {
                SplittableRandom rnd = new SplittableRandom(7);
                long ops = 0, pc = 0;
                try {
                    while (System.nanoTime() < deadline && failure.get() == null) {
                        long k = rnd.nextInt(keyspace);
                        Map.Entry<Long, Long> e;
                        switch (rnd.nextInt(6)) {
                            case 0: map.putIfAbsent(k, f(k)); break;         // insert iff absent
                            case 1: map.replace(k, f(k)); break;            // swap iff present (same value)
                            case 2: map.replace(k, f(k), f(k)); break;      // CAS iff present
                            case 3: map.remove(k); break;                   // blind-ish remove
                            case 4:
                                e = map.pollFirstEntry();
                                if (e != null) { checkKV(failure, "pollFirst", e); pc++; }
                                break;
                            default:
                                e = map.pollLastEntry();
                                if (e != null) { checkKV(failure, "pollLast", e); pc++; }
                                break;
                        }
                        ops++;
                    }
                } catch (Throwable t) { failure.compareAndSet(null, t); }
                writes.addAndGet(ops); polls.addAndGet(pc);
            }, "buftree-nav-writer");

            // ---- lock-free readers: point reads + navigation + iteration + descending scan ----
            for (int r = 0; r < nReaders; r++) {
                final long seed = 900 + r;
                threads[r + 1] = new Thread(() -> {
                    SplittableRandom rnd = new SplittableRandom(seed);
                    long rd = 0, qc = 0, itc = 0;
                    try {
                        while (System.nanoTime() < deadline && failure.get() == null) {
                            long tick = rd & 16383;
                            if (tick == 16383) {                      // occasional full ascending iteration
                                long prev = Long.MIN_VALUE;
                                Iterator<Map.Entry<Long, Long>> it = map.entryIterator();
                                while (it.hasNext()) {
                                    Map.Entry<Long, Long> e = it.next();
                                    long key = e.getKey();
                                    if (key <= prev) fail(failure, "iter not ascending " + prev + "->" + key);
                                    if (e.getValue() != f(key))
                                        fail(failure, "iter bad value key=" + key + " v=" + e.getValue());
                                    prev = key;
                                }
                                itc++;
                            } else if (tick == 8191) {                // occasional descending scan
                                long prev = Long.MAX_VALUE;
                                NavigableMap<Long, Long> dm = map.descendingMap();
                                Iterator<Map.Entry<Long, Long>> it = dm.entrySet().iterator();
                                while (it.hasNext()) {
                                    Map.Entry<Long, Long> e = it.next();
                                    long key = e.getKey();
                                    if (key >= prev) fail(failure, "desc not descending " + prev + "->" + key);
                                    if (e.getValue() != f(key))
                                        fail(failure, "desc bad value key=" + key + " v=" + e.getValue());
                                    prev = key;
                                }
                                itc++;
                            } else {
                                long q = rnd.nextInt(keyspace);
                                switch (rnd.nextInt(6)) {
                                    case 0: {
                                        Long v = map.get(q);
                                        if (v != null && v != f(q)) fail(failure, "get bad value key=" + q + " v=" + v);
                                        break;
                                    }
                                    case 1: map.containsKey(q); break; // must never throw / torn
                                    case 2: checkRel(failure, "floor",   map.floorEntry(q),   q, k -> k <= q); qc++; break;
                                    case 3: checkRel(failure, "ceiling", map.ceilingEntry(q), q, k -> k >= q); qc++; break;
                                    case 4: {
                                        Long hk = map.higherKey(q);
                                        if (hk != null && hk <= q) fail(failure, "higherKey relation key=" + hk + " q=" + q);
                                        qc++;
                                        break;
                                    }
                                    default: checkRel(failure, "lower", map.lowerEntry(q), q, k -> k < q); qc++; break;
                                }
                            }
                            rd++;
                        }
                    } catch (Throwable t) { failure.compareAndSet(null, t); }
                    reads.addAndGet(rd); queries.addAndGet(qc); iters.addAndGet(itc);
                }, "buftree-nav-reader-" + r);
            }

            runAll(threads);
            if (failure.get() != null) throw new AssertionError("buffer tree nav hammer failed", failure.get());

            // ---- quiescent: value integrity + ascending order, identical buffered vs flushed ----
            assertAscendingValueCorrect(map, "before flushAll");
            map.flushAll();
            assertAscendingValueCorrect(map, "after flushAll");
            store.verify();

            long nanos = durationMs() * 1_000_000L;
            summary("BufferTreeNav 1writer+" + nReaders + "readers store=" + STORE_KIND,
                writes.get() + reads.get(), nanos, 0);
            phase("  writer put/replace/poll/remove", writes.get(), nanos);
            phase("  poll winners",                   polls.get(),  nanos);
            phase("  reader point reads",             reads.get(),  nanos);
            phase("  reader nav queries",             queries.get(),nanos);
            phase("  reader iterations",              iters.get(),  nanos);
        } finally {
            store.close();
        }
    }

    // ---- helpers ----

    private interface KeyRel { boolean ok(long k); }

    private static void checkKV(AtomicReference<Throwable> failure, String name, Map.Entry<Long, Long> e) {
        long k = e.getKey(), v = e.getValue();
        if (v != f(k))
            fail(failure, name + " VALUE MISMATCH key=" + k + " got=" + v + " expected=" + f(k));
    }

    private static void checkRel(AtomicReference<Throwable> failure, String name,
                                 Map.Entry<Long, Long> e, long q, KeyRel rel) {
        if (e == null) return; // no such key relative to q -> valid
        long k = e.getKey(), v = e.getValue();
        if (v != f(k))
            fail(failure, name + "Entry(" + q + ") VALUE MISMATCH key=" + k + " got=" + v + " expected=" + f(k));
        if (!rel.ok(k))
            fail(failure, name + "Entry(" + q + ") RELATION broken: key=" + k);
    }

    private static void assertAscendingValueCorrect(BufferTreeMap<Long, Long> map, String when) {
        long prev = Long.MIN_VALUE;
        Iterator<Map.Entry<Long, Long>> it = map.entryIterator();
        while (it.hasNext()) {
            Map.Entry<Long, Long> e = it.next();
            long k = e.getKey(), v = e.getValue();
            if (k <= prev) throw new AssertionError("[" + when + "] final iter not ascending " + prev + "->" + k);
            if (v != f(k)) throw new AssertionError("[" + when + "] final value mismatch key=" + k
                + " got=" + v + " expected=" + f(k));
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
