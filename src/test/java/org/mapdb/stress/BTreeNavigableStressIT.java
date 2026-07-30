package org.mapdb.stress;

import org.junit.Test;
import org.mapdb.btree.BTreeMap;
import org.mapdb.ser.LongFormat;
import org.mapdb.store.Store;
import org.mapdb.store.StoreByteArray;
import org.mapdb.store.StoreDirect;

import java.util.Iterator;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.mapdb.stress.StressSupport.*;

/**
 * Concurrency stress hammer for the atomic poll added to {@link BTreeMap} (now a
 * {@code java.util.concurrent.ConcurrentNavigableMap}). Threads = 2x cores; each sub-test
 * runs {@code stress.durationMs} (default 30s). {@code *IT} => OPT-IN (run only via
 * {@code -Dtest=BTreeNavigableStressIT}). Self-validating; fails immediately on any
 * invariant break.
 *
 * <h3>Value convention (integrity oracle)</h3>
 * Every live entry has {@code value == f(key)} for the fixed pure function
 * {@code f(key) = key * 2654435761L}. Since the value depends ONLY on the key, any
 * poll/get/nav result can be checked for value integrity with zero bookkeeping, and the
 * check is stable across insert/remove/re-insert cycles of the same key.
 *
 * <h3>Poll guarantee under test (spec: nav-impl-context §Cost/consistency)</h3>
 * {@code pollFirstEntry/pollLastEntry} are ATOMIC on the (key,value) PAIR via a
 * conditional-remove retry loop: they NEVER remove a value they did not return. The
 * least/greatest SELECTION is only weakly consistent, so we do NOT assert linearizability
 * of WHICH key comes out — only:
 * <ul>
 *   <li><b>k-v atomicity</b>: every polled entry satisfies {@code value == f(key)};</li>
 *   <li><b>no phantom / no double-poll</b>: every polled key corresponds to exactly one
 *       live insert (a shared {@code live} map is CAS-cleared by the poll winner; clearing
 *       an already-absent key is impossible unless poll broke atomicity);</li>
 *   <li><b>conservation</b>: {@code finalSize == inserted - polled - producerRemoves}.</li>
 * </ul>
 *
 * <h3>Presence protocol</h3>
 * A key enters the map ONLY via its owning producer (producers own DISJOINT key ranges).
 * It leaves via producer {@code remove} OR a poll. All three transitions update a shared
 * {@code ConcurrentHashMap<Long,Boolean> live}:
 * <ul>
 *   <li>insert: {@code live.putIfAbsent(k,TRUE)} (gate: skip if already live), THEN
 *       {@code map.put(k, f(k))};</li>
 *   <li>poll winner (entry != null): assert {@code v==f(k)}, then {@code live.remove(k)}
 *       MUST return TRUE (else phantom/double-poll);</li>
 *   <li>producer remove winner (old != null): assert {@code v==f(k)}, then
 *       {@code live.remove(k)} MUST return TRUE (else an entry was present but not live).</li>
 * </ul>
 * Because both poll and {@code remove} bottom out in the store's atomic conditional-remove,
 * exactly one of two racing removers observes the value; the other sees null. So each live
 * insert is claimed exactly once, and the counters stay exact.
 */
public class BTreeNavigableStressIT {

    static final int CORES = Runtime.getRuntime().availableProcessors();
    /** Knuth multiplicative hash; value is a pure function of the key. */
    static long f(long key) { return key * 2654435761L; }

    // ============================ 1. poll atomicity hammer ============================

    @Test public void pollAtomicityDirect() {
        StoreDirect s = new StoreDirect(false);
        try { pollHammer(s, "StoreDirect"); } finally { s.close(); }
    }

    @Test public void pollAtomicityByteArray() {
        StoreByteArray s = new StoreByteArray();
        try { pollHammer(s, "StoreByteArray"); } finally { s.close(); }
    }

    private void pollHammer(Store store, String label) {
        // small maxNodeSize => deep tree, frequent splits/merges under the poll retry loop
        final int maxNodeSize = 8;
        BTreeMap<Long, Long> map =
            BTreeMap.create(store, LongFormat.INSTANCE, LongFormat.INSTANCE, maxNodeSize);

        final int nThreads   = Math.max(4, 2 * CORES);
        final int nPollers   = Math.max(1, nThreads / 2);
        final int nProducers = Math.max(1, nThreads - nPollers);
        final int keyspace   = Math.max(2_000, scaled(200_000L));
        final int rangeSize  = Math.max(16, keyspace / nProducers);

        // shared presence map: key -> Boolean.TRUE iff a live insert exists (or is being placed)
        final ConcurrentHashMap<Long, Boolean> live = new ConcurrentHashMap<>();
        final AtomicLong inserted = new AtomicLong();     // successful map.put (gate passed)
        final AtomicLong polled = new AtomicLong();       // poll winners
        final AtomicLong prodRemoves = new AtomicLong();  // producer remove winners
        final AtomicLong pollAttempts = new AtomicLong();
        final AtomicReference<Throwable> failure = new AtomicReference<>();

        // pre-populate the first half of each producer range so pollers have work at once
        for (int p = 0; p < nProducers; p++) {
            long base = (long) p * rangeSize;
            for (int i = 0; i < rangeSize / 2; i++) {
                long k = base + i;
                if (live.putIfAbsent(k, Boolean.TRUE) == null) {
                    map.put(k, f(k));
                    inserted.incrementAndGet();
                }
            }
        }

        long deadline = System.nanoTime() + durationMs() * 1_000_000L;
        Thread[] threads = new Thread[nProducers + nPollers];

        // ---- producers: disjoint ranges; put(f(k)) and remove(k) their own keys ----
        for (int p = 0; p < nProducers; p++) {
            final long base = (long) p * rangeSize;
            final int span = rangeSize;
            final long seed = 1_000 + p;
            threads[p] = new Thread(() -> {
                SplittableRandom rnd = new SplittableRandom(seed);
                try {
                    while (System.nanoTime() < deadline && failure.get() == null) {
                        long k = base + rnd.nextInt(span);
                        if (rnd.nextInt(100) < 65) {                 // INSERT (gated on live)
                            if (live.putIfAbsent(k, Boolean.TRUE) == null) {
                                map.put(k, f(k));                    // pollable only after this
                                inserted.incrementAndGet();
                            }
                        } else {                                     // REMOVE own key
                            Long old = map.remove(k);
                            if (old != null) {
                                if (old != f(k))
                                    fail(failure, "producer remove value mismatch key=" + k
                                        + " got=" + old + " expected=" + f(k));
                                Boolean was = live.remove(k);
                                if (was == null)
                                    fail(failure, "entry present but not live (remove winner) key=" + k);
                                prodRemoves.incrementAndGet();
                            }
                            // old==null: already polled/absent -> poller (or nobody) owns it
                        }
                    }
                } catch (Throwable e) { failure.compareAndSet(null, e); }
            }, label + "-prod-" + p);
        }

        // ---- pollers: pollFirst/pollLast on the whole map + on a random subMap view ----
        for (int q = 0; q < nPollers; q++) {
            final long seed = 5_000 + q;
            threads[nProducers + q] = new Thread(() -> {
                SplittableRandom rnd = new SplittableRandom(seed);
                long attempts = 0;
                try {
                    while (System.nanoTime() < deadline && failure.get() == null) {
                        Map.Entry<Long, Long> e;
                        int mode = rnd.nextInt(4);
                        if (mode == 0) {
                            e = map.pollFirstEntry();
                        } else if (mode == 1) {
                            e = map.pollLastEntry();
                        } else {
                            // random bounded view; returned entry belongs to the backing map
                            long lo = rnd.nextInt(keyspace);
                            long hi = lo + rnd.nextInt(Math.max(1, keyspace / 8));
                            var view = map.subMap(lo, true, hi + 1, false);
                            e = (mode == 2) ? view.pollFirstEntry() : view.pollLastEntry();
                        }
                        attempts++;
                        if (e != null) {
                            long k = e.getKey(), v = e.getValue();
                            // CORE (k,v)-atomicity: poll must never surface a mismatched value
                            if (v != f(k))
                                fail(failure, "POLL VALUE MISMATCH key=" + k + " got=" + v
                                    + " expected=" + f(k) + " (mode=" + mode + ")");
                            // no phantom / no double-poll: this poll must own a live insert
                            Boolean was = live.remove(k);
                            if (was == null)
                                fail(failure, "PHANTOM/DOUBLE POLL key=" + k
                                    + " (value ok but key not live; mode=" + mode + ")");
                            polled.incrementAndGet();
                        }
                    }
                } catch (Throwable e) { failure.compareAndSet(null, e); }
                pollAttempts.addAndGet(attempts);
            }, label + "-poll-" + q);
        }

        runAll(threads);
        if (failure.get() != null) throw new AssertionError(label + " poll hammer failed", failure.get());

        // ---- quiescent conservation + integrity + ordering ----
        long remaining = 0, prev = Long.MIN_VALUE;
        Iterator<Map.Entry<Long, Long>> it = map.entryIterator();
        while (it.hasNext()) {
            Map.Entry<Long, Long> e = it.next();
            long k = e.getKey(), v = e.getValue();
            if (k <= prev) throw new AssertionError(label + " final iter not ascending " + prev + "->" + k);
            if (v != f(k)) throw new AssertionError(label + " final value mismatch key=" + k
                + " got=" + v + " expected=" + f(k));
            if (!Boolean.TRUE.equals(live.get(k)))
                throw new AssertionError(label + " final entry present but not live key=" + k);
            prev = k;
            remaining++;
        }
        long expectedRemaining = inserted.get() - polled.get() - prodRemoves.get();
        if (remaining != expectedRemaining)
            throw new AssertionError(label + " CONSERVATION broken: remaining=" + remaining
                + " expected=" + expectedRemaining + " (inserted=" + inserted.get()
                + " polled=" + polled.get() + " prodRemoves=" + prodRemoves.get() + ")");
        // live map must contain exactly the remaining keys (no leaked presence marks)
        if (live.size() != remaining)
            throw new AssertionError(label + " live-set size " + live.size()
                + " != remaining " + remaining);
        store.verify();

        long nanos = durationMs() * 1_000_000L;
        summary("BTreeNav/" + label + " pollHammer " + nProducers + "prod+" + nPollers + "poll",
            inserted.get() + polled.get() + prodRemoves.get(), nanos, 0);
        phase("  inserts",        inserted.get(),    nanos);
        phase("  polls (won)",    polled.get(),      nanos);
        phase("  poll attempts",  pollAttempts.get(),nanos);
        phase("  prod removes",   prodRemoves.get(), nanos);
        phase("  remaining",      remaining,         nanos);
    }

    // ============================ 2. mixed nav-query hammer ============================

    @Test public void navQueriesDirect() {
        StoreDirect s = new StoreDirect(false);
        try { navHammer(s, "StoreDirect"); } finally { s.close(); }
    }

    @Test public void navQueriesByteArray() {
        StoreByteArray s = new StoreByteArray();
        try { navHammer(s, "StoreByteArray"); } finally { s.close(); }
    }

    /**
     * Interleaves floor/ceiling/lower/higher/first/last queries with writes. Every non-null
     * returned entry must satisfy {@code value == f(key)} AND the query's key relation. This
     * needs no conservation bookkeeping: all writes use {@code value == f(key)}, so any map
     * entry (whenever observed) is value-correct; only the relation and integrity are checked.
     */
    private void navHammer(Store store, String label) {
        final int maxNodeSize = 12;
        BTreeMap<Long, Long> map =
            BTreeMap.create(store, LongFormat.INSTANCE, LongFormat.INSTANCE, maxNodeSize);

        final int nThreads   = Math.max(4, 2 * CORES);
        final int nWriters   = Math.max(1, nThreads / 2);
        final int nReaders   = Math.max(1, nThreads - nWriters);
        final int keyspace   = Math.max(2_000, scaled(100_000L));
        final int wSpan      = Math.max(16, keyspace / nWriters);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final AtomicLong writes = new AtomicLong(), queries = new AtomicLong();

        for (int k = 0; k < keyspace; k += 2) map.put((long) k, f(k)); // seed evens

        long deadline = System.nanoTime() + durationMs() * 1_000_000L;
        Thread[] threads = new Thread[nWriters + nReaders];

        for (int w = 0; w < nWriters; w++) {
            final long base = (long) w * wSpan;
            final int span = wSpan;
            final long seed = 20_000 + w;
            threads[w] = new Thread(() -> {
                SplittableRandom rnd = new SplittableRandom(seed);
                long ops = 0;
                try {
                    while (System.nanoTime() < deadline && failure.get() == null) {
                        long k = base + rnd.nextInt(span);
                        if (rnd.nextInt(100) < 60) map.put(k, f(k));
                        else map.remove(k);
                        ops++;
                    }
                } catch (Throwable e) { failure.compareAndSet(null, e); }
                writes.addAndGet(ops);
            }, label + "-nwrite-" + w);
        }

        for (int r = 0; r < nReaders; r++) {
            final long seed = 40_000 + r;
            threads[nWriters + r] = new Thread(() -> {
                SplittableRandom rnd = new SplittableRandom(seed);
                long qc = 0;
                try {
                    while (System.nanoTime() < deadline && failure.get() == null) {
                        long q = rnd.nextInt(keyspace);
                        int mode = rnd.nextInt(6);
                        Map.Entry<Long, Long> e;
                        switch (mode) {
                            case 0: e = map.floorEntry(q);   checkRel(failure, "floor",   e, q, k -> k <= q); break;
                            case 1: e = map.ceilingEntry(q); checkRel(failure, "ceiling", e, q, k -> k >= q); break;
                            case 2: e = map.lowerEntry(q);   checkRel(failure, "lower",   e, q, k -> k <  q); break;
                            case 3: e = map.higherEntry(q);  checkRel(failure, "higher",  e, q, k -> k >  q); break;
                            case 4: e = map.firstEntry();    checkRel(failure, "first",   e, q, k -> true);   break;
                            default:e = map.lastEntry();     checkRel(failure, "last",    e, q, k -> true);   break;
                        }
                        qc++;
                    }
                } catch (Throwable e) { failure.compareAndSet(null, e); }
                queries.addAndGet(qc);
            }, label + "-nread-" + r);
        }

        runAll(threads);
        if (failure.get() != null) throw new AssertionError(label + " nav hammer failed", failure.get());
        store.verify();
        long nanos = durationMs() * 1_000_000L;
        summary("BTreeNav/" + label + " navQueries " + nWriters + "wr+" + nReaders + "rd",
            writes.get() + queries.get(), nanos, 0);
        phase("  writes",  writes.get(),  nanos);
        phase("  queries", queries.get(), nanos);
    }

    private interface KeyRel { boolean ok(long k); }

    private static void checkRel(AtomicReference<Throwable> failure, String name,
                                 Map.Entry<Long, Long> e, long q, KeyRel rel) {
        if (e == null) return; // map may be empty / no such key relative to q -> valid
        long k = e.getKey(), v = e.getValue();
        if (v != f(k))
            fail(failure, name + "Entry(" + q + ") VALUE MISMATCH key=" + k + " got=" + v + " expected=" + f(k));
        if (!rel.ok(k))
            fail(failure, name + "Entry(" + q + ") RELATION broken: key=" + k);
    }

    // ---- helpers ----

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
