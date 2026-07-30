package org.mapdb.htree;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.mapdb.ser.Serializers;
import org.mapdb.store.Store;
import org.mapdb.store.StoreOnHeap;

/**
 * missLoad promotes a valueLoader/overflow value with putIfAbsent semantics: a concurrent
 * put(fresh) between the read-miss and the promote must NOT be clobbered by the stale loaded
 * value. Uses a concurrency-capable store (the shared TCK also runs on the append-only store,
 * whose single-writer contract forbids the concurrent putters this exercises).
 */
public class HTreeCacheMissLoadRaceTest {

    /** DETERMINISTIC interleaving: establish a miss with the loader BLOCKED (missLoad runs the
     *  loader with no cache lock held), complete a put(key, fresh) inside that window, then
     *  release the loader. The promote must be putIfAbsent, so the fresh value survives — the
     *  old unconditional promote deterministically clobbered it with the stale loaded value. */
    @Test public void promoteNeverClobbersPutDeterministic() throws Exception {
        Store store = new StoreOnHeap(true);
        HTreeCache<Long, Long> m = HTreeCache.create(
                store, Serializers.LONG, Serializers.LONG, 0, false, Long.MAX_VALUE / 2);
        CountDownLatch inLoader = new CountDownLatch(1);
        CountDownLatch releaseLoader = new CountDownLatch(1);
        m.valueLoader(key -> {
            inLoader.countDown();
            try {
                if (!releaseLoader.await(30, TimeUnit.SECONDS))
                    throw new IllegalStateException("release latch timed out");
            } catch (InterruptedException e) { throw new IllegalStateException(e); }
            return -1L; // STALE
        });
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread getter = new Thread(() -> {
            try { m.get(5L); } catch (Throwable e) { failure.compareAndSet(null, e); }
        });
        getter.setDaemon(true);
        getter.start();
        assertTrue("loader never entered", inLoader.await(30, TimeUnit.SECONDS));
        m.put(5L, 42L); // fresh value lands between the read-miss and the promote
        releaseLoader.countDown();
        joinOrFail(getter);
        if (failure.get() != null) throw new AssertionError(failure.get());
        m.valueLoader(null); // the assertion read below must not re-enter the loader
        assertEquals("stale promote clobbered the fresh put", Long.valueOf(42L), m.get(5L));
        store.close();
    }

    /** Racy stress companion to the deterministic test above: guards crash-freedom and the
     *  putIfAbsent post-condition under a real storm. */
    @Test public void missLoadPromoteNeverClobbersConcurrentPut() throws Exception {
        Store store = new StoreOnHeap(true);
        // NO_LIMIT size so nothing is evicted; the loader returns a STALE sentinel (negative),
        // disjoint from the fresh (>= 0) values putters write.
        HTreeCache<Long, Long> m = HTreeCache.create(
                store, Serializers.LONG, Serializers.LONG, 0, false, Long.MAX_VALUE / 2);
        m.valueLoader(key -> -(key + 1));
        int keyCount = 512;
        int putters = 4, getters = 4;
        int iterations = 3000;
        Thread[] threads = new Thread[putters + getters];
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CyclicBarrier barrier = new CyclicBarrier(putters + getters);
        for (int t = 0; t < putters + getters; t++) {
            final boolean put = t < putters;
            final int tid = t;
            threads[t] = new Thread(() -> {
                try {
                    barrier.await(30, TimeUnit.SECONDS);
                    for (int i = 0; i < iterations; i++) {
                        long key = (i * 31L + tid) % keyCount;
                        if (put) m.put(key, (long) (i % 1000)); // fresh, always >= 0
                        else m.get(key); // miss -> load STALE -> promote (putIfAbsent)
                    }
                } catch (Throwable e) { failure.compareAndSet(null, e); }
            });
            threads[t].setDaemon(true); // a straggler must not keep the forked JVM alive
        }
        for (Thread thread : threads) thread.start();
        for (Thread thread : threads) joinOrFail(thread);
        if (failure.get() != null) throw new AssertionError(failure.get());
        // Every key was put (overwriting) repeatedly and never removed, so the storm leaves each
        // with a fresh (>= 0) value. A surviving STALE (negative) means a promote clobbered a put.
        for (long key = 0; key < keyCount; key++) {
            Long value = m.get(key);
            assertTrue("key " + key + " holds STALE loaded value " + value
                    + " (promote clobbered a fresh put)", value != null && value >= 0);
        }
        store.close();
    }

    static void joinOrFail(Thread thread) throws InterruptedException {
        thread.join(30_000);
        if (thread.isAlive())
            throw new AssertionError("thread did not finish within 30s (deadlock?): " + thread);
    }
}
