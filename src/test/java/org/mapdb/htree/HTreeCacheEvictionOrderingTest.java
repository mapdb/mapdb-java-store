package org.mapdb.htree;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.mapdb.SynchronousMapModificationListener;
import org.mapdb.ser.Serializers;
import org.mapdb.store.Store;
import org.mapdb.store.StoreOnHeap;

/**
 * Eviction victims must deliver their SYNC modification event UNDER the segment lock, at sweep
 * time — not in the post-unlock flush. Otherwise the original secondary-index desync race
 * survives for expiry caches: T1 evicts (k, old) and unlocks; T2 puts (k, fresh) and sync-updates
 * the index under the lock; T1's LATE flush then fires the removal and deletes the fresh entry.
 * This test blocks a sync listener inside the eviction event and checks a same-key writer is
 * held out until it completes. Two layers: the no-progress-window check is STRONG-PROBABILISTIC
 * (a starved writer also shows no progress), but the final-state check is DETERMINISTIC whenever
 * the writer did overtake — under post-unlock delivery it completes inside the window and the
 * released eviction event then always destroys the fresh index entry.
 */
public class HTreeCacheEvictionOrderingTest {

    @Test public void evictionSyncEventFiresUnderSegmentLock() throws Exception {
        Store store = new StoreOnHeap(true);
        // single segment (concShift=0), maxSize=1: the sweep at the head of each op evicts the
        // oldest entry once the segment holds more than one
        HTreeCache<Long, Long> m = HTreeCache.create(
                store, Serializers.LONG, Serializers.LONG, 0, 4, 8,
                0 /* no TTL */, false, 1 /* maxSize */, null);

        Map<Long, Long> secondary = new ConcurrentHashMap<>();
        CountDownLatch inEviction = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        m.modificationListenerAdd((SynchronousMapModificationListener<Long, Long>)
                (key, oldValue, newValue, triggered) -> {
            if (newValue == null) {
                if (key == 1L && triggered) {
                    inEviction.countDown();
                    try {
                        if (!release.await(30, TimeUnit.SECONDS))
                            throw new IllegalStateException("release latch timed out");
                    } catch (InterruptedException e) { throw new IllegalStateException(e); }
                }
                secondary.remove(key);
            } else {
                secondary.put(key, newValue);
            }
        });

        m.put(1L, 10L); // counter 1: no sweep victim yet
        m.put(2L, 20L); // counter 2: next op's sweep will evict the oldest (key 1)
        assertEquals(Long.valueOf(10L), secondary.get(1L));

        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread evictor = new Thread(() -> { // put(3) sweeps first: evicts key 1, listener blocks
            try { m.put(3L, 30L); } catch (Throwable e) { failure.compareAndSet(null, e); }
        });
        evictor.setDaemon(true);
        evictor.start();
        assertTrue("eviction listener never entered", inEviction.await(30, TimeUnit.SECONDS));

        // progress-observable side effect: writerDone bumps only after the put RETURNS
        java.util.concurrent.atomic.AtomicInteger writerDone =
                new java.util.concurrent.atomic.AtomicInteger();
        CountDownLatch writerStarting = new CountDownLatch(1);
        Thread writer = new Thread(() -> { // re-put the evicted key with a FRESH value
            try {
                writerStarting.countDown(); // handshake: about to enter put
                m.put(1L, 100L);
                writerDone.incrementAndGet();
            } catch (Throwable e) { failure.compareAndSet(null, e); }
        });
        writer.setDaemon(true);
        writer.start();
        assertTrue("writer never started", writerStarting.await(30, TimeUnit.SECONDS));
        Thread.sleep(300); // no-progress window (strong-probabilistic; see class javadoc)
        // the eviction's sync event still holds the segment lock, so the writer must be held out;
        // post-unlock delivery (the bug) would let it complete here and be clobbered on release
        assertEquals("same-key writer overtook the in-flight eviction event", 0, writerDone.get());

        release.countDown();
        joinOrFail(evictor);
        joinOrFail(writer);
        if (failure.get() != null) throw new AssertionError(failure.get());
        assertEquals(1, writerDone.get());

        // ordering held: remove(1) [eviction] happened-before put(1, fresh);
        // DETERMINISTIC on overtake: late delivery would have destroyed the fresh entry
        assertEquals(Long.valueOf(100L), m.get(1L));
        assertEquals("late eviction event destroyed the fresh index entry",
                Long.valueOf(100L), secondary.get(1L));
        // the writer's own sweep evicted key 2 (oldest) on the way in
        assertNull(m.get(2L));
        assertNull(secondary.get(2L));
        assertEquals(Long.valueOf(30L), secondary.get(3L));
        store.close();
    }

    private static void joinOrFail(Thread thread) throws InterruptedException {
        thread.join(30_000);
        if (thread.isAlive())
            throw new AssertionError("thread did not finish within 30s (deadlock?): " + thread);
    }
}
