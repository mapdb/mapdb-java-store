package org.mapdb.htree;

import org.junit.Test;
import org.mapdb.ser.Serializers;
import org.mapdb.store.Store;
import org.mapdb.store.StoreOnHeap;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Overflow-map handoff (eviction sink) and miss-load (promote-back), plus the lock
 * discipline that makes both deadlock-safe: handoff/miss-load always run with NO cache
 * segment lock held (snapshot-under-lock, flush-after-unlock).
 */
public class HTreeCacheOverflowTest {

    private HTreeCache<Long, Long> singleSegment(Store store, long ttl, long maxSize,
                                                 HTreeCacheTCK.FakeClock clock) {
        return HTreeCache.create(store, Serializers.LONG, Serializers.LONG,
                0, 4, 8, ttl, false, maxSize, 0, 0, null, clock);
    }

    /** max-size eviction hands the OLDEST (FIFO) evicted entries to the overflow map with
     *  their evicted-time values; survivors are the newest maxSize entries. */
    @Test
    public void maxSizeEvictionHandsOffOldest() {
        Store store = new StoreOnHeap();
        HTreeCache<Long, Long> m = singleSegment(store, 0, 10, null);
        Map<Long, Long> overflow = new HashMap<>();
        m.overflow(overflow);

        for (long k = 0; k < 30; k++) m.put(k, k * 2);
        m.expireEvict();

        assertEquals("10 newest survive", 10L, m.sizeLong());
        assertEquals("20 oldest overflowed", 20, overflow.size());
        for (long k = 0; k < 20; k++) {
            assertEquals("overflow value for " + k, Long.valueOf(k * 2), overflow.get(k));
            assertFalse("evicted key still resident: " + k, m.containsKey(k));
        }
        for (long k = 20; k < 30; k++) assertTrue(m.containsKey(k));
        store.verify();
        store.close();
    }

    /** A get() MISS consults the overflow map and PROMOTES the value back into the cache. */
    @Test
    public void getMissLoadsAndPromotesFromOverflow() {
        Store store = new StoreOnHeap();
        HTreeCache<Long, Long> m = singleSegment(store, 0, 10, null);
        Map<Long, Long> overflow = new HashMap<>();
        m.overflow(overflow);

        for (long k = 0; k < 30; k++) m.put(k, k * 2);
        // key 5 was evicted to overflow; a get must reload + promote it.
        assertFalse(m.containsKey(5L));
        assertEquals(Long.valueOf(10L), m.get(5L));   // 5*2, loaded from overflow
        assertTrue("promoted into the cache", m.containsKey(5L));

        // a genuine miss (never inserted, not in overflow) stays null.
        assertNull(m.get(999L));
        store.verify();
        store.close();
    }

    /** TTL eviction does NOT hand off: a TTL-lapsed entry is logically DEAD, so a sweep
     *  plain-deletes it (it must never reach the overflow tier) and a later get MUST NOT
     *  resurrect it. Regression for the strict-read {@code TTL-expired == absent} invariant. */
    @Test
    public void ttlEvictionDoesNotHandOffOrResurrect() {
        Store store = new StoreOnHeap();
        HTreeCacheTCK.FakeClock clock = new HTreeCacheTCK.FakeClock();
        HTreeCache<Long, Long> m = singleSegment(store, 1_000, 0, clock);
        Map<Long, Long> overflow = new HashMap<>();
        m.overflow(overflow);

        m.put(1L, 10L);
        m.put(2L, 20L);
        clock.advance(1_001); // both lapse
        m.expireEvict();      // sweep reclaims; TTL victims are plain-deleted, NOT overflowed

        assertTrue("TTL-expired entries must not overflow", overflow.isEmpty());
        assertEquals(0L, m.sizeLong());
        // and a get() MUST NOT bring an expired value back to life via the overflow tier
        assertNull("TTL-expired value resurrected via overflow", m.get(1L));
        assertNull("TTL-expired value resurrected via overflow", m.get(2L));
        assertTrue("miss-load must not repopulate overflow either", overflow.isEmpty());
        store.verify();
        store.close();
    }

    /** The reason-split, end to end: with BOTH a TTL and a max-size bound + overflow, a
     *  CAPACITY victim (evicted to make room, still within its TTL) reloads from overflow on a
     *  later get; a TTL-EXPIRED victim is plain-deleted and can never be reloaded. */
    @Test
    public void capacityVictimReloadsButTtlVictimDoesNot() {
        Store store = new StoreOnHeap();
        HTreeCacheTCK.FakeClock clock = new HTreeCacheTCK.FakeClock();
        HTreeCache<Long, Long> m = HTreeCache.create(store, Serializers.LONG, Serializers.LONG,
                0, 4, 8, 1_000, false, 5, 0, 0, null, clock); // single seg, ttl 1000, maxSize 5
        Map<Long, Long> overflow = new HashMap<>();
        m.overflow(overflow);

        // 20 puts within TTL: max-size 5 keeps the 5 newest resident; the 15 oldest are
        // CAPACITY victims and (being still live) go to overflow.
        for (long k = 0; k < 20; k++) m.put(k, k * 2);
        m.expireEvict();
        assertEquals(5L, m.sizeLong());
        assertTrue("capacity victims overflowed", overflow.size() >= 15);

        // a capacity-evicted key reloads + promotes from overflow (still logically live)
        assertFalse(m.containsKey(0L));
        assertEquals(Long.valueOf(0L), m.get(0L)); // 0*2, loaded from overflow
        assertTrue("capacity victim promoted", m.containsKey(0L));

        // now let everything lapse by TTL and sweep. The surviving RESIDENTS (keys 15..19)
        // were never capacity victims, so they were never handed to overflow. Their TTL lapse
        // plain-deletes them and adds NOTHING to overflow, and a later get cannot resurrect a
        // value that only ever expired by TTL. (A key that legitimately sits in the overflow
        // backing tier as a CAPACITY victim — e.g. key 0 — is intentionally reloadable; that is
        // the documented spillover-backing-store model, exercised above, not a resurrection.)
        int overflowBefore = overflow.size();
        clock.advance(2_000);
        m.expireEvict();
        assertEquals(0L, m.sizeLong());
        assertEquals("TTL-lapsed residents must not reach overflow", overflowBefore, overflow.size());
        assertNull("resident that only expired by TTL must not resurrect", m.get(17L));
        store.verify();
        store.close();
    }

    /** Explicit remove()/clear() are NOT evictions: they must not hand off to overflow. */
    @Test
    public void explicitRemoveAndClearDoNotHandOff() {
        Store store = new StoreOnHeap();
        HTreeCache<Long, Long> m = singleSegment(store, 0, 1_000, null);
        Map<Long, Long> overflow = new HashMap<>();
        m.overflow(overflow);

        m.put(1L, 10L);
        m.put(2L, 20L);
        assertEquals(Long.valueOf(10L), m.remove(1L));
        m.clear();
        assertTrue("explicit deletes must not overflow", overflow.isEmpty());
        store.verify();
        store.close();
    }

    /** A cache may not overflow into itself (would recursively re-insert its own evictions). */
    @Test(expected = IllegalArgumentException.class)
    public void selfOverflowRejected() {
        Store store = new StoreOnHeap();
        HTreeCache<Long, Long> m = singleSegment(store, 0, 10, null);
        m.overflow(m);
    }

    /**
     * Deadlock safety: two caches wired as EACH OTHER's overflow (a 2-cycle) with a tiny
     * max-size so nearly every put evicts, hammered concurrently from opposite directions.
     * If handoff ran under the segment lock this is the classic lock-order deadlock
     * (T1: A-lock waits B-lock; T2: B-lock waits A-lock). The flush-after-unlock discipline
     * plus the per-cache reentrancy guard make it terminate. Must complete within the timeout.
     */
    @Test(timeout = 60_000)
    public void overflowHandoffIsDeadlockSafeUnderContention() throws InterruptedException {
        Store storeA = new StoreOnHeap();
        Store storeB = new StoreOnHeap();
        HTreeCache<Long, Long> a = HTreeCache.create(storeA, Serializers.LONG, Serializers.LONG,
                0, false, 64); // default 16 segments, maxSize 64
        HTreeCache<Long, Long> b = HTreeCache.create(storeB, Serializers.LONG, Serializers.LONG,
                0, false, 64);
        a.overflow(b);
        b.overflow(a); // 2-cycle; reentrancy guard bounds the ping-pong

        final int OPS = 40_000;
        AtomicReference<Throwable> err = new AtomicReference<>();
        Runnable hammerA = () -> {
            try { for (long k = 0; k < OPS; k++) { a.put(k, k); if ((k & 7) == 0) a.get(k); } }
            catch (Throwable t) { err.compareAndSet(null, t); }
        };
        Runnable hammerB = () -> {
            try { for (long k = 0; k < OPS; k++) { b.put(k, k); if ((k & 7) == 0) b.get(k); } }
            catch (Throwable t) { err.compareAndSet(null, t); }
        };
        Thread t1 = new Thread(hammerA, "hammerA");
        Thread t2 = new Thread(hammerB, "hammerB");
        t1.start(); t2.start();
        t1.join(); t2.join();

        if (err.get() != null) throw new AssertionError("overflow handoff threw", err.get());
        // both caches stayed within their approximate bound (maxSize + one-per-segment slack)
        assertTrue("A over bound: " + a.sizeLong(), a.sizeLong() <= 64 + 16);
        assertTrue("B over bound: " + b.sizeLong(), b.sizeLong() <= 64 + 16);
        storeA.verify(); storeB.verify();
        storeA.close(); storeB.close();
    }

    /**
     * Deadlock safety with an overflow map that takes its OWN lock and calls BACK into the
     * source cache from inside put()/get() (a hostile re-entrant sink on the same thread).
     * Because handoff/miss-load hold no cache lock, the callback can freely touch the cache.
     */
    @Test(timeout = 30_000)
    public void overflowMapMayReenterSourceCache() {
        Store store = new StoreOnHeap();
        HTreeCache<Long, Long> m = singleSegment(store, 0, 5, null);
        // overflow sink that, on every put, also probes the source cache (re-entrancy).
        Map<Long, Long> reentrant = new ConcurrentHashMap<>() {
            @Override public Long put(Long k, Long v) {
                m.containsKey(k);      // read-lock a segment of the source cache
                m.get(k == 0 ? 1L : k - 1); // may itself miss-load/evict
                return super.put(k, v);
            }
        };
        m.overflow(reentrant);
        for (long k = 0; k < 200; k++) m.put(k, k * 3);
        assertTrue(reentrant.size() > 0);
        assertTrue(m.sizeLong() <= 6);
        store.verify();
        store.close();
    }
}
