package org.mapdb.htree;

import org.junit.Test;
import org.mapdb.hash.Hasher;
import org.mapdb.htree.HTreeCache.EvictionReason;
import org.mapdb.ser.Serializers;
import org.mapdb.store.Store;
import org.mapdb.store.StoreDirect;
import org.mapdb.store.StoreOnHeap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The three HTreeCache minor follow-ups:
 *  - Task A: eviction-listener callbacks (reason: TTL / MAX_SIZE / STORE_SIZE), fired
 *    OUTSIDE the segment lock, ordered listener-then-overflow, not fired for explicit ops;
 *  - Task B: {@code maxEvictPerOp} foreground-sweep throttle (bounds TTL+capacity per op,
 *    {@code expireEvict} stays unbounded), persisted in the header;
 *  - Task C: in-memory (per-handle) size counters reconstructed from the queue at open.
 */
public class HTreeCacheMinorTest {

    // ---- helpers ----

    private static HTreeCache<Long, Long> singleSeg(Store store, long ttl, long maxSize,
                                                    HTreeCacheTCK.FakeClock clock) {
        return HTreeCache.create(store, Serializers.LONG, Serializers.LONG,
                0, 4, 8, ttl, false, maxSize, 0, 0, null, clock);
    }

    private static HTreeCache<Long, Long> singleSegThrottled(Store store, long ttl, long maxSize,
                                                             long maxEvictPerOp,
                                                             HTreeCacheTCK.FakeClock clock) {
        return HTreeCache.create(store, Serializers.LONG, Serializers.LONG,
                0, 4, 8, ttl, false, maxSize, 0, maxEvictPerOp, 0, (Hasher<Long>) null, clock);
    }

    /** Records every eviction callback as {key,value,reason}. */
    private static final class Recorder implements HTreeCache.EvictionListener<Long, Long> {
        final List<Object[]> events = new ArrayList<>();
        @Override public void evicted(Long key, Long value, EvictionReason reason) {
            events.add(new Object[]{key, value, reason});
        }
    }

    // =====================================================================
    // Task A — eviction listener
    // =====================================================================

    @Test
    public void listenerFiresWithTtlReason() {
        Store store = new StoreOnHeap();
        HTreeCacheTCK.FakeClock clock = new HTreeCacheTCK.FakeClock();
        HTreeCache<Long, Long> m = singleSeg(store, 1_000, 0, clock);
        Recorder rec = new Recorder();
        m.evictionListener(rec);

        for (long k = 0; k < 5; k++) m.put(k, k * 10);
        clock.advance(1_001); // all lapse
        m.expireEvict();

        assertEquals(5, rec.events.size());
        Map<Long, Long> got = new HashMap<>();
        for (Object[] e : rec.events) {
            assertEquals("reason", EvictionReason.TTL, e[2]);
            got.put((Long) e[0], (Long) e[1]);
        }
        for (long k = 0; k < 5; k++) assertEquals(Long.valueOf(k * 10), got.get(k));
        store.verify();
        store.close();
    }

    @Test
    public void listenerFiresWithMaxSizeReason() {
        Store store = new StoreOnHeap();
        HTreeCache<Long, Long> m = singleSeg(store, 0, 10, null);
        Recorder rec = new Recorder();
        m.evictionListener(rec);

        for (long k = 0; k < 30; k++) m.put(k, k * 2);
        m.expireEvict();

        assertFalse("some entries evicted", rec.events.isEmpty());
        for (Object[] e : rec.events) assertEquals(EvictionReason.MAX_SIZE, e[2]);
        // the 20 oldest are the max-size victims
        Map<Long, Long> got = new HashMap<>();
        for (Object[] e : rec.events) got.put((Long) e[0], (Long) e[1]);
        for (long k = 0; k < 20; k++) assertEquals("victim " + k, Long.valueOf(k * 2), got.get(k));
        store.verify();
        store.close();
    }

    @Test
    public void listenerFiresWithStoreSizeReason() {
        StoreDirect store = new StoreDirect();
        long budget = 2L << 20; // 2 MB
        HTreeCache<Long, Long> m = HTreeCache.create(store, Serializers.LONG, Serializers.LONG,
                0, 4, 8, 0, false, 0, budget, 0, (Hasher<Long>) null, null);
        Recorder rec = new Recorder();
        m.evictionListener(rec);

        for (long k = 0; k < 200_000; k++) m.put(k, k);
        m.expireEvict();

        assertFalse("store-size eviction must fire the listener", rec.events.isEmpty());
        for (Object[] e : rec.events) assertEquals(EvictionReason.STORE_SIZE, e[2]);
        store.verify();
        store.close();
    }

    @Test
    public void listenerNotFiredForExplicitRemoveOrClear() {
        Store store = new StoreOnHeap();
        HTreeCache<Long, Long> m = singleSeg(store, 0, 1_000_000, null); // no eviction pressure
        m.evictionListener((k, v, r) -> fail("listener fired for explicit op: " + k + " " + r));

        for (long k = 0; k < 50; k++) m.put(k, k);
        m.put(0L, 999L);         // overwrite is not an eviction
        m.remove(1L);            // explicit remove
        m.remove(2L, 2L);        // CAS remove
        m.clear();               // clear
        store.verify();
        store.close();
    }

    /** The listener runs with NO segment lock held: it may re-enter the cache for a read
     *  without deadlocking, and the whole run must terminate. */
    @Test(timeout = 30_000)
    public void listenerRunsOutsideLockAndMayReenter() {
        Store store = new StoreOnHeap();
        HTreeCache<Long, Long> m = singleSeg(store, 0, 5, null);
        AtomicInteger fired = new AtomicInteger();
        m.evictionListener((k, v, r) -> {
            // re-enter the source cache with reads (would deadlock if a lock were held)
            m.containsKey(k);
            m.get(k == 0 ? 1L : k - 1);
            fired.incrementAndGet();
        });
        for (long k = 0; k < 200; k++) m.put(k, k * 3);
        assertTrue(fired.get() > 0);
        assertTrue(m.sizeLong() <= 6);
        store.verify();
        store.close();
    }

    /** Both a listener and an overflow map wired: a capacity victim goes to BOTH, and the
     *  OVERFLOW handoff runs first (so a throwing listener can never lose a live capacity
     *  victim), asserted by the overflow already holding the key inside the listener. */
    @Test
    public void listenerAndOverflowBothFireForCapacityVictim() {
        Store store = new StoreOnHeap();
        HTreeCache<Long, Long> m = singleSeg(store, 0, 5, null);
        Map<Long, Long> overflow = new HashMap<>();
        m.overflow(overflow);
        Recorder rec = new Recorder();
        m.evictionListener((k, v, r) -> {
            assertTrue("overflow handoff must run BEFORE the listener", overflow.containsKey(k));
            rec.evicted(k, v, r);
        });

        for (long k = 0; k < 20; k++) m.put(k, k * 2);
        m.expireEvict();

        assertFalse(rec.events.isEmpty());
        for (Object[] e : rec.events) assertEquals(EvictionReason.MAX_SIZE, e[2]);
        // every listener victim also reached the overflow
        for (Object[] e : rec.events) {
            assertEquals("overflow value for " + e[0], e[1], overflow.get((Long) e[0]));
        }
        store.verify();
        store.close();
    }

    /** A TTL victim fires the listener (reason TTL) but is NOT handed to the overflow. */
    @Test
    public void listenerFiresForTtlButOverflowDoesNot() {
        Store store = new StoreOnHeap();
        HTreeCacheTCK.FakeClock clock = new HTreeCacheTCK.FakeClock();
        HTreeCache<Long, Long> m = singleSeg(store, 1_000, 0, clock);
        Map<Long, Long> overflow = new HashMap<>();
        m.overflow(overflow);
        Recorder rec = new Recorder();
        m.evictionListener(rec);

        for (long k = 0; k < 3; k++) m.put(k, k * 10);
        clock.advance(1_001);
        m.expireEvict();

        assertEquals(3, rec.events.size());
        for (Object[] e : rec.events) assertEquals(EvictionReason.TTL, e[2]);
        assertTrue("TTL victims must NOT overflow", overflow.isEmpty());
        store.verify();
        store.close();
    }

    // =====================================================================
    // Task B — maxEvictPerOp foreground-sweep throttle
    // =====================================================================

    /** A burst of >>N simultaneously-expired entries is reclaimed at most N per FOREGROUND op;
     *  the rest stay physically present (strict reads treat them as absent, the counter counts
     *  them) until later ops; expireEvict drains everything and the counter stays exact. */
    @Test
    public void maxEvictPerOpThrottlesForegroundSweep() {
        Store store = new StoreOnHeap();
        HTreeCacheTCK.FakeClock clock = new HTreeCacheTCK.FakeClock();
        HTreeCache<Long, Long> m = singleSegThrottled(store, 1_000, 0, 5, clock);

        for (long k = 0; k < 50; k++) m.put(k, k);
        clock.advance(1_001); // all 50 lapse at once
        assertEquals("unswept expired entries still counted", 50L, m.sizeLong());
        // a strict read still treats them as absent even before any sweep
        assertNull(m.get(0L));

        // each foreground op (a remove of an absent key => pure sweep) evicts at most 5.
        assertNull(m.remove(999L));
        assertEquals(45L, m.sizeLong());
        assertNull(m.remove(999L));
        assertEquals(40L, m.sizeLong());
        // strict reads on the still-unswept expired entries remain absent
        for (long k = 0; k < 50; k++) assertNull("expired must read absent: " + k, m.get(k));

        m.expireEvict(); // unbounded: drains the rest regardless of the throttle
        assertEquals(0L, m.sizeLong());
        assertTrue(m.isEmpty());
        store.verify();
        store.close();
    }

    /** maxEvictPerOp=0 (default) keeps the current UNBOUNDED foreground sweep: one op reclaims all. */
    @Test
    public void maxEvictPerOpZeroIsUnbounded() {
        Store store = new StoreOnHeap();
        HTreeCacheTCK.FakeClock clock = new HTreeCacheTCK.FakeClock();
        HTreeCache<Long, Long> m = singleSegThrottled(store, 1_000, 0, 0, clock);
        for (long k = 0; k < 50; k++) m.put(k, k);
        clock.advance(1_001);
        m.put(100L, 100L); // single foreground op sweeps the whole cohort
        assertEquals(1L, m.sizeLong());
        store.verify();
        store.close();
    }

    /** The throttle is persisted in the header: a reopened handle still bounds foreground sweeps. */
    @Test
    public void maxEvictPerOpPersistsAcrossReopen() {
        Store store = new StoreOnHeap();
        HTreeCacheTCK.FakeClock clock = new HTreeCacheTCK.FakeClock();
        long headerRecid;
        {
            HTreeCache<Long, Long> m = singleSegThrottled(store, 1_000, 0, 5, clock);
            headerRecid = m.headerRecid();
            for (long k = 0; k < 50; k++) m.put(k, k);
        }
        HTreeCache<Long, Long> m2 = HTreeCache.open(store, headerRecid,
                Serializers.LONG, Serializers.LONG, null, clock);
        assertEquals(50L, m2.sizeLong());
        clock.advance(1_001);
        assertNull(m2.remove(999L)); // throttled foreground sweep
        assertEquals("reopened handle must honor the persisted throttle", 45L, m2.sizeLong());
        m2.expireEvict();
        assertEquals(0L, m2.sizeLong());
        store.verify();
        store.close();
    }

    /** Strict-TTL for MUTATING ops under the throttle: when the throttled foreground sweep
     *  SKIPS a key's own expired node, put/putIfAbsent/remove/replace must still treat that key
     *  as ABSENT — never returning or reviving-as-hit a stale expired value. (Regression: the
     *  pre-throttle code assumed the sweep always drained the whole expired prefix.) */
    @Test
    public void throttledExpiredTargetTreatedAsAbsentByMutatingOps() {
        // maxEvictPerOp=1 so a sweep reclaims only the OLDEST expired node, skipping the target.
        // put on a stale expired target => insert fresh (return null), leaving the older cohort.
        {
            Store store = new StoreOnHeap();
            HTreeCacheTCK.FakeClock clock = new HTreeCacheTCK.FakeClock();
            HTreeCache<Long, Long> m = singleSegThrottled(store, 1_000, 0, 1, clock);
            m.put(0L, 0L);
            m.put(1L, 1L);
            clock.advance(1_001); // both expired; key 1 is NOT the oldest (key 0 is)
            assertNull("put on an expired target returns null (absent), not the stale value",
                    m.put(1L, 99L));
            assertEquals(Long.valueOf(99L), m.get(1L)); // revived fresh
            store.close();
        }
        // putIfAbsent on a stale expired target => inserts (null), not the stale value
        {
            Store store = new StoreOnHeap();
            HTreeCacheTCK.FakeClock clock = new HTreeCacheTCK.FakeClock();
            HTreeCache<Long, Long> m = singleSegThrottled(store, 1_000, 0, 1, clock);
            m.put(0L, 0L);
            m.put(1L, 1L);
            clock.advance(1_001);
            assertNull("putIfAbsent on an expired target inserts", m.putIfAbsent(1L, 99L));
            assertEquals(Long.valueOf(99L), m.get(1L));
            store.close();
        }
        // remove on a stale expired target => null (absent), and no TTL callback lost: it fires
        // later when the entry is actually reclaimed.
        {
            Store store = new StoreOnHeap();
            HTreeCacheTCK.FakeClock clock = new HTreeCacheTCK.FakeClock();
            HTreeCache<Long, Long> m = singleSegThrottled(store, 1_000, 0, 1, clock);
            Recorder rec = new Recorder();
            m.evictionListener(rec);
            m.put(0L, 0L);
            m.put(1L, 1L);
            clock.advance(1_001);
            assertNull("remove of an expired target returns null", m.remove(1L));
            assertFalse("CAS remove of an expired target is false", m.remove(1L, 1L));
            m.expireEvict(); // now everything is reclaimed and the TTL listener fires for both
            assertEquals(2, rec.events.size());
            for (Object[] e : rec.events) assertEquals(EvictionReason.TTL, e[2]);
            assertEquals(0L, m.sizeLong());
            store.close();
        }
        // replace on a stale expired target => null / false (replace needs a LIVE entry)
        {
            Store store = new StoreOnHeap();
            HTreeCacheTCK.FakeClock clock = new HTreeCacheTCK.FakeClock();
            HTreeCache<Long, Long> m = singleSegThrottled(store, 1_000, 0, 1, clock);
            m.put(0L, 0L);
            m.put(1L, 1L);
            clock.advance(1_001);
            assertNull("replace of an expired target returns null", m.replace(1L, 99L));
            assertFalse("CAS replace of an expired target is false", m.replace(1L, 1L, 99L));
            assertNull("replace must not have revived it", m.get(1L));
            store.close();
        }
    }

    // =====================================================================
    // Task C — in-memory size counters
    // =====================================================================

    /** A second handle opened on the same store RECONSTRUCTS its counter from the queue length
     *  (the persisted counter records are never updated after create), so it reports the true
     *  physical size — proof the hot path no longer round-trips the counter record. */
    @Test
    public void reopenReconstructsSizeFromQueue() {
        Store store = new StoreOnHeap();
        HTreeCache<Long, Long> m = HTreeCache.create(store, Serializers.LONG, Serializers.LONG,
                4, 7, 4, 0, false, 1_000_000, 0, (Hasher<Long>) null, null);
        for (long k = 0; k < 1_234; k++) m.put(k, k * 3);

        HTreeCache<Long, Long> m2 = HTreeCache.open(store, m.headerRecid(),
                Serializers.LONG, Serializers.LONG);
        assertEquals("reconstructed from queue, not the stale counter record", 1_234L, m2.sizeLong());
        assertEquals(Long.valueOf(9L), m2.get(3L));
        store.verify();
        store.close();
    }

    /** Concurrent readers of sizeLong see a consistent (monotone-correct) value while a writer
     *  mutates: never negative, never above the number inserted so far. */
    @Test(timeout = 30_000)
    public void concurrentSizeLongIsConsistent() throws InterruptedException {
        Store store = new StoreOnHeap();
        HTreeCache<Long, Long> m = HTreeCache.create(store, Serializers.LONG, Serializers.LONG,
                4, 7, 4, 0, false, 1_000_000, 0, (Hasher<Long>) null, null);
        final int N = 100_000;
        final long[] err = {-1};
        Thread reader = new Thread(() -> {
            for (int i = 0; i < 2_000_000 && err[0] < 0; i++) {
                long s = m.sizeLong();
                if (s < 0 || s > N) { err[0] = s; return; }
            }
        });
        reader.start();
        for (long k = 0; k < N; k++) m.put(k, k);
        reader.join();
        assertEquals("sizeLong read an inconsistent value", -1L, err[0]);
        assertEquals(N, m.sizeLong());
        store.verify();
        store.close();
    }
}
