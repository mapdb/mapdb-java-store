package org.mapdb.htree;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mapdb.hash.Hasher;
import org.mapdb.hash.Hashers;
import org.mapdb.ser.Serializers;
import org.mapdb.store.Store;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.LongSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Abstract TCK for {@link HTreeCache}, bound to each store dialect by concrete
 * subclasses — mirrors {@link HTreeMapTCK}. Time-dependent cases inject a
 * deterministic {@link FakeClock} (never advanced backwards — the cache relies on a
 * monotone clock) and mostly use a SINGLE-SEGMENT geometry (concShift=0, 4x8) where
 * max-size enforcement is exact and eviction order is fully deterministic.
 *
 * Record-leak baselines: a drained default-config cache owns 81 records (header +
 * 16 x (root + counter + 3 queue pointers)); single-segment owns 6.
 */
public abstract class HTreeCacheTCK {

    protected abstract Store openStore();

    protected Store store;

    @Before
    public void setUp() {
        store = openStore();
    }

    @After
    public void tearDown() {
        try {
            if (store != null && !store.isClosed()) {
                store.verify();
                store.close();
            }
        } finally {
            cleanup();
        }
    }

    protected void cleanup() {}

    // ---------- helpers ----------

    static final class FakeClock implements LongSupplier {
        long now = 1_000_000; // arbitrary positive epoch
        @Override public long getAsLong() { return now; }
        void advance(long ms) { now += ms; }
    }

    private static final long NO_LIMIT = Long.MAX_VALUE / 2;

    /** Default geometry (16 segments), no TTL, effectively unbounded: plain-map shape. */
    private HTreeCache<Long, Long> plainCache() {
        return HTreeCache.create(store, Serializers.LONG, Serializers.LONG, 0, false, NO_LIMIT);
    }

    /** Single segment (exact max-size, deterministic eviction order), fixed seed 0. */
    private HTreeCache<Long, Long> singleSegment(long ttl, boolean accessOrder, long maxSize,
                                                 Hasher<Long> hasher, LongSupplier clock) {
        return HTreeCache.create(store, Serializers.LONG, Serializers.LONG,
                0, 4, 8, ttl, accessOrder, maxSize, 0, hasher, clock);
    }

    private long liveRecidCount() {
        long n = 0;
        for (PrimitiveIterator.OfLong it = store.getAllRecids(); it.hasNext(); it.nextLong()) n++;
        return n;
    }

    private static <K, V> Map<K, V> drain(HTreeCache<K, V> m) {
        Map<K, V> out = new HashMap<>();
        Iterator<Map.Entry<K, V>> it = m.entryIterator();
        while (it.hasNext()) {
            Map.Entry<K, V> e = it.next();
            assertNull("duplicate key in iteration: " + e.getKey(), out.put(e.getKey(), e.getValue()));
        }
        return out;
    }

    private static <K, V> void assertMatches(HTreeCache<K, V> m, Map<K, V> oracle) {
        assertEquals((long) oracle.size(), m.sizeLong());
        for (Map.Entry<K, V> e : oracle.entrySet()) {
            assertEquals("get " + e.getKey(), e.getValue(), m.get(e.getKey()));
        }
        assertEquals("iteration mismatch", oracle, drain(m));
        Map<K, V> viaForEach = new HashMap<>();
        m.forEach(viaForEach::put);
        assertEquals("forEach mismatch", oracle, viaForEach);
    }

    // ================= plain-map behavior (counters, queue upkeep, leak gates) =================

    @Test
    public void emptyCache() {
        HTreeCache<Long, Long> m = plainCache();
        assertNull(m.get(1L));
        assertFalse(m.containsKey(1L));
        assertEquals(0L, m.sizeLong());
        assertTrue(m.isEmpty());
        assertFalse(m.entryIterator().hasNext());
        assertEquals(81L, liveRecidCount());
    }

    @Test
    public void singleEntryLifecycle() {
        HTreeCache<Long, Long> m = plainCache();
        assertNull(m.put(10L, 100L));
        assertEquals(Long.valueOf(100L), m.get(10L));
        assertTrue(m.containsKey(10L));
        assertEquals(1L, m.sizeLong());

        assertEquals(Long.valueOf(100L), m.put(10L, 200L)); // overwrite bumps, size stable
        assertEquals(Long.valueOf(200L), m.get(10L));
        assertEquals(1L, m.sizeLong());

        assertEquals(Long.valueOf(200L), m.remove(10L)); // frees leaf + queue node
        assertNull(m.get(10L));
        assertTrue(m.isEmpty());
        assertEquals("remove leaked records", 81L, liveRecidCount());
    }

    /** Seeded random op mix vs HashMap oracle; the O(1) counter must track the oracle
     *  size exactly throughout, and a full drain returns to the 81-record skeleton. */
    @Test
    public void fuzzAgainstHashMap() {
        final long seed = 0xCAC4E5EEDL;
        System.out.println("[HTreeCacheTCK.fuzzAgainstHashMap] store=" + store.getClass().getSimpleName()
                + " seed=" + seed);
        Random rnd = new Random(seed);
        final int KEYSPACE = 8_000;
        final int OPS = 40_000;

        HTreeCache<Long, Long> m = plainCache();
        HashMap<Long, Long> oracle = new HashMap<>();

        for (int i = 0; i < OPS; i++) {
            long key = rnd.nextInt(KEYSPACE);
            int roll = rnd.nextInt(100);
            if (roll < 40) {
                long val = rnd.nextLong();
                assertEquals("put[" + i + "] key=" + key, oracle.put(key, val), m.put(key, val));
            } else if (roll < 65) {
                assertEquals("remove[" + i + "] key=" + key, oracle.remove(key), m.remove(key));
            } else if (roll < 80) {
                assertEquals("get[" + i + "] key=" + key, oracle.get(key), m.get(key));
            } else if (roll < 90) {
                assertEquals("containsKey[" + i + "] key=" + key,
                        oracle.containsKey(key), m.containsKey(key));
            } else {
                long val = rnd.nextLong();
                assertEquals("putIfAbsent[" + i + "] key=" + key,
                        oracle.putIfAbsent(key, val), m.putIfAbsent(key, val));
            }
            if ((i & 1023) == 1023) {
                assertEquals("counter drift at op " + i, (long) oracle.size(), m.sizeLong());
            }
        }
        assertMatches(m, oracle);

        for (Long key : new ArrayList<>(oracle.keySet())) {
            assertEquals(oracle.remove(key), m.remove(key));
        }
        assertTrue(m.isEmpty());
        assertEquals("drain leaked records", 81L, liveRecidCount());
    }

    @Test
    public void clearThenReuse() {
        HTreeCache<Long, Long> m = plainCache();
        for (long k = 0; k < 3_000; k++) m.put(k, k * 7);
        assertEquals(3_000L, m.sizeLong());

        m.clear();
        assertTrue(m.isEmpty());
        assertEquals(0L, m.sizeLong());
        assertEquals("clear leaked records", 81L, liveRecidCount());

        HashMap<Long, Long> oracle = new HashMap<>();
        for (long k = 0; k < 500; k++) {
            assertNull(m.put(k, k + 5));
            oracle.put(k, k + 5);
        }
        assertMatches(m, oracle);
    }

    @Test
    public void openSecondHandleSameStore() {
        HTreeCache<Long, Long> m = plainCache();
        HashMap<Long, Long> oracle = new HashMap<>();
        for (long k = 0; k < 1_000; k++) {
            m.put(k, k * 11);
            oracle.put(k, k * 11);
        }
        HTreeCache<Long, Long> m2 = HTreeCache.open(store, m.headerRecid(),
                Serializers.LONG, Serializers.LONG);
        assertMatches(m2, oracle);
    }

    // ================= TTL since write =================

    @Test
    public void ttlExpireAfterWrite() {
        FakeClock clock = new FakeClock();
        HTreeCache<Long, Long> m = singleSegment(1_000, false, 0, null, clock);

        m.put(1L, 10L);
        m.put(2L, 20L);
        clock.advance(500);
        assertEquals(Long.valueOf(10L), m.get(1L)); // get does NOT extend life (no accessOrder)
        assertEquals(2L, m.sizeLong());

        clock.advance(501); // both stamps now in the past
        // STRICT reads: expired entries are absent even before any sweep runs
        assertNull(m.get(1L));
        assertNull(m.get(2L));
        assertFalse(m.containsKey(1L));
        assertFalse(m.entryIterator().hasNext());
        m.forEach((k, v) -> fail("forEach visited expired entry " + k));
        assertEquals("counter counts un-swept expired entries", 2L, m.sizeLong());

        m.expireEvict(); // sweep reclaims records + counters
        assertEquals(0L, m.sizeLong());
        assertTrue(m.isEmpty());
        assertEquals("eviction leaked records", 6L, liveRecidCount());

        // the cache stays fully usable after expiring everything
        m.put(3L, 30L);
        assertEquals(Long.valueOf(30L), m.get(3L));
        assertEquals(1L, m.sizeLong());
    }

    @Test
    public void ttlOverwriteRestamps() {
        FakeClock clock = new FakeClock();
        HTreeCache<Long, Long> m = singleSegment(1_000, false, 0, null, clock);

        m.put(1L, 10L);                     // expires at +1000
        clock.advance(600);
        assertEquals(Long.valueOf(10L), m.put(1L, 20L)); // write re-stamps: expires at +1600
        clock.advance(600);                 // t=+1200: old stamp passed, new one not
        assertEquals(Long.valueOf(20L), m.get(1L));
        assertEquals(1L, m.sizeLong());

        clock.advance(401);                 // t=+1601: expired
        assertNull(m.get(1L));
        m.expireEvict();
        assertEquals(0L, m.sizeLong());
        assertEquals(6L, liveRecidCount());
    }

    /** Foreground sweeps: a mutating op on the segment reclaims expired entries
     *  without any explicit expireEvict call. */
    @Test
    public void putSweepsExpiredEntries() {
        FakeClock clock = new FakeClock();
        HTreeCache<Long, Long> m = singleSegment(1_000, false, 0, null, clock);

        for (long k = 0; k < 50; k++) m.put(k, k);
        clock.advance(1_001);
        assertEquals(50L, m.sizeLong()); // not yet swept
        m.put(100L, 100L); // put sweeps the (single) segment first
        assertEquals(1L, m.sizeLong());
        assertEquals(Long.valueOf(100L), m.get(100L));
    }

    /** Entries sharing one collision bucket expire independently: the sweep removes
     *  exactly the expired ones from the shared leaf. */
    @Test
    public void collisionBucketExpiry() {
        FakeClock clock = new FakeClock();
        Hasher<Long> constantHash = (k, seed) -> 0;
        HTreeCache<Long, Long> m = singleSegment(1_000, false, 0, constantHash, clock);

        m.put(1L, 10L);                 // stamp +1000
        clock.advance(100);
        m.put(2L, 20L);                 // stamp +1100
        clock.advance(950);             // t=+1050: entry 1 expired, entry 2 alive
        m.put(3L, 30L);                 // sweep evicts entry 1 from the shared bucket
        assertNull(m.get(1L));
        assertEquals(Long.valueOf(20L), m.get(2L));
        assertEquals(Long.valueOf(30L), m.get(3L));
        assertEquals(2L, m.sizeLong());

        clock.advance(51);              // t=+1101: entry 2 expired too
        assertNull(m.get(2L));
        m.expireEvict();
        assertEquals(1L, m.sizeLong()); // only entry 3 left
        assertEquals(Long.valueOf(30L), m.get(3L));

        assertEquals(Long.valueOf(30L), m.remove(3L));
        assertEquals("collision expiry leaked records", 6L, liveRecidCount());
    }

    // ================= TTL since access (accessOrder) =================

    @Test
    public void accessOrderGetExtendsLife() {
        FakeClock clock = new FakeClock();
        HTreeCache<Long, Long> m = singleSegment(1_000, true, 0, null, clock);

        m.put(1L, 10L);
        m.put(2L, 20L);                 // both stamped +1000
        clock.advance(600);
        assertEquals(Long.valueOf(10L), m.get(1L)); // bump: entry 1 now expires at +1600
        clock.advance(600);             // t=+1200: entry 2 expired, entry 1 alive
        assertEquals(Long.valueOf(10L), m.get(1L)); // this get also sweeps entry 2
        assertNull(m.get(2L));
        assertEquals(1L, m.sizeLong()); // sweep already reclaimed entry 2

        clock.advance(1_801);           // let entry 1 expire (stamped +1800 by last get)
        assertNull(m.get(1L));
        m.expireEvict();
        assertEquals("access-order expiry leaked records", 6L, liveRecidCount());
    }

    // ================= max-size eviction =================

    /** Single segment, no TTL: FIFO eviction. Sweeps run BEFORE each insert, so the
     *  size may sit one above maxSize until the next mutating op (documented). */
    @Test
    public void maxSizeFifo() {
        HTreeCache<Long, Long> m = singleSegment(0, false, 10, null, null);
        for (long k = 0; k < 30; k++) {
            assertNull(m.put(k, k * 3));
            assertTrue("size bound violated at k=" + k, m.sizeLong() <= 11);
        }
        assertEquals(11L, m.sizeLong()); // evict-before-insert leaves maxSize+1
        m.expireEvict();
        assertEquals(10L, m.sizeLong());
        // FIFO: exactly the 10 newest survive
        for (long k = 0; k < 20; k++) {
            assertFalse("evicted key present: " + k, m.containsKey(k));
        }
        HashMap<Long, Long> oracle = new HashMap<>();
        for (long k = 20; k < 30; k++) oracle.put(k, k * 3);
        assertMatches(m, oracle);

        for (long k = 20; k < 30; k++) m.remove(k);
        assertEquals("max-size eviction leaked records", 6L, liveRecidCount());
    }

    /** accessOrder + maxSize = LRU: a got entry survives eviction that claims the
     *  least-recently-used one instead. */
    @Test
    public void maxSizeLruWithAccessOrder() {
        HTreeCache<Long, Long> m = singleSegment(0, true, 10, null, null);
        for (long k = 0; k < 10; k++) m.put(k, k);
        assertEquals(Long.valueOf(0L), m.get(0L)); // bump key 0 to most-recently-used
        m.put(10L, 10L); // counter 10 -> no evict -> 11
        m.put(11L, 11L); // over budget: evicts LRU = key 1 (key 0 was bumped)
        assertTrue(m.containsKey(0L));
        assertFalse("LRU key 1 should be evicted", m.containsKey(1L));
        assertEquals(11L, m.sizeLong());
    }

    /** Regression: with accessOrder a segment may sit at share+1 (put sweeps BEFORE
     *  insert), and a {@code get} on the CURRENT-LRU key must return that key's value
     *  and PROMOTE it — not evict it as the over-capacity victim and return null. The
     *  get bumps the hit to the queue head first, then the sweep claims the now-oldest
     *  OTHER entry. (Before the fix the sweep ran first and returned null.) */
    @Test
    public void accessOrderGetOnLruKeyPromotesNotEvicts() {
        HTreeCache<Long, Long> m = singleSegment(0, true, 1, null, null); // per-segment share 1
        m.put(0L, 100L);
        m.put(1L, 101L);            // sweep-before-insert leaves counter 2 (share+1); LRU order 0,1
        assertEquals(2L, m.sizeLong());
        // key 0 is the LRU; a hit must return it and promote it, not sacrifice it.
        assertEquals("get on the LRU key must return its value, not evict it",
                Long.valueOf(100L), m.get(0L));
        // after the bump key 0 is newest; the sweep evicts the now-oldest OTHER key 1.
        assertTrue("bumped key 0 survives", m.containsKey(0L));
        assertFalse("now-LRU key 1 evicted by the get's sweep", m.containsKey(1L));
        assertEquals(1L, m.sizeLong());
    }

    /** Default 16-segment geometry: the bound is approximate (per-segment shares),
     *  but a big overshoot must still be reclaimed to maxSize + segments slack. */
    @Test
    public void maxSizeManySegmentsApproximateBound() {
        HTreeCache<Long, Long> m = HTreeCache.create(store, Serializers.LONG, Serializers.LONG,
                0, false, 160);
        for (long k = 0; k < 2_000; k++) m.put(k, k);
        m.expireEvict();
        long size = m.sizeLong();
        assertTrue("size " + size + " should be <= maxSize", size <= 160);
        assertTrue("size " + size + " unexpectedly small", size >= 160 - 16);
    }

    // ================= config / null / reopen =================

    @Test
    public void invalidConfigRejected() {
        Runnable[] calls = {
                () -> HTreeCache.create(store, Serializers.LONG, Serializers.LONG, 0, false, 0),
                () -> HTreeCache.create(store, Serializers.LONG, Serializers.LONG, -1, false, 10),
                () -> HTreeCache.create(store, Serializers.LONG, Serializers.LONG, 0, false, -1),
                () -> HTreeCache.create(store, Serializers.LONG, Serializers.LONG,
                        4, 7, 3, 1_000, false, 0, null), // 4+21 != 32
        };
        for (Runnable r : calls) {
            try {
                r.run();
                fail("expected IllegalArgumentException");
            } catch (IllegalArgumentException expected) { /* ok */ }
        }
    }

    @Test
    public void nullRejected() {
        HTreeCache<Long, Long> m = plainCache();
        m.put(1L, 1L);
        Runnable[] calls = {
                () -> m.get(null),
                () -> m.put(null, 1L),
                () -> m.put(1L, null),
                () -> m.remove(null),
                () -> m.containsKey(null),
                () -> m.putIfAbsent(null, 1L),
                () -> m.putIfAbsent(1L, null),
        };
        for (Runnable r : calls) {
            try {
                r.run();
                fail("expected NullPointerException");
            } catch (NullPointerException expected) { /* ok */ }
        }
        assertEquals(Long.valueOf(1L), m.get(1L));
        assertEquals(1L, m.sizeLong());
    }

    /** The expiry config rides in the header: a second handle opened without params
     *  enforces the same TTL against the same (injected) clock. */
    @Test
    public void openedHandleEnforcesPersistedConfig() {
        FakeClock clock = new FakeClock();
        HTreeCache<Long, Long> m = singleSegment(1_000, false, 0, null, clock);
        m.put(1L, 10L);

        HTreeCache<Long, Long> m2 = HTreeCache.open(store, m.headerRecid(),
                Serializers.LONG, Serializers.LONG, null, clock);
        assertEquals(Long.valueOf(10L), m2.get(1L));
        clock.advance(1_001);
        assertNull("reopened handle must enforce the persisted TTL", m2.get(1L));
        m2.expireEvict();
        assertEquals(0L, m2.sizeLong());
        // Size counters are now IN-MEMORY and per-handle (class doc, SIZE COUNTERS): the stale
        // first handle m does NOT observe m2's evictions live (nor would two live handles share
        // locks). The PERSISTED size is authoritative — a freshly-opened handle recomputes it
        // (0) from the queue.
        HTreeCache<Long, Long> m3 = HTreeCache.open(store, m.headerRecid(),
                Serializers.LONG, Serializers.LONG, null, clock);
        assertEquals("persisted size after evict+reopen", 0L, m3.sizeLong());
    }

    // ================= expiry-aware adversarial fuzz + edge cases =================

    /**
     * Seeded op-mix (put/get/containsKey/remove) against an EXPIRY-AWARE oracle in a
     * single-segment TTL cache. The FakeClock jumps a random amount between ops, so
     * many entries lapse without being swept: the oracle tracks a per-key expiry stamp
     * and mirrors the cache's STRICT reads (an expired-but-unswept entry reads as
     * absent) and its evict-on-mutate sweeps (put/remove reclaim all lapsed entries
     * first). Periodic expireEvict()+size checks pin the O(1) counter to the live count.
     */
    @Test
    public void expiryAwareFuzz() {
        final long seed = 0xE5C1AABBL;
        System.out.println("[HTreeCacheTCK.expiryAwareFuzz] store=" + store.getClass().getSimpleName()
                + " seed=" + seed);
        FakeClock clock = new FakeClock();
        final long ttl = 1_000;
        HTreeCache<Long, Long> m = singleSegment(ttl, false, 0, null, clock);
        HashMap<Long, Long> val = new HashMap<>();  // key -> value
        HashMap<Long, Long> exp = new HashMap<>();  // key -> expiry stamp
        Random rnd = new Random(seed);
        final int KEYSPACE = 200;
        final int OPS = 30_000;

        for (int i = 0; i < OPS; i++) {
            clock.advance(rnd.nextInt(260)); // monotone jump; some entries lapse
            long now = clock.now;
            long key = rnd.nextInt(KEYSPACE);
            int roll = rnd.nextInt(100);
            if (roll < 42) { // put — sweeps lapsed entries first, then insert/overwrite
                purge(val, exp, now);
                Long prev = val.get(key); // after purge, any survivor is live
                long v = rnd.nextLong();
                assertEquals("put[" + i + "] key=" + key, prev, m.put(key, v));
                val.put(key, v);
                exp.put(key, now + ttl);
            } else if (roll < 58) { // remove — sweeps first, so a lapsed entry returns null
                purge(val, exp, now);
                Long old = val.remove(key);
                exp.remove(key);
                assertEquals("remove[" + i + "] key=" + key, old, m.remove(key));
            } else if (roll < 80) { // strict get (no sweep, no restamp)
                assertEquals("get[" + i + "] key=" + key, liveValue(val, exp, key, now), m.get(key));
            } else { // strict containsKey
                assertEquals("containsKey[" + i + "] key=" + key,
                        liveValue(val, exp, key, now) != null, m.containsKey(key));
            }
            if ((i & 511) == 511) {
                m.expireEvict();
                purge(val, exp, clock.now);
                assertEquals("counter drift at op " + i, (long) val.size(), m.sizeLong());
            }
        }
        m.expireEvict();
        purge(val, exp, clock.now);
        assertEquals((long) val.size(), m.sizeLong());
        assertMatches(m, val);

        for (Long k : new ArrayList<>(val.keySet())) m.remove(k);
        assertTrue(m.isEmpty());
        assertEquals("expiry fuzz drain leaked records", 6L, liveRecidCount());
    }

    /** Drop every entry whose stamp lapsed strictly before {@code now} (mirrors a sweep). */
    private static void purge(HashMap<Long, Long> val, HashMap<Long, Long> exp, long now) {
        val.keySet().removeIf(k -> exp.get(k) < now);
        exp.keySet().removeIf(k -> exp.get(k) < now);
    }

    /** Strict-read oracle: the value iff present and NOT lapsed (stamp >= now), else null. */
    private static Long liveValue(HashMap<Long, Long> val, HashMap<Long, Long> exp,
                                  long key, long now) {
        Long e = exp.get(key);
        return (e != null && e >= now) ? val.get(key) : null;
    }

    /** accessOrder + maxSize + TTL together: eviction is LRU (a got entry outlives an
     *  untouched one) AND the TTL still expires survivors on schedule. Deterministic. */
    @Test
    public void accessOrderMaxSizeTtlCombined() {
        FakeClock clock = new FakeClock();
        HTreeCache<Long, Long> m = singleSegment(1_000, true, 3, null, clock);
        m.put(1L, 10L);
        m.put(2L, 20L);
        m.put(3L, 30L); // counter = 3 (== per-segment share)
        clock.advance(100);
        assertEquals(Long.valueOf(10L), m.get(1L)); // access-bump: LRU order 2,3,1; key1 stamp +1100
        m.put(4L, 40L); // sweep sees over=0; inserts -> counter 4, order 2,3,1,4
        m.put(5L, 50L); // sweep over=1 evicts LRU (oldest) = key 2; inserts 5 -> order 3,1,4,5
        assertFalse("LRU key 2 evicted", m.containsKey(2L));
        assertTrue("bumped key 1 survives", m.containsKey(1L));
        m.expireEvict(); // over=1 -> evict oldest = key 3
        assertFalse(m.containsKey(3L));
        assertEquals(3L, m.sizeLong()); // survivors 1,4,5 (all stamped +1100)

        clock.advance(1_001); // t=1101 > 1100: all three lapse
        assertNull(m.get(1L));
        assertNull(m.get(4L));
        assertNull(m.get(5L));
        m.expireEvict();
        assertEquals(0L, m.sizeLong());
        assertTrue(m.isEmpty());
        assertEquals("LRU+TTL leaked records", 6L, liveRecidCount());
    }

    /** putIfAbsent on an EXPIRED-but-unswept entry: put/putIfAbsent sweep first, so the
     *  stale entry is gone and the call inserts fresh, returning null. */
    @Test
    public void putIfAbsentOnExpiredUnsweptEntry() {
        FakeClock clock = new FakeClock();
        HTreeCache<Long, Long> m = singleSegment(1_000, false, 0, null, clock);
        m.put(1L, 10L);
        clock.advance(1_001); // lapsed, not yet swept
        assertEquals("counter still counts the unswept entry", 1L, m.sizeLong());
        assertNull("sweep removes it first, so this inserts fresh", m.putIfAbsent(1L, 99L));
        assertEquals(Long.valueOf(99L), m.get(1L));
        assertEquals(1L, m.sizeLong());
    }

    /** remove() of an EXPIRED-but-unswept entry returns null (the sweep runs first, so
     *  there is nothing to return), and leaves no leaked records. */
    @Test
    public void removeExpiredUnsweptEntryReturnsNull() {
        FakeClock clock = new FakeClock();
        HTreeCache<Long, Long> m = singleSegment(1_000, false, 0, null, clock);
        m.put(1L, 10L);
        clock.advance(1_001);
        assertNull("sweep runs before the remove scan", m.remove(1L));
        assertEquals(0L, m.sizeLong());
        assertTrue(m.isEmpty());
        assertEquals("remove-expired leaked records", 6L, liveRecidCount());
    }

    /** TTL with variable-length String serializers (single segment), across every store. */
    @Test
    public void ttlWithStringKeysAndValues() {
        FakeClock clock = new FakeClock();
        HTreeCache<String, String> m = HTreeCache.create(store, Serializers.STRING, Serializers.STRING,
                0, 4, 8, 1_000, false, 0, 0, null, clock);
        m.put("alpha", "one");
        m.put("beta", "two");
        clock.advance(500);
        assertEquals("one", m.get("alpha")); // get does not extend life (no accessOrder)
        clock.advance(501); // both lapse
        assertNull(m.get("alpha"));
        assertNull(m.get("beta"));
        assertFalse(m.containsKey("alpha"));
        m.forEach((k, v) -> fail("forEach visited expired entry " + k));
        m.expireEvict();
        assertEquals(0L, m.sizeLong());
        assertEquals("string TTL leaked records", 6L, liveRecidCount());

        m.put("gamma", "three"); // fully reusable afterwards
        assertEquals("three", m.get("gamma"));
    }

    /** Default 16-segment geometry with TTL: entries spread across segments all lapse,
     *  and a single expireEvict() reclaims everything back to the 81-record skeleton. */
    @Test
    public void multiSegmentTtlEvictsEverything() {
        FakeClock clock = new FakeClock();
        HTreeCache<Long, Long> m = HTreeCache.create(store, Serializers.LONG, Serializers.LONG,
                4, 7, 4, 1_000, false, 0, 0, null, clock);
        for (long k = 0; k < 800; k++) m.put(k, k * 3);
        assertEquals(800L, m.sizeLong());
        clock.advance(1_001);
        for (long k = 0; k < 800; k += 37) assertNull("strict read " + k, m.get(k)); // all lapsed
        assertEquals("counter counts unswept", 800L, m.sizeLong());
        m.expireEvict();
        assertEquals(0L, m.sizeLong());
        assertTrue(m.isEmpty());
        assertEquals("multi-segment TTL leaked records", 81L, liveRecidCount());
    }

    /**
     * Weak-consistency of iteration while the clock advances mid-drain. drainSegment
     * snapshots each segment's LIVE entries lazily; advancing the clock after some
     * segments are drained must not crash and must never yield an entry that was
     * already expired WHEN ITS SEGMENT WAS DRAINED — so every yielded value belongs to
     * the inserted set and the yield count never exceeds what was inserted.
     */
    @Test
    public void iteratorWeakConsistencyWhileExpiring() {
        FakeClock clock = new FakeClock();
        HTreeCache<Long, Long> m = HTreeCache.create(store, Serializers.LONG, Serializers.LONG,
                4, 7, 4, 1_000, false, 0, 0, null, clock);
        final int N = 400;
        for (long k = 0; k < N; k++) m.put(k, k * 10);

        Iterator<Map.Entry<Long, Long>> it = m.entryIterator();
        HashMap<Long, Long> seen = new HashMap<>();
        int steps = 0;
        while (it.hasNext()) {
            Map.Entry<Long, Long> e = it.next();
            assertNull("duplicate in weak iteration: " + e.getKey(), seen.put(e.getKey(), e.getValue()));
            if (steps++ == N / 4) clock.advance(2_000); // expire everything not yet drained
        }
        for (Map.Entry<Long, Long> e : seen.entrySet()) {
            assertEquals("yielded value must belong to the inserted set",
                    Long.valueOf(e.getKey() * 10), e.getValue());
        }
        assertTrue("yielded more than inserted", seen.size() <= N);
        m.expireEvict();
        assertEquals(0L, m.sizeLong());
        assertEquals("weak-iter leaked records", 81L, liveRecidCount());
    }

    /** Overwriting one entry in a shared collision bucket re-stamps + bumps ONLY that
     *  entry; the neighbor keeps its own queue node and expires on its own schedule. */
    @Test
    public void overwriteInSharedBucketKeepsNeighborQueueNode() {
        FakeClock clock = new FakeClock();
        Hasher<Long> constantHash = (k, seed) -> 0;
        HTreeCache<Long, Long> m = singleSegment(1_000, false, 0, constantHash, clock);
        m.put(1L, 10L);            // stamp +1000
        clock.advance(100);
        m.put(2L, 20L);            // stamp +1100
        clock.advance(100);        // t=200
        m.put(1L, 11L);            // overwrite key1: re-stamp +1200, bump ahead of key2

        clock.advance(901);        // t=1101: key2 (+1100) lapsed, key1 (+1200) alive
        assertNull("neighbor keeps its own (earlier) expiry", m.get(2L));
        assertEquals(Long.valueOf(11L), m.get(1L));
        m.expireEvict();
        assertEquals(1L, m.sizeLong());
        assertEquals(Long.valueOf(11L), m.get(1L));

        clock.advance(200);        // t=1301: key1 lapses too
        m.expireEvict();
        assertEquals(0L, m.sizeLong());
        assertEquals("shared-bucket overwrite leaked records", 6L, liveRecidCount());
    }

    // =====================================================================
    // java.util.Map / ConcurrentMap interface conformance. All cases use a
    // plainCache (ttl=0, unbounded maxSize, accessOrder=false) so NOTHING
    // expires or evicts during the test — then size()/isEmpty() (physical
    // occupancy) equal the live entry count and the collection-view size never
    // exceeds what is iterated (class-doc caveat neutralised). Also covers the
    // NEW cache CAS methods remove(k,v)/replace/replace-if.
    // =====================================================================

    /** Assert the java.util.Map collection views agree with a HashMap oracle
     *  (values must be distinct so the values-Collection compares as a Set). */
    private static void assertViewsMatchOracle(Map<Long, Long> m, Map<Long, Long> oracle) {
        assertEquals("size", oracle.size(), m.size());
        assertEquals("isEmpty", oracle.isEmpty(), m.isEmpty());
        assertEquals("entrySet", new HashSet<>(oracle.entrySet()), new HashSet<>(m.entrySet()));
        assertEquals("keySet", new HashSet<>(oracle.keySet()), new HashSet<>(m.keySet()));
        assertEquals("values", new HashSet<>(oracle.values()), new HashSet<>(m.values()));
        assertEquals("entrySet.size", oracle.size(), m.entrySet().size());
        assertEquals("keySet.size", oracle.size(), m.keySet().size());
        assertEquals("values.size", oracle.size(), m.values().size());
        for (Map.Entry<Long, Long> e : oracle.entrySet()) {
            assertTrue("keySet.contains " + e.getKey(), m.keySet().contains(e.getKey()));
            assertTrue("values.contains " + e.getValue(), m.values().contains(e.getValue()));
            assertTrue("containsValue " + e.getValue(), m.containsValue(e.getValue()));
        }
        assertFalse(m.containsValue(-999_999L));
        assertTrue(m.equals(oracle));
        assertTrue(oracle.equals(m));
        assertEquals(oracle.hashCode(), m.hashCode());
    }

    @Test
    public void sizeAgreesWithSizeLong() {
        HTreeCache<Long, Long> m = plainCache();
        assertEquals(0, m.size());
        assertTrue(m.isEmpty());
        for (long k = 0; k < 1_000; k++) m.put(k, k);
        assertEquals(1_000, m.size());
        assertEquals(m.sizeLong(), (long) m.size());
        assertFalse(m.isEmpty());
        for (long k = 0; k < 400; k++) m.remove(k);
        assertEquals(600, m.size());
        assertEquals(m.sizeLong(), (long) m.size());
    }

    @Test
    public void collectionViewsMatchOracle() {
        HTreeCache<Long, Long> m = plainCache();
        HashMap<Long, Long> oracle = new HashMap<>();
        for (long k = 0; k < 1_500; k++) { m.put(k, k * 7 + 1); oracle.put(k, k * 7 + 1); }
        assertViewsMatchOracle(m, oracle);
    }

    @Test
    public void entrySetContainsAndRemove() {
        HTreeCache<Long, Long> m = plainCache();
        for (long k = 0; k < 20; k++) m.put(k, k * 2);
        Set<Map.Entry<Long, Long>> es = m.entrySet();
        assertEquals(20, es.size());
        assertTrue(es.contains(new AbstractMap.SimpleImmutableEntry<>(3L, 6L)));
        assertFalse(es.contains(new AbstractMap.SimpleImmutableEntry<>(3L, 7L)));
        assertFalse(es.contains(new AbstractMap.SimpleImmutableEntry<>(99L, 0L)));
        assertFalse(es.contains("not an entry"));
        assertFalse(es.remove(new AbstractMap.SimpleImmutableEntry<>(3L, 7L)));
        assertTrue(m.containsKey(3L));
        assertTrue(es.remove(new AbstractMap.SimpleImmutableEntry<>(3L, 6L)));
        assertFalse(m.containsKey(3L));
        assertEquals(19L, m.sizeLong());
        es.clear();
        assertTrue(m.isEmpty());
        assertEquals("view clear leaked records", 81L, liveRecidCount());
    }

    /** The entrySet/keySet/values views are mutable: iterator.remove() and the derived
     *  Collection.remove/removeAll all delete from the live cache, freeing queue nodes. */
    @Test
    public void viewIteratorsSupportRemoval() {
        HTreeCache<Long, Long> m = plainCache();
        for (long k = 0; k < 20; k++) m.put(k, k);

        Iterator<Map.Entry<Long, Long>> eit = m.entrySet().iterator();
        Long removedKey = eit.next().getKey();
        eit.remove();
        assertFalse(m.containsKey(removedKey));
        assertEquals(19L, m.sizeLong());
        try { eit.remove(); fail("double remove must throw IllegalStateException"); }
        catch (IllegalStateException expected) { /* ok */ }

        Long k2 = m.keySet().iterator().next();
        assertTrue(m.keySet().remove(k2));
        assertFalse(m.containsKey(k2));
        Long v3 = m.values().iterator().next();
        assertTrue(m.values().remove(v3));
        assertEquals(17L, m.sizeLong());

        m.entrySet().removeAll(new HashSet<>(m.entrySet()));
        assertTrue(m.isEmpty());
        assertEquals("view removal must free records", 81L, liveRecidCount());
    }

    /** Blind putOnly/removeOnly on the cache: correct values, counters, and no record leak. */
    @Test
    public void blindPutAndRemove() {
        HTreeCache<Long, Long> m = plainCache();
        for (long k = 0; k < 50; k++) m.putOnly(k, k + 1);           // blind inserts
        assertEquals(50L, m.sizeLong());
        m.putOnly(0L, 999L);                                          // blind overwrite
        assertEquals(Long.valueOf(999L), m.get(0L));
        assertEquals(50L, m.sizeLong());
        assertTrue(m.removeOnly(0L));
        assertFalse(m.removeOnly(0L));                               // already gone
        for (long k = 1; k < 50; k++) assertTrue(m.removeOnly(k));
        assertTrue(m.isEmpty());
        assertEquals("blind put/remove must not leak cache records", 81L, liveRecidCount());
        try { m.putOnly(null, 1L); fail("null key"); } catch (NullPointerException ok) { }
        try { m.removeOnly(null); fail("null key"); } catch (NullPointerException ok) { }
    }

    @Test
    public void putAllAndClearThroughMapInterface() {
        HashMap<Long, Long> src = new HashMap<>();
        for (long k = 0; k < 500; k++) src.put(k, k * 3 + 1);
        Map<Long, Long> m = plainCache();
        m.putAll(src);
        assertViewsMatchOracle(m, src);
        m.clear();
        assertTrue(m.isEmpty());
        assertEquals(0, m.size());
        assertTrue(m.entrySet().isEmpty());
        assertEquals("putAll/clear leaked records", 81L, liveRecidCount());
    }

    @Test
    public void mapEqualsAndHashCode() {
        HTreeCache<Long, Long> m = plainCache();
        HashMap<Long, Long> oracle = new HashMap<>();
        for (long k = 0; k < 300; k++) { m.put(k, k + 1); oracle.put(k, k + 1); }
        assertTrue(m.equals(oracle));
        assertEquals(oracle.hashCode(), m.hashCode());
        m.put(0L, 999L);
        assertFalse(m.equals(oracle));
        assertFalse(oracle.equals(m));
        m.put(0L, 1L);
        assertTrue(m.equals(oracle));
        m.put(-1L, -1L);
        assertFalse(m.equals(oracle));
        assertTrue(m.equals(m));
        HTreeCache<Long, Long> empty = plainCache();
        assertTrue(empty.equals(new HashMap<Long, Long>()));
    }

    /** The NEW cache CAS methods (remove(k,v)/replace/replace-if) plus putIfAbsent,
     *  driven through a ConcurrentMap-typed reference along with the default methods. */
    @Test
    public void concurrentMapCasAndDefaultsThroughInterface() {
        ConcurrentMap<Long, Long> cm = plainCache();
        assertNull(cm.putIfAbsent(1L, 10L));
        assertEquals(Long.valueOf(10L), cm.putIfAbsent(1L, 20L));
        assertEquals(Long.valueOf(10L), cm.getOrDefault(1L, -1L));
        assertEquals(Long.valueOf(-1L), cm.getOrDefault(2L, -1L));
        assertNull(cm.replace(2L, 99L));                 // absent: no-op
        assertFalse(cm.replace(2L, 1L, 2L));             // absent: no-op
        assertEquals(Long.valueOf(10L), cm.replace(1L, 11L));
        assertFalse(cm.replace(1L, 999L, 12L));          // wrong expected
        assertTrue(cm.replace(1L, 11L, 12L));
        assertFalse(cm.remove(1L, 999L));                // wrong value
        assertTrue(cm.remove(1L, 12L));                  // matching value
        assertFalse(cm.containsKey(1L));
        assertEquals(Long.valueOf(5L), cm.computeIfAbsent(3L, k -> 5L));
        assertEquals(Long.valueOf(5L), cm.computeIfAbsent(3L, k -> 999L));
        assertEquals(Long.valueOf(8L), cm.merge(3L, 3L, Long::sum));
        assertEquals(Long.valueOf(100L), cm.compute(4L, (k, v) -> 100L));
        assertNull(cm.compute(4L, (k, v) -> null));      // null result removes
        assertFalse(cm.containsKey(4L));
        assertEquals(Long.valueOf(8L), cm.get(3L));
    }

    /** Direct coverage of the newly-added cache CAS methods on the concrete type,
     *  mirroring HTreeMapTCK.casOperations (no expiry: a plainCache). */
    @Test
    public void cacheCasOperations() {
        HTreeCache<Long, Long> m = plainCache();
        assertNull(m.putIfAbsent(1L, 10L));
        assertEquals(Long.valueOf(10L), m.putIfAbsent(1L, 20L));
        assertEquals(Long.valueOf(10L), m.get(1L));

        assertNull(m.replace(2L, 20L));                  // absent: no-op
        assertNull(m.get(2L));
        assertEquals(Long.valueOf(10L), m.replace(1L, 11L));
        assertEquals(Long.valueOf(11L), m.get(1L));

        assertFalse(m.replace(1L, 999L, 12L));           // wrong expected
        assertEquals(Long.valueOf(11L), m.get(1L));
        assertTrue(m.replace(1L, 11L, 12L));
        assertEquals(Long.valueOf(12L), m.get(1L));
        assertFalse(m.replace(2L, 1L, 2L));              // absent key

        assertFalse(m.remove(1L, 999L));                 // wrong value: kept
        assertTrue(m.containsKey(1L));
        assertTrue(m.remove(1L, 12L));                   // matching value: removed
        assertNull(m.get(1L));
        assertFalse(m.remove(1L, 12L));                  // already gone
        assertEquals(0L, m.sizeLong());
        assertTrue(m.isEmpty());
    }

    @Test
    public void drivenPurelyThroughMapInterface() {
        Map<Long, Long> map = plainCache();
        HashMap<Long, Long> oracle = new HashMap<>();
        Random rnd = new Random(2024);
        for (int i = 0; i < 15_000; i++) {
            long k = rnd.nextInt(2_000);
            int roll = rnd.nextInt(100);
            if (roll < 55) {
                long v = rnd.nextLong();
                assertEquals(oracle.put(k, v), map.put(k, v));
            } else if (roll < 80) {
                assertEquals(oracle.remove(k), map.remove(k));
            } else {
                assertEquals(oracle.get(k), map.get(k));
            }
            if ((i & 2047) == 2047) assertEquals(oracle.size(), map.size());
        }
        assertEquals(oracle.size(), map.size());
        assertEquals(oracle, map);
        assertEquals(map, oracle);
        HashMap<Long, Long> viaEntrySet = new HashMap<>();
        for (Map.Entry<Long, Long> e : map.entrySet()) viaEntrySet.put(e.getKey(), e.getValue());
        assertEquals(oracle, viaEntrySet);
        map.clear();
        assertTrue(map.isEmpty());
    }
}
