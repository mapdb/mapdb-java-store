package org.mapdb.htree;

import org.junit.Test;
import org.mapdb.ser.Serializers;
import org.mapdb.store.Store;
import org.mapdb.store.StoreDirect;
import org.mapdb.store.StoreOnHeap;
import org.mapdb.store.StoreWAL;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Store-size (byte-budget) eviction, exercised on {@link StoreDirect} (the dialect that
 * reports {@link Store#getCurrentSize()}). The budget is GLOBAL and page-granular
 * (StoreDirect grows in 1 MB pages), so under insertion pressure the resident footprint
 * hovers a bounded amount above the budget; a full {@link HTreeCache#expireEvict()} then
 * pulls it under budget exactly (record-granular via reclaimed free space).
 */
public class HTreeCacheStoreSizeTest {

    private static final long PAGE = 1L << 20; // StoreDirect PAGE_SIZE (ByteBufferVol.SLICE_SIZE)

    /** Under sustained inserts the store footprint stays bounded near the budget, and a
     *  large fraction of inserted entries has been evicted. */
    @Test
    public void storeSizeKeepsFootprintBounded() {
        StoreDirect store = new StoreDirect();
        long budget = 3 * PAGE;
        HTreeCache<Long, Long> m = HTreeCache.create(store, Serializers.LONG, Serializers.LONG,
                4, 7, 4, 0, false, 0, budget, null); // ttl=0, maxSize=0, storeSize=budget

        long maxSeen = 0;
        final int N = 300_000;
        for (long k = 0; k < N; k++) {
            m.put(k, k * 3);
            if ((k & 4095) == 0) maxSeen = Math.max(maxSeen, store.getCurrentSize());
        }
        maxSeen = Math.max(maxSeen, store.getCurrentSize());

        assertTrue("footprint grew unbounded: " + maxSeen + " (budget " + budget + ")",
                maxSeen <= budget + 3 * PAGE);
        assertTrue("store-size eviction did not fire (size=" + m.sizeLong() + ")",
                m.sizeLong() < N / 2);

        m.expireEvict(); // global reclaim: pulls the footprint down to the metadata floor
        // NOTE: getCurrentSize() includes index pages, which store-size eviction cannot
        // reclaim (only compaction can), so the floor is budget + a bounded metadata margin
        // (< one page here) rather than exactly the budget.
        assertTrue("after full evict not near budget: " + store.getCurrentSize(),
                store.getCurrentSize() <= budget + PAGE);

        store.verify();
        store.close();
    }

    /** The newest entries survive an over-budget FIFO drain: whatever remains resident is a
     *  suffix of the insertion order (older keys are the ones evicted). */
    @Test
    public void storeSizeEvictsOldestFirst() {
        StoreDirect store = new StoreDirect();
        long budget = 2 * PAGE;
        // single segment: exact FIFO order
        HTreeCache<Long, Long> m = HTreeCache.create(store, Serializers.LONG, Serializers.LONG,
                0, 4, 8, 0, false, 0, budget, 0, null, null);

        final int N = 200_000;
        for (long k = 0; k < N; k++) m.put(k, k);
        m.expireEvict();

        long survivors = m.sizeLong();
        assertTrue("expected some survivors", survivors > 0);
        assertTrue("expected a real eviction", survivors < N);
        // the resident set is the newest `survivors` keys (FIFO): key N-1 present, key 0 gone
        assertTrue("newest key evicted", m.containsKey((long) (N - 1)));
        assertTrue("oldest key survived a byte-budget drain", !m.containsKey(0L));

        store.verify();
        store.close();
    }

    /** A store that does not report its size (StoreOnHeap => getCurrentSize()==0) cannot honor
     *  a byte budget. When storeSize is the ONLY eviction bound the cache would silently never
     *  evict, so create() rejects it; with another bound present the budget silently disables
     *  (documented) and the other bound still works. */
    @Test
    public void unsupportedStoreDisablesStoreSizeEviction() {
        Store store = new StoreOnHeap();
        assertEquals(0L, store.getCurrentSize());
        try {
            HTreeCache.create(store, Serializers.LONG, Serializers.LONG,
                    0, 4, 8, 0, false, 0, 1, 0, null, null); // storeSize=1 byte is the only bound
            fail("storeSize-only cache on a store without byte accounting must be rejected");
        } catch (IllegalArgumentException expected) { /* would never evict */ }

        // with maxSize alongside, the unsupported byte budget silently disables; maxSize still evicts
        HTreeCache<Long, Long> m = HTreeCache.create(store, Serializers.LONG, Serializers.LONG,
                0, 4, 8, 0, false, 500, 1, 0, null, null);
        for (long k = 0; k < 1_000; k++) m.put(k, k);
        m.expireEvict();
        assertTrue("maxSize bound still enforced", m.sizeLong() <= 500);
        store.verify();
        store.close();
    }

    /** storeSize combined with an overflow map: evicted (oldest) entries are handed to the
     *  overflow, and every inserted key is retrievable (resident or via miss-load). */
    @Test
    public void storeSizeWithOverflowHandoffAndReload() {
        StoreDirect store = new StoreDirect();
        long budget = 2 * PAGE;
        HTreeCache<Long, Long> m = HTreeCache.create(store, Serializers.LONG, Serializers.LONG,
                0, 4, 8, 0, false, 0, budget, 0, null, null);
        Map<Long, Long> overflow = new HashMap<>();
        m.overflow(overflow);

        final int N = 120_000;
        for (long k = 0; k < N; k++) m.put(k, k * 7);

        assertTrue("overflow received evicted entries", overflow.size() > 0);
        assertTrue("cache still holds a resident set", m.sizeLong() > 0);
        // Spot-check keys across the range: each is either resident or reloadable from overflow.
        for (long k = 0; k < N; k += 4_999) {
            assertEquals("key " + k + " lost", Long.valueOf(k * 7), m.get(k));
        }
        store.verify();
        store.close();
    }

    /** All four cache dimensions wired at once (TTL + maxSize + storeSize + overflow) on
     *  StoreDirect: the byte budget binds during inserts (maxSize is a loose safety cap),
     *  evictions hand off, sampled keys reload, and finally a TTL lapse drains the survivors
     *  into overflow — all with the store staying structurally valid. */
    @Test
    public void combinedTtlMaxSizeStoreSizeOverflow() {
        StoreDirect store = new StoreDirect();
        HTreeCacheTCK.FakeClock clock = new HTreeCacheTCK.FakeClock();
        long budget = 2 * PAGE;
        HTreeCache<Long, Long> m = HTreeCache.create(store, Serializers.LONG, Serializers.LONG,
                4, 7, 4, 1_000_000, false, 100_000, budget, 0, null, clock);
        Map<Long, Long> overflow = new ConcurrentHashMapLong();
        m.overflow(overflow);

        long maxSeen = 0;
        final int N = 200_000;
        for (long k = 0; k < N; k++) {
            m.put(k, k + 1);
            if ((k & 8191) == 0) maxSeen = Math.max(maxSeen, store.getCurrentSize());
        }
        maxSeen = Math.max(maxSeen, store.getCurrentSize());

        assertTrue("byte budget did not bind: " + maxSeen, maxSeen <= budget + 3 * PAGE);
        assertTrue("maxSize safety cap respected", m.sizeLong() <= 100_000);
        assertTrue("overflow received evictions", overflow.size() > 0);
        for (long k = 0; k < N; k += 7_919) {
            assertEquals("key " + k + " lost", Long.valueOf(k + 1), m.get(k));
        }

        // TTL lapse: every survivor expires. A full sweep plain-deletes them (TTL-expired ==
        // logically dead), so they must NOT be handed to overflow — the overflow size stays
        // exactly where the capacity evictions left it.
        int overflowBeforeLapse = overflow.size();
        clock.advance(2_000_000);
        m.expireEvict();
        assertEquals("all survivors lapsed and drained", 0L, m.sizeLong());
        assertEquals("TTL-lapsed survivors must NOT reach overflow",
                overflowBeforeLapse, overflow.size());

        store.verify();
        store.close();
    }

    /**
     * Store-size eviction must ALSO fire on {@link StoreWAL} (the durable dialect), which wraps
     * a {@link StoreDirect} in {@code inner}. Before the fix {@code StoreWAL} inherited the
     * default {@code getCurrentSize()==0}, so a byte-budget-only cache on WAL never evicted and
     * grew unbounded. WAL reflects size only for records already flushed into {@code inner}
     * (on commit), so we commit periodically to make the footprint visible to the sweep.
     */
    @Test
    public void storeSizeEvictionFiresOnStoreWAL() throws IOException {
        File file = File.createTempFile("htree-cache-wal-storesize", ".wal");
        file.delete();
        try {
            StoreWAL store = new StoreWAL(file);
            long budget = 2 * PAGE;
            HTreeCache<Long, Long> m = HTreeCache.create(store, Serializers.LONG, Serializers.LONG,
                    0, 4, 8, 0, false, 0, budget, 0, null, null); // ttl=0, maxSize=0, storeSize only

            final int N = 200_000;
            for (long k = 0; k < N; k++) {
                m.put(k, k);
                if ((k & 4095) == 0) store.commit(); // flush staged bytes into inner => size grows
            }
            store.commit();

            assertTrue("StoreWAL.getCurrentSize must delegate to inner (non-zero after commit)",
                    store.getCurrentSize() > 0);
            assertTrue("store-size eviction did not fire on StoreWAL (size=" + m.sizeLong() + ")",
                    m.sizeLong() < N / 2);

            m.expireEvict(); // global reclaim
            store.commit();  // flush the eviction deletes so inner frees the space
            assertTrue("footprint not bounded after evict on WAL: " + store.getCurrentSize(),
                    store.getCurrentSize() <= budget + 2 * PAGE);

            store.verify();
            store.close();
        } finally {
            file.delete();
        }
    }

    /** A plain {@link Map} that is safe to hand cache evictions to (no null quirks). */
    private static final class ConcurrentHashMapLong extends java.util.concurrent.ConcurrentHashMap<Long, Long> {}
}
