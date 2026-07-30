package org.mapdb.htree;

import org.junit.Test;
import org.mapdb.ser.Serializers;
import org.mapdb.store.Store;
import org.mapdb.store.StoreOnHeap;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Pins the OVERFLOW map's deliberate SPILLOVER-BACKING-STORE semantics (mapdb3's
 * {@code expireOverflow}/{@code valueLoader} model, not a coherent two-tier map): cache-tier
 * mutations do NOT propagate to the overflow, so keys can transiently live in both tiers and
 * an overflow-only key stays reloadable. These are design choices — this test exists so
 * changing them is a decision, not an accident (mirrors {@link HTreeCacheSemanticsTest}).
 */
public class HTreeCacheOverflowSemanticsTest {

    private HTreeCache<Long, Long> singleSegment(Store store, long maxSize, Map<Long, Long> overflow) {
        HTreeCache<Long, Long> m = HTreeCache.create(store, Serializers.LONG, Serializers.LONG,
                0, 4, 8, 0, false, maxSize, 0, 0, null, null);
        m.overflow(overflow);
        return m;
    }

    /** A miss-load PROMOTES a copy but leaves the value in the overflow (both tiers hold it). */
    @Test
    public void missLoadLeavesValueInOverflow() {
        Store store = new StoreOnHeap();
        Map<Long, Long> overflow = new HashMap<>();
        HTreeCache<Long, Long> m = singleSegment(store, 10, overflow);

        for (long k = 0; k < 30; k++) m.put(k, k * 2);  // 0..~18 spill to overflow
        assertTrue(overflow.containsKey(5L));
        assertFalse(m.containsKey(5L));

        assertEquals(Long.valueOf(10L), m.get(5L));      // promote
        assertTrue("promoted into cache", m.containsKey(5L));
        assertTrue("left in overflow (backing-store model)", overflow.containsKey(5L));
        store.verify();
        store.close();
    }

    /** remove()/clear() on the cache do NOT delete from the overflow: an overflow-only key
     *  survives a cache remove and stays reloadable via get(). */
    @Test
    public void cacheRemoveDoesNotDeleteFromOverflow() {
        Store store = new StoreOnHeap();
        Map<Long, Long> overflow = new HashMap<>();
        HTreeCache<Long, Long> m = singleSegment(store, 10, overflow);

        for (long k = 0; k < 30; k++) m.put(k, k * 2);
        assertTrue(overflow.containsKey(3L));
        assertFalse(m.containsKey(3L));

        // remove a key that lives ONLY in the overflow: cache remove is a no-op there...
        assertNull("nothing resident to remove", m.remove(3L));
        assertTrue("overflow copy untouched by cache remove", overflow.containsKey(3L));
        // ...and a later get resurrects it from the backing store.
        assertEquals(Long.valueOf(6L), m.get(3L));

        // clear() the cache tier: overflow retains its keys.
        int overflowBefore = overflow.size();
        m.clear();
        assertTrue(m.isEmpty());
        assertEquals("clear does not touch overflow", overflowBefore, overflow.size());
        store.verify();
        store.close();
    }

    /** A cache-only overwrite does not update an older overflow copy that predates it. */
    @Test
    public void cacheOverwriteDoesNotUpdateStaleOverflowCopy() {
        Store store = new StoreOnHeap();
        Map<Long, Long> overflow = new HashMap<>();
        HTreeCache<Long, Long> m = singleSegment(store, 10, overflow);

        overflow.put(7L, 700L); // stale backing value, seeded directly
        m.put(7L, 7L);          // resident, different value
        assertEquals(Long.valueOf(7L), m.get(7L));
        assertEquals("overflow copy is not updated by a cache put", Long.valueOf(700L), overflow.get(7L));
        store.verify();
        store.close();
    }

    /** An {@code overflow.put} that throws propagates to the caller, but the cache and store
     *  stay structurally valid and the victim is not double-handled. */
    @Test
    public void overflowPutFailurePropagatesButLeavesCacheValid() {
        Store store = new StoreOnHeap();
        Map<Long, Long> exploding = new HashMap<>() {
            @Override public Long put(Long k, Long v) {
                throw new IllegalStateException("overflow sink down");
            }
        };
        HTreeCache<Long, Long> m = singleSegment(store, 10, exploding);

        boolean threw = false;
        try {
            for (long k = 0; k < 30; k++) m.put(k, k); // eventually a sweep evicts + hands off
        } catch (IllegalStateException expected) {
            threw = true;
        }
        assertTrue("overflow.put failure should surface", threw);

        // the cache/store remain usable and consistent despite the failed handoff.
        store.verify();
        m.overflow(null); // detach the broken sink
        m.put(1_000L, 1_000L);
        assertEquals(Long.valueOf(1_000L), m.get(1_000L));
        store.verify();
        store.close();
    }
}
