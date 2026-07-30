package org.mapdb.htree;

import org.junit.Test;
import org.mapdb.ser.Serializers;
import org.mapdb.store.Store;
import org.mapdb.store.StoreOnHeap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Pins the {@link HTreeCache} contract edges flagged by review:
 * the OPEN TTL deadline ({@code timestamp < now}), counter-based (physical)
 * {@code isEmpty}/{@code sizeLong} vs strict reads, the {@code maxSize < segments}
 * floor-division degeneracy, and stamp-domain validation (negative clock / overflow).
 * These are deliberate semantics — this test exists so changing them is a decision,
 * not an accident.
 */
public class HTreeCacheSemanticsTest {

    private HTreeCache<Long, Long> singleSegment(Store store, long ttl, HTreeCacheTCK.FakeClock clock) {
        return HTreeCache.create(store, Serializers.LONG, Serializers.LONG,
                0, 4, 8, ttl, false, 0, 0, null, clock);
    }

    /** The deadline is OPEN: live at exactly writeTime+ttl, expired one ms past it. */
    @Test
    public void ttlBoundaryIsOpenDeadline() {
        Store store = new StoreOnHeap();
        HTreeCacheTCK.FakeClock clock = new HTreeCacheTCK.FakeClock();
        HTreeCache<Long, Long> m = singleSegment(store, 1_000, clock);

        m.put(1L, 10L);
        clock.advance(1_000); // exactly the deadline instant
        assertEquals("live AT the deadline", Long.valueOf(10L), m.get(1L));
        assertTrue(m.containsKey(1L));

        clock.advance(1); // deadline has passed
        assertNull("expired past the deadline", m.get(1L));
        assertFalse(m.containsKey(1L));
        store.verify();
        store.close();
    }

    /** isEmpty/sizeLong are PHYSICAL (counter-based): un-swept expired entries count,
     *  even though every strict read already treats them as absent. */
    @Test
    public void isEmptyCountsUnsweptExpiredEntries() {
        Store store = new StoreOnHeap();
        HTreeCacheTCK.FakeClock clock = new HTreeCacheTCK.FakeClock();
        HTreeCache<Long, Long> m = singleSegment(store, 1_000, clock);

        m.put(1L, 10L);
        clock.advance(1_001);
        assertNull(m.get(1L)); // strictly absent...
        assertFalse(m.entryIterator().hasNext());
        assertFalse("...but physically still resident", m.isEmpty());
        assertEquals(1L, m.sizeLong());

        m.expireEvict();
        assertTrue(m.isEmpty());
        assertEquals(0L, m.sizeLong());
        store.verify();
        store.close();
    }

    /** maxSize below the segment count floors each segment's share to 0: every sweep
     *  drains the whole touched segment, so at most one resident entry per touched
     *  segment survives between ops and expireEvict() drains everything. */
    @Test
    public void tinyMaxSizeDegeneratesToPerSegmentResidents() {
        Store store = new StoreOnHeap();
        HTreeCache<Long, Long> m = HTreeCache.create(store, Serializers.LONG, Serializers.LONG,
                0, false, 1); // maxSize 1 << 16 segments
        for (long k = 0; k < 100; k++) m.put(k, k);
        long size = m.sizeLong();
        assertTrue("at most one resident per segment, got " + size, size <= 16);
        assertTrue(size >= 1);
        m.expireEvict(); // share 0 => full drain
        assertTrue(m.isEmpty());
        store.verify();
        store.close();
    }

    /** Stamps are persisted as packed longs: a clock+ttl overflow (or negative clock)
     *  is rejected up front instead of corrupting the queue wire format. */
    @Test
    public void stampOverflowAndNegativeClockRejected() {
        Store store = new StoreOnHeap();
        HTreeCacheTCK.FakeClock clock = new HTreeCacheTCK.FakeClock();
        HTreeCache<Long, Long> overflow = singleSegment(store, Long.MAX_VALUE - 5, clock);
        try {
            overflow.put(1L, 1L); // 1_000_000 + (MAX-5) overflows
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) { /* ok */ }
        assertTrue(overflow.isEmpty()); // nothing half-inserted

        HTreeCacheTCK.FakeClock negative = new HTreeCacheTCK.FakeClock();
        negative.now = -5;
        HTreeCache<Long, Long> negClock = singleSegment(store, 1_000, negative);
        try {
            negClock.put(1L, 1L);
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) { /* ok */ }
        store.verify();
        store.close();
    }
}
