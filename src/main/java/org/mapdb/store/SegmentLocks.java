package org.mapdb.store;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.StampedLock;

/**
 * Segment lock template: 2^k read-write locks, recid mapped by low bits.
 * Lock order elsewhere: segment lock before structural lock, never the reverse —
 * enforced at runtime by {@link DeadlockAsserts} when -ea is on.
 *
 * Locks are single-entry {@link StampedLock}s (via asReadWriteLock views): store locks
 * are never held reentrantly, so we skip ReentrantReadWriteLock's per-thread read-hold
 * ThreadLocal bookkeeping entirely. StampedLock does not *detect*
 * reentrancy — DeadlockAsserts does, under -ea, at zero production cost.
 *
 * With {@code threadSafe=false} every slot is the shared no-op lock.
 */
final class SegmentLocks {

    /**
     * Default segment count: a power of two ≈ 2×cores of the target
     * hardware class. mapdb3's fixed 8 was measurably too low on a 32-core box —
     * a 100%-read workload physically serializes on 8 lock cache lines (measured:
     * getHit collapses past 8 threads). Fixed (not computed from availableProcessors)
     * so behavior is deterministic across machines; the cost of unused segments is a
     * few KB per store.
     */
    static final int DEFAULT_COUNT = 64;

    /**
     * StampedLock padded so adjacent locks' hot state words never share a 64-byte
     * cache line (locks allocated in one loop land adjacent in the
     * TLAB). Subclass fields are laid out after the parent's, so the 64 pad bytes
     * push the next object's state word at least a full line away.
     */
    private static final class PaddedStampedLock extends StampedLock {
        @SuppressWarnings("unused") volatile long p0, p1, p2, p3, p4, p5, p6, p7;
    }

    final ReadWriteLock[] locks;
    /** Raw StampedLocks backing {@link #locks}; null when threadSafe=false (no-op locks). */
    private final StampedLock[] stamped;
    private final int mask;

    SegmentLocks(int count, boolean threadSafe, DeadlockAsserts asserts) {
        if (Integer.bitCount(count) != 1) throw new IllegalArgumentException("segment count must be power of two");
        locks = new ReadWriteLock[count];
        stamped = threadSafe ? new StampedLock[count] : null;
        for (int i = 0; i < count; i++) {
            if (threadSafe) {
                stamped[i] = new PaddedStampedLock();
                locks[i] = asserts.segment(stamped[i].asReadWriteLock());
            } else {
                locks[i] = asserts.segment(Locks.NO_OP_RW_LOCK);
            }
        }
        mask = count - 1;
    }

    ReadWriteLock forRecid(long recid) {
        return locks[(int) (recid & mask)];
    }

    /**
     * Raw StampedLock for the recid's segment, for {@code tryOptimisticRead} paths;
     * null when the store is not thread-safe (optimistic reads
     * are pointless without concurrency — callers use the locked path).
     */
    StampedLock stampedForRecid(long recid) {
        return stamped == null ? null : stamped[(int) (recid & mask)];
    }
}
