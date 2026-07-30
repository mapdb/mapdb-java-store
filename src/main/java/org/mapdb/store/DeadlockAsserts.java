package org.mapdb.store;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * Runtime deadlock/reentrancy assertions. One instance
 * per store; wraps that store's locks and tracks per-thread lock state:
 *
 * <ul>
 *   <li>at most ONE segment (or store-global) lock held per thread per store —
 *       catches RecordRead actions and Serializers calling back into the store
 *       (forbidden): those callbacks always run under a lock;</li>
 *   <li>never segment-after-structural — the lock order is segment → structural;</li>
 *   <li>the structural lock is not reentrant;</li>
 *   <li>unlock of a lock that is not held fails with a clear message.</li>
 * </ul>
 *
 * Active only when JVM assertions are enabled (-ea); otherwise locks are returned
 * unwrapped and there is zero overhead. No-op locks (isThreadSafe=false) are wrapped
 * too, so single-threaded mode still catches A3 violations.
 *
 * Cross-store nesting (StoreWAL holding its lock while calling the inner StoreDirect)
 * is legal and untouched: each store has its own instance and thread state.
 */
final class DeadlockAsserts {

    static final boolean ENABLED = DeadlockAsserts.class.desiredAssertionStatus();

    private static final class State {
        boolean segment;
        boolean structural;
    }

    private final ThreadLocal<State> state = ThreadLocal.withInitial(State::new);

    /** Wrap a segment (or store-global) read-write lock. */
    ReadWriteLock segment(ReadWriteLock inner) {
        if (!ENABLED) return inner;
        Lock r = new Guard(inner.readLock(), true);
        Lock w = new Guard(inner.writeLock(), true);
        return new ReadWriteLock() {
            @Override public Lock readLock() { return r; }
            @Override public Lock writeLock() { return w; }
        };
    }

    /** Wrap the structural (allocator) lock. */
    Lock structural(Lock inner) {
        return ENABLED ? new Guard(inner, false) : inner;
    }

    /**
     * Mark an OPTIMISTIC (lock-free) action execution, so A3 detection (actions must
     * not call back into the store) keeps working on paths that hold no lock at all.
     * Same thread-state as a segment lock: a callback that acquires
     * any lock of this store — or runs another optimistic action — trips the assert.
     * No-ops (and is JIT-eliminated) when -ea is off. Call exitAction from a finally
     * block ONLY if enterAction returned normally.
     */
    void enterAction() {
        if (!ENABLED) return;
        State s = state.get();
        if (s.segment) throw new AssertionError(
                "reentrant store access: this thread already holds a segment lock / runs a read action of this store — "
                + "a RecordRead action or Serializer likely called back into the store (forbidden)");
        if (s.structural) throw new AssertionError(
                "lock-order violation: running a read action while holding the structural lock");
        s.segment = true;
    }

    void exitAction() {
        if (!ENABLED) return;
        state.get().segment = false;
    }

    private final class Guard implements Lock {
        private final Lock inner;
        private final boolean isSegment;

        Guard(Lock inner, boolean isSegment) {
            this.inner = inner;
            this.isSegment = isSegment;
        }

        private void beforeAcquire() {
            State s = state.get();
            if (isSegment) {
                if (s.segment) throw new AssertionError(
                        "reentrant store access: this thread already holds a segment lock of this store — "
                        + "a RecordRead action or Serializer likely called back into the store (forbidden)");
                if (s.structural) throw new AssertionError(
                        "lock-order violation: acquiring a segment lock while holding the structural lock "
                        + "(order is segment -> structural)");
            } else {
                if (s.structural) throw new AssertionError("structural lock is not reentrant");
            }
        }

        private void markAcquired() {
            State s = state.get();
            if (isSegment) s.segment = true;
            else s.structural = true;
        }

        @Override public void lock() {
            beforeAcquire();
            inner.lock();
            markAcquired();
        }

        @Override public void lockInterruptibly() throws InterruptedException {
            beforeAcquire();
            inner.lockInterruptibly();
            markAcquired();
        }

        @Override public boolean tryLock() {
            beforeAcquire();
            boolean ok = inner.tryLock();
            if (ok) markAcquired();
            return ok;
        }

        @Override public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            beforeAcquire();
            boolean ok = inner.tryLock(time, unit);
            if (ok) markAcquired();
            return ok;
        }

        @Override public void unlock() {
            State s = state.get();
            if (isSegment) {
                if (!s.segment) throw new AssertionError("unlock of a segment lock this thread does not hold");
            } else {
                if (!s.structural) throw new AssertionError("unlock of the structural lock this thread does not hold");
            }
            inner.unlock();
            if (isSegment) s.segment = false;
            else s.structural = false;
        }

        @Override public Condition newCondition() { return inner.newCondition(); }
    }
}
