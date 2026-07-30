package org.mapdb.store;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * No-op locks for {@code isThreadSafe=false} stores (mapdb3 pattern —
 * mapdb3 used null locks; a shared no-op instance keeps call sites branch-free).
 */
final class Locks {

    private Locks() {}

    static final Lock NO_OP_LOCK = new Lock() {
        @Override public void lock() {}
        @Override public void lockInterruptibly() {}
        @Override public boolean tryLock() { return true; }
        @Override public boolean tryLock(long time, TimeUnit unit) { return true; }
        @Override public void unlock() {}
        @Override public Condition newCondition() { throw new UnsupportedOperationException(); }
    };

    static final ReadWriteLock NO_OP_RW_LOCK = new ReadWriteLock() {
        @Override public Lock readLock() { return NO_OP_LOCK; }
        @Override public Lock writeLock() { return NO_OP_LOCK; }
    };
}
