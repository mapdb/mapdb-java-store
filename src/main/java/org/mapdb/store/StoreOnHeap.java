package org.mapdb.store;

import org.mapdb.DBException;
import org.mapdb.ser.Serializer;

import java.util.ArrayDeque;
import java.util.PrimitiveIterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * Heap store: records are live objects, never serialized.
 * read() dispatches onObject/onNull — never onBytes.
 * Does not implement StoreDelta: heap records are permanently "materialized",
 * delta semantics are meaningless here (revisited by the hybrid cache).
 *
 * Reads (get/read) are LOCK-FREE: records live in a ConcurrentHashMap and are
 * handed out as-is — per-recid atomicity is the map entry's, and record objects
 * are immutable by the collection-layer contract, so there are no
 * torn states to guard against (the heap analogue of the volume-backed torn-read guard; measurements
 * flagged the old global-lock reads as regressing past 8 threads). Writes keep
 * the store-global write lock: allocation (maxRecid/freeRecids) and map mutation
 * must stay atomic per op.
 */
public class StoreOnHeap implements Store {

    /** Map sentinel for explicit null content (map values must be non-null). */
    private static final Object NULL = new Object();
    /** Map sentinel for preallocated records. */
    private static final Object PREALLOC = new Object();

    private final ConcurrentHashMap<Long, Object> records = new ConcurrentHashMap<>();
    private final ArrayDeque<Long> freeRecids = new ArrayDeque<>();
    private long maxRecid = 0;
    private volatile boolean closed = false;

    private final boolean threadSafe;
    private final ReadWriteLock lock;
    private final DeadlockAsserts asserts = new DeadlockAsserts();

    public StoreOnHeap() { this(true); }

    public StoreOnHeap(boolean threadSafe) {
        this.threadSafe = threadSafe;
        // single-entry StampedLock view: store locks are never held reentrantly
        this.lock = asserts.segment(
                threadSafe ? new java.util.concurrent.locks.StampedLock().asReadWriteLock() : Locks.NO_OP_RW_LOCK);
    }

    @Override public boolean isThreadSafe() { return threadSafe; }

    private void checkClosed() {
        if (closed) throw new DBException.StoreClosed();
    }

    private Object checkExists(long recid) {
        Object o = records.get(recid);
        if (o == null) throw new DBException.GetVoid(recid);
        return o;
    }

    private long allocRecid() {
        Long free = freeRecids.pollLast();
        return free != null ? free : ++maxRecid;
    }

    @Override public long preallocate() {
        lock.writeLock().lock();
        try {
            checkClosed();
            long recid = allocRecid();
            records.put(recid, PREALLOC);
            return recid;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override public <R> long put(R record, Serializer<R> serializer) {
        lock.writeLock().lock();
        try {
            checkClosed();
            long recid = allocRecid();
            records.put(recid, record == null ? NULL : record);
            return recid;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @SuppressWarnings("unchecked")
    @Override public <R> R get(long recid, Serializer<R> serializer) {
        checkClosed();
        // no user code runs here, but marking the read keeps A3 detection alive:
        // a get() from inside an action/serializer callback trips the assert even
        // though the lock-free path acquires nothing (-ea only, free in production)
        asserts.enterAction();
        try {
            Object o = checkExists(recid);
            return (o == NULL || o == PREALLOC) ? null : (R) o;
        } finally {
            asserts.exitAction();
        }
    }

    @Override public long read(long recid, RecordRead action) {
        checkClosed();
        Object o = checkExists(recid);
        // enterAction keeps A3 detection (no callbacks into the store) alive on the
        // lock-free path under -ea; free in production
        asserts.enterAction();
        try {
            return (o == NULL || o == PREALLOC) ? action.onNull() : action.onObject(o);
        } finally {
            asserts.exitAction();
        }
    }

    @Override public <R> void update(long recid, R record, Serializer<R> serializer) {
        lock.writeLock().lock();
        try {
            checkClosed();
            checkExists(recid);
            records.put(recid, record == null ? NULL : record);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @SuppressWarnings("unchecked")
    @Override public <R> boolean compareAndSwap(long recid, R expectedOldRecord, R newRecord, Serializer<R> serializer) {
        lock.writeLock().lock();
        try {
            checkClosed();
            Object o = checkExists(recid);
            R current = (o == NULL || o == PREALLOC) ? null : (R) o;
            boolean eq = (current == null && expectedOldRecord == null)
                    || (current != null && expectedOldRecord != null && serializer.equals(current, expectedOldRecord));
            if (!eq) return false;
            records.put(recid, newRecord == null ? NULL : newRecord);
            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override public <R> void delete(long recid, Serializer<R> serializer) {
        lock.writeLock().lock();
        try {
            checkClosed();
            checkExists(recid);
            records.remove(recid);
            freeRecids.addLast(recid);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override public void commit() {
        checkClosed();
        // no-op: heap store has no durability
    }

    @Override public void close() {
        lock.writeLock().lock();
        try {
            closed = true;
            records.clear();
            freeRecids.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override public boolean isClosed() { return closed; }

    @Override public void verify() {
        lock.readLock().lock();
        try {
            checkClosed();
            for (Long free : freeRecids) {
                if (records.containsKey(free))
                    throw new DBException.VerifyFailed("free recid is live: " + free);
                if (free > maxRecid)
                    throw new DBException.VerifyFailed("free recid beyond maxRecid: " + free);
            }
            for (Long recid : records.keySet()) {
                if (recid < 1 || recid > maxRecid)
                    throw new DBException.VerifyFailed("recid out of range: " + recid);
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override public PrimitiveIterator.OfLong getAllRecids() {
        lock.readLock().lock();
        try {
            checkClosed();
            return records.entrySet().stream()
                    .filter(e -> e.getValue() != PREALLOC)
                    .mapToLong(java.util.Map.Entry::getKey)
                    .sorted()
                    .iterator();
        } finally {
            lock.readLock().unlock();
        }
    }
}
