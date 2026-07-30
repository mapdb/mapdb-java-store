package org.mapdb.store;

import org.mapdb.DBException;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;
import org.mapdb.ser.Serializer;

import java.util.PrimitiveIterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * Reference implementation of StoreDelta: one byte[] per record,
 * buf.length is the capacity, used is the content length. Everything explicit —
 * this store doubles as the fuzz oracle for StoreDirect/StoreWAL.
 *
 * Capacity model: put() provisions zero headroom; updateWithHeadroom() provisions
 * explicitly; the first append to a record that never had capacity provisioned
 * (preallocated or null with empty buf) establishes the record with capacity == len.
 */
public class StoreByteArray implements StoreDelta {

    private static final byte LIVE = 0, NULLC = 1, PREALLOC = 2;
    private static final byte[] EMPTY = new byte[0];

    private static final class Rec {
        byte[] buf;
        int used;
        byte state;

        Rec(byte[] buf, int used, byte state) {
            this.buf = buf;
            this.used = used;
            this.state = state;
        }
    }

    private final ConcurrentHashMap<Long, Rec> records = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<Long> freeRecids = new ConcurrentLinkedQueue<>();
    private final AtomicLong maxRecid = new AtomicLong(0);
    private final boolean threadSafe;
    private final SegmentLocks segs;
    private volatile boolean closed = false;

    public StoreByteArray() { this(true); }

    public StoreByteArray(boolean threadSafe) {
        this.threadSafe = threadSafe;
        this.segs = new SegmentLocks(SegmentLocks.DEFAULT_COUNT, threadSafe, new DeadlockAsserts());
    }

    @Override public boolean isThreadSafe() { return threadSafe; }

    private void checkClosed() {
        if (closed) throw new DBException.StoreClosed();
    }

    private Rec checkExists(long recid) {
        Rec r = records.get(recid);
        if (r == null) throw new DBException.GetVoid(recid);
        return r;
    }

    private long allocRecid() {
        Long free = freeRecids.poll();
        return free != null ? free : maxRecid.incrementAndGet();
    }

    private <R> Rec newRec(R record, Serializer<R> ser, int headroom) {
        if (record == null) {
            return new Rec(headroom == 0 ? EMPTY : new byte[headroom], 0, NULLC);
        }
        DataOutput2 out = new DataOutput2(Math.max(16, ser.sizeHint()));
        ser.serialize(out, record);
        byte[] buf = new byte[out.pos + headroom];
        System.arraycopy(out.buf, 0, buf, 0, out.pos);
        return new Rec(buf, out.pos, LIVE);
    }

    @Override public long preallocate() {
        checkClosed();
        long recid = allocRecid();
        records.put(recid, new Rec(EMPTY, 0, PREALLOC));
        return recid;
    }

    @Override public <R> long put(R record, Serializer<R> serializer) {
        checkClosed();
        Rec rec = newRec(record, serializer, 0);
        long recid = allocRecid();
        records.put(recid, rec);
        return recid;
    }

    @Override public <R> R get(long recid, Serializer<R> serializer) {
        ReadWriteLock lock = segs.forRecid(recid);
        lock.readLock().lock();
        try {
            checkClosed();
            Rec r = checkExists(recid);
            if (r.state != LIVE) return null;
            return serializer.deserialize(new DataInput2.ByteArray(r.buf, 0), r.used);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override public long read(long recid, RecordRead action) {
        ReadWriteLock lock = segs.forRecid(recid);
        lock.readLock().lock();
        try {
            checkClosed();
            Rec r = checkExists(recid);
            if (r.state != LIVE) return action.onNull();
            return action.onBytes(new DataInput2.ByteArray(r.buf, 0), r.used);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override public <R> void update(long recid, R record, Serializer<R> serializer) {
        updateWithHeadroom(recid, record, serializer, 0);
    }

    @Override public <R> void updateWithHeadroom(long recid, R record, Serializer<R> serializer, int headroom) {
        Rec newRec = newRec(record, serializer, headroom);
        ReadWriteLock lock = segs.forRecid(recid);
        lock.writeLock().lock();
        try {
            checkClosed();
            checkExists(recid);
            records.put(recid, newRec);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override public <R> boolean compareAndSwap(long recid, R expectedOldRecord, R newRecord, Serializer<R> serializer) {
        ReadWriteLock lock = segs.forRecid(recid);
        lock.writeLock().lock();
        try {
            checkClosed();
            Rec r = checkExists(recid);
            R current = r.state != LIVE ? null
                    : serializer.deserialize(new DataInput2.ByteArray(r.buf, 0), r.used);
            boolean eq = (current == null && expectedOldRecord == null)
                    || (current != null && expectedOldRecord != null && serializer.equals(current, expectedOldRecord));
            if (!eq) return false;
            records.put(recid, newRec(newRecord, serializer, 0));
            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override public <R> void delete(long recid, Serializer<R> serializer) {
        ReadWriteLock lock = segs.forRecid(recid);
        lock.writeLock().lock();
        try {
            checkClosed();
            checkExists(recid);
            records.remove(recid);
            freeRecids.add(recid);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override public long append(long recid, byte[] data, int offset, int len) {
        ReadWriteLock lock = segs.forRecid(recid);
        lock.writeLock().lock();
        try {
            checkClosed();
            Rec r = checkExists(recid);
            if (r.used + len > r.buf.length) {
                boolean neverProvisioned = r.state != LIVE && r.buf.length == 0;
                if (!neverProvisioned) return StoreDelta.REFUSED;
                // first append establishes the record: capacity == len
                r.buf = new byte[len];
            }
            System.arraycopy(data, offset, r.buf, r.used, len);
            r.used += len;
            r.state = LIVE;
            return r.used;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override public long capacityRemaining(long recid) {
        ReadWriteLock lock = segs.forRecid(recid);
        lock.readLock().lock();
        try {
            checkClosed();
            Rec r = checkExists(recid);
            return r.buf.length - r.used;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override public void commit() {
        checkClosed();
        // no-op: in-memory store
    }

    @Override public void close() {
        closed = true;
        records.clear();
        freeRecids.clear();
    }

    @Override public boolean isClosed() { return closed; }

    @Override public void verify() {
        checkClosed();
        long max = maxRecid.get();
        for (var e : records.entrySet()) {
            long recid = e.getKey();
            Rec r = e.getValue();
            if (recid < 1 || recid > max)
                throw new DBException.VerifyFailed("recid out of range: " + recid);
            if (r.used < 0 || r.used > r.buf.length)
                throw new DBException.VerifyFailed("used beyond capacity, recid=" + recid);
        }
        for (Long free : freeRecids) {
            if (records.containsKey(free))
                throw new DBException.VerifyFailed("free recid is live: " + free);
        }
    }

    @Override public PrimitiveIterator.OfLong getAllRecids() {
        checkClosed();
        return records.entrySet().stream()
                .filter(e -> e.getValue().state != PREALLOC)
                .mapToLong(java.util.Map.Entry::getKey)
                .sorted()
                .iterator();
    }
}
