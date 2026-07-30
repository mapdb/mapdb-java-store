package org.mapdb.queue;

import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.mapdb.DBException;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;
import org.mapdb.ser.Serializer;
import org.mapdb.store.Store;

/**
 * Store-backed FIFO, LIFO stack, or overwrite-on-full circular queue.
 * Use one writable handle per persisted header. DB enforces that through its per-name handle
 * cache; direct callers must not open the same header twice concurrently because locks and
 * blocking conditions are handle-local.
 */
public final class PersistentBlockingQueue<E> extends AbstractQueue<E>
        implements BlockingQueue<E> {

    public enum Mode { FIFO, LIFO, CIRCULAR }

    private static final class Header {
        final int mode;
        final long head, tail, size, capacity;
        Header(int mode, long head, long tail, long size, long capacity) {
            this.mode = mode; this.head = head; this.tail = tail;
            this.size = size; this.capacity = capacity;
        }
        static final Serializer<Header> SERIALIZER = new Serializer<>() {
            @Override public void serialize(DataOutput2 out, Header h) {
                out.packInt(h.mode); out.packLong(h.head); out.packLong(h.tail);
                out.packLong(h.size); out.packLong(h.capacity);
            }
            @Override public Header deserialize(DataInput2 in, int size) {
                return new Header(in.unpackInt(), in.unpackLong(), in.unpackLong(),
                        in.unpackLong(), in.unpackLong());
            }
        };
    }

    private static final class Node<E> {
        final long next;
        final E value;
        Node(long next, E value) { this.next = next; this.value = value; }
    }

    private static final class NodeSerializer<E> implements Serializer<Node<E>> {
        private final Serializer<E> serializer;
        NodeSerializer(Serializer<E> serializer) { this.serializer = serializer; }
        @Override public void serialize(DataOutput2 out, Node<E> node) {
            out.packLong(node.next);
            serializer.serialize(out, node.value);
        }
        @Override public Node<E> deserialize(DataInput2 in, int size) {
            return new Node<>(in.unpackLong(), serializer.deserialize(in, -1));
        }
    }

    private final Store store;
    private final long headerRecid;
    private final Serializer<E> serializer;
    private final NodeSerializer<E> nodeSerializer;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final Condition notFull = lock.newCondition();
    private volatile boolean handleClosed;

    private PersistentBlockingQueue(Store store, long headerRecid, Serializer<E> serializer) {
        this.store = java.util.Objects.requireNonNull(store, "store");
        this.headerRecid = headerRecid;
        this.serializer = java.util.Objects.requireNonNull(serializer, "serializer");
        this.nodeSerializer = new NodeSerializer<>(serializer);
        Header h = header();
        if (h.mode < 0 || h.mode >= Mode.values().length || h.capacity <= 0 || h.size < 0)
            throw new DBException.DataCorruption("invalid persistent queue header");
    }

    public static <E> PersistentBlockingQueue<E> create(
            Store store, Serializer<E> serializer, Mode mode, long capacity) {
        if (mode == null) throw new NullPointerException("mode");
        long actualCapacity = mode == Mode.CIRCULAR ? capacity : Long.MAX_VALUE;
        if (actualCapacity <= 0) throw new IllegalArgumentException("capacity must be > 0");
        long headerRecid = store.put(new Header(mode.ordinal(), 0, 0, 0, actualCapacity),
                Header.SERIALIZER);
        return new PersistentBlockingQueue<>(store, headerRecid, serializer);
    }

    public static <E> PersistentBlockingQueue<E> open(
            Store store, long headerRecid, Serializer<E> serializer) {
        return new PersistentBlockingQueue<>(store, headerRecid, serializer);
    }

    /** Wake blocked operations without closing the shared store. Used by DB lifecycle teardown. */
    public void closeHandle() {
        lock.lock();
        try {
            handleClosed = true;
            notEmpty.signalAll();
            notFull.signalAll();
        } finally { lock.unlock(); }
    }

    public long headerRecid() { return headerRecid; }
    public Mode mode() { return Mode.values()[header().mode]; }
    public Serializer<E> serializer() { return serializer; }

    private Header header() {
        if (handleClosed) throw new DBException.StoreClosed();
        Header h = store.get(headerRecid, Header.SERIALIZER);
        if (h == null) throw new DBException.DataCorruption("queue header missing: " + headerRecid);
        return h;
    }

    private Node<E> node(long recid) {
        Node<E> n = store.get(recid, nodeSerializer);
        if (n == null) throw new DBException.DataCorruption("queue node missing: " + recid);
        return n;
    }

    private boolean full(Header h) { return h.size >= h.capacity; }

    private void enqueue(Header h, E value) {
        Mode mode = Mode.values()[h.mode];
        if (mode == Mode.CIRCULAR && full(h)) h = dequeue(h, null);
        if (mode == Mode.LIFO) {
            long recid = store.put(new Node<>(h.head, value), nodeSerializer);
            store.update(headerRecid, new Header(h.mode, recid,
                    h.size == 0 ? recid : h.tail, h.size + 1, h.capacity), Header.SERIALIZER);
        } else {
            long recid = store.put(new Node<>(0, value), nodeSerializer);
            if (h.tail != 0) {
                Node<E> tail = node(h.tail);
                store.update(h.tail, new Node<>(recid, tail.value), nodeSerializer);
            }
            store.update(headerRecid, new Header(h.mode, h.size == 0 ? recid : h.head,
                    recid, h.size + 1, h.capacity), Header.SERIALIZER);
        }
    }

    /** Removes the head and optionally stores its value in slot[0]. */
    private Header dequeue(Header h, Object[] slot) {
        if (h.size == 0) return h;
        Node<E> n = node(h.head);
        Header next = new Header(h.mode, n.next, h.size == 1 ? 0 : h.tail,
                h.size - 1, h.capacity);
        store.update(headerRecid, next, Header.SERIALIZER);
        store.delete(h.head, nodeSerializer);
        if (slot != null) slot[0] = n.value;
        return next;
    }

    @Override public boolean offer(E value) {
        if (value == null) throw new NullPointerException("value");
        lock.lock();
        try {
            Header h = header();
            if (full(h) && Mode.values()[h.mode] != Mode.CIRCULAR) return false;
            enqueue(h, value);
            notEmpty.signal();
            return true;
        } finally { lock.unlock(); }
    }

    @Override public void put(E value) throws InterruptedException {
        if (value == null) throw new NullPointerException("value");
        lock.lockInterruptibly();
        try {
            Header h = header();
            while (full(h) && Mode.values()[h.mode] != Mode.CIRCULAR) {
                notFull.await(); h = header();
            }
            enqueue(h, value); notEmpty.signal();
        } finally { lock.unlock(); }
    }

    @Override public boolean offer(E value, long timeout, TimeUnit unit) throws InterruptedException {
        if (value == null) throw new NullPointerException("value");
        long nanos = unit.toNanos(timeout);
        lock.lockInterruptibly();
        try {
            Header h = header();
            while (full(h) && Mode.values()[h.mode] != Mode.CIRCULAR) {
                if (nanos <= 0) return false;
                nanos = notFull.awaitNanos(nanos); h = header();
            }
            enqueue(h, value); notEmpty.signal(); return true;
        } finally { lock.unlock(); }
    }

    @SuppressWarnings("unchecked")
    @Override public E poll() {
        lock.lock();
        try {
            Header h = header();
            if (h.size == 0) return null;
            Object[] slot = new Object[1];
            dequeue(h, slot); notFull.signal();
            return (E) slot[0];
        } finally { lock.unlock(); }
    }

    @Override public E take() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (header().size == 0) {
                if (handleClosed) throw new DBException.StoreClosed();
                notEmpty.await();
            }
            return removeHeadLocked();
        } finally { lock.unlock(); }
    }

    @Override public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        long nanos = unit.toNanos(timeout);
        lock.lockInterruptibly();
        try {
            while (header().size == 0) {
                if (handleClosed) throw new DBException.StoreClosed();
                if (nanos <= 0) return null;
                nanos = notEmpty.awaitNanos(nanos);
            }
            return removeHeadLocked();
        } finally { lock.unlock(); }
    }

    @SuppressWarnings("unchecked")
    private E removeHeadLocked() {
        Object[] slot = new Object[1];
        dequeue(header(), slot); notFull.signal();
        return (E) slot[0];
    }

    @Override public E peek() {
        lock.lock();
        try {
            Header h = header();
            return h.size == 0 ? null : node(h.head).value;
        } finally { lock.unlock(); }
    }

    @Override public int size() {
        lock.lock();
        try { return (int) Math.min(Integer.MAX_VALUE, header().size); }
        finally { lock.unlock(); }
    }

    public long sizeLong() {
        lock.lock();
        try { return header().size; }
        finally { lock.unlock(); }
    }

    @Override public int remainingCapacity() {
        lock.lock();
        try {
            Header h = header();
            long left = h.capacity - h.size;
            return (int) Math.min(Integer.MAX_VALUE, left);
        } finally { lock.unlock(); }
    }

    @Override public Iterator<E> iterator() {
        lock.lock();
        try {
            Header h = header();
            List<E> snapshot = new ArrayList<>((int) Math.min(Integer.MAX_VALUE, h.size));
            long recid = h.head;
            while (recid != 0) {
                Node<E> n = node(recid); snapshot.add(n.value); recid = n.next;
            }
            Iterator<E> delegate = snapshot.iterator();
            return new Iterator<>() {
                E current;
                boolean removable;
                @Override public boolean hasNext() { return delegate.hasNext(); }
                @Override public E next() { current = delegate.next(); removable = true; return current; }
                @Override public void remove() {
                    if (!removable) throw new IllegalStateException();
                    PersistentBlockingQueue.this.remove(current);
                    removable = false;
                }
            };
        } finally { lock.unlock(); }
    }

    @Override public boolean contains(Object value) {
        if (value == null) return false;
        lock.lock();
        try {
            for (long recid = header().head; recid != 0;) {
                Node<E> n = node(recid);
                if (java.util.Objects.equals(n.value, value)) return true;
                recid = n.next;
            }
            return false;
        } finally { lock.unlock(); }
    }

    @Override public boolean remove(Object value) {
        if (value == null) return false;
        lock.lock();
        try {
            Header h = header();
            long previousRecid = 0, recid = h.head;
            while (recid != 0) {
                Node<E> n = node(recid);
                if (java.util.Objects.equals(n.value, value)) {
                    if (previousRecid == 0) {
                        store.update(headerRecid, new Header(h.mode, n.next,
                                h.size == 1 ? 0 : h.tail, h.size - 1, h.capacity), Header.SERIALIZER);
                    } else {
                        Node<E> previous = node(previousRecid);
                        store.update(previousRecid, new Node<>(n.next, previous.value), nodeSerializer);
                        store.update(headerRecid, new Header(h.mode, h.head,
                                h.tail == recid ? previousRecid : h.tail, h.size - 1, h.capacity),
                                Header.SERIALIZER);
                    }
                    store.delete(recid, nodeSerializer);
                    notFull.signal();
                    return true;
                }
                previousRecid = recid;
                recid = n.next;
            }
            return false;
        } finally { lock.unlock(); }
    }

    @Override public int drainTo(Collection<? super E> target) {
        return drainTo(target, Integer.MAX_VALUE);
    }

    @Override public int drainTo(Collection<? super E> target, int maxElements) {
        if (target == null) throw new NullPointerException("target");
        if (target == this) throw new IllegalArgumentException("cannot drain to self");
        if (maxElements <= 0) return 0;
        lock.lock();
        try {
            int count = 0;
            while (count < maxElements && header().size != 0) {
                target.add(removeHeadLocked()); count++;
            }
            return count;
        } finally { lock.unlock(); }
    }

    @Override public void clear() {
        lock.lock();
        try {
            Header h = header();
            while (h.size != 0) h = dequeue(h, null);
            notFull.signalAll();
        } finally { lock.unlock(); }
    }

    public void verify() {
        lock.lock();
        try {
            Header h = header();
            long count = 0, recid = h.head, last = 0;
            while (recid != 0) {
                if (++count > h.size) throw new DBException.VerifyFailed("queue cycle/size mismatch");
                last = recid; recid = node(recid).next;
            }
            if (count != h.size || (count == 0 ? h.tail != 0 : h.tail != last))
                throw new DBException.VerifyFailed("queue header/link mismatch");
        } finally { lock.unlock(); }
    }
}
