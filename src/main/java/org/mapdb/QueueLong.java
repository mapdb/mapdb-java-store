package org.mapdb;

import java.io.PrintStream;
import java.util.Arrays;

import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;
import org.mapdb.ser.Serializer;
import org.mapdb.ser.Serializers;
import org.mapdb.store.Store;
import org.mapdb.store.StoreOnHeap;

/**
 * Persistent FIFO of {@code (timestamp,value)} long pairs with O(1) removal and bump by node recid.
 * The three pointer recids make a queue reopenable without a separate header object.
 * Operations are synchronized per QueueLong handle; do not open two writable handles over the
 * same pointer recids concurrently.
 */
public final class QueueLong {
    public static final class Node {
        public final long prevRecid, nextRecid, timestamp, value;

        /**
         * Rejects a negative {@code timestamp} or {@code value} (as well as recids). This is
         * STRICTER than MapDB 3, whose QueueLong.Node accepted negative timestamps/values; the
         * fields are packed as unsigned longs here, so callers must supply non-negative values.
         */
        public Node(long prevRecid, long nextRecid, long timestamp, long value) {
            if (prevRecid < 0 || nextRecid < 0 || timestamp < 0 || value < 0)
                throw new IllegalArgumentException("QueueLong node fields must be non-negative");
            this.prevRecid = prevRecid;
            this.nextRecid = nextRecid;
            this.timestamp = timestamp;
            this.value = value;
        }

        Node withPrev(long recid) { return new Node(recid, nextRecid, timestamp, value); }
        Node withNext(long recid) { return new Node(prevRecid, recid, timestamp, value); }
        Node withLinksAndTimestamp(long prev, long next, long time) {
            return new Node(prev, next, time, value);
        }

        public static final Serializer<Node> SERIALIZER = new Serializer<>() {
            @Override public void serialize(DataOutput2 out, Node node) {
                Serializers.LONG_PACKED.serialize(out, node.prevRecid);
                Serializers.LONG_PACKED.serialize(out, node.nextRecid);
                out.packLong(node.timestamp);
                out.packLong(node.value);
            }

            @Override public Node deserialize(DataInput2 in, int size) {
                return new Node(Serializers.LONG_PACKED.deserialize(in, -1),
                        Serializers.LONG_PACKED.deserialize(in, -1), in.unpackLong(), in.unpackLong());
            }
        };
    }

    private final Store store;
    private final long tailRecid, headRecid, headPrevRecid;

    public QueueLong(Store store, long tailRecid, long headRecid, long headPrevRecid) {
        this.store = java.util.Objects.requireNonNull(store, "store");
        this.tailRecid = tailRecid;
        this.headRecid = headRecid;
        this.headPrevRecid = headPrevRecid;
        if (tailRecid == headRecid) throw new IllegalArgumentException("tailRecid == headRecid");
    }

    public static QueueLong make() { return make(new StoreOnHeap(true)); }

    public static QueueLong make(Store store) {
        long sentinel = store.preallocate();
        long tail = store.put(sentinel, Serializers.LONG_PACKED);
        long head = store.put(sentinel, Serializers.LONG_PACKED);
        long headPrev = store.put(0L, Serializers.LONG_PACKED);
        return new QueueLong(store, tail, head, headPrev);
    }

    public Store store() { return store; }
    public long tailRecid() { return tailRecid; }
    public long headRecid() { return headRecid; }
    public long headPrevRecid() { return headPrevRecid; }
    public long tail() { return requiredPointer(tailRecid); }
    public long head() { return requiredPointer(headRecid); }
    public long headPrev() { return requiredPointer(headPrevRecid); }

    private long requiredPointer(long recid) {
        Long value = store.get(recid, Serializers.LONG_PACKED);
        if (value == null) throw corrupt("missing queue pointer " + recid);
        return value;
    }
    private void tail(long value) { store.update(tailRecid, value, Serializers.LONG_PACKED); }
    private void head(long value) { store.update(headRecid, value, Serializers.LONG_PACKED); }
    private void headPrev(long value) { store.update(headPrevRecid, value, Serializers.LONG_PACKED); }
    private static DBException.DataCorruption corrupt(String message) {
        return new DBException.DataCorruption(message);
    }

    /** Append and return the recid representing the new node. */
    public synchronized long put(long timestamp, long value) {
        long next = store.preallocate();
        long oldHead = head();
        long oldPrev = headPrev();
        store.update(oldHead, new Node(oldPrev, next, timestamp, value), Node.SERIALIZER);
        head(next);
        headPrev(oldHead);
        return oldHead;
    }

    /** Insert a caller-preallocated node at the queue head. */
    public synchronized void put(long timestamp, long value, long nodeRecid) {
        long prev = headPrev();
        long sentinel = head();
        store.update(nodeRecid, new Node(prev, sentinel, timestamp, value), Node.SERIALIZER);
        headPrev(nodeRecid);
        if (prev != 0) {
            Node previous = store.get(prev, Node.SERIALIZER);
            if (previous == null) throw corrupt("previous node not found: " + prev);
            store.update(prev, previous.withNext(nodeRecid), Node.SERIALIZER);
        }
        if (tail() == sentinel) tail(nodeRecid);
    }

    /** Remove and return the oldest node, or null when empty. */
    public synchronized Node take() {
        long oldTail = tail();
        Node node = store.get(oldTail, Node.SERIALIZER);
        if (node == null) {
            headPrev(0L);
            return null;
        }
        store.delete(oldTail, Node.SERIALIZER);
        tail(node.nextRecid);
        store.compareAndSwap(headPrevRecid, oldTail, 0L, Serializers.LONG_PACKED);
        Node next = store.get(node.nextRecid, Node.SERIALIZER);
        if (next != null) store.update(node.nextRecid, next.withPrev(0L), Node.SERIALIZER);
        return node;
    }

    /** Consume oldest nodes while the callback returns true. */
    public synchronized void takeUntil(QueueLongTakeUntil callback) {
        java.util.Objects.requireNonNull(callback, "callback");
        for (;;) {
            long recid = tail();
            Node node = store.get(recid, Node.SERIALIZER);
            if (node == null || !callback.take(recid, node)) return;
            take();
        }
    }

    /** Unlink a node. When removeNode is false its record remains available to the caller. */
    public synchronized Node remove(long nodeRecid, boolean removeNode) {
        Node node = store.get(nodeRecid, Node.SERIALIZER);
        if (node == null) throw corrupt("node not found: " + nodeRecid);
        if (removeNode) store.delete(nodeRecid, Node.SERIALIZER);

        Node next = store.get(node.nextRecid, Node.SERIALIZER);
        if (next != null) {
            if (next.prevRecid != nodeRecid) throw corrupt("next-node backlink mismatch");
            store.update(node.nextRecid, next.withPrev(node.prevRecid), Node.SERIALIZER);
        } else {
            if (headPrev() != nodeRecid) throw corrupt("headPrev mismatch");
            headPrev(node.prevRecid);
        }
        if (node.prevRecid != 0) {
            Node previous = store.get(node.prevRecid, Node.SERIALIZER);
            if (previous == null || previous.nextRecid != nodeRecid)
                throw corrupt("previous-node link mismatch");
            store.update(node.prevRecid, previous.withNext(node.nextRecid), Node.SERIALIZER);
        } else {
            if (tail() != nodeRecid) throw corrupt("tail mismatch");
            tail(node.nextRecid);
        }
        return node;
    }

    /** Move a node to the newest position and replace its timestamp. */
    public synchronized void bump(long nodeRecid, long newTimestamp) {
        long newest = headPrev();
        Node node = store.get(nodeRecid, Node.SERIALIZER);
        if (node == null) throw corrupt("node not found: " + nodeRecid);
        if (newest == nodeRecid) {
            store.update(nodeRecid, node.withLinksAndTimestamp(
                    node.prevRecid, node.nextRecid, newTimestamp), Node.SERIALIZER);
            return;
        }
        remove(nodeRecid, false);
        put(newTimestamp, node.value, nodeRecid);
    }

    public synchronized void clear() { takeUntil((recid, node) -> true); }

    public synchronized long size() {
        long count = 0, sentinel = head(), recid = tail();
        while (recid != sentinel) {
            Node node = store.get(recid, Node.SERIALIZER);
            if (node == null) throw corrupt("linked node not found: " + recid);
            recid = node.nextRecid;
            count++;
        }
        return count;
    }

    public synchronized long[] valuesArray() {
        long[] values = new long[16];
        int size = 0;
        for (long recid = tail();;) {
            Node node = store.get(recid, Node.SERIALIZER);
            if (node == null) return Arrays.copyOf(values, size);
            if (size == values.length) values = Arrays.copyOf(values, size * 2);
            values[size++] = node.value;
            recid = node.nextRecid;
        }
    }

    @FunctionalInterface public interface NodeConsumer {
        void accept(long nodeRecid, long value, long timestamp);
    }

    public synchronized void forEach(NodeConsumer consumer) {
        for (long recid = tail();;) {
            Node node = store.get(recid, Node.SERIALIZER);
            if (node == null) return;
            consumer.accept(recid, node.value, node.timestamp);
            recid = node.nextRecid;
        }
    }

    public synchronized void verify() {
        long sentinel = head(), first = tail(), newest = headPrev();
        if (sentinel == first) {
            if (newest != 0) throw new DBException.VerifyFailed("empty QueueLong has headPrev");
            return;
        }
        long previous = 0, recid = first;
        while (recid != sentinel) {
            Node node = store.get(recid, Node.SERIALIZER);
            if (node == null) throw new DBException.VerifyFailed("QueueLong node missing: " + recid);
            if (node.prevRecid != previous) throw new DBException.VerifyFailed("QueueLong backlink mismatch");
            previous = recid;
            recid = node.nextRecid;
        }
        if (store.get(sentinel, Node.SERIALIZER) != null)
            throw new DBException.VerifyFailed("QueueLong sentinel is not preallocated");
        if (previous != newest) throw new DBException.VerifyFailed("QueueLong headPrev mismatch");
    }

    public synchronized void printContent(PrintStream out) {
        out.println("TAIL:" + tail() + ", HEAD:" + head() + ", HEADPREV:" + headPrev());
        forEach((recid, value, timestamp) -> out.println(
                "recid:" + recid + ", timestamp:" + timestamp + ", value:" + value));
    }
}
