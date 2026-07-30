package org.mapdb.htree;

import org.mapdb.DBException;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;
import org.mapdb.ser.Serializer;
import org.mapdb.ser.Serializers;
import org.mapdb.store.Store;

/**
 * FIFO queue of {@code (timestamp, value)} nodes over a Store, with removal from the
 * middle and re-insertion at the head ({@link #bump}). Port of mapdb3's
 * {@code QueueLong}; used by {@link HTreeCache} as the per-segment expiry queue
 * (value = leaf recid, timestamp = expiry instant, 0 = no TTL).
 *
 * Layout: a doubly-linked list of {@link Node} records. Three pointer records anchor
 * it: {@code tail} (oldest node — next to be taken), {@code head} (a PREALLOCATED,
 * still-empty record the next {@link #put} will fill; store.get on it returns null,
 * which is how traversals detect the end) and {@code headPrev} (newest filled node,
 * 0 when empty). Timestamps must be enqueued monotonically (callers use a single
 * fixed TTL) so {@link #takeUntil} can stop at the first non-expired node.
 *
 * Not thread safe: callers serialize access (HTreeCache holds the segment write lock).
 */
final class ExpireQueue {

    private final Store store;
    /** Recids of the three pointer records (each holds one recid, LONG-serialized). */
    final long tailRecid, headRecid, headPrevRecid;

    ExpireQueue(Store store, long tailRecid, long headRecid, long headPrevRecid) {
        assert tailRecid != headRecid : "head==tail";
        this.store = store;
        this.tailRecid = tailRecid;
        this.headRecid = headRecid;
        this.headPrevRecid = headPrevRecid;
    }

    /** Allocate an empty queue: one preallocated node + the three pointer records. */
    static ExpireQueue create(Store store) {
        long emptyNode = store.preallocate();
        long tailRecid = store.put(emptyNode, Serializers.LONG);
        long headRecid = store.put(emptyNode, Serializers.LONG);
        long headPrevRecid = store.put(0L, Serializers.LONG);
        return new ExpireQueue(store, tailRecid, headRecid, headPrevRecid);
    }

    /** Queue node. {@code value} is the payload (a leaf recid for HTreeCache);
     *  {@code timestamp} 0 means "no TTL" (evictable only by size pressure). */
    static final class Node {
        final long prevRecid, nextRecid, timestamp, value;

        Node(long prevRecid, long nextRecid, long timestamp, long value) {
            this.prevRecid = prevRecid;
            this.nextRecid = nextRecid;
            this.timestamp = timestamp;
            this.value = value;
        }

        static final Serializer<Node> SER = new Serializer<>() {
            @Override public void serialize(DataOutput2 out, Node n) {
                out.packLong(n.prevRecid);
                out.packLong(n.nextRecid);
                out.packLong(n.timestamp);
                out.packLong(n.value);
            }

            @Override public Node deserialize(DataInput2 in, int size) {
                return new Node(in.unpackLong(), in.unpackLong(), in.unpackLong(), in.unpackLong());
            }
        };
    }

    /** Take-decision callback for {@link #takeUntil}. */
    interface TakeUntil {
        /** @return true to take (dequeue + delete) this node and continue, false to stop. */
        boolean take(long nodeRecid, Node node);
    }

    private long tail()                { return store.get(tailRecid, Serializers.LONG); }
    private void tail(long v)          { store.update(tailRecid, v, Serializers.LONG); }
    private long head()                { return store.get(headRecid, Serializers.LONG); }
    private void head(long v)          { store.update(headRecid, v, Serializers.LONG); }
    private long headPrev()            { return store.get(headPrevRecid, Serializers.LONG); }
    private void headPrev(long v)      { store.update(headPrevRecid, v, Serializers.LONG); }

    /** Append a node at the head; returns its recid (stable for the node's lifetime). */
    long put(long timestamp, long value) {
        // the current head record is preallocated: fill it and preallocate the next
        long nextRecid = store.preallocate();
        long node = head();
        head(nextRecid);
        long prev = headPrev();
        headPrev(node);
        store.update(node, new Node(prev, nextRecid, timestamp, value), Node.SER);
        // link the previous newest node forward (its nextRecid already points here:
        // it was this node's recid when that node was filled)
        assert prev == 0 || store.get(prev, Node.SER).nextRecid == node;
        return node;
    }

    /** Dequeue the oldest node (deleting its record), or null when empty. */
    Node take() {
        long t = tail();
        Node curr = store.get(t, Node.SER);
        if (curr == null) {
            // tail == head (preallocated): empty. A null tail record that is NOT the head is a
            // broken chain — fail fast like unlink() does, instead of absorbing it as "empty".
            // No write here: headPrev==0-when-empty is already maintained by the non-empty
            // branch below (CAS) and by unlink()'s tail case, and take() may run on read paths.
            if (t != head()) throw new DBException.DataCorruption("queue tail points at a missing node");
            return null;
        }
        store.delete(t, Node.SER);
        tail(curr.nextRecid);
        // if the taken node was also the newest, headPrev must drop to 0
        store.compareAndSwap(headPrevRecid, t, 0L, Serializers.LONG);
        Node next = store.get(curr.nextRecid, Node.SER);
        if (next != null) { // new tail exists: it has no predecessor now
            if (next.prevRecid != t)
                throw new DBException.DataCorruption("queue tail successor back-link mismatch");
            store.update(curr.nextRecid, new Node(0L, next.nextRecid, next.timestamp, next.value), Node.SER);
        }
        return curr;
    }

    /** Take nodes oldest-first while the callback returns true; the first refused
     *  node stays queued. The callback runs BEFORE its node is dequeued. */
    void takeUntil(TakeUntil f) {
        while (true) {
            long t = tail();
            Node node = store.get(t, Node.SER);
            if (node == null) return; // reached the preallocated head: empty
            assert node.prevRecid == 0 : "tail node has a predecessor";
            if (!f.take(t, node)) return;
            Node taken = take();
            assert taken != null && taken.value == node.value : "takeUntil dequeued a different node";
        }
    }

    /** Unlink {@code nodeRecid} from the middle (deleting its record when
     *  {@code deleteNode}); returns the unlinked node. */
    Node remove(long nodeRecid, boolean deleteNode) {
        Node node = store.get(nodeRecid, Node.SER);
        if (node == null) throw new DBException.DataCorruption("queue node not found");
        unlink(nodeRecid, node);
        if (deleteNode) store.delete(nodeRecid, Node.SER);
        return node;
    }

    /** Splice {@code node} out of the linked list (its own record is NOT touched). */
    private void unlink(long nodeRecid, Node node) {
        Node next = store.get(node.nextRecid, Node.SER);
        Node prev = node.prevRecid == 0 ? null : store.get(node.prevRecid, Node.SER);
        if (next != null) {
            if (next.prevRecid != nodeRecid) throw new DBException.DataCorruption("queue next link error");
        } else {
            if (headPrev() != nodeRecid) throw new DBException.DataCorruption("queue headPrev link error");
        }
        if (node.prevRecid != 0) {
            if (prev == null) throw new DBException.DataCorruption("queue prev node not found");
            if (prev.nextRecid != nodeRecid) throw new DBException.DataCorruption("queue prev link error");
        } else {
            if (tail() != nodeRecid) throw new DBException.DataCorruption("queue tail link error");
        }

        if (next != null) {
            store.update(node.nextRecid, new Node(node.prevRecid, next.nextRecid, next.timestamp, next.value), Node.SER);
        } else { // node was the newest
            headPrev(node.prevRecid);
        }
        if (prev != null) {
            store.update(node.prevRecid, new Node(prev.prevRecid, node.nextRecid, prev.timestamp, prev.value), Node.SER);
        } else { // node was the oldest
            tail(node.nextRecid);
        }
    }

    /** Move {@code nodeRecid} to the head with a fresh timestamp (recid unchanged,
     *  so leaf references stay valid). */
    void bump(long nodeRecid, long newTimestamp) {
        long newest = headPrev();
        Node node = store.get(nodeRecid, Node.SER);
        if (node == null) throw new DBException.DataCorruption("queue node not found");
        if (newest == nodeRecid) { // already newest: just restamp
            store.update(nodeRecid, new Node(node.prevRecid, node.nextRecid, newTimestamp, node.value), Node.SER);
            return;
        }
        unlink(nodeRecid, node);
        // re-insert as newest, in front of the preallocated head record
        headPrev(nodeRecid);
        Node prevNewest = store.get(newest, Node.SER);
        if (prevNewest == null) throw new DBException.DataCorruption("queue newest node not found");
        store.update(newest, new Node(prevNewest.prevRecid, nodeRecid, prevNewest.timestamp, prevNewest.value), Node.SER);
        store.update(nodeRecid, new Node(newest, prevNewest.nextRecid, newTimestamp, node.value), Node.SER);
    }

    /** Dequeue and delete every node. */
    void clear() {
        takeUntil((recid, node) -> true);
    }

    /** Node count; O(n), test/verify use only. */
    long size() {
        long n = 0;
        long head = head();
        for (long recid = tail(); recid != head; ) {
            Node node = store.get(recid, Node.SER);
            if (node == null) throw new DBException.DataCorruption("queue node not found");
            recid = node.nextRecid;
            n++;
        }
        return n;
    }

    /** Visitor over live nodes oldest-first; test/verify use only. */
    interface NodeVisitor {
        void visit(long nodeRecid, Node node);
    }

    void forEach(NodeVisitor visitor) {
        for (long recid = tail(); ; ) {
            Node node = store.get(recid, Node.SER);
            if (node == null) return; // reached preallocated head
            visitor.visit(recid, node);
            recid = node.nextRecid;
        }
    }

    /** Structural invariants (mirrors mapdb3 QueueLong.verify); test use. */
    void verify() {
        long head = head(), tail = tail(), headPrev = headPrev();
        if (head == tail) {
            if (headPrev != 0) throw new AssertionError("empty queue with headPrev != 0");
            return;
        }
        Node node = store.get(tail, Node.SER);
        if (node == null) throw new AssertionError("tail node missing");
        if (node.prevRecid != 0) throw new AssertionError("tail prevRecid != 0");
        long prevRecid = tail;
        while (node.nextRecid != head) {
            long recid = node.nextRecid;
            node = store.get(recid, Node.SER);
            if (node == null) throw new AssertionError("chain node missing");
            if (node.prevRecid != prevRecid) throw new AssertionError("prev link broken");
            prevRecid = recid;
        }
        if (store.get(head, Node.SER) != null) throw new AssertionError("head record not preallocated");
        if (prevRecid != headPrev) throw new AssertionError("headPrev mismatch");
    }
}
