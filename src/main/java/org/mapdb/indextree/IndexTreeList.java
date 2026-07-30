package org.mapdb.indextree;

import org.mapdb.htree.DirTree;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;
import org.mapdb.ser.Serializer;
import org.mapdb.ser.Serializers;
import org.mapdb.store.Store;

import java.util.AbstractList;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * {@link AbstractList}-backed persistent positional list over a Store4 {@link Store}
 * (port of mapdb3's {@code IndexTreeList}). A position {@code 0..size-1} maps through
 * the sparse {@link DirTree} to a record recid; each element is its own Store record,
 * serialized with a caller-supplied {@link Serializer}.
 *
 * <h3>DirTree reuse decision</h3>
 * Unlike {@link IndexTreeLongLongMap}, a list's values are naturally NON-ZERO recids, so
 * the tree slot holds them inline directly — the recid IS the element record, exactly the
 * shape {@link DirTree#treePut} is built for; no per-position value record. Positions are
 * {@code int}, far within the {@code dirShift=7 × levels=9 = 63}-bit index range, so the
 * whole {@code int} position domain is addressable.
 *
 * <h3>Positional insert / remove</h3>
 * {@code add(int, E)} and {@code remove(int)} SHIFT the tail of the index→recid mapping by
 * one slot (up on insert, down on remove) so positions stay contiguous — the same recids
 * are re-keyed, the element records themselves are untouched. A removed element's record is
 * deleted (no leak).
 *
 * <h3>On-store layout & concurrency</h3>
 * Records: the DirTree root (stable, updated in place), one element record per live entry,
 * a counter record ({@code long} size, O(1) {@link #size()}), and a header
 * {@code {dirShift, levels, rootRecid, counterRecid}} — persist {@link #headerRecid()} and
 * {@link #open} (with the same serializer) to reattach. A single
 * {@link ReentrantReadWriteLock} guards the list (no-op when {@code !store.isThreadSafe()});
 * structural mutations are single-writer as DirTree requires.
 */
public final class IndexTreeList<E> extends AbstractList<E> {

    private final Store store;
    private final Serializer<E> serializer;
    private final long headerRecid;
    private final int dirShift, levels;
    private final long rootRecid;
    private final long counterRecid;

    private final boolean threadSafe;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private IndexTreeList(Store store, Serializer<E> serializer, long headerRecid, Header header) {
        this.store = store;
        this.serializer = serializer;
        this.headerRecid = headerRecid;
        this.dirShift = header.dirShift;
        this.levels = header.levels;
        this.rootRecid = header.rootRecid;
        this.counterRecid = header.counterRecid;
        this.threadSafe = store.isThreadSafe();
    }

    // ================= create / open =================

    public static <E> IndexTreeList<E> create(Store store, Serializer<E> serializer) {
        long rootRecid = store.put(DirTree.dirEmpty(), DirTree.DIR_SER);
        long counterRecid = store.put(0L, Serializers.LONG);
        Header header = new Header(IndexTreeLongLongMap.DIR_SHIFT, IndexTreeLongLongMap.LEVELS,
                rootRecid, counterRecid);
        long headerRecid = store.put(header, Header.SER);
        return new IndexTreeList<>(store, serializer, headerRecid, header);
    }

    /** Reattach a list persisted under {@code headerRecid}; the same {@code serializer} must be supplied. */
    public static <E> IndexTreeList<E> open(Store store, long headerRecid, Serializer<E> serializer) {
        Header header = store.get(headerRecid, Header.SER);
        return new IndexTreeList<>(store, serializer, headerRecid, header);
    }

    /** Recid of the header record; persist this to reopen the list. */
    public long headerRecid() { return headerRecid; }

    // ================= locks =================

    private void lockRead()    { if (threadSafe) lock.readLock().lock(); }
    private void unlockRead()  { if (threadSafe) lock.readLock().unlock(); }
    private void lockWrite()   { if (threadSafe) lock.writeLock().lock(); }
    private void unlockWrite() { if (threadSafe) lock.writeLock().unlock(); }

    private int sizeRaw() {
        long n = store.get(counterRecid, Serializers.LONG);
        return (int) n;
    }

    private void setSize(int size) {
        store.update(counterRecid, (long) size, Serializers.LONG);
    }

    private void checkElementIndex(int index, int size) {
        if (index < 0 || index >= size) throw new IndexOutOfBoundsException("index " + index + ", size " + size);
    }

    private void checkPositionIndex(int index, int size) {
        if (index < 0 || index > size) throw new IndexOutOfBoundsException("index " + index + ", size " + size);
    }

    private long treeGet(long index) {
        return DirTree.treeGet(dirShift, rootRecid, store, levels - 1, index);
    }

    private void treePut(long index, long recid) {
        DirTree.treePut(dirShift, rootRecid, store, levels - 1, index, recid);
    }

    private void treeRemove(long index) {
        DirTree.treeRemove(dirShift, rootRecid, store, levels - 1, index);
    }

    // ================= reads =================

    @Override public int size() {
        lockRead();
        try {
            return sizeRaw();
        } finally {
            unlockRead();
        }
    }

    @Override public boolean isEmpty() {
        return size() == 0;
    }

    @Override public E get(int index) {
        lockRead();
        try {
            checkElementIndex(index, sizeRaw());
            long recid = treeGet(index);
            if (recid == 0) return null;
            return store.get(recid, serializer);
        } finally {
            unlockRead();
        }
    }

    // ================= writes =================

    @Override public boolean add(E element) {
        lockWrite();
        try {
            int index = sizeRaw();
            long recid = store.put(element, serializer);
            treePut(index, recid);
            setSize(index + 1);
            modCount++;
            return true;
        } finally {
            unlockWrite();
        }
    }

    @Override public void add(int index, E element) {
        lockWrite();
        try {
            int size = sizeRaw();
            checkPositionIndex(index, size);
            // store.put first: it is the only fallible step (serializer exception), so the
            // dense-list invariant survives a failure (no half-shifted tail, no leaked record).
            long newRecid = store.put(element, serializer);
            // shift [index, size-1] up by one so the tail keeps contiguous positions.
            // The list is dense (every position < size holds a non-zero recid), so each
            // moved slot is occupied and the destination `index` is empty afterward.
            for (int i = size - 1; i >= index; i--) {
                long recid = treeGet(i);
                assert recid != 0 : "dense list invariant broken at " + i;
                treeRemove(i);
                treePut(i + 1, recid);
            }
            treePut(index, newRecid);
            setSize(size + 1);
            modCount++;
        } finally {
            unlockWrite();
        }
    }

    @Override public E set(int index, E element) {
        lockWrite();
        try {
            checkElementIndex(index, sizeRaw());
            long recid = treeGet(index);
            if (recid == 0) {
                treePut(index, store.put(element, serializer));
                return null;
            }
            E old = store.get(recid, serializer);
            store.update(recid, element, serializer);
            return old;
        } finally {
            unlockWrite();
        }
    }

    @Override public E remove(int index) {
        lockWrite();
        try {
            int size = sizeRaw();
            checkElementIndex(index, size);
            long recid = treeGet(index);
            E ret = null;
            if (recid != 0) {
                ret = store.get(recid, serializer);
                store.delete(recid, serializer);
                treeRemove(index);
            }
            // shift (index, size-1] down by one (dense list: every such slot is occupied)
            for (int i = index + 1; i < size; i++) {
                long r = treeGet(i);
                assert r != 0 : "dense list invariant broken at " + i;
                treeRemove(i);
                treePut(i - 1, r);
            }
            setSize(size - 1);
            modCount++;
            return ret;
        } finally {
            unlockWrite();
        }
    }

    @Override public void clear() {
        lockWrite();
        try {
            DirTree.treeClear(rootRecid, store, levels - 1,
                    (index, recid) -> store.delete(recid, serializer));
            setSize(0);
            modCount++;
        } finally {
            unlockWrite();
        }
    }

    // ================= record format =================

    /** Immutable metadata; written once at create (root/counter recids stay stable). */
    private static final class Header {
        final int dirShift, levels;
        final long rootRecid, counterRecid;

        Header(int dirShift, int levels, long rootRecid, long counterRecid) {
            this.dirShift = dirShift;
            this.levels = levels;
            this.rootRecid = rootRecid;
            this.counterRecid = counterRecid;
        }

        static final Serializer<Header> SER = new Serializer<>() {
            @Override public void serialize(DataOutput2 out, Header h) {
                out.packInt(h.dirShift);
                out.packInt(h.levels);
                out.packLong(h.rootRecid);
                out.packLong(h.counterRecid);
            }

            @Override public Header deserialize(DataInput2 in, int size) {
                int dirShift = in.unpackInt();
                int levels = in.unpackInt();
                long rootRecid = in.unpackLong();
                long counterRecid = in.unpackLong();
                return new Header(dirShift, levels, rootRecid, counterRecid);
            }
        };
    }
}
