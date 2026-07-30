package org.mapdb.indextree;

import org.mapdb.htree.DirTree;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;
import org.mapdb.ser.Serializer;
import org.mapdb.ser.Serializers;
import org.mapdb.store.RecordRead;
import org.mapdb.store.Store;

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Persistent primitive {@code long -> long} map over a Store4 {@link Store}, backed by
 * the sparse, bitmap-compressed {@link DirTree} that also powers {@link org.mapdb.htree.HTreeMap}.
 * The API is fully primitive — {@link #put(long, long)} / {@link #get(long)} never box
 * keys or values.
 *
 * <h3>Addressing range (DirTree reuse decision)</h3>
 * DirTree resolves a bounded non-negative index in exactly {@code levels*dirShift} bits.
 * This map uses {@code dirShift=7}, {@code levels=9} → {@code 63} index bits. DirTree stores
 * a terminal key as {@code key+1} (0 is its subdir marker) and packs it as a NON-NEGATIVE
 * long, so the key domain is capped just below the sign boundary at {@link #MAX_KEY}
 * ({@code Long.MAX_VALUE - 1}); {@code key+1} then never overflows into a negative. Keys
 * outside {@code [0, MAX_KEY]} are rejected ({@code put}/{@code get}/{@code remove} throw
 * {@link IllegalArgumentException}; {@link #containsKey}/{@link #getOrDefault} treat them as
 * absent). No change to DirTree's algorithm was needed — only its visibility was widened; the
 * htree's 32-bit assumption is HTreeMap's own, not DirTree's.
 *
 * <h3>On-store layout</h3>
 * DirTree stores a non-negative recid inline in each slot and reserves {@code 0} as its
 * "absent" sentinel (see {@link DirTree#treePut}). A general {@code long} value (including
 * {@code 0} and negatives) therefore cannot live inline. So the tree maps
 * {@code key -> valueRecid}, and {@code valueRecid} points at an 8-byte record
 * ({@link Serializers#LONG}) holding the raw value. This keeps the full value range and
 * makes {@link #containsKey} exact ({@code treeGet != 0}). Records:
 * <ul>
 *   <li>the DirTree root (recid stable for the map's lifetime, updated in place);</li>
 *   <li>one 8-byte value record per live entry;</li>
 *   <li>a counter record (a single {@code long}) for O(1) {@link #size()};</li>
 *   <li>a header record {@code {dirShift, levels, defaultValue, rootRecid, counterRecid}} —
 *       persist {@link #headerRecid()} and {@link #open} to reattach.</li>
 * </ul>
 *
 * <h3>Concurrency</h3>
 * A single {@link ReentrantReadWriteLock} guards the whole map (reads take the read lock,
 * mutations the write lock); the locks are no-ops when {@code !store.isThreadSafe()}. A
 * tree mutation spans several records, so it is single-writer, exactly as DirTree requires.
 */
public final class IndexTreeLongLongMap {

    /** Slot bits per dir level; DirTree caps this at {@link DirTree#MAX_DIR_SHIFT}. */
    static final int DIR_SHIFT = 7;
    /** Dir levels: {@code 9*7 == 63} index bits covers all non-negative longs. */
    static final int LEVELS = 9;
    /** Largest permitted key: one below {@code Long.MAX_VALUE} so the DirTree {@code key+1}
     *  terminal sentinel stays non-negative (packLong's domain). */
    public static final long MAX_KEY = Long.MAX_VALUE - 1;

    private final Store store;
    private final long headerRecid;
    private final int dirShift, levels;
    private final long rootRecid;
    private final long counterRecid;
    private final long defaultValue;

    private final boolean threadSafe;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /** Consumer over primitive {@code (key, value)} entries, no boxing. */
    public interface LongLongConsumer {
        void accept(long key, long value);
    }

    private IndexTreeLongLongMap(Store store, long headerRecid, Header header) {
        this.store = store;
        this.headerRecid = headerRecid;
        this.dirShift = header.dirShift;
        this.levels = header.levels;
        this.rootRecid = header.rootRecid;
        this.counterRecid = header.counterRecid;
        this.defaultValue = header.defaultValue;
        this.threadSafe = store.isThreadSafe();
    }

    // ================= create / open =================

    /** Fresh map whose {@link #get(long)} returns {@code 0} for absent keys (mapdb3 default). */
    public static IndexTreeLongLongMap create(Store store) {
        return create(store, 0L);
    }

    /** @param defaultValue returned by {@link #get(long)} for a key that is not present. */
    public static IndexTreeLongLongMap create(Store store, long defaultValue) {
        long rootRecid = store.put(DirTree.dirEmpty(), DirTree.DIR_SER);
        long counterRecid = store.put(0L, Serializers.LONG);
        Header header = new Header(DIR_SHIFT, LEVELS, defaultValue, rootRecid, counterRecid);
        long headerRecid = store.put(header, Header.SER);
        return new IndexTreeLongLongMap(store, headerRecid, header);
    }

    /** Reattach a map persisted under {@code headerRecid} by a prior {@link #create}. */
    public static IndexTreeLongLongMap open(Store store, long headerRecid) {
        Header header = store.get(headerRecid, Header.SER);
        return new IndexTreeLongLongMap(store, headerRecid, header);
    }

    /** Recid of the header record; persist this to reopen the map. */
    public long headerRecid() { return headerRecid; }

    /** Value {@link #get(long)} returns for an absent key. */
    public long defaultValue() { return defaultValue; }

    // ================= locks =================

    private void lockRead()    { if (threadSafe) lock.readLock().lock(); }
    private void unlockRead()  { if (threadSafe) lock.readLock().unlock(); }
    private void lockWrite()   { if (threadSafe) lock.writeLock().lock(); }
    private void unlockWrite() { if (threadSafe) lock.writeLock().unlock(); }

    private static void checkKey(long key) {
        if (key < 0) throw new IllegalArgumentException("negative key: " + key);
        if (key > MAX_KEY) throw new IllegalArgumentException("key exceeds MAX_KEY: " + key);
    }

    private static boolean inRange(long key) {
        return key >= 0 && key <= MAX_KEY;
    }

    // ================= reads =================

    /** @return the mapped value, or {@link #defaultValue()} if {@code key} is absent. */
    public long get(long key) {
        checkKey(key);
        lockRead();
        try {
            long valueRecid = DirTree.treeGet(dirShift, rootRecid, store, levels - 1, key);
            if (valueRecid == 0) return defaultValue;
            return readLong(valueRecid);
        } finally {
            unlockRead();
        }
    }

    /**
     * @param ifAbsent value to return when {@code key} is absent (independent of the
     *                 configured {@link #defaultValue()}).
     */
    public long getOrDefault(long key, long ifAbsent) {
        if (!inRange(key)) return ifAbsent;
        lockRead();
        try {
            long valueRecid = DirTree.treeGet(dirShift, rootRecid, store, levels - 1, key);
            return valueRecid == 0 ? ifAbsent : readLong(valueRecid);
        } finally {
            unlockRead();
        }
    }

    public boolean containsKey(long key) {
        if (!inRange(key)) return false;
        lockRead();
        try {
            return DirTree.treeGet(dirShift, rootRecid, store, levels - 1, key) != 0;
        } finally {
            unlockRead();
        }
    }

    /** Number of entries. O(1): reads the counter record. */
    public long size() {
        lockRead();
        try {
            return store.get(counterRecid, Serializers.LONG);
        } finally {
            unlockRead();
        }
    }

    public boolean isEmpty() {
        lockRead();
        try {
            return DirTree.treeIsEmpty(rootRecid, store);
        } finally {
            unlockRead();
        }
    }

    /** Visit every {@code (key, value)} entry, unordered. Callback runs under the read lock. */
    public void forEach(LongLongConsumer consumer) {
        lockRead();
        try {
            DirTree.treeFold(rootRecid, store, levels - 1,
                    (index, valueRecid) -> consumer.accept(index, readLong(valueRecid)));
        } finally {
            unlockRead();
        }
    }

    // ================= writes =================

    /** Insert or overwrite {@code key -> value}. */
    public void put(long key, long value) {
        checkKey(key);
        lockWrite();
        try {
            long valueRecid = DirTree.treeGet(dirShift, rootRecid, store, levels - 1, key);
            if (valueRecid == 0) { // fresh: write the value record, then link it
                valueRecid = store.put(value, Serializers.LONG);
                DirTree.treePut(dirShift, rootRecid, store, levels - 1, key, valueRecid);
                incrementSize(1);
            } else {
                store.update(valueRecid, value, Serializers.LONG);
            }
        } finally {
            unlockWrite();
        }
    }

    /** Insert only if absent; @return the existing value, or {@code newValue} if inserted. */
    public long putIfAbsent(long key, long newValue) {
        checkKey(key);
        lockWrite();
        try {
            long valueRecid = DirTree.treeGet(dirShift, rootRecid, store, levels - 1, key);
            if (valueRecid != 0) return readLong(valueRecid);
            valueRecid = store.put(newValue, Serializers.LONG);
            DirTree.treePut(dirShift, rootRecid, store, levels - 1, key, valueRecid);
            incrementSize(1);
            return newValue;
        } finally {
            unlockWrite();
        }
    }

    /** {@code put(key, get(key) + delta)}; @return the new value. */
    public long addTo(long key, long delta) {
        checkKey(key);
        lockWrite();
        try {
            long valueRecid = DirTree.treeGet(dirShift, rootRecid, store, levels - 1, key);
            if (valueRecid == 0) {
                long v = defaultValue + delta;
                valueRecid = store.put(v, Serializers.LONG);
                DirTree.treePut(dirShift, rootRecid, store, levels - 1, key, valueRecid);
                incrementSize(1);
                return v;
            }
            long v = readLong(valueRecid) + delta;
            store.update(valueRecid, v, Serializers.LONG);
            return v;
        } finally {
            unlockWrite();
        }
    }

    /** Remove {@code key}. @return true iff it was present (its value record is freed). */
    public boolean remove(long key) {
        checkKey(key);
        lockWrite();
        try {
            long valueRecid = DirTree.treeGet(dirShift, rootRecid, store, levels - 1, key);
            if (valueRecid == 0) return false;
            boolean removed = DirTree.treeRemove(dirShift, rootRecid, store, levels - 1, key);
            assert removed : "dir tree lost the entry it just resolved";
            store.delete(valueRecid, Serializers.LONG);
            incrementSize(-1);
            return true;
        } finally {
            unlockWrite();
        }
    }

    /** Remove every entry, freeing all value records; the root recid stays stable. */
    public void clear() {
        lockWrite();
        try {
            DirTree.treeClear(rootRecid, store, levels - 1,
                    (index, valueRecid) -> store.delete(valueRecid, Serializers.LONG));
            store.update(counterRecid, 0L, Serializers.LONG);
        } finally {
            unlockWrite();
        }
    }

    // ================= internals =================

    private void incrementSize(long delta) {
        long n = store.get(counterRecid, Serializers.LONG);
        store.update(counterRecid, n + delta, Serializers.LONG);
    }

    /** Read the raw long from a value record without boxing (push-down). */
    private long readLong(long valueRecid) {
        return store.read(valueRecid, READ_LONG);
    }

    /** Decodes {@link Serializers#LONG}'s 8-byte fixed record (or the live {@link Long}). */
    private static final RecordRead READ_LONG = new RecordRead() {
        @Override public long onBytes(DataInput2 in, int size) { return in.readLong(); }
        @Override public long onObject(Object record) { return (Long) record; }
        @Override public long onNull() {
            throw new IllegalStateException("value record is null");
        }
    };

    /** Immutable metadata; written once at create (root/counter recids stay stable). */
    private static final class Header {
        final int dirShift, levels;
        final long defaultValue, rootRecid, counterRecid;

        Header(int dirShift, int levels, long defaultValue, long rootRecid, long counterRecid) {
            this.dirShift = dirShift;
            this.levels = levels;
            this.defaultValue = defaultValue;
            this.rootRecid = rootRecid;
            this.counterRecid = counterRecid;
        }

        static final Serializer<Header> SER = new Serializer<>() {
            @Override public void serialize(DataOutput2 out, Header h) {
                out.packInt(h.dirShift);
                out.packInt(h.levels);
                out.writeLong(h.defaultValue); // may be negative
                out.packLong(h.rootRecid);
                out.packLong(h.counterRecid);
            }

            @Override public Header deserialize(DataInput2 in, int size) {
                int dirShift = in.unpackInt();
                int levels = in.unpackInt();
                long defaultValue = in.readLong();
                long rootRecid = in.unpackLong();
                long counterRecid = in.unpackLong();
                return new Header(dirShift, levels, defaultValue, rootRecid, counterRecid);
            }
        };
    }
}
