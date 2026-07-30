package org.mapdb.htree;

import org.mapdb.MapModificationListener;
import org.mapdb.ModificationAwareMap;
import org.mapdb.MapExtra;
import org.mapdb.hash.Hasher;
import org.mapdb.hash.Hashers;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;
import org.mapdb.ser.Serializer;
import org.mapdb.store.RecordRead;
import org.mapdb.store.Store;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;

/**
 * {@link HTreeMap} variant with EXTERNAL values: the bucket stores {@code (key,
 * valueRecid)} pairs and each value lives in its own store record. A deliberate,
 * self-contained duplicate of the lean inline-value HTreeMap (kept as a separate class
 * rather than feature flags in the hot paths); the dir-tree machinery
 * ({@link DirTree}) is shared unchanged.
 *
 * The trade vs inline values:
 *  - bucket scans deserialize KEYS ONLY — a collision bucket holding megabyte values
 *    costs the same to scan as one holding bytes ({@code containsKey} never touches
 *    a value record at all);
 *  - overwrites/replaces rewrite ONLY the value record, never the bucket;
 *  - every hit pays one extra record read (recid → value).
 * Right for large values; for small values inline {@link HTreeMap} is faster.
 *
 * Value records are owned by their entry: put allocates, overwrite updates in place,
 * remove/clear delete (the TCK leak-gates this via store.getAllRecids counts).
 * Value reads always happen under the segment read lock — a resolved recid must not
 * outlive the lock or a concurrent remove could free it mid-read.
 *
 * Hashing, locking, iteration snapshots, persistence and the concurrency contract
 * are identical to {@link HTreeMap}; see its javadoc.
 *
 * Implements {@link java.util.Map} and {@link java.util.concurrent.ConcurrentMap}
 * with the same weak-consistency and {@link #size} saturation caveats as
 * {@link HTreeMap} — see its javadoc.
 */
public final class HTreeMapExternal<K, V> extends AbstractMap<K, V> implements MapExtra<K, V> {

    private final Store store;
    private final Serializer<K> keySer;
    private final Serializer<V> valueSer;

    private final int concShift, dirShift, levels;
    private final int hashSeed;
    private final long headerRecid;
    private final long[] segmentRoots;
    private final Hasher<? super K> hasher;
    private final ReentrantReadWriteLock[] locks;
    private final boolean threadSafe;

    private final int segmentShift;
    private final long indexMask;
    private final int concMask;

    private final Serializer<Object[]> leafSer;
    private final MapRuntime<K, V> runtime = new MapRuntime<>();
    /** The bucket holds keys + packed recids, so the lock-free optimistic scan only
     *  needs FIXED-SIZE KEYS to be torn-read safe (§0.11): torn bytes then read
     *  bounded key bytes and a bounded packed long, never a garbage length prefix.
     *  (Value size does not matter — values are not in the bucket.) */
    private final boolean leafReadOptimistic;

    private HTreeMapExternal(Store store, Serializer<K> keySer, Serializer<V> valueSer,
                             long headerRecid, Header header, Hasher<? super K> hasher) {
        this.store = store;
        this.keySer = keySer;
        this.valueSer = valueSer;
        this.headerRecid = headerRecid;
        this.concShift = header.concShift;
        this.dirShift = header.dirShift;
        this.levels = header.levels;
        this.hashSeed = header.hashSeed;
        this.segmentRoots = header.segmentRoots;
        this.hasher = hasher != null ? hasher : Hashers.objectHasher();
        this.segmentShift = levels * dirShift;
        this.indexMask = (1L << segmentShift) - 1;
        this.concMask = (1 << concShift) - 1;
        this.threadSafe = store.isThreadSafe();
        this.locks = new ReentrantReadWriteLock[1 << concShift];
        for (int i = 0; i < locks.length; i++) locks[i] = new ReentrantReadWriteLock();
        this.leafSer = leafSerializer(keySer);
        this.leafReadOptimistic = keySer.fixedSize() > 0;
    }

    private static void checkConfig(int concShift, int dirShift, int levels) {
        // hard cap 12 (4096 segments): segments are lock granularity, more was measured
        // as a cache-scatter regression
        if (concShift < 0 || concShift > 12)
            throw new IllegalArgumentException("concShift must be in [0,12]");
        if (dirShift < 1 || dirShift > DirTree.MAX_DIR_SHIFT)
            throw new IllegalArgumentException("dirShift must be in [1," + DirTree.MAX_DIR_SHIFT + "]");
        if (levels < 1) throw new IllegalArgumentException("levels must be >= 1");
        if (concShift + levels * dirShift != 32)
            throw new IllegalArgumentException("concShift + levels*dirShift must equal 32, got "
                    + concShift + " + " + levels + "*" + dirShift);
    }

    // ================= create / open =================

    /** Defaults: 16 segments, 4 dir levels x 7 bits (same geometry as HTreeMap). */
    public static <K, V> HTreeMapExternal<K, V> create(Store store, Serializer<K> keySer,
                                                       Serializer<V> valueSer) {
        return create(store, keySer, valueSer, 4, 7, 4, null);
    }

    public static <K, V> HTreeMapExternal<K, V> create(Store store, Serializer<K> keySer,
                                                       Serializer<V> valueSer,
                                                       int concShift, int dirShift, int levels) {
        return create(store, keySer, valueSer, concShift, dirShift, levels, null);
    }

    /** @param hasher custom {@link Hasher} (e.g. {@link Hashers#mixing} over a content
     *                hash); null = {@link Hashers#objectHasher()}. Must be supplied
     *                identically on every open of this map. */
    public static <K, V> HTreeMapExternal<K, V> create(Store store, Serializer<K> keySer,
                                                       Serializer<V> valueSer,
                                                       int concShift, int dirShift, int levels,
                                                       Hasher<? super K> hasher) {
        return create(store, keySer, valueSer, concShift, dirShift, levels,
                new Random().nextInt(), hasher);
    }

    /**
     * Create with an explicit {@code hashSeed} (also used for deterministic tests, §0.10).
     * The {@code hashSeed} is persisted in the header and consumed ONLY at create time —
     * {@link #open} reconstructs it from the header, so no seed is passed to open.
     */
    public static <K, V> HTreeMapExternal<K, V> create(Store store, Serializer<K> keySer,
                                                Serializer<V> valueSer,
                                                int concShift, int dirShift, int levels,
                                                int hashSeed, Hasher<? super K> hasher) {
        checkConfig(concShift, dirShift, levels);
        long[] roots = new long[1 << concShift];
        for (int i = 0; i < roots.length; i++) {
            roots[i] = store.put(DirTree.dirEmpty(), DirTree.DIR_SER);
        }
        Header header = new Header(concShift, dirShift, levels, hashSeed, roots);
        long headerRecid = store.put(header, Header.SER);
        return new HTreeMapExternal<>(store, keySer, valueSer, headerRecid, header, hasher);
    }

    /**
     * Bulk-load an external-value map from entries sorted by non-descending unsigned
     * 32-bit hash computed with the same {@code hashSeed} and {@code hasher}. Values
     * are written before the bucket records that reference them. The factory does not
     * sort, commit, or close the store.
     */
    public static <K, V> HTreeMapExternal<K, V> createFromSortedByHash(
            Store store, Serializer<K> keySer, Serializer<V> valueSer,
            int concShift, int dirShift, int levels,
            int hashSeed, Hasher<? super K> hasher,
            Iterator<? extends Map.Entry<K, V>> entries) {
        checkConfig(concShift, dirShift, levels);
        Hasher<? super K> h = hasher != null ? hasher : Hashers.objectHasher();
        Serializer<Object[]> leafSer = leafSerializer(keySer);
        HTreePump.BuildResult result = HTreePump.build(store, concShift, dirShift, levels, entries,
                key -> Integer.toUnsignedLong(h.hash(key, hashSeed)),
                keySer,
                leaf -> {
                    Object[] externalLeaf = leaf.clone();
                    for (int i = 1; i < externalLeaf.length; i += 2) {
                        @SuppressWarnings("unchecked")
                        V value = (V) externalLeaf[i];
                        externalLeaf[i] = store.put(value, valueSer);
                    }
                    return store.put(externalLeaf, leafSer);
                },
                roots -> store.put(new Header(concShift, dirShift, levels, hashSeed, roots), Header.SER));
        Header header = new Header(concShift, dirShift, levels, hashSeed, result.segmentRoots);
        return new HTreeMapExternal<>(store, keySer, valueSer, result.headerRecid, header, h);
    }

    public static <K, V> HTreeMapExternal<K, V> open(Store store, long headerRecid,
                                                     Serializer<K> keySer, Serializer<V> valueSer) {
        return open(store, headerRecid, keySer, valueSer, null);
    }

    /** @param hasher must match the hasher supplied at create (or null if none was). */
    public static <K, V> HTreeMapExternal<K, V> open(Store store, long headerRecid,
                                                     Serializer<K> keySer, Serializer<V> valueSer,
                                                     Hasher<? super K> hasher) {
        Header header = store.get(headerRecid, Header.SER);
        return new HTreeMapExternal<>(store, keySer, valueSer, headerRecid, header, hasher);
    }

    /** Recid of the header record; persist this to reopen the map. */
    public long headerRecid() { return headerRecid; }

    // ================= hashing (§0.3) =================

    private int hash(K key) {
        return hasher.hash(key, hashSeed);
    }

    private int segment(int hash) {
        return (hash >>> segmentShift) & concMask;
    }

    private long index(int hash) {
        return Integer.toUnsignedLong(hash) & indexMask;
    }

    // ================= locks =================

    private void lockRead(int seg)    { if (threadSafe) locks[seg].readLock().lock(); }
    private void unlockRead(int seg)  { if (threadSafe) locks[seg].readLock().unlock(); }
    private void lockWrite(int seg)   { runtime.deferBegin(); if (threadSafe) locks[seg].writeLock().lock(); }
    private void unlockWrite(int seg) {
        if (threadSafe) locks[seg].writeLock().unlock();
        runtime.deferEnd();
    }

    /** finally-block unlock with {@code body} = the throwable already propagating from the try
     *  body (null if none): a throw from deferEnd's deferred delivery is suppressed behind it
     *  instead of masking it (a finally-throw would discard the in-flight exception). */
    private void unlockWrite(int seg, Throwable body) {
        try {
            unlockWrite(seg);
        } catch (RuntimeException | Error t) {
            if (body == null) throw t;
            MapRuntime.suppress(body, t);
        }
    }

    // ================= bucket lookup =================

    /**
     * Push-down key scan: find {@code key} in a bucket and return its VALUE RECID
     * (0 = key absent — recid 0 is the store's null sentinel, so it cannot collide
     * with a live value record). Stateless: the whole answer rides in the return
     * long, so re-invocation on torn bytes is trivially safe — the
     * store discards torn results and retries under its lock.
     */
    private final class LeafFindAction implements RecordRead {
        final K key;

        LeafFindAction(K key) { this.key = key; }

        @Override public long onBytes(DataInput2 in, int size) {
            int n = in.unpackInt();
            // each entry is >= 2 serialized bytes (key + packed recid): torn/corrupt guard
            if (n > size) throw new IllegalStateException("leaf entry count exceeds record size");
            for (int i = 0; i < n; i++) {
                K k = keySer.deserialize(in, -1);
                long valueRecid = in.unpackLong();
                if (keySer.equals(k, key)) return valueRecid;
            }
            return 0;
        }

        @SuppressWarnings("unchecked")
        @Override public long onObject(Object record) {
            Object[] leaf = (Object[]) record;
            for (int i = 0; i < leaf.length; i += 2) {
                if (keySer.equals((K) leaf[i], key)) return (Long) leaf[i + 1];
            }
            return 0;
        }

        @Override public long onNull() { return 0; }
    }

    /** Value recid for {@code key} within the bucket, or 0. Caller holds the segment lock. */
    @SuppressWarnings("unchecked")
    private long findValueRecid(long leafRecid, K key) {
        if (leafReadOptimistic) {
            return store.read(leafRecid, new LeafFindAction(key));
        }
        Object[] leaf = store.get(leafRecid, leafSer);
        for (int i = 0; i < leaf.length; i += 2) {
            if (keySer.equals((K) leaf[i], key)) return (Long) leaf[i + 1];
        }
        return 0;
    }

    // ================= get / containsKey =================

    @Override public V get(Object keyObj) {
        V value = getRaw(keyObj);
        if (value != null) return value;
        @SuppressWarnings("unchecked") K key = (K) keyObj;
        V loaded = runtime.load(key);
        if (loaded == null || store.isReadOnly()) return loaded;
        V existing = putIfAbsent(key, loaded);
        return existing == null ? loaded : existing;
    }

    @SuppressWarnings("unchecked")
    private V getRaw(Object keyObj) {
        if (keyObj == null) throw new NullPointerException();
        K key = (K) keyObj;
        int h = hash(key);
        int seg = segment(h);
        long idx = index(h);
        lockRead(seg);
        try {
            long leafRecid = DirTree.treeGet(dirShift, segmentRoots[seg], store, levels - 1, idx);
            if (leafRecid == 0) return null;
            long valueRecid = findValueRecid(leafRecid, key);
            if (valueRecid == 0) return null;
            // resolve INSIDE the read lock: the recid must not outlive it (remove frees it)
            return store.get(valueRecid, valueSer);
        } finally {
            unlockRead(seg);
        }
    }

    public void modificationListenerAdd(MapModificationListener<K, V> listener) { runtime.add(listener); }
    public void modificationListenerRemove(MapModificationListener<K, V> listener) { runtime.remove(listener); }
    public void valueLoader(java.util.function.Function<? super K, ? extends V> loader) {
        runtime.valueLoader = loader;
    }
    @Override public boolean isClosed() { return store.isClosed(); }
    @Override public Serializer<K> keySerializer() { return keySer; }
    @Override public Serializer<V> valueSerializer() { return valueSer; }

    /** True iff the key has an entry; never reads a value record. */
    @SuppressWarnings("unchecked")
    @Override public boolean containsKey(Object keyObj) {
        if (keyObj == null) throw new NullPointerException();
        K key = (K) keyObj;
        int h = hash(key);
        int seg = segment(h);
        long idx = index(h);
        lockRead(seg);
        try {
            long leafRecid = DirTree.treeGet(dirShift, segmentRoots[seg], store, levels - 1, idx);
            return leafRecid != 0 && findValueRecid(leafRecid, key) != 0;
        } finally {
            unlockRead(seg);
        }
    }

    // ================= put =================

    /** Insert or replace; returns the previous value or null. Overwrite rewrites only
     *  the value record — the bucket is untouched. */
    @Override public V put(K key, V value) {
        if (key == null || value == null) throw new NullPointerException();
        return putInternal(key, value, false, false);
    }

    /** Insert only if absent; returns the existing value, or null if this call inserted. */
    @Override public V putIfAbsent(K key, V value) {
        if (key == null || value == null) throw new NullPointerException();
        return putInternal(key, value, true, false);
    }

    /**
     * Blind put — insert or overwrite WITHOUT returning the previous value (JCache's
     * {@code void put}; not in {@code java.util.Map}). Unlike {@link #put}, an overwrite
     * here does NOT read the old value record, so it saves one record read (and the
     * deserialization of a potentially large value) — the payoff of external values.
     */
    public void putOnly(K key, V value) {
        if (key == null || value == null) throw new NullPointerException();
        putInternal(key, value, false, !runtime.hasListeners());
    }

    @SuppressWarnings("unchecked")
    private V putInternal(K key, V value, boolean onlyIfAbsent, boolean blind) {
        int h = hash(key);
        int seg = segment(h);
        long idx = index(h);
        lockWrite(seg);
        Throwable body = null;
        try {
            long leafRecid = DirTree.treeGet(dirShift, segmentRoots[seg], store, levels - 1, idx);
            if (leafRecid == 0) { // fresh bucket: value record first, then leaf, then link
                long valueRecid = store.put(value, valueSer);
                leafRecid = store.put(new Object[]{key, valueRecid}, leafSer);
                DirTree.treePut(dirShift, segmentRoots[seg], store, levels - 1, idx, leafRecid);
                runtime.fire(key, null, value, false);
                return null;
            }
            Object[] leaf = store.get(leafRecid, leafSer);
            for (int i = 0; i < leaf.length; i += 2) {
                if (keySer.equals((K) leaf[i], key)) { // key present
                    long valueRecid = (Long) leaf[i + 1];
                    if (blind && !runtime.hasListeners()) { // overwrite without reading old value
                        store.update(valueRecid, value, valueSer);
                        return null;
                    }
                    V old = store.get(valueRecid, valueSer);
                    if (onlyIfAbsent) return old;
                    store.update(valueRecid, value, valueSer); // leaf untouched
                    runtime.fire(key, old, value, false);
                    return old;
                }
            }
            // full-index hash collision: grow the bucket by one entry
            long valueRecid = store.put(value, valueSer);
            Object[] leaf2 = java.util.Arrays.copyOf(leaf, leaf.length + 2);
            leaf2[leaf.length] = key;
            leaf2[leaf.length + 1] = valueRecid;
            store.update(leafRecid, leaf2, leafSer);
            runtime.fire(key, null, value, false);
            return null;
        } catch (RuntimeException | Error t) {
            body = t;
            throw t;
        } finally {
            unlockWrite(seg, body);
        }
    }

    // ================= remove / replace =================

    /** Remove the key's entry; returns the previous value or null. */
    @SuppressWarnings("unchecked")
    @Override public V remove(Object key) {
        if (key == null) throw new NullPointerException();
        return removeInternal((K) key, null);
    }

    /** Remove only if the current value equals {@code value} (via valueSer.equals). */
    @SuppressWarnings("unchecked")
    @Override public boolean remove(Object key, Object value) {
        if (key == null || value == null) throw new NullPointerException();
        return removeInternal((K) key, (V) value) != null;
    }

    /**
     * Blind remove — delete the key's entry, returning only WHETHER one existed (JCache's
     * {@code boolean remove}). Unlike {@link #remove(Object)} it does NOT read the old
     * value record before freeing it, saving one record read (and the deserialization of
     * a potentially large value).
     */
    @SuppressWarnings("unchecked")
    public boolean removeOnly(K key) {
        if (key == null) throw new NullPointerException();
        if (runtime.hasListeners()) return removeInternal(key, null) != null;
        int h = hash(key);
        int seg = segment(h);
        long idx = index(h);
        lockWrite(seg);
        Throwable body = null;
        try {
            long leafRecid = DirTree.treeGet(dirShift, segmentRoots[seg], store, levels - 1, idx);
            if (leafRecid == 0) return false;
            Object[] leaf = store.get(leafRecid, leafSer);
            for (int i = 0; i < leaf.length; i += 2) {
                if (!keySer.equals((K) leaf[i], key)) continue;
                deleteEntryAt(seg, leaf, leafRecid, i, idx); // frees value record, no read
                return true;
            }
            return false;
        } catch (RuntimeException | Error t) {
            body = t;
            throw t;
        } finally {
            unlockWrite(seg, body);
        }
    }

    @SuppressWarnings("unchecked")
    private V removeInternal(K key, V expectedValue) {
        int h = hash(key);
        int seg = segment(h);
        long idx = index(h);
        lockWrite(seg);
        Throwable body = null;
        try {
            long leafRecid = DirTree.treeGet(dirShift, segmentRoots[seg], store, levels - 1, idx);
            if (leafRecid == 0) return null;
            Object[] leaf = store.get(leafRecid, leafSer);
            for (int i = 0; i < leaf.length; i += 2) {
                if (!keySer.equals((K) leaf[i], key)) continue;
                long valueRecid = (Long) leaf[i + 1];
                V old = store.get(valueRecid, valueSer);
                if (expectedValue != null && !valueSer.equals(old, expectedValue)) return null;
                deleteEntryAt(seg, leaf, leafRecid, i, idx);
                runtime.fire(key, old, null, false);
                return old;
            }
            return null;
        } catch (RuntimeException | Error t) {
            body = t;
            throw t;
        } finally {
            unlockWrite(seg, body);
        }
    }

    /** Free entry {@code i}'s value record and unlink it (collapse the bucket, or shrink
     *  it by one pair). Caller holds the segment write lock. */
    private void deleteEntryAt(int seg, Object[] leaf, long leafRecid, int i, long idx) {
        store.delete((Long) leaf[i + 1], valueSer); // entry owns its value record
        if (leaf.length == 2) {
            boolean removed = DirTree.treeRemove(dirShift, segmentRoots[seg], store,
                    levels - 1, idx);
            assert removed : "dir tree lost the bucket it just resolved";
            store.delete(leafRecid, leafSer);
        } else {
            Object[] leaf2 = new Object[leaf.length - 2];
            System.arraycopy(leaf, 0, leaf2, 0, i);
            System.arraycopy(leaf, i + 2, leaf2, i, leaf2.length - i);
            store.update(leafRecid, leaf2, leafSer);
        }
    }

    /** Replace only if present; returns the previous value or null. */
    @Override public V replace(K key, V value) {
        if (key == null || value == null) throw new NullPointerException();
        return replaceInternal(key, null, value);
    }

    /** Replace only if the current value equals {@code oldValue} (via valueSer.equals). */
    @Override public boolean replace(K key, V oldValue, V newValue) {
        if (key == null || oldValue == null || newValue == null) throw new NullPointerException();
        return replaceInternal(key, oldValue, newValue) != null;
    }

    @SuppressWarnings("unchecked")
    private V replaceInternal(K key, V expectedValue, V newValue) {
        int h = hash(key);
        int seg = segment(h);
        long idx = index(h);
        lockWrite(seg);
        Throwable body = null;
        try {
            long leafRecid = DirTree.treeGet(dirShift, segmentRoots[seg], store, levels - 1, idx);
            if (leafRecid == 0) return null;
            long valueRecid = findValueRecid(leafRecid, key);
            if (valueRecid == 0) return null;
            V old = store.get(valueRecid, valueSer);
            if (expectedValue != null && !valueSer.equals(old, expectedValue)) return null;
            store.update(valueRecid, newValue, valueSer); // leaf untouched
            runtime.fire(key, old, newValue, false);
            return old;
        } catch (RuntimeException | Error t) {
            body = t;
            throw t;
        } finally {
            unlockWrite(seg, body);
        }
    }

    /** Remove all entries: frees every value record, bucket record and subdir record;
     *  segment root recids stay stable (§0.6). */
    @SuppressWarnings("unchecked")
    @Override public void clear() {
        clearInternal(true, false);
    }

    public void clearWithoutNotification() { clearInternal(false, false); }
    public void clearWithExpire() { clearInternal(true, true); }

    @SuppressWarnings("unchecked")
    private void clearInternal(boolean notify, boolean triggered) {
        // a throwing LISTENER (sync in the batch, or deferred inside unlockWrite) must not
        // abort the loop and leave later segments populated (their external value records
        // still allocated) — its segment's removal already committed, so capture, keep
        // clearing, rethrow after. A MUTATION/store failure must abort: the current segment
        // is inconsistent, and continuing would destructively erase healthy later segments.
        Throwable listenerFail = null;
        for (int seg = 0; seg < segmentRoots.length; seg++) {
            lockWrite(seg);
            // collect first, fire only AFTER the whole traversal commits: treeClear detaches
            // the root before visiting, so a throwing listener inside the visitor would orphan
            // every unvisited subtree AND leak its external value records; batch delivery also
            // keeps event sets complete
            java.util.List<Object[]> removed = notify ? new java.util.ArrayList<>() : null;
            try {
                DirTree.treeClear(segmentRoots[seg], store, levels - 1, (index, leafRecid) -> {
                    Object[] leaf = store.get(leafRecid, leafSer);
                    for (int i = 0; i < leaf.length; i += 2) {
                        long valueRecid = (Long) leaf[i + 1];
                        if (removed != null)
                            removed.add(new Object[]{leaf[i], store.get(valueRecid, valueSer)});
                        store.delete(valueRecid, valueSer);
                    }
                    store.delete(leafRecid, leafSer);
                });
            } catch (RuntimeException | Error t) {
                unlockWrite(seg, t);
                if (listenerFail != null) MapRuntime.suppress(t, listenerFail);
                throw t;
            }
            try {
                if (removed != null) runtime.fireRemovalBatch(removed, triggered, false);
            } catch (RuntimeException | Error t) {
                listenerFail = MapRuntime.suppress(listenerFail, t);
            } finally {
                try {
                    unlockWrite(seg);
                } catch (RuntimeException | Error t) {
                    listenerFail = MapRuntime.suppress(listenerFail, t);
                }
            }
        }
        MapRuntime.rethrow(listenerFail);
    }

    /** True iff the map has no entries; reads only each root's bitmaps. */
    @Override public boolean isEmpty() {
        for (int seg = 0; seg < segmentRoots.length; seg++) {
            lockRead(seg);
            try {
                if (!DirTree.treeIsEmpty(segmentRoots[seg], store)) return false;
            } finally {
                unlockRead(seg);
            }
        }
        return true;
    }

    // ================= iteration =================

    /** Snapshot one segment's entries under its read lock (§0.5); values are resolved
     *  inside the lock so their recids are still live. */
    @SuppressWarnings("unchecked")
    private List<Map.Entry<K, V>> drainSegment(int seg) {
        ArrayList<Map.Entry<K, V>> out = new ArrayList<>();
        lockRead(seg);
        try {
            DirTree.treeFold(segmentRoots[seg], store, levels - 1, (index, leafRecid) -> {
                Object[] leaf = store.get(leafRecid, leafSer);
                for (int i = 0; i < leaf.length; i += 2) {
                    V value = store.get((Long) leaf[i + 1], valueSer);
                    out.add(new AbstractMap.SimpleImmutableEntry<>((K) leaf[i], value));
                }
            });
        } finally {
            unlockRead(seg);
        }
        return out;
    }

    /** Unordered entry iterator; snapshot-per-segment, weakly consistent (§0.5).
     *  {@link Iterator#remove()} deletes the last-returned key from the live map, so
     *  the entrySet/keySet/values views are mutable. */
    public Iterator<Map.Entry<K, V>> entryIterator() {
        return new Iterator<>() {
            private int seg = 0;
            private Iterator<Map.Entry<K, V>> current = Collections.emptyIterator();
            private K lastKey;
            private boolean removable;

            @Override public boolean hasNext() {
                while (!current.hasNext() && seg < segmentRoots.length) {
                    current = drainSegment(seg++).iterator();
                }
                return current.hasNext();
            }

            @Override public Map.Entry<K, V> next() {
                if (!hasNext()) throw new NoSuchElementException();
                Map.Entry<K, V> e = current.next();
                lastKey = e.getKey();
                removable = true;
                return e;
            }

            @Override public void remove() {
                if (!removable) throw new IllegalStateException();
                removable = false;
                HTreeMapExternal.this.remove(lastKey);
            }
        };
    }

    /** Unordered traversal; the callback runs OUTSIDE segment locks and may mutate the map. */
    @Override public void forEach(BiConsumer<? super K, ? super V> action) {
        for (int seg = 0; seg < segmentRoots.length; seg++) {
            for (Map.Entry<K, V> e : drainSegment(seg)) {
                action.accept(e.getKey(), e.getValue());
            }
        }
    }

    /** Entry count: sums bucket sizes per segment under its read lock. */
    public long sizeLong() {
        long total = 0;
        for (int seg = 0; seg < segmentRoots.length; seg++) {
            lockRead(seg);
            try {
                long[] acc = new long[1];
                DirTree.treeFold(segmentRoots[seg], store, levels - 1, (index, leafRecid) ->
                        acc[0] += store.get(leafRecid, leafSer).length >>> 1);
                total += acc[0];
            } finally {
                unlockRead(seg);
            }
        }
        return total;
    }

    /** {@link Map#size()}: saturates at {@link Integer#MAX_VALUE}; use {@link #sizeLong}
     *  for the exact count of a map with more than 2^31-1 entries. */
    @Override public int size() {
        long n = sizeLong();
        return n > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) n;
    }

    // ================= collection views =================

    /** Weakly consistent entry-set view (§0.5): iteration is {@link #entryIterator},
     *  the rest delegates to the map. {@code AbstractMap} derives keySet/values/
     *  containsValue/putAll from this. */
    @Override public Set<Map.Entry<K, V>> entrySet() {
        return new AbstractSet<>() {
            @Override public Iterator<Map.Entry<K, V>> iterator() { return entryIterator(); }
            @Override public int size() { return HTreeMapExternal.this.size(); }
            @Override public boolean isEmpty() { return HTreeMapExternal.this.isEmpty(); }
            @Override public void clear() { HTreeMapExternal.this.clear(); }

            @Override public boolean contains(Object o) {
                if (!(o instanceof Map.Entry)) return false;
                Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
                Object k = e.getKey(), v = e.getValue();
                if (k == null || v == null) return false;
                V cur = get(k);
                return cur != null && valueSer.equals(cur, castValue(v));
            }

            @Override public boolean remove(Object o) {
                if (!(o instanceof Map.Entry)) return false;
                Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
                Object k = e.getKey(), v = e.getValue();
                return k != null && v != null && HTreeMapExternal.this.remove(k, v);
            }
        };
    }

    @SuppressWarnings("unchecked")
    private V castValue(Object v) { return (V) v; }

    // ================= record formats =================

    /**
     * Bucket wire format: packInt(entryCount), then (key, packLong(valueRecid)) per
     * entry. Values are NOT in the bucket — that is the point of this class.
     */
    private static <K> Serializer<Object[]> leafSerializer(Serializer<K> keySer) {
        return new Serializer<>() {

        @SuppressWarnings("unchecked")
        @Override public void serialize(DataOutput2 out, Object[] leaf) {
            assert leaf.length >= 2 && leaf.length % 2 == 0;
            out.packInt(leaf.length >>> 1);
            for (int i = 0; i < leaf.length; i += 2) {
                keySer.serialize(out, (K) leaf[i]);
                out.packLong((Long) leaf[i + 1]);
            }
        }

        @Override public Object[] deserialize(DataInput2 in, int size) {
            int n = in.unpackInt();
            Object[] leaf = new Object[n * 2];
            for (int i = 0; i < leaf.length; i += 2) {
                leaf[i] = keySer.deserialize(in, -1);
                leaf[i + 1] = in.unpackLong();
            }
            return leaf;
        }
        };
    }

    /** Immutable map metadata; written once at create (§0.6). Same shape as
     *  HTreeMap's header, duplicated on purpose — the two classes stay independent. */
    private static final class Header {
        final int concShift, dirShift, levels, hashSeed;
        final long[] segmentRoots;

        Header(int concShift, int dirShift, int levels, int hashSeed, long[] segmentRoots) {
            this.concShift = concShift;
            this.dirShift = dirShift;
            this.levels = levels;
            this.hashSeed = hashSeed;
            this.segmentRoots = segmentRoots;
            assert segmentRoots.length == 1 << concShift;
        }

        static final Serializer<Header> SER = new Serializer<>() {

            @Override public void serialize(DataOutput2 out, Header h) {
                out.packInt(h.concShift);
                out.packInt(h.dirShift);
                out.packInt(h.levels);
                out.writeInt(h.hashSeed);
                for (long root : h.segmentRoots) out.packLong(root);
            }

            @Override public Header deserialize(DataInput2 in, int size) {
                int concShift = in.unpackInt();
                int dirShift = in.unpackInt();
                int levels = in.unpackInt();
                int hashSeed = in.readInt();
                long[] roots = new long[1 << concShift];
                for (int i = 0; i < roots.length; i++) roots[i] = in.unpackLong();
                return new Header(concShift, dirShift, levels, hashSeed, roots);
            }
        };
    }
}
