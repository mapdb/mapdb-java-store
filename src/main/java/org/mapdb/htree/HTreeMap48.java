package org.mapdb.htree;

import org.mapdb.MapModificationListener;
import org.mapdb.ModificationAwareMap;
import org.mapdb.MapExtra;
import org.mapdb.hash.Hasher64;
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
 * {@link HTreeMap} variant with a 48-BIT hash budget — for maps whose index space
 * outgrows the 32-bit partition (the 32-bit map saturates around 10^9 entries:
 * buckets then grow linearly and lookups degrade to scans). A self-contained
 * duplicate of the lean HTreeMap (maintainer steer: separate classes over flags in
 * the hot paths); {@link DirTree} runs on long indices already and is shared
 * unchanged.
 *
 * Hashing: an external {@link Hasher64} maps (key, persisted long seed) to 64 mixed
 * bits; the map consumes the TOP 48 ({@code hash >>> 16}), partitioned
 * {@code concShift + levels*dirShift == 48}. Defaults 6 / 6 / 7 (64 segments,
 * 7 levels x 6 bits, 64-slot dirs). Why 48 and not 64: terminal dir entries persist
 * their index as a packed long, and uniform random indices cost ~2 packed bytes more
 * at 42 bits vs 28 — but ~4-5 more at 60 bits. 48 buys ~16000x the index space of
 * the 32-bit map for roughly half the byte growth of full 64 (HTreeMap at scale is
 * memory-bandwidth-bound; dir bytes are traffic).
 *
 * ENTROPY: the default {@link Hashers#objectHasher64()} derives from the 32-bit
 * {@code Object.hashCode()}, so collision behavior is no better than HTreeMap's —
 * fine for compatibility, pointless at scale. Maps that NEED the 48-bit space must
 * supply a genuinely 64-bit hasher: {@link Hashers#LONG64}, {@link Hashers#STRING64},
 * {@link Hashers#BYTE_ARRAY64} or {@link Hashers#mixing64}.
 *
 * Depth does not regress for small maps: path compression stores a lone index where
 * it first becomes unique, so actual dir depth stays ~log_slots(N/segments) — the
 * extra levels materialize only when entries genuinely collide that deep.
 *
 * Everything else — locking, torn-read discipline, iteration snapshots, persistence,
 * the one-live-mutable-handle contract — is HTreeMap's (spec-htreemap §0.2–0.8);
 * see its javadoc. The header is NOT interchangeable with HTreeMap's (long seed,
 * different budget); a headerRecid belongs to exactly one of the two classes.
 *
 * Implements {@link java.util.Map} and {@link java.util.concurrent.ConcurrentMap}
 * with the same weak-consistency and {@link #size} saturation caveats as
 * {@link HTreeMap} — see its javadoc.
 */
public final class HTreeMap48<K, V> extends AbstractMap<K, V> implements MapExtra<K, V> {

    /** Bits of hash the map consumes (top slice of the 64-bit hash). */
    static final int HASH_BITS = 48;

    private final Store store;
    private final Serializer<K> keySer;
    private final Serializer<V> valueSer;

    private final int concShift, dirShift, levels;
    private final long hashSeed;
    private final long headerRecid;
    private final long[] segmentRoots;
    private final Hasher64<? super K> hasher;
    private final ReentrantReadWriteLock[] locks;
    private final boolean threadSafe;

    private final int segmentShift;
    private final long indexMask;
    private final int concMask;

    private final Serializer<Object[]> leafSer;
    private final MapRuntime<K, V> runtime = new MapRuntime<>();
    /** Same §0.11 gate as HTreeMap: lock-free optimistic leaf reads only when both
     *  elements are fixed-size (torn reads then can't over-allocate). */
    private final boolean leafReadOptimistic;

    private HTreeMap48(Store store, Serializer<K> keySer, Serializer<V> valueSer,
                       long headerRecid, Header header, Hasher64<? super K> hasher) {
        this.store = store;
        this.keySer = keySer;
        this.valueSer = valueSer;
        this.headerRecid = headerRecid;
        this.concShift = header.concShift;
        this.dirShift = header.dirShift;
        this.levels = header.levels;
        this.hashSeed = header.hashSeed;
        this.segmentRoots = header.segmentRoots;
        this.hasher = hasher != null ? hasher : Hashers.objectHasher64();
        this.segmentShift = levels * dirShift;
        this.indexMask = (1L << segmentShift) - 1;
        this.concMask = (1 << concShift) - 1;
        this.threadSafe = store.isThreadSafe();
        this.locks = new ReentrantReadWriteLock[1 << concShift];
        for (int i = 0; i < locks.length; i++) locks[i] = new ReentrantReadWriteLock();
        this.leafSer = leafSerializer(keySer, valueSer);
        this.leafReadOptimistic = keySer.fixedSize() > 0 && valueSer.fixedSize() > 0;
    }

    private static void checkConfig(int concShift, int dirShift, int levels) {
        // hard cap 12 (4096 segments): segments are lock granularity, more was measured
        // as a cache-scatter regression — and past 30 the int shifts would wrap mod 32
        // and silently alias every segment to 0 (48-bit budget leaves room for both)
        if (concShift < 0 || concShift > 12)
            throw new IllegalArgumentException("concShift must be in [0,12]");
        if (dirShift < 1 || dirShift > DirTree.MAX_DIR_SHIFT)
            throw new IllegalArgumentException("dirShift must be in [1," + DirTree.MAX_DIR_SHIFT + "]");
        if (levels < 1) throw new IllegalArgumentException("levels must be >= 1");
        // full 48-bit partition; anything less silently drops hash bits (§0.4)
        if (concShift + levels * dirShift != HASH_BITS)
            throw new IllegalArgumentException("concShift + levels*dirShift must equal " + HASH_BITS
                    + ", got " + concShift + " + " + levels + "*" + dirShift);
    }

    // ================= create / open =================

    /** Defaults: 64 segments, 7 dir levels x 6 bits (6 + 7*6 = 48). */
    public static <K, V> HTreeMap48<K, V> create(Store store, Serializer<K> keySer,
                                                 Serializer<V> valueSer) {
        return create(store, keySer, valueSer, 6, 6, 7, null);
    }

    /** Default geometry with a caller-supplied 64-bit hasher (see class doc: required
     *  for genuine >32-bit entropy). */
    public static <K, V> HTreeMap48<K, V> create(Store store, Serializer<K> keySer,
                                                 Serializer<V> valueSer,
                                                 Hasher64<? super K> hasher) {
        return create(store, keySer, valueSer, 6, 6, 7, hasher);
    }

    /** @param hasher custom {@link Hasher64}; null = {@link Hashers#objectHasher64()}
     *                (32-bit entropy cap — see class doc). Must be supplied identically
     *                on every open of this map. */
    public static <K, V> HTreeMap48<K, V> create(Store store, Serializer<K> keySer,
                                                 Serializer<V> valueSer,
                                                 int concShift, int dirShift, int levels,
                                                 Hasher64<? super K> hasher) {
        return create(store, keySer, valueSer, concShift, dirShift, levels,
                new Random().nextLong(), hasher);
    }

    /** Create with an explicit persisted 64-bit hash seed. */
    public static <K, V> HTreeMap48<K, V> create(Store store, Serializer<K> keySer,
                                          Serializer<V> valueSer,
                                          int concShift, int dirShift, int levels,
                                          long hashSeed, Hasher64<? super K> hasher) {
        checkConfig(concShift, dirShift, levels);
        long[] roots = new long[1 << concShift];
        for (int i = 0; i < roots.length; i++) {
            roots[i] = store.put(DirTree.dirEmpty(), DirTree.DIR_SER);
        }
        Header header = new Header(concShift, dirShift, levels, hashSeed, roots);
        long headerRecid = store.put(header, Header.SER);
        return new HTreeMap48<>(store, keySer, valueSer, headerRecid, header, hasher);
    }

    /**
     * Bulk-load a 48-bit map from entries sorted by non-descending unsigned 64-bit
     * hash computed with the same {@code hashSeed} and {@code hasher}. The map uses
     * the top 48 bits of that hash, so full-64 sorting is a valid finer order. The
     * factory does not sort, commit, or close the store.
     */
    public static <K, V> HTreeMap48<K, V> createFromSortedByHash(
            Store store, Serializer<K> keySer, Serializer<V> valueSer,
            int concShift, int dirShift, int levels,
            long hashSeed, Hasher64<? super K> hasher,
            Iterator<? extends Map.Entry<K, V>> entries) {
        checkConfig(concShift, dirShift, levels);
        Hasher64<? super K> h = hasher != null ? hasher : Hashers.objectHasher64();
        Serializer<Object[]> leafSer = leafSerializer(keySer, valueSer);
        HTreePump.BuildResult result = HTreePump.build(store, concShift, dirShift, levels, entries,
                key -> h.hash(key, hashSeed) >>> (64 - HASH_BITS),
                keySer,
                leaf -> store.put(leaf, leafSer),
                roots -> store.put(new Header(concShift, dirShift, levels, hashSeed, roots), Header.SER));
        Header header = new Header(concShift, dirShift, levels, hashSeed, result.segmentRoots);
        return new HTreeMap48<>(store, keySer, valueSer, result.headerRecid, header, h);
    }

    public static <K, V> HTreeMap48<K, V> open(Store store, long headerRecid,
                                               Serializer<K> keySer, Serializer<V> valueSer) {
        return open(store, headerRecid, keySer, valueSer, null);
    }

    /** @param hasher must match the hasher supplied at create (or null if none was). */
    public static <K, V> HTreeMap48<K, V> open(Store store, long headerRecid,
                                               Serializer<K> keySer, Serializer<V> valueSer,
                                               Hasher64<? super K> hasher) {
        Header header = store.get(headerRecid, Header.SER);
        return new HTreeMap48<>(store, keySer, valueSer, headerRecid, header, hasher);
    }

    /** Recid of the header record; persist this to reopen the map. */
    public long headerRecid() { return headerRecid; }

    // ================= hashing =================

    /** Top 48 bits of the 64-bit hash — the best-mixed slice, and keeping segment
     *  selection at the very top mirrors the 32-bit map's layout. */
    private long hash48(K key) {
        return hasher.hash(key, hashSeed) >>> (64 - HASH_BITS);
    }

    private int segment(long hash48) {
        return (int) (hash48 >>> segmentShift) & concMask;
    }

    private long index(long hash48) {
        return hash48 & indexMask;
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

    // ================= get / put =================

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
        long h = hash48(key);
        int seg = segment(h);
        long idx = index(h);
        lockRead(seg);
        try {
            long leafRecid = DirTree.treeGet(dirShift, segmentRoots[seg], store, levels - 1, idx);
            if (leafRecid == 0) return null;
            if (leafReadOptimistic) { // see HTreeMap.get: lock-free leaf read (§0.11)
                LeafGetAction action = new LeafGetAction(key);
                store.read(leafRecid, action);
                return action.found ? action.value : null;
            }
            @SuppressWarnings("unchecked")
            Object[] leaf = store.get(leafRecid, leafSer);
            for (int i = 0; i < leaf.length; i += 2) {
                if (keySer.equals((K) leaf[i], key)) return (V) leaf[i + 1];
            }
            return null;
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

    /** Push-down bucket lookup; identical discipline to HTreeMap.LeafGetAction
     *  (resets state on every invocation, clamps the entry count — §0.11). */
    private final class LeafGetAction implements RecordRead {
        final K key;
        V value;
        boolean found;

        LeafGetAction(K key) { this.key = key; }

        @Override public long onBytes(DataInput2 in, int size) {
            found = false;
            value = null;
            int n = in.unpackInt();
            if (n > size) throw new IllegalStateException("leaf entry count exceeds record size");
            for (int i = 0; i < n; i++) {
                K k = keySer.deserialize(in, -1);
                V v = valueSer.deserialize(in, -1);
                if (keySer.equals(k, key)) { found = true; value = v; return 1; }
            }
            return 0;
        }

        @SuppressWarnings("unchecked")
        @Override public long onObject(Object record) {
            found = false;
            value = null;
            Object[] leaf = (Object[]) record;
            for (int i = 0; i < leaf.length; i += 2) {
                if (keySer.equals((K) leaf[i], key)) { found = true; value = (V) leaf[i + 1]; return 1; }
            }
            return 0;
        }

        @Override public long onNull() { found = false; value = null; return 0; }
    }

    /** True iff the key has an entry; same lock-free optimistic leaf path as {@link #get}. */
    @SuppressWarnings("unchecked")
    @Override public boolean containsKey(Object keyObj) {
        if (keyObj == null) throw new NullPointerException();
        K key = (K) keyObj;
        long h = hash48(key);
        int seg = segment(h);
        long idx = index(h);
        lockRead(seg);
        try {
            long leafRecid = DirTree.treeGet(dirShift, segmentRoots[seg], store, levels - 1, idx);
            if (leafRecid == 0) return false;
            if (leafReadOptimistic) {
                LeafGetAction action = new LeafGetAction(key);
                store.read(leafRecid, action);
                return action.found;
            }
            Object[] leaf = store.get(leafRecid, leafSer);
            for (int i = 0; i < leaf.length; i += 2) {
                if (keySer.equals((K) leaf[i], key)) return true;
            }
            return false;
        } finally {
            unlockRead(seg);
        }
    }

    /** Insert or replace; returns the previous value or null. */
    @Override public V put(K key, V value) {
        if (key == null || value == null) throw new NullPointerException();
        return putInternal(key, value, false);
    }

    /** Insert only if absent; returns the existing value, or null if this call inserted. */
    @Override public V putIfAbsent(K key, V value) {
        if (key == null || value == null) throw new NullPointerException();
        return putInternal(key, value, true);
    }

    /** Blind put — insert/overwrite WITHOUT returning the previous value (JCache's
     *  {@code void put}; not in {@code java.util.Map}). Same cost as {@link #put} here
     *  (inline values); the read-skipping win is on {@link HTreeMapExternal#putOnly}. */
    public void putOnly(K key, V value) {
        if (key == null || value == null) throw new NullPointerException();
        putInternal(key, value, false);
    }

    @SuppressWarnings("unchecked")
    private V putInternal(K key, V value, boolean onlyIfAbsent) {
        long h = hash48(key);
        int seg = segment(h);
        long idx = index(h);
        lockWrite(seg);
        Throwable body = null;
        try {
            long leafRecid = DirTree.treeGet(dirShift, segmentRoots[seg], store, levels - 1, idx);
            if (leafRecid == 0) { // fresh bucket: write leaf first, then link it
                leafRecid = store.put(new Object[]{key, value}, leafSer);
                DirTree.treePut(dirShift, segmentRoots[seg], store, levels - 1, idx, leafRecid);
                runtime.fire(key, null, value, false);
                return null;
            }
            Object[] leaf = store.get(leafRecid, leafSer);
            for (int i = 0; i < leaf.length; i += 2) {
                if (keySer.equals((K) leaf[i], key)) { // key present
                    V old = (V) leaf[i + 1];
                    if (onlyIfAbsent) return old;
                    Object[] leaf2 = leaf.clone(); // never mutate: heap stores alias
                    leaf2[i + 1] = value;
                    store.update(leafRecid, leaf2, leafSer);
                    runtime.fire(key, old, value, false);
                    return old;
                }
            }
            // full-index hash collision: grow the bucket by one entry
            Object[] leaf2 = java.util.Arrays.copyOf(leaf, leaf.length + 2);
            leaf2[leaf.length] = key;
            leaf2[leaf.length + 1] = value;
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

    /** Blind remove — delete the key's entry, returning only WHETHER one existed (JCache's
     *  {@code boolean remove}). Same cost as {@link #remove(Object)} here. */
    public boolean removeOnly(K key) {
        if (key == null) throw new NullPointerException();
        return removeInternal(key, null) != null;
    }

    @SuppressWarnings("unchecked")
    private V removeInternal(K key, V expectedValue) {
        long h = hash48(key);
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
                V old = (V) leaf[i + 1];
                if (expectedValue != null && !valueSer.equals(old, expectedValue)) return null;
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
        long h = hash48(key);
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
                V old = (V) leaf[i + 1];
                if (expectedValue != null && !valueSer.equals(old, expectedValue)) return null;
                Object[] leaf2 = leaf.clone(); // never mutate: heap stores alias
                leaf2[i + 1] = newValue;
                store.update(leafRecid, leaf2, leafSer);
                runtime.fire(key, old, newValue, false);
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

    /** Remove all entries; segment root recids stay stable (§0.6). */
    @Override public void clear() {
        clearInternal(true, false);
    }

    public void clearWithoutNotification() { clearInternal(false, false); }
    public void clearWithExpire() { clearInternal(true, true); }

    @SuppressWarnings("unchecked")
    private void clearInternal(boolean notify, boolean triggered) {
        // a throwing LISTENER (sync in the batch, or deferred inside unlockWrite) must not
        // abort the loop and leave later segments populated — its segment's removal already
        // committed, so capture, keep clearing, rethrow after. A MUTATION/store failure must
        // abort: the current segment is inconsistent, and continuing would destructively
        // erase healthy later segments behind it.
        Throwable listenerFail = null;
        for (int seg = 0; seg < segmentRoots.length; seg++) {
            lockWrite(seg);
            // collect first, fire only AFTER the whole traversal commits: treeClear detaches
            // the root before visiting, so a throwing listener inside the visitor would orphan
            // every unvisited subtree; batch delivery also keeps event sets complete
            java.util.List<Object[]> removed = notify ? new java.util.ArrayList<>() : null;
            try {
                DirTree.treeClear(segmentRoots[seg], store, levels - 1,
                        (index, leafRecid) -> {
                            if (removed != null) {
                                Object[] leaf = store.get(leafRecid, leafSer);
                                for (int i = 0; i < leaf.length; i += 2)
                                    removed.add(new Object[]{leaf[i], leaf[i + 1]});
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

    /** Snapshot one segment's entries under its read lock (§0.5). */
    @SuppressWarnings("unchecked")
    private List<Map.Entry<K, V>> drainSegment(int seg) {
        ArrayList<Map.Entry<K, V>> out = new ArrayList<>();
        lockRead(seg);
        try {
            DirTree.treeFold(segmentRoots[seg], store, levels - 1, (index, leafRecid) -> {
                Object[] leaf = store.get(leafRecid, leafSer);
                for (int i = 0; i < leaf.length; i += 2) {
                    out.add(new AbstractMap.SimpleImmutableEntry<>((K) leaf[i], (V) leaf[i + 1]));
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
                HTreeMap48.this.remove(lastKey);
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
            @Override public int size() { return HTreeMap48.this.size(); }
            @Override public boolean isEmpty() { return HTreeMap48.this.isEmpty(); }
            @Override public void clear() { HTreeMap48.this.clear(); }

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
                return k != null && v != null && HTreeMap48.this.remove(k, v);
            }
        };
    }

    @SuppressWarnings("unchecked")
    private V castValue(Object v) { return (V) v; }

    // ================= record formats =================

    /** Bucket wire format: identical to HTreeMap's (packInt count, inline key/value
     *  pairs) — only the dir INDEX width differs between the two classes. */
    private static <K, V> Serializer<Object[]> leafSerializer(Serializer<K> keySer,
                                                              Serializer<V> valueSer) {
        return new Serializer<>() {

        @SuppressWarnings("unchecked")
        @Override public void serialize(DataOutput2 out, Object[] leaf) {
            assert leaf.length >= 2 && leaf.length % 2 == 0;
            out.packInt(leaf.length >>> 1);
            for (int i = 0; i < leaf.length; i += 2) {
                keySer.serialize(out, (K) leaf[i]);
                valueSer.serialize(out, (V) leaf[i + 1]);
            }
        }

        @Override public Object[] deserialize(DataInput2 in, int size) {
            int n = in.unpackInt();
            Object[] leaf = new Object[n * 2];
            for (int i = 0; i < leaf.length; i += 2) {
                leaf[i] = keySer.deserialize(in, -1);
                leaf[i + 1] = valueSer.deserialize(in, -1);
            }
            return leaf;
        }
        };
    }

    /** Immutable map metadata; written once at create (§0.6). Differs from HTreeMap's
     *  header: LONG hashSeed (64-bit hasher salt). */
    private static final class Header {
        final int concShift, dirShift, levels;
        final long hashSeed;
        final long[] segmentRoots;

        Header(int concShift, int dirShift, int levels, long hashSeed, long[] segmentRoots) {
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
                out.writeLong(h.hashSeed); // may be negative: raw long, not packed
                for (long root : h.segmentRoots) out.packLong(root);
            }

            @Override public Header deserialize(DataInput2 in, int size) {
                int concShift = in.unpackInt();
                int dirShift = in.unpackInt();
                int levels = in.unpackInt();
                long hashSeed = in.readLong();
                long[] roots = new long[1 << concShift];
                for (int i = 0; i < roots.length; i++) roots[i] = in.unpackLong();
                return new Header(concShift, dirShift, levels, hashSeed, roots);
            }
        };
    }
}
