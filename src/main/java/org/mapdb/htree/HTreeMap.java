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
 * Segmented hash tree over a Store4 store implementing {@link java.util.Map} and
 * {@link java.util.concurrent.ConcurrentMap} (get/put/remove, ConcurrentMap CAS,
 * containsKey/clear/iterate; no expiration/listeners — see {@link HTreeCache} for
 * those). Follows the mapdb3 HTreeMap lineage, minimal scope, matching the
 * {@link org.mapdb.btree.BTreeMap} skeleton's altitude.
 *
 * As a {@code ConcurrentMap}: null keys and values are rejected (NPE), the
 * collection views ({@link #entrySet}/{@link #keySet}/{@link #values}) and
 * {@link #size} are weakly consistent (backed by the snapshot-per-segment iterator,
 * §0.5), and {@link #size} saturates at {@link Integer#MAX_VALUE} — use
 * {@link #sizeLong} for the exact count.
 *
 * Structure — two layers stacked:
 *  - SEGMENTATION: the top {@code concShift} bits of the mixed 32-bit key hash pick
 *    one of {@code 1<<concShift} segments; each segment is an independent hash tree
 *    guarded by its own ReadWriteLock (like ConcurrentHashMap's classic segments).
 *  - HASH DIRECTORY TREE (per segment, {@link DirTree}): an auto-expanding sparse
 *    tree keyed by the remaining {@code levels*dirShift} hash bits. Unlike an array
 *    hash table it never rehashes: single occupants are stored where their index
 *    first becomes unique (path compression) and collisions split lazily one level
 *    at a time. Tree values are recids of BUCKET records: flat {@code [k0,v0,k1,v1,…]}
 *    lists of entries whose keys share the full hash index; key equality inside a
 *    bucket is a linear scan with {@code keySer.equals}.
 *
 * A lookup is: mix(key.hashCode) → segment → walk dir tree by index bits → bucket
 * record → linear key scan. Only hashes live in the tree, so lookups never
 * deserialize foreign keys except within the terminal bucket.
 *
 * Hashing (spec-htreemap §0.2/§0.3): an external {@link Hasher} maps (key, persisted
 * seed) to fully-mixed hash bits — default {@link Hashers#objectHasher()} (seeded
 * fmix32 over hashCode), which requires keys whose hashCode/equals agree with
 * {@code keySer.equals} (Long, String, boxed primitives); byte[] keys need a content
 * hasher ({@link Hashers#mixing}).
 *
 * Concurrency: get/iteration take the segment read lock, put the write lock; locks
 * are no-ops when {@code !store.isThreadSafe()}. Iteration drains one segment at a
 * time under its read lock and yields from the snapshot after release — weakly
 * consistent across segments, and callbacks may safely mutate the map (§0.5). The
 * per-segment locks confine only THIS instance: at most one live HTreeMap handle per
 * (store, headerRecid) may mutate at a time unless externally synchronized (§0.7).
 *
 * Persistence: {@code create} allocates one empty dir root per segment plus a header
 * record {@code {concShift, dirShift, levels, hashSeed, segmentRoots[]}}; persist
 * {@link #headerRecid()} and {@code open} to reattach. Dir roots are only ever
 * updated in place, so the header is immutable after create (§0.6).
 */
public final class HTreeMap<K, V> extends AbstractMap<K, V> implements MapExtra<K, V> {

    private final Store store;
    private final Serializer<K> keySer;
    private final Serializer<V> valueSer;

    /** Segment bits / slot bits per dir level / number of dir levels; the full
     *  32-bit hash is partitioned: concShift + levels*dirShift == 32 (§0.4). */
    private final int concShift, dirShift, levels;
    /** Mixed into every hash; random at create, persisted so reopen hashes identically. */
    private final int hashSeed;
    private final long headerRecid;
    /** Dir-tree root recid per segment; stable after create (§0.6). */
    private final long[] segmentRoots;
    private final Hasher<? super K> hasher;
    private final ReentrantReadWriteLock[] locks;
    private final boolean threadSafe;

    /** levels*dirShift: bits of hash consumed by the dir tree. */
    private final int segmentShift;
    private final long indexMask;
    private final int concMask;

    private final Serializer<Object[]> leafSer;
    /** get() may use the lock-free optimistic leaf read only when both elements are
     *  fixed-size — then a torn optimistic read reads bounded bytes and cannot
     *  over-allocate from a garbage length prefix (§0.11). Variable-length keys/values
     *  keep the locked store.get path. */
    private final boolean leafReadOptimistic;
    private final MapRuntime<K, V> runtime = new MapRuntime<>();

    private HTreeMap(Store store, Serializer<K> keySer, Serializer<V> valueSer,
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
        this.leafSer = leafSerializer(keySer, valueSer);
        this.leafReadOptimistic = keySer.fixedSize() > 0 && valueSer.fixedSize() > 0;
    }

    private static void checkConfig(int concShift, int dirShift, int levels) {
        // hard cap 12 (4096 segments): segments are lock granularity, more was measured
        // as a cache-scatter regression (spec-htreemap STATUS.md reverted-experiments)
        if (concShift < 0 || concShift > 12)
            throw new IllegalArgumentException("concShift must be in [0,12]");
        if (dirShift < 1 || dirShift > DirTree.MAX_DIR_SHIFT)
            throw new IllegalArgumentException("dirShift must be in [1," + DirTree.MAX_DIR_SHIFT + "]");
        if (levels < 1) throw new IllegalArgumentException("levels must be >= 1");
        // full 32-bit partition; anything less silently drops hash bits (§0.4)
        if (concShift + levels * dirShift != 32)
            throw new IllegalArgumentException("concShift + levels*dirShift must equal 32, got "
                    + concShift + " + " + levels + "*" + dirShift);
    }

    // ================= create / open =================

    /** Defaults: 16 segments, 4 dir levels x 7 bits (mapdb1's classic 4 + 4*7 = 32). */
    public static <K, V> HTreeMap<K, V> create(Store store, Serializer<K> keySer,
                                               Serializer<V> valueSer) {
        return create(store, keySer, valueSer, 4, 7, 4, null);
    }

    public static <K, V> HTreeMap<K, V> create(Store store, Serializer<K> keySer,
                                               Serializer<V> valueSer,
                                               int concShift, int dirShift, int levels) {
        return create(store, keySer, valueSer, concShift, dirShift, levels, null);
    }

    /**
     * @param hasher custom {@link Hasher} (e.g. {@link Hashers#mixing} over a content
     *               hash for byte[]-like keys); null = {@link Hashers#objectHasher()}.
     *               Must be supplied identically on every open of this map.
     */
    public static <K, V> HTreeMap<K, V> create(Store store, Serializer<K> keySer,
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
    public static <K, V> HTreeMap<K, V> create(Store store, Serializer<K> keySer,
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
        return new HTreeMap<>(store, keySer, valueSer, headerRecid, header, hasher);
    }

    /**
     * Bulk-load a map from entries sorted by non-descending unsigned 32-bit hash
     * computed with the same {@code hashSeed} and {@code hasher}. The builder writes
     * each bucket, dir node, segment root and header exactly once; it does not sort,
     * commit, close, or change read-path behavior. The resulting map is a normal live
     * handle, with the same lookup performance as an incrementally built map.
     */
    public static <K, V> HTreeMap<K, V> createFromSortedByHash(
            Store store, Serializer<K> keySer, Serializer<V> valueSer,
            int concShift, int dirShift, int levels,
            int hashSeed, Hasher<? super K> hasher,
            Iterator<? extends Map.Entry<K, V>> entries) {
        checkConfig(concShift, dirShift, levels);
        Hasher<? super K> h = hasher != null ? hasher : Hashers.objectHasher();
        Serializer<Object[]> leafSer = leafSerializer(keySer, valueSer);
        HTreePump.BuildResult result = HTreePump.build(store, concShift, dirShift, levels, entries,
                key -> Integer.toUnsignedLong(h.hash(key, hashSeed)),
                keySer,
                leaf -> store.put(leaf, leafSer),
                roots -> store.put(new Header(concShift, dirShift, levels, hashSeed, roots), Header.SER));
        Header header = new Header(concShift, dirShift, levels, hashSeed, result.segmentRoots);
        return new HTreeMap<>(store, keySer, valueSer, result.headerRecid, header, h);
    }

    public static <K, V> HTreeMap<K, V> open(Store store, long headerRecid,
                                             Serializer<K> keySer, Serializer<V> valueSer) {
        return open(store, headerRecid, keySer, valueSer, null);
    }

    /** @param hasher must match the hasher supplied at create (or null if none was). */
    public static <K, V> HTreeMap<K, V> open(Store store, long headerRecid,
                                             Serializer<K> keySer, Serializer<V> valueSer,
                                             Hasher<? super K> hasher) {
        Header header = store.get(headerRecid, Header.SER);
        return new HTreeMap<>(store, keySer, valueSer, headerRecid, header, hasher);
    }

    /** Recid of the header record; persist this to reopen the map. */
    public long headerRecid() { return headerRecid; }

    // ================= hashing (§0.3) =================

    /** All mixing lives in the {@link Hasher} (default: seeded fmix32 over hashCode,
     *  {@link Hashers#objectHasher}) — the HIGH bits pick the segment (§0.3). */
    private int hash(K key) {
        return hasher.hash(key, hashSeed);
    }

    private int segment(int hash) {
        return (hash >>> segmentShift) & concMask;
    }

    private long index(int hash) {
        // MUST widen unsigned before masking: a long mask over 28+ bits would
        // otherwise smear a negative int's sign bits into the index
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
        int h = hash(key);
        int seg = segment(h);
        long idx = index(h);
        lockRead(seg);
        try {
            long leafRecid = DirTree.treeGet(dirShift, segmentRoots[seg], store, levels - 1, idx);
            if (leafRecid == 0) return null;
            if (leafReadOptimistic) {
                // Lock-free leaf read via store.read: the whole get path is then
                // optimistic, dropping the segment readLock().lock()/unlock() CAS pair
                // that store.get writes to the StampedLock state word (~+8% aggregate
                // GET at 32 threads; neutral single-threaded — see HTreeScaleIT h4).
                // Gated on fixed-size elements so a TORN optimistic read can never
                // over-allocate from a garbage length prefix (§0.11).
                LeafGetAction action = new LeafGetAction(key);
                store.read(leafRecid, action);
                return action.found ? action.value : null;
            }
            // variable-length elements: locked read (torn-safe; store.get bounds-checks
            // under the segment lock, no length-prefixed allocation off torn bytes)
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

    public void modificationListenerAdd(MapModificationListener<K, V> listener) {
        runtime.add(listener);
    }

    public void modificationListenerRemove(MapModificationListener<K, V> listener) {
        runtime.remove(listener);
    }

    public void valueLoader(java.util.function.Function<? super K, ? extends V> loader) {
        runtime.valueLoader = loader;
    }
    @Override public boolean isClosed() { return store.isClosed(); }
    @Override public Serializer<K> keySerializer() { return keySer; }
    @Override public Serializer<V> valueSerializer() { return valueSer; }

    /**
     * Push-down bucket lookup (mirrors BTreeMap.GetAction): find {@code key} in a leaf
     * and stash its value without materializing the bucket array. Both dialects —
     * {@code onBytes} for byte stores, {@code onObject} for the live {@code Object[]}
     * on {@link org.mapdb.store.StoreOnHeap}.
     *
     * Optimistic-read discipline: the store may invoke this more than
     * once and on TORN bytes, so every invocation first fully resets {@code found}/
     * {@code value} (a torn hit must not survive the locked retry), and the entry count
     * is clamped against the record size so a torn header can't drive an unbounded scan.
     * Element deserialization of a torn record is bounded by the record's byte slice and
     * ends in an exception the store catches and retries under the lock.
     */
    private final class LeafGetAction implements RecordRead {
        final K key;
        V value;
        boolean found;

        LeafGetAction(K key) { this.key = key; }

        @Override public long onBytes(DataInput2 in, int size) {
            found = false;
            value = null;
            int n = in.unpackInt();
            // each entry is >= 2 serialized bytes (key + value), so n > size is torn/corrupt
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
            Object[] leaf = (Object[]) record; // heap store: live bucket array
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
        int h = hash(key);
        int seg = segment(h);
        long idx = index(h);
        lockRead(seg);
        try {
            long leafRecid = DirTree.treeGet(dirShift, segmentRoots[seg], store, levels - 1, idx);
            if (leafRecid == 0) return false;
            if (leafReadOptimistic) { // see get(): gated on fixed-size elements (§0.11)
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

    /**
     * Blind put — insert or overwrite WITHOUT returning the previous value (JCache's
     * {@code void put} semantics; there is no such method in {@code java.util.Map}).
     * On this inline-value map the old value is materialized as part of the bucket read
     * anyway, so this costs exactly the same as {@link #put}; it exists for API symmetry
     * with {@link HTreeMapExternal#putOnly}, where it genuinely skips reading the old
     * value record.
     */
    public void putOnly(K key, V value) {
        if (key == null || value == null) throw new NullPointerException();
        putInternal(key, value, false);
    }

    @SuppressWarnings("unchecked")
    private V putInternal(K key, V value, boolean onlyIfAbsent) {
        int h = hash(key);
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
                    if (onlyIfAbsent) return old; // putIfAbsent: leave as-is
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

    // ================= remove / replace (all read-then-act under the segment write lock) =================

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

    /** Blind remove — delete the key's entry, returning only WHETHER one existed, never
     *  the previous value (JCache's {@code boolean remove}). Same cost as
     *  {@link #remove(Object)} here; skips the old-value record read on
     *  {@link HTreeMapExternal}. */
    public boolean removeOnly(K key) {
        if (key == null) throw new NullPointerException();
        return removeInternal(key, null) != null;
    }

    /** @param expectedValue non-null = remove only on valueSer.equals match.
     *  @return the removed value, or null if nothing was removed (values are never
     *          null, so null is unambiguous). */
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
                V old = (V) leaf[i + 1];
                if (expectedValue != null && !valueSer.equals(old, expectedValue)) return null;
                if (leaf.length == 2) {
                    // last entry: unlink the bucket from the dir tree (collapsing empty
                    // dir nodes on the way out), THEN free the bucket record
                    boolean removed = DirTree.treeRemove(dirShift, segmentRoots[seg], store,
                            levels - 1, idx);
                    assert removed : "dir tree lost the bucket it just resolved";
                    store.delete(leafRecid, leafSer);
                } else { // shrink the bucket by one entry (reverse of the put grow path)
                    Object[] leaf2 = new Object[leaf.length - 2];
                    System.arraycopy(leaf, 0, leaf2, 0, i);
                    System.arraycopy(leaf, i + 2, leaf2, i, leaf2.length - i);
                    store.update(leafRecid, leaf2, leafSer);
                }
                runtime.fire(key, old, null, false);
                return old;
            }
            return null; // full-index collision but no key match
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

    /** @param expectedValue non-null = replace only on valueSer.equals match. */
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
            return null; // key absent
        } catch (RuntimeException | Error t) {
            body = t;
            throw t;
        } finally {
            unlockWrite(seg, body);
        }
    }

    /**
     * Remove all entries. Per segment under its write lock: frees every bucket record
     * and every subdir record in ONE traversal, resetting the segment root's CONTENT
     * to an empty dir in place — root recids stay stable (§0.6), so open handles and
     * the header remain valid.
     */
    @Override public void clear() {
        clearInternal(true, false);
    }

    /** Clear without emitting modification callbacks. */
    public void clearWithoutNotification() { clearInternal(false, false); }

    /** Clear and mark every removal callback as automatically triggered. */
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

    /** True iff the map has no entries. O(segments): reads only each root's bitmaps
     *  (empty root ⇔ empty segment, see DirTree.treeIsEmpty), short-circuiting on the
     *  first non-empty — far cheaper than {@link #sizeLong()}. */
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

    /**
     * Unordered entry iterator. Snapshot-per-segment: each segment is drained under
     * its read lock as the iteration reaches it, then yielded lock-free — weakly
     * consistent across segments, never blocks writers for longer than one drain.
     * {@link Iterator#remove()} deletes the last-returned key from the LIVE map (via
     * {@link #remove(Object)}), so the {@code entrySet}/{@code keySet}/{@code values}
     * views and their bulk {@code removeAll}/{@code retainAll} are all mutable.
     */
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
                HTreeMap.this.remove(lastKey);
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

    /**
     * Weakly consistent entry-set view (§0.5): iteration is the snapshot-per-segment
     * {@link #entryIterator}, {@code size}/{@code contains}/{@code remove}/{@code clear}
     * delegate to the map. {@code AbstractMap} builds {@link #keySet}, {@link #values},
     * {@link #containsValue} and {@link #putAll} on top of this.
     */
    @Override public Set<Map.Entry<K, V>> entrySet() {
        return new AbstractSet<>() {
            @Override public Iterator<Map.Entry<K, V>> iterator() { return entryIterator(); }
            @Override public int size() { return HTreeMap.this.size(); }
            @Override public boolean isEmpty() { return HTreeMap.this.isEmpty(); }
            @Override public void clear() { HTreeMap.this.clear(); }

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
                return k != null && v != null && HTreeMap.this.remove(k, v);
            }
        };
    }

    @SuppressWarnings("unchecked")
    private V castValue(Object v) { return (V) v; }

    // ================= record formats =================

    /**
     * Bucket wire format: packInt(entryCount), then (key, value) per entry, inline.
     * Element serializers get size=-1 (framed inside a larger record: self-delimit).
     */
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

    /** Immutable map metadata; written once at create (§0.6). */
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
                out.writeInt(h.hashSeed); // may be negative: raw int, not packed
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
