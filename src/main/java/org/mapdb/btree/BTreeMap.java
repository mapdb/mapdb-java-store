package org.mapdb.btree;

import org.mapdb.DBException;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;
import org.mapdb.ser.ColumnarValueFormat;
import org.mapdb.ser.GroupCursor;
import org.mapdb.ser.GroupFormat;
import org.mapdb.ser.LongFormat;
import org.mapdb.ser.Serializer;
import org.mapdb.ser.Serializers;
import org.mapdb.store.RecordRead;
import org.mapdb.store.Store;
import org.mapdb.store.StoreTx;

import java.util.AbstractMap;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;

/**
 * B-link tree over a Store4 store (get/put/remove/iterator — deliberately NOT the
 * full NavigableMap interface). Demonstrates the push-down architecture: the read
 * path executes format-owned search actions against store-resident bytes
 * ({@link GetAction}) — or against live Node objects on heap stores — without
 * materializing nodes.
 *
 * Structure (mapdb3 lineage, simplified):
 *  - root reached via double indirection: rootRecidRecid → (Long record) → root node recid
 *  - every non-rightmost node has {@code link} = recid of its right sibling; readers
 *    racing a split follow links (Lehman-Yao)
 *  - splits write the right sibling FIRST, then the left node, then the parent —
 *    referent before referrer, relying only on per-recid atomicity
 *
 * Directory node shape: non-rightmost dirs have childCount == keysLen (last key is
 * the node's inclusive high bound); rightmost dirs have childCount == keysLen + 1.
 * Dir high bounds are stable: separators are only ever inserted strictly inside a
 * dir's coverage and removal never touches dirs, so a dir's last key IS its bound.
 * Leaf shape: keysLen entries, keysLen values, plus — on non-rightmost leaves — an
 * explicit {@code fence}: the leaf's inclusive high bound as a 1-element key group.
 * The fence is needed because a leaf's coverage (lo, fence] outlives its content:
 * removal can shrink the max entry below the separator published in the parent, and
 * a key between the two MUST still be inserted here, never in the right sibling.
 *
 * Concurrency (Lehman-Yao STRUCTURE, Sagiv 1986 LOCK DISCIPLINE). Deadlock-freedom
 * follows from writers holding at most one node lock at a time, always acquired while
 * holding none, so there is no hold-and-wait cycle to close:
 *  - readers are lock-free at this layer: nodes are copy-on-write and republished
 *    with per-recid atomic updates; a reader that raced a split detects "key beyond
 *    this node" and follows the link chain rightward (keys only ever move right);
 *  - writers take per-node locks from a recid-keyed parking try-lock table and hold
 *    AT MOST ONE at a time — every acquisition happens while holding none: descend
 *    unlocked remembering the parent path, lock the leaf, move right by RELEASING
 *    the current node before locking the next, mutate copy-on-write; a split writes
 *    the right sibling first, republishes the left half (link already pointing
 *    right), RELEASES the child lock, and only then locks the parent to insert the
 *    separator — Sagiv's "overtaking": the link pointer keeps the tree searchable
 *    during the parent-update gap. Deadlock-freedom needs no ordering argument:
 *    a writer waiting for a node lock holds none, so hold-and-wait never occurs
 *    (Sagiv Thm 1). Every locked section unlocks in a finally — a lock leaked on
 *    an unchecked throw would self-deadlock the retrying thread, because the
 *    locks are non-reentrant.
 *  - the PROTOCOL reserves capacity for up to THREE overlapping node locks per
 *    writer, for future Sagiv-style compression (mapdb2 {@code compactLevel}
 *    lineage: parent + child + right sibling, acquired TOP-DOWN then
 *    LEFT-TO-RIGHT — the order Sagiv proves composable with one-lock writers).
 *    Binding consequences for all future code: never acquire a parent/ancestor
 *    while holding a descendant, never acquire leftward; Lehman-Yao's bottom-up
 *    3-lock discipline is permanently excluded (Sagiv p. 277: top-down compression
 *    deadlocks against it). Because overlap is protocol capacity, the lock table
 *    MUST key locks by exact recid: a striped/fixed-size lock array aliases
 *    distinct recids to one lock, and tree-order acquisition (uncorrelated with
 *    stripe order) then deadlocks — including self-deadlock when parent and child
 *    collide on a stripe. Reentrant locks are equally banned: reentrancy MASKS
 *    aliasing and wrong-direction overlap instead of failing fast at the
 *    acquisition point (see docs/history/spinlock-study/ and the reentrant modulo
 *    stripe arrays of the historical mapdb store layer).
 *  - a root split locks the root-pointer recid only; concurrent writers ascending
 *    past a level whose parent does not exist yet park in {@code leftEdge} (the
 *    per-level left-edge recids — stable: splits keep the left half's recid). A
 *    propagation that fails AFTER its split published poisons the map so those
 *    waiters fail fast instead of parking forever.
 *  - removal never merges nodes (mapdb3 semantics): empty leaves stay linked.
 *
 * The reader/writer safety above rests on the backing store's per-recid atomic
 * publication, so the {@code ConcurrentMap} guarantees hold only when
 * {@code store.isThreadSafe()} — the default. A store opted OUT of thread safety
 * (e.g. {@code StoreOnHeap}/{@code StoreByteArray}/{@code StoreDirect} constructed
 * with {@code threadSafe=false}) makes its locks no-ops; this map then also SKIPS
 * its own node locks ({@code lockNode}/{@code unlockNode} return early) and does not
 * guarantee lock-free readers a happens-before against a concurrent writer, so such a
 * handle is single-threaded only.
 *
 * <h2>Map interfaces</h2>
 *
 * Implements {@link ConcurrentNavigableMap} (spec-btree-map iface) — i.e. ConcurrentMap +
 * NavigableMap + SortedMap. Weakly-consistent collection views (entrySet/keySet/values)
 * backed by leaf-link iteration; {@code iterator.remove()} routes the last key to
 * {@link #remove(Object)}. The full navigable surface (lower/floor/ceiling/higher,
 * first/last/poll, descendingMap, navigableKeySet, inclusive-flag sub-maps) is delegated
 * to the shared {@link ConcurrentOrderedNavigableView} over an {@link #Adapter}; sub-views
 * are themselves {@code ConcurrentNavigableMap}. {@code pollFirstEntry}/{@code pollLastEntry}
 * are atomic on the (key,value) pair via a conditional-remove retry loop (see
 * {@link #pollFirstEntry}); their first/last SELECTION is weakly consistent (matches the
 * iterators) — they never remove a value they did not return. ConcurrentMap CAS
 * ({@code putIfAbsent}, {@code remove(k,v)},
 * {@code replace}) is ATOMIC: the condition is evaluated UNDER the covering leaf's lock
 * (after move-right resolves the real owner), with no separate {@code get()} first, and
 * value equality uses the value format's {@link org.mapdb.ser.Serializer#equals} (not
 * {@code Object.equals}). Null keys/values are rejected (NPE).
 *
 * <p>{@code get}/{@code containsKey}/{@code remove} accept {@code Object}: an absent key
 * yields {@code null}/{@code false}; an ineligible key type may raise
 * {@link ClassCastException} (permitted by the {@link Map} contract — not caught as
 * control flow). {@code size()} saturates at {@link Integer#MAX_VALUE}; use
 * {@link #sizeLong()} for the exact count.
 */
public class BTreeMap<K, V> extends AbstractMap<K, V>
        implements ConcurrentNavigableMap<K, V>, org.mapdb.MapExtra<K, V> {

    static final int DIR = 8, LEFT = 4, RIGHT = 2;

    private final Store store;
    private final GroupFormat<K> keyFormat;
    private final GroupFormat<V> valueFormat;
    /** Leaf representation: real values when inline, packed value recids otherwise. */
    private final GroupFormat<?> nodeValueFormat;
    private final boolean valueInline;
    /**
     * External-value (non-inline) mode only: prevents an external value recid from being deleted
     * and its store slot reused while a lock-free reader still holds a leaf snapshot referencing
     * it. Readers take the READ lock across the {@code store.get} of the external value (disk I/O),
     * and {@link #removeInternal} takes the WRITE lock for the whole remove. Tradeoffs, both
     * accepted for correctness:
     * <ul>
     *   <li>every remove on an external-value map serializes map-WIDE (single map-global write
     *       lock, not per-leaf), and</li>
     *   <li>an in-flight reader's {@code store.get} disk I/O blocks concurrent removes for its
     *       duration.</li>
     * </ul>
     * The lock is per-BTreeMap-instance, so it only guards concurrency WITHIN one instance. Two
     * BTreeMap instances opened over the same store reintroduce the recid-reuse race — consistent
     * with the existing single-writer-instance invariant (one live writer per store map).
     */
    private final java.util.concurrent.locks.ReentrantReadWriteLock externalValueLock =
            new java.util.concurrent.locks.ReentrantReadWriteLock();
    private final int maxNodeSize;
    private final long rootRecidRecid;
    private final NodeSerializer nodeSer;
    private final boolean threadSafe;

    /**
     * Optional O(1) size counter (Feature A). 0 = disabled; otherwise the recid of a
     * dedicated {@code Long} record holding the live entry count, EXTERNAL to the tree
     * nodes (node serialization is unchanged and byte-compatible with a counter-less
     * map). Maintained by a compare-and-swap loop on this recid (see
     * {@link #addToCounter}) AFTER the structural mutation commits: +1 when a put
     * inserts a genuinely new key, -1 when a remove drops a present key; a replace/
     * update of an existing key leaves it untouched. When enabled, {@link #sizeLong()}
     * returns this value in O(1); otherwise it falls back to a leaf traversal.
     */
    private final long counterRecid;

    /**
     * Runtime-only modification listeners (Feature B); NOT persisted. Fired AFTER the
     * node lock is released on every successful put (insert/update), remove and
     * replace. {@link java.util.concurrent.CopyOnWriteArrayList} so firing needs no
     * lock and tolerates concurrent {@link #addModificationListener} registration.
     */
    private final java.util.concurrent.CopyOnWriteArrayList<ModificationListener<K, V>> modListeners =
            new java.util.concurrent.CopyOnWriteArrayList<>();
    private final java.util.concurrent.CopyOnWriteArrayList<org.mapdb.MapModificationListener<K, V>>
            compatibilityModListeners = new java.util.concurrent.CopyOnWriteArrayList<>();
    /**
     * Order-sensitive listeners fired SYNCHRONOUSLY, while the covering leaf lock that serialized
     * the mutation is still held (see {@link org.mapdb.SynchronousMapModificationListener}). This
     * preserves per-key event order under same-key contention for last-writer-wins secondary
     * indexes; ordinary listeners in {@code compatibilityModListeners} stay deferred to avoid
     * re-entrancy deadlocks. A synchronous listener runs under the node lock, so it must not
     * re-enter this map.
     */
    private final java.util.concurrent.CopyOnWriteArrayList<org.mapdb.MapModificationListener<K, V>>
            syncModListeners = new java.util.concurrent.CopyOnWriteArrayList<>();

    /**
     * Per-node write locks, keyed by EXACT recid (mapdb1/2/3 lineage). Acquisition
     * uses putIfAbsent and parks briefly after a failed attempt; it does not busy-spin.
     * The ops implemented here hold AT MOST ONE lock at a time — every acquisition
     * happens while holding none (see class javadoc; enforced under {@code -ea} by the
     * zero-held checker in {@link #lockNode}). The PROTOCOL reserves capacity for up to
     * three overlapping locks per writer (future top-down Sagiv compression), so the
     * table must never alias two recids to one lock (no striping) and the locks must not
     * be reentrant — either would mask a wrong-direction or aliased acquisition instead
     * of failing fast. Deadlock-freedom proof: docs/research/btree-deadlock-freedom.md.
     * Node locks nest OUTSIDE store locks (store calls happen under a node lock,
     * never the reverse). The root-pointer recid is lockable here too (root splits).
     */
    private final ConcurrentHashMap<Long, Thread> nodeLocks = new ConcurrentHashMap<>();

    /**
     * Structural-failure backstop (parity with rust r2/r3 and zig; see
     * docs/research/btree-deadlock-freedom.md §6). Set when a split's separator
     * propagation — or a root grow — fails AFTER the split was already published: the
     * tree is then searchable via links but a level a later writer expects may never be
     * created, so a writer could park forever in {@link #leftEdge}. Every op entry
     * ({@link #rootRecid}) and the {@link #leftEdge} park loop read this O(1) volatile
     * and fail fast with {@link org.mapdb.DBException.DataCorruption} instead. A poisoned
     * in-memory handle stays poisoned; reopening the store rebuilds it from durable state.
     */
    private volatile boolean poisoned;

    /**
     * Left-edge node recid per level, index 0 = leaf level, last = root. Stable by
     * construction: a split keeps the left half's recid, so a left-edge recid never
     * changes once a level exists; only root splits append. Written under the
     * root-pointer node lock, read via volatile snapshot. Used when split
     * propagation ascends above where its descent started (the tree grew in the
     * meantime): the parent level is entered at its left edge and move-right finds
     * the covering node.
     */
    private volatile long[] leftEdges;

    /**
     * Cached root recid (0 = not loaded). Every op starts at the root, so re-reading
     * the root pointer record per op funnels all threads through one segment lock —
     * a false-serialization hotspot independent of tree depth. A STALE cached root is
     * harmless: nodes are never freed by this layer, and an old root still covers the
     * whole key space via its right-links (Lehman-Yao), so descent from it only costs
     * extra link hops. Not cacheable over transactional stores: rollback() can void
     * node recids written since the last commit, and a cached pointer would dangle.
     */
    private volatile long cachedRootRecid;
    private final boolean rootCacheable;

    /**
     * Last {@link Store#structuralGeneration()} the {@link #leftEdges} cache was known
     * consistent with. Advances only when a transactional-store {@code rollback()}
     * bumps the store's generation; a mismatch triggers a one-shot rebuild (see
     * {@link #refreshLeftEdgesIfTx}). Always {@code 0} for non-tx stores. Mirrors the
     * rust port's {@code last_struct_gen}.
     */
    private volatile long lastStructGen;

    private BTreeMap(Store store, GroupFormat<K> keyFormat, GroupFormat<V> valueFormat,
                     int maxNodeSize, long rootRecidRecid, long counterRecid, boolean valueInline) {
        if (maxNodeSize < 4) throw new IllegalArgumentException("maxNodeSize must be >= 4");
        this.store = store;
        this.keyFormat = keyFormat;
        this.valueFormat = valueFormat;
        this.valueInline = valueInline;
        this.nodeValueFormat = valueInline ? valueFormat : LongFormat.INSTANCE;
        this.maxNodeSize = maxNodeSize;
        this.rootRecidRecid = rootRecidRecid;
        this.counterRecid = counterRecid < 0 ? 0 : counterRecid;
        this.nodeSer = new NodeSerializer();
        this.rootCacheable = !(store instanceof StoreTx);
        this.lastStructGen = store.structuralGeneration();
        this.threadSafe = store.isThreadSafe();
        this.leftEdges = rootRecidRecid == 0 ? new long[0] : buildLeftEdges();
    }

    /** Walk the leftmost spine root→leaf; returns recids with index 0 = leaf level. */
    private long[] buildLeftEdges() {
        ArrayDeque<Long> spine = new ArrayDeque<>();
        long current = store.get(rootRecidRecid, Serializers.LONG);
        spine.push(current);
        Node n = load(current);
        while (n.isDir()) {
            current = n.children()[0];
            spine.push(current);
            n = load(current);
        }
        long[] r = new long[spine.size()];
        int i = 0;
        for (long recid : spine) r[i++] = recid; // head = leaf, tail = root
        return r;
    }

    /** Create a fresh empty map with NO size counter (delegates to the counter overload). */
    public static <K, V> BTreeMap<K, V> create(Store store, GroupFormat<K> keyFormat,
                                               GroupFormat<V> valueFormat, int maxNodeSize) {
        return create(store, keyFormat, valueFormat, maxNodeSize, false);
    }

    /**
     * Create a fresh empty map, optionally with an O(1) size counter (Feature A). When
     * {@code counterEnable} is true a dedicated {@code Long} record (initial value 0) is
     * allocated to hold the live entry count; its recid is exposed via
     * {@link #counterRecid()} so a persistence layer can reopen the map with the counter.
     * The counter is EXTERNAL to the tree nodes — the stored node/root format is
     * unchanged and byte-compatible with a counter-less map.
     */
    public static <K, V> BTreeMap<K, V> create(Store store, GroupFormat<K> keyFormat,
                                               GroupFormat<V> valueFormat, int maxNodeSize,
                                               boolean counterEnable) {
        long rootRecidRecid = createRoot(store, keyFormat, valueFormat, maxNodeSize, true);
        long counterRecid = counterEnable ? store.put(0L, Serializers.LONG) : 0L;
        return new BTreeMap<>(store, keyFormat, valueFormat, maxNodeSize, rootRecidRecid, counterRecid, true);
    }

    /** Create a map whose leaf nodes contain value recids rather than value bytes. */
    public static <K, V> BTreeMap<K, V> createExternalValues(Store store,
            GroupFormat<K> keyFormat, GroupFormat<V> valueFormat, int maxNodeSize,
            boolean counterEnable) {
        long rootRecidRecid = createRoot(store, keyFormat, valueFormat, maxNodeSize, false);
        long counterRecid = counterEnable ? store.put(0L, Serializers.LONG) : 0L;
        return new BTreeMap<>(store, keyFormat, valueFormat, maxNodeSize,
                rootRecidRecid, counterRecid, false);
    }

    private static <K, V> long createRoot(Store store, GroupFormat<K> kf, GroupFormat<V> vf,
                                           int maxNodeSize, boolean valueInline) {
        BTreeMap<K, V> tmp = new BTreeMap<>(store, kf, vf, maxNodeSize, 0, 0, valueInline);
        Node emptyLeaf = new Node(LEFT | RIGHT, 0L, kf.empty(), tmp.nodeValueFormat.empty(), null);
        long rootRecid = store.put(emptyLeaf, tmp.nodeSer);
        return store.put(rootRecid, Serializers.LONG);
    }

    /** Reopen a map with NO size counter (delegates to the counter overload). */
    public static <K, V> BTreeMap<K, V> open(Store store, long rootRecidRecid, GroupFormat<K> keyFormat,
                                             GroupFormat<V> valueFormat, int maxNodeSize) {
        return open(store, rootRecidRecid, keyFormat, valueFormat, maxNodeSize, 0L);
    }

    /**
     * Reopen a map, wiring up its O(1) size counter when {@code counterRecid > 0}
     * (Feature A); {@code counterRecid <= 0} means "no counter". The counter recid is
     * the value returned by {@link #counterRecid()} at create time and must be
     * persisted by the caller alongside {@link #rootRecidRecid()}.
     */
    public static <K, V> BTreeMap<K, V> open(Store store, long rootRecidRecid, GroupFormat<K> keyFormat,
                                             GroupFormat<V> valueFormat, int maxNodeSize, long counterRecid) {
        return new BTreeMap<>(store, keyFormat, valueFormat, maxNodeSize,
                rootRecidRecid, counterRecid, true);
    }

    /** Reopen a map created by {@link #createExternalValues}. */
    public static <K, V> BTreeMap<K, V> openExternalValues(Store store, long rootRecidRecid,
            GroupFormat<K> keyFormat, GroupFormat<V> valueFormat, int maxNodeSize,
            long counterRecid) {
        return new BTreeMap<>(store, keyFormat, valueFormat, maxNodeSize,
                rootRecidRecid, counterRecid, false);
    }

    /** Bulk build with the default pump fill ({@link TreePump#defaultFill}: 3/4 of maxNodeSize), no counter. */
    public static <K, V> BTreeMap<K, V> createFromSorted(Store store, GroupFormat<K> keyFormat,
                                                         GroupFormat<V> valueFormat, int maxNodeSize,
                                                         Iterator<? extends Map.Entry<K, V>> sortedEntries) {
        return createFromSorted(store, keyFormat, valueFormat, maxNodeSize,
                TreePump.defaultFill(maxNodeSize), sortedEntries, false);
    }

    /**
     * Bulk build with the default pump fill, optionally enabling an O(1) size counter
     * (Feature A) initialized to the number of entries written. See
     * {@link #createFromSorted(Store, GroupFormat, GroupFormat, int, int, Iterator, boolean)}.
     */
    public static <K, V> BTreeMap<K, V> createFromSorted(Store store, GroupFormat<K> keyFormat,
                                                         GroupFormat<V> valueFormat, int maxNodeSize,
                                                         Iterator<? extends Map.Entry<K, V>> sortedEntries,
                                                         boolean counterEnable) {
        return createFromSorted(store, keyFormat, valueFormat, maxNodeSize,
                TreePump.defaultFill(maxNodeSize), sortedEntries, counterEnable);
    }

    /**
     * Bulk build (Pump): construct the tree bottom-up from entries in
     * STRICTLY ascending key order — throws {@link org.mapdb.DBException.NotSorted}
     * on a misordered or duplicate key. Every node is written exactly once via
     * preallocate → update-fill, siblings linked forward without back-patching, so
     * on a fresh store both recids and data land sequentially in key order.
     * Interior nodes are filled to {@code nodeFill} entries (≤ maxNodeSize; keep
     * below it to leave room for post-load inserts). Single-threaded; the caller
     * commits. The returned map is fully functional — subsequent put/remove/get
     * behave exactly as on an incrementally built tree.
     */
    public static <K, V> BTreeMap<K, V> createFromSorted(Store store, GroupFormat<K> keyFormat,
                                                         GroupFormat<V> valueFormat, int maxNodeSize,
                                                         int nodeFill,
                                                         Iterator<? extends Map.Entry<K, V>> sortedEntries) {
        return createFromSorted(store, keyFormat, valueFormat, maxNodeSize, nodeFill, sortedEntries, false);
    }

    /**
     * Bulk build (Pump) overload that additionally enables an O(1) size counter
     * (Feature A) when {@code counterEnable} is true, setting it to the exact number of
     * entries written. The counter recid is exposed via {@link #counterRecid()}. See the
     * {@code nodeFill}-only overload for the build semantics.
     */
    public static <K, V> BTreeMap<K, V> createFromSorted(Store store, GroupFormat<K> keyFormat,
                                                         GroupFormat<V> valueFormat, int maxNodeSize,
                                                         int nodeFill,
                                                         Iterator<? extends Map.Entry<K, V>> sortedEntries,
                                                         boolean counterEnable) {
        Sink<K, V> sink = createFromSink(store, keyFormat, valueFormat, maxNodeSize,
                nodeFill, counterEnable);
        while (sortedEntries.hasNext()) {
            Map.Entry<K, V> e = sortedEntries.next();
            sink.put(e.getKey(), e.getValue());
        }
        return sink.create();
    }

    /** Incremental sorted bulk builder using the default pump fill and no counter. */
    public static <K, V> Sink<K, V> createFromSink(Store store, GroupFormat<K> keyFormat,
                                                   GroupFormat<V> valueFormat, int maxNodeSize) {
        return createFromSink(store, keyFormat, valueFormat, maxNodeSize,
                TreePump.defaultFill(maxNodeSize), false);
    }

    /** Incremental sorted bulk builder using the default pump fill. */
    public static <K, V> Sink<K, V> createFromSink(Store store, GroupFormat<K> keyFormat,
                                                   GroupFormat<V> valueFormat, int maxNodeSize,
                                                   boolean counterEnable) {
        return createFromSink(store, keyFormat, valueFormat, maxNodeSize,
                TreePump.defaultFill(maxNodeSize), counterEnable);
    }

    /**
     * Create a single-use streaming bulk builder. Feed strictly ascending keys with
     * {@link Sink#put(Object, Object)}, then finalize exactly once with
     * {@link Sink#create()}. Completed nodes are written as the stream advances, so
     * memory use is bounded by one in-progress node per tree level.
     */
    public static <K, V> Sink<K, V> createFromSink(Store store, GroupFormat<K> keyFormat,
                                                   GroupFormat<V> valueFormat, int maxNodeSize,
                                                   int nodeFill, boolean counterEnable) {
        return new Sink<>(store, keyFormat, valueFormat, maxNodeSize, nodeFill, counterEnable);
    }

    /** Streaming counterpart of {@link #createFromSorted}. Single-threaded and single-use. */
    public static final class Sink<K, V> {
        private final Store store;
        private final GroupFormat<K> keyFormat;
        private final GroupFormat<V> valueFormat;
        private final int maxNodeSize;
        private final boolean counterEnable;
        private final BTreeMap<K, V> temporary;
        private final TreePump<K, V> pump;
        private long count;
        private boolean done;

        private Sink(Store store, GroupFormat<K> keyFormat, GroupFormat<V> valueFormat,
                     int maxNodeSize, int nodeFill, boolean counterEnable) {
            this.store = store;
            this.keyFormat = keyFormat;
            this.valueFormat = valueFormat;
            this.maxNodeSize = maxNodeSize;
            this.counterEnable = counterEnable;
            this.temporary = new BTreeMap<>(store, keyFormat, valueFormat, maxNodeSize, 0, 0, true);
            TreePump.NodeSink<K, V> nodeSink = new TreePump.NodeSink<>() {
                @Override public void writeLeaf(long recid, int flags, long link,
                                                Object[] keys, Object[] values) {
                    Object fence = (flags & RIGHT) == 0
                            ? keyFormat.fromArray(new Object[]{keys[keys.length - 1]}) : null;
                    Node node = new Node(flags, link, keyFormat.fromArray(keys),
                            valueFormat.fromArray(values), fence);
                    store.update(recid, node, temporary.nodeSer);
                }

                @Override public void writeDir(long recid, int flags, long link,
                                               Object[] keys, long[] children) {
                    store.update(recid, new Node(flags, link, keyFormat.fromArray(keys),
                            children, null), temporary.nodeSer);
                }
            };
            this.pump = new TreePump<>(store, nodeSink, keyFormat.element(), maxNodeSize, nodeFill);
        }

        /** Append the next strictly ascending entry. */
        public Sink<K, V> put(K key, V value) {
            if (done) throw new IllegalStateException("sink already finished");
            pump.put(key, value);
            count++;
            return this;
        }

        /** Finalize and return the live map. May be called exactly once. */
        public BTreeMap<K, V> create() {
            if (done) throw new IllegalStateException("sink already finished");
            done = true;
            long rootRecid = pump.finish();
            long rootRecidRecid = store.put(rootRecid, Serializers.LONG);
            long counterRecid = counterEnable ? store.put(count, Serializers.LONG) : 0L;
            return new BTreeMap<>(store, keyFormat, valueFormat, maxNodeSize,
                    rootRecidRecid, counterRecid, true);
        }
    }

    /** Recid of the root-pointer record; persist this to reopen the map. */
    public long rootRecidRecid() { return rootRecidRecid; }
    /** True when values are encoded directly in leaf nodes, false when leaves store value recids. */
    public boolean valueInline() { return valueInline; }
    @Override public boolean isClosed() { return store.isClosed(); }
    @Override public Serializer<K> keySerializer() { return keyFormat.element(); }
    @Override public Serializer<V> valueSerializer() { return valueFormat.element(); }

    /**
     * Recid of the O(1) size-counter record (Feature A), or 0 when no counter is
     * enabled. Persist this alongside {@link #rootRecidRecid()} and pass it to
     * {@link #open(Store, long, GroupFormat, GroupFormat, int, long)} to reopen the map
     * with its counter.
     */
    public long counterRecid() { return counterRecid; }

    /** True when this map maintains an O(1) size counter. */
    private boolean counterEnabled() { return counterRecid != 0; }

    /**
     * Apply {@code delta} to the shared counter record via a CAS retry loop (mirrors
     * {@link org.mapdb.db.Atomic.Long#getAndAdd}). Called AFTER the structural
     * mutation commits, so the delta reflects an already-applied change; the CAS loop
     * serializes concurrent counter updates against each other without holding any node
     * lock. No-op when the counter is disabled.
     */
    private void addToCounter(long delta) {
        if (counterRecid == 0) return;
        for (;;) {
            long cur = store.get(counterRecid, Serializers.LONG);
            if (store.compareAndSwap(counterRecid, cur, cur + delta, Serializers.LONG)) return;
        }
    }

    // ================= modification listener (Feature B) =================

    /**
     * Modification callback fired on every successful mutation of a mapping. Exactly
     * one of {@code oldValue}/{@code newValue} is null on a pure insert/remove:
     * <ul>
     *   <li>{@code oldValue == null} — the key was inserted ({@code newValue} is the value);</li>
     *   <li>{@code newValue == null} — the key was removed ({@code oldValue} was its value);</li>
     *   <li>both non-null — an existing key's value was updated (put over an existing key,
     *       or a successful {@code replace}).</li>
     * </ul>
     */
    public interface ModificationListener<K, V> {
        void modified(K key, V oldValue, V newValue);
    }

    /**
     * Register a modification listener (Feature B). Multiple listeners are supported and
     * fire in registration order. Listeners are RUNTIME-ONLY (not persisted) and are
     * invoked AFTER the covering node lock is released — see {@link #fireModified}.
     */
    public void addModificationListener(ModificationListener<K, V> l) {
        if (l == null) throw new NullPointerException();
        modListeners.addIfAbsent(l);
    }

    @Override public void modificationListenerAdd(org.mapdb.MapModificationListener<K, V> listener) {
        if (listener == null) throw new NullPointerException("listener");
        if (listener instanceof org.mapdb.SynchronousMapModificationListener)
            syncModListeners.addIfAbsent(listener);
        else
            compatibilityModListeners.addIfAbsent(listener);
    }

    @Override public void modificationListenerRemove(org.mapdb.MapModificationListener<K, V> listener) {
        compatibilityModListeners.remove(listener);
        syncModListeners.remove(listener);
    }

    /**
     * Invoke every registered listener with an operation's OWN captured (key, oldValue,
     * newValue). Called AFTER {@code unlockNode} (and after any split propagation
     * completes), so user code never runs under a node lock — avoiding reentrancy into
     * the map and lock-ordering deadlocks. ORDERING GUARANTEE: the values passed to a
     * listener are exactly the ones the leaf-locked operation observed and applied, so
     * they are per-operation correct (Lehman-Yao serializes same-key mutations at the
     * leaf lock). The GLOBAL fire order across CONCURRENT operations is NOT strictly
     * serialized with respect to the leaf-mutation order — two operations on different
     * keys, or racing distinct-key ops, may fire in either order; only the (key, old,
     * new) triple each carries is guaranteed accurate.
     */
    private void fireModified(K key, V oldValue, V newValue) {
        // per-listener continuation: every listener sees the event even when an earlier one
        // throws; first throwable rethrown after, later ones suppressed
        Throwable first = null;
        for (ModificationListener<K, V> l : modListeners) {
            try {
                l.modified(key, oldValue, newValue);
            } catch (RuntimeException | Error t) {
                first = suppress(first, t);
            }
        }
        for (org.mapdb.MapModificationListener<K, V> l : compatibilityModListeners) {
            try {
                l.modify(key, oldValue, newValue, false);
            } catch (RuntimeException | Error t) {
                first = suppress(first, t);
            }
        }
        rethrow(first);
    }

    /**
     * Fire the SYNCHRONOUS listeners for an operation's own (key, oldValue, newValue) while the
     * covering leaf lock is STILL HELD, so same-key mutations deliver their events in leaf-mutation
     * order (the losing writer of a same-key race cannot overwrite the winner's index entry). Only
     * {@link org.mapdb.SynchronousMapModificationListener}s registered here run; ordinary deferred
     * listeners fire later via {@link #fireModified}. Runs under the node lock, so a listener that
     * re-enters this map or violates lock ordering can deadlock — the mode is opt-in for that reason.
     */
    private void fireModifiedSync(K key, V oldValue, V newValue) {
        // per-listener continuation, same semantics as fireModified. A sync throw still skips
        // this operation's DEFERRED fireModified — consistent with the other mutation branches.
        Throwable first = null;
        for (org.mapdb.MapModificationListener<K, V> l : syncModListeners) {
            try {
                l.modify(key, oldValue, newValue, false);
            } catch (RuntimeException | Error t) {
                first = suppress(first, t);
            }
        }
        rethrow(first);
    }

    private static Throwable suppress(Throwable first, Throwable t) {
        if (first == null) return t;
        if (first != t) first.addSuppressed(t); // a shared exception instance must not self-suppress
        return first;
    }

    private static void rethrow(Throwable first) {
        if (first == null) return;
        if (first instanceof Error) throw (Error) first;
        throw (RuntimeException) first;
    }

    /** Op entry: fail fast if a prior structural failure poisoned the map. */
    private void checkPoison() {
        if (poisoned) throw new DBException.DataCorruption(
                "btree poisoned by a failed structural update; reopen the store");
    }

    private long rootRecid() {
        checkPoison();
        long r = cachedRootRecid;
        if (r != 0) return r;
        r = store.get(rootRecidRecid, Serializers.LONG);
        if (rootCacheable) cachedRootRecid = r;
        return r;
    }

    /**
     * Authoritative "is {@code recid} the current root?" — reads the root-pointer
     * record FRESH (never the possibly-stale {@link #cachedRootRecid}). Root growth
     * is gated on this AND the node's {@code LEFT|RIGHT} flags (see the split sites):
     * the flag test is the CONCURRENCY serialization (a splitter republishes the root
     * LEFT-only under its lock before releasing, so a second splitter sees the cleared
     * flag and does not also grow a root), and this identity test rejects a CRAFTED
     * descendant falsely flagged root-shaped so it cannot replace the real tree. Both,
     * flags first (its short-circuit skips this store read on ordinary splits). Mirrors
     * the rust port's {@code is_current_root}.
     */
    private boolean isCurrentRoot(long recid) {
        return store.get(rootRecidRecid, Serializers.LONG) == recid;
    }

    /**
     * Resync the {@link #leftEdges} structural cache with the tx-visible tree when a
     * rollback may have shrunk it. {@code leftEdges} is normally append-only and always
     * current, but a transactional store's tree can be reverted out-of-band by a
     * {@code rollback()} that shrinks its height while this map object (and its longer
     * cached array) stays open; the next root grow would then append onto a stale array
     * whose entries name deleted/reused recids. Gated on the store's
     * {@link Store#structuralGeneration()}, so this is a cheap load-and-compare on the
     * common (no-rollback) path and rebuilds only ONCE after each rollback — never per
     * put.
     *
     * <p>Concurrency contract: a transactional store is a SINGLE GLOBAL WRITER (the
     * conventional WAL model). {@code rollback}/{@code commit} are transaction
     * boundaries that must not race in-flight mutations — they revert/publish exactly
     * those mutations — so the post-rollback rebuild here never runs concurrently with a
     * root grow, and needs no locking beyond what the non-tx paths already use (mirrors
     * the rust port's {@code refresh_left_edges_if_tx}). No-op (zero cost) for non-tx
     * stores, mirroring {@link #cachedRootRecid}.
     */
    private void refreshLeftEdgesIfTx() {
        if (rootCacheable) return; // non-tx: the append-only cache is authoritative
        long gen = store.structuralGeneration();
        if (gen == lastStructGen) return; // no rollback since last resync — cache is current
        checkPoison();
        leftEdges = buildLeftEdges();
        lastStructGen = gen;
    }

    // ================= node locks =================

    /**
     * -ea-only zero-held checker (task 4c; sibling of {@link org.mapdb.store.DeadlockAsserts}).
     * The recids THIS thread currently holds in THIS map — enforcing the protocol invariant
     * that a node-lock acquisition happens while this thread holds NO node lock of this map
     * (the current 1-lock protocol; becomes a ranked-order assert when a multi-lock op lands).
     * Populated only under {@code assert} so it is JIT-eliminated and zero-cost with
     * assertions off. See docs/research/btree-deadlock-freedom.md §6.
     */
    private final ThreadLocal<ArrayDeque<Long>> heldNodeLocks =
            ThreadLocal.withInitial(ArrayDeque::new);

    /** Assert (before acquiring {@code recid}) that this thread holds no node lock of this map. */
    private boolean assertNoLockHeld(long recid) {
        ArrayDeque<Long> held = heldNodeLocks.get();
        if (!held.isEmpty())
            throw new AssertionError("node-lock protocol violation: acquiring " + recid
                    + " while holding " + held + " — writers hold at most one lock; never acquire "
                    + "a parent/ancestor while holding a descendant, nor leftward (class javadoc)");
        return true;
    }

    private boolean recordAcquired(long recid) { heldNodeLocks.get().push(recid); return true; }

    private boolean recordReleased(long recid) {
        if (!heldNodeLocks.get().remove(recid))
            throw new AssertionError("releasing node lock " + recid
                    + " not held by this thread; held=" + heldNodeLocks.get());
        return true;
    }

    private void lockNode(long recid) {
        if (!threadSafe) return;
        Thread me = Thread.currentThread();
        assert nodeLocks.get(recid) != me : "reentrant node lock: " + recid;
        assert assertNoLockHeld(recid);
        while (nodeLocks.putIfAbsent(recid, me) != null) {
            LockSupport.parkNanos(10L);
        }
        assert recordAcquired(recid);
    }

    private void unlockNode(long recid) {
        if (!threadSafe) return;
        Thread prev = nodeLocks.remove(recid);
        assert prev == Thread.currentThread() : "node lock " + recid + " unlocked by non-owner";
        assert recordReleased(recid);
    }

    // ================= node =================

    /**
     * Immutable node value. keys/values are OPAQUE groups owned by the formats;
     * dir values are long[] child recids handled by the node serializer directly.
     * fence: non-rightmost LEAF only — the inclusive high bound as a 1-element key
     * group (kept as a group so bound checks reuse GroupFormat.search — no extra
     * format API). Dirs use their last key as the bound; rightmost nodes have none.
     */
    static final class Node {
        final int flags;
        final long link;
        final Object keys;
        final Object values; // leaf: value group; dir: long[] children
        final Object fence;  // leaf && !RIGHT: 1-element key group; else null

        Node(int flags, long link, Object keys, Object values, Object fence) {
            this.flags = flags;
            this.link = link;
            this.keys = keys;
            this.values = values;
            this.fence = fence;
            assert isRight() == (link == 0) : "link/RIGHT mismatch";
            assert (fence != null) == (!isDir() && !isRight()) : "fence presence mismatch";
        }

        boolean isDir() { return (flags & DIR) != 0; }
        boolean isRight() { return (flags & RIGHT) != 0; }
        long[] children() { return (long[]) values; }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private Object nodeValueGet(Object values, int pos) {
        return ((GroupFormat) nodeValueFormat).get(values, pos);
    }

    @SuppressWarnings("unchecked")
    private V expandValue(Object stored) {
        if (valueInline) return (V) stored;
        return store.get((Long) stored, valueFormat.element());
    }

    private V valueAt(Object values, int pos) { return expandValue(nodeValueGet(values, pos)); }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private Object insertValue(Object values, int pos, V value) {
        Object stored = valueInline ? value : store.put(value, valueFormat.element());
        return ((GroupFormat) nodeValueFormat).insert(values, pos, stored);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private Object setValue(Object values, int pos, V value) {
        if (!valueInline) {
            store.update((Long) nodeValueGet(values, pos), value, valueFormat.element());
            return values;
        }
        return ((GroupFormat) nodeValueFormat).set(values, pos, value);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private Object deleteValue(Object values, int pos) {
        return ((GroupFormat) nodeValueFormat).delete(values, pos);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private Object copyValues(Object values, int from, int to) {
        return ((GroupFormat) nodeValueFormat).copyRange(values, from, to);
    }

    /**
     * Node wire format (mapdb3 lineage):
     *   packInt(keysLen<<4 | flags), [packLong(link) unless RIGHT],
     *   key group, then child recids (dir, packed longs) or value group (leaf),
     *   then [fence key group of size 1, leaf && !RIGHT only].
     * The fence sits LAST so the read path (which never needs it — readers
     * over-follow links harmlessly) never decodes or skips it.
     */
    /** Per-element byte budget assumed for variable-width formats (e.g. String) when
     *  sizing the serialization buffer — big enough to cover typical short keys/values
     *  without over-allocating for tiny ones. */
    private static final int VAR_ELEM_EST = 32;

    final class NodeSerializer implements Serializer<Node> {

        /**
         * Serialization buffer-capacity hint. A node holds at most
         * {@code maxNodeSize} entries (transiently maxNodeSize+1 just before a split),
         * plus possibly a 1-element fence group, so estimating to a full node
         * eliminates StoreDirect.serialize's grow-by-doubling copies from the 16-byte
         * start (measured: ~4.2 KB/insert, weakness A1). Fixed-width elements give
         * an exact-ish bound; variable-width elements fall back to
         * {@link #VAR_ELEM_EST} per element. Slightly over-estimating still allocates
         * less garbage than the doubling chain.
         */
        private final int sizeHint;

        NodeSerializer() {
            int ke = keyFormat.element().fixedSize();
            int ve = nodeValueFormat.element().fixedSize();
            int keyBytes = ke > 0 ? ke : VAR_ELEM_EST;
            // leaf: one value per key; dir: one packed-long child per key (<=9 B)
            int valBytes = ve > 0 ? Math.max(ve, 9) : VAR_ELEM_EST;
            // header: packInt(keysLen<<4|flags) + packLong(link) <= ~11 B, round to 16;
            // +2 key slots: pre-split transient element and the fence group
            this.sizeHint = 16 + (maxNodeSize + 1) * (keyBytes + valBytes) + 2 * keyBytes + 8;
        }

        @Override public int sizeHint() { return sizeHint; }

        @Override public void serialize(DataOutput2 out, Node n) {
            int keysLen = keyFormat.size(n.keys);
            out.packInt((keysLen << 4) | n.flags);
            if (!n.isRight()) out.packLong(n.link);
            keyFormat.serialize(out, n.keys);
            if (n.isDir()) {
                for (long child : n.children()) out.packLong(child);
            } else {
                @SuppressWarnings("rawtypes") GroupFormat f = nodeValueFormat;
                f.serialize(out, n.values);
                if (!n.isRight()) keyFormat.serialize(out, n.fence);
            }
        }

        @Override public Node deserialize(DataInput2 in, int size) {
            int h = in.unpackInt();
            int flags = h & 0xF;
            int keysLen = h >>> 4;
            long link = (flags & RIGHT) != 0 ? 0L : in.unpackLong();
            Object keys = keyFormat.deserialize(in, keysLen);
            Object values;
            Object fence = null;
            if ((flags & DIR) != 0) {
                int childCount = keysLen + (((flags & RIGHT) != 0) ? 1 : 0);
                long[] children = new long[childCount];
                for (int i = 0; i < childCount; i++) children[i] = in.unpackLong();
                values = children;
            } else {
                values = nodeValueFormat.deserialize(in, keysLen);
                if ((flags & RIGHT) == 0) fence = keyFormat.deserialize(in, 1);
            }
            return new Node(flags, link, keys, values, fence);
        }
    }

    // ================= get: push-down read actions =================

    /**
     * One traversal step executed inside the store. Return encoding:
     * positive = next recid to visit; 0 = terminal (found/absent in the action fields).
     * Implements BOTH dialects: onBytes searches serialized bytes via the format's
     * binary ops (or explicit deserialize when the format declares no binary support),
     * onObject searches the live Node.
     *
     * Optimistic-read discipline: the store may invoke this on TORN
     * bytes and retry, so each invocation fully overwrites found/value (a torn
     * terminal hit must not survive the locked retry), and the header is sanity-
     * clamped so a torn keysLen fails fast instead of driving a huge deserialize.
     */
    private final class GetAction implements RecordRead {
        final K key;
        Object storedValue;
        boolean found;

        GetAction(K key) { this.key = key; }

        @Override public long onBytes(DataInput2 in, int size) {
            found = false;
            storedValue = null;
            int h = in.unpackInt();
            int flags = h & 0xF;
            int keysLen = h >>> 4;
            // every key occupies >= 1 serialized byte, so keysLen > size is torn/corrupt
            if (keysLen > size) throw new IllegalStateException("node header keysLen exceeds record size");
            long link = (flags & RIGHT) != 0 ? 0L : in.unpackLong();

            int pos;
            if (keyFormat.supportsBinary()) {
                pos = keyFormat.binarySearch(key, in, keysLen);
            } else {
                pos = keyFormat.search(keyFormat.deserialize(in, keysLen), key);
            }

            if ((flags & DIR) != 0) {
                int childIdx = pos >= 0 ? pos : -pos - 1;
                int childCount = keysLen + (((flags & RIGHT) != 0) ? 1 : 0);
                if (childIdx >= childCount) return link; // beyond high bound: right sibling
                in.unpackLongSkip(childIdx);
                return in.unpackLong();
            }
            if (pos >= 0) {
                found = true;
                storedValue = nodeValueFormat.supportsBinary()
                        ? nodeValueFormat.binaryGet(in, keysLen, pos)
                        : nodeValueGet(nodeValueFormat.deserialize(in, keysLen), pos);
                return 0;
            }
            int ip = -pos - 1;
            if (ip >= keysLen && link != 0) return link;
            return 0;
        }

        @SuppressWarnings("unchecked")
        @Override public long onObject(Object record) {
            found = false;
            storedValue = null;
            Node n = (Node) record;
            int pos = keyFormat.search(n.keys, key);
            if (n.isDir()) {
                int childIdx = pos >= 0 ? pos : -pos - 1;
                long[] children = n.children();
                if (childIdx >= children.length) return n.link;
                return children[childIdx];
            }
            if (pos >= 0) {
                found = true;
                storedValue = nodeValueGet(n.values, pos);
                return 0;
            }
            int ip = -pos - 1;
            if (ip >= keyFormat.size(n.keys) && n.link != 0) return n.link;
            return 0;
        }
    }

    @SuppressWarnings("unchecked")
    @Override public V get(Object key) {
        if (key == null) throw new NullPointerException();
        if (valueInline) return doGet((K) key);
        externalValueLock.readLock().lock();
        try { return doGet((K) key); }
        finally { externalValueLock.readLock().unlock(); }
    }

    /** Lock-free push-down lookup; ineligible key types surface as CCE inside search. */
    private V doGet(K key) {
        GetAction action = new GetAction(key);
        long current = rootRecid();
        while (current != 0) {
            current = store.read(current, action);
        }
        return action.found ? expandValue(action.storedValue) : null;
    }

    @SuppressWarnings("unchecked")
    @Override public boolean containsKey(Object key) {
        if (key == null) throw new NullPointerException();
        GetAction action = new GetAction((K) key);
        long current = rootRecid();
        while (current != 0) {
            current = store.read(current, action);
        }
        return action.found;
    }

    // ================= put / remove (object path, Lehman-Yao writers) =================

    private Node load(long recid) {
        return store.get(recid, nodeSer);
    }

    /** Route within a dir node; -1 = follow link. */
    private int routeChild(Node dir, K key) {
        int pos = keyFormat.search(dir.keys, key);
        int childIdx = pos >= 0 ? pos : -pos - 1;
        return childIdx >= dir.children().length ? -1 : childIdx;
    }

    /** Leaf coverage check: true when key lies beyond this leaf's inclusive fence. */
    private boolean beyondLeaf(Node leaf, K key) {
        // search on the 1-element fence group: -2 == insertion point 1 == key > fence
        return !leaf.isRight() && keyFormat.search(leaf.fence, key) == -2;
    }

    /** Dir coverage check: true when key lies beyond this dir's last key (its bound). */
    private boolean beyondDir(Node dir, K key) {
        int pos = keyFormat.search(dir.keys, key);
        return pos < 0 && -pos - 1 == keyFormat.size(dir.keys);
    }

    /**
     * Lock {@code recid}, load it, and move right (hand-over-hand, one lock held)
     * until the node covers {@code key}. Returns the covering node; its recid is in
     * {@code cursor[0]} and its lock is HELD by the caller.
     */
    private Node lockCovering(long recid, K key, boolean dirLevel, long[] cursor) {
        lockNode(recid);
        long locked = recid; // 0 = released; backstops every throw in the move-right window
        try {
            Node n = load(recid);
            while (dirLevel ? (!n.isRight() && beyondDir(n, key)) : beyondLeaf(n, key)) {
                long next = n.link;
                unlockNode(recid);
                locked = 0;
                recid = next;
                lockNode(recid);
                locked = recid;
                n = load(recid);
            }
            cursor[0] = recid;
            locked = 0; // hand the held lock to the caller (normal exit)
            return n;
        } finally {
            if (locked != 0) unlockNode(locked); // load() threw with a lock held
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
     * Blind put (API parity with the htree/JCache {@code void put}): insert or overwrite
     * without returning the previous value. NOT an I/O win here — the leaf is
     * materialized under the lock either way — but symmetric with {@link #putOnly} on
     * the buffer/external maps.
     */
    public void putOnly(K key, V value) {
        if (key == null || value == null) throw new NullPointerException();
        putInternal(key, value, false);
    }

    /**
     * Unlocked descent to the leaf that routes {@code key}, then {@link #lockCovering}
     * (move-right under one lock) to the real owner. Returns that LOCKED leaf; its recid
     * is left in {@code cursor[0]} and the caller MUST {@code unlockNode(cursor[0])}
     * (unless it hands off to split propagation, which releases the lock itself).
     * {@code parentStack}, when non-null, is filled with the covered parent path for
     * split propagation; pass null for ops that never split (remove/replace).
     */
    private Node lockLeaf(K key, long[] cursor, ArrayDeque<Long> parentStack) {
        long current = rootRecid();
        Node n = load(current);
        while (n.isDir()) {
            int childIdx = routeChild(n, key);
            if (childIdx < 0) {
                current = n.link;
            } else {
                if (parentStack != null) parentStack.push(current);
                current = n.children()[childIdx];
            }
            n = load(current);
        }
        return lockCovering(current, key, false, cursor);
    }

    /**
     * The put/putIfAbsent core, evaluated UNDER the covering leaf's lock — the CAS
     * decision (onlyIfAbsent) and the mutation are in one locked critical section, so
     * putIfAbsent is atomic against concurrent writers with no lost-update window.
     */
    private V putInternal(K key, V value, boolean onlyIfAbsent) {
        // Tx stores only: resync the structural cache with the tx-visible tree (a prior
        // rollback may have shrunk it) before a split/grow can consult or extend it.
        // No-op for non-tx stores.
        refreshLeftEdgesIfTx();
        ArrayDeque<Long> stack = new ArrayDeque<>();
        long[] cursor = new long[1];
        Node n = lockLeaf(key, cursor, stack);
        long current = cursor[0];
        // the locked node now covers key: (lo, fence] or rightmost. `locked` (0 = released)
        // backstops EVERY exit — a store/format/listener throw between here and the deliberate
        // release below would otherwise leak the non-reentrant lock and self-deadlock a retry.
        long locked = current;
        try {
            int pos = keyFormat.search(n.keys, key);
            if (pos >= 0) { // key present
                V old = valueAt(n.values, pos);
                if (onlyIfAbsent) { // putIfAbsent: leave existing value untouched
                    unlockNode(current);
                    locked = 0;
                    return old; // no mutation: no counter change, no listener
                }
                Object values = setValue(n.values, pos, value);
                if (valueInline)
                    store.update(current, new Node(n.flags, n.link, n.keys, values, n.fence), nodeSer);
                try {
                    fireModifiedSync(key, old, value); // under leaf lock: preserves same-key event order
                } finally {
                    unlockNode(current); // a throwing listener must never leak the node lock
                    locked = 0;
                }
                fireModified(key, old, value); // update: existing key, counter unchanged
                return old;
            }
            int ip = -pos - 1;
            Object newKeys = keyFormat.insert(n.keys, ip, key);
            Object newVals = insertValue(n.values, ip, value);
            if (keyFormat.size(newKeys) <= maxNodeSize) {
                store.update(current, new Node(n.flags, n.link, newKeys, newVals, n.fence), nodeSer);
                // counter BEFORE the listeners: the mutation is committed, so a throwing listener
                // must not be able to desync sizeLong() from the tree contents
                addToCounter(1L);
                try {
                    fireModifiedSync(key, null, value); // under leaf lock: preserves same-key event order
                } finally {
                    unlockNode(current);
                    locked = 0;
                }
            } else {
                // publishes the split, bumps the counter, fires sync listeners under the leaf lock,
                // and releases the leaf lock itself on EVERY exit (throw-safe) — see the fire-point
                // note on the method. The lock is handed off, so clear our backstop first.
                locked = 0;
                splitLeafAndPropagate(current, n, newKeys, newVals, stack, key, value);
            }
            fireModified(key, null, value);
            return null;
        } finally {
            if (locked != 0) unlockNode(locked);
        }
    }

    /**
     * Split the (locked) overfull leaf and propagate separators upward. Lock
     * discipline: the right sibling B is written FIRST (referent before referrer),
     * the left half republished with link=q — from that instant the split is fully
     * searchable through the link — and only THEN is the child lock released and the
     * parent locked. At most one node lock is held at any moment.
     *
     * <p>SYNC-LISTENER FIRE POINT: after the left half republishes (the insert's commit
     * point) but before this leaf's lock is released. Firing under the left half's lock
     * is ordering-safe even when the inserted key landed in B: until the separator
     * reaches the parent, every locking path to B goes through A's link, and
     * {@link #lockCovering} is hand-over-hand — so no writer can lock B (or A) and fire
     * a competing same-key event before this one. The counter is bumped before the
     * listeners and the unlock sits in a finally, so a throwing listener cannot desync
     * sizeLong() or leak the node lock.
     *
     * <p>THROW RECOVERY: a throwing listener must NOT skip {@code propagateSplit}. In
     * particular, skipping the ROOT leaf split's propagation would leave the tree
     * root-less-but-searchable, and a LATER split of B would spin forever waiting for a
     * level-1 left edge that was never created ({@link #leftEdge}). So the listener's
     * throwable is captured, the lock released, separator/root propagation COMPLETED,
     * and only then is the listener's exception rethrown. If propagation itself also
     * fails (a store-level failure), that exception wins — it signals the more severe,
     * structural problem — with the listener's throwable attached as suppressed.
     */
    private void splitLeafAndPropagate(long recid, Node orig, Object keys, Object values,
                                       ArrayDeque<Long> stack, K key, V value) {
        // Root-growth gate — BOTH conditions, in this order (flags first so the cheap
        // test short-circuits the store read on ordinary non-root splits):
        //   (a) `orig` still carries LEFT|RIGHT (root shape) — the CONCURRENCY
        //       serialization (a concurrent root splitter flips this under its lock);
        //   (b) the root pointer authoritatively names this recid — rejects a CRAFTED
        //       descendant falsely flagged root-shaped (see {@link #isCurrentRoot}).
        // On a flags/identity mismatch wasRoot is false → treat it as a normal
        // (non-root) split and propagate the separator upward (mirrors rust).
        boolean wasRoot =
                (orig.flags & (LEFT | RIGHT)) == (LEFT | RIGHT) && isCurrentRoot(recid);
        long locked = recid; // caller handed us the leaf lock; backstop until published/handed off
        try {
            int total = keyFormat.size(keys);
            int h = total / 2;
            int bFlags = orig.flags & ~LEFT; // B keeps RIGHT status of the original
            Node b = new Node(bFlags, orig.link,
                    keyFormat.copyRange(keys, h, total),
                    copyValues(values, h, total),
                    orig.fence);
            // Pre-publication: a throw here (copyRange/store.put/store.update) leaves the split
            // UNpublished — `b` is orphan garbage referenced by nothing — so the finally just
            // releases the leaf lock and no poison is needed (the tree is unchanged).
            long q = store.put(b, nodeSer);
            @SuppressWarnings("unchecked")
            K sep = keyFormat.get(keys, h - 1);
            Node a = new Node(orig.flags & ~RIGHT, q,
                    keyFormat.copyRange(keys, 0, h),
                    copyValues(values, 0, h),
                    keyFormat.insert(keyFormat.empty(), 0, sep));
            store.update(recid, a, nodeSer); // PUBLISHES the split (searchable via A's link)
            // Split published: separator/root propagation MUST still run — the counter bump and
            // sync listeners are SECONDARY. Capture their failures and complete propagation first,
            // because skipping it would leave a level uncreated and park a later split of B forever
            // in leftEdge (THROW RECOVERY, class + method javadoc).
            Throwable counterFailure = null;
            try {
                // counter BEFORE the listeners: the mutation is committed, so a throwing listener
                // must not be able to desync sizeLong() from the tree contents
                addToCounter(1L);
            } catch (RuntimeException | Error e) {
                counterFailure = e;
            }
            Throwable listenerFailure = null;
            try {
                fireModifiedSync(key, null, value);
            } catch (RuntimeException | Error e) {
                listenerFailure = e; // rethrown AFTER propagation completes — see javadoc
            } finally {
                unlockNode(recid);
                locked = 0;
            }
            try {
                propagateSplit(recid, q, sep, wasRoot, stack, 1);
            } catch (RuntimeException | Error e) {
                poisoned = true; // split published but its separator/root never landed
                if (counterFailure != null) suppress(e, counterFailure);
                if (listenerFailure != null) suppress(e, listenerFailure);
                throw e; // structural error is primary
            }
            // Propagation completed. Surface a secondary failure — counter first (a desynced
            // O(1) count is corruption, so poison), then the listener error.
            if (counterFailure != null) {
                poisoned = true;
                if (listenerFailure != null) suppress(counterFailure, listenerFailure);
                rethrow(counterFailure);
            }
            if (listenerFailure instanceof Error) throw (Error) listenerFailure;
            if (listenerFailure != null) throw (RuntimeException) listenerFailure;
        } finally {
            if (locked != 0) unlockNode(locked); // pre-publication throw: release the leaf lock
        }
    }

    /**
     * Insert (sep → newChild placed right of oldChild) into the parent level; split
     * upward as needed. {@code level} counts from 0 = leaf; the parent being entered
     * is at {@code level}. No lock is held on entry.
     */
    private void propagateSplit(long oldChild, long newChild, K sep, boolean childWasRoot,
                                ArrayDeque<Long> stack, int level) {
        while (true) {
            if (childWasRoot) {
                // the split node was the whole top level: grow the tree by one level
                Object rootKeys = keyFormat.insert(keyFormat.empty(), 0, sep);
                Node newRoot = new Node(DIR | LEFT | RIGHT, 0L, rootKeys, new long[]{oldChild, newChild}, null);
                lockNode(rootRecidRecid);
                try {
                    long newRootRecid = store.put(newRoot, nodeSer);
                    store.update(rootRecidRecid, newRootRecid, Serializers.LONG);
                    if (rootCacheable) cachedRootRecid = newRootRecid;
                    long[] le = leftEdges;
                    // Root grow appends exactly one level, so the cache must describe a tree of
                    // height `level`. A live mismatch means the cache drifted from the tree (a
                    // crafted uneven-depth tree, or a tx-store leftEdges left stale by a rollback
                    // that shrank height): poison and fail hard rather than a debug-only assert or
                    // a silent append onto a stale vector naming deleted/reused recids.
                    if (le.length != level) {
                        poisoned = true;
                        throw new DBException.DataCorruption(
                                "btree leftEdges/level mismatch (stale structural cache); reopen the store");
                    }
                    long[] grown = Arrays.copyOf(le, le.length + 1);
                    grown[le.length] = newRootRecid;
                    leftEdges = grown;
                } finally {
                    unlockNode(rootRecidRecid);
                }
                return;
            }
            long start = stack.isEmpty() ? leftEdge(level) : stack.pop();
            long[] cursor = new long[1];
            Node n = lockCovering(start, sep, true, cursor);
            long current = cursor[0];
            // `locked` (0 = released) backstops every throw between here and the deliberate
            // release below: the dir-level store/format work must not leak this node lock.
            long locked = current;
            try {
                int pos = keyFormat.search(n.keys, sep);
                // a separator is the max key of a freshly split left half — strictly
                // inside the covering dir's bounds and distinct from every existing
                // separator (key spaces of siblings are disjoint)
                assert pos < 0 : "duplicate separator " + sep;
                int ip = -pos - 1;
                Object newKeys = keyFormat.insert(n.keys, ip, sep);
                long[] newChildren = insertLong(n.children(), ip + 1, newChild);
                int keysLen = keyFormat.size(newKeys);
                if (keysLen <= maxNodeSize) {
                    store.update(current, new Node(n.flags, n.link, newKeys, newChildren, null), nodeSer);
                    unlockNode(current);
                    locked = 0;
                    return;
                }
                // split dir node
                int h = keysLen / 2;
                Node b = new Node(n.flags & ~LEFT, n.link,
                        keyFormat.copyRange(newKeys, h, keysLen),
                        Arrays.copyOfRange(newChildren, h, newChildren.length),
                        null);
                long q = store.put(b, nodeSer);
                @SuppressWarnings("unchecked")
                K parentSep = keyFormat.get(newKeys, h - 1);
                Node a = new Node(n.flags & ~RIGHT, q,
                        keyFormat.copyRange(newKeys, 0, h),
                        Arrays.copyOfRange(newChildren, 0, h),
                        null);
                store.update(current, a, nodeSer);
                // Same dual gate as the leaf split: LEFT|RIGHT flag (concurrency
                // serialization) AND authoritative root-pointer identity (crafted-flag
                // protection), flag test first so ordinary dir splits skip the store read.
                childWasRoot =
                        (n.flags & (LEFT | RIGHT)) == (LEFT | RIGHT) && isCurrentRoot(current);
                unlockNode(current);
                locked = 0;
                oldChild = current;
                newChild = q;
                sep = parentSep;
                level++;
            } finally {
                if (locked != 0) unlockNode(locked);
            }
        }
    }

    /** Left-edge recid of {@code level}; spins while a concurrent root split that
     *  creates this level is between publishing the child and appending here. Bails with
     *  {@link org.mapdb.DBException.DataCorruption} if the map was poisoned by a failed
     *  root grow — a level that will never be published must not park a writer forever. */
    private long leftEdge(int level) {
        while (true) {
            long[] le = leftEdges;
            if (level < le.length) return le[level];
            checkPoison();
            LockSupport.parkNanos(100L);
        }
    }

    private static long[] insertLong(long[] arr, int pos, long value) {
        long[] r = new long[arr.length + 1];
        System.arraycopy(arr, 0, r, 0, pos);
        r[pos] = value;
        System.arraycopy(arr, pos, r, pos + 1, arr.length - pos);
        return r;
    }

    /** Remove the key's entry; returns the previous value or null. */
    @SuppressWarnings("unchecked")
    @Override public V remove(Object key) {
        if (key == null) throw new NullPointerException();
        return removeInternal((K) key, null);
    }

    /** Remove only if the current value equals {@code value} (via the value format's equals). */
    @SuppressWarnings("unchecked")
    @Override public boolean remove(Object key, Object value) {
        if (key == null || value == null) throw new NullPointerException();
        return removeInternal((K) key, (V) value) != null;
    }

    /** Blind remove (API parity): returns only WHETHER an entry existed. Same cost as
     *  {@link #remove(Object)} here — the leaf is materialized under the lock anyway. */
    public boolean removeOnly(K key) {
        if (key == null) throw new NullPointerException();
        return removeInternal(key, null) != null;
    }

    /**
     * Remove under the covering leaf's lock. {@code expected != null} makes it a CAS:
     * removes only when the live value equals {@code expected} (value-format equality).
     * @return the removed value, or null if nothing was removed.
     */
    private V removeInternal(K key, V expected) {
        V old;
        if (valueInline) {
            old = removeInternalWithExternalReadBarrier(key, expected);
        } else {
            externalValueLock.writeLock().lock();
            try { old = removeInternalWithExternalReadBarrier(key, expected); }
            finally { externalValueLock.writeLock().unlock(); }
        }
        if (old != null) fireModified(key, old, null); // counter already adjusted under the lock
        return old;
    }

    private V removeInternalWithExternalReadBarrier(K key, V expected) {
        long[] cursor = new long[1];
        Node n = lockLeaf(key, cursor, null);
        long current = cursor[0];
        // `locked` (0 = released) backstops every throw between here and the deliberate
        // release below — store/format work under the lock must never leak it.
        long locked = current;
        try {
            int pos = keyFormat.search(n.keys, key);
            // the locked node covers key, so an absent key is definitively absent
            if (pos < 0) {
                unlockNode(current);
                locked = 0;
                return null;
            }
            V old = valueAt(n.values, pos);
            if (expected != null && !valueFormat.element().equals(old, expected)) {
                unlockNode(current);
                locked = 0;
                return null;
            }
            // no node merging/rebalance (mapdb3 semantics): the fence — the leaf's
            // coverage bound — is retained even when the max entry is removed
            Node updated = new Node(n.flags, n.link,
                    keyFormat.delete(n.keys, pos), deleteValue(n.values, pos), n.fence);
            store.update(current, updated, nodeSer);
            if (!valueInline) store.delete((Long) nodeValueGet(n.values, pos), valueFormat.element());
            // counter BEFORE the listeners (mutation committed): a throwing listener must not desync
            // sizeLong(); the unlock sits in a finally so it can never leak the node lock either
            addToCounter(-1L);
            try {
                fireModifiedSync(key, old, null); // under leaf lock: preserves same-key event order
            } finally {
                unlockNode(current);
                locked = 0;
            }
            return old;
        } finally {
            if (locked != 0) unlockNode(locked);
        }
    }

    /** Replace only if present; returns the previous value or null. */
    @Override public V replace(K key, V value) {
        if (key == null || value == null) throw new NullPointerException();
        return replaceInternal(key, null, value);
    }

    /** Replace only if the current value equals {@code oldValue} (value-format equality). */
    @Override public boolean replace(K key, V oldValue, V newValue) {
        if (key == null || oldValue == null || newValue == null) throw new NullPointerException();
        return replaceInternal(key, oldValue, newValue) != null;
    }

    /**
     * Replace under the covering leaf's lock (never inserts). {@code expected != null}
     * gates on value-format equality. @return the previous value, or null if absent /
     * value mismatch.
     */
    private V replaceInternal(K key, V expected, V newValue) {
        long[] cursor = new long[1];
        Node n = lockLeaf(key, cursor, null);
        long current = cursor[0];
        // `locked` (0 = released) backstops every throw until the deliberate release below.
        long locked = current;
        try {
            int pos = keyFormat.search(n.keys, key);
            if (pos < 0) {
                unlockNode(current);
                locked = 0;
                return null;
            }
            V old = valueAt(n.values, pos);
            if (expected != null && !valueFormat.element().equals(old, expected)) {
                unlockNode(current);
                locked = 0;
                return null;
            }
            Object values = setValue(n.values, pos, newValue);
            if (valueInline)
                store.update(current, new Node(n.flags, n.link, n.keys, values, n.fence), nodeSer);
            try {
                fireModifiedSync(key, old, newValue); // under leaf lock: preserves same-key event order
            } finally {
                unlockNode(current); // a throwing listener must never leak the node lock
                locked = 0;
            }
            fireModified(key, old, newValue); // update of an existing key: counter unchanged
            return old;
        } finally {
            if (locked != 0) unlockNode(locked);
        }
    }

    // ================= iteration =================

    /** Ascending entry iterator over the whole map (weakly consistent: walks leaf links). */
    public Iterator<Map.Entry<K, V>> entryIterator() {
        return entryIterator(null, true, null, true);
    }

    /** Leaf that routes {@code lo} (leftmost leaf when {@code lo == null}), reached by the
     *  same unlocked routing as the writers; the bounded scan then follows links rightward. */
    private Node firstLeafForLowerBound(K lo) {
        long current = rootRecid();
        Node n = load(current);
        while (n.isDir()) {
            int childIdx = (lo == null) ? 0 : routeChild(n, lo);
            current = childIdx < 0 ? n.link : n.children()[childIdx];
            n = load(current);
        }
        return n;
    }

    /**
     * Bounded ascending entry iterator over {@code [lo,hi]} (null bound = open),
     * weakly consistent. Descends to the leaf covering {@code lo}, positions at the
     * first in-range entry, then scans leaf links; empty leaves are skipped and the
     * scan stops at the upper bound. Subsequent leaves need no re-filtering: their keys
     * are strictly greater than {@code lo} (keys only move right).
     */
    Iterator<Map.Entry<K, V>> entryIterator(K lo, boolean loInc, K hi, boolean hiInc) {
        if (valueInline) return entryIteratorLive(lo, loInc, hi, hiInc);
        // Do not retain a leaf snapshot after releasing the value-reclamation barrier. Resume
        // from the last emitted key on each step; O(log n) traversal per entry, O(1) memory.
        return new Iterator<>() {
            K resume = lo;
            boolean resumeInclusive = loInc;
            Map.Entry<K, V> next;
            boolean ready, done;

            private void advance() {
                if (ready || done) return;
                externalValueLock.readLock().lock();
                try {
                    Iterator<Map.Entry<K, V>> live =
                            entryIteratorLive(resume, resumeInclusive, hi, hiInc);
                    if (!live.hasNext()) { done = true; return; }
                    next = live.next();
                    resume = next.getKey();
                    resumeInclusive = false;
                    ready = true;
                } finally { externalValueLock.readLock().unlock(); }
            }

            @Override public boolean hasNext() { advance(); return ready; }
            @Override public Map.Entry<K, V> next() {
                advance();
                if (!ready) throw new NoSuchElementException();
                ready = false;
                return next;
            }
        };
    }

    private Iterator<Map.Entry<K, V>> entryIteratorLive(K lo, boolean loInc, K hi, boolean hiInc) {
        Node startLeaf = firstLeafForLowerBound(lo);
        int sp;
        if (lo == null) {
            sp = 0;
        } else {
            int p = keyFormat.search(startLeaf.keys, lo);
            sp = p >= 0 ? (loInc ? p : p + 1) : (-p - 1);
        }
        final int startPos = sp;

        return new Iterator<>() {
            Node leaf = startLeaf;
            int pos = startPos;
            boolean done = false, has = false;
            // startPos skips the bulk of the start leaf, but an unlocked descent can land
            // LEFT of lo's true leaf during a split published through leaf links before the
            // parent separator — then following links would surface keys < lo. Re-filter the
            // lower bound until the first in-range key; ascending order lets us then stop.
            boolean loPending = lo != null;
            K nk;
            V nv;

            private void advance() {
                if (has || done) return;
                for (;;) {
                    while (leaf != null && pos >= keyFormat.size(leaf.keys)) {
                        leaf = leaf.link == 0 ? null : load(leaf.link);
                        pos = 0;
                    }
                    if (leaf == null) { done = true; return; }
                    K k = keyFormat.get(leaf.keys, pos);
                    if (loPending) {
                        int c = keyFormat.compare(k, lo);
                        if (c < 0 || (c == 0 && !loInc)) { pos++; continue; } // below lo: skip
                        loPending = false;
                    }
                    if (hi != null) {
                        int c = keyFormat.compare(k, hi);
                        if (c > 0 || (c == 0 && !hiInc)) { done = true; return; }
                    }
                    V value = valueAt(leaf.values, pos);
                    pos++;
                    nk = k;
                    nv = value;
                    has = true;
                    return;
                }
            }

            @Override public boolean hasNext() { advance(); return has; }

            @Override public Map.Entry<K, V> next() {
                advance();
                if (!has) throw new NoSuchElementException();
                has = false;
                return new AbstractMap.SimpleImmutableEntry<>(nk, nv);
            }
        };
    }

    /**
     * Bounded DESCENDING entry iterator over {@code [lo,hi]} (null bound = open), weakly
     * consistent. FIRST CUT (spec-btree-map item B3): materialize the bounded ascending
     * range and iterate it reversed — O(range) memory, reuses the tested ascending
     * iterator. The long-term impl (per-leaf buffering + re-descend to the predecessor
     * leaf, O(maxLeafEntries) memory) is scoped out. Used ONLY for actual reverse
     * ITERATION (descending views / descendingKeySet); single-entry predecessor queries
     * (floor/lower/lastEntry) go through the shared view's O(1)-memory ascending
     * scan-keep-last instead, and {@link #pollLastEntry} picks its candidate the same way,
     * so neither materializes the range.
     */
    Iterator<Map.Entry<K, V>> descendingEntryIterator(K lo, boolean loInc, K hi, boolean hiInc) {
        ArrayList<Map.Entry<K, V>> buf = new ArrayList<>();
        for (Iterator<Map.Entry<K, V>> it = entryIterator(lo, loInc, hi, hiInc); it.hasNext(); ) buf.add(it.next());
        ListIterator<Map.Entry<K, V>> li = buf.listIterator(buf.size());
        return new Iterator<>() {
            @Override public boolean hasNext() { return li.hasPrevious(); }
            @Override public Map.Entry<K, V> next() {
                if (!li.hasPrevious()) throw new NoSuchElementException();
                return li.previous();
            }
        };
    }

    /**
     * Atomically remove and return the LEAST in-range entry, or null when empty
     * (spec-btree-map item B4, correctness risk #1). Retry loop: pick the first ascending
     * in-range key as an ADVISORY candidate, then {@code removeInternal(k, v)} — an atomic
     * conditional remove that succeeds only if the live value still equals {@code v}. On
     * failure (value changed / key gone) retry with a fresh least candidate. The successful
     * conditional remove is the mutation point, so poll never removes a value it did not
     * return; the "least in range" selection is weakly consistent, exactly like the
     * iterators (a smaller key inserted concurrently between the read and the remove may be
     * missed — see class javadoc). Returns an immutable snapshot.
     */
    Map.Entry<K, V> pollFirstEntry(K lo, boolean loInc, K hi, boolean hiInc) {
        for (;;) {
            Iterator<Map.Entry<K, V>> it = entryIterator(lo, loInc, hi, hiInc);
            if (!it.hasNext()) return null;
            Map.Entry<K, V> e = it.next();
            if (removeInternal(e.getKey(), e.getValue()) != null) {
                return new AbstractMap.SimpleImmutableEntry<>(e.getKey(), e.getValue());
            }
        }
    }

    /**
     * Atomically remove and return the GREATEST in-range entry, or null when empty. Same
     * conditional-remove retry discipline as {@link #pollFirstEntry}; the candidate is the
     * greatest in-range key, found by an O(1)-memory ascending scan-keep-last (NOT the
     * O(range)-memory descending iterator). Cost is O(range) TIME per attempt — more
     * expensive than {@code pollFirstEntry}; documented as a first
     * cut pending a predecessor-leaf primitive.
     */
    Map.Entry<K, V> pollLastEntry(K lo, boolean loInc, K hi, boolean hiInc) {
        for (;;) {
            Iterator<Map.Entry<K, V>> it = entryIterator(lo, loInc, hi, hiInc);
            if (!it.hasNext()) return null;
            Map.Entry<K, V> last = null;
            while (it.hasNext()) last = it.next();
            if (removeInternal(last.getKey(), last.getValue()) != null) {
                return new AbstractMap.SimpleImmutableEntry<>(last.getKey(), last.getValue());
            }
        }
    }

    /**
     * Entry count. When an O(1) size counter is enabled (Feature A) this reads the
     * counter record in O(1); otherwise it walks the leaf chain (weakly consistent).
     * {@link #size()} saturates this to int.
     */
    public long sizeLong() {
        if (counterEnabled()) return store.get(counterRecid, Serializers.LONG);
        long count = 0;
        Iterator<Map.Entry<K, V>> it = entryIterator();
        while (it.hasNext()) {
            it.next();
            count++;
        }
        return count;
    }

    private long sizeLong(K lo, boolean loInc, K hi, boolean hiInc) {
        long count = 0;
        Iterator<Map.Entry<K, V>> it = entryIterator(lo, loInc, hi, hiInc);
        while (it.hasNext()) {
            it.next();
            count++;
        }
        return count;
    }

    // ================= columnar single-column scan (spec-missing #10 / R7) =================

    /**
     * Scan ONE value column over the ascending key range {@code [fromKey, toKey]} (a null bound is
     * open), invoking {@code consumer} with each in-range key paired with that column's value —
     * WITHOUT materializing whole value rows on the serialized (onBytes) path. Requires the value
     * format to be a {@link ColumnarValueFormat}: the byte path reads only the requested column's
     * contiguous bytes via {@link ColumnarValueFormat#columnCursor} (the columnar scan win), while
     * the heap (onObject) path falls back to a materialized row read (no byte-level win on heap).
     *
     * <p>Weakly consistent, exactly like {@link #entryIterator}: it descends unlocked to the leaf
     * covering {@code fromKey} and follows leaf links rightward, so a concurrent split may cause a
     * key to be seen once, skipped, or duplicated to the same extent as ordinary iteration. Keys
     * are delivered in ascending order. Read-only.
     *
     * @param column   0-based column index into the {@link ColumnarValueFormat} schema
     * @param consumer receives (key, column-value); the column value is the boxed cell
     * @throws UnsupportedOperationException if the value format is not a {@link ColumnarValueFormat}
     * @throws IndexOutOfBoundsException     if {@code column} is outside the schema
     * @throws NullPointerException          if {@code consumer} is null
     */
    public void forEachValueColumn(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive,
                                   int column, BiConsumer<? super K, Object> consumer) {
        if (consumer == null) throw new NullPointerException(); // eager, like Map.forEach
        if (!valueInline)
            throw new UnsupportedOperationException("column scan is unavailable for external values");
        if (!(valueFormat instanceof ColumnarValueFormat))
            throw new UnsupportedOperationException(
                    "value format is not columnar: " + valueFormat.getClass().getName());
        ColumnarValueFormat cf = (ColumnarValueFormat) valueFormat;
        if (column < 0 || column >= cf.columnCount())
            throw new IndexOutOfBoundsException("column=" + column + " columns=" + cf.columnCount());

        LeafColumnScan action = new LeafColumnScan(cf, column, fromKey, fromInclusive, toKey, toInclusive);
        action.loPendingIn = fromKey != null;
        long recid = firstLeafRecidForLowerBound(fromKey);
        while (recid != 0) {
            recid = store.read(recid, action);
            // emit AFTER the (validated) read — never run user code inside a RecordRead, which the
            // store may invoke more than once on an optimistic-read retry
            for (int i = 0; i < action.keys.size(); i++) consumer.accept(action.keys.get(i), action.vals.get(i));
            if (action.done) return;
            action.loPendingIn = action.loPendingOut;
        }
    }

    /** Leaf recid covering {@code lo} (leftmost leaf when null), by the same unlocked routing as
     *  {@link #firstLeafForLowerBound}; the caller then follows leaf links rightward. */
    private long firstLeafRecidForLowerBound(K lo) {
        long current = rootRecid();
        Node n = load(current);
        while (n.isDir()) {
            int childIdx = (lo == null) ? 0 : routeChild(n, lo);
            current = childIdx < 0 ? n.link : n.children()[childIdx];
            n = load(current);
        }
        return current;
    }

    /**
     * Per-leaf push-down action for {@link #forEachValueColumn}: collects one leaf's in-range
     * (key, column-value) pairs, reading only the requested column's bytes on the byte path, and
     * returns the next leaf recid to visit — or 0 to STOP (upper bound reached, or rightmost leaf).
     * Optimistic-read safe: every {@code onBytes}/{@code onObject} invocation FULLY
     * overwrites all output state before decoding, tolerates torn bytes (bounded, then throws), and
     * runs no user callback (the outer loop emits once, after validation).
     */
    private final class LeafColumnScan implements RecordRead {
        private final ColumnarValueFormat cf;
        private final int column;
        private final K fromKey, toKey;
        private final boolean fromInclusive, toInclusive;

        // input: set by the outer loop before each store.read (this action never writes it)
        boolean loPendingIn;
        // outputs: fully reset at the start of every invocation
        final ArrayList<K> keys = new ArrayList<>();
        final ArrayList<Object> vals = new ArrayList<>();
        boolean loPendingOut;
        boolean done;

        LeafColumnScan(ColumnarValueFormat cf, int column, K fromKey, boolean fromInclusive,
                       K toKey, boolean toInclusive) {
            this.cf = cf; this.column = column;
            this.fromKey = fromKey; this.fromInclusive = fromInclusive;
            this.toKey = toKey; this.toInclusive = toInclusive;
        }

        private void resetOutputs() {
            keys.clear();
            vals.clear();
            loPendingOut = loPendingIn;
            done = false;
        }

        @Override public long onBytes(DataInput2 in, int size) {
            resetOutputs();
            int h = in.unpackInt();
            int flags = h & 0xF;
            int keysLen = h >>> 4;
            if ((flags & DIR) != 0) throw new IllegalStateException("column scan reached a directory node");
            // every key occupies >= 1 serialized byte, so keysLen > size is torn/corrupt
            if (keysLen > size) throw new IllegalStateException("node header keysLen exceeds record size");
            long link = (flags & RIGHT) != 0 ? 0L : in.unpackLong();
            Object keyGroup = keyFormat.deserialize(in, keysLen); // leaves `in` at value-group start

            int loPos = (loPendingIn && fromKey != null) ? lowerPos(keyGroup) : 0;
            int toPos = upperPos(keyGroup, keysLen);             // also sets `done`
            loPendingOut = loPendingIn && loPos >= keysLen;      // stay pending if leaf is entirely below lo
            int fromPos = Math.min(loPos, toPos);

            GroupCursor<Object> vc = cf.columnCursor(in, keysLen, column, fromPos, toPos);
            int i = fromPos;
            while (vc.next()) {
                keys.add(keyFormat.get(keyGroup, i));
                vals.add(vc.value());
                i++;
            }
            return done ? 0L : link;
        }

        @SuppressWarnings("unchecked")
        @Override public long onObject(Object record) {
            resetOutputs();
            Node n = (Node) record;
            if (n.isDir()) throw new IllegalStateException("column scan reached a directory node");
            int keysLen = keyFormat.size(n.keys);
            int loPos = (loPendingIn && fromKey != null) ? lowerPos(n.keys) : 0;
            int toPos = upperPos(n.keys, keysLen);
            loPendingOut = loPendingIn && loPos >= keysLen;
            for (int i = Math.min(loPos, toPos); i < toPos; i++) {
                keys.add(keyFormat.get(n.keys, i));
                vals.add(((Object[]) valueFormat.get(n.values, i))[column]); // materialized fallback
            }
            return done ? 0L : n.link;
        }

        /** First position satisfying the lower bound (mirrors entryIterator's startPos). */
        private int lowerPos(Object keyGroup) {
            int p = keyFormat.search(keyGroup, fromKey);
            return p >= 0 ? (fromInclusive ? p : p + 1) : (-p - 1);
        }

        /** Exclusive end position for the upper bound; sets {@code done} when no later leaf can
         *  contain an in-range key (the bound key, or a key strictly beyond it, lives here). */
        private int upperPos(Object keyGroup, int keysLen) {
            if (toKey == null) return keysLen;
            int p = keyFormat.search(keyGroup, toKey);
            int tp;
            if (p >= 0) {
                tp = toInclusive ? p + 1 : p;
                done = true;                 // found the bound key: every later leaf's keys are greater
            } else {
                tp = -p - 1;
                done = tp < keysLen;         // an existing key strictly beyond the bound lives here
            }
            return Math.min(tp, keysLen);
        }
    }

    // ================= Map / SortedMap surface =================

    /** {@link Map#size()}: saturates at {@link Integer#MAX_VALUE}; use {@link #sizeLong}. */
    @Override public int size() {
        long n = sizeLong();
        return n > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) n;
    }

    @Override public boolean isEmpty() { return !entryIterator().hasNext(); }

    /** Removes every entry (leaving empty leaves linked, mapdb3 semantics). Weakly
     *  consistent: iterates a live ascending cursor, routing each key to {@link #remove}
     *  so per-key remove listeners fire and the counter is decremented. There is no final
     *  counter reset: each successful removal already accounts for itself, and resetting
     *  after the scan could erase a concurrent insertion's increment. */
    @Override public void clear() {
        for (Iterator<Map.Entry<K, V>> it = entryIterator(); it.hasNext(); ) {
            remove(it.next().getKey());
        }
    }

    @Override public void forEach(BiConsumer<? super K, ? super V> action) {
        if (action == null) throw new NullPointerException(); // Map.forEach contract: eager, even when empty
        for (Iterator<Map.Entry<K, V>> it = entryIterator(); it.hasNext(); ) {
            Map.Entry<K, V> e = it.next();
            action.accept(e.getKey(), e.getValue());
        }
    }

    /**
     * Weakly-consistent entry-set delegated to the shared full-range navigable view
     * (ascending); {@code iterator.remove()} routes the last key to {@link #remove(Object)},
     * so {@code values} (derived by {@link AbstractMap}) and the navigable key-set stay
     * mutable.
     */
    @Override public Set<Map.Entry<K, V>> entrySet() { return fullView().entrySet(); }

    /** Null for the natural order of built-in formats; see {@link GroupFormat#comparator}. */
    @Override public Comparator<? super K> comparator() { return keyFormat.comparator(); }

    // ---- NavigableMap / ConcurrentNavigableMap: delegated to the full-range view ----

    /**
     * Cached open-bounds ascending {@link ConcurrentOrderedNavigableView} that backs the
     * whole navigable surface (nav queries, sub-maps, key-sets, descending map). Immutable
     * and stateless beyond the adapter, so one instance is reused; navigation is not the
     * hot path (spec-btree-map: navigation may be slower, the write path is unchanged).
     */
    private volatile ConcurrentOrderedNavigableView<K, V> fullView;

    private ConcurrentOrderedNavigableView<K, V> fullView() {
        ConcurrentOrderedNavigableView<K, V> v = fullView;
        if (v == null) {
            v = new ConcurrentOrderedNavigableView<>(new Adapter(), null, true, null, true, false);
            fullView = v;
        }
        return v;
    }

    @Override public Map.Entry<K, V> firstEntry() { return fullView().firstEntry(); }
    @Override public Map.Entry<K, V> lastEntry() { return fullView().lastEntry(); }
    @Override public Map.Entry<K, V> pollFirstEntry() { return fullView().pollFirstEntry(); }
    @Override public Map.Entry<K, V> pollLastEntry() { return fullView().pollLastEntry(); }
    @Override public Map.Entry<K, V> lowerEntry(K key) { return fullView().lowerEntry(key); }
    @Override public Map.Entry<K, V> floorEntry(K key) { return fullView().floorEntry(key); }
    @Override public Map.Entry<K, V> ceilingEntry(K key) { return fullView().ceilingEntry(key); }
    @Override public Map.Entry<K, V> higherEntry(K key) { return fullView().higherEntry(key); }
    @Override public K lowerKey(K key) { return fullView().lowerKey(key); }
    @Override public K floorKey(K key) { return fullView().floorKey(key); }
    @Override public K ceilingKey(K key) { return fullView().ceilingKey(key); }
    @Override public K higherKey(K key) { return fullView().higherKey(key); }

    /** Routes through {@link #firstEntry()} (empty-leaf-safe). */
    @Override public K firstKey() {
        Map.Entry<K, V> e = firstEntry();
        if (e == null) throw new NoSuchElementException();
        return e.getKey();
    }

    /** Routes through {@link #lastEntry()}; O(n) (no back-links to the rightmost live key). */
    @Override public K lastKey() {
        Map.Entry<K, V> e = lastEntry();
        if (e == null) throw new NoSuchElementException();
        return e.getKey();
    }

    @Override public NavigableSet<K> keySet() { return fullView().navigableKeySet(); }
    @Override public NavigableSet<K> navigableKeySet() { return fullView().navigableKeySet(); }
    @Override public NavigableSet<K> descendingKeySet() { return fullView().descendingKeySet(); }
    @Override public ConcurrentNavigableMap<K, V> descendingMap() { return fullView().descendingMap(); }

    @Override public ConcurrentNavigableMap<K, V> subMap(K fromKey, boolean fromInc, K toKey, boolean toInc) {
        return fullView().subMap(fromKey, fromInc, toKey, toInc);
    }
    @Override public ConcurrentNavigableMap<K, V> headMap(K toKey, boolean inclusive) {
        return fullView().headMap(toKey, inclusive);
    }
    @Override public ConcurrentNavigableMap<K, V> tailMap(K fromKey, boolean inclusive) {
        return fullView().tailMap(fromKey, inclusive);
    }
    @Override public ConcurrentNavigableMap<K, V> subMap(K fromKey, K toKey) {
        return fullView().subMap(fromKey, toKey);
    }
    @Override public ConcurrentNavigableMap<K, V> headMap(K toKey) { return fullView().headMap(toKey); }
    @Override public ConcurrentNavigableMap<K, V> tailMap(K fromKey) { return fullView().tailMap(fromKey); }

    /** Bridges this map to the shared range/view layer (stateless — cheap to re-create). */
    private final class Adapter implements ConcurrentOrderedMapAdapter<K, V> {
        @Override public Comparator<? super K> comparator() { return keyFormat.comparator(); }
        @Override public int compare(K a, K b) { return keyFormat.compare(a, b); }
        @Override public V get(Object key) { return BTreeMap.this.get(key); }
        @Override public boolean containsKey(Object key) { return BTreeMap.this.containsKey(key); }
        @Override public V put(K key, V value) { return BTreeMap.this.put(key, value); }
        @Override public V remove(Object key) { return BTreeMap.this.remove(key); }
        @Override public boolean remove(Object key, Object value) { return BTreeMap.this.remove(key, value); }
        @Override public V putIfAbsent(K key, V value) { return BTreeMap.this.putIfAbsent(key, value); }
        @Override public V replace(K key, V value) { return BTreeMap.this.replace(key, value); }
        @Override public boolean replace(K key, V oldValue, V newValue) { return BTreeMap.this.replace(key, oldValue, newValue); }
        @Override public boolean valueEquals(V a, V b) { return valueFormat.element().equals(a, b); }
        @Override public Iterator<Map.Entry<K, V>> entryIterator(K lo, boolean loInc, K hi, boolean hiInc) {
            return BTreeMap.this.entryIterator(lo, loInc, hi, hiInc);
        }
        @Override public Iterator<Map.Entry<K, V>> descendingEntryIterator(K lo, boolean loInc, K hi, boolean hiInc) {
            return BTreeMap.this.descendingEntryIterator(lo, loInc, hi, hiInc);
        }
        @Override public Map.Entry<K, V> pollFirstEntry(K lo, boolean loInc, K hi, boolean hiInc) {
            return BTreeMap.this.pollFirstEntry(lo, loInc, hi, hiInc);
        }
        @Override public Map.Entry<K, V> pollLastEntry(K lo, boolean loInc, K hi, boolean hiInc) {
            return BTreeMap.this.pollLastEntry(lo, loInc, hi, hiInc);
        }
        @Override public long sizeLong(K lo, boolean loInc, K hi, boolean hiInc) {
            return BTreeMap.this.sizeLong(lo, loInc, hi, hiInc);
        }
    }
}
