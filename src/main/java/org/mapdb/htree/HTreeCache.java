package org.mapdb.htree;

import org.mapdb.DBException;
import org.mapdb.MapModificationListener;
import org.mapdb.ModificationAwareMap;
import org.mapdb.MapExtra;
import org.mapdb.hash.Hasher;
import org.mapdb.hash.Hashers;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;
import org.mapdb.ser.Serializer;
import org.mapdb.ser.Serializers;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;

/**
 * HTreeMap-as-cache: the segmented hash tree with per-entry EXPIRATION (TTL and/or
 * max-size eviction). A self-contained sibling of the lean {@link HTreeMap}
 * (kept as a separate collection rather than feature flags in the hot paths);
 * {@link DirTree} is shared unchanged.
 *
 * Structure on top of HTreeMap's segment/dir-tree layout:
 *  - buckets store TRIPLES {@code [key, value, queueNodeRecid]} (values inline);
 *  - each segment owns a FIFO {@link ExpireQueue} ordered oldest-first: every entry
 *    has exactly one queue node carrying {@code (expiryTimestamp, leafRecid)};
 *  - each segment owns an O(1) IN-MEMORY size counter ({@link AtomicLong}, bumped under
 *    the write lock) so {@code sizeLong} and max-size eviction never traverse the tree
 *    NOR touch the store (see the counter note below).
 *
 * Expiration config (fixed at create, persisted in the header):
 *  - {@code ttl} (ms, 0 = off): entries expire {@code ttl} after their last WRITE
 *    (put/overwrite re-stamps), or after their last ACCESS when {@code accessOrder}
 *    is set (get bumps the entry to the queue head with a fresh stamp). The deadline
 *    is OPEN: an entry is still live at exactly {@code writeTime + ttl} and expires
 *    once that instant has PASSED (mapdb3's {@code timestamp < now} semantics).
 *  - {@code accessOrder}: get/putIfAbsent-hit bump the entry to the queue head —
 *    with {@code maxSize} this makes eviction LRU instead of FIFO. Gets then take
 *    the segment WRITE lock.
 *  - {@code maxSize} (0 = off): APPROXIMATE bound, enforced per segment as
 *    {@code maxSize / segmentCount} (floor; mapdb3 semantics) — actual size can
 *    briefly exceed it by up to one entry per segment, and {@code maxSize <
 *    segmentCount} degenerates to a share of 0 (sweeps drain whole segments). Use
 *    fewer segments (e.g. {@code concShift=0}, dirShift 4, levels 8) when the bound
 *    must be tight.
 *  - {@code storeSize} (bytes, 0 = off): APPROXIMATE byte budget on the BACKING STORE
 *    (mapdb3's {@code expireStoreSize}). When {@code store.getCurrentSize()} exceeds it,
 *    sweeps evict oldest entries until back under budget. The budget is GLOBAL (the whole
 *    store) but enforced by whichever segment a mutating op touches, evicting that
 *    segment's own oldest — so the store is assumed DEDICATED to this cache (a shared
 *    store's other collections count against the budget and can never be evicted). A
 *    foreground op evicts at most {@link #FG_STORE_EVICT_CAP} store-size victims (bounded
 *    latency / no hot-segment over-drain); {@link #expireEvict()} enforces it exactly and
 *    globally. Requires a store that reports {@link Store#getCurrentSize()} &gt; 0
 *    (StoreDirect); stores reporting 0 silently disable store-size eviction. The budget is
 *    page-granular in practice (StoreDirect grows in 1 MB pages), so the resident size
 *    hovers up to ~one page above the budget under insertion pressure.
 *
 * Optional OVERFLOW (runtime wiring via {@link #overflow(Map)}, NOT persisted): a bigger
 * map that receives CAPACITY-evicted entries ({@code overflow.put}) and backs get() misses
 * ({@code overflow.get} + promote). Only CAPACITY victims (max-size / store-size) overflow —
 * they are still logically LIVE, evicted only to make room; TTL-EXPIRED victims are logically
 * DEAD and are plain-deleted (never overflowed), so a value that ONLY ever expires by TTL —
 * never a capacity victim — is never handed to overflow and can never be resurrected. (A value
 * that FIRST left the cache as a still-live capacity victim legitimately stays in the overflow
 * backing tier and IS reloadable by a later miss-load with a fresh stamp — the documented
 * spillover-backing-store model; cache-tier TTL expiry of a promoted copy does not purge that
 * backing copy.) Handoff/miss-load ALWAYS run with NO segment lock held
 * (snapshot-under-lock, flush-after-unlock), so the overflow map's own locks cannot deadlock
 * with the cache; overflow graphs must be acyclic (self-overflow is rejected). Best-effort:
 * an {@code overflow.put} that throws propagates and the entry is already gone from the cache.
 *
 * Eviction is a SWEEP, not a visibility barrier, and runs in the foreground at the
 * start of every mutating op (put/putIfAbsent/remove, and get when accessOrder) on
 * that entry's segment, or for all segments via {@link #expireEvict()} — callers
 * wanting a background sweeper schedule {@code expireEvict} on their own executor.
 * Reads are STRICT nevertheless: get/containsKey/iteration check the entry's expiry
 * stamp and treat TTL-expired entries as absent even before a sweep reclaims them.
 * {@link #sizeLong()} and {@link #isEmpty()} are counter-based (PHYSICAL occupancy):
 * they count not-yet-swept expired entries.
 *
 * Implements {@link java.util.Map} and {@link java.util.concurrent.ConcurrentMap}.
 * The ConcurrentMap CAS ops ({@code remove(k,v)}, {@code replace}) treat a
 * TTL-expired entry as absent (the foreground sweep runs first) and, being writes,
 * re-stamp and bump the matched entry to the queue head like {@code put}. Caveat on
 * the collection views: {@link #size}/{@link #isEmpty} are counter-based PHYSICAL
 * occupancy (they include un-swept expired entries), whereas {@link #entrySet} and
 * the other iterators yield only LIVE entries — so {@code entrySet().size()} can
 * exceed the number actually iterated until a sweep reclaims the expired ones.
 * The same divergence makes the inherited {@link java.util.AbstractMap#equals}/
 * {@code hashCode} (which mix {@code size()} with live-entry iteration) unreliable
 * while un-swept expired entries exist — do not compare caches by {@code equals}
 * during an expiry window. {@link #size} additionally saturates at
 * {@link Integer#MAX_VALUE}; use {@link #sizeLong} for the exact count.
 *
 * A single queue per segment stays expiry-ordered because every enqueue/bump uses
 * the same fixed TTL — timestamps are monotone in queue order, so sweeps stop at
 * the first live node having reclaimed everything expired.
 *
 * SIZE COUNTERS (in-memory). The per-segment size counter is an {@link AtomicLong}
 * held in memory, NOT a store record on the hot path — a cache mutates constantly, so
 * a persisted counter (read + rewritten on every put/remove/evict) was pure write
 * amplification. It is authoritative for {@link #sizeLong}/{@link #isEmpty}/max-size and
 * is mutated under the segment write lock; {@link #sizeLong} reads it under the read lock,
 * and the {@code AtomicLong} guarantees visibility even for a non-thread-safe store (no
 * locks). On OPEN each segment's counter is RECONSTRUCTED from its {@link ExpireQueue}
 * length (the {@code counter == queue length == physical entries} invariant), so a
 * committed size survives close+reopen exactly. The per-segment counter records are still
 * ALLOCATED in the header (format stability + the record-count leak baselines) but are
 * NEVER read or written after create — a deliberate choice over dropping them + bumping the
 * header version, chosen to keep every existing reopen path byte-compatible.
 *
 * Time comes from an injectable clock (test hook); production maps use
 * {@code System.currentTimeMillis}. Hashing, locking, iteration snapshots and the
 * one-live-mutable-handle contract are HTreeMap's.
 */
public final class HTreeCache<K, V> extends AbstractMap<K, V> implements MapExtra<K, V> {

    private final Store store;
    private final Serializer<K> keySer;
    private final Serializer<V> valueSer;

    private final int concShift, dirShift, levels;
    private final int hashSeed;
    private final long headerRecid;
    private final long[] segmentRoots;
    /** Per-segment entry-count records (LONG). Still allocated (header format stability +
     *  record-leak baselines) but NEVER read/written after create — the live counter is the
     *  in-memory {@link #counters} (see class doc, SIZE COUNTERS). */
    private final long[] counterRecids;
    /** Per-segment in-memory size counter, authoritative for size/isEmpty/max-size. Mutated
     *  under the segment write lock, read under the read lock; reconstructed at open from the
     *  queue length. */
    private final AtomicLong[] counters;
    private final ExpireQueue[] queues;

    /** Expiry config; see class doc. Persisted in the header. */
    private final long ttl, maxSize, storeSize;
    private final boolean accessOrder;

    /** Max entries a single FOREGROUND sweep may evict (TTL + capacity COMBINED), 0 = off =
     *  unbounded (backward-compatible default). Persisted in the header. Bounds the worst-case
     *  latency of the op that trips a mass TTL cohort — with one fixed TTL a write burst all
     *  expires at once, and the next mutating op would otherwise sweep the whole cohort under
     *  the segment write lock. Throttled-out expired entries stay physically present (strict
     *  reads treat them as absent; {@link #sizeLong} counts them) until a later op or the
     *  UNBOUNDED {@link #expireEvict()} reclaims them. Never affects correctness or contents. */
    private final long maxEvictPerOp;

    /**
     * Optional OVERFLOW target (runtime wiring, NOT persisted — re-supply on every open,
     * like the serializers). When set, every CAPACITY-evicted entry (max-size / store-size)
     * is handed off via {@code overflow.put(k,v)}, and a get() MISS consults
     * {@code overflow.get(k)} and promotes the value back into the cache. TTL-EXPIRED victims
     * are logically DEAD and are NEVER handed off (a plain delete) — otherwise a later
     * miss-load would resurrect a TTL-expired value with a fresh stamp, breaking the
     * strict-read {@code TTL-expired == absent} invariant. Explicit
     * remove/clear do NOT hand off. Handoff and miss-load ALWAYS run with NO segment lock
     * held (snapshot-under-lock, flush-after-unlock) so the arbitrary overflow map's own
     * locks can never form a cycle with ours.
     *
     * <p>SEMANTICS — the overflow is a SPILLOVER BACKING STORE, NOT a coherent second tier
     * (mapdb3's {@code expireOverflow}/{@code valueLoader} model). Mutations apply to the
     * CACHE tier only and do NOT propagate to the overflow: (a) a miss-load promotes a copy
     * but LEAVES it in the overflow (so the key can transiently exist in both tiers); (b)
     * {@code remove}/{@code clear} on the cache do NOT delete from the overflow, so a key
     * that lives only in the overflow stays reloadable via {@code get}; (c) a cache-only
     * overwrite does not update an older overflow copy. Callers wanting coherent
     * remove/overwrite must mirror those ops onto the overflow map themselves. This is a
     * deliberate design choice, pinned by {@code HTreeCacheOverflowSemanticsTest}.
     *
     * <p>Volatile: rewiring {@link #overflow} concurrently with in-flight ops may send (or
     * drop, if cleared) those ops' already-collected victims to the newly-wired map — rewire
     * during quiescence. Best-effort: an {@code overflow.put} that throws propagates to the
     * caller and the entry is already gone from the cache (the cache/store stay valid).
     */
    private volatile Map<K, V> overflow;

    /**
     * Optional EVICTION LISTENER (runtime wiring, NOT persisted — re-supply on every open, like
     * {@link #overflow}). Invoked once for EVERY entry that LEAVES the cache due to a SWEEP, with
     * the {@link EvictionReason} ({@link EvictionReason#TTL} vs {@link EvictionReason#MAX_SIZE} /
     * {@link EvictionReason#STORE_SIZE}). It fires for ALL eviction reasons — including TTL victims,
     * which are deliberately NOT handed to {@link #overflow}. Explicit {@code remove}/{@code clear}
     * and a {@code put}-overwrite are NOT evictions and never fire it.
     *
     * <p>LOCK SAFETY: like the overflow handoff, the listener is arbitrary user code invoked with
     * NO segment lock held (snapshot-under-lock, flush-after-unlock), so it may freely re-enter this
     * cache without deadlock. ORDERING vs overflow: when both are wired, a capacity victim goes to
     * BOTH — the OVERFLOW handoff runs FIRST, then the listener. A listener that throws PROPAGATES
     * to the caller, but the victims are already safe in the overflow map by then; a throwing
     * {@code overflow.put} propagates and skips the listener for that batch.
     */
    private volatile EvictionListener<K, V> evictionListener;

    /** Reentrancy guard: true while this thread is flushing to / promoting from the overflow
     *  map, so nested evictions on THIS cache do NOT re-hand-off. This breaks overflow-graph
     *  cycles the moment control re-enters an already-guarded cache (a 2-cycle A&lt;-&gt;B is
     *  bounded; a longer finite cycle is bounded by its length; an acyclic chain recurses to
     *  its end). It does not bound arbitrary user code inside an overflow map. */
    private final ThreadLocal<Boolean> inOverflow = ThreadLocal.withInitial(() -> Boolean.FALSE);

    /** Max store-size victims a single FOREGROUND sweep may evict (bounds worst-case op
     *  latency and one-segment over-drain of a GLOBAL byte budget; see class doc).
     *  {@link #expireEvict()} is unbounded. */
    private static final long FG_STORE_EVICT_CAP = 16;

    private final Hasher<? super K> hasher;
    private final LongSupplier clock;
    private final ReentrantReadWriteLock[] locks;
    private final boolean threadSafe;

    private final int segmentShift;
    private final long indexMask;
    private final int concMask;

    private final LeafSerializer leafSer;
    private final MapRuntime<K, V> runtime = new MapRuntime<>();

    private HTreeCache(Store store, Serializer<K> keySer, Serializer<V> valueSer,
                       long headerRecid, Header header,
                       Hasher<? super K> hasher, LongSupplier clock) {
        this.store = store;
        this.keySer = keySer;
        this.valueSer = valueSer;
        this.headerRecid = headerRecid;
        this.concShift = header.concShift;
        this.dirShift = header.dirShift;
        this.levels = header.levels;
        this.hashSeed = header.hashSeed;
        this.ttl = header.ttl;
        this.maxSize = header.maxSize;
        this.storeSize = header.storeSize;
        this.maxEvictPerOp = header.maxEvictPerOp;
        this.accessOrder = header.accessOrder;
        this.segmentRoots = header.segmentRoots;
        this.counterRecids = header.counterRecids;
        this.queues = new ExpireQueue[segmentRoots.length];
        this.counters = new AtomicLong[segmentRoots.length];
        for (int i = 0; i < queues.length; i++) {
            queues[i] = new ExpireQueue(store, header.queueTails[i], header.queueHeads[i],
                    header.queueHeadPrevs[i]);
            // Reconstruct the in-memory counter from the persisted queue (invariant:
            // counter == queue length == physical entries), so a committed size survives reopen.
            counters[i] = new AtomicLong(queues[i].size());
        }
        this.hasher = hasher != null ? hasher : Hashers.objectHasher();
        this.clock = clock != null ? clock : System::currentTimeMillis;
        this.segmentShift = levels * dirShift;
        this.indexMask = (1L << segmentShift) - 1;
        this.concMask = (1 << concShift) - 1;
        this.threadSafe = store.isThreadSafe();
        this.locks = new ReentrantReadWriteLock[1 << concShift];
        for (int i = 0; i < locks.length; i++) locks[i] = new ReentrantReadWriteLock();
        this.leafSer = new LeafSerializer();
    }

    private static void checkConfig(int concShift, int dirShift, int levels,
                                    long ttl, boolean accessOrder, long maxSize, long storeSize,
                                    long maxEvictPerOp) {
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
        if (ttl < 0) throw new IllegalArgumentException("ttl must be >= 0");
        if (maxSize < 0) throw new IllegalArgumentException("maxSize must be >= 0");
        if (storeSize < 0) throw new IllegalArgumentException("storeSize must be >= 0");
        if (maxEvictPerOp < 0) throw new IllegalArgumentException("maxEvictPerOp must be >= 0");
        if (ttl == 0 && maxSize == 0 && storeSize == 0)
            throw new IllegalArgumentException("cache needs ttl and/or maxSize and/or storeSize; for a plain map use HTreeMap");
    }

    // ================= create / open =================

    /** Default geometry (16 segments, 4x7); see class doc for the expiry params. */
    public static <K, V> HTreeCache<K, V> create(Store store, Serializer<K> keySer,
                                                 Serializer<V> valueSer,
                                                 long ttl, boolean accessOrder, long maxSize) {
        return create(store, keySer, valueSer, 4, 7, 4, ttl, accessOrder, maxSize, 0, null);
    }

    /** Default geometry with an optional {@code storeSize} byte budget (0 = off; requires a
     *  store that reports {@link Store#getCurrentSize()}, e.g. StoreDirect). */
    public static <K, V> HTreeCache<K, V> create(Store store, Serializer<K> keySer,
                                                 Serializer<V> valueSer,
                                                 long ttl, boolean accessOrder, long maxSize,
                                                 long storeSize) {
        return create(store, keySer, valueSer, 4, 7, 4, ttl, accessOrder, maxSize, storeSize, null);
    }

    /** @param hasher custom {@link Hasher} (e.g. {@link Hashers#mixing} over a content
     *                hash); null = {@link Hashers#objectHasher()}. Must be supplied
     *                identically on every open of this cache. */
    public static <K, V> HTreeCache<K, V> create(Store store, Serializer<K> keySer,
                                                 Serializer<V> valueSer,
                                                 int concShift, int dirShift, int levels,
                                                 long ttl, boolean accessOrder, long maxSize,
                                                 Hasher<? super K> hasher) {
        return create(store, keySer, valueSer, concShift, dirShift, levels,
                ttl, accessOrder, maxSize, 0, hasher);
    }

    /** Full public create with the optional {@code storeSize} byte budget (bytes, 0 = off). */
    public static <K, V> HTreeCache<K, V> create(Store store, Serializer<K> keySer,
                                                 Serializer<V> valueSer,
                                                 int concShift, int dirShift, int levels,
                                                 long ttl, boolean accessOrder, long maxSize,
                                                 long storeSize, Hasher<? super K> hasher) {
        return create(store, keySer, valueSer, concShift, dirShift, levels,
                ttl, accessOrder, maxSize, storeSize, new Random().nextInt(), hasher, null);
    }

    /** Full public create with the optional {@code storeSize} byte budget AND the optional
     *  {@code maxEvictPerOp} foreground-sweep throttle (entries per op, 0 = off = unbounded;
     *  see class doc). */
    public static <K, V> HTreeCache<K, V> create(Store store, Serializer<K> keySer,
                                                 Serializer<V> valueSer,
                                                 int concShift, int dirShift, int levels,
                                                 long ttl, boolean accessOrder, long maxSize,
                                                 long storeSize, long maxEvictPerOp,
                                                 Hasher<? super K> hasher) {
        return create(store, keySer, valueSer, concShift, dirShift, levels,
                ttl, accessOrder, maxSize, storeSize, maxEvictPerOp, new Random().nextInt(),
                hasher, null);
    }

    /** Package-private (storeSize = 0): fixed hashSeed + injectable clock, for deterministic tests. */
    static <K, V> HTreeCache<K, V> create(Store store, Serializer<K> keySer,
                                          Serializer<V> valueSer,
                                          int concShift, int dirShift, int levels,
                                          long ttl, boolean accessOrder, long maxSize,
                                          int hashSeed, Hasher<? super K> hasher,
                                          LongSupplier clock) {
        return create(store, keySer, valueSer, concShift, dirShift, levels,
                ttl, accessOrder, maxSize, 0, hashSeed, hasher, clock);
    }

    /** Package-private: fixed hashSeed + injectable clock + storeSize, for deterministic tests. */
    static <K, V> HTreeCache<K, V> create(Store store, Serializer<K> keySer,
                                          Serializer<V> valueSer,
                                          int concShift, int dirShift, int levels,
                                          long ttl, boolean accessOrder, long maxSize,
                                          long storeSize, int hashSeed, Hasher<? super K> hasher,
                                          LongSupplier clock) {
        return create(store, keySer, valueSer, concShift, dirShift, levels,
                ttl, accessOrder, maxSize, storeSize, 0, hashSeed, hasher, clock);
    }

    /**
     * Full public create with an explicit {@code hashSeed} (feature parity with mapdb3's
     * {@code hashSeed(int)} builder option). The {@code hashSeed} is persisted in the header
     * and consumed ONLY at create time — {@link #open} reconstructs it from the header, so no
     * seed is passed to open. Uses the production clock ({@code System.currentTimeMillis});
     * the injectable-clock variant stays test-only (package-private).
     */
    public static <K, V> HTreeCache<K, V> create(Store store, Serializer<K> keySer,
                                                 Serializer<V> valueSer,
                                                 int concShift, int dirShift, int levels,
                                                 long ttl, boolean accessOrder, long maxSize,
                                                 long storeSize, long maxEvictPerOp, int hashSeed,
                                                 Hasher<? super K> hasher) {
        return create(store, keySer, valueSer, concShift, dirShift, levels,
                ttl, accessOrder, maxSize, storeSize, maxEvictPerOp, hashSeed, hasher, null);
    }

    /** Package-private funnel: fixed hashSeed + injectable clock + storeSize + maxEvictPerOp. */
    static <K, V> HTreeCache<K, V> create(Store store, Serializer<K> keySer,
                                          Serializer<V> valueSer,
                                          int concShift, int dirShift, int levels,
                                          long ttl, boolean accessOrder, long maxSize,
                                          long storeSize, long maxEvictPerOp, int hashSeed,
                                          Hasher<? super K> hasher, LongSupplier clock) {
        checkConfig(concShift, dirShift, levels, ttl, accessOrder, maxSize, storeSize, maxEvictPerOp);
        if (storeSize > 0 && ttl == 0 && maxSize == 0 && store.getCurrentSize() == 0)
            throw new IllegalArgumentException(
                    "storeSize is the only eviction bound but this store reports getCurrentSize()==0 "
                    + "(no byte accounting): the cache would never evict");
        int segments = 1 << concShift;
        long[] roots = new long[segments];
        long[] counters = new long[segments];
        long[] qTails = new long[segments], qHeads = new long[segments], qHeadPrevs = new long[segments];
        for (int i = 0; i < segments; i++) {
            roots[i] = store.put(DirTree.dirEmpty(), DirTree.DIR_SER);
            counters[i] = store.put(0L, Serializers.LONG);
            ExpireQueue q = ExpireQueue.create(store);
            qTails[i] = q.tailRecid;
            qHeads[i] = q.headRecid;
            qHeadPrevs[i] = q.headPrevRecid;
        }
        Header header = new Header(concShift, dirShift, levels, hashSeed, ttl, accessOrder,
                maxSize, storeSize, maxEvictPerOp, roots, counters, qTails, qHeads, qHeadPrevs);
        long headerRecid = store.put(header, Header.SER);
        return new HTreeCache<>(store, keySer, valueSer, headerRecid, header, hasher, clock);
    }

    public static <K, V> HTreeCache<K, V> open(Store store, long headerRecid,
                                               Serializer<K> keySer, Serializer<V> valueSer) {
        return open(store, headerRecid, keySer, valueSer, null, null);
    }

    /** @param hasher must match the hasher supplied at create (or null if none was). */
    public static <K, V> HTreeCache<K, V> open(Store store, long headerRecid,
                                               Serializer<K> keySer, Serializer<V> valueSer,
                                               Hasher<? super K> hasher) {
        return open(store, headerRecid, keySer, valueSer, hasher, null);
    }

    /** Package-private: injectable clock, for deterministic tests. */
    static <K, V> HTreeCache<K, V> open(Store store, long headerRecid,
                                        Serializer<K> keySer, Serializer<V> valueSer,
                                        Hasher<? super K> hasher, LongSupplier clock) {
        Header header = store.get(headerRecid, Header.SER);
        return new HTreeCache<>(store, keySer, valueSer, headerRecid, header, hasher, clock);
    }

    /** Recid of the header record; persist this to reopen the cache. */
    public long headerRecid() { return headerRecid; }

    /**
     * Wire (or clear with {@code null}) the optional OVERFLOW map — a bigger/slower map
     * (e.g. a persistent HTreeMap/BTreeMap on disk) that receives SWEEP-evicted entries
     * and serves get() misses. Runtime-only (NOT persisted): re-supply it after every
     * {@link #open}, like the serializers. Handoff and miss-load run with no cache lock
     * held (see the {@link #overflow} field doc), so any thread-safe map is safe here.
     * @throws IllegalArgumentException if {@code overflow == this} (a cache cannot overflow
     *         into itself); overflow graphs must be acyclic.
     */
    public HTreeCache<K, V> overflow(Map<K, V> overflow) {
        if (overflow == this) throw new IllegalArgumentException("a cache cannot overflow into itself");
        this.overflow = overflow;
        return this;
    }

    /** The current overflow map, or null. */
    public Map<K, V> overflow() { return overflow; }

    /** Why an entry was evicted, passed to the {@link EvictionListener}. A TTL-expired entry
     *  always reports {@link #TTL} even if it also happened to sit over a capacity bound (it is
     *  logically dead and is never handed to overflow). */
    public enum EvictionReason {
        /** The entry's TTL lapsed (logically dead; never overflowed). */
        TTL,
        /** Evicted to keep the segment within its max-size share (a live capacity victim). */
        MAX_SIZE,
        /** Evicted to keep the backing store under its byte budget (a live capacity victim). */
        STORE_SIZE
    }

    /** Callback invoked (with NO cache lock held) when an entry leaves the cache due to a sweep;
     *  see the {@link #evictionListener} field doc for reasons, ordering vs overflow and the
     *  best-effort throwing contract. */
    @FunctionalInterface
    public interface EvictionListener<K, V> {
        void evicted(K key, V value, EvictionReason reason);
    }

    /**
     * Wire (or clear with {@code null}) the optional EVICTION LISTENER — a callback fired once per
     * SWEEP-evicted entry with its {@link EvictionReason}. Runtime-only (NOT persisted): re-supply
     * it after every {@link #open}, like the serializers / {@link #overflow}. The callback runs with
     * no cache lock held (see the {@link #evictionListener} field doc), so it may re-enter this cache.
     */
    public HTreeCache<K, V> evictionListener(EvictionListener<K, V> listener) {
        this.evictionListener = listener;
        return this;
    }

    /** The current eviction listener, or null. */
    public EvictionListener<K, V> evictionListener() { return evictionListener; }

    public void modificationListenerAdd(MapModificationListener<K, V> listener) { runtime.add(listener); }
    public void modificationListenerRemove(MapModificationListener<K, V> listener) { runtime.remove(listener); }
    public void valueLoader(java.util.function.Function<? super K, ? extends V> loader) {
        runtime.valueLoader = loader;
    }
    @Override public boolean isClosed() { return store.isClosed(); }
    @Override public Serializer<K> keySerializer() { return keySer; }
    @Override public Serializer<V> valueSerializer() { return valueSer; }

    // ================= hashing / locks (HTreeMap's, duplicated on purpose) =================

    private int hash(K key) {
        return hasher.hash(key, hashSeed);
    }

    private int segment(int hash) {
        return (hash >>> segmentShift) & concMask;
    }

    private long index(int hash) {
        return Integer.toUnsignedLong(hash) & indexMask;
    }

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

    /** finally-block unlock + post-unlock eviction flush, keeping {@code body} (the throwable
     *  already propagating from the try body, or null) primary: unlock/flush failures are
     *  suppressed behind it instead of masking it. {@link #flushEvicted} still runs (with no
     *  lock held) even when the unlock itself throws. */
    private void unlockWriteAndFlush(int seg, List<Object[]> victims, Throwable body) {
        Throwable first = body;
        try {
            unlockWrite(seg);
        } catch (RuntimeException | Error t) {
            first = MapRuntime.suppress(first, t);
        }
        try {
            flushEvicted(victims); // no lock held
        } catch (RuntimeException | Error t) {
            first = MapRuntime.suppress(first, t);
        }
        if (body == null) MapRuntime.rethrow(first);
    }

    // ================= expiry plumbing =================

    /** Expiry instant for an entry written/accessed now; 0 = no TTL. Stamps are
     *  persisted as packed longs, so a negative clock or a {@code now + ttl} overflow
     *  is rejected rather than silently corrupting the queue's wire format. */
    private long stamp() {
        if (ttl == 0) return 0;
        long now = clock.getAsLong();
        long s = now + ttl;
        if (now < 0 || s <= 0) // s == 0 would alias the "no TTL" sentinel; s < 0 is overflow
            throw new IllegalArgumentException("clock + ttl out of range: " + now + " + " + ttl);
        return s;
    }

    /** True iff a queue node's stamp says "expired" (strict-read check). */
    private boolean expired(long timestamp) {
        return timestamp != 0 && timestamp < clock.getAsLong();
    }

    /** In-memory per-segment entry count (no store round-trip); see class doc, SIZE COUNTERS. */
    private long counter(int seg) {
        return counters[seg].get();
    }

    private void counterAdd(int seg, long delta) {
        counters[seg].addAndGet(delta);
    }

    /**
     * Sweep one segment (caller holds its write lock): dequeue oldest-first while the
     * segment is over its max-size share, OR the GLOBAL store size is over budget (up to
     * {@code storeEvictCap} victims), OR the node is TTL-expired — removing the entry each
     * node points at, but taking AT MOST {@code maxEvict} entries TOTAL (TTL + capacity
     * combined; the {@code maxEvictPerOp} throttle). Store-size and max-size victims are
     * checked BEFORE the TTL predicate so the monotone-queue stop stays complete for TTL.
     * Stops at the first non-purged node, or once {@code maxEvict} entries have been taken.
     *
     * <p>Throttling is safe for the {@code counter == queue length == physical entries}
     * invariant: a throttled-out expired entry simply stays physically present (strict reads
     * treat it as absent, {@link #sizeLong} counts it) until a later op or the UNBOUNDED
     * {@link #expireEvict} reclaims it — nothing is lost or resurrected.
     *
     * <p>Eviction REASON ({@link EvictionReason}) drives both the listener AND overflow handoff.
     * A CAPACITY victim (max-size / store-size) is still logically LIVE — evicted only to make
     * room — so it is handed to the {@link #overflow} map. A TTL-EXPIRED victim is logically DEAD
     * (strict reads already treat it as absent), so it must NEVER reach the overflow map —
     * otherwise a later miss-load would resurrect it with a fresh timestamp, violating the
     * strict-read TTL invariant (TTL-expired == absent). A victim that is BOTH over a capacity
     * bound AND TTL-expired reports {@link EvictionReason#TTL} (dead wins) and is not overflowed.
     * When {@code victims != null} every purged entry's {@code (key, value, reason)} is appended
     * (already materialized) for the caller to flush AFTER releasing the segment lock
     * ({@link #flushEvicted}); the eviction LISTENER fires for all of them, overflow only for the
     * non-TTL ones.
     *
     * <p>The GLOBAL byte budget is enforced by the touched segment only (per-op approximation,
     * mirroring the per-segment max-size share): {@code store.getCurrentSize()} is re-read per
     * node and drops as extents are freed, so the loop converges; {@code storeEvictCap} bounds
     * how many store-size victims one op may take, preventing a single foreground op from
     * draining a hot segment to pay for a global overshoot ({@link #expireEvict} is unbounded).
     */
    private void evictSegment(int seg, List<Object[]> victims, long storeEvictCap, long maxEvict) {
        long now = clock.getAsLong();
        long over = maxSize == 0 ? 0
                : Math.max(0, counter(seg) - maxSize / segmentRoots.length);
        long[] toTake = {over};
        long[] storeLeft = {storeSize == 0 ? 0 : storeEvictCap};
        long[] taken = {0};
        queues[seg].takeUntil((nodeRecid, node) -> {
            if (taken[0] >= maxEvict) return false; // maxEvictPerOp throttle: stop this foreground sweep
            boolean ttlExpired = node.timestamp != 0 && node.timestamp < now;
            boolean purge = false;
            boolean maxVictim = false, storeVictim = false;
            if (toTake[0] > 0) {
                toTake[0]--;
                purge = true;
                maxVictim = true;
            }
            if (!purge && storeLeft[0] > 0 && store.getCurrentSize() > storeSize) {
                storeLeft[0]--;
                purge = true;
                storeVictim = true;
            }
            if (!purge && ttlExpired) purge = true; // TTL: dead
            if (purge) {
                // A TTL-expired entry is logically DEAD (strict reads treat it as absent) even when
                // it happens to sit over a capacity bound, so it reports TTL and must NEVER overflow —
                // otherwise a later miss-load would resurrect it with a fresh stamp, breaking the
                // strict-read TTL invariant. Non-expired capacity victims report their capacity reason.
                EvictionReason reason = ttlExpired ? EvictionReason.TTL
                        : (maxVictim ? EvictionReason.MAX_SIZE : EvictionReason.STORE_SIZE);
                evictEntry(seg, node.value, nodeRecid, victims, reason);
                taken[0]++;
            }
            return purge;
        });
        // SYNC listeners must see victim removals UNDER the segment lock (per-key ordering vs
        // concurrent puts — a late post-unlock removal event could delete a fresh secondary-index
        // entry written by a racing put). Fired after the takeUntil sweep completes so queue/leaf
        // state is coherent even if a listener throws. Callers pass a FRESH victims list per op
        // (newVictimList) and call evictSegment once, so this covers exactly this sweep's victims;
        // the deferred events and overflow/eviction-listener flush stay post-unlock (flushEvicted).
        if (victims != null) runtime.fireRemovalBatch(victims, true, true);
    }

    /** Remove the entry whose queue node is {@code nodeRecid} from leaf
     *  {@code leafRecid}. The queue node itself is NOT touched — the takeUntil
     *  sweep dequeues and deletes it right after this returns. When {@code victims != null}
     *  the {@code (key, value, reason)} is captured (materialized) BEFORE the leaf is touched,
     *  for the listener/overflow flush. */
    @SuppressWarnings("unchecked")
    private void evictEntry(int seg, long leafRecid, long nodeRecid, List<Object[]> victims,
                            EvictionReason reason) {
        Object[] leaf = store.get(leafRecid, leafSer);
        for (int i = 0; i < leaf.length; i += 3) {
            if ((Long) leaf[i + 2] != nodeRecid) continue;
            K key = (K) leaf[i];
            if (victims != null) victims.add(new Object[]{key, leaf[i + 1], reason});
            int h = hash(key);
            assert segment(h) == seg : "queue node crossed segments";
            long idx = index(h);
            if (leaf.length == 3) {
                boolean removed = DirTree.treeRemove(dirShift, segmentRoots[seg], store,
                        levels - 1, idx);
                assert removed : "dir tree lost the bucket it just resolved";
                store.delete(leafRecid, leafSer);
            } else {
                store.update(leafRecid, leafDelete(leaf, i), leafSer);
            }
            counterAdd(seg, -1);
            return;
        }
        throw new DBException.DataCorruption("queue node not found in its leaf");
    }

    /** A fresh list to collect sweep victims for the eviction listener and/or overflow handoff,
     *  or null when NEITHER is due. The listener needs EVERY victim (all reasons); overflow needs
     *  only the non-TTL ones AND only when not already inside an overflow flush on this thread
     *  (the reentrancy guard) — so a list is allocated when a listener is wired, OR an overflow
     *  handoff is due. {@link #flushEvicted} re-checks each sink at flush time. */
    private List<Object[]> newVictimList() {
        boolean needListener = evictionListener != null || runtime.hasListeners();
        boolean needOverflow = overflow != null && !inOverflow.get();
        return (needListener || needOverflow) ? new ArrayList<>(4) : null;
    }

    /** Flush sweep victims to the OVERFLOW map and/or the eviction LISTENER with NO cache lock held
     *  (caller must have released the segment lock). The OVERFLOW handoff runs FIRST ({@code
     *  overflow.put} for the non-TTL / capacity victims): the entries are already gone from the
     *  cache, so a throwing listener must not be able to skip the handoff — that would silently
     *  drop still-live capacity victims. Then the LISTENER runs for every victim (all reasons).
     *  Overflow is reentrancy-guarded so a victim's own eviction inside the overflow map cannot
     *  ping-pong back into this cache's handoff. A throwing listener or overflow.put propagates. */
    @SuppressWarnings("unchecked")
    private void flushEvicted(List<Object[]> victims) {
        if (victims == null || victims.isEmpty()) return;
        EvictionListener<K, V> el = evictionListener;
        Map<K, V> ov = overflow;
        boolean doOverflow = ov != null && !inOverflow.get();
        // Overflow first (capacity victims only), then listener (all victims).
        if (doOverflow) {
            inOverflow.set(Boolean.TRUE);
            try {
                for (Object[] v : victims) {
                    if (v[2] != EvictionReason.TTL) ov.put((K) v[0], (V) v[1]);
                }
            } finally {
                inOverflow.set(Boolean.FALSE);
            }
        }
        if (el != null) {
            for (Object[] v : victims) el.evicted((K) v[0], (V) v[1], (EvictionReason) v[2]);
        }
        // deferred listeners only: the victims' SYNC events already fired under the segment lock
        // at sweep time (evictSegment) to preserve per-key ordering against concurrent writers
        for (Object[] v : victims) runtime.fireDeferred((K) v[0], (V) v[1], null, true);
    }

    private static Object[] leafDelete(Object[] leaf, int i) {
        Object[] leaf2 = new Object[leaf.length - 3];
        System.arraycopy(leaf, 0, leaf2, 0, i);
        System.arraycopy(leaf, i + 3, leaf2, i, leaf2.length - i);
        return leaf2;
    }

    /** The per-op total-eviction cap for a FOREGROUND sweep: {@code maxEvictPerOp}, or
     *  unbounded when the throttle is off ({@code maxEvictPerOp == 0}). */
    private long fgMaxEvict() {
        return maxEvictPerOp == 0 ? Long.MAX_VALUE : maxEvictPerOp;
    }

    /** Sweep every segment now (foreground sweeps only cover segments that mutating
     *  ops touch), UNBOUNDED — the escape hatch that reclaims every expired/over-budget
     *  entry regardless of the {@code maxEvictPerOp} foreground throttle. Schedule this on
     *  an executor for background expiration. */
    public void expireEvict() {
        for (int seg = 0; seg < segmentRoots.length; seg++) {
            List<Object[]> victims = newVictimList();
            lockWrite(seg);
            Throwable body = null;
            try {
                // unbounded on BOTH the store-size sub-cap and the total cap: explicit full reclaim
                evictSegment(seg, victims, Long.MAX_VALUE, Long.MAX_VALUE);
            } catch (RuntimeException | Error t) {
                body = t;
                throw t;
            } finally {
                unlockWriteAndFlush(seg, victims, body);
            }
        }
    }

    // ================= get / containsKey =================

    /**
     * Value for {@code key}, or null if absent or TTL-expired (strict even before a
     * sweep reclaims the entry). With {@code accessOrder} this is a WRITE: under the
     * write lock it bumps the hit entry's queue node to the head FIRST, then sweeps the
     * segment — so the max-size sweep evicts some other (now oldest) entry, never the
     * key just read.
     */
    @SuppressWarnings("unchecked")
    @Override public V get(Object keyObj) {
        if (keyObj == null) throw new NullPointerException();
        K key = (K) keyObj;
        int h = hash(key);
        int seg = segment(h);
        long idx = index(h);
        V result;
        if (accessOrder && !store.isReadOnly()) {
            List<Object[]> victims = newVictimList();
            lockWrite(seg);
            Throwable body = null;
            try {
                // LOOKUP + BUMP FIRST, then sweep: bumping the hit to the queue head makes
                // it the NEWEST node, so the max-size sweep below evicts some OTHER (now
                // oldest) entry, never the key we just read. (Sweeping first would let a
                // segment sitting at share+1 evict the current-LRU key on a hit and turn a
                // resident hit into a miss.) A TTL-expired hit is treated as a miss (not
                // bumped/returned) and left for the sweep to reclaim.
                V hit = null;
                Object[] leaf = leafFor(seg, idx);
                if (leaf != null) {
                    for (int i = 0; i < leaf.length; i += 3) {
                        if (keySer.equals((K) leaf[i], key)) {
                            long nodeRecid = (Long) leaf[i + 2];
                            if (!nodeExpired(nodeRecid)) { // expired => miss; sweep reclaims it
                                queues[seg].bump(nodeRecid, stamp());
                                hit = (V) leaf[i + 1];
                            }
                            break;
                        }
                    }
                }
                evictSegment(seg, victims, FG_STORE_EVICT_CAP, fgMaxEvict()); // bumped hit is newest, so safe
                result = hit;
            } catch (RuntimeException | Error t) {
                body = t;
                throw t;
            } finally {
                unlockWriteAndFlush(seg, victims, body);
            }
        } else {
            lockRead(seg);
            try {
                V hit = null;
                Object[] leaf = leafFor(seg, idx);
                if (leaf != null) {
                    for (int i = 0; i < leaf.length; i += 3) {
                        if (keySer.equals((K) leaf[i], key)) {
                            hit = nodeExpired((Long) leaf[i + 2]) ? null : (V) leaf[i + 1];
                            break;
                        }
                    }
                }
                result = hit;
            } finally {
                unlockRead(seg);
            }
        }
        if (result == null) return missLoad(key); // consult overflow (no lock held)
        return result;
    }

    /** On a get() MISS, load {@code key} from the overflow map (if wired) with NO cache lock
     *  held, and promote a non-null value back into the cache. Reentrancy-guarded so a
     *  promote's own eviction/handoff cannot recurse through overflow forever. */
    private V missLoad(K key) {
        Map<K, V> ov = overflow;
        V loaded = null;
        if (ov != null && !inOverflow.get()) {
            inOverflow.set(Boolean.TRUE);
            try {
                loaded = ov.get(key);
            } finally {
                inOverflow.set(Boolean.FALSE);
            }
        }
        if (loaded == null) loaded = runtime.load(key);
        if (loaded == null || store.isReadOnly()) return loaded;
        // putIfAbsent semantics: a concurrent put(key, fresh) between the read-miss and this
        // promote must win — never clobber a fresh value with the stale loaded one.
        putInternal(key, loaded, true); // promote (respects bounds; may evict+hand off others)
        return loaded;
    }

    /** True iff the key has a live (non-expired) entry. Never bumps, never sweeps. */
    @SuppressWarnings("unchecked")
    @Override public boolean containsKey(Object keyObj) {
        if (keyObj == null) throw new NullPointerException();
        K key = (K) keyObj;
        int h = hash(key);
        int seg = segment(h);
        long idx = index(h);
        lockRead(seg);
        try {
            Object[] leaf = leafFor(seg, idx);
            if (leaf == null) return false;
            for (int i = 0; i < leaf.length; i += 3) {
                if (keySer.equals((K) leaf[i], key)) return !nodeExpired((Long) leaf[i + 2]);
            }
            return false;
        } finally {
            unlockRead(seg);
        }
    }

    /** Strict, NON-bumping lookup: the live value for {@code keyObj}, else null
     *  (absent or TTL-expired). Unlike {@link #get} this never re-stamps/evicts even
     *  with {@code accessOrder} — it backs {@link #entrySet}'s {@code contains} so
     *  membership checks stay observational. */
    @SuppressWarnings("unchecked")
    private V getQuiet(Object keyObj) {
        K key = (K) keyObj;
        int h = hash(key);
        int seg = segment(h);
        long idx = index(h);
        lockRead(seg);
        try {
            Object[] leaf = leafFor(seg, idx);
            if (leaf == null) return null;
            for (int i = 0; i < leaf.length; i += 3) {
                if (keySer.equals((K) leaf[i], key)) {
                    return nodeExpired((Long) leaf[i + 2]) ? null : (V) leaf[i + 1];
                }
            }
            return null;
        } finally {
            unlockRead(seg);
        }
    }

    /** Bucket for {@code idx}, or null. Caller holds the segment lock. */
    private Object[] leafFor(int seg, long idx) {
        long leafRecid = DirTree.treeGet(dirShift, segmentRoots[seg], store, levels - 1, idx);
        return leafRecid == 0 ? null : store.get(leafRecid, leafSer);
    }

    /** Strict-read TTL check: one extra record read per hit, only when TTL is on. */
    private boolean nodeExpired(long nodeRecid) {
        if (ttl == 0) return false;
        ExpireQueue.Node node = store.get(nodeRecid, ExpireQueue.Node.SER);
        if (node == null) throw new DBException.DataCorruption("queue node not found");
        return expired(node.timestamp);
    }

    // ================= put =================

    /** Insert or replace; returns the previous live value or null. A write always
     *  re-stamps the entry (TTL-since-write) and bumps it to the queue head. */
    @Override public V put(K key, V value) {
        if (key == null || value == null) throw new NullPointerException();
        return putInternal(key, value, false);
    }

    /** Insert only if absent; returns the existing value (bumped as an access when
     *  {@code accessOrder}), or null if this call inserted. */
    @Override public V putIfAbsent(K key, V value) {
        if (key == null || value == null) throw new NullPointerException();
        return putInternal(key, value, true);
    }

    /** Blind put — insert/overwrite WITHOUT returning the previous value (JCache's
     *  {@code void put}). Identical to {@link #put} otherwise: it sweeps, re-stamps and
     *  bumps the entry. Same cost as {@link #put} (values are inline). */
    public void putOnly(K key, V value) {
        if (key == null || value == null) throw new NullPointerException();
        putInternal(key, value, false);
    }

    @SuppressWarnings("unchecked")
    private V putInternal(K key, V value, boolean onlyIfAbsent) {
        int h = hash(key);
        int seg = segment(h);
        long idx = index(h);
        List<Object[]> victims = newVictimList();
        lockWrite(seg);
        Throwable body = null;
        try {
            evictSegment(seg, victims, FG_STORE_EVICT_CAP, fgMaxEvict()); // sweep; frees headroom first
            long expireAt = stamp(); // may reject a bad clock: fail before allocating
            long leafRecid = DirTree.treeGet(dirShift, segmentRoots[seg], store, levels - 1, idx);
            if (leafRecid == 0) {
                // fresh bucket: leafRecid and its queue node reference each other, so
                // preallocate the leaf recid, enqueue, then fill the leaf (mapdb3 trick)
                leafRecid = store.preallocate();
                long nodeRecid = queues[seg].put(expireAt, leafRecid);
                store.update(leafRecid, new Object[]{key, value, nodeRecid}, leafSer);
                DirTree.treePut(dirShift, segmentRoots[seg], store, levels - 1, idx, leafRecid);
                counterAdd(seg, +1);
                runtime.fire(key, null, value, false);
                return null;
            }
            Object[] leaf = store.get(leafRecid, leafSer);
            for (int i = 0; i < leaf.length; i += 3) {
                if (keySer.equals((K) leaf[i], key)) { // key present
                    V old = (V) leaf[i + 1];
                    long nodeRecid = (Long) leaf[i + 2];
                    // A throttled sweep (maxEvictPerOp) may have SKIPPED this key's own expired node,
                    // so a match is not necessarily live. A logically-absent (expired) match is
                    // (re)written in place and reported as an INSERT (returns null), never revived as
                    // a hit — keeping the strict-read TTL invariant for mutating ops. When the
                    // throttle is off the sweep drains the whole expired prefix, so a match is always
                    // live and the extra node read is skipped (unchanged hot path).
                    boolean live = maxEvictPerOp == 0 || !nodeExpired(nodeRecid);
                    if (onlyIfAbsent && live) {
                        if (accessOrder) queues[seg].bump(nodeRecid, stamp());
                        return old;
                    }
                    queues[seg].bump(nodeRecid, stamp()); // a write re-stamps (revives an expired slot)
                    Object[] leaf2 = leaf.clone(); // never mutate: heap stores alias
                    leaf2[i + 1] = value;
                    store.update(leafRecid, leaf2, leafSer);
                    runtime.fire(key, live ? old : null, value, false);
                    return live ? old : null;
                }
            }
            // full-index hash collision: grow the bucket by one entry
            long nodeRecid = queues[seg].put(stamp(), leafRecid);
            Object[] leaf2 = java.util.Arrays.copyOf(leaf, leaf.length + 3);
            leaf2[leaf.length] = key;
            leaf2[leaf.length + 1] = value;
            leaf2[leaf.length + 2] = nodeRecid;
            store.update(leafRecid, leaf2, leafSer);
            counterAdd(seg, +1);
            runtime.fire(key, null, value, false);
            return null;
        } catch (RuntimeException | Error t) {
            body = t;
            throw t;
        } finally {
            unlockWriteAndFlush(seg, victims, body);
        }
    }

    // ================= remove / replace / clear =================

    /** Remove the key's entry; returns the previous live value or null. */
    @SuppressWarnings("unchecked")
    @Override public V remove(Object key) {
        if (key == null) throw new NullPointerException();
        return removeInternal((K) key, null);
    }

    /** Remove only if the current live value equals {@code value} (via valueSer.equals);
     *  an expired entry is swept and treated as absent (returns false). */
    @SuppressWarnings("unchecked")
    @Override public boolean remove(Object key, Object value) {
        if (key == null || value == null) throw new NullPointerException();
        return removeInternal((K) key, (V) value) != null;
    }

    /** Blind remove — delete the key's entry, returning only WHETHER a live one existed,
     *  never the previous value (JCache's {@code boolean remove}). Sweeps first, so an
     *  expired entry counts as absent. Same cost as {@link #remove(Object)}. */
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
        List<Object[]> victims = newVictimList();
        lockWrite(seg);
        Throwable body = null;
        try {
            evictSegment(seg, victims, FG_STORE_EVICT_CAP, fgMaxEvict()); // an expired entry is swept, not returned
            long leafRecid = DirTree.treeGet(dirShift, segmentRoots[seg], store, levels - 1, idx);
            if (leafRecid == 0) return null;
            Object[] leaf = store.get(leafRecid, leafSer);
            for (int i = 0; i < leaf.length; i += 3) {
                if (!keySer.equals((K) leaf[i], key)) continue;
                // A throttled sweep may have SKIPPED this key's own expired node: treat an expired
                // match as ABSENT (returns null / false for CAS-remove) and leave it physically
                // present for a later sweep / expireEvict to reclaim (and fire its TTL listener).
                if (maxEvictPerOp != 0 && nodeExpired((Long) leaf[i + 2])) return null;
                V old = (V) leaf[i + 1];
                if (expectedValue != null && !valueSer.equals(old, expectedValue)) return null;
                queues[seg].remove((Long) leaf[i + 2], true);
                if (leaf.length == 3) {
                    boolean removed = DirTree.treeRemove(dirShift, segmentRoots[seg], store,
                            levels - 1, idx);
                    assert removed : "dir tree lost the bucket it just resolved";
                    store.delete(leafRecid, leafSer);
                } else {
                    store.update(leafRecid, leafDelete(leaf, i), leafSer);
                }
                counterAdd(seg, -1);
                runtime.fire(key, old, null, false);
                return old;
            }
            return null;
        } catch (RuntimeException | Error t) {
            body = t;
            throw t;
        } finally {
            unlockWriteAndFlush(seg, victims, body);
        }
    }

    /** Replace only if present (and live); returns the previous value or null. Like
     *  {@code put}, a replace re-stamps the entry and bumps it to the queue head. */
    @Override public V replace(K key, V value) {
        if (key == null || value == null) throw new NullPointerException();
        return replaceInternal(key, null, value);
    }

    /** Replace only if the current live value equals {@code oldValue}. */
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
        List<Object[]> victims = newVictimList();
        lockWrite(seg);
        Throwable body = null;
        try {
            evictSegment(seg, victims, FG_STORE_EVICT_CAP, fgMaxEvict()); // an expired entry is swept, not matched
            long expireAt = stamp(); // may reject a bad clock: fail before mutating
            long leafRecid = DirTree.treeGet(dirShift, segmentRoots[seg], store, levels - 1, idx);
            if (leafRecid == 0) return null;
            Object[] leaf = store.get(leafRecid, leafSer);
            for (int i = 0; i < leaf.length; i += 3) {
                if (!keySer.equals((K) leaf[i], key)) continue;
                // A throttled sweep may have SKIPPED this key's own expired node: replace requires a
                // LIVE entry, so treat an expired match as ABSENT (returns null / false) and leave it
                // physically present for a later sweep / expireEvict.
                if (maxEvictPerOp != 0 && nodeExpired((Long) leaf[i + 2])) return null;
                V old = (V) leaf[i + 1];
                if (expectedValue != null && !valueSer.equals(old, expectedValue)) return null;
                queues[seg].bump((Long) leaf[i + 2], expireAt); // a write re-stamps
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
            unlockWriteAndFlush(seg, victims, body);
        }
    }

    /** Remove all entries: frees every bucket record and queue node, zeroes the
     *  counters; segment root recids stay stable (§0.6). */
    @Override public void clear() {
        clearInternal(true, false);
    }

    public void clearWithoutNotification() { clearInternal(false, false); }
    public void clearWithExpire() { clearInternal(true, true); }

    @SuppressWarnings("unchecked")
    private void clearInternal(boolean notify, boolean triggered) {
        // a throwing LISTENER (sync in the batch, or deferred inside unlockWrite) must not
        // abort the loop and leave later segments populated (queues/counters intact) — its
        // segment's removal already committed, so capture, keep clearing, rethrow after.
        // A MUTATION/store failure must abort: the current segment is inconsistent, and
        // continuing would destructively erase healthy later segments behind it.
        Throwable listenerFail = null;
        for (int seg = 0; seg < segmentRoots.length; seg++) {
            lockWrite(seg);
            // collect first, fire only after the segment's WHOLE mutation (buckets, queue,
            // counter) commits — a throwing sync listener must not leave queue nodes pointing
            // at deleted buckets, and sync listeners must only observe committed removals
            List<Object[]> removed = notify ? new ArrayList<>() : null;
            try {
                DirTree.treeClear(segmentRoots[seg], store, levels - 1,
                        (index, leafRecid) -> {
                            if (removed != null) {
                                Object[] leaf = store.get(leafRecid, leafSer);
                                for (int i = 0; i < leaf.length; i += 3)
                                    removed.add(new Object[]{leaf[i], leaf[i + 1]});
                            }
                            store.delete(leafRecid, leafSer);
                        });
                queues[seg].clear();
                counters[seg].set(0L);
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

    // ================= size / iteration =================

    /** O(segments) entry count via the per-segment counters. INCLUDES TTL-expired
     *  entries not yet reclaimed by a sweep (run {@link #expireEvict()} first for
     *  an exact live count). */
    public long sizeLong() {
        long total = 0;
        for (int seg = 0; seg < segmentRoots.length; seg++) {
            lockRead(seg);
            try {
                total += counter(seg);
            } finally {
                unlockRead(seg);
            }
        }
        return total;
    }

    /** Counter-based like {@link #sizeLong()}: false while un-swept TTL-expired
     *  entries still occupy the cache (strict reads already treat them as absent). */
    @Override public boolean isEmpty() {
        for (int seg = 0; seg < segmentRoots.length; seg++) {
            lockRead(seg);
            try {
                if (counter(seg) != 0) return false;
            } finally {
                unlockRead(seg);
            }
        }
        return true;
    }

    /** {@link Map#size()}: PHYSICAL occupancy (includes un-swept TTL-expired entries,
     *  see {@link #sizeLong}) saturated at {@link Integer#MAX_VALUE}. */
    @Override public int size() {
        long n = sizeLong();
        return n > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) n;
    }

    /** Weakly consistent entry-set view over LIVE entries (§0.5): iteration is
     *  {@link #entryIterator}, {@code remove} is CAS. Note {@code size()} here is the
     *  counter-based physical occupancy and may exceed the live entries iterated until
     *  a sweep runs (class doc). {@code AbstractMap} derives keySet/values/containsValue
     *  from this. */
    @Override public Set<Map.Entry<K, V>> entrySet() {
        return new AbstractSet<>() {
            @Override public Iterator<Map.Entry<K, V>> iterator() { return entryIterator(); }
            @Override public int size() { return HTreeCache.this.size(); }
            @Override public boolean isEmpty() { return HTreeCache.this.isEmpty(); }
            @Override public void clear() { HTreeCache.this.clear(); }

            @Override public boolean contains(Object o) {
                if (!(o instanceof Map.Entry)) return false;
                Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
                Object k = e.getKey(), v = e.getValue();
                if (k == null || v == null) return false;
                V cur = getQuiet(k); // observational: never bumps/evicts
                return cur != null && valueSer.equals(cur, castValue(v));
            }

            @Override public boolean remove(Object o) {
                if (!(o instanceof Map.Entry)) return false;
                Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
                Object k = e.getKey(), v = e.getValue();
                return k != null && v != null && HTreeCache.this.remove(k, v);
            }
        };
    }

    @SuppressWarnings("unchecked")
    private V castValue(Object v) { return (V) v; }

    /** Snapshot one segment's LIVE entries under its read lock (§0.5): TTL-expired
     *  entries are skipped (strict reads), matching get/containsKey. */
    @SuppressWarnings("unchecked")
    private List<Map.Entry<K, V>> drainSegment(int seg) {
        ArrayList<Map.Entry<K, V>> out = new ArrayList<>();
        lockRead(seg);
        try {
            DirTree.treeFold(segmentRoots[seg], store, levels - 1, (index, leafRecid) -> {
                Object[] leaf = store.get(leafRecid, leafSer);
                for (int i = 0; i < leaf.length; i += 3) {
                    if (nodeExpired((Long) leaf[i + 2])) continue;
                    out.add(new AbstractMap.SimpleImmutableEntry<>((K) leaf[i], (V) leaf[i + 1]));
                }
            });
        } finally {
            unlockRead(seg);
        }
        return out;
    }

    /** Unordered iterator over live entries; snapshot-per-segment, weakly consistent.
     *  {@link Iterator#remove()} deletes the last-returned key from the live cache, so
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
                HTreeCache.this.remove(lastKey);
            }
        };
    }

    /** Unordered traversal of live entries; the callback runs OUTSIDE segment locks
     *  and may mutate the cache. */
    @Override public void forEach(BiConsumer<? super K, ? super V> action) {
        for (int seg = 0; seg < segmentRoots.length; seg++) {
            for (Map.Entry<K, V> e : drainSegment(seg)) {
                action.accept(e.getKey(), e.getValue());
            }
        }
    }

    // ================= record formats =================

    /**
     * Bucket wire format: packInt(entryCount), then (key, value, packLong(nodeRecid))
     * per entry — mapdb3's triple layout, with the expireId simplified to the single
     * queue's node recid (no queue tag bits: there is one queue per segment).
     */
    private final class LeafSerializer implements Serializer<Object[]> {

        @SuppressWarnings("unchecked")
        @Override public void serialize(DataOutput2 out, Object[] leaf) {
            assert leaf.length >= 3 && leaf.length % 3 == 0;
            out.packInt(leaf.length / 3);
            for (int i = 0; i < leaf.length; i += 3) {
                keySer.serialize(out, (K) leaf[i]);
                valueSer.serialize(out, (V) leaf[i + 1]);
                out.packLong((Long) leaf[i + 2]);
            }
        }

        @Override public Object[] deserialize(DataInput2 in, int size) {
            int n = in.unpackInt();
            Object[] leaf = new Object[n * 3];
            for (int i = 0; i < leaf.length; i += 3) {
                leaf[i] = keySer.deserialize(in, -1);
                leaf[i + 1] = valueSer.deserialize(in, -1);
                leaf[i + 2] = in.unpackLong();
            }
            return leaf;
        }
    }

    /** Immutable cache metadata (layout + expiry config + all per-segment recids);
     *  written once at create (§0.6). The on-disk header format is PRE-RELEASE and not yet
     *  stable: fields (e.g. {@code storeSize}) are added inline as the collection evolves, so
     *  a header must be written and read by the same code version (no cross-version migration
     *  — mapdb5 is under the store redesign and makes no on-disk compatibility promise). */
    private static final class Header {
        final int concShift, dirShift, levels, hashSeed;
        final long ttl, maxSize, storeSize, maxEvictPerOp;
        final boolean accessOrder;
        final long[] segmentRoots, counterRecids, queueTails, queueHeads, queueHeadPrevs;

        Header(int concShift, int dirShift, int levels, int hashSeed,
               long ttl, boolean accessOrder, long maxSize, long storeSize, long maxEvictPerOp,
               long[] segmentRoots, long[] counterRecids,
               long[] queueTails, long[] queueHeads, long[] queueHeadPrevs) {
            this.concShift = concShift;
            this.dirShift = dirShift;
            this.levels = levels;
            this.hashSeed = hashSeed;
            this.ttl = ttl;
            this.accessOrder = accessOrder;
            this.maxSize = maxSize;
            this.storeSize = storeSize;
            this.maxEvictPerOp = maxEvictPerOp;
            this.segmentRoots = segmentRoots;
            this.counterRecids = counterRecids;
            this.queueTails = queueTails;
            this.queueHeads = queueHeads;
            this.queueHeadPrevs = queueHeadPrevs;
            assert segmentRoots.length == 1 << concShift;
        }

        static final Serializer<Header> SER = new Serializer<>() {

            @Override public void serialize(DataOutput2 out, Header h) {
                out.packInt(h.concShift);
                out.packInt(h.dirShift);
                out.packInt(h.levels);
                out.writeInt(h.hashSeed);
                out.packLong(h.ttl);
                out.packLong(h.maxSize);
                out.packLong(h.storeSize);
                out.packLong(h.maxEvictPerOp);
                out.writeByte(h.accessOrder ? 1 : 0);
                for (long r : h.segmentRoots) out.packLong(r);
                for (long r : h.counterRecids) out.packLong(r);
                for (long r : h.queueTails) out.packLong(r);
                for (long r : h.queueHeads) out.packLong(r);
                for (long r : h.queueHeadPrevs) out.packLong(r);
            }

            @Override public Header deserialize(DataInput2 in, int size) {
                int concShift = in.unpackInt();
                int dirShift = in.unpackInt();
                int levels = in.unpackInt();
                int hashSeed = in.readInt();
                long ttl = in.unpackLong();
                long maxSize = in.unpackLong();
                long storeSize = in.unpackLong();
                long maxEvictPerOp = in.unpackLong();
                boolean accessOrder = in.readByte() != 0;
                int segments = 1 << concShift;
                long[][] arrays = new long[5][segments];
                for (long[] a : arrays) {
                    for (int i = 0; i < segments; i++) a[i] = in.unpackLong();
                }
                return new Header(concShift, dirShift, levels, hashSeed, ttl, accessOrder,
                        maxSize, storeSize, maxEvictPerOp, arrays[0], arrays[1], arrays[2],
                        arrays[3], arrays[4]);
            }
        };
    }
}
