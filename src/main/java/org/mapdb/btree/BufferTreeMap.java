package org.mapdb.btree;

import org.mapdb.format.BufferedPageFormat;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;
import org.mapdb.ser.GroupFormat;
import org.mapdb.ser.Serializer;
import org.mapdb.ser.Serializers;
import org.mapdb.store.RecordRead;
import org.mapdb.store.StoreDelta;
import org.mapdb.store.StoreTx;

import java.util.AbstractMap;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;

/**
 * Buffer tree (Bε-tree / fractal-tree family, Arge lineage) over a Store4 delta
 * store — a WRITE-OPTIMIZED companion to {@link BTreeMap}, deliberately a separate
 * structure so the buffer is the primary design element and BTreeMap's read path
 * stays untaxed.
 *
 * <h2>Architecture</h2>
 *
 * The tree is a standard B-link base tree (same node shape and split discipline
 * as {@link BTreeMap}) where every node record additionally carries a BUFFER of
 * pending operations in its spare record capacity. The buffer is not a separate
 * allocation: it is the record's headroom, written
 * through the store's delta primitives and read back as part of the same record.
 *
 * Writes are BLIND MESSAGES: put/remove append a keyed op (PUT / TOMBSTONE) into
 * the ROOT node's spare capacity via {@link StoreDelta#append} — no node rewrite,
 * no old-value return. When a node's buffer is full (append REFUSED — the store's
 * capacity-refusal contract as flush trigger), its ops are flushed one level down
 * in per-child batches; leaves consolidate ops into the base image (LWW,
 * tombstones drop out) and split k-way when overfull. Buffer budget is
 * LEVEL-DEPENDENT headroom provisioned by {@link StoreDelta#updateWithHeadroom}
 * on every rewrite: {@code bufferBytes} at directory nodes, {@code leafHeadroom}
 * (possibly 0) at leaves — leaves consolidate rather than buffer, so their
 * headroom buys little and at scale dominates unused store capacity. This turns
 * W random single-entry node rewrites into ~W/(ops-per-buffer) batched rewrites
 * — the write-amplification win the whole design exists for.
 *
 * Record layout (one record per node; the base image is self-delimiting because
 * appends concatenate and read()/get() hand the serializer base+ops as one span):
 * <pre>
 *   int32 baseLen
 *   base: packInt(keysLen&lt;&lt;4|flags), [packLong(link) unless RIGHT],
 *         key group, child recids (dir, packed longs) | value group (leaf)
 *   op*:  byte type (0=TOMBSTONE, 1=PUT), packInt(keyLen), key bytes,
 *         [packInt(valLen), value bytes]           — element-serializer encoded
 * </pre>
 *
 * <h2>Invariants</h2>
 *
 * <ul>
 * <li><b>LWW order:</b> within one record, later ops override earlier (append
 *     order); across levels, a node's ops override its whole subtree (ops enter
 *     at the root and only ever move DOWN, so higher == newer). A point lookup
 *     scans each node's op tail top-down and the FIRST node with a match answers
 *     (last matching op in that tail); only on no match does it search the leaf
 *     base image.</li>
 * <li><b>Coverage:</b> a node's buffered ops always lie within its key coverage —
 *     buffers are fully drained before a node splits or is buried by a root
 *     split, and flush partitions by the current base keys.</li>
 * <li><b>Flush ordering:</b> ops are appended to children BEFORE the parent is
 *     rewritten without them, so a racing reader sees a moving op once or twice
 *     (LWW-idempotent), never zero times. Splits write right siblings first and
 *     keep B-link pointers (referent before referrer).</li>
 * <li><b>Sorted traversal:</b> {@link #forEach} / {@link #entryIterator} emit
 *     strictly ascending key order even with un-flushed buffers at multiple
 *     levels: leaf merges insert ops at their binary-search position, and the
 *     DFS partitions inherited ops to exactly the child whose range covers them.</li>
 * <li><b>Removal:</b> mapdb3/BTreeMap semantics — consolidation never merges
 *     nodes; empty leaves stay linked. Tombstones are dropped at leaf
 *     consolidation (the leaf is ground truth; nothing lives below it).</li>
 * </ul>
 *
 * Concurrency: THREAD-SAFE, but writes DO NOT SCALE — a single map-global writer
 * lock serializes every mutation (put/remove, the atomic CAS methods and poll all
 * run inside it). Readers are lock-free exactly as in BTreeMap — nodes republished
 * per-recid-atomically (the happens-before edge), torn optimistic reads handled by
 * the same re-invocable/bounds-clamped action discipline. Iteration
 * is a read-only DFS merge: ancestor buffer ops are partitioned down and overlaid
 * per leaf (weakly consistent). This is enough for the {@link java.util.concurrent.ConcurrentMap}
 * contract (thread safety + atomic CAS + weakly-consistent views + memory
 * visibility), which mandates none of it be lock-free on the write side — a single
 * writer lock is a valid, if unscalable, strategy. Genuine multi-writer scaling is
 * a deliberately separate concern (future improvement 4; use {@link BTreeMap} when
 * write concurrency is the goal).
 *
 * <p>The reader safety above rests on the backing store's read/publish discipline,
 * so the {@code ConcurrentMap} guarantees hold only when {@code store.isThreadSafe()}
 * — the default. A store opted OUT of thread safety (e.g. {@code StoreByteArray}/
 * {@code StoreDirect} constructed with {@code threadSafe=false}) makes its locks
 * no-ops and does not guarantee lock-free readers a happens-before against a concurrent
 * writer; such a handle is single-threaded only. This exactly mirrors {@link BTreeMap},
 * which likewise skips its own node locks on a non-thread-safe store.
 *
 * <h2>Future improvements (roughly by expected payoff)</h2>
 *
 * <ol>
 * <li><b>Root append hotspot.</b> Every write appends to the one root record:
 *     it serializes all writers AND bumps the root segment's StampedLock stamp,
 *     kicking concurrent optimistic readers of that segment onto the locked
 *     path (measured in BufferTreeStressIT: writer ~2.5K op/s under 63-reader
 *     load vs BTreeMap's ~19K). Mitigations, cheapest first: batch appends in a
 *     small writer-side arena before touching the store; write THROUGH the top
 *     level(s) so appends start at dirs with enough fanout to spread segment
 *     traffic; per-writer sharded root buffers merged at flush.</li>
 * <li><b>Buffer-scan acceleration.</b> DONE for canonical
 *     element serializers: when {@link Serializer#equalsBySerializedBytes()},
 *     the op-tail scan byte-compares the pre-serialized probe key in place —
 *     no per-op deserialization. Still open: a per-op fingerprint byte (side
 *     array, appended in step) to skip the length+bytes compare, and an
 *     incremental min/max key gate to skip whole tails.</li>
 * <li><b>bufferBytes tuning.</b> The buffer must comfortably exceed the node's
 *     base byte size before buffering wins (it is the ε knob: flush fan-out ≈
 *     bufferBytes / childCount per level). Derive the default from
 *     maxNodeSize × element sizeHint instead of a raw constant. The dir/leaf
 *     split is done ({@code leafHeadroom}); per-DEPTH dir budgets (big at top,
 *     shrinking toward the leaves) remain open.</li>
 * <li><b>Multi-writer.</b> The base tree can adopt BTreeMap's Lehman-Yao
 *     protocol (per-node locks, fences, move-right) — but only pays once the
 *     root hotspot (item 1) is addressed, since the root buffer is otherwise
 *     the global serialization point regardless of locking.</li>
 * <li><b>Range reads.</b> forEachWhile(BiPredicate) for early exit is trivial
 *     in the push traversal; a bounded range scan additionally prunes the DFS
 *     to covering subtrees and range-filters inherited ops.</li>
 * <li><b>Map-contract variant.</b> putReturning/removeReturning = point lookup
 *     then append, for callers that need old values; keep the blind path
 *     primary.</li>
 * <li><b>WAL batching.</b> Flush emits one append per child batch already;
 *     store-side I_APPEND batching per (recid, commit) will
 *     shrink WAL traffic further with no change here.</li>
 * <li><b>Background consolidation.</b> flushAll is currently a test hook; an
 *     incremental variant could drain hot buffers opportunistically — but never
 *     as a correctness requirement.</li>
 * </ol>
 *
 * <h2>Map interfaces</h2>
 *
 * Implements {@link ConcurrentNavigableMap} (hence {@code ConcurrentMap} +
 * {@link NavigableMap} + {@link SortedMap}) — the thread-safety guarantees, NOT a
 * write-scaling promise (see Concurrency above; the single writer lock is the
 * serialization point). The blind hot path is exposed unchanged as
 * {@link #putOnly}/{@link #removeOnly} — it stays the reason the structure exists,
 * and callers that don't need old values should prefer it; the Map-contract
 * {@link #put}/{@link #remove} add a point lookup so they can return the old value.
 * The {@code ConcurrentMap} CAS methods ({@code putIfAbsent}, {@code remove(k,v)},
 * {@code replace}) are atomic — each runs its lookup + conditional append in one
 * writer-lock critical section. Views are weakly consistent;
 * {@code get}/{@code containsKey}/{@code remove} accept {@code Object} (absent →
 * null/false; ineligible type may CCE). The full navigable surface
 * (lower/floor/ceiling/higher, first/last/poll, descendingMap, navigableKeySet,
 * inclusive-flag sub-maps) is delegated to the shared {@link ConcurrentOrderedNavigableView}
 * over an {@link Adapter}; sub-views are themselves {@code ConcurrentNavigableMap}.
 * {@code pollFirstEntry}/{@code pollLastEntry} run under the writer lock (find the
 * first/last live entry, then a private tombstone append) — atomic on the returned
 * entry (never removes a value it did not return); least/greatest selection is
 * weakly consistent, as everywhere here.
 */
public class BufferTreeMap<K, V> extends AbstractMap<K, V> implements ConcurrentNavigableMap<K, V> {

    static final int DIR = 8, LEFT = 4, RIGHT = 2;

    /**
     * Default leaf append headroom, chosen by sweep (30M shuffled Long-&gt;Long
     * puts, maxNodeSize 32, bufferBytes 4096; JFR-profiled): vs flat
     * bufferBytes-everywhere it halves the live footprint (2.0 → 0.9 GB) and
     * speeds up every read shape (buffered gets +34%, consolidated gets +26%,
     * forEach +53%, flushAll +20%) for ~11% put throughput — leaves carry
     * shorter op tails and denser records. 128 beat both 512 and 0 on buffered
     * gets and stayed near-best on the rest.
     */
    public static final int DEFAULT_LEAF_HEADROOM = 128;

    /**
     * Whether store-supplied LSNs are stamped into delta frames (R2 §3.3 / R3 prep).
     *
     * <p><b>OFF, deliberately.</b> The {@code DeltaEncoder} seam is fully wired — the store
     * supplies the LSN, this format writes it, and {@code StoreWALDeltaLsnTest} exercises
     * that end to end — but the frame's LSN field is flag-gated and omitted when zero, so
     * stamping a real value adds 8 bytes to EVERY frame, and those frames are then folded
     * into node images by consolidation.
     *
     * <p>📊 Measured cost of turning it on (5 000 random updates, BufferTreeMap over
     * StoreWAL, {@code WalWriteAmplificationTest}): <b>1034 → 1134 device bytes/op
     * (+9.7%)</b> at the default leaf headroom, 173 → 192 (+11%) at headroom 8192. That is
     * a real device-byte cost for a field NOTHING reads until R3 lands. So the mechanism
     * ships proven and disabled.
     *
     * <p><b>Do not flip this without settling the envelope first.</b> Varint-encoding the
     * field is NOT the fix — {@code packLong} is 7 bits per byte, and with one LSN per
     * transaction the 1–3 byte regime lasts only ~2.1M commits, after which 4 bytes is the
     * normal regime (≈ +4.9%). Worse, the value stamped here is a PLACEMENT LSN, not a
     * commit LSN: {@link #deliver} re-encodes already-committed ops with the CURRENT
     * transaction's LSN, so an op committed at 50 can be framed at 500 — and the probe's
     * {@code lsn <= snapshotLsn} test would then hide it from a snapshot taken at 100.
     * Fixing that means either pinning flush-down below active snapshots or propagating the
     * original commit LSN through {@code Op}, and it decides the right granularity — a
     * reverse-readable BATCH-level envelope is the leading candidate, since one append is a
     * whole batch sharing one LSN. Ports are gated on choosing and implementing that
     * snapshot/LSN policy.
     */
    static final boolean STAMP_LSN = false;

    /**
     * Bloom bits per base key for the leaf negative-lookup accelerator ({@link BufferedPageFormat}
     * base fingerprint). A point read whose key is absent from a leaf base returns ABSENT WITHOUT
     * binary-searching the base (after the reverse log scan misses). 8 bits/key ≈ 1 byte/key of
     * base overhead and a ~2% false-positive rate; correctness never depends on it (P7). Fanout is
     * unaffected — splits are key-COUNT based ({@code keysLen > maxNodeSize}), not record-byte based.
     */
    static final int BASE_FP_BITS_PER_KEY = 8;

    private final StoreDelta store;
    private final GroupFormat<K> keyFormat;
    private final GroupFormat<V> valueFormat;
    private final Serializer<K> keyElem;
    private final Serializer<V> valueElem;
    /**
     * R1 buffered-page codec: owns the reverse-readable framed delta-entry
     * encoding of every node's op tail, the true newest-first reverse point probe, and the
     * byte-side range/consolidation primitives. The base image and node shape stay owned here.
     */
    private final BufferedPageFormat<K, V> codec;
    private final int maxNodeSize;
    private final int bufferBytes;
    private final int leafHeadroom;
    private final long rootRecidRecid;
    private final BNodeSerializer nodeSer;
    private final ReentrantLock writeLock = new ReentrantLock();

    /** Cached root recid; same contract as BTreeMap (stale-safe, StoreTx-gated). */
    private volatile long cachedRootRecid;
    private final boolean rootCacheable;

    private BufferTreeMap(StoreDelta store, GroupFormat<K> keyFormat, GroupFormat<V> valueFormat,
                          int maxNodeSize, int bufferBytes, int leafHeadroom, long rootRecidRecid) {
        if (maxNodeSize < 4) throw new IllegalArgumentException("maxNodeSize must be >= 4");
        if (bufferBytes < 32) throw new IllegalArgumentException("bufferBytes must be >= 32");
        if (leafHeadroom < 0) throw new IllegalArgumentException("leafHeadroom must be >= 0");
        this.store = store;
        this.keyFormat = keyFormat;
        this.valueFormat = valueFormat;
        this.keyElem = keyFormat.element();
        this.valueElem = valueFormat.element();
        this.codec = new BufferedPageFormat<>(keyFormat, valueFormat, true, true, BASE_FP_BITS_PER_KEY);
        this.maxNodeSize = maxNodeSize;
        this.bufferBytes = bufferBytes;
        this.leafHeadroom = leafHeadroom;
        this.rootRecidRecid = rootRecidRecid;
        this.nodeSer = new BNodeSerializer();
        this.rootCacheable = !(store instanceof StoreTx);
    }

    /** Creates with {@link #DEFAULT_LEAF_HEADROOM} at leaves; see the 6-arg overload. */
    public static <K, V> BufferTreeMap<K, V> create(StoreDelta store, GroupFormat<K> keyFormat,
                                                    GroupFormat<V> valueFormat,
                                                    int maxNodeSize, int bufferBytes) {
        return create(store, keyFormat, valueFormat, maxNodeSize, bufferBytes, DEFAULT_LEAF_HEADROOM);
    }

    /**
     * Create with a level-dependent buffer budget: directory nodes get
     * {@code bufferBytes} of append headroom, leaves only {@code leafHeadroom}
     * (leaves consolidate ops into their base image rather than hold a buffer,
     * so at scale their headroom is the bulk of unused store capacity — see
     * class javadoc, future improvement 3). {@code leafHeadroom == 0} is valid:
     * where the store enforces record capacity immediately, an append to such a
     * leaf is REFUSED and the op takes the ordinary deliver/consolidate path
     * (transactional staging, e.g. StoreWAL, may defer enforcement to commit —
     * still correct, just later).
     *
     * Like {@code maxNodeSize} and {@code bufferBytes}, {@code leafHeadroom} is
     * open-time policy, NOT persisted: it governs future rewrites only, so pass
     * the same value to {@link #open} to keep the allocation policy on reopen.
     */
    public static <K, V> BufferTreeMap<K, V> create(StoreDelta store, GroupFormat<K> keyFormat,
                                                    GroupFormat<V> valueFormat,
                                                    int maxNodeSize, int bufferBytes, int leafHeadroom) {
        BufferTreeMap<K, V> tmp =
                new BufferTreeMap<>(store, keyFormat, valueFormat, maxNodeSize, bufferBytes, leafHeadroom, 0);
        long rootRecid = tmp.newNode(tmp.new BNode(LEFT | RIGHT, 0L, keyFormat.empty(), valueFormat.empty(), List.of()));
        long rootRecidRecid = store.put(rootRecid, Serializers.LONG);
        return new BufferTreeMap<>(store, keyFormat, valueFormat, maxNodeSize, bufferBytes, leafHeadroom, rootRecidRecid);
    }

    /** Opens with {@link #DEFAULT_LEAF_HEADROOM} at leaves; see the 7-arg overload. */
    public static <K, V> BufferTreeMap<K, V> open(StoreDelta store, long rootRecidRecid,
                                                  GroupFormat<K> keyFormat, GroupFormat<V> valueFormat,
                                                  int maxNodeSize, int bufferBytes) {
        return open(store, rootRecidRecid, keyFormat, valueFormat, maxNodeSize, bufferBytes, DEFAULT_LEAF_HEADROOM);
    }

    public static <K, V> BufferTreeMap<K, V> open(StoreDelta store, long rootRecidRecid,
                                                  GroupFormat<K> keyFormat, GroupFormat<V> valueFormat,
                                                  int maxNodeSize, int bufferBytes, int leafHeadroom) {
        return new BufferTreeMap<>(store, keyFormat, valueFormat, maxNodeSize, bufferBytes, leafHeadroom, rootRecidRecid);
    }

    /** Bulk build with {@link #DEFAULT_LEAF_HEADROOM} and the default pump fill; see the full overload. */
    public static <K, V> BufferTreeMap<K, V> createFromSorted(StoreDelta store, GroupFormat<K> keyFormat,
                                                              GroupFormat<V> valueFormat,
                                                              int maxNodeSize, int bufferBytes,
                                                              Iterator<? extends Map.Entry<K, V>> sortedEntries) {
        return createFromSorted(store, keyFormat, valueFormat, maxNodeSize, bufferBytes,
                DEFAULT_LEAF_HEADROOM, TreePump.defaultFill(maxNodeSize), sortedEntries);
    }

    /**
     * Bulk build (Pump): construct the base tree bottom-up from entries
     * in STRICTLY ascending key order — throws {@link org.mapdb.DBException.NotSorted}
     * on a misordered or duplicate key. Every node is written exactly once via
     * preallocate → updateWithHeadroom, provisioned with its level's buffer budget
     * ({@code bufferBytes} at dirs, {@code leafHeadroom} at leaves) and an EMPTY op
     * tail — the pumped tree is fully consolidated. Interior nodes are filled to
     * {@code nodeFill} entries (≤ maxNodeSize; keep below it so post-load flushes
     * have room before the first wave of splits). Single-threaded; the caller
     * commits.
     */
    public static <K, V> BufferTreeMap<K, V> createFromSorted(StoreDelta store, GroupFormat<K> keyFormat,
                                                              GroupFormat<V> valueFormat,
                                                              int maxNodeSize, int bufferBytes,
                                                              int leafHeadroom, int nodeFill,
                                                              Iterator<? extends Map.Entry<K, V>> sortedEntries) {
        BufferTreeMap<K, V> tmp =
                new BufferTreeMap<>(store, keyFormat, valueFormat, maxNodeSize, bufferBytes, leafHeadroom, 0);
        TreePump.NodeSink<K, V> sink = new TreePump.NodeSink<>() {
            @Override public void writeLeaf(long recid, int flags, long link, Object[] keys, Object[] values) {
                BufferTreeMap<K, V>.BNode node = tmp.new BNode(flags, link,
                        keyFormat.fromArray(keys), valueFormat.fromArray(values), List.of());
                store.updateWithHeadroom(recid, node, tmp.nodeSer, tmp.headroomFor(flags));
            }

            @Override public void writeDir(long recid, int flags, long link, Object[] keys, long[] children) {
                BufferTreeMap<K, V>.BNode node = tmp.new BNode(flags, link,
                        keyFormat.fromArray(keys), children, List.of());
                store.updateWithHeadroom(recid, node, tmp.nodeSer, tmp.headroomFor(flags));
            }
        };
        TreePump<K, V> pump = new TreePump<>(store, sink, keyFormat.element(), maxNodeSize, nodeFill);
        while (sortedEntries.hasNext()) {
            Map.Entry<K, V> e = sortedEntries.next();
            pump.put(e.getKey(), e.getValue());
        }
        long rootRecid = pump.finish();
        long rootRecidRecid = store.put(rootRecid, Serializers.LONG);
        return new BufferTreeMap<>(store, keyFormat, valueFormat, maxNodeSize, bufferBytes, leafHeadroom, rootRecidRecid);
    }

    /** Recid of the root-pointer record; persist this to reopen the map. */
    public long rootRecidRecid() { return rootRecidRecid; }

    private long rootRecid() {
        long r = cachedRootRecid;
        if (r != 0) return r;
        r = store.get(rootRecidRecid, Serializers.LONG);
        if (rootCacheable) cachedRootRecid = r;
        return r;
    }

    // ================= node =================

    /** Buffered op: a keyed PUT or TOMBSTONE message. value == null iff tombstone. */
    private final class Op {
        final K key;
        final V value; // null = tombstone

        Op(K key, V value) { this.key = key; this.value = value; }
    }

    /** Immutable node: BTreeMap-shaped base plus the decoded op tail. */
    private final class BNode {
        final int flags;
        final long link;
        final Object keys;
        final Object values; // leaf: value group; dir: long[] children
        final List<Op> ops;  // append order == oldest first

        BNode(int flags, long link, Object keys, Object values, List<Op> ops) {
            this.flags = flags;
            this.link = link;
            this.keys = keys;
            this.values = values;
            this.ops = ops;
            assert isRight() == (link == 0) : "link/RIGHT mismatch";
        }

        boolean isDir() { return (flags & DIR) != 0; }
        boolean isRight() { return (flags & RIGHT) != 0; }
        long[] children() { return (long[]) values; }
    }

    private final class BNodeSerializer implements Serializer<BNode> {

        @Override public int sizeHint() {
            int ke = keyElem.fixedSize();
            int ve = valueElem.fixedSize();
            int keyBytes = ke > 0 ? ke : 32;
            int valBytes = ve > 0 ? Math.max(ve, 9) : 32;
            // + base-fingerprint header allowance (~1 byte/key at 8 bits/key + a few header bytes).
            int fpBytes = codec.baseFingerprintEnabled() ? maxNodeSize + 4 : 0;
            return 24 + (maxNodeSize + 1) * (keyBytes + valBytes) + bufferBytes + fpBytes;
        }

        @Override public void serialize(DataOutput2 out, BNode n) {
            int lenSlot = out.pos;
            out.writeInt(0); // baseLen backpatched below
            int baseStart = out.pos;
            // R1 base fingerprint: self-describing header FIRST in the base region (leaves only —
            // a dir point read routes to a child, so "absent" is meaningless). Rebuilt here on every
            // whole-image base rewrite, so it always matches the base keys it precedes.
            codec.writeBaseFp(out, n.isDir() ? null : n.keys);
            int keysLen = keyFormat.size(n.keys);
            out.packInt((keysLen << 4) | n.flags);
            if (!n.isRight()) out.packLong(n.link);
            keyFormat.serialize(out, n.keys);
            if (n.isDir()) {
                for (long child : n.children()) out.packLong(child);
            } else {
                valueFormat.serialize(out, n.values);
            }
            int baseLen = out.pos - baseStart;
            out.buf[lenSlot] = (byte) (baseLen >>> 24);
            out.buf[lenSlot + 1] = (byte) (baseLen >>> 16);
            out.buf[lenSlot + 2] = (byte) (baseLen >>> 8);
            out.buf[lenSlot + 3] = (byte) baseLen;
            for (Op op : n.ops) writeOp(out, op.key, op.value);
        }

        @Override public BNode deserialize(DataInput2 in, int size) {
            int start = in.pos();
            if (size < 4) throw new IllegalStateException("record too short for node header");
            int baseLen = in.readInt();
            if (baseLen < 0 || baseLen > size - 4) throw new IllegalStateException("torn baseLen");
            BufferedPageFormat.BaseHeader hdr = new BufferedPageFormat.BaseHeader();
            codec.parseBaseHeader(in, start + 4, baseLen, hdr);
            in.pos(hdr.nodeBase); // skip the base fingerprint header to the node base
            int h = in.unpackInt();
            int flags = h & 0xF;
            int keysLen = h >>> 4;
            long link = (flags & RIGHT) != 0 ? 0L : in.unpackLong();
            Object keys = keyFormat.deserialize(in, keysLen);
            Object values;
            if ((flags & DIR) != 0) {
                int childCount = keysLen + (((flags & RIGHT) != 0) ? 1 : 0);
                long[] children = new long[childCount];
                for (int i = 0; i < childCount; i++) children[i] = in.unpackLong();
                values = children;
            } else {
                values = valueFormat.deserialize(in, keysLen);
            }
            // op tail: [start+4+baseLen, start+size) — decoded oldest→newest via the R1 codec.
            // The codec frames each entry (totalLen/opType/flags/lsn/keyHash/key/value/crc/trailer);
            // OP_DELETE maps back to the in-memory null-value tombstone Op.
            int end = start + size;
            List<Op> ops = new ArrayList<>();
            codec.forEachEntryOldestFirst(in, start + 4 + baseLen, end,
                    (opType, k, v, lsn) -> ops.add(new Op(k, opType == BufferedPageFormat.OP_PUT ? v : null)));
            return new BNode(flags, link, keys, values, ops);
        }
    }

    /** R1 framed delta entry (reverse-readable, LSN=0, keyHash+CRC) via the shared codec. */
    private void writeOp(DataOutput2 out, K key, V value) {
        codec.encodeInto(out, value == null ? BufferedPageFormat.OP_DELETE : BufferedPageFormat.OP_PUT,
                key, value, BufferedPageFormat.LSN_UNASSIGNED);
    }

    private byte[] encodeOps(List<Op> ops) {
        DataOutput2 out = new DataOutput2(64);
        for (Op op : ops) writeOp(out, op.key, op.value);
        return java.util.Arrays.copyOf(out.buf, out.pos);
    }

    /**
     * The same op batch as a {@link StoreDelta.DeltaEncoder}, so the STORE supplies the LSN
     * and this format writes it into each frame (R2 §3.3; the seam R1 declared as
     * {@code BufferedPageFormat.AppendContext}). A transactional store stamps every frame of
     * a transaction with that transaction's LSN; a plain store passes 0, which encodes
     * exactly as before (the LSN field is flag-gated and omitted when zero).
     *
     * <p>Pure and deterministic, as the contract requires: it may be invoked more than once
     * (the root-retry path below) and must produce identical bytes for a given LSN.
     */
    private StoreDelta.DeltaEncoder opsEncoder(List<Op> ops) {
        return (out, lsn) -> {
            long stamp = STAMP_LSN ? lsn : BufferedPageFormat.LSN_UNASSIGNED;
            for (Op op : ops) {
                codec.encodeInto(out,
                        op.value == null ? BufferedPageFormat.OP_DELETE : BufferedPageFormat.OP_PUT,
                        op.key, op.value, stamp);
            }
        };
    }

    private BNode load(long recid) {
        return store.get(recid, nodeSer);
    }

    /** Buffer budget by level: dirs get the full bufferBytes, leaves only leafHeadroom. */
    private int headroomFor(int flags) {
        return (flags & DIR) != 0 ? bufferBytes : leafHeadroom;
    }

    /** preallocate + first-write-with-headroom: node gets its level's appendable space. */
    private long newNode(BNode n) {
        long recid = store.preallocate();
        store.updateWithHeadroom(recid, n, nodeSer, headroomFor(n.flags));
        return recid;
    }

    private void rewrite(long recid, BNode n) {
        store.updateWithHeadroom(recid, n, nodeSer, headroomFor(n.flags));
    }

    // ================= write path (single writer, blind messages) =================

    /**
     * Blind upsert — the PRIMARY, unchanged hot path: appends a PUT message into the
     * root buffer without reading the old value (one append, no node materialization —
     * the whole point of the structure).
     */
    public void putOnly(K key, V value) {
        if (key == null || value == null) throw new NullPointerException();
        writeLock.lock();
        try {
            appendMessage(key, value);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Blind delete — unchanged hot path: appends a TOMBSTONE message. Truly blind (no
     * read), so it stays void; making it boolean would force a lookup and defeat the
     * optimization.
     */
    public void removeOnly(K key) {
        if (key == null) throw new NullPointerException();
        writeLock.lock();
        try {
            appendMessage(key, null);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Map-contract put: returns the previous value. Slower than {@link #putOnly} (it
     * adds a point lookup), but the lookup and the append run in the SAME writeLock
     * critical section, so the returned old value is writer-linearized with the write.
     */
    @Override public V put(K key, V value) {
        if (key == null || value == null) throw new NullPointerException();
        writeLock.lock();
        try {
            V old = doGet(key);
            appendMessage(key, value);
            return old;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Map-contract remove: returns the previous value. Lookup + conditional append in
     * one critical section. On an ABSENT key it appends NOTHING — the observable result
     * is null anyway, so a tombstone (and its read cost) would be wasted.
     */
    @SuppressWarnings("unchecked")
    @Override public V remove(Object key) {
        if (key == null) throw new NullPointerException();
        K k = (K) key;
        writeLock.lock();
        try {
            V old = doGet(k);
            if (old != null) appendMessage(k, null);
            return old;
        } finally {
            writeLock.unlock();
        }
    }

    // -- ConcurrentMap CAS methods: lookup + conditional append in ONE writeLock
    // critical section, so each is atomic against every other writer (all writers
    // hold the same lock). Readers observe only the committed append (weakly
    // consistent), never the intermediate lookup. --

    @Override public V putIfAbsent(K key, V value) {
        if (key == null || value == null) throw new NullPointerException();
        writeLock.lock();
        try {
            V old = doGet(key);
            if (old != null) return old;
            appendMessage(key, value);
            return null;
        } finally {
            writeLock.unlock();
        }
    }

    @SuppressWarnings("unchecked")
    @Override public boolean remove(Object key, Object value) {
        if (key == null || value == null) throw new NullPointerException();
        K k = (K) key;
        writeLock.lock();
        try {
            V old = doGet(k);
            if (old != null && valueFormat.element().equals(old, (V) value)) {
                appendMessage(k, null);
                return true;
            }
            return false;
        } finally {
            writeLock.unlock();
        }
    }

    @Override public V replace(K key, V value) {
        if (key == null || value == null) throw new NullPointerException();
        writeLock.lock();
        try {
            V old = doGet(key);
            if (old != null) appendMessage(key, value);
            return old;
        } finally {
            writeLock.unlock();
        }
    }

    @Override public boolean replace(K key, V oldValue, V newValue) {
        if (key == null || oldValue == null || newValue == null) throw new NullPointerException();
        writeLock.lock();
        try {
            V cur = doGet(key);
            if (cur != null && valueFormat.element().equals(cur, oldValue)) {
                appendMessage(key, newValue);
                return true;
            }
            return false;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Append one op into the root buffer; on capacity refusal flush one level down
     * (splits grow the tree) and retry. Assumes the writeLock is HELD — byte-for-byte
     * the old blind {@code writeMessage} body, so the blind hot path is unchanged.
     */
    private void appendMessage(K key, V value) {
        List<Op> single = List.of(new Op(key, value));
        StoreDelta.DeltaEncoder enc = opsEncoder(single);
        long root = rootRecid();
        if (store.append(root, enc) != StoreDelta.REFUSED) return;
        // root buffer full: flush it one level down (splits grow the tree)
        List<SplitPart> parts = deliver(root, List.of());
        if (parts != null) growRoot(root, parts);
        root = rootRecid();
        if (store.append(root, enc) != StoreDelta.REFUSED) return;
        // op larger than an empty buffer: push it down directly
        parts = deliver(root, single);
        if (parts != null) growRoot(root, parts);
    }

    /** New right sibling produced by a split: sep = left neighbour's inclusive high bound. */
    private final class SplitPart {
        final K sep;
        final long recid;

        SplitPart(K sep, long recid) { this.sep = sep; this.recid = recid; }
    }

    /**
     * Deliver {@code incoming} ops (newer than anything buffered in the node) to
     * the node at {@code recid}, flushing its own buffer along the way. Leaf:
     * consolidate everything into the base image. Dir: partition ops per child,
     * append each batch (recursing when a child's buffer is full), then rewrite
     * this dir with an empty buffer and any new separators. Child writes happen
     * BEFORE the parent rewrite (reader safety: an op is never invisible).
     * Returns new right siblings of this node, or null when it did not split.
     */
    /** Node's own (older) ops followed by inherited/incoming (newer) ones. */
    private List<Op> combine(List<Op> own, List<Op> newer) {
        if (own.isEmpty()) return newer;
        List<Op> all = new ArrayList<>(own);
        all.addAll(newer);
        return all;
    }

    /** Partition ops per child by base-key routing. Entries may be null (no ops). */
    @SuppressWarnings("unchecked")
    private List<Op>[] partition(BNode dir, List<Op> ops) {
        long[] children = dir.children();
        List<Op>[] batches = new List[children.length];
        for (Op op : ops) {
            int pos = keyFormat.search(dir.keys, op.key);
            int ci = pos >= 0 ? pos : -pos - 1;
            if (ci >= children.length) ci = children.length - 1;
            if (batches[ci] == null) batches[ci] = new ArrayList<>();
            batches[ci].add(op);
        }
        return batches;
    }

    private List<SplitPart> deliver(long recid, List<Op> incoming) {
        BNode n = load(recid);
        List<Op> all = combine(n.ops, incoming);
        if (!n.isDir()) {
            Object[] kv = applyOps(n.keys, n.values, all);
            return writeSplitParts(recid, n.flags, n.link, kv[0], kv[1]);
        }
        long[] children = n.children();
        // writer lock held: routing is never stale
        List<Op>[] batches = partition(n, all);
        Object newKeys = n.keys;
        long[] newChildren = children;
        boolean baseChanged = false;
        for (int ci = 0; ci < children.length; ci++) {
            if (batches[ci] == null) continue;
            if (store.append(children[ci], opsEncoder(batches[ci])) != StoreDelta.REFUSED) continue;
            List<SplitPart> childParts = deliver(children[ci], batches[ci]);
            if (childParts == null) continue;
            for (SplitPart p : childParts) {
                int pos = keyFormat.search(newKeys, p.sep);
                assert pos < 0 : "duplicate separator " + p.sep;
                int ip = -pos - 1;
                newKeys = keyFormat.insert(newKeys, ip, p.sep);
                newChildren = insertLong(newChildren, ip + 1, p.recid);
                baseChanged = true;
            }
        }
        if (!baseChanged && all.isEmpty()) return null; // nothing to write
        return writeSplitParts(recid, n.flags, n.link, newKeys, newChildren);
    }

    /**
     * Rewrite the node with an EMPTY buffer, splitting k-way when the key count
     * exceeds maxNodeSize (a flushed batch can overfill a node by more than 2x).
     * Right parts are written first (referent before referrer), the leftmost part
     * is rewritten in place at {@code recid} last — B-link discipline, so
     * lock-free readers stay safe throughout. Returns the new right siblings
     * (ascending) or null.
     */
    private List<SplitPart> writeSplitParts(long recid, int flags, long link, Object keys, Object values) {
        int total = keyFormat.size(keys);
        if (total <= maxNodeSize) {
            rewrite(recid, new BNode(flags, link, keys, values, List.of()));
            return null;
        }
        boolean dir = (flags & DIR) != 0;
        boolean right = (flags & RIGHT) != 0;
        int p = Math.max(2, (total + maxNodeSize - 1) / maxNodeSize);
        if ((total + p - 1) / p >= maxNodeSize) p++;
        // part i covers keys [cut[i], cut[i+1])
        int[] cut = new int[p + 1];
        int base = total / p, rem = total % p;
        for (int i = 0; i < p; i++) cut[i + 1] = cut[i] + base + (i < rem ? 1 : 0);
        long[] children = dir ? (long[]) values : null;
        long[] partRecid = new long[p];
        long nextLink = link;
        for (int i = p - 1; i >= 1; i--) { // rights first
            int a = cut[i], b = cut[i + 1];
            int f = (dir ? DIR : 0) | (i == p - 1 && right ? RIGHT : 0);
            Object pk = keyFormat.copyRange(keys, a, b);
            Object pv;
            if (dir) {
                int cb = (i == p - 1 && right) ? b + 1 : b;
                pv = java.util.Arrays.copyOfRange(children, a, cb);
            } else {
                pv = valueFormat.copyRange(values, a, b);
            }
            partRecid[i] = newNode(new BNode(f, (f & RIGHT) != 0 ? 0L : nextLink, pk, pv, List.of()));
            nextLink = partRecid[i];
        }
        int f0 = (dir ? DIR : 0) | (flags & LEFT);
        Object k0 = keyFormat.copyRange(keys, 0, cut[1]);
        Object v0 = dir ? java.util.Arrays.copyOfRange(children, 0, cut[1])
                        : valueFormat.copyRange(values, 0, cut[1]);
        rewrite(recid, new BNode(f0, nextLink, k0, v0, List.of()));
        partRecid[0] = recid;
        List<SplitPart> parts = new ArrayList<>(p - 1);
        for (int i = 0; i < p - 1; i++) {
            @SuppressWarnings("unchecked")
            K sep = keyFormat.get(keys, cut[i + 1] - 1); // last key of part i (inclusive bound, stays left)
            parts.add(new SplitPart(sep, partRecid[i + 1]));
        }
        return parts;
    }

    /** Grow the tree upward: new root(s) over [oldRoot, parts...]. */
    private void growRoot(long oldRoot, List<SplitPart> parts) {
        // level = [child recids], seps between them
        ArrayList<Long> childs = new ArrayList<>();
        ArrayList<K> seps = new ArrayList<>();
        childs.add(oldRoot);
        for (SplitPart sp : parts) {
            seps.add(sp.sep);
            childs.add(sp.recid);
        }
        while (seps.size() > maxNodeSize) {
            // build one dir level, rightmost chunk carries the extra child (RIGHT dir shape)
            ArrayList<Long> upChilds = new ArrayList<>();
            ArrayList<K> upSeps = new ArrayList<>();
            int m = childs.size();
            int chunk = Math.max(2, (maxNodeSize + 1) / 2);
            long linkRight = 0;
            // build right-to-left so links are known
            ArrayList<int[]> ranges = new ArrayList<>(); // [from, to) over childs
            for (int a = 0; a < m; a += chunk) ranges.add(new int[]{a, Math.min(m, a + chunk)});
            long[] recids = new long[ranges.size()];
            for (int r = ranges.size() - 1; r >= 0; r--) {
                int a = ranges.get(r)[0], b = ranges.get(r)[1];
                boolean isRight = r == ranges.size() - 1;
                // dir keys: seps[a .. b-1) plus (non-right) the bound sep[b-1]
                Object dk = keyFormat.empty();
                int keyTo = isRight ? b - 1 : b; // seps index range [a, keyTo)
                for (int s = a; s < keyTo; s++) dk = keyFormat.insert(dk, keyFormat.size(dk), seps.get(s));
                long[] dc = new long[b - a];
                for (int c = a; c < b; c++) dc[c - a] = childs.get(c);
                int fl = DIR | (r == 0 ? LEFT : 0) | (isRight ? RIGHT : 0);
                recids[r] = newNode(new BNode(fl, isRight ? 0L : linkRight, dk, dc, List.of()));
                linkRight = recids[r];
            }
            ArrayList<Long> nc = new ArrayList<>();
            ArrayList<K> ns = new ArrayList<>();
            for (int r = 0; r < ranges.size(); r++) {
                nc.add(recids[r]);
                if (r < ranges.size() - 1) ns.add(seps.get(ranges.get(r)[1] - 1));
            }
            childs = nc;
            seps = ns;
        }
        Object rk = keyFormat.empty();
        for (int i = 0; i < seps.size(); i++) rk = keyFormat.insert(rk, i, seps.get(i));
        long[] rc = new long[childs.size()];
        for (int i = 0; i < rc.length; i++) rc[i] = childs.get(i);
        long newRoot = newNode(new BNode(DIR | LEFT | RIGHT, 0L, rk, rc, List.of()));
        store.update(rootRecidRecid, newRoot, Serializers.LONG);
        if (rootCacheable) cachedRootRecid = newRoot;
    }

    /**
     * LWW-apply ops (oldest first) onto a leaf base image; tombstones drop out.
     *
     * Batch merge: instead of copy-on-write per op (O(B*N) array copies), collapse
     * the batch to its net effect per key (last writer wins) and two-pointer merge
     * it with the sorted base image, building the result groups exactly once
     * (O(N+B)). Uses only method-local state — also runs on lock-free reader
     * threads (forEachNode, entryIterator.descend).
     */
    private Object[] applyOps(Object keys, Object values, List<Op> ops) {
        if (ops.isEmpty()) return new Object[]{keys, values};

        // net effect per key: ops are oldest-first, so later put() overwrites earlier.
        // Compare via keyFormat (not keyElem) so the merge order can never diverge from
        // the tree/search order: there is exactly one coherent order.
        java.util.TreeMap<K, Op> net = new java.util.TreeMap<>(keyFormat::compare);
        for (Op op : ops) net.put(op.key, op);

        int n = keyFormat.size(keys);
        ArrayList<Object> outKeys = new ArrayList<>(n + net.size());
        ArrayList<Object> outVals = new ArrayList<>(n + net.size());

        Iterator<Op> opIt = net.values().iterator();
        Op op = opIt.hasNext() ? opIt.next() : null;
        int i = 0;
        while (i < n) {
            K baseKey = keyFormat.get(keys, i);
            int cmp = (op == null) ? -1 : keyFormat.compare(baseKey, op.key);
            if (cmp < 0) { // base entry untouched by the batch
                outKeys.add(baseKey);
                outVals.add(valueFormat.get(values, i));
                i++;
            } else if (cmp == 0) { // op wins over base; tombstone drops the entry
                if (op.value != null) {
                    outKeys.add(op.key);
                    outVals.add(op.value);
                }
                i++;
                op = opIt.hasNext() ? opIt.next() : null;
            } else { // fresh key from the batch; tombstone of absent key is a no-op
                if (op.value != null) {
                    outKeys.add(op.key);
                    outVals.add(op.value);
                }
                op = opIt.hasNext() ? opIt.next() : null;
            }
        }
        while (op != null) { // base exhausted; remaining fresh keys
            if (op.value != null) {
                outKeys.add(op.key);
                outVals.add(op.value);
            }
            op = opIt.hasNext() ? opIt.next() : null;
        }

        Object newKeys = keyFormat.fromArray(outKeys.toArray());
        Object newValues = valueFormat.fromArray(outVals.toArray());
        return new Object[]{newKeys, newValues};
    }

    private static long[] insertLong(long[] arr, int pos, long value) {
        long[] r = new long[arr.length + 1];
        System.arraycopy(arr, 0, r, 0, pos);
        r[pos] = value;
        System.arraycopy(arr, pos, r, pos + 1, arr.length - pos);
        return r;
    }

    /** TEST HOOK: flush every buffer down to the leaves (writer op). */
    public void flushAll() {
        writeLock.lock();
        try {
            for (;;) {
                boolean[] any = {false};
                long root = rootRecid();
                List<SplitPart> parts = forceFlush(root, any);
                if (parts != null) {
                    growRoot(root, parts);
                    continue; // split interrupted the pass; run another
                }
                if (!any[0]) return; // full pass with nothing left to flush
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * One flush pass: push this node's ops down, then recurse into children,
     * folding their splits into this base. Returns this node's own splits (the
     * caller re-passes — subtrees of fresh splits may still hold ops).
     */
    private List<SplitPart> forceFlush(long recid, boolean[] any) {
        BNode n = load(recid);
        if (!n.isDir()) {
            if (n.ops.isEmpty()) return null;
            any[0] = true;
            return deliver(recid, List.of());
        }
        if (!n.ops.isEmpty()) {
            any[0] = true;
            List<SplitPart> own = deliver(recid, List.of());
            if (own != null) return own;
            n = load(recid);
        }
        Object newKeys = n.keys;
        long[] newChildren = n.children();
        boolean changed = false;
        for (long child : n.children()) {
            List<SplitPart> parts = forceFlush(child, any);
            if (parts == null) continue;
            for (SplitPart p : parts) {
                int pos = keyFormat.search(newKeys, p.sep);
                assert pos < 0 : "duplicate separator " + p.sep;
                int ip = -pos - 1;
                newKeys = keyFormat.insert(newKeys, ip, p.sep);
                newChildren = insertLong(newChildren, ip + 1, p.recid);
                changed = true;
            }
        }
        if (changed) return writeSplitParts(recid, n.flags, n.link, newKeys, newChildren);
        return null;
    }

    // ================= get: push-down read action =================

    /**
     * Traversal step over [baseLen][base][ops]: scan the op tail first (a hit here
     * is newer than anything below — terminal), else route/search the base image.
     * Optimistic-read discipline as in BTreeMap.GetAction: re-invocable, every
     * decoded length bounds-clamped so torn bytes fail fast.
     */
    private final class BufferGetAction implements RecordRead {
        final K key;
        /**
         * Probe key serialized once — lazily, on the first non-empty op tail, so
         * gets over consolidated trees pay nothing — when the element serializer
         * is canonical ({@link Serializer#equalsBySerializedBytes()}): the op-tail
         * scan then byte-compares each op key in place instead of deserializing it
         * — the dominant cost of buffered gets (class javadoc, future improvement 2).
         * Stays null when the capability is off (decode-and-compare fallback).
         */
        final boolean binaryEquality;
        byte[] probeKeyBytes;
        final BufferedPageFormat.PointResult<V> pr = new BufferedPageFormat.PointResult<>();
        final BufferedPageFormat.BaseHeader hdr = new BufferedPageFormat.BaseHeader();
        V value;
        boolean found;

        BufferGetAction(K key) {
            this.key = key;
            this.binaryEquality = keyElem.equalsBySerializedBytes();
        }

        private byte[] probeKeyBytes() {
            byte[] p = probeKeyBytes;
            if (p == null) {
                DataOutput2 scratch = new DataOutput2(32);
                keyElem.serialize(scratch, key);
                probeKeyBytes = p = scratch.copyBytes();
            }
            return p;
        }

        @Override public long onBytes(DataInput2 in, int size) {
            found = false;
            value = null;
            int start = in.pos();
            if (size < 4) throw new IllegalStateException("record too short for node header");
            int baseLen = in.readInt();
            if (baseLen < 0 || baseLen > size - 4) throw new IllegalStateException("torn baseLen");
            int end = start + size;

            // ---- op tail: TRUE newest-first reverse scan, STOP at first visible match (R1) ----
            // The codec frames entries reverse-readably (trailing trailerLen), so "newest wins"
            // is literal: the last-appended matching op is the first hit walking backwards. This
            // node's tail is scanned top-down across levels (root first = newest), so the first
            // node with any match is terminal — identical LWW semantics to the pre-R1 forward
            // last-match scan, now O(entries until the match) instead of O(whole tail).
            byte[] probe = binaryEquality ? probeKeyBytes() : null;
            if (codec.probeNewestFirst(in, start + 4 + baseLen, end, key, probe, Long.MAX_VALUE, pr)) {
                if (pr.put) {
                    value = pr.value;
                    found = true;
                }
                return 0; // PUT → value; DELETE → definitively absent
            }

            // ---- base image ----
            // Parse+validate the self-describing base header (fail-closed) and read the node header.
            codec.parseBaseHeader(in, start + 4, baseLen, hdr);
            in.pos(hdr.nodeBase);
            int h = in.unpackInt();
            int flags = h & 0xF;
            int keysLen = h >>> 4;
            if (keysLen > baseLen) throw new IllegalStateException("torn keysLen");
            long link = (flags & RIGHT) != 0 ? 0L : in.unpackLong();
            int keyGroupPos = in.pos();

            // Fingerprint skip: ONLY after the node is PROVEN a LEAF (dir nodes route to a child and
            // carry no filter, fpLen=0). On a definite base-miss return ABSENT without the base
            // binary search. No false negatives: a real base member always tests "maybe"; a deleted
            // base key was already caught by the log DELETE above (log-first). P7-neutral: fpMode
            // FORCE_SEARCH/RANDOM only ever forces MORE searching. baseFpMightContain moves the
            // cursor, so re-seek to the key group before searching.
            if ((flags & DIR) == 0 && probe != null && !codec.baseFpMightContain(in, hdr, probe)) {
                return 0; // definitely absent from this leaf base
            }
            in.pos(keyGroupPos);
            int pos;
            if (keyFormat.supportsBinary()) {
                pos = keyFormat.binarySearch(key, in, keysLen);
            } else {
                pos = keyFormat.search(keyFormat.deserialize(in, keysLen), key);
            }
            if ((flags & DIR) != 0) {
                int childIdx = pos >= 0 ? pos : -pos - 1;
                int childCount = keysLen + (((flags & RIGHT) != 0) ? 1 : 0);
                if (childIdx >= childCount) return link;
                in.unpackLongSkip(childIdx);
                return in.unpackLong();
            }
            if (pos >= 0) {
                found = true;
                value = valueFormat.supportsBinary()
                        ? valueFormat.binaryGet(in, keysLen, pos)
                        : valueFormat.get(valueFormat.deserialize(in, keysLen), pos);
                return 0;
            }
            int ip = -pos - 1;
            if (ip >= keysLen && link != 0) return link;
            return 0;
        }
    }

    @SuppressWarnings("unchecked")
    @Override public V get(Object key) {
        if (key == null) throw new NullPointerException();
        return doGet((K) key);
    }

    /**
     * Lock-free push-down lookup. Also the private point-read the value-returning/CAS
     * mutators call INSIDE the writeLock (it takes no writer lock, so it never
     * re-enters the public API and shares the caller's critical section for
     * linearization).
     */
    private V doGet(K key) {
        BufferGetAction action = new BufferGetAction(key);
        long current = rootRecid();
        while (current != 0) {
            current = store.read(current, action);
        }
        return action.found ? action.value : null;
    }

    @SuppressWarnings("unchecked")
    @Override public boolean containsKey(Object key) {
        if (key == null) throw new NullPointerException();
        BufferGetAction action = new BufferGetAction((K) key);
        long current = rootRecid();
        while (current != 0) {
            current = store.read(current, action);
        }
        return action.found;
    }

    // ================= iteration: read-only DFS merge =================

    /**
     * Push-based traversal: the DFS discovers each live entry (base + buffered
     * ops merged LWW, tombstones filtered) and hands it to {@code consumer} in
     * STRICTLY ASCENDING key order — the sort holds even with un-flushed buffers
     * at multiple levels (see class javadoc, "Sorted traversal"). This is the
     * SIMPLE traversal — the recursion mirrors the flush partition logic
     * one-to-one; prefer it over {@link #entryIterator()} unless a pull cursor
     * is genuinely needed. Weakly consistent (nodes snapshot as visited).
     */
    @Override public void forEach(BiConsumer<? super K, ? super V> consumer) {
        if (consumer == null) throw new NullPointerException(); // Map.forEach contract: eager, even when empty
        forEachNode(rootRecid(), List.of(), consumer);
    }

    private void forEachNode(long recid, List<Op> inherited, BiConsumer<? super K, ? super V> consumer) {
        BNode n = load(recid);
        List<Op> all = combine(n.ops, inherited);
        if (!n.isDir()) {
            Object[] kv = applyOps(n.keys, n.values, all);
            int len = keyFormat.size(kv[0]);
            for (int i = 0; i < len; i++) {
                consumer.accept(keyFormat.get(kv[0], i), valueFormat.get(kv[1], i));
            }
            return;
        }
        long[] children = n.children();
        List<Op>[] batches = partition(n, all);
        for (int ci = 0; ci < children.length; ci++) {
            forEachNode(children[ci], batches[ci] == null ? List.of() : batches[ci], consumer);
        }
    }

    /**
     * Pull cursor over the same merge as {@link #forEach} — strictly ascending
     * key order, un-flushed buffers included. DFS by child pointers via an
     * explicit frame stack (the price of inverting the recursion into a pull
     * API), carrying ancestor buffer ops partitioned per child; each leaf is
     * materialized as base+ops merged (LWW, tombstones filtered). Weakly
     * consistent: nodes are snapshot as visited; a concurrent writer's flushes
     * may be seen or not per node.
     */
    public Iterator<Map.Entry<K, V>> entryIterator() {
        final ArrayDeque<Frame> stack = new ArrayDeque<>();

        return new Iterator<>() {
            Object leafKeys, leafVals;
            int leafLen, pos;
            boolean started = false;

            private void descend(long recid, List<Op> inherited) {
                for (;;) {
                    BNode n = load(recid);
                    List<Op> all = combine(n.ops, inherited);
                    if (!n.isDir()) {
                        Object[] kv = applyOps(n.keys, n.values, all);
                        leafKeys = kv[0];
                        leafVals = kv[1];
                        leafLen = keyFormat.size(leafKeys);
                        pos = 0;
                        return;
                    }
                    long[] children = n.children();
                    List<Op>[] batches = partition(n, all);
                    stack.push(new Frame(children, 1, batches));
                    recid = children[0];
                    inherited = batches[0] == null ? List.of() : batches[0];
                }
            }

            private void advanceToData() {
                if (!started) {
                    started = true;
                    descend(rootRecid(), List.of());
                }
                while (pos >= leafLen) {
                    Frame f = stack.peek();
                    if (f == null) { leafLen = -1; return; } // exhausted (pos stays past)
                    if (f.idx >= f.children.length) {
                        stack.pop();
                        continue;
                    }
                    int ci = f.idx++;
                    descend(f.children[ci], f.batches[ci] == null ? List.of() : f.batches[ci]);
                }
            }

            @Override public boolean hasNext() {
                advanceToData();
                return leafLen >= 0 && pos < leafLen;
            }

            @Override public Map.Entry<K, V> next() {
                advanceToData();
                if (leafLen < 0 || pos >= leafLen) throw new NoSuchElementException();
                K k = keyFormat.get(leafKeys, pos);
                V v = valueFormat.get(leafVals, pos);
                pos++;
                return new AbstractMap.SimpleImmutableEntry<>(k, v);
            }
        };
    }

    private final class Frame {
        final long[] children;
        int idx;
        final List<Op>[] batches;

        Frame(long[] children, int idx, List<Op>[] batches) {
            this.children = children;
            this.idx = idx;
            this.batches = batches;
        }
    }

    public long sizeLong() {
        long[] count = {0};
        forEach((k, v) -> count[0]++);
        return count[0];
    }

    /**
     * Bounded ASCENDING range iterator over {@code [lo,hi]} (null bound = open) — R1 PRUNED
     * cross-level cursor (replacing the pre-R1 O(full-map) full-DFS-then-filter).
     *
     * <p>Two paths:
     * <ul>
     * <li><b>Single-leaf root (byte-side, NO base materialization):</b> when the whole map is one
     *     leaf and the key/value formats support binary access, the range is answered entirely on
     *     page bytes via {@link BufferedPageFormat}: a byte-side base cursor over only the in-range
     *     base slice ({@code binarySearch}+{@code binaryGet}, never the whole group) merged with the
     *     bounded log shadow — a warm range scan with no base materialization.</li>
     * <li><b>Multi-level (pruned DFS):</b> descend ONLY children whose routing band covers
     *     {@code [lo,hi]} (derived by the SAME inclusive-separator routing as point get, so equality
     *     on a separator is handled identically), range-filter inherited ops BEFORE partition, and
     *     release non-covering child batches. Each covering leaf is materialized once (bounded by
     *     {@code maxNodeSize}).</li>
     * </ul>
     *
     * <p>Delivered per-query bound: {@code O(covering dirs+leaves visited)} descent, and per covering
     * leaf {@code O(base keys in range + visible log entries in range)} (single-leaf: strictly no base
     * materialization; multi-level: one {@code <= maxNodeSize} leaf materialization per covering leaf).
     * NOT {@code O(full-map)}.
     */
    Iterator<Map.Entry<K, V>> entryIterator(K lo, boolean loInc, K hi, boolean hiInc) {
        List<Map.Entry<K, V>> single = tryByteRangeSingleLeaf(lo, loInc, hi, hiInc, false);
        if (single != null) return single.iterator();
        return prunedRangeIterator(lo, loInc, hi, hiInc, false);
    }

    // ---- R1 pruned range machinery (shared by ascending/descending) ----

    private boolean inRange(K key, K lo, boolean loInc, K hi, boolean hiInc) {
        if (lo != null) {
            int c = keyFormat.compare(key, lo);
            if (c < 0 || (c == 0 && !loInc)) return false;
        }
        if (hi != null) {
            int c = keyFormat.compare(key, hi);
            if (c > 0 || (c == 0 && !hiInc)) return false;
        }
        return true;
    }

    private List<Op> filterRange(List<Op> ops, K lo, boolean loInc, K hi, boolean hiInc) {
        if ((lo == null && hi == null) || ops.isEmpty()) return ops;
        List<Op> out = new ArrayList<>();
        for (Op op : ops) if (inRange(op.key, lo, loInc, hi, hiInc)) out.add(op);
        return out;
    }

    /** Routing index into a dir's children (same inclusive-separator rule as point get). */
    private int route(Object keys, K key) {
        int pos = keyFormat.search(keys, key);
        return pos >= 0 ? pos : -pos - 1;
    }

    /** Lowest covering child index for a lower bound (null = first child). */
    private int childBandLo(Object keys, int childCount, K lo) {
        if (lo == null) return 0;
        int ci = route(keys, lo);
        return ci >= childCount ? childCount - 1 : ci;
    }

    /** Highest covering child index for an upper bound (null = last child). */
    private int childBandHi(Object keys, int childCount, K hi) {
        if (hi == null) return childCount - 1;
        int ci = route(keys, hi);
        return ci >= childCount ? childCount - 1 : ci;
    }

    /** Materialize a covering leaf's in-range live entries (base∪ops, LWW, tombstones dropped). */
    private List<Map.Entry<K, V>> leafInRange(BNode n, List<Op> all, K lo, boolean loInc, K hi,
                                              boolean hiInc, boolean descending) {
        Object[] kv = applyOps(n.keys, n.values, all);
        int len = keyFormat.size(kv[0]);
        List<Map.Entry<K, V>> out = new ArrayList<>();
        for (int i = 0; i < len; i++) {
            K k = keyFormat.get(kv[0], i);
            if (inRange(k, lo, loInc, hi, hiInc)) {
                out.add(new AbstractMap.SimpleImmutableEntry<>(k, valueFormat.get(kv[1], i)));
            }
        }
        if (descending) java.util.Collections.reverse(out);
        return out;
    }

    private final class RangeFrame {
        final long[] children;
        final List<Op>[] batches;
        int idx;
        final int first, last;

        RangeFrame(long[] children, List<Op>[] batches, int idx, int first, int last) {
            this.children = children;
            this.batches = batches;
            this.idx = idx;
            this.first = first;
            this.last = last;
        }
    }

    private Iterator<Map.Entry<K, V>> prunedRangeIterator(K lo, boolean loInc, K hi, boolean hiInc,
                                                          boolean descending) {
        final ArrayDeque<RangeFrame> stack = new ArrayDeque<>();
        return new Iterator<>() {
            List<Map.Entry<K, V>> leaf;
            int lpos;
            boolean started = false, done = false;

            private void descend(long recid, List<Op> inherited) {
                for (;;) {
                    BNode n = load(recid);
                    List<Op> all = filterRange(combine(n.ops, inherited), lo, loInc, hi, hiInc);
                    if (!n.isDir()) {
                        leaf = leafInRange(n, all, lo, loInc, hi, hiInc, descending);
                        lpos = 0;
                        return;
                    }
                    long[] children = n.children();
                    List<Op>[] batches = partition(n, all);
                    int first = childBandLo(n.keys, children.length, lo);
                    int last = childBandHi(n.keys, children.length, hi);
                    if (first > last) { leaf = List.of(); lpos = 0; return; }
                    int startCi = descending ? last : first;
                    int nextIdx = descending ? last - 1 : first + 1;
                    stack.push(new RangeFrame(children, batches, nextIdx, first, last));
                    recid = children[startCi];
                    inherited = batches[startCi] == null ? List.of() : batches[startCi];
                }
            }

            private void advance() {
                if (done) return;
                if (!started) {
                    started = true;
                    descend(rootRecid(), List.of());
                }
                while (leaf == null || lpos >= leaf.size()) {
                    RangeFrame f = stack.peek();
                    if (f == null) { done = true; return; }
                    boolean more = descending ? f.idx >= f.first : f.idx <= f.last;
                    if (!more) { stack.pop(); continue; }
                    int ci = f.idx;
                    f.idx += descending ? -1 : 1;
                    descend(f.children[ci], f.batches[ci] == null ? List.of() : f.batches[ci]);
                }
            }

            @Override public boolean hasNext() { advance(); return !done && leaf != null && lpos < leaf.size(); }

            @Override public Map.Entry<K, V> next() {
                advance();
                if (done || leaf == null || lpos >= leaf.size()) throw new NoSuchElementException();
                return leaf.get(lpos++);
            }
        };
    }

    /**
     * Single-page (root-is-leaf) byte-side range read: a warm range cursor with NO base
     * materialization — merges a byte-side base cursor over only the in-range base slice with the
     * bounded log shadow. Returns null when the root is a directory (fall back to the pruned DFS)
     * or when a format lacks binary access (must materialize the base).
     */
    private List<Map.Entry<K, V>> tryByteRangeSingleLeaf(K lo, boolean loInc, K hi, boolean hiInc,
                                                         boolean descending) {
        if (!keyFormat.supportsBinary() || !valueFormat.supportsBinary()) return null;
        RangeLeafReader r = new RangeLeafReader(lo, loInc, hi, hiInc, descending);
        store.read(rootRecid(), r);
        return r.isDir ? null : r.out;
    }

    /** Byte-side ascending cursor over a leaf base's in-range slice {@code [startIdx,endIdx)}. */
    private final class LeafBaseCursor implements BufferedPageFormat.KvCursor<K, V> {
        final DataInput2 in;
        final int keyGroupStart, valueGroupStart, keysLen, endIdx;
        int idx;
        K curKey;
        V curVal;

        LeafBaseCursor(DataInput2 in, int keyGroupStart, int valueGroupStart, int keysLen,
                       int startIdx, int endIdx) {
            this.in = in;
            this.keyGroupStart = keyGroupStart;
            this.valueGroupStart = valueGroupStart;
            this.keysLen = keysLen;
            this.idx = startIdx;
            this.endIdx = endIdx;
        }

        @Override public boolean hasNext() { return idx < endIdx; }

        @Override public void next() {
            in.pos(keyGroupStart);
            curKey = keyFormat.binaryGet(in, keysLen, idx);   // reads only the idx-th key
            in.pos(valueGroupStart);
            curVal = valueFormat.binaryGet(in, keysLen, idx); // reads only the idx-th value
            idx++;
        }

        @Override public K key() { return curKey; }
        @Override public V value() { return curVal; }
    }

    /** RecordRead producing a single leaf's in-range entries entirely from page bytes. */
    private final class RangeLeafReader implements RecordRead {
        final K lo, hi;
        final boolean loInc, hiInc, descending;
        boolean isDir;
        List<Map.Entry<K, V>> out;

        RangeLeafReader(K lo, boolean loInc, K hi, boolean hiInc, boolean descending) {
            this.lo = lo;
            this.loInc = loInc;
            this.hi = hi;
            this.hiInc = hiInc;
            this.descending = descending;
        }

        @Override public long onBytes(DataInput2 in, int size) {
            int start = in.pos();
            int baseLen = in.readInt();
            if (baseLen < 0 || baseLen > size - 4) throw new IllegalStateException("torn baseLen");
            BufferedPageFormat.BaseHeader hdr = new BufferedPageFormat.BaseHeader();
            codec.parseBaseHeader(in, start + 4, baseLen, hdr);
            in.pos(hdr.nodeBase); // skip the base fingerprint header (range needs all in-range keys)
            int h = in.unpackInt();
            int flags = h & 0xF;
            int keysLen = h >>> 4;
            if ((flags & DIR) != 0) { isDir = true; return 0; } // caller falls back to pruned DFS
            if ((flags & RIGHT) == 0) in.unpackLong(); // skip link
            int keyGroupStart = in.pos();

            int startIdx, endIdx, valueGroupStart;
            if (keysLen == 0) {
                valueGroupStart = keyGroupStart;
                startIdx = 0;
                endIdx = 0;
            } else {
                if (lo != null) {
                    in.pos(keyGroupStart);
                    int r = keyFormat.binarySearch(lo, in, keysLen);
                    valueGroupStart = in.pos();
                    startIdx = r >= 0 ? (loInc ? r : r + 1) : -r - 1;
                } else {
                    in.pos(keyGroupStart);
                    keyFormat.binaryGet(in, keysLen, 0); // advance past key group → value group start
                    valueGroupStart = in.pos();
                    startIdx = 0;
                }
                endIdx = startIdx;
                while (endIdx < keysLen) {
                    in.pos(keyGroupStart);
                    K k = keyFormat.binaryGet(in, keysLen, endIdx);
                    if (hi != null) {
                        int c = keyFormat.compare(k, hi);
                        if (c > 0 || (c == 0 && !hiInc)) break;
                    }
                    endIdx++;
                }
            }

            NavigableMap<K, Object> shadow = new java.util.TreeMap<>(keyFormat::compare);
            codec.logShadowInRange(in, start + 4 + baseLen, start + size, lo, loInc, hi, hiInc,
                    Long.MAX_VALUE, shadow);
            LeafBaseCursor base = new LeafBaseCursor(in, keyGroupStart, valueGroupStart, keysLen, startIdx, endIdx);
            Iterator<Map.Entry<K, V>> merged = codec.merge(base, shadow);
            out = new ArrayList<>();
            while (merged.hasNext()) out.add(merged.next());
            if (descending) java.util.Collections.reverse(out);
            return 0;
        }
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

    /**
     * NATIVE reverse DFS: the mirror of {@link #entryIterator()} — children visited
     * RIGHT→LEFT, each leaf materialized by {@code applyOps} then emitted
     * high→low, ancestor buffer ops partitioned per child exactly as in the ascending DFS.
     * Strictly DESCENDING key order with un-flushed buffers included; weakly consistent.
     */
    public Iterator<Map.Entry<K, V>> descendingEntryIterator() {
        final ArrayDeque<Frame> stack = new ArrayDeque<>();

        return new Iterator<>() {
            Object leafKeys, leafVals;
            int pos;
            boolean started = false, done = false;

            private void descend(long recid, List<Op> inherited) {
                for (;;) {
                    BNode n = load(recid);
                    List<Op> all = combine(n.ops, inherited);
                    if (!n.isDir()) {
                        Object[] kv = applyOps(n.keys, n.values, all);
                        leafKeys = kv[0];
                        leafVals = kv[1];
                        pos = keyFormat.size(leafKeys) - 1; // emit downward
                        return;
                    }
                    long[] children = n.children();
                    List<Op>[] batches = partition(n, all);
                    int last = children.length - 1;
                    stack.push(new Frame(children, last - 1, batches)); // next-down child
                    recid = children[last];
                    inherited = batches[last] == null ? List.of() : batches[last];
                }
            }

            private void advance() {
                if (done) return;
                if (!started) {
                    started = true;
                    descend(rootRecid(), List.of());
                }
                while (pos < 0) {
                    Frame f = stack.peek();
                    if (f == null) { done = true; return; }
                    if (f.idx < 0) {
                        stack.pop();
                        continue;
                    }
                    int ci = f.idx--;
                    descend(f.children[ci], f.batches[ci] == null ? List.of() : f.batches[ci]);
                }
            }

            @Override public boolean hasNext() { advance(); return !done && pos >= 0; }

            @Override public Map.Entry<K, V> next() {
                advance();
                if (done || pos < 0) throw new NoSuchElementException();
                K k = keyFormat.get(leafKeys, pos);
                V v = valueFormat.get(leafVals, pos);
                pos--;
                return new AbstractMap.SimpleImmutableEntry<>(k, v);
            }
        };
    }

    /**
     * Bounded DESCENDING iterator over {@code [lo,hi]} (null bound = open) — R1 PRUNED reverse
     * cross-level cursor, the mirror of {@link #entryIterator(Object, boolean, Object, boolean)}:
     * single-leaf byte-side fast path (no base materialization), else a pruned reverse DFS
     * (covering children right→left, inherited ops range-filtered before partition). Same
     * per-query bound; never {@code O(full-map)}.
     */
    Iterator<Map.Entry<K, V>> descendingEntryIterator(K lo, boolean loInc, K hi, boolean hiInc) {
        List<Map.Entry<K, V>> single = tryByteRangeSingleLeaf(lo, loInc, hi, hiInc, true);
        if (single != null) return single.iterator();
        return prunedRangeIterator(lo, loInc, hi, hiInc, true);
    }

    /**
     * Atomically remove and return the LEAST in-range entry, or null when empty. Single-writer:
     * under the writeLock, find the first live in-range entry, extract its key/value, DISCARD
     * the iterator, then append a PRIVATE tombstone ({@code appendMessage(k, null)}, not the
     * public {@code remove}) — the tombstone/flush never runs while a cursor is live. No retry
     * loop is needed: the whole find-and-tombstone is one writer-lock critical section, so it
     * is atomic on the returned entry (never removes a value it did not return); which entry is
     * least/greatest is weakly consistent, as everywhere here.
     */
    Map.Entry<K, V> pollFirstEntry(K lo, boolean loInc, K hi, boolean hiInc) {
        writeLock.lock();
        try {
            Iterator<Map.Entry<K, V>> it = entryIterator(lo, loInc, hi, hiInc);
            if (!it.hasNext()) return null;
            Map.Entry<K, V> e = it.next();
            K k = e.getKey();
            V v = e.getValue();
            appendMessage(k, null);
            return new AbstractMap.SimpleImmutableEntry<>(k, v);
        } finally {
            writeLock.unlock();
        }
    }

    /** Atomically remove and return the GREATEST in-range entry, or null when empty. Same
     *  writeLock discipline as {@link #pollFirstEntry}, candidate from the reverse DFS. */
    Map.Entry<K, V> pollLastEntry(K lo, boolean loInc, K hi, boolean hiInc) {
        writeLock.lock();
        try {
            Iterator<Map.Entry<K, V>> it = descendingEntryIterator(lo, loInc, hi, hiInc);
            if (!it.hasNext()) return null;
            Map.Entry<K, V> e = it.next();
            K k = e.getKey();
            V v = e.getValue();
            appendMessage(k, null);
            return new AbstractMap.SimpleImmutableEntry<>(k, v);
        } finally {
            writeLock.unlock();
        }
    }

    // ================= Map / SortedMap surface =================

    /** {@link Map#size()}: saturates at {@link Integer#MAX_VALUE}; use {@link #sizeLong}. */
    @Override public int size() {
        long n = sizeLong();
        return n > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) n;
    }

    @Override public boolean isEmpty() { return !entryIterator().hasNext(); }

    /** Removes every entry via blind tombstones. Snapshots the key set first: a tombstone
     *  append can trigger a flush/split, which would disturb a live DFS cursor. */
    @Override public void clear() {
        List<K> keys = new ArrayList<>();
        for (Iterator<Map.Entry<K, V>> it = entryIterator(); it.hasNext(); ) {
            keys.add(it.next().getKey());
        }
        for (K k : keys) removeOnly(k);
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

    /** Cached open-bounds ascending {@link ConcurrentOrderedNavigableView} backing the
     *  navigable surface, so every sub-view is itself a {@code ConcurrentNavigableMap}. */
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

    /** Routes through {@link #firstEntry()}. */
    @Override public K firstKey() {
        Map.Entry<K, V> e = firstEntry();
        if (e == null) throw new NoSuchElementException();
        return e.getKey();
    }

    /** Routes through {@link #lastEntry()}; O(n) (the DFS yields no direct rightmost access). */
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
    @Override public ConcurrentNavigableMap<K, V> subMap(K fromKey, K toKey) { return fullView().subMap(fromKey, toKey); }
    @Override public ConcurrentNavigableMap<K, V> headMap(K toKey) { return fullView().headMap(toKey); }
    @Override public ConcurrentNavigableMap<K, V> tailMap(K fromKey) { return fullView().tailMap(fromKey); }

    /** Bridges this map to the shared range/view layer (stateless — cheap to re-create).
     *  Concurrent adapter: every CAS/point method delegates to the map's writer-lock'd
     *  primitive, so sub-views offer the same atomic {@code ConcurrentMap} surface. */
    private final class Adapter implements ConcurrentOrderedMapAdapter<K, V> {
        @Override public Comparator<? super K> comparator() { return keyFormat.comparator(); }
        @Override public int compare(K a, K b) { return keyFormat.compare(a, b); }
        @Override public V get(Object key) { return BufferTreeMap.this.get(key); }
        @Override public boolean containsKey(Object key) { return BufferTreeMap.this.containsKey(key); }
        @Override public V put(K key, V value) { return BufferTreeMap.this.put(key, value); }
        @Override public V remove(Object key) { return BufferTreeMap.this.remove(key); }
        @Override public boolean remove(Object key, Object value) { return BufferTreeMap.this.remove(key, value); }
        @Override public V putIfAbsent(K key, V value) { return BufferTreeMap.this.putIfAbsent(key, value); }
        @Override public V replace(K key, V value) { return BufferTreeMap.this.replace(key, value); }
        @Override public boolean replace(K key, V oldValue, V newValue) { return BufferTreeMap.this.replace(key, oldValue, newValue); }
        @Override public boolean valueEquals(V a, V b) { return valueFormat.element().equals(a, b); }
        @Override public Iterator<Map.Entry<K, V>> entryIterator(K lo, boolean loInc, K hi, boolean hiInc) {
            return BufferTreeMap.this.entryIterator(lo, loInc, hi, hiInc);
        }
        @Override public Iterator<Map.Entry<K, V>> descendingEntryIterator(K lo, boolean loInc, K hi, boolean hiInc) {
            return BufferTreeMap.this.descendingEntryIterator(lo, loInc, hi, hiInc);
        }
        @Override public Map.Entry<K, V> pollFirstEntry(K lo, boolean loInc, K hi, boolean hiInc) {
            return BufferTreeMap.this.pollFirstEntry(lo, loInc, hi, hiInc);
        }
        @Override public Map.Entry<K, V> pollLastEntry(K lo, boolean loInc, K hi, boolean hiInc) {
            return BufferTreeMap.this.pollLastEntry(lo, loInc, hi, hiInc);
        }
        @Override public long sizeLong(K lo, boolean loInc, K hi, boolean hiInc) {
            return BufferTreeMap.this.sizeLong(lo, loInc, hi, hiInc);
        }
    }
}
