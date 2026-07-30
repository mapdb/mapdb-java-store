package org.mapdb.store;

import org.mapdb.DBException;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;
import org.mapdb.ser.Serializer;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.PrimitiveIterator;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Append-only store: SINGLE WRITER, WAIT-FREE readers, no locks anywhere.
 *
 * <h2>Why it exists</h2>
 *
 * {@link StoreDirect}'s segment StampedLocks are priced for many writers. For a
 * single-writer structure like {@link org.mapdb.btree.BufferTreeMap} they are pure
 * overhead, and worse: every root append bumps the root segment's stamp, so ALL
 * concurrent optimistic readers hashing to that segment fall back to the locked
 * path (the "root append hotspot" — SegmentLocks.forRecid tops the JFR load
 * profile at every leaf-headroom setting). This store removes the mechanism
 * instead of tuning it.
 *
 * <h2>Design</h2>
 *
 * The data area is APPEND-ONLY: an extent, once written, is never mutated below
 * its published length, and its bytes are never overwritten — reclamation works
 * by copying (see below), not by reuse. The only mutable word per record is its
 * index entry — one packed long:
 *
 * <pre>
 *   bit 63..21  byte offset of the extent (43 bits, 8 TB of monotonic address space)
 *   bit 20..0   used content length (21 bits; MAX_CAPACITY is ~1 MB)
 *   offset == 0 is a state sentinel: 0 = void, 1 = preallocated (P),
 *   2 = explicit null content, 3 = deleted. Live extents start at offset 16.
 * </pre>
 *
 * Extent layout: {@code [int capBytes][content .. capacity)}; the capacity header
 * is written once at allocation and never changes (only the writer reads it).
 *
 * <ul>
 * <li><b>Write publication:</b> content bytes are plain-written, then the index
 *     entry is release-stored ({@link VarHandle#setRelease}). Rewrites go to a
 *     FRESH extent; {@link #append} plain-writes only bytes at [used, used+len)
 *     — beyond anything published — then release-stores the new length.</li>
 * <li><b>Reads are wait-free:</b> one acquire-load of the index entry, then the
 *     bytes [offset+4, offset+4+used) are immutable — no lock, no stamp
 *     validation, no retry, and torn bytes are IMPOSSIBLE (a stale entry points
 *     at a dead but forever-intact previous version; recid reuse cannot ABA a
 *     reader because the entry is one atomic long and extents are never
 *     recycled). {@link RecordRead} actions run exactly once per read.</li>
 * <li><b>Writer runs bare:</b> allocator state (dataTail, recid free list) is
 *     plain fields owned by the single writer; an append is a memcpy plus one
 *     release-store. Writers may be different threads over time when the caller
 *     provides the happens-before (BufferTreeMap's writer lock does); OVERLAPPING
 *     writers are caller error, asserted under -ea at zero production cost.</li>
 * </ul>
 *
 * <h2>Compaction: GC-based safe memory reclamation</h2>
 *
 * Every rewrite abandons the old extent, so garbage grows with rewrite volume.
 * A copying compactor (writer-context only; auto-triggered from the allocator
 * when garbage exceeds {@code max(minGarbageBytes, live)}, or explicit via
 * {@link #compact()}) evacuates majority-dead slices: each live extent is copied
 * to the tail — capacity preserved, so append headroom survives the move — and
 * its index entry release-stored, the same publication as any rewrite. Fully-dead
 * victim slices are then RETIRED: unlinked from the volume, never overwritten.
 *
 * Readers stay wait-free because the JVM's GC is the reclamation epoch: a reader
 * that resolved a slice before retirement holds the ByteBuffer reference and
 * reads an intact stale version (the slice is collected only when the last such
 * reference dies); a reader that resolves after sees the null slot and retries
 * against the re-pointed entry ({@link ByteBufferVol#dataInputOrNull}). No grace
 * periods, no reader registration, no happy-path validation — the retry only
 * fires when a read races the retirement of the exact slice it targets.
 *
 * Steady-state footprint converges on roughly 2× live bytes — an amortized
 * heuristic, not a hard bound (whole-slice granularity and the partial tail
 * slip past it in small stores): victims are majority-dead slices, so
 * evacuating them frees more than it copies, and when average garbage exceeds
 * 50%, some slice is majority-dead by pigeonhole.
 * Address space burns monotonically — retired offsets are never revisited —
 * against the 8 TB packed-offset limit ({@link DBException.StoreFull}).
 * {@link #allocatedBytes()} is the monotonic high-water mark;
 * {@link #occupiedBytes()} the actual non-retired footprint. Non-durable, like
 * StoreDirect's heap-index milestone.
 *
 * With {@code directBuffers=true} the release/acquire edges run over off-heap
 * ByteBuffer content, where the JMM's letter is thinner than for heap arrays;
 * the fence shape is the standard HotSpot-sound one and matches the rest of the
 * store layer's assumptions.
 */
public class StoreAppendOnly implements StoreDelta {

    private static final VarHandle IDX = MethodHandles.arrayElementVarHandle(long[].class);

    private static final int INDEX_PAGE_SHIFT = 17;
    private static final int INDEX_PAGE_SIZE = 1 << INDEX_PAGE_SHIFT;
    private static final int INDEX_PAGE_MASK = INDEX_PAGE_SIZE - 1;

    private static final int USED_BITS = 21; // 2 MB > MAX_CAPACITY; leaves 43 offset bits (8 TB)
    private static final long USED_MASK = (1L << USED_BITS) - 1;
    private static final long MAX_OFFSET = (1L << (64 - USED_BITS)) - 1;

    /** Default auto-compaction floor: no sweep until at least this much garbage exists. */
    public static final long DEFAULT_MIN_GARBAGE_BYTES = 64L << 20;

    // offset==0 sentinels (live extents start at 16, so offset 0 never occurs)
    private static final long IV_VOID = 0, IV_PREALLOC = 1, IV_NULL = 2, IV_DELETED = 3;

    private final ByteBufferVol vol;

    /** Paged index; page objects are stable, growth copies only page references. */
    private volatile long[][] indexPages = new long[0][];

    // ---- writer-owned state (single-writer contract; no locks) ----
    private volatile long maxRecid = 0; // volatile only for getAllRecids/verify
    private long[] freeRecids = new long[16];
    private int freeRecidsSize = 0;
    private long dataTail = 16; // offset 0 reserved: live offsets are never 0

    // ---- compaction accounting (writer-owned) ----
    private final long minGarbageBytes;
    private long totalLive = 0;              // sum of live extent capacities
    private long[] sliceLive = new long[16]; // live extent bytes per slice
    private final java.util.BitSet retired = new java.util.BitSet();
    private int retiredSlices = 0;
    private boolean compacting = false;
    private byte[] copyBuf = new byte[0];    // sweep scratch, grown lazily

    /** -ea-only overlapping-writer detector (production cost: none — asserts compile away). */
    private final AtomicReference<Thread> writerGuard = new AtomicReference<>();

    private volatile boolean closed = false;

    public StoreAppendOnly() { this(false); }

    public StoreAppendOnly(boolean directBuffers) {
        this(directBuffers, DEFAULT_MIN_GARBAGE_BYTES);
    }

    /**
     * @param minGarbageBytes auto-compaction floor: a sweep triggers only once garbage
     *                        exceeds {@code max(minGarbageBytes, live bytes)}. Small
     *                        values make compaction eager (tests); {@link Long#MAX_VALUE}
     *                        disables auto-compaction ({@link #compact()} still works).
     */
    public StoreAppendOnly(boolean directBuffers, long minGarbageBytes) {
        this.vol = new ByteBufferVol(directBuffers);
        this.minGarbageBytes = minGarbageBytes;
    }

    /**
     * True in the SWMR sense: any number of concurrent readers are safe alongside
     * at most ONE writer at a time (sequential writer handoff needs caller-provided
     * happens-before; BufferTreeMap's writer lock is one). NOT safe for overlapping
     * writers — that is caller error, detected under -ea by {@link #writerGuard}.
     */
    @Override public boolean isThreadSafe() { return true; }

    // ---------- packing ----------

    private static long pack(long offset, int used) {
        assert offset >= 16 && offset <= MAX_OFFSET : "offset out of range: " + offset;
        assert used >= 0 && used <= USED_MASK : "used out of range: " + used;
        return (offset << USED_BITS) | used;
    }

    private static long offset(long iv) { return iv >>> USED_BITS; }

    private static int used(long iv) { return (int) (iv & USED_MASK); }

    private static boolean isLive(long iv) { return offset(iv) != 0; }

    // ---------- helpers ----------

    private void checkClosed() {
        if (closed) throw new DBException.StoreClosed();
    }

    private boolean enterWriter() {
        Thread other = writerGuard.compareAndExchange(null, Thread.currentThread());
        if (other != null)
            throw new AssertionError("concurrent writers (single-writer contract): "
                    + other + " and " + Thread.currentThread());
        return true;
    }

    private boolean exitWriter() {
        writerGuard.set(null);
        return true;
    }

    /** Acquire-load; 0 (void) when never allocated / out of range. */
    private long ivGet(long recid) {
        long[][] pages = indexPages;
        int p = (int) (recid >>> INDEX_PAGE_SHIFT);
        if (recid < 1 || p >= pages.length) return IV_VOID;
        return (long) IDX.getAcquire(pages[p], (int) (recid & INDEX_PAGE_MASK));
    }

    /** enforces the record-state contract: throws GetVoid on N and D states. */
    private long ivGetChecked(long recid) {
        long iv = ivGet(recid);
        if (iv == IV_VOID || iv == IV_DELETED) throw new DBException.GetVoid(recid);
        return iv;
    }

    /** Release-store: publishes every plain byte write program-ordered before it. */
    private void ivSet(long recid, long iv) {
        IDX.setRelease(indexPages[(int) (recid >>> INDEX_PAGE_SHIFT)], (int) (recid & INDEX_PAGE_MASK), iv);
    }

    /** Writer only. */
    private void ensureIndexPage(long recid) {
        int p = (int) (recid >>> INDEX_PAGE_SHIFT);
        long[][] pages = indexPages;
        if (p < pages.length) return;
        long[][] grown = Arrays.copyOf(pages, p + 1);
        for (int i = pages.length; i <= p; i++) grown[i] = new long[INDEX_PAGE_SIZE];
        indexPages = grown; // volatile store; readers of published recids see the page (see ivGet chain)
    }

    /** Writer only. */
    private long allocRecid() {
        long recid = freeRecidsSize > 0 ? freeRecids[--freeRecidsSize] : maxRecid + 1;
        if (recid > maxRecid) maxRecid = recid;
        ensureIndexPage(recid);
        return recid;
    }

    /** Writer only. */
    private void freeRecid(long recid) {
        if (freeRecidsSize == freeRecids.length) freeRecids = Arrays.copyOf(freeRecids, freeRecids.length * 2);
        freeRecids[freeRecidsSize++] = recid;
    }

    /**
     * Writer only. Allocates a fresh 16-aligned extent (never reused, never crosses
     * a slice) and writes its immutable capacity header; content area is unwritten.
     * May trigger a compaction sweep first — callers that hold a record's current
     * location across this call must re-read the index entry afterwards.
     */
    private long allocExtent(int capBytes) {
        assert capBytes >= 4 && (capBytes & 15) == 0 : "bad extent capacity " + capBytes;
        if (!compacting) maybeCompact();
        long rem = ByteBufferVol.SLICE_SIZE - (dataTail & ByteBufferVol.SLICE_MASK);
        if (rem < capBytes) dataTail += rem; // abandon the remainder (append-only: no free lists)
        long off = dataTail;
        if (off > MAX_OFFSET) // pack() would silently truncate — refuse before touching anything
            throw new DBException.StoreFull("append-only data area exceeds packed-offset limit ("
                    + MAX_OFFSET + " bytes)");
        dataTail += capBytes;
        vol.ensureAvailable(dataTail);
        int s = (int) (off >>> ByteBufferVol.SLICE_SHIFT);
        if (s >= sliceLive.length) sliceLive = Arrays.copyOf(sliceLive, Math.max(s + 1, sliceLive.length * 2));
        sliceLive[s] += capBytes;
        totalLive += capBytes;
        vol.putInt(off, capBytes);
        return off;
    }

    /** Writer only. Un-account a no-longer-current extent (its bytes stay intact for in-flight readers). */
    private void abandon(long iv) {
        if (!isLive(iv)) return;
        long off = offset(iv);
        int cap = vol.getInt(off);
        sliceLive[(int) (off >>> ByteBufferVol.SLICE_SHIFT)] -= cap;
        totalLive -= cap;
    }

    private void maybeCompact() {
        // full slices only (floor, tail excluded): the partial tail is not collectable,
        // so counting it would fire no-op sweeps whenever the floor is small
        long fullSliceBytes = ((dataTail >>> ByteBufferVol.SLICE_SHIFT) - retiredSlices)
                << ByteBufferVol.SLICE_SHIFT;
        long garbage = fullSliceBytes - totalLive; // may go negative when live sits in the tail
        if (garbage > minGarbageBytes && garbage > totalLive) compactSweep();
    }

    /**
     * Copying compaction, writer-context (same single-writer contract as any mutation).
     * Bounded per sweep: one index scan plus copies of the live extents in majority-dead
     * slices. Self-limiting: post-sweep every surviving slice is majority-live, so the
     * trigger condition (garbage &gt; live) is off until at least that much new garbage
     * accumulates.
     */
    @Override public void compact() {
        checkClosed();
        assert enterWriter();
        try {
            compactSweep();
        } finally {
            assert exitWriter();
        }
    }

    private void compactSweep() {
        assert !compacting;
        compacting = true;
        try {
            int tailSlice = (int) (dataTail >>> ByteBufferVol.SLICE_SHIFT);
            int nSlices = (int) ((dataTail + ByteBufferVol.SLICE_MASK) >>> ByteBufferVol.SLICE_SHIFT);
            // victims: non-tail, not yet retired, at most half live
            boolean[] victim = new boolean[nSlices];
            boolean any = false;
            for (int s = 0; s < nSlices; s++) {
                if (s == tailSlice || retired.get(s)) continue;
                long live = s < sliceLive.length ? sliceLive[s] : 0;
                if (live <= ByteBufferVol.SLICE_SIZE / 2) {
                    victim[s] = true;
                    any = true;
                }
            }
            if (!any) return;
            // evacuate: copy live extents out of victims, republishing entries one release-store
            // at a time (readers see old or new version, both intact — never a mix)
            long max = maxRecid;
            for (long recid = 1; recid <= max; recid++) {
                long iv = ivGet(recid);
                if (!isLive(iv)) continue;
                long off = offset(iv);
                int s = (int) (off >>> ByteBufferVol.SLICE_SHIFT);
                if (s >= victim.length || !victim[s]) continue;
                int used = used(iv);
                int cap = vol.getInt(off);
                if (copyBuf.length < used) copyBuf = new byte[Math.max(used, copyBuf.length * 2)];
                vol.getData(off + 4, copyBuf, 0, used);
                long newOff = allocExtent(cap); // capacity preserved: append headroom survives the move
                vol.putData(newOff + 4, copyBuf, 0, used);
                abandon(iv);
                ivSet(recid, pack(newOff, used));
            }
            // retire: unlink so the GC can reclaim once the last in-flight reader lets go
            for (int s = 0; s < nSlices; s++) {
                if (!victim[s]) continue;
                assert s >= sliceLive.length || sliceLive[s] == 0 : "victim slice " + s + " still live";
                vol.retireSlice(s);
                retired.set(s);
                retiredSlices++;
            }
        } finally {
            compacting = false;
        }
    }

    /** Writer only. Fresh extent with content + headroom capacity; NOT yet published. */
    private long writeExtent(byte[] buf, int bufOff, int len, int capBytes) {
        long off = allocExtent(capBytes);
        vol.putData(off + 4, buf, bufOff, len);
        return off;
    }

    private static <R> DataOutput2 serialize(R record, Serializer<R> ser) {
        DataOutput2 out = new DataOutput2(Math.max(16, ser.sizeHint() + 4));
        ser.serialize(out, record);
        return out;
    }

    /** Long math so overflowing int expressions can never yield a bogus small/negative capacity. */
    private static int checkedCapacity(long need) {
        if (need > IndexVal.MAX_CAPACITY) throw new DBException.RecordTooLarge(need);
        return IndexVal.roundUp16((int) need);
    }

    // ---------- Store ----------

    @Override public long preallocate() {
        checkClosed();
        assert enterWriter();
        try {
            long recid = allocRecid();
            ivSet(recid, IV_PREALLOC);
            return recid;
        } finally {
            assert exitWriter();
        }
    }

    @Override public <R> long put(R record, Serializer<R> serializer) {
        checkClosed();
        DataOutput2 out = record == null ? null : serialize(record, serializer);
        int capBytes = out == null ? 0 : checkedCapacity(4L + out.pos);
        assert enterWriter();
        try {
            long recid = allocRecid();
            if (out == null) ivSet(recid, IV_NULL);
            else ivSet(recid, pack(writeExtent(out.buf, 0, out.pos, capBytes), out.pos));
            return recid;
        } finally {
            assert exitWriter();
        }
    }

    @Override public <R> R get(long recid, Serializer<R> serializer) {
        for (;;) {
            checkClosed();
            long iv = ivGetChecked(recid);
            if (!isLive(iv)) return null;
            int used = used(iv);
            DataInput2 in = vol.dataInputOrNull(offset(iv) + 4, used);
            if (in == null) continue; // raced that slice's retirement; entry is already re-pointed
            return serializer.deserialize(in, used);
        }
    }

    /**
     * Read path: the acquire-load of the index entry pairs with the writer's
     * release-store, so the bytes it points at are fully published and — the
     * append-only guarantee — immutable while reachable. There is no validation
     * and the action never sees torn bytes; the only non-wait-free case is losing
     * a race with compaction retiring the exact slice being resolved, which
     * re-reads the (already re-pointed) entry and retries. A resolved view stays
     * valid for the whole action even if the slice retires mid-read: retirement
     * unlinks, never overwrites, and the view's reference holds off the GC.
     */
    @Override public long read(long recid, RecordRead action) {
        for (;;) {
            checkClosed();
            long iv = ivGetChecked(recid);
            if (!isLive(iv)) return action.onNull();
            int used = used(iv);
            DataInput2 in = vol.dataInputOrNull(offset(iv) + 4, used);
            if (in == null) continue;
            return action.onBytes(in, used);
        }
    }

    @Override public <R> void update(long recid, R record, Serializer<R> serializer) {
        updateWithHeadroom(recid, record, serializer, 0);
    }

    @Override public <R> void updateWithHeadroom(long recid, R record, Serializer<R> serializer, int headroom) {
        checkClosed();
        if (headroom < 0) throw new IllegalArgumentException("headroom must be >= 0: " + headroom);
        DataOutput2 out = record == null ? null : serialize(record, serializer);
        assert enterWriter();
        try {
            updateImpl(recid, out, headroom);
        } finally {
            assert exitWriter();
        }
    }

    /** Writer only. Always a fresh extent — published bytes are never rewritten. */
    private void updateImpl(long recid, DataOutput2 out, int headroom) {
        ivGetChecked(recid); // GetVoid on N/D
        if (out == null) {
            abandon(ivGet(recid));
            ivSet(recid, IV_NULL);
            return;
        }
        int capBytes = checkedCapacity(4L + out.pos + headroom);
        long off = writeExtent(out.buf, 0, out.pos, capBytes); // may compact — can MOVE this record
        abandon(ivGet(recid)); // so re-read the entry only now, and un-account wherever it lives
        ivSet(recid, pack(off, out.pos));
    }

    @Override public <R> boolean compareAndSwap(long recid, R expectedOldRecord, R newRecord, Serializer<R> serializer) {
        checkClosed();
        assert enterWriter();
        try {
            long iv = ivGetChecked(recid);
            R current = null;
            if (isLive(iv)) {
                int used = used(iv);
                current = serializer.deserialize(vol.dataInput(offset(iv) + 4, used), used);
            }
            boolean eq = (current == null && expectedOldRecord == null)
                    || (current != null && expectedOldRecord != null && serializer.equals(current, expectedOldRecord));
            if (!eq) return false;
            updateImpl(recid, newRecord == null ? null : serialize(newRecord, serializer), 0);
            return true;
        } finally {
            assert exitWriter();
        }
    }

    @Override public <R> void delete(long recid, Serializer<R> serializer) {
        checkClosed();
        assert enterWriter();
        try {
            abandon(ivGetChecked(recid));
            ivSet(recid, IV_DELETED);
            freeRecid(recid);
        } finally {
            assert exitWriter();
        }
    }

    // ---------- StoreDelta ----------

    @Override public long append(long recid, byte[] data, int offset, int len) {
        checkClosed();
        java.util.Objects.checkFromIndexSize(offset, len, data.length);
        assert enterWriter();
        try {
            long iv = ivGetChecked(recid);
            if (!isLive(iv)) {
                // first append establishes the record: capacity == exactly what is needed
                int capBytes = checkedCapacity(4L + len);
                ivSet(recid, pack(writeExtent(data, offset, len, capBytes), len));
                return len;
            }
            long off = offset(iv);
            int used = used(iv);
            int capBytes = vol.getInt(off); // immutable header; only the writer reads it
            if (4L + used + len > capBytes) return StoreDelta.REFUSED;
            // bytes beyond `used` were never published — plain writes, then release the new length
            vol.putData(off + 4 + used, data, offset, len);
            ivSet(recid, pack(off, used + len));
            return used + len;
        } finally {
            assert exitWriter();
        }
    }

    @Override public long capacityRemaining(long recid) {
        for (;;) {
            checkClosed();
            long iv = ivGetChecked(recid);
            if (!isLive(iv)) return 0;
            DataInput2 in = vol.dataInputOrNull(offset(iv), 4); // capacity header
            if (in == null) continue; // raced slice retirement
            return in.readInt() - 4L - used(iv);
        }
    }

    // ---------- lifecycle ----------

    @Override public void commit() {
        checkClosed();
        // no-op: non-durable, like StoreDirect's heap-index milestone
    }

    @Override public void close() {
        closed = true;
        vol.close();
        indexPages = new long[0][];
    }

    @Override public boolean isClosed() { return closed; }

    /**
     * Data-area high-water mark in bytes — MONOTONIC address-space consumption
     * (compaction retires slices but never revisits offsets). Weak snapshot — under
     * a concurrent writer the value may be stale (dataTail is writer-owned plain
     * state); call quiescently for an exact figure.
     */
    public long allocatedBytes() { return dataTail; }

    /**
     * Actual data-area footprint: bytes in non-retired slices (live extents plus
     * not-yet-collected garbage). Bounded at roughly 2× live by the compactor.
     * Weak snapshot, like {@link #allocatedBytes()}.
     */
    public long occupiedBytes() {
        long slices = (dataTail + ByteBufferVol.SLICE_MASK) >>> ByteBufferVol.SLICE_SHIFT;
        return (slices - retiredSlices) << ByteBufferVol.SLICE_SHIFT;
    }

    /**
     * Quiescent/test API like the rest of the layer: under a concurrent writer this
     * is a weak snapshot (may miss in-flight allocations; dataTail may be stale).
     */
    @Override public void verify() {
        checkClosed();
        // abandoned extents are untracked, so gaps are expected; live extents must
        // be sane, disjoint, in non-retired slices, and match the compaction accounting
        ArrayList<long[]> extents = new ArrayList<>(); // {offset, capBytes}
        long[] liveBySlice = new long[(int) ((dataTail + ByteBufferVol.SLICE_MASK) >>> ByteBufferVol.SLICE_SHIFT)];
        long liveSum = 0;
        long max = maxRecid;
        for (long recid = 1; recid <= max; recid++) {
            long iv = ivGet(recid);
            if (!isLive(iv)) continue;
            long off = offset(iv);
            int s = (int) (off >>> ByteBufferVol.SLICE_SHIFT);
            if (retired.get(s)) throw new DBException.VerifyFailed("live record in retired slice, recid=" + recid);
            int capBytes = vol.getInt(off);
            if ((off & 15) != 0) throw new DBException.VerifyFailed("offset not aligned, recid=" + recid);
            if (off + capBytes > dataTail) throw new DBException.VerifyFailed("record beyond dataTail, recid=" + recid);
            if ((off & ByteBufferVol.SLICE_MASK) + capBytes > ByteBufferVol.SLICE_SIZE)
                throw new DBException.VerifyFailed("record crosses slice, recid=" + recid);
            if (4 + used(iv) > capBytes)
                throw new DBException.VerifyFailed("used beyond capacity, recid=" + recid);
            extents.add(new long[]{off, capBytes});
            liveBySlice[s] += capBytes;
            liveSum += capBytes;
        }
        extents.sort((a, b) -> Long.compare(a[0], b[0]));
        long prevEnd = 16;
        for (long[] ext : extents) {
            if (ext[0] < prevEnd) throw new DBException.VerifyFailed("overlapping extents at offset " + ext[0]);
            prevEnd = ext[0] + ext[1];
        }
        if (prevEnd > dataTail) throw new DBException.VerifyFailed("extent beyond dataTail");
        if (liveSum != totalLive)
            throw new DBException.VerifyFailed("totalLive accounting off: " + totalLive + " != " + liveSum);
        for (int s = 0; s < liveBySlice.length; s++) {
            long tracked = s < sliceLive.length ? sliceLive[s] : 0;
            if (tracked != liveBySlice[s])
                throw new DBException.VerifyFailed("sliceLive[" + s + "] accounting off: "
                        + tracked + " != " + liveBySlice[s]);
        }
    }

    /** Weak snapshot under a concurrent writer (may miss recids allocated in flight). */
    @Override public PrimitiveIterator.OfLong getAllRecids() {
        checkClosed();
        long max = maxRecid;
        long[] result = new long[64];
        int size = 0;
        for (long recid = 1; recid <= max; recid++) {
            long iv = ivGet(recid);
            if (iv == IV_VOID || iv == IV_DELETED || iv == IV_PREALLOC) continue;
            if (size == result.length) result = Arrays.copyOf(result, size * 2);
            result[size++] = recid;
        }
        final long[] arr = Arrays.copyOf(result, size);
        return Arrays.stream(arr).iterator();
    }
}
