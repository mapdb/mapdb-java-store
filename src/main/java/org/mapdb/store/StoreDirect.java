package org.mapdb.store;

import org.mapdb.DBException;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;
import org.mapdb.ser.Serializer;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.PrimitiveIterator;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * Durable direct store: every structure — recid index, free lists, allocator
 * metadata and record data — lives ON THE VOLUME (the Store4Direct
 * plan; algorithms ported from mapdb3 StoreDirect/StoreDirectAbstract, adapted
 * to the capacity-based {@link IndexVal} encoding).
 *
 * <h2>On-volume format v1</h2>
 * <pre>
 * page size == volume slice == 1 MB; nothing ever crosses a page boundary.
 *
 * page 0 (header):
 *   0   magic "MDBS.SD1" (8)
 *   8   feature bits int (must be 0)                12  reserved int
 *   16  header checksum int (written by commit/close; verified on open)
 *   24  dataTail  parity4   (allocation cursor inside the current data page; 0 = none)
 *   32  maxRecid&lt;&lt;4 parity4  (recid high-water)
 *   40  fileTail  parity16  (end of last allocated page == logical file size)
 *   64  free-recid long-stack master link, parity4
 *   72  free-data long-stack master links, one per 16-byte size class
 *       (capacityUnits 1..0xFFFD), parity4 each ... HEAD_END = 524336
 *   HEAD_END        ZERO_PAGE_LINK: first index page pointer, parity16 (+8 reserved)
 *   HEAD_END + 16   index slots for recids 1..65528
 *
 * index page (1 MB, allocated at fileTail): [0..8) reserved, [8..16) next index
 *   page parity16, [16..) 8-byte slots. Index values are parity1-encoded
 *   {@link IndexVal}s; raw 0 = never allocated.
 *
 * long stack chunk: [0..8) parity4 (chunkSize&lt;&lt;48 | prevChunkOffset), then
 *   packed longs (7 bits/byte MSB-first, 0x80 marks the LAST byte — the same
 *   packing as DataOutput2.packLong, popped by scanning backwards for the
 *   previous terminator). Freed values are zeroed so chunk tails stay parseable.
 *   Free-data stack values are parity1(offset&gt;&gt;&gt;3); free-recid values parity1(recid&lt;&lt;1).
 *
 * linked record chunk (records whose content exceeds MAX_CAPACITY-4):
 *   [0..4) chunk data length int, [4..12) next pointer parity1(capUnits&lt;&lt;48|offset)
 *   (parity1(0) terminates), [12..) data. The root chunk is addressed by an
 *   index value carrying FLAG_LINKED; chains are written tail-first.
 * </pre>
 *
 * Durability model: crash-unsafe by design. Mutations write straight
 * to the (possibly mmap'd) volume; {@link #commit()} stamps the header checksum and
 * syncs — after a clean commit/close the store reopens; after a crash the checksum
 * mismatch is DETECTED on reopen and open refuses (repair/transactions are
 * {@link StoreWAL}'s job, which layers over an in-memory instance of this store).
 *
 * Locking: segment R/W locks by recid low bits; one structural lock
 * for allocator state (header vars, free lists, index page table). Order: segment,
 * then structural. Serialization happens outside locks on the put path. Reads are
 * optimistic-first (seqlock over the segment StampedLock).
 */
public class StoreDirect implements StoreDelta {

    // ---------- on-volume geometry ----------

    static final long PAGE_SIZE = ByteBufferVol.SLICE_SIZE;

    /** "MDBS.SD1" — the trailing byte is the format version. */
    static final long MAGIC = 0x4D4442532E534431L;

    static final long O_FEATURES = 8;
    static final long O_HEAD_CHECKSUM = 16;
    static final long O_DATA_TAIL = 24;
    static final long O_MAX_RECID = 32;
    static final long O_FILE_TAIL = 40;
    static final long O_FREE_RECID_STACK = 64;
    static final long O_FREE_DATA_STACKS = 72;
    static final int MAX_CAP_UNITS = IndexVal.CAP_MAX_UNITS;
    static final long HEAD_END = O_FREE_DATA_STACKS + 8L * MAX_CAP_UNITS; // 524336
    static final long ZERO_PAGE_LINK = HEAD_END;
    static final long ZERO_SLOTS_START = HEAD_END + 16;
    static final long RECIDS_PER_ZERO_PAGE = (PAGE_SIZE - ZERO_SLOTS_START) / 8; // 65528
    static final long RECIDS_PER_PAGE = (PAGE_SIZE - 16) / 8; // 131070

    static final int HEAD_CHECKSUM_SEED = 0x5D1B_A5E1;

    static final long LONG_STACK_PREF_SIZE = 160;
    static final long LONG_STACK_MAX_SIZE = 256;

    /** Linked chunk header: 4-byte data length + 8-byte next pointer. */
    static final int LINKED_CHUNK_HDR = 12;
    static final int MAX_CHUNK_DATA = IndexVal.MAX_CAPACITY - LINKED_CHUNK_HDR;

    /** Offsets must fit IndexVal's 44-bit field. */
    static final long MAX_VOLUME_SIZE = 1L << 44;

    // ---------- state ----------

    private final ByteBufferVol vol;

    /** Offsets of non-zero index pages, in chain order; copy-on-write, volatile published. */
    private volatile long[] indexPages = new long[0];

    private final boolean threadSafe;
    private final Lock structuralLock;
    /**
     * Outermost barrier (lock order: barrier-read → segment-write →
     * structural): every volume-mutating operation holds it SHARED for its whole
     * span; read-only operations hold it SHARED while touching the volume;
     * {@link #commit()} and {@link #close()} hold it EXCLUSIVE so the header
     * checksum is never stamped over a half-applied mutation (allocated extent
     * whose index slot is not yet published) and the volume is never closed under
     * an in-flight operation.
     */
    private final ReadWriteLock commitLock;
    private final SegmentLocks segs;
    private final DeadlockAsserts asserts;
    private volatile boolean closed = false;

    /** Set (with {@code closed}) when a faulted compaction left the volume half-rebuilt:
     *  {@code close()} then releases resources WITHOUT stamping a valid checksum, so the
     *  poisoned state can never be resealed as clean. */
    private volatile boolean poisoned = false;

    /**
     * Running total of bytes currently sitting on the free-DATA long-stacks (guarded by
     * {@code structuralLock}, published to {@link #getCurrentSize()} readers via the same
     * lock). {@code getCurrentSize() = fileTail - freeDataBytes} = allocated bytes minus
     * reclaimed free space — the approximate footprint used for byte-budget cache eviction.
     * Maintained incrementally in {@link #allocateDataLocked}/{@link #releaseDataLocked};
     * reconstructed by {@link #recomputeFreeDataBytes} on open and after {@link #compact}.
     */
    private long freeDataBytes = 0;

    // ---------- optional background maintenance (R6; DISABLED by default) ----------
    // Guarded by the intrinsic lock of this store instance. Null = maintenance disabled (all
    // fallbacks synchronous; correctness never depends on the background thread).
    private MaintenanceExecutor maintenance;
    private boolean ownsMaintenance;
    private MaintenanceExecutor.Handle compactionHandle;
    private DirectCompactionTask compactionTask;
    /** Cheap lock-free gate so the delete hot path skips the {@code synchronized} churn signal when disabled. */
    private volatile boolean maintenanceActive;
    /** Set once by close (under {@code synchronized(this)}); refuses any later start/attach so no thread leaks. */
    private boolean maintenanceShutdown;

    private static final class CompactEntry {
        final long recid;
        final boolean prealloc;
        final int capBytes;
        final byte[] content;

        CompactEntry(long recid, boolean prealloc, int capBytes, byte[] content) {
            this.recid = recid;
            this.prealloc = prealloc;
            this.capBytes = capBytes;
            this.content = content;
        }
    }

    public StoreDirect() { this(false); }

    public StoreDirect(boolean directBuffers) { this(directBuffers, true); }

    public StoreDirect(boolean directBuffers, boolean threadSafe) {
        this.vol = new ByteBufferVol(directBuffers);
        this.threadSafe = threadSafe;
        this.asserts = new DeadlockAsserts();
        this.structuralLock = asserts.structural(threadSafe ? new ReentrantLock() : Locks.NO_OP_LOCK);
        this.commitLock = threadSafe ? new java.util.concurrent.locks.ReentrantReadWriteLock() : Locks.NO_OP_RW_LOCK;
        this.segs = new SegmentLocks(SegmentLocks.DEFAULT_COUNT, threadSafe, asserts);
        initCreate();
    }

    /** Durable file-backed store (mmap volume, force + truncate). */
    public StoreDirect(File file) { this(file, true); }

    public StoreDirect(File file, boolean threadSafe) {
        this.vol = new ByteBufferVol(file);
        this.threadSafe = threadSafe;
        this.asserts = new DeadlockAsserts();
        this.structuralLock = asserts.structural(threadSafe ? new ReentrantLock() : Locks.NO_OP_LOCK);
        this.commitLock = threadSafe ? new java.util.concurrent.locks.ReentrantReadWriteLock() : Locks.NO_OP_RW_LOCK;
        this.segs = new SegmentLocks(SegmentLocks.DEFAULT_COUNT, threadSafe, asserts);
        try {
            long length = vol.length();
            if (length == 0) {
                initCreate();
            } else {
                if (length < PAGE_SIZE)
                    throw new DBException.DataCorruption("store file smaller than the header page");
                vol.ensureAvailable(PAGE_SIZE);
                initOpen();
            }
        } catch (RuntimeException e) {
            try {
                vol.close();
            } catch (RuntimeException ignore) { /* surface the original failure */ }
            throw e;
        }
    }

    @Override public boolean isThreadSafe() { return threadSafe; }

    // ---------- header init / open ----------

    private void initCreate() {
        vol.ensureAvailable(PAGE_SIZE);
        vol.putLong(0, MAGIC);
        vol.putInt(O_FEATURES, 0);
        vol.putInt(O_FEATURES + 4, 0);
        vol.putInt(O_HEAD_CHECKSUM + 4, 0);
        dataTail(0);
        maxRecid(0);
        fileTail(PAGE_SIZE);
        vol.putLong(O_FREE_RECID_STACK, Parity.p4set(0));
        for (int u = 1; u <= MAX_CAP_UNITS; u++) {
            vol.putLong(masterLinkOffset(u), Parity.p4set(0));
        }
        vol.putLong(ZERO_PAGE_LINK, Parity.p16set(0));
        vol.putInt(O_HEAD_CHECKSUM, headChecksum());
        vol.sync();
    }

    private void initOpen() {
        if (vol.length() < PAGE_SIZE)
            throw new DBException.DataCorruption("store file smaller than the header page");
        if (vol.getLong(0) != MAGIC)
            throw new DBException.DataCorruption("not a mapdb StoreDirect file (bad magic)");
        int features = vol.getInt(O_FEATURES);
        if (features != 0)
            throw new DBException("store uses unsupported feature bits: 0x" + Integer.toHexString(features));
        if (vol.getInt(O_HEAD_CHECKSUM) != headChecksum())
            throw new DBException.DataCorruption(
                    "header checksum mismatch: store was not closed cleanly or the header is corrupted");
        long ft = fileTail();
        if (ft < PAGE_SIZE || ft % PAGE_SIZE != 0)
            throw new DBException.DataCorruption("bad fileTail: " + ft);
        long physical = vol.length();
        if (physical < ft)
            throw new DBException.DataCorruption("store file truncated: length=" + physical + ", fileTail=" + ft);
        vol.ensureAvailable(ft);
        // validate the remaining header vars' parity up front
        dataTail();
        maxRecid();
        loadIndexPages(ft);
        recomputeFreeDataBytes();
    }

    private void loadIndexPages(long fileTail) {
        ArrayList<Long> pages = new ArrayList<>();
        long ptr = ZERO_PAGE_LINK;
        while (true) {
            long page = Parity.p16get(vol.getLong(ptr));
            if (page == 0) break;
            if (page % PAGE_SIZE != 0 || page >= fileTail)
                throw new DBException.DataCorruption("bad index page pointer: " + page);
            pages.add(page);
            if (pages.size() > (1 << 24)) throw new DBException.DataCorruption("index page chain loop");
            ptr = page + 8;
        }
        long[] arr = new long[pages.size()];
        for (int i = 0; i < arr.length; i++) arr[i] = pages.get(i);
        indexPages = arr;
    }

    /** Mix of every header word the allocator depends on; stamped by commit/close. */
    private int headChecksum() {
        int c = HEAD_CHECKSUM_SEED;
        for (long o = O_DATA_TAIL; o < ZERO_SLOTS_START; o += 8) {
            long v = vol.getLong(o);
            c = c * 31 + (int) (v ^ (v >>> 32));
        }
        return c;
    }

    // ---------- header variable accessors (writes require structural lock) ----------

    private long dataTail() { return Parity.p4get(vol.getLong(O_DATA_TAIL)); }

    private void dataTail(long v) { vol.putLong(O_DATA_TAIL, Parity.p4set(v)); }

    private long maxRecid() { return Parity.p4get(vol.getLong(O_MAX_RECID)) >>> 4; }

    private void maxRecid(long v) { vol.putLong(O_MAX_RECID, Parity.p4set(v << 4)); }

    private long fileTail() { return Parity.p16get(vol.getLong(O_FILE_TAIL)); }

    private void fileTail(long v) { vol.putLong(O_FILE_TAIL, Parity.p16set(v)); }

    static long masterLinkOffset(long capUnits) {
        assert capUnits >= 1 && capUnits <= MAX_CAP_UNITS;
        return O_FREE_DATA_STACKS + 8 * (capUnits - 1);
    }

    // ---------- recid index ----------

    /** Volume offset of the recid's index slot, or -1 when its index page does not exist. */
    long recidToOffset(long recid) {
        long r0 = recid - 1;
        if (r0 < RECIDS_PER_ZERO_PAGE) return ZERO_SLOTS_START + r0 * 8;
        r0 -= RECIDS_PER_ZERO_PAGE;
        long page = r0 / RECIDS_PER_PAGE;
        long[] pages = indexPages;
        if (page >= pages.length) return -1;
        return pages[(int) page] + 16 + (r0 % RECIDS_PER_PAGE) * 8;
    }

    /** Raw (parity1-encoded) index slot; 0 when never allocated / out of range. */
    private long rawIndexGet(long recid) {
        if (recid < 1) return 0;
        long off = recidToOffset(recid);
        if (off < 0) return 0;
        try {
            return vol.getLong(off);
        } catch (IndexOutOfBoundsException e) {
            // volume shrank under us (racing close): same "does not exist" answer
            // the heap-index prototype gave; locked paths re-check closed state
            return 0;
        }
    }

    private static boolean ivParityOk(long iv) {
        return (Long.bitCount(iv) & 1) == 1;
    }

    /** Index read that enforces the record-state contract: throws GetVoid on N and D states. */
    private long indexGetChecked(long recid) {
        long iv = rawIndexGet(recid);
        if (iv == 0) throw new DBException.GetVoid(recid);
        if (!ivParityOk(iv))
            throw new DBException.DataCorruption("index slot parity broken, recid=" + recid);
        if (IndexVal.capUnits(iv) == IndexVal.CAP_DELETED) throw new DBException.GetVoid(recid);
        return iv;
    }

    private void indexSet(long recid, long iv) {
        long off = recidToOffset(recid);
        assert off > 0 : "index slot not allocated for recid " + recid;
        vol.putLong(off, Parity.p1set(iv));
    }

    /** structuralLock must be held. Allocates index pages until {@code recid} has a slot. */
    private void ensureIndexCapacityLocked(long recid) {
        while (recidToOffset(recid) < 0) allocateNewIndexPageLocked();
    }

    /** structuralLock must be held. */
    private void allocateNewIndexPageLocked() {
        long page = allocateNewPageLocked();
        // index slots must read 0 until written; pages past a crash-truncated tail
        // could hold garbage, and clearing is cheap relative to 131k new recids
        vol.clear(page, page + PAGE_SIZE);
        vol.putLong(page + 8, Parity.p16set(0));
        long[] pages = indexPages;
        long ptr = pages.length == 0 ? ZERO_PAGE_LINK : pages[pages.length - 1] + 8;
        vol.putLong(ptr, Parity.p16set(page));
        long[] grown = Arrays.copyOf(pages, pages.length + 1);
        grown[grown.length - 1] = page;
        indexPages = grown;
    }

    // ---------- allocator ----------

    /** structuralLock must be held. */
    private long allocateNewPageLocked() {
        long eof = fileTail();
        long newEof = eof + PAGE_SIZE;
        if (newEof > MAX_VOLUME_SIZE)
            throw new DBException.StoreFull("volume exceeds the 44-bit offset limit");
        vol.ensureAvailable(newEof);
        fileTail(newEof);
        return eof;
    }

    /** structuralLock must be held. */
    private long allocRecidLocked() {
        long v = longStackTake(O_FREE_RECID_STACK);
        if (v != 0) return Parity.p1get(v) >>> 1;
        long recid = maxRecid() + 1;
        ensureIndexCapacityLocked(recid);
        maxRecid(recid);
        return recid;
    }

    /** structuralLock must be held. */
    private void freeRecidLocked(long recid) {
        longStackPut(O_FREE_RECID_STACK, Parity.p1set(recid << 1));
    }

    /**
     * structuralLock must be held. {@code capBytes} is 16-aligned, within
     * [16, MAX_CAPACITY]. With {@code recursive} (long-stack chunk allocation)
     * the free lists are not consulted and the request is guaranteed by the
     * caller to fit the current data page, so no nested stack mutation happens.
     */
    private long allocateDataLocked(long capBytes, boolean recursive) {
        assert (capBytes & 15) == 0 && capBytes >= 16 && capBytes <= IndexVal.MAX_CAPACITY;
        if (!recursive) {
            long v = longStackTake(masterLinkOffset(capBytes / 16));
            if (v != 0) {
                freeDataBytes -= capBytes; // extent leaves the free list
                return Parity.p1get(v) << 3;
            }
        }
        long tail = dataTail();
        if (tail == 0) {
            long page = allocateNewPageLocked();
            advanceDataTail(page, capBytes);
            return page;
        }
        if ((tail % PAGE_SIZE) + capBytes <= PAGE_SIZE) {
            advanceDataTail(tail, capBytes);
            return tail;
        }
        // would cross a page boundary: place the record on a fresh page FIRST, then
        // donate the old remainder — so free-list bookkeeping (a possible new stack
        // chunk) lands in the new page's tail instead of hijacking a page of its own
        assert !recursive : "chunk allocation must fit the current page";
        long rem = PAGE_SIZE - (tail % PAGE_SIZE);
        long page = allocateNewPageLocked();
        advanceDataTail(page, capBytes);
        releaseDataLocked(rem, tail);
        return page;
    }

    /** structuralLock must be held; resets dataTail to 0 on an exactly-filled page. */
    private void advanceDataTail(long start, long capBytes) {
        long newTail = start + capBytes;
        dataTail(newTail % PAGE_SIZE == 0 ? 0 : newTail);
    }

    /** structuralLock must be held. {@code sizeBytes} 16-aligned, {@code offset} 16-aligned. */
    private void releaseDataLocked(long sizeBytes, long offset) {
        assert (sizeBytes & 15) == 0 && sizeBytes >= 16 && sizeBytes / 16 <= MAX_CAP_UNITS;
        assert (offset & 15) == 0 && offset >= PAGE_SIZE;
        longStackPut(masterLinkOffset(sizeBytes / 16), Parity.p1set(offset >>> 3));
        freeDataBytes += sizeBytes; // extent joins the free list

    }

    // ---------- long stacks (structural lock held throughout) ----------

    private static int packLongSize(long v) {
        int c = 1;
        while ((v >>>= 7) != 0) c++;
        return c;
    }

    /** DataOutput2.packLong framing: MSB-first 7-bit groups, 0x80 on the last byte. */
    private int putPackedLong(long offset, long v) {
        int size = packLongSize(v);
        int shift = (size - 1) * 7;
        long p = offset;
        while (shift > 0) {
            vol.putByte(p++, (int) ((v >>> shift) & 0x7F));
            shift -= 7;
        }
        vol.putByte(p, (int) (v & 0x7F) | 0x80);
        return size;
    }

    private long getPackedLong(long offset) {
        long ret = 0;
        for (int i = 0; i < 10; i++) {
            int b = vol.getUnsignedByte(offset + i);
            ret = (ret << 7) | (b & 0x7F);
            if ((b & 0x80) != 0) return ret;
        }
        throw new DBException.DataCorruption("unterminated packed long at " + offset);
    }

    private void longStackPut(long masterLinkOffset, long value) {
        assert value != 0 && (value >>> 48) == 0;
        long master = Parity.p4get(vol.getLong(masterLinkOffset));
        if (master == 0) {
            longStackNewChunk(masterLinkOffset, 0, value);
            return;
        }
        long chunkOffset = master & IndexVal.MOFFSET;
        long currPos = master >>> 48;
        long chunkSize = Parity.p4get(vol.getLong(chunkOffset)) >>> 48;
        int valueSize = packLongSize(value);
        if (currPos + valueSize > chunkSize) {
            longStackNewChunk(masterLinkOffset, chunkOffset, value);
            return;
        }
        putPackedLong(chunkOffset + currPos, value);
        vol.putLong(masterLinkOffset, Parity.p4set(((currPos + valueSize) << 48) | chunkOffset));
    }

    /**
     * Pushes a fresh chunk on top of the stack. The chunk size is chosen to ALWAYS
     * fit the current data page remainder, so the nested {@code allocateDataLocked}
     * can never recurse into another stack mutation (the reentrant-put lost-chunk
     * hazard mapdb3 only asserts against in paranoid mode is structurally excluded).
     */
    private void longStackNewChunk(long masterLinkOffset, long prevChunkOffset, long value) {
        long tail = dataTail();
        long chunkSize;
        if (tail == 0) {
            chunkSize = LONG_STACK_PREF_SIZE;
        } else {
            long rem = PAGE_SIZE - (tail % PAGE_SIZE);
            chunkSize = Math.min(rem, LONG_STACK_PREF_SIZE);
        }
        int valueSize = packLongSize(value);
        assert 8 + valueSize <= chunkSize : "packed value too large for a minimal chunk";
        long chunkOffset = allocateDataLocked(chunkSize, true);
        vol.clear(chunkOffset, chunkOffset + chunkSize); // zero tails delimit the value area
        vol.putLong(chunkOffset, Parity.p4set((chunkSize << 48) | prevChunkOffset));
        putPackedLong(chunkOffset + 8, value);
        vol.putLong(masterLinkOffset, Parity.p4set(((8L + valueSize) << 48) | chunkOffset));
    }

    /** Pops the most recent value (raw, still parity1-encoded), or 0 when empty. */
    private long longStackTake(long masterLinkOffset) {
        long master = Parity.p4get(vol.getLong(masterLinkOffset));
        if (master == 0) return 0;
        long chunkOffset = master & IndexVal.MOFFSET;
        long pos = Math.max((master >>> 48) - 1, 8);
        while (pos > 8 && (vol.getUnsignedByte(chunkOffset + pos - 1) & 0x80) == 0) pos--;
        long value = getPackedLong(chunkOffset + pos);
        vol.clear(chunkOffset + pos, chunkOffset + pos + packLongSize(value));
        if (pos > 8) {
            vol.putLong(masterLinkOffset, Parity.p4set((pos << 48) | chunkOffset));
            return value;
        }
        // chunk emptied: relink master to the previous chunk, then free this one
        long hdr = Parity.p4get(vol.getLong(chunkOffset));
        long chunkSize = hdr >>> 48;
        long prevChunkOffset = hdr & IndexVal.MOFFSET;
        long prevPos = 0;
        if (prevChunkOffset != 0) {
            long prevSize = Parity.p4get(vol.getLong(prevChunkOffset)) >>> 48;
            prevPos = longStackFindEnd(prevChunkOffset, prevSize);
        }
        vol.putLong(masterLinkOffset, Parity.p4set((prevPos << 48) | prevChunkOffset));
        releaseDataLocked(chunkSize, chunkOffset);
        return value;
    }

    /** End of the live value area in a non-top chunk: trims the zeroed tail. */
    private long longStackFindEnd(long chunkOffset, long pos) {
        while (pos > 8 && vol.getUnsignedByte(chunkOffset + pos - 1) == 0) pos--;
        return pos;
    }

    // ---------- helpers ----------

    private void checkClosed() {
        if (closed) throw new DBException.StoreClosed();
    }

    /**
     * Enter a volume-mutating operation: shared barrier + closed re-check.
     * A mutator that raced {@link #close()} and lost must not touch the volume.
     */
    private void mutateEnter() {
        commitLock.readLock().lock();
        if (closed) {
            commitLock.readLock().unlock();
            throw new DBException.StoreClosed();
        }
    }

    private void mutateExit() {
        commitLock.readLock().unlock();
    }

    private static <R> DataOutput2 serialize(R record, Serializer<R> ser) {
        DataOutput2 out = new DataOutput2(Math.max(16, ser.sizeHint() + 4));
        ser.serialize(out, record);
        return out;
    }

    /** Long math on purpose: {@code 4 + len + headroom} must never wrap an int. */
    private static void checkSize(long capBytes) {
        if (capBytes < 0 || capBytes > IndexVal.MAX_CAPACITY) throw new DBException.RecordTooLarge(capBytes);
    }

    /** Rounded-up capacity for a payload+header size, validated against MAX_CAPACITY. */
    private static int capBytesFor(long need) {
        long rounded = (need + 15) & ~15L;
        checkSize(rounded);
        return (int) rounded;
    }

    private static boolean needsLinked(long contentLen) {
        return 4L + contentLen > IndexVal.MAX_CAPACITY;
    }

    /** Allocate data area + write content + set index. Segment write lock must be held. */
    private void writeNewData(long recid, byte[] buf, int len, int capBytes, int flags) {
        long off;
        structuralLock.lock();
        try {
            off = allocateDataLocked(capBytes, false);
        } finally {
            structuralLock.unlock();
        }
        vol.putInt(off, len);
        vol.putData(off + 4, buf, 0, len);
        indexSet(recid, IndexVal.compose(capBytes / 16, off, flags));
    }

    /**
     * Writes an oversize record as a linked chunk chain (tail-first, so every next
     * pointer is known when its chunk is written) and points the index at the root.
     * Segment write lock must be held.
     */
    private void writeNewLinked(long recid, byte[] buf, int len) {
        assert needsLinked(len);
        long tailData = len % MAX_CHUNK_DATA;
        if (tailData == 0) tailData = MAX_CHUNK_DATA;
        long pos = len - tailData;
        long chunkDataLen = tailData;
        long nextPtr = Parity.p1set(0); // tail terminator
        while (true) {
            int capBytes = capBytesFor(LINKED_CHUNK_HDR + chunkDataLen);
            long off;
            structuralLock.lock();
            try {
                off = allocateDataLocked(capBytes, false);
            } finally {
                structuralLock.unlock();
            }
            vol.putInt(off, (int) chunkDataLen);
            vol.putLong(off + 4, nextPtr);
            vol.putData(off + LINKED_CHUNK_HDR, buf, (int) pos, (int) chunkDataLen);
            if (pos == 0) {
                indexSet(recid, IndexVal.compose(capBytes / 16, off, IndexVal.FLAG_LINKED));
                return;
            }
            nextPtr = Parity.p1set(((long) (capBytes / 16) << 48) | off);
            chunkDataLen = MAX_CHUNK_DATA;
            pos -= MAX_CHUNK_DATA;
        }
    }

    /** Walks a linked chain; returns {offset, dataLen, capBytes} per chunk. Lock must be held. */
    private ArrayList<long[]> linkedChain(long iv) {
        ArrayList<long[]> chunks = new ArrayList<>();
        long capUnits = IndexVal.capUnits(iv);
        long off = IndexVal.offset(iv);
        long total = 0;
        while (true) {
            long capBytes = capUnits * 16;
            int len = vol.getInt(off);
            if (len < 0 || LINKED_CHUNK_HDR + len > capBytes)
                throw new DBException.DataCorruption("linked chunk length out of range at " + off);
            chunks.add(new long[]{off, len, capBytes});
            total += len;
            if (total > Integer.MAX_VALUE || chunks.size() > (1 << 22))
                throw new DBException.DataCorruption("linked chain too long");
            long next = Parity.p1get(vol.getLong(off + 4));
            if (next == 0) break;
            capUnits = next >>> 48;
            off = next & IndexVal.MOFFSET;
            if (capUnits < 1 || capUnits > MAX_CAP_UNITS || off < PAGE_SIZE)
                throw new DBException.DataCorruption("bad linked chunk pointer at " + off);
        }
        return chunks;
    }

    /** Current content length for any live record shape. Segment lock must be held. */
    private long currentContentLen(long iv) {
        int cap = IndexVal.capUnits(iv);
        if (cap == IndexVal.CAP_NULL) return 0;
        if (IndexVal.isLinked(iv)) {
            long total = 0;
            for (long[] c : linkedChain(iv)) total += c[1];
            return total;
        }
        return vol.getInt(IndexVal.offset(iv));   // hidden 4-byte used-length prefix
    }

    /** Assembles a linked record's full content. Segment lock must be held. */
    private byte[] linkedGet(long iv) {
        ArrayList<long[]> chunks = linkedChain(iv);
        long total = 0;
        for (long[] c : chunks) total += c[1];
        byte[] out = new byte[(int) total];
        int p = 0;
        for (long[] c : chunks) {
            vol.getData(c[0] + LINKED_CHUNK_HDR, out, p, (int) c[1]);
            p += (int) c[1];
        }
        return out;
    }

    /** Segment write lock must be held. Frees data area of iv if it has one. */
    private void releaseOldData(long iv) {
        int cap = IndexVal.capUnits(iv);
        if (cap == IndexVal.CAP_NULL || cap == IndexVal.CAP_DELETED) return;
        if (IndexVal.isLinked(iv)) {
            ArrayList<long[]> chunks = linkedChain(iv);
            structuralLock.lock();
            try {
                for (long[] c : chunks) releaseDataLocked(c[2], c[0]);
            } finally {
                structuralLock.unlock();
            }
        } else {
            structuralLock.lock();
            try {
                releaseDataLocked(cap * 16L, IndexVal.offset(iv));
            } finally {
                structuralLock.unlock();
            }
        }
    }

    // ---------- Store ----------

    @Override public long preallocate() {
        checkClosed();
        mutateEnter();
        try {
            return preallocateInner();
        } finally {
            mutateExit();
        }
    }

    private long preallocateInner() {
        long recid;
        structuralLock.lock();
        try {
            recid = allocRecidLocked();
        } finally {
            structuralLock.unlock();
        }
        ReadWriteLock lock = segs.forRecid(recid);
        lock.writeLock().lock();
        try {
            indexSet(recid, IndexVal.compose(IndexVal.CAP_NULL, 0, IndexVal.FLAG_PREALLOC));
        } finally {
            lock.writeLock().unlock();
        }
        return recid;
    }

    @Override public <R> long put(R record, Serializer<R> serializer) {
        checkClosed();
        DataOutput2 out = record == null ? null : serialize(record, serializer);
        boolean linked = out != null && needsLinked(out.pos);
        int capBytes = out == null || linked ? 0 : capBytesFor(4L + out.pos);
        mutateEnter();
        try {
            return putInner(out, linked, capBytes);
        } finally {
            mutateExit();
        }
    }

    private <R> long putInner(DataOutput2 out, boolean linked, int capBytes) {
        long recid;
        structuralLock.lock();
        try {
            recid = allocRecidLocked();
        } finally {
            structuralLock.unlock();
        }
        ReadWriteLock lock = segs.forRecid(recid);
        lock.writeLock().lock();
        try {
            if (out == null) indexSet(recid, IndexVal.compose(IndexVal.CAP_NULL, 0, 0));
            else if (linked) writeNewLinked(recid, out.buf, out.pos);
            else writeNewData(recid, out.buf, out.pos, capBytes, 0);
        } finally {
            lock.writeLock().unlock();
        }
        return recid;
    }

    @Override public <R> R get(long recid, Serializer<R> serializer) {
        mutateEnter();
        try {
            ReadWriteLock lock = segs.forRecid(recid);
            lock.readLock().lock();
            try {
                long iv = indexGetChecked(recid);
                if (IndexVal.capUnits(iv) == IndexVal.CAP_NULL) return null;
                if (IndexVal.isLinked(iv)) {
                    byte[] b = linkedGet(iv);
                    return serializer.deserialize(new DataInput2.ByteArray(b, 0), b.length);
                }
                long off = IndexVal.offset(iv);
                int used = vol.getInt(off);
                return serializer.deserialize(vol.dataInput(off + 4, used), used);
            } finally {
                lock.readLock().unlock();
            }
        } finally {
            mutateExit();
        }
    }

    /**
     * Optimistic-first read (seqlock over the segment StampedLock).
     * The hot path never writes the lock's state word: load the stamp, run the
     * action over the bytes, re-validate. If any writer touched the segment in
     * between, every intermediate result is discarded and the read retries under
     * the plain read lock — so the action may observe TORN BYTES on a lost race.
     * That is safe because (a) actions are re-invocable and side-effect-free
     * ({@link RecordRead}), (b) all byte access is bounds-checked
     * (ByteBuffer) so garbage lengths end in a runtime exception, caught here,
     * never memory corruption, and (c) the result of a non-validated attempt is
     * never returned. GetVoid/onNull decisions are likewise only acted on after
     * the stamp validates. Linked records always use the locked path (multi-chunk
     * assembly is not a single-slice zero-copy read).
     */
    @Override public long read(long recid, RecordRead action) {
        mutateEnter();
        try {
            java.util.concurrent.locks.StampedLock sl = segs.stampedForRecid(recid);
            if (sl != null && !closed) {
                long stamp = sl.tryOptimisticRead();
                if (stamp != 0) {
                    long iv = rawIndexGet(recid);
                    int cap = IndexVal.capUnits(iv);
                    if (iv != 0 && !ivParityOk(iv)) {
                        // torn concurrent write or real corruption: the locked retry
                        // below distinguishes them (indexGetChecked -> DataCorruption)
                    } else if (iv == 0 || cap == IndexVal.CAP_DELETED) {
                        if (sl.validate(stamp)) throw new DBException.GetVoid(recid);
                    } else if (cap == IndexVal.CAP_NULL) {
                        if (sl.validate(stamp)) {
                            asserts.enterAction();
                            try {
                                return action.onNull();
                            } finally {
                                asserts.exitAction();
                            }
                        }
                    } else if (!IndexVal.isLinked(iv)) {
                        long off = IndexVal.offset(iv);
                        asserts.enterAction();
                        try {
                            int used = vol.getInt(off);
                            if (used >= 0 && (off & ByteBufferVol.SLICE_MASK) + 4 + used <= ByteBufferVol.SLICE_SIZE) {
                                long ret = action.onBytes(vol.dataInput(off + 4, used), used);
                                if (sl.validate(stamp)) return ret;
                            }
                        } catch (Throwable torn) {
                            // action crashed on torn bytes (racing same-segment writer);
                            // if it is a genuine failure the locked retry throws it again
                        } finally {
                            asserts.exitAction();
                        }
                    }
                }
            }
            return readLocked(recid, action);
        } finally {
            mutateExit();
        }
    }

    private long readLocked(long recid, RecordRead action) {
        ReadWriteLock lock = segs.forRecid(recid);
        lock.readLock().lock();
        try {
            checkClosed();
            long iv = indexGetChecked(recid);
            if (IndexVal.capUnits(iv) == IndexVal.CAP_NULL) return action.onNull();
            if (IndexVal.isLinked(iv)) {
                byte[] b = linkedGet(iv);
                return action.onBytes(new DataInput2.ByteArray(b, 0), b.length);
            }
            long off = IndexVal.offset(iv);
            int used = vol.getInt(off);
            return action.onBytes(vol.dataInput(off + 4, used), used);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override public <R> void update(long recid, R record, Serializer<R> serializer) {
        updateWithHeadroom(recid, record, serializer, 0);
    }

    @Override public <R> void updateWithHeadroom(long recid, R record, Serializer<R> serializer, int headroom) {
        checkClosed();
        if (headroom < 0) throw new IllegalArgumentException("negative headroom: " + headroom);
        DataOutput2 out = record == null ? null : serialize(record, serializer);
        // content that fits a plain record must also fit with its headroom
        // (RecordTooLarge otherwise — linked records take no appends, so silently
        // going linked would break the headroom-appendability guarantee);
        // oversize content is stored linked and the check does not apply
        if (out != null && !needsLinked(out.pos)) capBytesFor(4L + out.pos + headroom);
        mutateEnter();
        try {
            ReadWriteLock lock = segs.forRecid(recid);
            lock.writeLock().lock();
            try {
                updateLocked(recid, out, headroom);
            } finally {
                lock.writeLock().unlock();
            }
        } finally {
            mutateExit();
        }
    }

    /** Segment write lock must be held. */
    private void updateLocked(long recid, DataOutput2 out, int headroom) {
        long iv = indexGetChecked(recid);
        int oldCap = IndexVal.capUnits(iv);
        if (out == null) {
            releaseOldData(iv);
            indexSet(recid, IndexVal.compose(IndexVal.CAP_NULL, 0, 0));
            return;
        }
        if (needsLinked(out.pos)) {
            releaseOldData(iv);
            writeNewLinked(recid, out.buf, out.pos);
            return;
        }
        long need = 4L + out.pos + headroom;
        if (!IndexVal.isLinked(iv) && oldCap != IndexVal.CAP_NULL && need <= oldCap * 16L) {
            // in-place: capacity retained; a fill of a preallocated recid never lands
            // here (P has CAP_NULL) so this is always a genuine rewrite
            long off = IndexVal.offset(iv);
            vol.putInt(off, out.pos);
            vol.putData(off + 4, out.buf, 0, out.pos);
            indexSet(recid, IndexVal.compose(oldCap, off, 0));
        } else {
            releaseOldData(iv);
            writeNewData(recid, out.buf, out.pos, capBytesFor(need), 0);
        }
    }

    @Override public <R> boolean compareAndSwap(long recid, R expectedOldRecord, R newRecord, Serializer<R> serializer) {
        checkClosed();
        mutateEnter();
        try {
            return casInner(recid, expectedOldRecord, newRecord, serializer);
        } finally {
            mutateExit();
        }
    }

    private <R> boolean casInner(long recid, R expectedOldRecord, R newRecord, Serializer<R> serializer) {
        ReadWriteLock lock = segs.forRecid(recid);
        lock.writeLock().lock();
        try {
            long iv = indexGetChecked(recid);
            R current = null;
            if (IndexVal.capUnits(iv) != IndexVal.CAP_NULL) {
                if (IndexVal.isLinked(iv)) {
                    byte[] b = linkedGet(iv);
                    current = serializer.deserialize(new DataInput2.ByteArray(b, 0), b.length);
                } else {
                    long off = IndexVal.offset(iv);
                    int used = vol.getInt(off);
                    current = serializer.deserialize(vol.dataInput(off + 4, used), used);
                }
            }
            boolean eq = (current == null && expectedOldRecord == null)
                    || (current != null && expectedOldRecord != null && serializer.equals(current, expectedOldRecord));
            if (!eq) return false;
            updateLocked(recid, newRecord == null ? null : serialize(newRecord, serializer), 0);
            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override public <R> void delete(long recid, Serializer<R> serializer) {
        checkClosed();
        mutateEnter();
        try {
            deleteInner(recid);
        } finally {
            mutateExit();
        }
    }

    private void deleteInner(long recid) {
        ReadWriteLock lock = segs.forRecid(recid);
        lock.writeLock().lock();
        try {
            long iv = indexGetChecked(recid);
            releaseOldData(iv);
            structuralLock.lock();
            try {
                freeRecidLocked(recid);
            } finally {
                structuralLock.unlock();
            }
            indexSet(recid, IndexVal.compose(IndexVal.CAP_DELETED, 0, 0));
        } finally {
            lock.writeLock().unlock();
        }
        signalCompactionChurn(); // deletes free data/recids -> hint the background compactor (P7; no-op if disabled)
    }

    // ---------- StoreDelta ----------

    @Override public long append(long recid, byte[] data, int offset, int len) {
        checkClosed();
        mutateEnter();
        try {
            return appendInner(recid, data, offset, len);
        } finally {
            mutateExit();
        }
    }

    private long appendInner(long recid, byte[] data, int offset, int len) {
        ReadWriteLock lock = segs.forRecid(recid);
        lock.writeLock().lock();
        try {
            long iv = indexGetChecked(recid);
            if (len == 0) {
                // Appending nothing cannot fail and cannot need capacity, so it is a no-op
                // returning the current size — for EVERY record shape, including linked and
                // null. Refusing here would make the zero-length case shape-dependent, and
                // StoreWAL would then stage an empty append that this method later refuses
                // at commit-apply (an AssertionError in the writer).
                return currentContentLen(iv);
            }
            if (IndexVal.isLinked(iv)) return StoreDelta.REFUSED; // consolidation via update
            if (IndexVal.capUnits(iv) == IndexVal.CAP_NULL) {
                // first append establishes the record: capacity == exactly what is needed
                if (needsLinked(len)) {
                    writeNewLinked(recid, dataSlice(data, offset, len), len);
                    return len;
                }
                int capBytes = capBytesFor(4L + len);
                writeNewData(recid, dataSlice(data, offset, len), len, capBytes, 0);
                return len;
            }
            long off = IndexVal.offset(iv);
            int capBytes = IndexVal.capUnits(iv) * 16;
            int used = vol.getInt(off);
            if (4 + used + len > capBytes) return StoreDelta.REFUSED;
            vol.putData(off + 4 + used, data, offset, len);
            vol.putInt(off, used + len);
            return used + len;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private static byte[] dataSlice(byte[] data, int offset, int len) {
        if (offset == 0 && len == data.length) return data;
        return Arrays.copyOfRange(data, offset, offset + len);
    }

    @Override public long capacityRemaining(long recid) {
        mutateEnter();
        try {
            ReadWriteLock lock = segs.forRecid(recid);
            lock.readLock().lock();
            try {
                long iv = indexGetChecked(recid);
                if (IndexVal.capUnits(iv) == IndexVal.CAP_NULL) return 0;
                if (IndexVal.isLinked(iv)) return 0; // appends on linked records are always REFUSED
                long off = IndexVal.offset(iv);
                return IndexVal.capUnits(iv) * 16L - 4 - vol.getInt(off);
            } finally {
                lock.readLock().unlock();
            }
        } finally {
            mutateExit();
        }
    }

    // ---------- lifecycle ----------

    /**
     * Two-phase durability point: force the data slices first, then stamp the header
     * checksum and force the header page. The order is load-bearing — a single sync that
     * flushes the freshly stamped header before the data slices leaves a crash window
     * where the store reopens false-clean (checksum matches, committed data never hit disk).
     */
    private void stampHeaderDurable() {
        vol.sync();
        vol.putInt(O_HEAD_CHECKSUM, headChecksum());
        vol.syncHeader();
    }

    /** Stamps the header checksum and syncs the volume: durable-of-last-commit. */
    @Override public void commit() {
        checkClosed();
        commitLock.writeLock().lock(); // exclusive: no half-applied mutation under the stamp
        try {
            checkClosed();
            stampHeaderDurable();
        } finally {
            commitLock.writeLock().unlock();
        }
    }

    /**
     * Stop-the-world copy compaction: snapshot every index-visible record, rebuild
     * the recid index and allocator metadata from an empty header, copy live data
     * into fresh dense extents, then truncate pages no longer referenced.
     */
    @Override public void compact() {
        checkClosed();
        commitLock.writeLock().lock();
        try {
            checkClosed();
            ArrayList<CompactEntry> entries = compactSnapshot();
            long max = maxRecid();

            try {
                // 0) CRASH BARRIER (same as executeCompaction step 0): the rebuild below reuses
                // extents in place with no rollback, so a hard crash mid-rebuild can leave data
                // pages written back while the header page is not — reopening FALSE-CLEAN under
                // the last commit's still-valid checksum over clobbered extents. Invalidate the
                // on-disk checksum and make that DURABLE before touching anything; the final
                // stampHeaderDurable() below is what re-accepts the store.
                vol.putInt(O_HEAD_CHECKSUM, ~headChecksum());
                vol.sync();

                structuralLock.lock();
                try {
                    dataTail(0);
                    fileTail(PAGE_SIZE);
                    vol.putLong(O_FREE_RECID_STACK, Parity.p4set(0));
                    for (int u = 1; u <= MAX_CAP_UNITS; u++) {
                        vol.putLong(masterLinkOffset(u), Parity.p4set(0));
                    }
                    vol.putLong(ZERO_PAGE_LINK, Parity.p16set(0));
                    vol.clear(ZERO_SLOTS_START, PAGE_SIZE);
                    freeDataBytes = 0; // free-data stacks emptied; writeNew* re-donate remainders
                    indexPages = new long[0];
                    maxRecid(max);
                    if (max > 0) ensureIndexCapacityLocked(max);
                } finally {
                    structuralLock.unlock();
                }

                Runnable hook = testCompactCrashHook;
                for (CompactEntry e : entries) {
                    if (hook != null) hook.run();
                    if (e.content == null) {
                        indexSet(e.recid, IndexVal.compose(IndexVal.CAP_NULL, 0,
                                e.prealloc ? IndexVal.FLAG_PREALLOC : 0));
                    } else if (needsLinked(e.content.length)) {
                        writeNewLinked(e.recid, e.content, e.content.length);
                    } else {
                        writeNewData(e.recid, e.content, e.content.length, e.capBytes, 0);
                    }
                }
                rebuildFreeRecidsInner();
                stampHeaderDurable();
                vol.truncate(fileTail());
            } catch (Throwable t) {
                // The store is half-rebuilt with no rollback. Poison it: a later ordinary
                // close() must NOT stamp a valid checksum over this state (silent data loss
                // on reopen). Refusing reopen matches the incremental compactor's no-undo
                // crash contract.
                poisoned = true;
                closed = true;
                try {
                    vol.putInt(O_HEAD_CHECKSUM, ~headChecksum());
                    vol.sync();
                } catch (Throwable ignored) {
                    // best-effort: the in-memory poisoned flag alone already blocks close()'s stamp
                }
                throw t;
            }
        } finally {
            commitLock.writeLock().unlock();
        }
    }

    // ---------- incremental compaction ----------
    //
    // Bounded, structure-aware relocation compactor: instead of rebuilding the WHOLE
    // store (as compact() does), compactStep() reclaims a bounded run of TRAILING DATA
    // pages by relocating their live records into free space in the retained region,
    // then truncating. The whole step runs under the EXCLUSIVE commitLock — coarse but
    // BOUNDED (one step touches at most `maxPages` pages of live data), so the store is
    // fully available BETWEEN steps, unlike full compact() which blocks for the entire
    // rebuild. Correctness/crash-safety are prioritized over concurrency (there is no
    // maintenance-executor R6 and no op-log page consolidation R1 yet — both deferred).
    //
    // Relocation protocol (per record): write the new copy at its planned target, then
    // FLIP the recid index slot (a single atomic parity1 putLong) to point at the new
    // offset, then the old extent is reclaimed by the free-list rebuild / truncate. The
    // index re-point never changes the recid, so — even though the exclusive barrier
    // means no reader is in flight during a step — the write-new-then-flip ordering is
    // preserved as the primitive's contract (write bytes are durable before any pointer
    // to them is published).
    //
    // Crash safety follows StoreDirect's checksum model exactly like compact(): the whole
    // step mutates the (possibly mmap'd) volume freely, then — and only at the very end,
    // under the exclusive lock — stamps the header checksum, syncs, and truncates. A crash
    // before the final stamp leaves a checksum mismatch, so reopen REFUSES (no partial
    // state is ever accepted); a completed step is the first accepted state. There is no
    // window where a crash yields an accepted-but-inconsistent store.
    //
    // The free lists (both free-data size-class stacks and the free-recid stack) are
    // rebuilt from scratch over the retained region rather than surgically edited: the
    // ENTIRE final retained-region tiling (live extents + relocation targets + rebuilt
    // long-stack chunks + free-data extents) is planned IN MEMORY and validated to tile
    // every retained data page exactly once BEFORE any volume write. If planning cannot
    // produce an exact tiling (e.g. too little retained free space to hold the relocated
    // records plus the rebuilt free-list metadata) it returns null and the step retains
    // one more page and retries; worst case it reclaims nothing and returns 0. A planning
    // bug therefore degrades to a no-op, never to corruption.
    //
    // Compaction tail: the drop region may also include INDEX pages and pages holding LINKED
    // (oversize) chunks. Both are relocated with the same write-copy-then-flip
    // primitive folded into the plan->validate->write pipeline:
    //   * Index-page relocation (IndexPageMove): copy a whole 1 MiB page to a lower retained page
    //     that holds no live extent, rewrite the index-page chain links, publish the indexPages
    //     mirror. recids are unchanged (recidToOffset resolves by ordinal). Target pages are
    //     EXCLUDED from data/free tiling (retainedIndexPagesAfterMove). No free target -> no-op.
    //   * Linked-chunk relocation (LinkedChainMove): copy each moved chunk, rewrite the chain's
    //     next pointers to final offsets (head via the index slot, interior via the predecessor's
    //     +4), preserving FLAG_LINKED and per-chunk capacity (never re-planed to a plain record).
    // Two PRE-WRITE content validators (validateIndexPagesAfterMove / validateLinkedChainsAfterMove)
    // plus the exact-tiling validator run BEFORE any volume write, so a planning bug degrades to a
    // NO-OP (return null) rather than corruption. verify() stays the public double-use oracle.

    /** Default per-step page budget for {@link #compactIncremental()}. */
    public static final int DEFAULT_COMPACT_STEP_PAGES = 4;

    /** A plain record that must move off a dropped page: relocate whole (capacity preserved). */
    private static final class Move {
        final long recid;
        final long oldOffset;
        final int capUnits;   // capacity preserved across the move
        final int copyBytes;  // 4 + used: header+content actually copied
        final int flags;      // preserved (archive bit etc.)
        long newOffset = -1;
        Move(long recid, long oldOffset, int capUnits, int copyBytes, int flags) {
            this.recid = recid; this.oldOffset = oldOffset; this.capUnits = capUnits;
            this.copyBytes = copyBytes; this.flags = flags;
        }
    }

    /** One rebuilt long-stack chunk (16-aligned, <=256B): header + packed values, zero tail. */
    private static final class ChunkDesc {
        long offset = -1;
        final int chunkSize;
        final long[] values; // raw parity-encoded, in push order (index 0 = oldest in chunk)
        ChunkDesc(int chunkSize, long[] values) { this.chunkSize = chunkSize; this.values = values; }
    }

    /** A rebuilt long stack: master link offset + its chunk chain (index 0 = bottom). */
    private static final class StackDesc {
        final long masterLinkOffset;
        final ArrayList<ChunkDesc> chain;
        StackDesc(long masterLinkOffset, ArrayList<ChunkDesc> chain) {
            this.masterLinkOffset = masterLinkOffset; this.chain = chain;
        }
    }

    /**
     * A whole 1 MiB index page relocated to a lower page-aligned target.
     * The move preserves the page's ordinal position (recids are unchanged: {@link #recidToOffset}
     * resolves by ordinal), copies all 65528/131070 slots plus the reserved header {@code [0..16)},
     * and re-links the index-page chain to the new offset. {@code newPage} is a whole retained
     * data page that holds no retained live extent (see planning) and is excluded from data tiling.
     */
    private static final class IndexPageMove {
        final int ordinal;   // position in the indexPages mirror / chain
        final long oldPage;  // current (drop-region) page offset
        final long newPage;  // retained page-aligned target
        IndexPageMove(int ordinal, long oldPage, long newPage) {
            this.ordinal = ordinal; this.oldPage = oldPage; this.newPage = newPage;
        }
    }

    /**
     * One linked (oversize) record whose chain has at least one chunk in the drop region.
     * The FULL chain is captured (head..tail); {@code newOffsets[k] >= 0} marks a moved chunk.
     * Capacity and FLAG_LINKED are preserved — oversize content is NEVER converted to a plain
     * record. Chain order is head -> ... -> tail; the head is addressed by the recid index slot,
     * interior/tail chunks by the predecessor's {@code +4} next pointer.
     */
    private static final class LinkedChainMove {
        final long recid;
        final int flags;          // preserved index flags (FLAG_LINKED, ARCHIVE)
        final long[] oldOffsets;  // per-chunk current offset
        final int[] capUnits;     // per-chunk capacity units (preserved)
        final int[] copyBytes;    // per-chunk bytes to copy = LINKED_CHUNK_HDR + dataLen
        final long[] newOffsets;  // per-chunk target, or -1 when the chunk stays put
        final int totalDataLen;   // sum of chunk dataLen (assembled content length; invariant)
        LinkedChainMove(long recid, int flags, long[] oldOffsets, int[] capUnits,
                        int[] copyBytes, long[] newOffsets, int totalDataLen) {
            this.recid = recid; this.flags = flags; this.oldOffsets = oldOffsets;
            this.capUnits = capUnits; this.copyBytes = copyBytes; this.newOffsets = newOffsets;
            this.totalDataLen = totalDataLen;
        }
        long finalOffset(int k) { return newOffsets[k] >= 0 ? newOffsets[k] : oldOffsets[k]; }
    }

    private static final class CompactPlan {
        long retainedTail;
        final ArrayList<Move> moves = new ArrayList<>();
        final ArrayList<LinkedChainMove> linkedMoves = new ArrayList<>();
        final ArrayList<IndexPageMove> indexPageMoves = new ArrayList<>();
        /** Index pages AFTER the move (non-moved retained pages UNION target pages): EXCLUDED from data tiling. */
        final HashSet<Long> retainedIndexPagesAfterMove = new HashSet<>();
        final ArrayList<long[]> freeDataExtents = new ArrayList<>(); // {offset, capUnits}
        final ArrayList<StackDesc> stacks = new ArrayList<>();        // rebuilt free lists (data + recid)
    }

    /**
     * Reclaim up to {@code maxPages} trailing data pages by relocating their live
     * records into retained free space and truncating. Returns the number of pages
     * actually reclaimed (0 = nothing more to reclaim, or the tail is blocked by an
     * index/linked-record page). Bounded, steppable, crash-safe (see the section
     * comment above). Safe to interleave with normal reads/writes/commit BETWEEN calls.
     */
    public int compactStep(int maxPages) {
        if (maxPages <= 0) throw new IllegalArgumentException("maxPages must be positive: " + maxPages);
        checkClosed();
        commitLock.writeLock().lock();
        try {
            checkClosed();
            structuralLock.lock();
            try {
                return compactStepLocked(maxPages);
            } finally {
                structuralLock.unlock();
            }
        } finally {
            commitLock.writeLock().unlock();
        }
    }

    /**
     * Drive {@link #compactStep} to completion in bounded batches. Returns the total
     * pages reclaimed. A convenience wrapper for callers without a maintenance executor;
     * each underlying step is still a bounded exclusive critical section.
     */
    public long compactIncremental() {
        long total = 0;
        int n;
        while ((n = compactStep(DEFAULT_COMPACT_STEP_PAGES)) > 0) total += n;
        return total;
    }

    /** commitLock EXCLUSIVE + structuralLock held. */
    private int compactStepLocked(int maxPages) {
        long oldFileTail = fileTail();
        if (oldFileTail <= PAGE_SIZE) return 0; // no data pages

        // widest drop region: up to maxPages trailing pages (DATA or INDEX), never dropping page 0.
        // planCompaction relocates any index page / linked chunk in the region; if it cannot (no free
        // target, no exact tiling) it returns null and the region shrinks one page at a time until a
        // valid plan is found or the region is empty (0-page no-op).
        long retainedTail = oldFileTail - PAGE_SIZE;
        int want = 1;
        while (want < maxPages) {
            long next = retainedTail - PAGE_SIZE;
            if (next < PAGE_SIZE) break; // never drop page 0
            retainedTail = next;
            want++;
        }
        while (retainedTail < oldFileTail) {
            CompactPlan plan = planCompaction(retainedTail, oldFileTail);
            if (plan != null) {
                try {
                    executeCompaction(plan);
                } catch (Throwable t) {
                    // Planning is read-only, but executeCompaction mutates the volume with no
                    // rollback (the step-0 barrier already made the on-disk checksum durably
                    // invalid). A catchable throw here must not release the locks over a
                    // half-relocated store that later mutations/close() could reseal as clean.
                    poisoned = true;
                    closed = true;
                    throw t;
                }
                return (int) ((oldFileTail - retainedTail) / PAGE_SIZE);
            }
            retainedTail += PAGE_SIZE; // retain one more page, drop fewer
        }
        return 0;
    }

    /**
     * Build (in memory) and validate the full retained-region tiling for dropping
     * [retainedTail, oldFileTail). Returns a validated plan, or null when this
     * retainedTail is infeasible (linked/oversize record in the drop region, not enough
     * retained free space, or the tiling did not come out exact). NO volume writes here.
     */
    private CompactPlan planCompaction(long retainedTail, long oldFileTail) {
        long maxRecid = maxRecid();
        ArrayList<long[]> liveRetained = new ArrayList<>(); // {offset, sizeBytes} of records/chunks that stay
        ArrayList<Move> moves = new ArrayList<>();
        ArrayList<LinkedChainMove> linkedMoves = new ArrayList<>();
        ArrayList<Long> freeRecidValues = new ArrayList<>();
        long[] oldPages = indexPages; // current mirror (positions still valid during planning)

        for (long recid = 1; recid <= maxRecid; recid++) {
            long iv = rawIndexGet(recid);
            if (iv == 0) { freeRecidValues.add(Parity.p1set(recid << 1)); continue; }
            if (!ivParityOk(iv)) return null; // corruption: leave it to verify()/normal paths
            int cap = IndexVal.capUnits(iv);
            if (cap == IndexVal.CAP_DELETED) { freeRecidValues.add(Parity.p1set(recid << 1)); continue; }
            if (cap == IndexVal.CAP_NULL) continue; // null/prealloc: index slot only, no data extent
            if (IndexVal.isLinked(iv)) {
                ArrayList<long[]> chunks = linkedChain(iv); // {off, dataLen, capBytes}
                int n = chunks.size();
                long[] oldOffsets = new long[n];
                int[] capUnits = new int[n];
                int[] copyBytes = new int[n];
                long[] newOffsets = new long[n];
                int totalDataLen = 0;
                boolean anyMove = false;
                for (int k = 0; k < n; k++) {
                    long[] c = chunks.get(k);
                    oldOffsets[k] = c[0];
                    capUnits[k] = (int) (c[2] / 16);
                    copyBytes[k] = LINKED_CHUNK_HDR + (int) c[1];
                    newOffsets[k] = -1;
                    totalDataLen += (int) c[1];
                    if (c[0] >= retainedTail) anyMove = true;         // chunk in drop region: relocate
                    else liveRetained.add(new long[]{c[0], c[2]});    // stays put
                }
                if (anyMove) {
                    int flags = (int) (iv & (IndexVal.FLAG_LINKED | IndexVal.FLAG_ARCHIVE));
                    linkedMoves.add(new LinkedChainMove(recid, flags, oldOffsets, capUnits, copyBytes,
                            newOffsets, totalDataLen));
                }
                continue;
            }
            long off = IndexVal.offset(iv);
            long capBytes = cap * 16L;
            if (off >= retainedTail) {
                int used = vol.getInt(off);
                if (used < 0 || 4 + used > capBytes) return null;
                moves.add(new Move(recid, off, cap, 4 + used, (int) (iv & IndexVal.FLAG_ARCHIVE)));
            } else {
                liveRetained.add(new long[]{off, capBytes});
            }
        }

        // ---- index-page relocation: every index page in the drop region needs a lower target ----
        HashSet<Long> oldIndexSet = new HashSet<>();
        for (long p : oldPages) oldIndexSet.add(p);
        // pages that hold ANY retained live byte (extents never cross a page boundary, so a
        // page-granular mark is an exact interval test). Such pages cannot host a relocated index page.
        HashSet<Long> pagesWithLive = new HashSet<>();
        for (long[] e : liveRetained) pagesWithLive.add(e[0] - e[0] % PAGE_SIZE);
        // candidate targets: whole retained data pages with no retained live extent (old free-list /
        // long-stack bytes on them are abandoned — free lists are rebuilt from scratch).
        ArrayList<Long> candidateEmptyPages = new ArrayList<>();
        for (long page = PAGE_SIZE; page < retainedTail; page += PAGE_SIZE) {
            if (oldIndexSet.contains(page) || pagesWithLive.contains(page)) continue;
            candidateEmptyPages.add(page);
        }
        long[] newMirror = oldPages.clone();
        ArrayList<IndexPageMove> indexPageMoves = new ArrayList<>();
        int nextCand = 0;
        for (int i = 0; i < oldPages.length; i++) {
            if (oldPages[i] < retainedTail) continue; // retained index page: not moved
            if (nextCand >= candidateEmptyPages.size()) return null; // no free target -> no-op degrade
            long newPage = candidateEmptyPages.get(nextCand++);
            newMirror[i] = newPage;
            indexPageMoves.add(new IndexPageMove(i, oldPages[i], newPage));
        }
        // index pages AFTER the move (all are < retainedTail): excluded from data/free tiling
        HashSet<Long> retainedIndexAfter = new HashSet<>();
        for (long p : newMirror) if (p < retainedTail) retainedIndexAfter.add(p);

        // retained free runs = retained data pages MINUS live records (old free extents and old
        // long-stack chunks are NOT subtracted: their bytes become free after the rebuild); index
        // pages (after move) are excluded so an extent/chunk never lands on a relocated index page.
        ArrayList<long[]> runs = computeFreeRuns(retainedTail, liveRetained, retainedIndexAfter);

        // place plain moves AND moved linked chunks (largest first) into the lowest run that fits
        ArrayList<long[]> placeable = new ArrayList<>(); // {capBytes, kind(0=plain,1=linked), i, k}
        for (int i = 0; i < moves.size(); i++)
            placeable.add(new long[]{moves.get(i).capUnits * 16L, 0, i, 0});
        for (int i = 0; i < linkedMoves.size(); i++) {
            LinkedChainMove lm = linkedMoves.get(i);
            for (int k = 0; k < lm.oldOffsets.length; k++)
                if (lm.oldOffsets[k] >= retainedTail)
                    placeable.add(new long[]{lm.capUnits[k] * 16L, 1, i, k});
        }
        placeable.sort((a, b) -> Long.compare(b[0], a[0]));
        runs.sort((a, b) -> Long.compare(a[0], b[0]));
        for (long[] p : placeable) {
            long capBytes = p[0];
            long[] best = null;
            for (long[] r : runs) if (r[1] >= capBytes) { best = r; break; }
            if (best == null) return null; // no retained hole fits: infeasible at this retainedTail
            long newOffset = best[0];
            best[0] += capBytes;
            best[1] -= capBytes;
            if (p[1] == 0) moves.get((int) p[2]).newOffset = newOffset;
            else linkedMoves.get((int) p[2]).newOffsets[(int) p[3]] = newOffset;
        }
        runs.removeIf(r -> r[1] == 0);

        // rebuild the free lists (free-recid + free-data) over the leftover runs
        CompactPlan plan = new CompactPlan();
        plan.retainedTail = retainedTail;
        plan.moves.addAll(moves);
        plan.linkedMoves.addAll(linkedMoves);
        plan.indexPageMoves.addAll(indexPageMoves);
        plan.retainedIndexPagesAfterMove.addAll(retainedIndexAfter);
        if (!buildFreeLists(runs, freeRecidValues, plan)) return null;

        // TEST HOOK: inject a deliberate plan inconsistency so the validators below must degrade it
        // to a NO-OP (never corruption). Guarded by a volatile int; default 0 = disabled.
        if (testTamperMode != 0) tamperPlanForTest(plan, retainedTail);

        // PRE-WRITE content-preservation validators: turn any planning bug into a no-op.
        if (!validateIndexPagesAfterMove(plan, oldPages, retainedTail)) return null;
        if (!validateLinkedChainsAfterMove(plan, retainedTail)) return null;
        // exact tiling of every retained data page by {liveRetained, move targets, linked targets,
        // rebuilt chunks, free extents}, index pages (after move) excluded.
        if (!validateTiling(retainedTail, retainedIndexAfter, liveRetained, plan)) return null;
        return plan;
    }

    /** Per retained data page, the gaps not covered by {@code live} extents (sorted, 16-aligned). */
    private ArrayList<long[]> computeFreeRuns(long retainedTail, ArrayList<long[]> live,
                                              HashSet<Long> indexPageSet) {
        HashMap<Long, ArrayList<long[]>> byPage = new HashMap<>();
        for (long[] e : live) {
            long page = e[0] - e[0] % PAGE_SIZE;
            byPage.computeIfAbsent(page, k -> new ArrayList<>()).add(e);
        }
        ArrayList<long[]> runs = new ArrayList<>();
        for (long page = PAGE_SIZE; page < retainedTail; page += PAGE_SIZE) {
            if (indexPageSet.contains(page)) continue;
            ArrayList<long[]> list = byPage.get(page);
            long cursor = page;
            if (list != null) {
                list.sort((a, b) -> Long.compare(a[0], b[0]));
                for (long[] e : list) {
                    if (e[0] > cursor) runs.add(new long[]{cursor, e[0] - cursor});
                    cursor = e[0] + e[1];
                }
            }
            long end = page + PAGE_SIZE;
            if (cursor < end) runs.add(new long[]{cursor, end - cursor});
        }
        return runs;
    }

    /**
     * Plan the rebuilt free lists (free-recid stack + free-data size-class stacks) whose
     * long-stack chunks live inside the leftover {@code runs}, publishing the rest of the
     * runs as free-data extents. Solves the bootstrap circularity (chunks need space that
     * comes from the free space they catalog) by reserving ONE contiguous run as a chunk
     * "arena" and iterating the arena leftover to a fixpoint. Fills {@code plan.stacks}
     * and {@code plan.freeDataExtents}. Returns false when infeasible.
     */
    private boolean buildFreeLists(ArrayList<long[]> runs, ArrayList<Long> freeRecidValues, CompactPlan plan) {
        long[] recidVals = new long[freeRecidValues.size()];
        for (int i = 0; i < recidVals.length; i++) recidVals[i] = freeRecidValues.get(i);

        // extents contributed by every run except the arena (the arena hosts the chunks)
        runs.sort((a, b) -> Long.compare(b[1], a[1])); // largest first
        long[] arena = runs.isEmpty() ? null : runs.get(0);
        ArrayList<long[]> otherRuns = new ArrayList<>(runs.subList(arena == null ? 0 : 1, runs.size()));
        ArrayList<long[]> baseExtents = new ArrayList<>();
        for (long[] r : otherRuns) splitMaxExtents(r[0], r[1], baseExtents);

        // any free space (a nonempty arena) or free recid produces values that need chunks;
        // free-data extents are themselves values stored in the free-data stacks.
        if (arena == null) return recidVals.length == 0; // no space: fine iff nothing to store

        long arenaOff = arena[0], arenaLen = arena[1];
        ArrayList<long[]> extraExtents = new ArrayList<>(); // arena leftover published as free-data
        for (int iter = 0; iter < 8; iter++) {
            ArrayList<long[]> dataExtents = new ArrayList<>(baseExtents);
            dataExtents.addAll(extraExtents);
            ArrayList<StackDesc> stacks = buildStacks(recidVals, dataExtents);
            long used = 0;
            for (StackDesc s : stacks) for (ChunkDesc c : s.chain) used += c.chunkSize;
            if (used > arenaLen) return false; // arena too small: retain more (retry higher)
            long leftover = arenaLen - used;
            ArrayList<long[]> newExtra = new ArrayList<>();
            if (leftover > 0) splitMaxExtents(arenaOff + used, leftover, newExtra);
            if (sameExtents(newExtra, extraExtents)) {
                // converged: assign arena offsets to chunks, publish extents
                long cursor = arenaOff;
                for (StackDesc s : stacks) for (ChunkDesc c : s.chain) { c.offset = cursor; cursor += c.chunkSize; }
                plan.stacks.addAll(stacks);
                plan.freeDataExtents.addAll(baseExtents);
                plan.freeDataExtents.addAll(extraExtents);
                return true;
            }
            extraExtents = newExtra;
        }
        return false; // did not converge (degenerate fragmentation): retain more
    }

    /** Build free-recid + per-size-class free-data stacks (chunk chains) for the given values. */
    private ArrayList<StackDesc> buildStacks(long[] recidVals, ArrayList<long[]> dataExtents) {
        ArrayList<StackDesc> stacks = new ArrayList<>();
        if (recidVals.length > 0)
            stacks.add(new StackDesc(O_FREE_RECID_STACK, packChain(recidVals)));
        // group data-free values by size class
        HashMap<Integer, ArrayList<Long>> byClass = new HashMap<>();
        for (long[] e : dataExtents) {
            int capUnits = (int) e[1];
            byClass.computeIfAbsent(capUnits, k -> new ArrayList<>()).add(Parity.p1set(e[0] >>> 3));
        }
        for (var en : byClass.entrySet()) {
            long[] vals = new long[en.getValue().size()];
            for (int i = 0; i < vals.length; i++) vals[i] = en.getValue().get(i);
            stacks.add(new StackDesc(masterLinkOffset(en.getKey()), packChain(vals)));
        }
        return stacks;
    }

    /** Pack raw values into a chunk chain (<=160B chunks, matching the live long-stack format). */
    private ArrayList<ChunkDesc> packChain(long[] values) {
        ArrayList<ChunkDesc> chain = new ArrayList<>();
        int i = 0;
        while (i < values.length) {
            int bytes = 0;
            int start = i;
            while (i < values.length) {
                int vs = packLongSize(values[i]);
                if (i > start && 8 + bytes + vs > (int) LONG_STACK_PREF_SIZE - 8) break;
                bytes += vs;
                i++;
            }
            int chunkSize = (int) (((8 + bytes) + 15) & ~15);
            chain.add(new ChunkDesc(chunkSize, Arrays.copyOfRange(values, start, i)));
        }
        return chain;
    }

    /** Split a contiguous 16-aligned run into free-data extents ({offset, capUnits}), each <= MAX_CAPACITY. */
    private static void splitMaxExtents(long off, long len, ArrayList<long[]> out) {
        while (len > 0) {
            long take = Math.min(len, IndexVal.MAX_CAPACITY);
            out.add(new long[]{off, take / 16});
            off += take;
            len -= take;
        }
    }

    private static boolean sameExtents(ArrayList<long[]> a, ArrayList<long[]> b) {
        if (a.size() != b.size()) return false;
        for (int i = 0; i < a.size(); i++) {
            if (a.get(i)[0] != b.get(i)[0] || a.get(i)[1] != b.get(i)[1]) return false;
        }
        return true;
    }

    /** Confirm the plan tiles every retained data page exactly once (no gap/overlap/out-of-range). */
    private boolean validateTiling(long retainedTail, HashSet<Long> indexPageSet,
                                   ArrayList<long[]> liveRetained, CompactPlan plan) {
        ArrayList<long[]> extents = new ArrayList<>(); // {offset, sizeBytes}
        for (long[] e : liveRetained) extents.add(e);
        for (Move m : plan.moves) {
            if (m.newOffset < PAGE_SIZE || m.newOffset >= retainedTail) return false;
            extents.add(new long[]{m.newOffset, m.capUnits * 16L});
        }
        for (LinkedChainMove lm : plan.linkedMoves) {
            for (int k = 0; k < lm.oldOffsets.length; k++) {
                if (lm.newOffsets[k] < 0) continue; // chunk stays put (already in liveRetained)
                if (lm.newOffsets[k] < PAGE_SIZE || lm.newOffsets[k] >= retainedTail) return false;
                extents.add(new long[]{lm.newOffsets[k], lm.capUnits[k] * 16L});
            }
        }
        for (StackDesc s : plan.stacks) for (ChunkDesc c : s.chain) {
            if (c.offset < PAGE_SIZE || c.offset >= retainedTail) return false;
            extents.add(new long[]{c.offset, c.chunkSize});
        }
        for (long[] e : plan.freeDataExtents) extents.add(new long[]{e[0], e[1] * 16L});

        for (long[] e : extents) {
            long off = e[0], size = e[1];
            if ((off & 15) != 0 || (size & 15) != 0 || size < 16) return false;
            if (off < PAGE_SIZE || off + size > retainedTail) return false;
            if ((off % PAGE_SIZE) + size > PAGE_SIZE) return false;
            if (indexPageSet.contains(off - off % PAGE_SIZE)) return false;
        }
        HashMap<Long, ArrayList<long[]>> byPage = new HashMap<>();
        for (long[] e : extents) byPage.computeIfAbsent(e[0] - e[0] % PAGE_SIZE, k -> new ArrayList<>()).add(e);
        for (long page = PAGE_SIZE; page < retainedTail; page += PAGE_SIZE) {
            if (indexPageSet.contains(page)) continue;
            ArrayList<long[]> list = byPage.remove(page);
            long cursor = page;
            if (list != null) {
                list.sort((a, b) -> Long.compare(a[0], b[0]));
                for (long[] e : list) {
                    if (e[0] != cursor) return false; // gap or overlap
                    cursor = e[0] + e[1];
                }
            }
            if (cursor != page + PAGE_SIZE) return false; // page not fully covered
        }
        return byPage.isEmpty();
    }

    /**
     * PRE-WRITE validator over the SYNTHETIC final index-page layout (built from the old mirror plus
     * the planned moves — it never reads current on-volume links). Ensures a planning/tamper bug
     * degrades to a no-op: page count preserved, all final pages distinct + page-aligned + in
     * [PAGE_SIZE, retainedTail), the moved set matches {@code retainedIndexPagesAfterMove}, and the
     * synthetic incoming-link chain from ZERO_PAGE_LINK reproduces exactly the ordinal order (so
     * moved ordinals keep their outgoing links / chain identity). No volume writes.
     */
    private boolean validateIndexPagesAfterMove(CompactPlan plan, long[] oldPages, long retainedTail) {
        int n = oldPages.length;
        long[] finalPages = oldPages.clone();
        for (IndexPageMove mv : plan.indexPageMoves) {
            if (mv.ordinal < 0 || mv.ordinal >= n) return false;
            if (finalPages[mv.ordinal] != mv.oldPage) return false; // ordinal/oldPage disagree with mirror
            finalPages[mv.ordinal] = mv.newPage;
        }
        HashSet<Long> seen = new HashSet<>();
        for (int i = 0; i < n; i++) {
            long p = finalPages[i];
            if (p % PAGE_SIZE != 0 || p < PAGE_SIZE || p >= retainedTail) return false;
            if (!seen.add(p)) return false; // duplicate index page
        }
        // the excluded-from-tiling set must be exactly the final index pages (keeps validateTiling's
        // "no data extent on an index page" check honest against a desynced plan)
        if (seen.size() != plan.retainedIndexPagesAfterMove.size()
                || !seen.containsAll(plan.retainedIndexPagesAfterMove)) return false;
        // synthetic chain from ZERO_PAGE_LINK is the ordinal order (tautological on a correct plan;
        // catches a tampered ordinal/target). No on-volume reads.
        return true;
    }

    /**
     * PRE-WRITE validator over the planned linked-chunk relocations. Guards content preservation so a
     * bug is a no-op, not corruption: FLAG_LINKED retained (never re-planed to a plain record); every
     * final chunk offset aligned, in [PAGE_SIZE, retainedTail), within one page, cap fits; the copied
     * body fits the preserved capacity; and sum(dataLen) is unchanged (no dropped chunk). Global
     * offset disjointness across all extent classes is enforced by validateTiling. No volume writes.
     */
    private boolean validateLinkedChainsAfterMove(CompactPlan plan, long retainedTail) {
        for (LinkedChainMove lm : plan.linkedMoves) {
            int n = lm.oldOffsets.length;
            if (n == 0) return false;
            if ((lm.flags & IndexVal.FLAG_LINKED) == 0) return false; // must stay linked
            int sum = 0;
            for (int k = 0; k < n; k++) {
                long fo = lm.finalOffset(k);
                int cu = lm.capUnits[k];
                if (cu < 1 || cu > MAX_CAP_UNITS) return false;
                long capBytes = cu * 16L;
                if ((fo & 15) != 0 || fo < PAGE_SIZE || fo + capBytes > retainedTail) return false;
                if ((fo % PAGE_SIZE) + capBytes > PAGE_SIZE) return false;
                if (lm.copyBytes[k] < LINKED_CHUNK_HDR || lm.copyBytes[k] > capBytes) return false;
                sum += lm.copyBytes[k] - LINKED_CHUNK_HDR;
            }
            if (sum != lm.totalDataLen) return false; // a chunk was dropped/duplicated
        }
        return true;
    }

    /** TEST-ONLY: corrupt a freshly-planned plan so the validators must reject it (see testTamperMode). */
    private void tamperPlanForTest(CompactPlan plan, long retainedTail) {
        switch (testTamperMode) {
            case 1 -> // UNIVERSAL: inject an out-of-range free extent (offset 16 is in the header page):
                      // validateTiling must reject EVERY plan -> compactStep degrades to a 0-page no-op.
                    plan.freeDataExtents.add(new long[]{16, 1});
            case 2 -> { // index target un-aligned: validateIndexPagesAfterMove must reject
                if (!plan.indexPageMoves.isEmpty()) {
                    IndexPageMove mv = plan.indexPageMoves.get(0);
                    plan.indexPageMoves.set(0, new IndexPageMove(mv.ordinal, mv.oldPage, mv.newPage + 16));
                }
            }
            case 3 -> { // un-move a drop-region linked chunk: validateLinkedChainsAfterMove must reject
                for (LinkedChainMove lm : plan.linkedMoves)
                    for (int k = 0; k < lm.oldOffsets.length; k++)
                        if (lm.newOffsets[k] >= 0) { lm.newOffsets[k] = -1; return; }
            }
            default -> { }
        }
    }

    /** Apply a validated plan: copy+flip plain/linked/index moves, rebuild free lists, shrink, stamp, sync, truncate. */
    private void executeCompaction(CompactPlan plan) {
        // 0) CRASH BARRIER: the header checksum covers only header words (not data bytes or
        // index slots), so relocating a record and flipping its index slot changes NOTHING the
        // stamped checksum protects. Were we to relocate first, a crash mid-step could reopen
        // with the OLD checksum still valid over a half-relocated state — an index slot pointing
        // into space the (unchanged) old free lists still consider free. So first invalidate the
        // on-disk checksum and make that DURABLE: any crash from here until the final stamp+sync
        // reopens as a mismatch and is refused. The completed step is the first accepted state —
        // making compactStep as crash-safe as commit() (durability-of-last-commit).
        vol.putInt(O_HEAD_CHECKSUM, ~headChecksum());
        vol.sync();

        // 1) relocate PLAIN records: write the new copy, THEN atomically re-point the recid index slot.
        // Move targets (retained) never overlap move sources (drop region) or fixed live extents, so
        // copy order is irrelevant. Slot flips resolve through the OLD indexPages mirror (index pages
        // are not published to their new offsets until step 3).
        for (Move m : plan.moves) {
            byte[] tmp = new byte[m.copyBytes];
            vol.getData(m.oldOffset, tmp, 0, m.copyBytes);
            vol.putData(m.newOffset, tmp, 0, m.copyBytes);
            indexSet(m.recid, IndexVal.compose(m.capUnits, m.newOffset, m.flags));
        }

        // 2) relocate LINKED chunks: copy each moved chunk body, then rewrite the WHOLE chain's next
        // pointers to final offsets (head via the recid index slot, interior via predecessor +4, tail
        // next explicitly zeroed). Rewriting unchanged links is idempotent under the exclusive barrier.
        // FLAG_LINKED and per-chunk capacity are preserved (oversize stays oversize).
        for (LinkedChainMove lm : plan.linkedMoves) {
            int n = lm.oldOffsets.length;
            for (int k = 0; k < n; k++) {
                if (lm.newOffsets[k] < 0) continue;
                byte[] tmp = new byte[lm.copyBytes[k]];
                vol.getData(lm.oldOffsets[k], tmp, 0, lm.copyBytes[k]);
                vol.putData(lm.newOffsets[k], tmp, 0, lm.copyBytes[k]);
            }
            // head incoming = recid index slot (preserving FLAG_LINKED + archive)
            indexSet(lm.recid, IndexVal.compose(lm.capUnits[0], lm.finalOffset(0), lm.flags));
            // interior links: predecessor(k-1) next -> chunk k
            for (int k = 1; k < n; k++)
                vol.putLong(lm.finalOffset(k - 1) + 4,
                        Parity.p1set(((long) lm.capUnits[k] << 48) | lm.finalOffset(k)));
            // tail next explicitly terminated (do not trust the copied body)
            vol.putLong(lm.finalOffset(n - 1) + 4, Parity.p1set(0));
        }

        // 3) relocate INDEX pages: copy each whole 1 MiB page (LAST — captures every slot flip from
        // steps 1/2), rewrite the entire index-page chain links to final offsets, then publish the
        // in-memory mirror. recids are unchanged (recidToOffset resolves by ordinal).
        if (!plan.indexPageMoves.isEmpty()) {
            long[] mirror = indexPages.clone();
            for (IndexPageMove mv : plan.indexPageMoves) {
                byte[] page = new byte[(int) PAGE_SIZE];
                vol.getData(mv.oldPage, page, 0, (int) PAGE_SIZE);
                vol.putData(mv.newPage, page, 0, (int) PAGE_SIZE);
                mirror[mv.ordinal] = mv.newPage;
            }
            long ptr = ZERO_PAGE_LINK;
            for (long p : mirror) { vol.putLong(ptr, Parity.p16set(p)); ptr = p + 8; }
            vol.putLong(ptr, Parity.p16set(0)); // terminator
            indexPages = mirror; // publish AFTER the on-volume chain flip
        }

        // Test hook: simulate a crash in the dangerous window (all relocation applied, free lists not
        // yet rebuilt). The barrier above guarantees this reopens as a checksum mismatch.
        Runnable hook = testCompactCrashHook;
        if (hook != null) hook.run();
        // 4) rebuild free lists: reset every master, then write the planned chunk chains.
        vol.putLong(O_FREE_RECID_STACK, Parity.p4set(0));
        for (int u = 1; u <= MAX_CAP_UNITS; u++) vol.putLong(masterLinkOffset(u), Parity.p4set(0));
        for (StackDesc s : plan.stacks) writeStack(s);
        // 5) shrink: no bump frontier (all free space is published), so dataTail = 0.
        dataTail(0);
        fileTail(plan.retainedTail);
        recomputeFreeDataBytes(); // free lists rebuilt directly, not via releaseDataLocked

        // 6) durability point (identical ordering to compact()): data first, then stamp, then truncate.
        stampHeaderDurable();
        vol.truncate(plan.retainedTail);

        // telemetry for tests: count applied relocations of the two new move kinds
        if (!plan.linkedMoves.isEmpty()) testLinkedMovesApplied += plan.linkedMoves.size();
        if (!plan.indexPageMoves.isEmpty()) testIndexPageMovesApplied += plan.indexPageMoves.size();
    }

    /** Write one rebuilt long-stack chain to the volume and set its master link. */
    private void writeStack(StackDesc s) {
        long prev = 0;
        long topOffset = 0, topPos = 0;
        for (ChunkDesc c : s.chain) {
            vol.clear(c.offset, c.offset + c.chunkSize); // zero tails delimit the value area
            vol.putLong(c.offset, Parity.p4set(((long) c.chunkSize << 48) | prev));
            long p = c.offset + 8;
            for (long v : c.values) p += putPackedLong(p, v);
            prev = c.offset;
            topOffset = c.offset;
            topPos = p - c.offset;
        }
        if (topOffset != 0)
            vol.putLong(s.masterLinkOffset, Parity.p4set((topPos << 48) | topOffset));
    }

    private ArrayList<CompactEntry> compactSnapshot() {
        ArrayList<CompactEntry> entries = new ArrayList<>();
        long max = maxRecid();
        for (long recid = 1; recid <= max; recid++) {
            long iv = rawIndexGet(recid);
            if (iv == 0) continue;
            if (!ivParityOk(iv))
                throw new DBException.DataCorruption("index slot parity broken, recid=" + recid);
            int cap = IndexVal.capUnits(iv);
            if (cap == IndexVal.CAP_DELETED) continue;
            if (cap == IndexVal.CAP_NULL) {
                entries.add(new CompactEntry(recid, IndexVal.isPrealloc(iv), 0, null));
            } else if (IndexVal.isLinked(iv)) {
                entries.add(new CompactEntry(recid, false, 0, linkedGet(iv)));
            } else {
                long off = IndexVal.offset(iv);
                int used = vol.getInt(off);
                int capBytes = cap * 16;
                if (used < 0 || 4 + used > capBytes)
                    throw new DBException.DataCorruption("used beyond capacity, recid=" + recid);
                byte[] content = new byte[used];
                vol.getData(off + 4, content, 0, used);
                entries.add(new CompactEntry(recid, false, capBytes, content));
            }
        }
        return entries;
    }

    // ---------- background maintenance wiring (R6) ----------

    /**
     * Enable background maintenance for this store, creating an OWNED {@link MaintenanceExecutor}
     * and registering a {@link DirectCompactionTask} that drives {@link #compactStep}. Idempotent;
     * returns the executor. The store remains fully correct with maintenance disabled — this only
     * moves the (already synchronous) compaction work to a bounded background thread (P7).
     */
    public synchronized MaintenanceExecutor startMaintenance() {
        checkClosed();
        if (maintenanceShutdown) throw new DBException.StoreClosed();
        if (maintenance == null) {
            maintenance = new MaintenanceExecutor("direct");
            ownsMaintenance = true;
            compactionTask = new DirectCompactionTask(this);
            compactionHandle = maintenance.register(compactionTask);
            maintenanceActive = true;
        }
        return maintenance;
    }

    /**
     * Register this store's compaction task into an EXTERNALLY-owned executor (e.g. a shared DB-level
     * one). Close will cancel the task but not shut down the shared executor.
     */
    public synchronized void attachMaintenance(MaintenanceExecutor executor) {
        checkClosed();
        if (maintenanceShutdown) throw new DBException.StoreClosed();
        if (executor == null) throw new NullPointerException("executor");
        if (compactionHandle != null) throw new IllegalStateException("maintenance already attached");
        maintenance = executor;
        ownsMaintenance = false;
        compactionTask = new DirectCompactionTask(this);
        compactionHandle = executor.register(compactionTask);
        maintenanceActive = true;
    }

    /** Hint the compaction task that fresh fragmentation exists (no-op when maintenance is disabled). */
    private void signalCompactionChurn() {
        if (!maintenanceActive) return; // fast path: maintenance disabled
        DirectCompactionTask t;
        MaintenanceExecutor ex;
        synchronized (this) { t = compactionTask; ex = maintenance; }
        if (t != null && ex != null) { t.signalChurn(); ex.signal(); }
    }

    /**
     * Stop background maintenance as the first step of {@link #close}: snapshot the fields, then
     * (owned) shut the executor down and join it — letting any in-flight bounded {@code compactStep}
     * finish and RELEASE the commitLock — or (attached) just cancel the task. Done BEFORE close takes
     * the commitLock so there is no shutdown/compaction deadlock.
     */
    private void stopMaintenanceForClose() {
        MaintenanceExecutor ex;
        boolean owns;
        MaintenanceExecutor.Handle h;
        synchronized (this) {
            if (maintenanceShutdown) return; // idempotent; a concurrent close already handled it
            maintenanceShutdown = true;      // refuses any start/attach that races this close
            ex = maintenance;
            owns = ownsMaintenance;
            h = compactionHandle;
            compactionHandle = null;
            compactionTask = null;
            maintenance = null;
            maintenanceActive = false;
        }
        if (ex == null) return;
        if (owns) ex.shutdown(true);
        else if (h != null) h.cancel();
    }

    @Override public void close() {
        if (closed && !poisoned) return;
        stopMaintenanceForClose();
        commitLock.writeLock().lock(); // waits out in-flight mutators
        try {
            if (closed && !poisoned) return;
            boolean stamp = !poisoned; // a poisoned store must never be resealed as clean
            closed = true;
            poisoned = false; // resources released below; further close() calls no-op
            if (stamp) {
                long tail = fileTail();
                stampHeaderDurable();
                vol.close(tail);
            } else {
                vol.close(-1); // release channel/mappings, no truncate, no stamp
            }
            indexPages = new long[0];
        } finally {
            commitLock.writeLock().unlock();
        }
    }

    @Override public boolean isClosed() { return closed; }

    /**
     * Full-store invariant walk (mapdb3 verify() lineage): every byte of every data
     * page must be accounted for exactly once — by a live record, a linked chunk, a
     * long-stack chunk, or a free-list extent — up to dataTail on the current data
     * page. Free recids must reference deleted/never-written slots, without
     * duplicates. Any parity failure surfaces as VerifyFailed.
     */
    @Override public void verify() {
        mutateEnter();
        try {
            structuralLock.lock();
            try {
                verifyLocked();
            } catch (DBException.DataCorruption e) {
                throw new DBException.VerifyFailed(e.getMessage());
            } finally {
                structuralLock.unlock();
            }
        } finally {
            mutateExit();
        }
    }

    private void verifyLocked() {
        long fileTail = fileTail();
        long dataTail = dataTail();
        long maxRecid = maxRecid();
        if (fileTail < PAGE_SIZE || fileTail % PAGE_SIZE != 0)
            throw new DBException.VerifyFailed("bad fileTail " + fileTail);
        if (dataTail != 0 && (dataTail % 16 != 0 || dataTail % PAGE_SIZE == 0
                || dataTail < PAGE_SIZE || dataTail >= fileTail))
            throw new DBException.VerifyFailed("bad dataTail " + dataTail);
        if (maxRecid > 0 && recidToOffset(maxRecid) < 0)
            throw new DBException.VerifyFailed("maxRecid beyond allocated index pages");

        // index page chain on the volume must match the heap mirror
        HashSet<Long> indexPageSet = new HashSet<>();
        long[] mirror = indexPages;
        long ptr = ZERO_PAGE_LINK;
        int n = 0;
        while (true) {
            long page = Parity.p16get(vol.getLong(ptr));
            if (page == 0) break;
            if (n >= mirror.length || mirror[n] != page)
                throw new DBException.VerifyFailed("index page chain diverges from mirror at " + n);
            if (page % PAGE_SIZE != 0 || page >= fileTail)
                throw new DBException.VerifyFailed("index page out of range: " + page);
            if (!indexPageSet.add(page)) throw new DBException.VerifyFailed("index page loop");
            ptr = page + 8;
            n++;
        }
        if (n != mirror.length) throw new DBException.VerifyFailed("index page mirror longer than chain");

        ArrayList<long[]> extents = new ArrayList<>(); // {offset, size}

        // live records
        for (long recid = 1; recid <= maxRecid; recid++) {
            long iv = rawIndexGet(recid);
            if (iv == 0) continue;
            if (!ivParityOk(iv))
                throw new DBException.VerifyFailed("index parity broken, recid=" + recid);
            int cap = IndexVal.capUnits(iv);
            if (cap == IndexVal.CAP_DELETED || cap == IndexVal.CAP_NULL) {
                if (IndexVal.offset(iv) != 0)
                    throw new DBException.VerifyFailed("sentinel index value with offset, recid=" + recid);
                continue;
            }
            if (IndexVal.isLinked(iv)) {
                for (long[] c : linkedChain(iv)) extents.add(new long[]{c[0], c[2]});
            } else {
                long off = IndexVal.offset(iv);
                long capBytes = cap * 16L;
                int used = vol.getInt(off);
                if (used < 0 || 4 + used > capBytes)
                    throw new DBException.VerifyFailed("used beyond capacity, recid=" + recid);
                extents.add(new long[]{off, capBytes});
            }
        }

        // free recid stack
        HashSet<Long> freeRecids = new HashSet<>();
        forEachLongStack(O_FREE_RECID_STACK, extents, v -> {
            long recid = v >>> 1;
            if (recid < 1 || recid > maxRecid)
                throw new DBException.VerifyFailed("free recid out of range: " + recid);
            long iv = rawIndexGet(recid);
            if (iv != 0 && (!ivParityOk(iv) || IndexVal.capUnits(iv) != IndexVal.CAP_DELETED))
                throw new DBException.VerifyFailed("free-list recid is live: " + recid);
            if (!freeRecids.add(recid))
                throw new DBException.VerifyFailed("duplicate free recid: " + recid);
        });

        // free data stacks
        long[] freeSum = {0};
        for (long u = 1; u <= MAX_CAP_UNITS; u++) {
            final long size = u * 16;
            forEachLongStack(masterLinkOffset(u), extents, v -> {
                long off = v << 3;
                extents.add(new long[]{off, size});
                freeSum[0] += size;
            });
        }
        // the getCurrentSize() running counter must equal the free-data stack total
        if (freeSum[0] != freeDataBytes)
            throw new DBException.VerifyFailed("freeDataBytes drift: counter=" + freeDataBytes
                    + " actual=" + freeSum[0]);

        // geometry checks + exact tiling of the data pages
        for (long[] e : extents) {
            long off = e[0], size = e[1];
            if ((off & 15) != 0 || (size & 15) != 0 || size < 16)
                throw new DBException.VerifyFailed("unaligned extent at " + off);
            if (off < PAGE_SIZE || off + size > fileTail)
                throw new DBException.VerifyFailed("extent out of bounds at " + off);
            if ((off % PAGE_SIZE) + size > PAGE_SIZE)
                throw new DBException.VerifyFailed("extent crosses page boundary at " + off);
            long page = off - off % PAGE_SIZE;
            if (indexPageSet.contains(page))
                throw new DBException.VerifyFailed("extent inside an index page at " + off);
        }
        HashMap<Long, ArrayList<long[]>> byPage = new HashMap<>();
        for (long[] e : extents) {
            byPage.computeIfAbsent(e[0] - e[0] % PAGE_SIZE, k -> new ArrayList<>()).add(e);
        }
        long dataTailPage = dataTail == 0 ? -1 : dataTail - dataTail % PAGE_SIZE;
        for (long page = PAGE_SIZE; page < fileTail; page += PAGE_SIZE) {
            if (indexPageSet.contains(page)) continue;
            long coverEnd = page == dataTailPage ? dataTail : page + PAGE_SIZE;
            ArrayList<long[]> list = byPage.remove(page);
            long cursor = page;
            if (list != null) {
                list.sort((a, b) -> Long.compare(a[0], b[0]));
                for (long[] e : list) {
                    if (e[0] < cursor)
                        throw new DBException.VerifyFailed("overlapping extents at " + e[0]);
                    if (e[0] > cursor)
                        throw new DBException.VerifyFailed("lost extent: gap at " + cursor);
                    cursor = e[0] + e[1];
                }
            }
            if (cursor != coverEnd)
                throw new DBException.VerifyFailed("lost extent: page " + page
                        + " covered to " + cursor + ", expected " + coverEnd);
        }
        if (!byPage.isEmpty())
            throw new DBException.VerifyFailed("extents on unallocated pages: " + byPage.keySet());
    }

    private interface LongStackValueCheck {
        void value(long decoded);
    }

    /**
     * Walks one long stack: registers each chunk as an extent and feeds every
     * (parity1-decoded) value to {@code check}. Structural lock must be held.
     */
    private void forEachLongStack(long masterLinkOffset, ArrayList<long[]> extents, LongStackValueCheck check) {
        long master = Parity.p4get(vol.getLong(masterLinkOffset));
        if (master == 0) return;
        long chunkOffset = master & IndexVal.MOFFSET;
        long pos = master >>> 48;
        int guard = 0;
        while (chunkOffset != 0) {
            if (++guard > (1 << 24)) throw new DBException.VerifyFailed("long stack chunk loop");
            long hdr = Parity.p4get(vol.getLong(chunkOffset));
            long chunkSize = hdr >>> 48;
            long prev = hdr & IndexVal.MOFFSET;
            if (chunkSize < 16 || chunkSize > LONG_STACK_MAX_SIZE || (chunkSize & 15) != 0)
                throw new DBException.VerifyFailed("bad long stack chunk size " + chunkSize);
            if (pos < 8 || pos > chunkSize)
                throw new DBException.VerifyFailed("bad long stack position " + pos);
            extents.add(new long[]{chunkOffset, chunkSize});
            long p = chunkOffset + 8;
            long end = chunkOffset + pos;
            while (p < end) {
                if (vol.getUnsignedByte(p) == 0)
                    throw new DBException.VerifyFailed("zero byte inside long stack value area at " + p);
                long raw = getPackedLong(p);
                p += packLongSize(raw);
                if (p > end) throw new DBException.VerifyFailed("long stack value overruns chunk at " + p);
                check.value(Parity.p1get(raw));
            }
            chunkOffset = prev;
            if (prev != 0) {
                long prevSize = Parity.p4get(vol.getLong(prev)) >>> 48;
                pos = longStackFindEnd(prev, prevSize);
            }
        }
    }

    @Override public PrimitiveIterator.OfLong getAllRecids() {
        mutateEnter();
        try {
            long max;
            structuralLock.lock();
            try {
                max = maxRecid();
            } finally {
                structuralLock.unlock();
            }
            long[] result = new long[64];
            int size = 0;
            for (long recid = 1; recid <= max; recid++) {
                long iv = rawIndexGet(recid);
                if (iv == 0) continue;
                int cap = IndexVal.capUnits(iv);
                if (cap == IndexVal.CAP_DELETED || IndexVal.isPrealloc(iv)) continue;
                if (size == result.length) result = Arrays.copyOf(result, size * 2);
                result[size++] = recid;
            }
            final long[] arr = Arrays.copyOf(result, size);
            return Arrays.stream(arr).iterator();
        } finally {
            mutateExit();
        }
    }

    // ---------- package-private hooks for StoreWAL (commit apply + recovery replay) ----------

    /** Force-allocate a recid in P state; idempotent when already P. */
    void walPrealloc(long recid) {
        mutateEnter();
        try {
            walPreallocInner(recid);
        } finally {
            mutateExit();
        }
    }

    private void walPreallocInner(long recid) {
        structuralLock.lock();
        try {
            ensureIndexCapacityLocked(recid);
            if (recid > maxRecid()) maxRecid(recid);
        } finally {
            structuralLock.unlock();
        }
        ReadWriteLock lock = segs.forRecid(recid);
        lock.writeLock().lock();
        try {
            long iv = rawIndexGet(recid);
            // replay may legitimately re-preallocate a recid deleted in an earlier
            // section (the live allocator reuses freed recids)
            if (iv == 0 || IndexVal.capUnits(iv) == IndexVal.CAP_DELETED)
                indexSet(recid, IndexVal.compose(IndexVal.CAP_NULL, 0, IndexVal.FLAG_PREALLOC));
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Rebuilds the free-recid list from the index. MUST be called after WAL replay:
     * replay applies delete-then-reuse histories through {@link #delete} +
     * {@link #walPut}/{@link #walPrealloc}, which pushes a recid onto the free list
     * on delete but does not remove it when a later section revives it — a stale
     * entry there would hand a LIVE recid to the next allocation and overwrite
     * committed data. Also reclaims never-referenced gaps (e.g. recids a checkpoint
     * snapshot omitted because they were deleted).
     */
    void rebuildFreeRecids() {
        mutateEnter();
        try {
            rebuildFreeRecidsInner();
        } finally {
            mutateExit();
        }
    }

    private void rebuildFreeRecidsInner() {
        structuralLock.lock();
        try {
            // drain the on-volume stack: release its chunks, then repopulate from the index
            long master = Parity.p4get(vol.getLong(O_FREE_RECID_STACK));
            long chunkOffset = master & IndexVal.MOFFSET;
            ArrayList<long[]> chunks = new ArrayList<>();
            while (chunkOffset != 0) {
                long hdr = Parity.p4get(vol.getLong(chunkOffset));
                chunks.add(new long[]{chunkOffset, hdr >>> 48});
                chunkOffset = hdr & IndexVal.MOFFSET;
                if (chunks.size() > (1 << 24)) throw new DBException.DataCorruption("free recid stack loop");
            }
            vol.putLong(O_FREE_RECID_STACK, Parity.p4set(0));
            for (long[] c : chunks) releaseDataLocked(c[1], c[0]);
            long max = maxRecid();
            for (long recid = 1; recid <= max; recid++) {
                long iv = rawIndexGet(recid);
                if (iv == 0 || IndexVal.capUnits(iv) == IndexVal.CAP_DELETED) freeRecidLocked(recid);
            }
        } finally {
            structuralLock.unlock();
        }
    }

    /**
     * Force-write full record content with explicit capacity (WAL RECORD entry).
     * Allocates the recid if this store has never seen it (recovery replay).
     * {@code capBytes == 0} with non-null content means "choose the layout":
     * oversize content is stored as a linked chain (the WAL writer encodes
     * capacity 0 for those — a linked record has no meaningful plain capacity).
     */
    void walPut(long recid, int capBytes, byte[] content) {
        mutateEnter();
        try {
            walPutInner(recid, capBytes, content);
        } finally {
            mutateExit();
        }
    }

    private void walPutInner(long recid, int capBytes, byte[] content) {
        structuralLock.lock();
        try {
            ensureIndexCapacityLocked(recid);
            if (recid > maxRecid()) maxRecid(recid);
        } finally {
            structuralLock.unlock();
        }
        ReadWriteLock lock = segs.forRecid(recid);
        lock.writeLock().lock();
        try {
            long iv = rawIndexGet(recid);
            if (iv != 0) {
                if (!ivParityOk(iv))
                    throw new DBException.DataCorruption("index slot parity broken, recid=" + recid);
                releaseOldData(iv);
            }
            if (content == null) {
                indexSet(recid, IndexVal.compose(IndexVal.CAP_NULL, 0, 0));
            } else if (needsLinked(content.length)) {
                writeNewLinked(recid, content, content.length);
            } else {
                int cap = capBytes == 0 ? capBytesFor(4L + content.length) : capBytes;
                checkSize(cap);
                if (cap < 4L + content.length || (cap & 15) != 0)
                    throw new DBException.DataCorruption("bad record capacity " + cap);
                writeNewData(recid, content, content.length, cap, 0);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Recovery-only delete (WAL DELETE entry): identical to {@link #delete} except that a
     * void recid is a NO-OP instead of {@code GetVoid}.
     *
     * <p>Ordered replay over base identity can legitimately reach a DELETE
     * for a recid that is not there: a stranded {@code T_APPEND} whose base was cleaned away
     * is skipped, so the recid it names may never have been revived by the retained set. The
     * delete that follows it in the same history must then no-op rather than refuse the open.
     */
    void walDelete(long recid) {
        mutateEnter();
        try {
            boolean churned = false;
            ReadWriteLock lock = segs.forRecid(recid);
            lock.writeLock().lock();
            try {
                long iv = rawIndexGet(recid);
                if (iv != 0) {
                    if (!ivParityOk(iv))
                        throw new DBException.DataCorruption("index slot parity broken, recid=" + recid);
                    if (IndexVal.capUnits(iv) != IndexVal.CAP_DELETED) {
                        releaseOldData(iv);
                        structuralLock.lock();
                        try {
                            freeRecidLocked(recid);
                        } finally {
                            structuralLock.unlock();
                        }
                        indexSet(recid, IndexVal.compose(IndexVal.CAP_DELETED, 0, 0));
                        churned = true;
                    }
                }
            } finally {
                lock.writeLock().unlock();
            }
            if (churned) signalCompactionChurn();
        } finally {
            mutateExit();
        }
    }

    /** Record state for StoreWAL merge logic. */
    static final int STATE_VOID = 0, STATE_NULL = 1, STATE_LIVE = 2;

    int recState(long recid) {
        mutateEnter();
        try {
            ReadWriteLock lock = segs.forRecid(recid);
            lock.readLock().lock();
            try {
                long iv = rawIndexGet(recid);
                if (iv == 0) return STATE_VOID;
                if (!ivParityOk(iv))
                    throw new DBException.DataCorruption("index slot parity broken, recid=" + recid);
                if (IndexVal.capUnits(iv) == IndexVal.CAP_DELETED) return STATE_VOID;
                return IndexVal.capUnits(iv) == IndexVal.CAP_NULL ? STATE_NULL : STATE_LIVE;
            } finally {
                lock.readLock().unlock();
            }
        } finally {
            mutateExit();
        }
    }

    /** Snapshot sink for WAL checkpointing: one call per recid that must survive. */
    interface WalSnapshotConsumer {
        /**
         * @param recid    recid to reconstruct
         * @param prealloc true for P-state recids (null content, prealloc flag)
         * @param capBytes exact record capacity in bytes; 0 when {@code content} is
         *                 null AND for oversize (linked) records, whose content
         *                 exceeds the plain capacity model
         * @param content  full record content copy; null for P-state and null-content records
         */
        void entry(long recid, boolean prealloc, int capBytes, byte[] content);
    }

    /**
     * Visits every recid that must survive a WAL checkpoint (live, null-content and
     * preallocated records; deleted/void recids are skipped), in ascending recid order.
     * Content is copied and capacity reported exactly, so append/capacityRemaining
     * semantics are preserved when the snapshot is replayed via {@link #walPut}/
     * {@link #walPrealloc}. The sink is invoked outside all locks.
     */
    void walSnapshot(WalSnapshotConsumer sink) {
        mutateEnter();
        try {
            long max;
            structuralLock.lock();
            try {
                max = maxRecid();
            } finally {
                structuralLock.unlock();
            }
            for (long recid = 1; recid <= max; recid++) snapshotOne(recid, sink);
        } finally {
            mutateExit();
        }
    }

    /**
     * {@link #walSnapshot} for ONE recid — what the incremental cleaner needs, since it
     * re-emits the handful of records a retiring segment still owns rather than the whole store.
     * Returns whether the recid must survive (i.e. whether the sink was invoked); a void or deleted
     * recid emits nothing.
     *
     * <p>Identical rules to the full sweep, deliberately sharing one implementation: an image the
     * cleaner writes and an image a checkpoint writes must be byte-for-byte the same shape, or
     * replay's capacity and prealloc reasoning would depend on which of the two produced it.
     */
    boolean walSnapshotOne(long recid, WalSnapshotConsumer sink) {
        mutateEnter();
        try {
            return snapshotOne(recid, sink);
        } finally {
            mutateExit();
        }
    }

    /** {@link #mutateEnter} must be held. Returns whether the sink was invoked. */
    private boolean snapshotOne(long recid, WalSnapshotConsumer sink) {
        boolean emit = false;
        boolean prealloc = false;
        int capBytes = 0;
        byte[] content = null;
        ReadWriteLock lock = segs.forRecid(recid);
        lock.readLock().lock();
        try {
            long iv = rawIndexGet(recid);
            int cap = IndexVal.capUnits(iv);
            if (iv != 0 && cap != IndexVal.CAP_DELETED) {
                emit = true;
                if (cap == IndexVal.CAP_NULL) {
                    prealloc = IndexVal.isPrealloc(iv);
                } else if (IndexVal.isLinked(iv)) {
                    content = linkedGet(iv);
                    capBytes = 0; // linked: layout re-chosen on replay
                } else {
                    long off = IndexVal.offset(iv);
                    int used = vol.getInt(off);
                    content = new byte[used];
                    vol.getData(off + 4, content, 0, used);
                    capBytes = cap * 16;
                }
            }
        } finally {
            lock.readLock().unlock();
        }
        if (emit) sink.entry(recid, prealloc, capBytes, content);
        return emit;
    }

    /** Copy of record content, or null for null/P records. Throws GetVoid on N/D. */
    byte[] rawGet(long recid) {
        mutateEnter();
        try {
            ReadWriteLock lock = segs.forRecid(recid);
            lock.readLock().lock();
            try {
                long iv = indexGetChecked(recid);
                if (IndexVal.capUnits(iv) == IndexVal.CAP_NULL) return null;
                if (IndexVal.isLinked(iv)) return linkedGet(iv);
                long off = IndexVal.offset(iv);
                int used = vol.getInt(off);
                byte[] r = new byte[used];
                vol.getData(off + 4, r, 0, used);
                return r;
            } finally {
                lock.readLock().unlock();
            }
        } finally {
            mutateExit();
        }
    }

    // ---------- package-private test hooks ----------

    /** Test hook: run mid-{@link #executeCompaction} (after all relocation, before free-list rebuild). */
    static volatile Runnable testCompactCrashHook;

    /** Test hook: when non-zero, {@link #planCompaction} corrupts the plan so the validators must
     *  degrade it to a NO-OP (proves a bad plan never reaches the volume). See {@link #tamperPlanForTest}. */
    static volatile int testTamperMode = 0;

    /** Test telemetry: cumulative count of linked chunk / index page relocations actually applied. */
    volatile long testLinkedMovesApplied = 0;
    volatile long testIndexPageMovesApplied = 0;

    /** Test hook: raw long-stack push under the structural lock. Keeps {@link #freeDataBytes}
     *  consistent (the value lands on the free-data stack of its size class) so the verify net holds. */
    void testLongStackPut(long masterLinkOffset, long rawParity1Value) {
        structuralLock.lock();
        try {
            longStackPut(masterLinkOffset, rawParity1Value);
            freeDataBytes += testStackSizeBytes(masterLinkOffset);
        } finally {
            structuralLock.unlock();
        }
    }

    /** Test hook: raw long-stack pop under the structural lock; 0 when empty. */
    long testLongStackTake(long masterLinkOffset) {
        structuralLock.lock();
        try {
            long v = longStackTake(masterLinkOffset);
            if (v != 0) freeDataBytes -= testStackSizeBytes(masterLinkOffset);
            return v;
        } finally {
            structuralLock.unlock();
        }
    }

    /** Byte size class for a free-data master link offset (for the raw test hooks). */
    private static long testStackSizeBytes(long masterLinkOffset) {
        return ((masterLinkOffset - O_FREE_DATA_STACKS) / 8 + 1) * 16L;
    }

    /** Test hook: force index page allocation up to {@code recid}. */
    void testEnsureIndexCapacity(long recid) {
        structuralLock.lock();
        try {
            ensureIndexCapacityLocked(recid);
        } finally {
            structuralLock.unlock();
        }
    }

    /** Test hook: current logical end of the volume (last allocated page). */
    long testFileTail() {
        structuralLock.lock();
        try {
            return fileTail();
        } finally {
            structuralLock.unlock();
        }
    }

    /** Test hook: index page offsets in chain order. */
    long[] testIndexPages() {
        return indexPages.clone();
    }

    /**
     * Approximate byte footprint = {@code fileTail - freeDataBytes} (allocated bytes minus
     * reclaimed free space). Page-granular for fresh growth (1 MB steps), record-granular
     * for reused/freed extents; DECREASES on delete so a byte-budget eviction sweep
     * converges. See {@link Store#getCurrentSize()}.
     */
    @Override public long getCurrentSize() {
        structuralLock.lock();
        try {
            return fileTail() - freeDataBytes;
        } finally {
            structuralLock.unlock();
        }
    }

    /** Recompute {@link #freeDataBytes} by walking the free-data long-stacks. Called on
     *  open (single-threaded); {@code compact()} resets it inline instead. */
    private void recomputeFreeDataBytes() {
        ArrayList<long[]> scratch = new ArrayList<>();
        long total = 0;
        for (long u = 1; u <= MAX_CAP_UNITS; u++) {
            long[] count = {0};
            forEachLongStack(masterLinkOffset(u), scratch, v -> count[0]++);
            total += count[0] * (u * 16L);
            scratch.clear();
        }
        freeDataBytes = total;
    }
}
