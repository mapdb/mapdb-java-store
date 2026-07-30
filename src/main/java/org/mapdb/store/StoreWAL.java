package org.mapdb.store;

import org.mapdb.DBException;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;
import org.mapdb.ser.Serializer;
import org.mapdb.store.WalSegmentSet.Segment;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.TreeSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.zip.CRC32;

/**
 * Transactional store: StoreDirect volume + write-ahead log.
 *
 * Uncommitted mutations are staged in memory; commit serializes them as WAL entries
 * (appends logged as deltas), fsyncs (durability point), then applies
 * to the inner StoreDirect. The inner store is memory-backed, so recovery replays
 * all committed WAL sections from the start of the retained log.
 *
 * <h2>On-disk format v3 — a SEGMENT SET, not a file</h2>
 * <pre>
 * name    := &lt;db&gt; ".wal." &lt;16 lowercase hex digits of segmentSeq&gt;   // first segment is 1
 * segment := segmentHeader(36) section*
 * segmentHeader := magic "MDB5.WAL"(8) | version int32 = 3 | flags int32 = 0
 *                | segmentSeq int64 | firstLsn int64 | headerCrc int32
 * section := tag byte ('S' commit, 'C' cleaner/checkpoint image, 'K' clean mark)
 *          | lsn int64 (exactly consecutive across the retained set)
 *          | bodyLen int64 | hdrCrc int32 | bodyCrc int32
 *          | body: entries T_PREALLOC/T_RECORD/T_APPEND/T_DELETE (packLong framing),
 *                  or — for 'K' — exactly cleanedThroughSeq int64 | logStartLsn int64
 * T_APPEND := tag(1) | packLong(recid) | packLong(lsn - baseLsn) | packLong(len) | bytes
 * </pre>
 *
 * <p>The two int64s v3 added — {@code firstLsn} and {@code logStartLsn} — are what let recovery
 * CHECK where the retained log begins instead of inferring it; see {@link #adjudicate}.
 *
 * <p>Prefix truncation of ONE file needs a hole-punch (Linux-only) or a rewrite — which is the
 * cost R2 exists to remove. Segmentation is the portable answer, and it is the reason the log
 * can be bounded by <em>retiring</em> rather than <em>rewriting</em> it. The whole multi-file
 * state machine — directory durability, orphan classification, the clean mark, per-segment
 * torn-tail rules, crash-safe unlink — is implemented here and in {@link WalSegmentSet}, which
 * holds the namespace half. <b>This code is the specification of that state machine.</b> The
 * rule identifiers in the comments below (N1-N7, H1-H8, S1-S9, K1-K9, W1-W10, R0-R7) are
 * stable labels kept so a rule can be discussed by name. This code, not the labels, is
 * the authority.
 *
 * <p><b>Both CRCs are domain-separated</b> by {@code segmentHeader(36) || be64(sectionOffset)}
 * — a plain CRC-32 over a prefix, deliberately not a preloaded register, since
 * {@code java.util.zip.CRC32} cannot express one and every port would have to reimplement the
 * convention. This binds a section to its segment AND its position, which is what rejects a
 * section byte-copied elsewhere.
 *
 * <p>The length-prefixed, CRC-in-header section means: CRCs are validated BEFORE any entry is
 * decoded (garbage never allocates); replay applies entry-by-entry in O(1) memory; a section
 * body may exceed 2 GiB; and a damaged section FOLLOWED by a valid one is distinguishable from
 * a torn tail — mid-log corruption raises {@link DBException.DataCorruption} instead of silently
 * discarding committed history. W3 (rollover only at a section boundary, after the
 * sealed segment's last section is forced) is what lets a tear in a NON-FINAL segment be called
 * corruption outright: only the active segment can legitimately end mid-section.
 *
 * <p>Recovery is STREAMING and runs in two passes over the segment set. Pass 1 is
 * <b>namespace-only</b> — section boundaries, held verdicts, LSN density, the newest {@code 'K'}
 * — and keeps NO per-recid state; a port that reintroduces a lookahead table there has
 * reimplemented the defect §4.2 removed. Pass 2 applies, in ascending (segment, offset) order,
 * the state transition table over two per-recid identities
 * ({@link #contentBaseLsn}, {@link #stateLsn}) plus a deferred skip audit: an append whose base
 * is no longer present is skipped rather than refused, and the open fails only if some skip was
 * never superseded by a later self-contained entry.
 *
 * <p>{@link #checkpoint()} bounds the log by <em>cleaning</em>: it rolls to a fresh segment,
 * writes the whole committed store as one {@code 'C'} image, then a forced {@code 'K'} mark
 * authorizing every older segment's removal, and unlinks them. That is the step-3 cleaner with
 * its budget set to "everything" — the v1 whole-file rewrite and its {@code ATOMIC_MOVE} commit
 * point are gone, along with the {@code .ckpt} temp file.
 */
public class StoreWAL implements StoreDelta, StoreTx {

    private static final int T_PREALLOC = 1, T_RECORD = 2, T_APPEND = 3, T_DELETE = 4;
    /** Classifier-only pseudo-op: recid created AND deleted in one transaction; never logged. */
    private static final int T_TRANSIENT = 0;

    /** Section header: tag(1) + lsn(8) + bodyLen(8) + hdrCrc(4) + bodyCrc(4). */
    static final int SEC_HDR = 25;
    /** Bytes of the section header covered by hdrCrc (tag + lsn + bodyLen). */
    static final int SEC_HDR_CRC_LEN = 17;
    /**
     * The valid tag set is {@code {'S','C','K'}} EVERYWHERE a section is recognized — the main
     * scan and the suspect-header lookahead alike (Q5 §4.1). v1 accepted only {@code 'S'}/{@code 'C'};
     * a port transcribing that treats a valid {@code 'K'} after a rotted section as invalid and
     * so misclassifies mid-log rot as an ordinary torn tail.
     */
    private static final int TAG_SECTION = 'S', TAG_IMAGE = 'C', TAG_MARK = 'K';
    /**
     * A {@code 'K'} body is exactly {@code cleanedThroughSeq int64 | logStartLsn int64} — nothing
     * else, ever. {@code logStartLsn} is the LSN the retained log begins at once this mark's
     * removals are applied, i.e. the LSN of the image that superseded them. Recording it is what
     * lets recovery CHECK the log's lower bound instead of inferring it from the tag of the first
     * retained section.
     */
    private static final int MARK_BODY_LEN = 16;

    /** Default segment size; the writer seals and rolls past this, at a section boundary (§5.1). */
    public static final long DEFAULT_SEGMENT_BYTES = 64L << 20;

    private static final class Staged {
        final boolean created;
        boolean baseSet;
        byte[] base;          // null with baseSet=true means explicit null content
        int headroom;
        boolean deleted;
        final ArrayList<byte[]> appends = new ArrayList<>();
        int appendsLen;

        Staged(boolean created) { this.created = created; }
    }

    /**
     * Classified commit operation, computed before any apply (state must not shift mid-apply).
     *
     * @param baseLsn for {@code T_APPEND} only: the LSN of the content image this delta
     *                extends, read from {@link #contentBaseLsn} at classify time and written
     *                to the log as {@code packLong(sectionLsn - baseLsn)}. 0 for every other
     *                op type, which are all self-contained.
     */
    private record WalOp(int type, long recid, int cap, byte[] data, long baseLsn) {}

    /** Default streaming-replay window (bytes); package-private ctor overrides for tests. */
    static final int DEFAULT_REPLAY_BUF = 1 << 20;

    /**
     * Floor under the cleaning trigger: a log smaller than this is never cleaned, however small the
     * live data is. Without a floor, a store holding a few hundred bytes would clean on every commit.
     */
    public static final long DEFAULT_MIN_LOG_BYTES = 1L << 30;

    /**
     * Default space-amplification target: clean once the log exceeds this multiple of the live data.
     * It bounds <b>space</b>, not write amplification — see the honest-limits note
     * on {@link #cleaningDue}.
     */
    public static final int DEFAULT_SPACE_AMPLIFICATION = 2;

    /**
     * One cleaning tick from the commit path. Bounded so a single commit never pays for a whole
     * segment at once; a commit takes ONE slice, and P7's hard ceiling is the only path that runs
     * cleaning to completion inline, which is what keeps a store with no maintenance thread
     * bounded.
     *
     * <p><b>These numbers are the deliverable, not a detail — step 4 measured that.</b> This was
     * {@code (4096 records, 8 MiB, maxNanos = 0)}, and every part of that was wrong for what the
     * budget is for. 8 MiB is not a bound on a store whose whole log is 8 MiB, 4096 records is
     * more than a retiring segment usually holds, and {@code maxNanos = 0} means <em>no time
     * bound at all</em> — so the field that names the quantity step 3 exists to bound was the one
     * field switched off. Six review rounds went past it, because it is a value rather than a
     * rule and nothing in the code reads wrong.
     *
     * <p>Measured on the store-size sweep in {@code WalPauseTest} (tmpfs, maintenance off, 1 op/tx,
     * 320k entries / 350k commits), old budget → new: commits over 1 ms <b>326 → 1</b>, p99.9
     * 744 µs → 176 µs, max 2 827 µs → 1 610 µs. The cost is ~1% of log high-water (1.00 → 1.00,
     * 1.02 → 1.03 of target at the smaller sizes) and <b>zero</b> device bytes — the byte counts
     * are identical to the byte at two of the three sizes. Before this, the incremental cleaner
     * was <em>worse</em> than the whole-store checkpoint it replaced on every pause percentile
     * below ~320k entries; after it, it is better than that checkpoint at every size measured.
     *
     * <p>{@code maxNanos} is a SOFT ceiling: it is checked between work units, so a single
     * oversize image still runs whole (§5.5's explicit oversize-unit exception). It is a bound on
     * how much work is <em>started</em>, not a deadline.
     */
    private static final MaintenanceBudget FOREGROUND_BUDGET =
            new MaintenanceBudget(0, 256, 512 << 10, 500_000, true);

    /** The budget {@link #checkpoint} runs under: no limit, because the caller asked for all of it. */
    private static final MaintenanceBudget UNBOUNDED_BUDGET =
            new MaintenanceBudget(0, 0, 0, 0, true);

    private StoreDirect inner;
    private final File file;
    private WalSegmentSet segs;
    /** Highest-sequence segment: the only one ever appended to. Touched under the write lock. */
    private Segment activeSeg;
    private long segmentBytes = DEFAULT_SEGMENT_BYTES;
    private final boolean readOnly;
    private final HashMap<Long, Staged> staged = new HashMap<>();
    private final boolean threadSafe;
    private final ReadWriteLock rw;
    /**
     * {@link Store#structuralGeneration()} counter — bumped by {@link #rollback()},
     * which can leave an open collection's append-only structural cache (e.g. a
     * BTreeMap left-edge spine) describing a taller-than-real tree. Read/written
     * atomically because a reader (the collection) may consult it outside the
     * store write lock.
     */
    private final java.util.concurrent.atomic.AtomicLong structGen =
            new java.util.concurrent.atomic.AtomicLong(0);
    private final int replayBufSize;
    private long minLogBytes = DEFAULT_MIN_LOG_BYTES;
    private int spaceAmplification = DEFAULT_SPACE_AMPLIFICATION;
    /** The cleaning cycle in progress, or null when idle. Touched under the write lock only. */
    private Cleaner cleaner;
    /**
     * The active segment when the log first became due; no cycle may select at or above it, so
     * reaching it means the whole log has been rewritten once. 0 = no episode in progress.
     */
    private long cleanFloorSeq;
    /**
     * The cleaner's lifetime retired/written counters as the current episode began. Its achievement
     * is {@code retired - written} over the episode — <b>net progress</b>, not the change in log
     * size, because concurrent commits move the log for reasons that have nothing to do with
     * whether cleaning is working. Measuring the log instead called a write-heavy workload
     * "unachievable" while the cleaner was reclaiming exactly as it should.
     */
    private long episodeRetired, episodeWritten;
    /**
     * Images this episode has re-emitted, and whether its cycles retire the WHOLE pre-floor range
     * rather than one segment at a time. The two go together: a futile INCREMENTAL episode is not a
     * terminal, because retiring one segment per cycle pays a mark per segment, and a mark can cost
     * more than a small segment holds — 65,528 minimum-size segments cannot be compacted one at a
     * time and can be compacted in one pass. So futility escalates before it latches, and only a
     * whole-range episode that gained nothing is evidence the ratio is unachievable.
     */
    private long episodeRecords;
    /**
     * Segments below the floor when this episode's FIRST cycle opened — the range the episode set out
     * to rewrite. The terminal is qualified against this, not against what is left: a completed
     * episode retires prefixes until nothing remains, so its final cycle is always as wide as the
     * remainder, and "the last cycle covered what was left" therefore says nothing about any cycle
     * having been wide enough to amortise its mark.
     */
    private int episodeSegments;
    /**
     * Segments the NEXT cycle may retire in one go, and the counters as the current cycle began.
     *
     * <p>A cycle that retires one segment pays for one mark, and a mark can cost more than a small
     * segment holds: at the minimum segment size a cycle retires ~61 bytes and appends ~107, so
     * one-at-a-time cleaning grows the log forever on a log a single wide pass would collapse. So
     * the width DOUBLES after any cycle that did not pay for itself and resets to one after any
     * that did. Widening is not a longer pause — ticks stay budgeted; it is one mark amortised over
     * more segments.
     */
    private int cycleWidth = 1;
    /**
     * Ceiling on {@link #cycleWidth}. A cycle's CLOSE is not budgeted — {@code finishCycleLocked}
     * sums the retiring prefix and {@code unlinkThrough} closes, deletes and fsyncs every file in
     * it, all under the write lock — so an uncapped width buys mark amortisation with an unbounded
     * pause, which is the trade step 3 exists to refuse. Sixty-four segments is 64x the amortisation
     * of one-at-a-time cleaning and a close of at most 64 file deletions.
     */
    private static final int CYCLE_WIDTH_CAP = 64;
    /** @see #cycleWidth */
    private long cycleRetiredAt, cycleWrittenAt;
    /**
     * The cycle now open runs at the WIDEST width available to it — the cap, or everything left
     * below the floor. Only an episode whose last cycle was saturated may arm the latch: a futile
     * narrow cycle is evidence about the width, not about the log, and "covered the remainder" is
     * not the same claim, because a completed episode's final cycle covers the remainder whatever
     * width it ran at.
     */
    private boolean cycleSaturated, lastCycleSaturated;
    /**
     * Committed self-contained entries over this store's lifetime — every one of which can obsolete
     * an earlier image, which is what makes it the latch's staleness clock. See
     * {@link #beginCycleIfDueLocked()}.
     */
    private long committedStateChanges;
    /**
     * Log size when an episode completed its whole range WITHOUT shrinking the log — the configured
     * ratio is unachievable. Latched so it is not retried on every commit; cleared when the trigger
     * goes quiet, when the configuration changes, or once the log grows a further target's worth.
     * 0 = not latched.
     */
    private long futileAtBytes;
    /** The target when {@link #futileAtBytes} armed; a materially LOWER one re-arms the cleaner. */
    private long futileAtTarget;
    /**
     * {@link #committedStateChanges} when the latch armed, and the images the futile episode
     * re-emitted. A latch is a proof about the log as it stood, and commits invalidate proofs: a
     * mass delete of null-content or preallocated records obsoletes every image in the log while
     * moving neither the log's size nor the target, so neither of the other two release rules can
     * see it. Retrying once per live-set's worth of state changes is the amortisation: the wasted
     * work is one whole-range pass per that many commits.
     */
    private long futileAtChanges, futileRecords;
    /** Lifetime cleaner accounting, both halves — bytes re-emitted and bytes retired (§5.5). */
    private long cleanerBytesWritten, cleanerBytesRetired;
    private long nextLsn = 1;         // next section sequence number (exactly consecutive)
    /**
     * {@code logStartLsn} of the newest valid {@code 'K'}, or 0 when the log carries no mark.
     * Recovery-only: set during pass 1 alongside {@code cleanedThroughSeq}, which it always
     * accompanies, and read by R4 as the log's lower bound.
     */
    private long markLogStartLsn;
    /**
     * LSN the CURRENT transaction will commit at, or 0 when it holds no reservation
     * (R2 §3.2: one LSN per transaction, not per append). Reserving is just READING
     * {@code nextLsn} — it is not incremented until the section is written — so a rollback
     * needs no rewind, it simply drops the reservation.
     *
     * <p>A reservation is taken only by {@link #append(long, StoreDelta.DeltaEncoder)},
     * because only an encoder needs a value to stamp into the frames it writes. The raw
     * byte[] append path never reserves, which is why the existing
     * stage → checkpoint → commit tests are unaffected.
     */
    private long txLsn = 0;
    private volatile boolean closed = false;

    // ---------- the two per-recid identities ----------
    // Both are maintained ATOMICALLY WITH THE COMMITTED APPLY of the entry that sets them —
    // never before, never from staged state — by exactly one helper per §4.2 transition row
    // (identityContent / identityStateOnly / identityVoid). Replay rebuilds them incrementally
    // while applying; the commit classifier and the writer read the live copies. Absent = none.
    //
    // They are NOT the deleted replay floor under another name. The floor was derived by
    // LOOKAHEAD (a pass that scanned ahead for each recid's newest self-contained entry) and
    // then DECIDED what to apply, so a wrong floor silently recovered different data. These are
    // derived purely from what has already been applied, and they decide nothing on their own.

    /**
     * LSN of the content image currently applied for a recid — set by a content-bearing
     * {@code T_RECORD} or {@code 'C'} image, CLEARED by {@code T_DELETE}, by a null-content
     * {@code T_RECORD} and by {@code T_PREALLOC}. Consumed by {@code T_APPEND} stamping
     * (writer) and matching (replay).
     */
    private final HashMap<Long, Long> contentBaseLsn = new HashMap<>();

    /**
     * LSN at which a recid's current state was last made self-contained — set by EVERY
     * self-contained non-void entry (content {@code T_RECORD}, null {@code T_RECORD},
     * {@code T_PREALLOC}, {@code 'C'} image), cleared by {@code T_DELETE}. Consumed by the
     * cleaner's re-emission criterion and by W10.
     */
    private final HashMap<Long, Long> stateLsn = new HashMap<>();

    /**
     * Deferred skip audit: recids whose stranded {@code T_APPEND} replay
     * skipped, minus those a later self-contained entry has since superseded. Non-empty at
     * end of replay = {@code DataCorruption}. It <em>decides</em> nothing — that is the whole
     * point of it replacing the floor — so a wrong audit refuses an open where a wrong floor
     * silently recovered a different state.
     */
    private final java.util.HashSet<Long> skippedAppends = new java.util.HashSet<>();

    // ---------- optional background maintenance (R6; DISABLED by default) ----------
    private MaintenanceExecutor maintenance;
    private boolean ownsMaintenance;
    private MaintenanceExecutor.Handle checkpointHandle;
    private boolean maintenanceShutdown; // guarded by synchronized(this); refuses start/attach racing close

    interface DirectorySync {
        void sync(File dir) throws IOException;
    }

    private static final DirectorySync DEFAULT_DIRECTORY_SYNC = StoreWAL::fsyncDir;
    private static volatile DirectorySync directorySync = DEFAULT_DIRECTORY_SYNC;

    // ---------- the tier-2 seam (Q5 §7 tier 2) ----------

    /**
     * The durability-relevant file operations. One constant per operation the writer obligations
     * are stated over, which is why {@code SEC_HEADER} and {@code SEC_BODY} are separate: a
     * failure between them is a <em>partial section write</em>, and that is precisely the state
     * W9 exists to forbid appending after.
     */
    enum WalOpKind { CREATE, SEG_HEADER, SEC_HEADER, SEC_BODY, FORCE_DATA, FORCE_FULL, TRUNCATE,
        UNLINK, DIRSYNC }

    /**
     * @param seq the segment's sequence number, or {@code 0} for {@code DIRSYNC}
     * @param off byte offset the operation starts at; for a force, the file length it makes durable
     * @param len bytes the operation writes, or {@code 0} where it writes none
     * @param tag the section tag ({@code 'S'}, {@code 'C'}, {@code 'K'}) for section events, else 0
     */
    record WalIoEvent(WalOpKind kind, long seq, long off, long len, int tag) {}

    /**
     * <b>Tier-2 writer fault injection and trace seam</b> (Q5 §7). Called immediately <em>before</em>
     * each operation in {@link WalOpKind}; throwing an {@code IOException} makes that operation fail
     * exactly as the platform would, and returning lets it proceed.
     *
     * <p><b>Why this exists at all.</b> Q5 states five writer obligations — W1, W2, W3, W4 and W5 —
     * plus P11 ("no {@code 'K'} before its attested bytes are forced"), and every one of them is an
     * <em>ordering</em> claim about operations that leave no trace in the resulting bytes. Until this
     * seam they were held by structural argument alone: the calls appear in the right order in the
     * source, and nothing checks that they still do. The cleaner gave P11 a second writer, so the
     * argument came to cover two code paths, which is the point at which "one refactor away from
     * silently false" stopped being a figure of speech.
     *
     * <p><b>What it does NOT model.</b> This is an I/O-failure seam, not a power-loss one: throwing
     * here makes a syscall fail, it does not make already-written bytes vanish. Power-loss images —
     * torn tails at every offset, non-prefix unlink subsets, create-crash residue — are the
     * segment-event enumerator's subject and stay there. A test may however reconstruct the
     * power-loss image implied by a fault point, by truncating each segment to the length its last
     * successful force made durable; that is how P11 becomes an outcome rather than a trace shape.
     */
    interface WalIo {
        void before(WalIoEvent e) throws IOException;
    }

    private static final WalIo NO_WAL_IO = e -> { };
    private static volatile WalIo walIo = NO_WAL_IO;

    /**
     * Reports one operation to the tier-2 seam. Costs a volatile read against a syscall when no
     * injector is installed, which is why it can sit on the commit path unconditionally — a
     * seam that had to be enabled at open would not be exercised by the histories that matter.
     */
    static void walIoEvent(WalOpKind kind, long seq, long off, long len, int tag) throws IOException {
        WalIo io = walIo;
        if (io != NO_WAL_IO) io.before(new WalIoEvent(kind, seq, off, len, tag));
    }

    /** Test hook; {@code null} restores the no-op. */
    static void testSetWalIo(WalIo io) { walIo = io == null ? NO_WAL_IO : io; }

    public StoreWAL(File file) { this(file, false); }

    public StoreWAL(File file, boolean directBuffers) { this(file, directBuffers, true); }

    public StoreWAL(File file, boolean directBuffers, boolean threadSafe) {
        this(file, directBuffers, threadSafe, DEFAULT_REPLAY_BUF);
    }

    /** Test hook: a tiny {@code replayBufSize} forces window-refill edges in streaming replay. */
    StoreWAL(File file, boolean directBuffers, boolean threadSafe, int replayBufSize) {
        this(file, directBuffers, threadSafe, replayBufSize, false);
    }

    /**
     * Opens the store read-only (Q5 R-RO): recovery runs R0, R1, R3, R4 and R6 and computes
     * {@code nextLsn}/{@code nextSeq} by the same rules <em>without acting on them</em>, so <b>no
     * segment is created, truncated or unlinked</b>. By the retained-set-equivalence invariant
     * (Q5 §2.3) this recovers an identical record map; residue and superseded segments simply stay
     * on disk for the next writable open. Every mutating method throws.
     *
     * <p><b>It may create {@code <db>.lock}</b>, which is the one file it can write. §3.1's lock is
     * two-sided — a writer must be rejected while a reader holds the shared lock — and waiting for
     * the file to appear gave only half of that, leaving a reader admitted first to be run over by
     * a later writer's recovery. So "read-only" means read-only with respect to the <em>segment
     * namespace</em>, not "writes nothing to the directory".
     */
    public static StoreWAL openReadOnly(File file) {
        return new StoreWAL(file, false, true, DEFAULT_REPLAY_BUF, true);
    }

    StoreWAL(File file, boolean directBuffers, boolean threadSafe, int replayBufSize, boolean readOnly) {
        this.threadSafe = threadSafe;
        // the WAL-global lock uses segment-rule assertions: at most one held per thread;
        // holding it while calling into the inner StoreDirect is legal (separate instance).
        // Single-entry StampedLock view: never held reentrantly.
        this.rw = new DeadlockAsserts().segment(
                threadSafe ? new java.util.concurrent.locks.StampedLock().asReadWriteLock() : Locks.NO_OP_RW_LOCK);
        this.file = file;
        this.readOnly = readOnly;
        this.inner = new StoreDirect(directBuffers, threadSafe);
        this.replayBufSize = replayBufSize;
        if (!readOnly) requireDurableDirectorySync();
        try {
            this.segs = new WalSegmentSet(file, readOnly);
            recover();
        } catch (IOException e) {
            closeOnFailedOpen();
            throw new DBException("WAL open failed: " + file, e);
        } catch (RuntimeException e) {
            closeOnFailedOpen();
            throw e;
        }
    }

    private void closeOnFailedOpen() {
        if (segs != null) segs.close();
        inner.close();
    }

    // ---------- recovery: Q5 §4, steps R3-R7 (R0-R2 live in WalSegmentSet) ----------

    /**
     * The ordered recovery algorithm. R0-R2 (enumerate, classify, remove create-crash residue)
     * ran in {@link WalSegmentSet}'s constructor; this is the rest, in the normative order:
     *
     * <ol>
     * <li><b>R3</b> pass 1 — namespace only, no per-recid state, per segment: the valid section
     *     prefix, any HELD corruption verdict, LSN continuity; globally the newest {@code 'K'}.</li>
     * <li><b>R4</b> adjudicate — now that {@code cleanedThroughSeq} is known, discard every
     *     verdict from a superseded segment, throw the rest, and check N3 and S9 over the
     *     retained set.</li>
     * <li><b>R5</b> unlink the superseded segments, then fsync the directory.</li>
     * <li><b>R6</b> pass 2 — apply the §4.2 table in ascending (segment, offset) order, then the
     *     skip audit.</li>
     * <li><b>R7</b> finish — {@code nextLsn}, and IFF the active segment's valid prefix is
     *     shorter than its length: truncate, force, rotate (W7), fsync the directory.</li>
     * </ol>
     *
     * <p>R5 before R6 is a performance choice, not a correctness one — by Q5 §2.3 replaying
     * those segments changes nothing — but it is fixed here so the tier-1 fixture oracle can
     * assert an exact file set.
     */
    private void recover() throws IOException {
        if (segs.segments().isEmpty()) {                                   // N1: fresh store
            activeSeg = readOnly ? null : segs.createSegment(1);
            nextLsn = 1;
            inner.rebuildFreeRecids();
            return;
        }
        Segment active = segs.active();
        long cleanedThroughSeq = pass1(active);
        List<Segment> retained = adjudicate(cleanedThroughSeq);
        segs.unlinkThrough(cleanedThroughSeq);                             // R5
        // Q5 R3 phrases this as "the highest LSN of ANY valid section". Taken over the RETAINED
        // set instead, deliberately, and the two agree on every conforming image: K4 puts a
        // mark's own segment above everything it authorizes removing, and LSNs ascend with
        // (segment, offset), so the retained set always holds the maximum. They differ only on a
        // doctored image where a superseded segment carries a spuriously high LSN — and there
        // the global reading would set nextLsn above the retained maximum, leaving a gap that
        // fails S9 on the NEXT open. Deviating toward the safe reading; raised for the ports.
        long maxValidSectionLsn = 0;
        for (Segment s : retained) maxValidSectionLsn = Math.max(maxValidSectionLsn, s.lastLsn);
        // An all-empty retained set holds no LSN to count from, and "0 + 1" would restart the log
        // at 1 — reissuing LSNs a mark already accounted for. The lowest segment's header says
        // where the log begins; that is the answer, and it is why the field is in the header.
        if (maxValidSectionLsn == 0 && !retained.isEmpty())
            maxValidSectionLsn = WalSegmentSet.headerFirstLsn(retained.get(0)) - 1;
        for (Segment s : retained) {
            try {
                pass2(s);                                                  // R6
            } finally {
                if (s != active) s.release();
            }
        }
        // The audit runs BEFORE R7's truncate: an open that refuses must have mutated nothing
        // (Q5 §2.1, and the tier-1 oracle asserts file equality on every corruption row). The
        // bytes a torn tail would lose were never a valid section, so this is conformance and
        // forensics rather than data — but a port that reordered it would fail the fixtures.
        auditSkippedAppends();
        nextLsn = maxValidSectionLsn + 1;                                  // R7
        activeSeg = active;
        if (!readOnly && active.validEnd < active.fileLen) {
            // W7: truncate is not itself forced, so a crash after truncate-then-shorter-reappend
            // can resurface pre-truncation bytes. Force, then rotate, so later appends never
            // reuse this segment's checksum domain at all. Conditional on an ACTUAL truncation:
            // rotating on every open would burn a sequence number per open and demote a
            // legitimate valid-empty highest segment to non-highest (H8).
            walIoEvent(WalOpKind.TRUNCATE, active.seq, active.validEnd, 0, 0);
            active.channel().truncate(active.validEnd);
            active.fileLen = active.validEnd;
            walIoEvent(WalOpKind.FORCE_FULL, active.seq, active.validEnd, 0, 0);
            active.channel().force(true);
            activeSeg = segs.createSegment(nextLsn);
        }
        // replay of delete-then-reuse histories leaves stale free-list entries for
        // revived recids: rebuild the allocator's free list from the final index
        inner.rebuildFreeRecids();
    }

    /**
     * R3, pass 1. Scans every surviving segment in ascending sequence order and, within each,
     * ascending offset, recording per segment its valid section prefix, any corruption verdict
     * (<b>held, not thrown</b> — the segment may be about to be deleted) and its LSN span.
     * Returns the maximum {@code cleanedThroughSeq} over all valid {@code 'K'} marks (K2), or 0
     * when there is none (K1).
     *
     * <p>The mark lives in the torn-tail-prone active segment and may itself be truncated (K3);
     * that is harmless by Q5 §2.3 — a regressed mark only under-collects.
     */
    private long pass1(Segment active) throws IOException {
        long cleanedThroughSeq = 0;
        long lastLsn = 0;
        for (Segment s : segs.segments()) {
            try {
                cleanedThroughSeq = Math.max(cleanedThroughSeq, scanSegment(s, lastLsn, s == active));
            } finally {
                // released the moment this segment's scan is done; pass 2 reopens the ones it needs
                s.release();
            }
            if (s.lastLsn != 0) lastLsn = s.lastLsn;
        }
        return cleanedThroughSeq;
    }

    /**
     * Scans one segment's sections (table S), leaving {@code validEnd}, {@code firstLsn},
     * {@code lastLsn} and at most one held verdict on it. Returns the highest
     * {@code cleanedThroughSeq} attested by a valid {@code 'K'} inside it.
     *
     * <p>{@code lastLsn} enters as the previous segment's last accepted LSN and is used ONLY as
     * the suspect-header lookahead's anchor. <b>The density check deliberately restarts at each
     * segment boundary</b>, and that is not a relaxation: the cross-boundary link is checked at
     * R4, over the RETAINED set alone. Comparing across the boundary here would fail a
     * legitimate crash image — segment 1 present, segment 2 already unlinked, segment 3 carrying
     * the mark that authorized it — because segment 3's first LSN legitimately does not follow
     * segment 1's last. The verdict would originate in a retained segment while being caused by
     * a superseded one, which is exactly the shape R4's "discard verdicts from below the mark"
     * rule cannot rescue.
     *
     * <p>The lookahead itself never crosses a segment boundary either: W3 guarantees a non-final
     * segment ends exactly at a section boundary, so every tear there is corruption and only the
     * active segment needs it.
     */
    private long scanSegment(Segment seg, long lastLsn, boolean isActive) throws IOException {
        long cleanedThroughSeq = 0;
        ByteBuffer hdr = ByteBuffer.allocate(SEC_HDR);
        long pos = WalSegmentSet.SEG_HDR;
        long len = seg.fileLen;
        seg.validEnd = pos;
        while (pos + SEC_HDR <= len) {
            long lsn;
            long bodyLen;
            int tag;
            try {
                readFullyAt(seg.channel(), hdr, pos);
                tag = hdr.get(0) & 0xFF;
                lsn = hdr.getLong(1);
                bodyLen = hdr.getLong(9);
                int storedHdrCrc = hdr.getInt(17);
                int storedBodyCrc = hdr.getInt(21);
                long bodyStart = pos + SEC_HDR;
                CRC32 hcrc = new CRC32();
                seg.crcDomain(hcrc, pos);
                hcrc.update(hdr.array(), 0, SEC_HDR_CRC_LEN);
                boolean hdrOk = (int) hcrc.getValue() == storedHdrCrc && validTag(tag);
                if (!hdrOk) {                                              // S3
                    if (!isActive) return hold(seg, cleanedThroughSeq,
                            "section header damaged at offset " + pos + " in a non-final segment");
                    if (bodyLen >= 0 && bodyLen <= len - bodyStart
                            && anyValidSectionFrom(seg, bodyStart + bodyLen, len, lastLsn, true))
                        return hold(seg, cleanedThroughSeq, "mid-log corruption: section header damaged"
                                + " at offset " + pos + " but valid sections follow (not a torn tail)");
                    return cleanedThroughSeq;                              // torn tail
                }
                if (bodyLen < 0 || bodyLen > len - bodyStart) {            // S5
                    if (!isActive) return hold(seg, cleanedThroughSeq,
                            "section body extends past the end of a non-final segment at offset " + pos);
                    return cleanedThroughSeq;                              // torn tail by construction
                }
                if (bodyCrc(seg, pos, bodyStart, bodyStart + bodyLen) != storedBodyCrc) {  // S4
                    // bodyEnd is TRUSTED (hdrCrc valid): anything valid after it proves bit rot
                    if (!isActive) return hold(seg, cleanedThroughSeq,
                            "section body CRC mismatch at offset " + pos + " in a non-final segment");
                    if (anyValidSectionFrom(seg, bodyStart + bodyLen, len, lastLsn, false))
                        return hold(seg, cleanedThroughSeq, "mid-log corruption: section body CRC"
                                + " mismatch at offset " + pos + " but valid sections follow");
                    return cleanedThroughSeq;                              // torn tail
                }
            } catch (TornTail t) {
                if (!isActive) return hold(seg, cleanedThroughSeq,
                        "non-final segment is shorter than its own sections claim");
                return cleanedThroughSeq;
            }
            // The section is whole. Everything from here is a WRITER-defect class: CRC-valid
            // means these bytes were produced deliberately, so the verdict is corruption rather
            // than a torn tail — but it is still HELD, because this segment may be superseded.
            if (seg.lastLsn != 0) {
                if (lsn <= seg.lastLsn)                                    // S2
                    return hold(seg, cleanedThroughSeq, "section LSN " + lsn + " at offset " + pos
                            + " does not follow " + seg.lastLsn);
                if (lsn != seg.lastLsn + 1)                                // S9
                    // LSNs are DENSE by construction — one per section, the reservation never
                    // burns one, rollback never mints one (§3.2) — so recovery demands them
                    // consecutive rather than merely increasing. A gap is what detects a clean
                    // whose 'C' sections vanished WHOLLY, leaving the predecessor ending at a
                    // clean section boundary so nothing else looks wrong; without it the mark
                    // silently authorizes deleting the only surviving copy of that data.
                    return hold(seg, cleanedThroughSeq, "section LSNs must be consecutive: " + lsn
                            + " at offset " + pos + " after " + seg.lastLsn);
            }
            if (tag == TAG_MARK) {
                if (bodyLen != MARK_BODY_LEN)                              // S8
                    return hold(seg, cleanedThroughSeq,
                            "clean mark body is " + bodyLen + " bytes, not " + MARK_BODY_LEN);
                ByteBuffer body = ByteBuffer.allocate(MARK_BODY_LEN);
                readFullyAt(seg.channel(), body, pos + SEC_HDR);
                long through = body.getLong(0);
                long logStart = body.getLong(8);
                if (through <= 0)                                          // S8
                    return hold(seg, cleanedThroughSeq, "clean mark attests cleanedThroughSeq " + through);
                if (logStart <= 0 || logStart > lsn)                       // S8
                    return hold(seg, cleanedThroughSeq, "clean mark attests logStartLsn " + logStart
                            + ", which is not an LSN at or below the mark's own " + lsn);
                if (through >= seg.seq)                                    // K4
                    return hold(seg, cleanedThroughSeq, "clean mark in segment " + seg.seq
                            + " authorizes removing segment " + through + ", including itself");
                if (through > cleanedThroughSeq) {
                    cleanedThroughSeq = through;
                    markLogStartLsn = logStart;   // always the NEWEST mark's, never a mix
                }
            }
            if (seg.firstLsn == 0) seg.firstLsn = lsn;
            seg.lastLsn = lsn;
            lastLsn = lsn;
            pos += SEC_HDR + bodyLen;
            seg.validEnd = pos;
        }
        if (pos < len && !isActive)                                        // S6, non-final
            return hold(seg, cleanedThroughSeq,
                    "non-final segment has " + (len - pos) + " trailing bytes past its last section");
        return cleanedThroughSeq;
    }

    /** Records a verdict against a segment and stops scanning it; R4 decides whether it matters. */
    private static long hold(Segment seg, long cleanedThroughSeq, String message) {
        if (seg.held == null) seg.held = message;
        return cleanedThroughSeq;
    }

    private static boolean validTag(int tag) {
        return tag == TAG_SECTION || tag == TAG_IMAGE || tag == TAG_MARK;
    }

    /**
     * R4. The retained set is the segments above {@code cleanedThroughSeq}. Verdicts and LSN
     * discontinuities originating below it are DISCARDED — those segments are superseded and about
     * to be deleted, so rot inside them is irrelevant and throwing on it would brick a store over
     * bytes nobody will read.
     *
     * <h2>Every LSN is accounted for by a recorded number, not by inference</h2>
     *
     * <p>Two questions decide whether a retained log is whole: <b>where does it legitimately
     * begin</b>, and <b>is each missing segment authorized</b>. v2 answered both circumstantially —
     * from LSN density, the position of the mark, and the tag of the first retained section — and
     * six defects lived in that reasoning across four revisions, two of them permanent bricks and
     * two silent data loss. The answers are now written down by the writer and merely checked here:
     *
     * <ul>
     * <li>each segment header states {@code firstLsn}, the LSN its first section holds;</li>
     * <li>each {@code 'K'} states {@code logStartLsn}, where the log begins once its removals apply.</li>
     * </ul>
     *
     * <p>So the checks are equalities. The lowest retained segment's stated start must equal the
     * newest mark's {@code logStartLsn}, or 1 when there is no mark. Each subsequent segment's
     * stated start must equal where its present predecessor ended — or, when that predecessor holds
     * no section, where IT said it would start, which is exactly what distinguishes W7's legitimately
     * empty rotate target from a segment whose sections vanished. And a segment must hold what its
     * own header promised.
     *
     * <p><b>A missing sequence number needs no rule at all.</b> If it held sections its successor's
     * stated start will not match its predecessor's end; if it held none, nothing is missing. That is
     * why the sequence numbers W6 burns on create-crash residue are simply invisible here — the
     * inconsistency between W6 and a literal reading of N3, which permanently bricked a store on a
     * one-crash history, cannot arise when the accounting is over LSNs the writer recorded.
     */
    private List<Segment> adjudicate(long cleanedThroughSeq) {
        ArrayList<Segment> retained = new ArrayList<>();
        for (Segment s : segs.segments()) {
            if (s.seq > cleanedThroughSeq) retained.add(s);
        }
        if (retained.isEmpty())
            // Unreachable: K4 makes a mark's own segment outrank everything it authorizes
            // removing, so the segment holding the newest mark is always retained. Asserted
            // rather than assumed, because everything below depends on it.
            throw new DBException.DataCorruption("WAL clean mark " + cleanedThroughSeq
                    + " retires the whole segment set");
        for (Segment s : retained) {
            if (s.held != null)
                throw new DBException.DataCorruption("WAL segment " + s.file.getName() + ": " + s.held);
        }
        // The floor runs ALWAYS, not only when there is no anchor. The two witness different
        // things — the anchor witnesses LSN continuity, the floor witnesses the mark-image contract
        // — and making them alternatives left a hole: a mark with no image behind it, whose
        // superseded segments are still present, satisfies the chain (their data is below the mark,
        // so no LSN is missing) and violates the floor. The open then succeeded, pass 2 replayed
        // only the retained set, and R5 UNLINKED the segments holding the only copy of the data.
        // On every conforming image the floor passes trivially — marked logs begin with their
        // image, markless ones begin at LSN 1 — so there is no false-positive surface to trade.
        // Every LSN in the retained set is now ACCOUNTED FOR by two recorded numbers rather than
        // inferred. Each segment's header states the LSN its first section holds, so:
        //
        //   * the log's start is read off the lowest retained segment and compared with what the
        //     newest mark says it should be (or with 1 when there is no mark);
        //   * each segment's stated start is compared with where its present predecessor actually
        //     ended — or, when that predecessor holds no section, with where IT said it would start,
        //     which is what separates "always empty" (W7's rotate, legal) from "its sections
        //     vanished" (corruption);
        //   * a missing sequence number needs no rule at all. If it held sections, the successor's
        //     stated start will not match its predecessor's end. If it held none, nothing is missing.
        //     That is why W6's burnt residue numbers are simply invisible here.
        long expectedStart = markLogStartLsn > 0 ? markLogStartLsn : 1;
        Segment prev = null;
        for (Segment s : retained) {
            long stated = WalSegmentSet.headerFirstLsn(s);
            if (prev == null) {
                if (stated != expectedStart)
                    throw new DBException.DataCorruption("WAL retained log begins at LSN " + stated
                            + " in " + s.file.getName() + " but "
                            + (markLogStartLsn > 0 ? "the clean mark attests it begins at "
                                                     + markLogStartLsn
                                                   : "an unmarked log must begin at LSN 1")
                            + ": sections below it are gone");
            } else {
                long after = prev.lastLsn != 0 ? prev.lastLsn + 1 : WalSegmentSet.headerFirstLsn(prev);
                if (stated != after)
                    throw new DBException.DataCorruption("WAL segment " + s.file.getName()
                            + " states it begins at LSN " + stated + " but "
                            + prev.file.getName() + " accounts for LSNs up to " + (after - 1)
                            + ": sections between them are gone");
            }
            // A segment must also hold what its own header promised, or its prefix was lost.
            if (s.firstLsn != 0 && s.firstLsn != stated)
                throw new DBException.DataCorruption("WAL segment " + s.file.getName()
                        + " states it begins at LSN " + stated + " but its first section is "
                        + s.firstLsn + ": its leading sections are gone");
            prev = s;
        }
        return retained;
    }



    /**
     * R6, pass 2 over one segment. Pass 1 is the sole authority on section boundaries: this walk
     * re-reads the headers it already validated and never re-derives them, so a disagreement
     * between the two passes is impossible by construction.
     */
    private void pass2(Segment seg) throws IOException {
        ByteBuffer hdr = ByteBuffer.allocate(SEC_HDR);
        WalIn in = new WalIn(seg.channel(), replayBufSize);
        long pos = WalSegmentSet.SEG_HDR;
        while (pos < seg.validEnd) {
            readFullyAt(seg.channel(), hdr, pos);
            int tag = hdr.get(0) & 0xFF;
            long lsn = hdr.getLong(1);
            long bodyLen = hdr.getLong(9);
            long bodyStart = pos + SEC_HDR;
            // A 'K' body carries no entries and is NEVER passed to the entry decoder; 'C' is
            // semantically identical to 'S' and gets no special handling — the retained 'C'
            // sections are collectively the checkpoint, so "restore the newest image" would be
            // meaningless here.
            if (tag != TAG_MARK) applySection(in, bodyStart, bodyStart + bodyLen, lsn);
            pos = bodyStart + bodyLen;
        }
    }

    /**
     * End of replay: every skipped append must have been superseded. A recid
     * still in the set means the retained log contains a delta whose base is gone AND nothing
     * later re-established that recid — the store cannot be reconstructed, so refuse the open
     * rather than silently return a record that is missing acknowledged bytes.
     */
    private void auditSkippedAppends() {
        if (skippedAppends.isEmpty()) return;
        long recid = skippedAppends.iterator().next();
        int n = skippedAppends.size();
        skippedAppends.clear();
        throw new DBException.DataCorruption("WAL replay skipped " + n + " append(s) whose base image"
                + " is absent and which no later entry superseded (recid " + recid + "): the log is"
                + " missing sections it depends on");
    }

    // ---------- streaming decode ----------

    /** Thrown by {@link WalIn} on a read past EOF: torn tail, stop at the last valid commit. */
    private static final class TornTail extends RuntimeException {
        TornTail() { super(null, null, false, false); }
    }

    /**
     * Reads a {@link WalIn} issued, so the cleaner's scan cost is an <b>observable</b> rather than
     * something only a stopwatch can see.
     *
     * <p>It exists because the property worth holding is not "the scan is fast" — a timing
     * assertion measures the machine — but "the scan reads the range a bounded number of times,
     * whatever shape the range has". That is exactly what was false: a log of many small sections
     * was read once per SECTION rather than once per window. Only a counter can state it, and
     * {@code StoreWALCleanerScanCostTest} does.
     */
    static final class ReadCount {
        long n;
    }

    /**
     * Streaming WAL decoder: a fixed-size window over the file channel with long
     * positions, bounded by {@code [start, limit)}. Never materializes the log (or any
     * unbounded slice of it) in memory, so 2 GiB+ segments replay fine. It computes no CRC:
     * a section is verified whole, from its header, before a single entry is decoded.
     */
    private static final class WalIn {
        private final FileChannel ch;
        /** Soft bound: end of the unit being decoded. Governs {@link #seek} and {@link #remaining}. */
        private long limit;
        /**
         * Hard bound: the highest offset {@link #refill} may read to, which is what lets one window
         * span several units. Defaults to {@link #limit}, so a caller that never sets it separately
         * sees exactly the old behaviour.
         */
        private long hardLimit;
        private final ByteBuffer win;
        private long winStart; // file offset of win[0]
        /** Where this decoder reports its reads, or {@code null}. See {@link ReadCount}. */
        private final ReadCount reads;

        WalIn(FileChannel ch, int bufSize) { this(ch, bufSize, null); }

        WalIn(FileChannel ch, int bufSize, ReadCount reads) {
            this.ch = ch;
            this.win = ByteBuffer.allocate(Math.max(16, bufSize));
            win.limit(0); // empty until first refill
            this.reads = reads;
        }

        /** Repositions the cursor to {@code start}, reading no further than {@code end}. */
        void reset(long start, long end) {
            winStart = start;
            limit = end;
            hardLimit = end;
            win.position(0);
            win.limit(0);
        }

        /**
         * Moves to {@code start} and narrows the soft bound to {@code end}, <b>keeping the window
         * when {@code start} is a forward move that lands inside it</b>. Any other move drops it,
         * exactly as {@link #reset} would.
         *
         * <p>This is what makes a section boundary free, and {@link #reset} cannot do it: reset
         * drops the window unconditionally, so a scan that walks many small sections re-reads the
         * bytes it already holds and issues a syscall per section rather than per window. The
         * caller establishes the hard bound once per segment with {@code reset}, then re-bounds
         * per section with this.
         */
        void rebound(long start, long end) {
            // The soft bound may only narrow the hard one. A caller that widened past it would get
            // a window truncated at hardLimit and then a TornTail, which reads as corruption
            // rather than as the misuse it is.
            assert end <= hardLimit : end + " > hardLimit " + hardLimit;
            limit = end;
            long inWindow = start - winStart;
            if (start >= pos() && inWindow <= win.limit()) {
                win.position((int) inWindow);
                return;
            }
            winStart = start;
            win.position(0);
            win.limit(0);
        }

        long pos() { return winStart + win.position(); }

        long remaining() { return limit - pos(); }

        private void refill() {
            if (reads != null) reads.n++;
            winStart = pos();
            // Both bounds, because they can disagree: a section header claiming a body that runs
            // past the segment's validated end would otherwise leave the window empty at every
            // attempt and spin. Before the hard bound existed this read past EOF and the platform
            // reported it; the guard says the same thing without depending on that.
            if (winStart >= limit || winStart >= hardLimit) throw new TornTail();
            win.clear();
            long room = hardLimit - winStart;
            if (room < win.capacity()) win.limit((int) room);
            try {
                long readPos = winStart;
                while (win.hasRemaining()) {
                    int n = ch.read(win, readPos);
                    if (n < 0) throw new TornTail(); // file shrank under us: treat as torn
                    readPos += n;
                }
            } catch (IOException e) {
                throw new DBException("WAL read failed", e);
            }
            win.flip();
        }

        /** Unsigned byte. */
        int readByteRaw() {
            if (!win.hasRemaining()) refill();
            return win.get() & 0xFF;
        }

        long unpackLong() {
            long ret = 0;
            int v;
            do {
                v = readByteRaw();
                ret = (ret << 7) | (v & 0x7F);
            } while ((v & 0x80) == 0);
            return ret;
        }

        void readFully(byte[] b) {
            int off = 0;
            while (off < b.length) {
                if (!win.hasRemaining()) refill();
                int n = Math.min(win.remaining(), b.length - off);
                win.get(b, off, n);
                off += n;
            }
        }

        /**
         * Repositions the cursor to {@code to} and drops the window, WITHOUT reading the bytes in
         * between — what the cleaner's recid-only scans need, since they walk over record payloads
         * they have no use for.
         *
         * <p>Not reading them is the point, not an optimization: a scan that streamed through
         * payloads would cost the full byte size of the sections it walked, and the cleaner's
         * "one bounded unit" would then be bounded only by the log, under the write lock.
         *
         * <p><b>A seek that lands inside the window keeps it.</b> Dropping it unconditionally —
         * as this did — turned "skip the payload" into "re-read the bytes we already hold": with
         * entries shorter than the window, every single one forced a fresh {@code pread} of the
         * same 4 KiB, so the cleaner's two scans issued one syscall per ENTRY rather than one per
         * window and read an order of magnitude more than the range contains. This never reads a
         * byte the old form would not have read; it only declines to read one twice.
         */
        void seek(long to) {
            if (to < pos() || to > limit)
                throw new DBException.DataCorruption("WAL entry length runs past its section: "
                        + to + " not in [" + pos() + ", " + limit + "]");
            long inWindow = to - winStart;                  // >= win.position(), by the check above
            if (inWindow <= win.limit()) {
                win.position((int) inWindow);
                return;
            }
            winStart = to;
            win.position(0);
            win.limit(0);
        }
    }

    private static void readFullyAt(FileChannel ch, ByteBuffer b, long pos) throws IOException {
        b.clear();
        long p = pos;
        while (b.hasRemaining()) {
            int n = ch.read(b, p);
            if (n < 0) throw new TornTail();
            p += n;
        }
        b.flip();
    }

    /** CRC32 over a section body, domain-separated by the segment identity and section offset. */
    private int bodyCrc(Segment seg, long sectionOffset, long start, long end) throws IOException {
        CRC32 crc = new CRC32();
        seg.crcDomain(crc, sectionOffset);
        if (start < end) {
            ByteBuffer buf = ByteBuffer.allocate((int) Math.min(Math.max(16, replayBufSize), end - start));
            long p = start;
            while (p < end) {
                buf.clear();
                if (end - p < buf.capacity()) buf.limit((int) (end - p));
                int n = seg.channel().read(buf, p);
                if (n < 0) throw new TornTail();
                p += n;
                buf.flip();
                crc.update(buf);
            }
        }
        return (int) crc.getValue();
    }

    /**
     * True when {@code [from, limit)} of this segment holds at least one fully valid section,
     * proving that durable committed sections follow a bad one (=> corruption, not a torn tail).
     * With {@code exactNext} (untrusted anchor: the damaged section's own bodyLen) the candidate
     * must carry EXACTLY the next expected LSN ({@code lastLsn + 2} — the damaged section was
     * {@code lastLsn + 1}); otherwise (trusted anchor: hdrCrc-sealed bodyEnd, so a real section
     * boundary) any strictly future LSN counts. Both reject "embedded fake" byte patterns from
     * user data containing copies of EARLIER sections: stale copies carry old LSNs, and under
     * the §6.2 CRC domain a copied section also fails its CRCs at any other offset.
     *
     * <p>Never crosses a segment boundary — {@code limit} is this segment's length.
     */
    private boolean anyValidSectionFrom(Segment seg, long from, long limit, long lastLsn,
                                        boolean exactNext) throws IOException {
        long pos = from;
        try {
            ByteBuffer hdr = ByteBuffer.allocate(SEC_HDR);
            while (pos + SEC_HDR <= limit) {
                readFullyAt(seg.channel(), hdr, pos);
                int tag = hdr.get(0) & 0xFF;
                long lsn = hdr.getLong(1);
                long bodyLen = hdr.getLong(9);
                long bodyStart = pos + SEC_HDR;
                CRC32 hcrc = new CRC32();
                seg.crcDomain(hcrc, pos);
                hcrc.update(hdr.array(), 0, SEC_HDR_CRC_LEN);
                if ((int) hcrc.getValue() != hdr.getInt(17) || !validTag(tag)
                        || bodyLen < 0 || bodyLen > limit - bodyStart)
                    return false;
                boolean lsnOk = exactNext ? lsn == lastLsn + 2 : lsn > lastLsn + 1;
                if (lsnOk && bodyCrc(seg, pos, bodyStart, bodyStart + bodyLen) == hdr.getInt(21))
                    return true;
                pos = bodyStart + bodyLen;
            }
        } catch (TornTail t) {
            // ran off the end: nothing valid there
        }
        return false;
    }

    /**
     * Decodes and applies one CRC-verified section body as the STATE TRANSITION
     * TABLE; malformed entries = writer bug/corruption. Sections are applied in ascending
     * (segment, offset) order, which is ascending LSN order, and {@code lsn} is the enclosing
     * section's.
     *
     * <p>Every row states what happens to BOTH identities, because getting that wrong is how
     * the in-memory tables desynchronize from the store:
     *
     * <pre>
     * entry                 precondition                          action           contentBase  state   skip
     * T_RECORD content      —                                     walPut           = lsn        = lsn   clear
     * T_RECORD null         —                                     walPut(null)     cleared      = lsn   clear
     * T_PREALLOC            R is not content-live                 walPrealloc      cleared      = lsn   clear
     * T_PREALLOC            R IS content-live                     DataCorruption
     * T_DELETE              —                                     walDelete        cleared      cleared clear
     * T_APPEND              baseLsn == contentBase[R]             append           unchanged    unch.   unch.
     * T_APPEND              contentBase[R] absent or > baseLsn    SKIP             unchanged    unch.   add R
     * T_APPEND              contentBase[R] < baseLsn              DataCorruption
     * </pre>
     *
     * A superseded {@code T_RECORD} is RE-APPLIED rather than skipped, which is correct
     * because it is idempotent, and costs only recovery-time work.
     */
    private void applySection(WalIn in, long start, long end, long lsn) {
        in.reset(start, end);
        // Decoder rule: at most one entry per recid per section, for 'C' sections as well as
        // 'S'. The classifier coalesces every append() call for a recid into one entry, so a
        // second entry would mean the ordered-replay reasoning above no longer applies to
        // this section.
        java.util.HashSet<Long> seen = new java.util.HashSet<>();
        try {
            while (in.pos() < end) {
                int type = in.readByteRaw();
                switch (type) {
                    case T_PREALLOC -> {
                        long recid = once(seen, in.unpackLong());
                        // walPrealloc no-ops on ANY set slot, so applying it to a content-live
                        // record would silently leave a record that is still there while the
                        // identities describe a preallocated one. The precondition is stated
                        // as "not content-live" rather than "void or already P" to be TOTAL:
                        // a null-content target matches neither of those and must not fall
                        // through undefined (ports would diverge on doctored images).
                        if (inner.recState(recid) == StoreDirect.STATE_LIVE)
                            throw new DBException.DataCorruption(
                                    "WAL PREALLOC over a content-live record, recid=" + recid);
                        inner.walPrealloc(recid);
                        identityStateOnly(recid, lsn);
                    }
                    case T_DELETE -> {
                        long recid = once(seen, in.unpackLong());
                        inner.walDelete(recid); // no-op on void: the skipped-append history
                        identityVoid(recid);
                    }
                    case T_RECORD -> {
                        long recid = once(seen, in.unpackLong());
                        long cap = in.unpackLong();
                        long lenPlus = in.unpackLong();
                        byte[] data = null;
                        if (lenPlus != 0) {
                            long len = lenPlus - 1;
                            if (len < 0 || len > Integer.MAX_VALUE || len > in.remaining())
                                throw new DBException.DataCorruption("bad WAL record length " + len);
                            data = new byte[(int) len];
                            in.readFully(data);
                        }
                        if (!capValid(cap, data))
                            throw new DBException.DataCorruption("bad WAL record capacity " + cap);
                        inner.walPut(recid, (int) cap, data);
                        if (data == null) identityStateOnly(recid, lsn);
                        else identityContent(recid, lsn);
                    }
                    case T_APPEND -> {
                        long recid = once(seen, in.unpackLong());
                        long baseLsn = decodeBaseLsn(in.unpackLong(), lsn, recid);
                        long len = in.unpackLong();
                        if (len < 0 || len > Integer.MAX_VALUE || len > in.remaining())
                            throw new DBException.DataCorruption("bad WAL append length " + len);
                        Long base = contentBaseLsn.get(recid);
                        if (base != null && base < baseLsn)
                            // unreachable in a conforming set (retirement is a prefix in LSN
                            // order, so a base below the current one cannot be the missing
                            // part), and defence in depth over the LSN density rule
                            throw new DBException.DataCorruption("WAL append cites base LSN " + baseLsn
                                    + " above the applied base " + base + ", recid=" + recid
                                    + ": sections are missing");
                        byte[] data = new byte[(int) len];
                        in.readFully(data); // consumed either way: the frame is still framed
                        if (base == null || base > baseLsn) {
                            // the base this delta extends is gone (cleaned, or superseded by a
                            // newer image that already contains these bytes): skip and remember
                            skippedAppends.add(recid);
                        } else {
                            long r = inner.append(recid, data, 0, data.length);
                            if (r == StoreDelta.REFUSED)
                                throw new DBException.DataCorruption("WAL append refused, recid=" + recid);
                        }
                    }
                    default -> throw new DBException.DataCorruption("bad WAL entry tag " + type);
                }
            }
        } catch (TornTail t) {
            throw new DBException.DataCorruption("WAL entry overran its section body at " + end);
        }
    }

    /** Enforces the one-entry-per-recid-per-section decoder rule. */
    private static long once(java.util.HashSet<Long> seen, long recid) {
        if (!seen.add(recid))
            throw new DBException.DataCorruption("two WAL entries for recid " + recid + " in one section");
        return recid;
    }

    /**
     * Turns the encoded {@code packLong(lsn - baseLsn)} back into an absolute base LSN,
     * BEFORE any mutation. The delta must be ≥ 1 — so an append can never cite a base in its
     * own section, and {@code baseLsn < lsn} always — and must leave a base LSN ≥ 1, since
     * LSNs start at 1. Both bounds are what make the comparison in the table meaningful
     * instead of an accidental "skip" on a garbage value.
     */
    private static long decodeBaseLsn(long delta, long lsn, long recid) {
        if (delta < 1 || delta > lsn - 1)
            throw new DBException.DataCorruption("bad WAL append base delta " + delta
                    + " in section LSN " + lsn + ", recid=" + recid);
        return lsn - delta;
    }

    // ----- the §4.2 identity transitions: exactly one helper per row shape -----

    /** A content-bearing image: both identities move to this section's LSN. */
    private void identityContent(long recid, long lsn) {
        contentBaseLsn.put(recid, lsn);
        stateLsn.put(recid, lsn);
        skippedAppends.remove(recid);
    }

    /**
     * A self-contained entry that leaves the record with NO content image — a null
     * {@code T_RECORD} or a {@code T_PREALLOC}. Merely declining to set a new content base is
     * not enough: a recid that was content-live and became null or preallocated would keep a
     * stale base, and a later writer could then stamp an append from a state in which append
     * is not valid.
     */
    private void identityStateOnly(long recid, long lsn) {
        contentBaseLsn.remove(recid);
        stateLsn.put(recid, lsn);
        skippedAppends.remove(recid);
    }

    /** The record is gone: both identities are cleared, and any pending skip is discharged. */
    private void identityVoid(long recid) {
        contentBaseLsn.remove(recid);
        stateLsn.remove(recid);
        skippedAppends.remove(recid);
    }

    /**
     * Capacity as the writer encodes it: 0 for null content, else 16-aligned, big
     * enough for header+content, within the plain-record limit — EXCEPT oversize
     * (linked) records, which the writer encodes with capacity 0 (the plain
     * capacity model does not apply to a chunk chain; the layout is re-chosen on
     * replay). Anything else never came from this writer — reject before the
     * (int) cast can distort it.
     */
    private static boolean capValid(long cap, byte[] data) {
        if (data == null) return cap == 0;
        if (cap == 0) return 4L + data.length > IndexVal.MAX_CAPACITY;
        return cap >= 4L + data.length && cap <= IndexVal.MAX_CAPACITY && (cap & 15) == 0;
    }

    // ---------- helpers ----------

    private void checkClosed() {
        if (closed) throw new DBException.StoreClosed();
    }

    private void checkWritable() {
        if (readOnly) throw new DBException("WAL store " + file + " is open read-only");
    }

    private static <R> byte[] toBytes(R record, Serializer<R> ser) {
        if (record == null) return null;
        DataOutput2 out = new DataOutput2(Math.max(16, ser.sizeHint()));
        ser.serialize(out, record);
        return out.copyBytes();
    }

    /** Merged content = (staged base or inner content) ++ staged appends; null = null record. */
    private byte[] merged(long recid, Staged s) {
        byte[] base = s.baseSet ? s.base
                : (inner.recState(recid) == StoreDirect.STATE_LIVE ? inner.rawGet(recid) : null);
        if (base == null && s.appends.isEmpty()) return null;
        int len = (base == null ? 0 : base.length) + s.appendsLen;
        byte[] m = new byte[len];
        int p = 0;
        if (base != null) {
            System.arraycopy(base, 0, m, 0, base.length);
            p = base.length;
        }
        for (byte[] a : s.appends) {
            System.arraycopy(a, 0, m, p, a.length);
            p += a.length;
        }
        return m;
    }

    /** True for a recid the current, uncommitted transaction allocated. */
    private boolean stagedCreated(long recid) {
        Staged s = staged.get(recid);
        return s != null && s.created;
    }

    private Staged stagedForWrite(long recid) {
        Staged s = staged.get(recid);
        if (s != null) {
            if (s.deleted) throw new DBException.GetVoid(recid);
            return s;
        }
        if (inner.recState(recid) == StoreDirect.STATE_VOID) throw new DBException.GetVoid(recid);
        s = new Staged(false);
        staged.put(recid, s);
        return s;
    }

    // ---------- Store ----------

    @Override public long preallocate() {
        rw.writeLock().lock();
        try {
            checkClosed();
            checkWritable();
            long recid = inner.preallocate();
            staged.put(recid, new Staged(true));
            return recid;
        } finally {
            rw.writeLock().unlock();
        }
    }

    @Override public <R> long put(R record, Serializer<R> serializer) {
        byte[] bytes = toBytes(record, serializer);
        rw.writeLock().lock();
        try {
            checkClosed();
            checkWritable();
            long recid = inner.preallocate();
            Staged s = new Staged(true);
            s.baseSet = true;
            s.base = bytes;
            staged.put(recid, s);
            return recid;
        } finally {
            rw.writeLock().unlock();
        }
    }

    @Override public <R> R get(long recid, Serializer<R> serializer) {
        rw.readLock().lock();
        try {
            checkClosed();
            Staged s = staged.get(recid);
            if (s == null) return inner.get(recid, serializer);
            if (s.deleted) throw new DBException.GetVoid(recid);
            byte[] m = merged(recid, s);
            if (m == null) return null;
            return serializer.deserialize(new DataInput2.ByteArray(m, 0), m.length);
        } finally {
            rw.readLock().unlock();
        }
    }

    @Override public long read(long recid, RecordRead action) {
        rw.readLock().lock();
        try {
            checkClosed();
            Staged s = staged.get(recid);
            if (s == null) return inner.read(recid, action);
            if (s.deleted) throw new DBException.GetVoid(recid);
            byte[] m = merged(recid, s);
            // staged reads copy; post-commit reads are zero-copy via inner
            return m == null ? action.onNull() : action.onBytes(new DataInput2.ByteArray(m, 0), m.length);
        } finally {
            rw.readLock().unlock();
        }
    }

    @Override public <R> void update(long recid, R record, Serializer<R> serializer) {
        updateWithHeadroom(recid, record, serializer, 0);
    }

    @Override public <R> void updateWithHeadroom(long recid, R record, Serializer<R> serializer, int headroom) {
        if (headroom < 0) throw new IllegalArgumentException("negative headroom: " + headroom);
        byte[] bytes = toBytes(record, serializer);
        // fail at update time, not commit time; long math so nothing wraps an int.
        // Oversize CONTENT is fine (stored linked at commit) — but content that fits
        // a plain record must also fit with its headroom, because linked records
        // take no appends and silently going linked would break the guarantee.
        if (bytes != null && 4L + bytes.length <= IndexVal.MAX_CAPACITY
                && ((4L + bytes.length + headroom + 15) & ~15L) > IndexVal.MAX_CAPACITY)
            throw new DBException.RecordTooLarge(4L + bytes.length + headroom);
        rw.writeLock().lock();
        try {
            checkClosed();
            checkWritable();
            Staged s = stagedForWrite(recid);
            s.baseSet = true;
            s.base = bytes;
            s.headroom = headroom;
            s.appends.clear();
            s.appendsLen = 0;
        } finally {
            rw.writeLock().unlock();
        }
    }

    @Override public <R> boolean compareAndSwap(long recid, R expectedOldRecord, R newRecord, Serializer<R> serializer) {
        rw.writeLock().lock();
        try {
            checkClosed();
            checkWritable();
            R current;
            Staged s = staged.get(recid);
            if (s == null) {
                if (inner.recState(recid) == StoreDirect.STATE_VOID) throw new DBException.GetVoid(recid);
                current = inner.get(recid, serializer);
            } else {
                if (s.deleted) throw new DBException.GetVoid(recid);
                byte[] m = merged(recid, s);
                current = m == null ? null : serializer.deserialize(new DataInput2.ByteArray(m, 0), m.length);
            }
            boolean eq = (current == null && expectedOldRecord == null)
                    || (current != null && expectedOldRecord != null && serializer.equals(current, expectedOldRecord));
            if (!eq) return false;
            Staged sw = stagedForWrite(recid);
            sw.baseSet = true;
            sw.base = toBytes(newRecord, serializer);
            sw.headroom = 0;
            sw.appends.clear();
            sw.appendsLen = 0;
            return true;
        } finally {
            rw.writeLock().unlock();
        }
    }

    @Override public <R> void delete(long recid, Serializer<R> serializer) {
        rw.writeLock().lock();
        try {
            checkClosed();
            checkWritable();
            Staged s = stagedForWrite(recid);
            s.deleted = true;
            s.baseSet = false;
            s.base = null;
            s.appends.clear();
            s.appendsLen = 0;
        } finally {
            rw.writeLock().unlock();
        }
    }

    // ---------- StoreDelta ----------

    @Override public long append(long recid, byte[] data, int offset, int len) {
        rw.writeLock().lock();
        try {
            checkClosed();
            checkWritable();
            return appendLocked(recid, data, offset, len);
        } finally {
            rw.writeLock().unlock();
        }
    }

    /**
     * Write lock must be held. Shared by both append forms — the encoder form must NOT call
     * the public one, because {@code rw} is {@code DeadlockAsserts.segment}-wrapped and
     * re-entering it trips the reentrancy assert.
     */
    private long appendLocked(long recid, byte[] data, int offset, int len) {
        // Validate the caller's arguments BEFORE staging anything. Staging used to happen
        // first, so a bad offset/length threw out of the middle of the method and left an
        // empty Staged entry behind — which the classifier then emitted as T_PREALLOC over a
        // still content-live record, and the reopen after the next (acknowledged!) commit
        // refused the whole log. A rejected call must leave the transaction as it found it.
        //
        java.util.Objects.requireNonNull(data, "data");
        java.util.Objects.checkFromIndexSize(offset, len, data.length);
        // A zero-length append stages NOTHING — not even the empty Staged entry that
        // stagedForWrite() would leave behind on an otherwise untouched record, which would
        // turn a contract-defined no-op into a T_PREALLOC section burning an LSN. (It still
        // runs stagedForWrite for its GetVoid validation.)
        boolean wasStaged = staged.containsKey(recid);
        Staged s = stagedForWrite(recid);
        // Staging an empty byte[] also used to emit a T_APPEND at commit that inner.append
        // refuses on a linked record, tripping the "commit append refused" assertion
        // (DeltaTCK.zero_length_append_after_commit_is_noop).
        if (len == 0 && !wasStaged) staged.remove(recid);
        if (len != 0) {
            if (!s.baseSet && inner.recState(recid) == StoreDirect.STATE_LIVE) {
                // base capacity is already fixed in inner: enforce refusal now
                long capRem = inner.capacityRemaining(recid);
                if (s.appendsLen + len > capRem) {
                    // REFUSED is a no-op, so it must stage NOTHING — the same rule as the
                    // zero-length append above. The empty Staged entry left behind by the
                    // earlier code was classified as T_PREALLOC at commit: it burnt an LSN,
                    // and under v2 it is a PREALLOC naming a content-live record, which
                    // replay must reject (§4.2). Found by the small-model enumerator at
                    // depth 5 — put, commit, append-past-capacity, commit, reopen.
                    if (!wasStaged) staged.remove(recid);
                    return StoreDelta.REFUSED;
                }
            }
            byte[] copy = java.util.Arrays.copyOfRange(data, offset, offset + len);
            s.appends.add(copy);
            s.appendsLen += len;
        }
        byte[] base = s.baseSet ? s.base
                : (inner.recState(recid) == StoreDirect.STATE_LIVE ? inner.rawGet(recid) : null);
        return (base == null ? 0 : base.length) + s.appendsLen;
    }

    /**
     * Encode-and-append with a real, transaction-scoped LSN (R2 §3.2/§3.3). The store mints
     * the value; the format writes it while encoding, so no opaque byte is ever patched.
     *
     * <p>Every frame written by every encoder append in one transaction carries the SAME
     * LSN — the one the transaction's commit section will carry. That is deliberate: R3
     * then sees whole committed transactions rather than an arbitrary prefix of one, and
     * ties inside a page break on append position, exactly as R1 specifies.
     */
    @Override public long append(long recid, StoreDelta.DeltaEncoder enc) {
        rw.writeLock().lock();
        try {
            checkClosed();
            checkWritable();
            // Reserve before encoding: the encoder needs the value. nextLsn is only READ
            // here; the section that consumes it is written by commit().
            long lsn = txLsn != 0 ? txLsn : nextLsn;
            DataOutput2 out = new DataOutput2(64);
            enc.encode(out, lsn);                     // may throw: nothing staged yet
            long r = appendLocked(recid, out.buf, 0, out.pos);
            if (r != StoreDelta.REFUSED) txLsn = lsn; // hold the reservation only on success
            return r;
        } finally {
            rw.writeLock().unlock();
        }
    }

    /**
     * True while a transaction holds an LSN reservation. Maintenance must not publish a
     * section in that window: it would consume {@code nextLsn}, and the frames already
     * encoded with the reserved value would then disagree with the LSN their commit section
     * finally carries (R2 §3.2 — this is the defect that killed per-append LSNs).
     */
    private boolean lsnReserved() { return txLsn != 0; }

    @Override public long capacityRemaining(long recid) {
        rw.readLock().lock();
        try {
            checkClosed();
            Staged s = staged.get(recid);
            if (s == null) return inner.capacityRemaining(recid);
            if (s.deleted) throw new DBException.GetVoid(recid);
            if (s.baseSet) return Long.MAX_VALUE; // capacity established at commit
            if (inner.recState(recid) == StoreDirect.STATE_LIVE)
                return inner.capacityRemaining(recid) - s.appendsLen;
            return Long.MAX_VALUE;
        } finally {
            rw.readLock().unlock();
        }
    }

    // ---------- the segment writer (W1-W4, W7, W9) ----------

    /**
     * Appends one complete section to the active segment and forces it, rolling over first when
     * the segment is full. This is the ONLY place a section is written, which is what makes the
     * writer obligations checkable in one spot:
     *
     * <ul>
     * <li><b>W1/W4</b> — the force happens before this method returns, so no section is ever
     *     appended before its predecessor's force completed. §6.1's mid-log-rot inference
     *     ("a valid section follows an invalid one ⇒ corruption") is sound only under it:
     *     batching sections under one fsync lets writeback reorder, and recovery would then
     *     refuse an ordinary torn tail.</li>
     * <li><b>W3</b> — rollover happens only at a section boundary, after the sealed segment's
     *     last section is forced with a SIZE-persisting force. So a non-final segment ends
     *     exactly at a section boundary with zero trailing bytes, which is what lets a tear
     *     there be corruption without a lookahead.</li>
     * <li><b>W9</b> — a failed or partial write/force fails the store CLOSED. The v1 code threw
     *     without closing, leaving partial garbage at the write position on an open store; the
     *     caller's retry then wrote a complete, forced, ACKNOWLEDGED section after the garbage
     *     at {@code lastLsn+1}, while the suspect-header lookahead demands exactly
     *     {@code lastLsn+2} — so the next open truncated at the garbage and discarded the
     *     acknowledged commit. A latent v1 defect, fixed here as a writer obligation.</li>
     * </ul>
     */
    private void appendSection(int tag, long lsn, byte[] body, int bodyLen) {
        try {
            if (activeSeg.fileLen >= segmentBytes && !activeSeg.empty()) rollover(lsn);
            long off = activeSeg.fileLen;
            ByteBuffer hdr = ByteBuffer.allocate(SEC_HDR);
            hdr.put((byte) tag).putLong(lsn).putLong(bodyLen);
            CRC32 hcrc = new CRC32();
            activeSeg.crcDomain(hcrc, off);
            hcrc.update(hdr.array(), 0, SEC_HDR_CRC_LEN);
            CRC32 bcrc = new CRC32();
            activeSeg.crcDomain(bcrc, off);
            bcrc.update(body, 0, bodyLen);
            hdr.putInt((int) hcrc.getValue()).putInt((int) bcrc.getValue()).flip();
            long p = off;
            walIoEvent(WalOpKind.SEC_HEADER, activeSeg.seq, p, SEC_HDR, tag);
            p += writeFullyAt(activeSeg.channel(), hdr, p);
            walIoEvent(WalOpKind.SEC_BODY, activeSeg.seq, p, bodyLen, tag);
            writeFullyAt(activeSeg.channel(), ByteBuffer.wrap(body, 0, bodyLen), p);
            // force(false) — a DATA sync. This relies on the POSIX guarantee that fdatasync
            // persists "the metadata required to retrieve the data", which for an append means the
            // new file size; FileChannel.force(false)'s own javadoc promises less than that, so the
            // reliance is named here rather than left implicit for the ports. Where the SIZE itself
            // is the payload — creating a segment, sealing one at rollover — force(true) is used
            // instead and the distinction is deliberate (W2/W3).
            walIoEvent(WalOpKind.FORCE_DATA, activeSeg.seq, off + SEC_HDR + bodyLen, 0, tag);
            activeSeg.channel().force(false);
            activeSeg.fileLen = off + SEC_HDR + bodyLen;
            activeSeg.validEnd = activeSeg.fileLen;
        } catch (IOException | RuntimeException e) {
            failClosed("WAL write failed", e);
        }
    }

    /**
     * W3 + the force-flavor rule: seal the active segment with a force that persists SIZE
     * metadata, then create the successor (W2: create → header → force(true) → directory fsync).
     * {@code force(false)} would be wrong here — this is a four-language contract, and W3's
     * entire load collapses if a port's data-only sync loses a sealed segment's tail extent,
     * because recovery would then see a torn NON-FINAL segment and refuse a legitimate image.
     */
    private void rollover(long firstLsn) throws IOException {
        walIoEvent(WalOpKind.FORCE_FULL, activeSeg.seq, activeSeg.fileLen, 0, 0);
        activeSeg.channel().force(true);
        activeSeg = segs.createSegment(firstLsn);
    }

    /**
     * The store cannot be made consistent again: close it rather than let a caller retry into a
     * segment holding partial bytes. Durable state on disk is intact and reopen replays it.
     */
    private void failClosed(String what, Throwable cause) {
        closed = true;
        try {
            segs.close();
        } catch (RuntimeException ignored) { }
        try {
            inner.close();
        } catch (RuntimeException ignored) { }
        throw new DBException(what + "; store closed, reopen to recover the durable sections", cause);
    }

    // ---------- StoreTx ----------

    @Override public void commit() {
        rw.writeLock().lock();
        try {
            checkClosed();
            checkWritable();
            if (staged.isEmpty()) return;

            // classify all ops BEFORE applying any (apply shifts inner state)
            long[] recids = staged.keySet().stream().mapToLong(Long::longValue).sorted().toArray();
            ArrayList<WalOp> ops = new ArrayList<>(recids.length);
            for (long recid : recids) {
                Staged s = staged.get(recid);
                if (s.deleted) {
                    if (!s.created) ops.add(new WalOp(T_DELETE, recid, 0, null, 0));
                    // created+deleted in one transaction: apply-only cleanup, not logged
                    else ops.add(new WalOp(T_TRANSIENT, recid, 0, null, 0));
                } else if (!s.baseSet && s.appends.isEmpty()) {
                    // T_PREALLOC exists to make a NEWLY ALLOCATED recid durable. On a record
                    // that was already committed, an empty staged entry means nothing was
                    // changed, so nothing is logged — structural defence in depth for the
                    // defect above: no path that leaves an empty entry behind can turn it
                    // into a prealloc over a live record, which §4.2 rejects on replay.
                    if (s.created) ops.add(new WalOp(T_PREALLOC, recid, 0, null, 0));
                } else if (s.baseSet || inner.recState(recid) != StoreDirect.STATE_LIVE) {
                    byte[] m = merged(recid, s);
                    // long math on purpose; an over-capacity merged record is logged
                    // with capacity 0 and stored as a LINKED chain on apply/replay
                    long capL = m == null ? 0 : (4L + m.length + s.headroom + 15) & ~15L;
                    if (capL > IndexVal.MAX_CAPACITY) {
                        // Headroom is a HINT; the record is the promise. A staged base reports
                        // unlimited capacity, so an append can push the merged content to the
                        // plain maximum and the requested headroom then overflows it. Falling
                        // to cap 0 — the "oversize, store it linked" code — made the writer
                        // acknowledge a commit the decoder rejects as a garbage capacity
                        // (capValid allows 0 only when the CONTENT itself is oversize), so the
                        // log became unopenable. Clamp instead: the record stays plain and its
                        // capacity stays exact, which is what a later T_APPEND needs. Only
                        // genuinely oversize content still goes linked.
                        capL = 4L + m.length <= IndexVal.MAX_CAPACITY ? IndexVal.MAX_CAPACITY : 0;
                    }
                    ops.add(new WalOp(T_RECORD, recid, (int) capL, m, 0));
                } else {
                    byte[] m = new byte[s.appendsLen];
                    int p = 0;
                    for (byte[] a : s.appends) {
                        System.arraycopy(a, 0, m, p, a.length);
                        p += a.length;
                    }
                    // This branch is reached only when the record is committed, content-bearing
                    // and plain (!baseSet && recState == STATE_LIVE), which is exactly the shape
                    // that has a content base — so the identity must be there. Its absence is a
                    // writer bug, and the design's weakest point is a WRONG stamp, so refuse to
                    // invent one: a delta with a fabricated base is a silent-loss channel.
                    Long base = contentBaseLsn.get(recid);
                    if (base == null)
                        throw new AssertionError("no content base LSN for appended recid " + recid);
                    ops.add(new WalOp(T_APPEND, recid, 0, m, base));
                }
            }

            // WAL v2 section: header(tag,lsn,bodyLen,crcs) + entries; fsync = durability point.
            // The section LSN is decided here, so the base deltas below are
            // relative to a value that cannot change under them.
            long sectionLsn = nextLsn;
            DataOutput2 out = new DataOutput2(1024);
            for (WalOp op : ops) {
                switch (op.type()) {
                    case T_PREALLOC, T_DELETE -> {
                        out.writeByte(op.type());
                        out.packLong(op.recid());
                    }
                    case T_RECORD -> {
                        out.writeByte(T_RECORD);
                        out.packLong(op.recid());
                        out.packLong(op.cap());
                        if (op.data() == null) out.packLong(0);
                        else {
                            out.packLong(op.data().length + 1);
                            out.write(op.data());
                        }
                    }
                    case T_APPEND -> {
                        out.writeByte(T_APPEND);
                        out.packLong(op.recid());
                        // base identity, as a delta against this section's own LSN (§4.2):
                        // >= 1 by construction, because the base was established by a
                        // strictly earlier section, and typically one byte because a hot
                        // record's base is recent. Envelope, not payload: the encoder and
                        // the frame bytes it produced are untouched.
                        long delta = sectionLsn - op.baseLsn();
                        if (delta < 1) throw new AssertionError("append base LSN " + op.baseLsn()
                                + " is not below its section LSN " + sectionLsn + ", recid=" + op.recid());
                        out.packLong(delta);
                        out.packLong(op.data().length);
                        out.write(op.data());
                    }
                    case T_TRANSIENT -> { /* not logged */ }
                    default -> throw new AssertionError();
                }
            }
            appendSection(TAG_SECTION, sectionLsn, out.buf, out.pos);
            nextLsn++;
            // The latch's staleness clock (see `futileAtChanges`). SELF-CONTAINED entries only: an
            // append extends a record whose image is already the log's youngest, so it obsoletes
            // nothing, while a record, a delete and a prealloc each supersede whatever stood before.
            for (WalOp op : ops)
                if (op.type() == T_RECORD || op.type() == T_DELETE || op.type() == T_PREALLOC)
                    committedStateChanges++;

            // Apply to the inner volume. PAST THE DURABILITY POINT: the section is on disk
            // and owns an LSN, so if any apply throws, memory and log have diverged and the
            // handle can never be made consistent again — a retried commit() would reuse the
            // already-encoded frames under a NEW section LSN, and the forced section would be
            // applied twice on reopen. Fail closed; the durable state on disk is intact and
            // reopen replays it correctly.
            try {
                for (WalOp op : ops) {
                    // Each case updates the identities by the SAME §4.2 transition row replay
                    // would take for the entry just written — that shared table is what keeps
                    // the live maps and a rebuilt-from-log copy identical.
                    switch (op.type()) {
                        case T_TRANSIENT -> {
                            inner.delete(op.recid(), null); // created+deleted: free the P recid
                            // nothing was logged, so nothing established an identity for this
                            // incarnation; clearing is defensive, not load-bearing
                            identityVoid(op.recid());
                        }
                        case T_PREALLOC -> {
                            /* already P in inner since op time */
                            identityStateOnly(op.recid(), sectionLsn);
                        }
                        case T_RECORD -> {
                            inner.walPut(op.recid(), op.cap(), op.data());
                            if (op.data() == null) identityStateOnly(op.recid(), sectionLsn);
                            else identityContent(op.recid(), sectionLsn);
                        }
                        case T_APPEND -> {
                            long r = inner.append(op.recid(), op.data(), 0, op.data().length);
                            if (r == StoreDelta.REFUSED) throw new AssertionError("commit append refused, recid=" + op.recid());
                            // an append leaves both identities where they are: the base image
                            // it extends is still the one a later append must cite
                        }
                        case T_DELETE -> {
                            inner.delete(op.recid(), null);
                            identityVoid(op.recid());
                        }
                    }
                }
            } catch (Throwable t) {
                closed = true;
                segs.close();
                try { inner.close(); } catch (RuntimeException ignored) { }
                throw new DBException("WAL commit failed after the durability point; "
                        + "store closed, reopen to recover the committed section", t);
            }
            staged.clear();
            // Release the reservation BEFORE any maintenance: the commit section is durable,
            // so a clean may now consume the next LSN. Doing it in the other order is the P7
            // hard-ceiling trap — the writer that needs space would block itself
            // (R2 §3.2).
            txLsn = 0;
            autoCleanLocked();
        } finally {
            rw.writeLock().unlock();
        }
    }

    @Override public void rollback() {
        rw.writeLock().lock();
        try {
            checkClosed();
            for (var e : staged.entrySet()) {
                if (e.getValue().created) inner.delete(e.getKey(), null); // free the P recid
            }
            staged.clear();
            // NEITHER IDENTITY MOVES ON ROLLBACK, and that is the whole rule: both are set
            // only by a committed apply, so a transaction that never committed established
            // nothing to undo. The `created` recids deleted just above were preallocated in
            // inner at op time but never logged, so they hold no identity either — the
            // asymmetry is only apparent (see the T_TRANSIENT case in commit).
            // Drop the reservation. No rewind is needed: reserving only READ nextLsn, and
            // the frames encoded with it were staged, never written to the log.
            txLsn = 0;
        } finally {
            rw.writeLock().unlock();
        }
        // Signal open collections (after the write lock releases, mirroring the rust
        // StoreWAL) that their append-only structural caches may now describe a
        // taller-than-real tree — a reverted uncommitted grow.
        structGen.incrementAndGet();
    }

    @Override public long structuralGeneration() { return structGen.get(); }

    // ---------- cleaning (log compaction) ----------

    /**
     * Cleans the log <b>all the way down</b>: retire every segment below a freshly rolled one, by
     * re-emitting above it a self-contained image of every record those segments still own, then a
     * forced {@code 'K'} mark authorizing their removal, then the unlink. Staged (uncommitted)
     * mutations are untouched — they exist only in memory and are not part of any log.
     *
     * <p>This is the incremental cleaner with its budget set to "everything", which is the only
     * sense in which a whole-store checkpoint still exists: the mechanism is
     * {@link #cleanTickLocked}, and a full clean differs from a background tick only in how many
     * segments one cycle retires and how long it is allowed to run. The v1 whole-file rewrite, its
     * {@code .ckpt} temp file and its {@code ATOMIC_MOVE} commit point are gone, and so is the
     * step-2 stand-in that wrote the entire store as one image on every trigger.
     *
     * <p>Safe to call at any time EXCEPT while a transaction holds an LSN reservation (i.e.
     * after an {@link StoreDelta#append(long, StoreDelta.DeltaEncoder)} and before
     * commit/rollback): cleaning consumes LSNs, which would strand the frames already encoded
     * with the reserved value. Commit or roll back first. Transactions that used only the raw
     * byte[] append path hold no reservation and are unaffected.
     */
    public void checkpoint() {
        rw.writeLock().lock();
        try {
            checkClosed();
            checkWritable();
            if (lsnReserved())
                throw new DBException("checkpoint while a delta transaction holds LSN "
                        + txLsn + "; commit or rollback first");
            cleanWholeLogLocked();
        } finally {
            rw.writeLock().unlock();
        }
    }

    @Override public void compact() {
        checkpoint();
    }

    // ---------- background maintenance wiring (R6) ----------

    /**
     * Enable background maintenance: an OWNED {@link MaintenanceExecutor} running the budgeted
     * {@link WalCleanerTask}. Idempotent. The task now does the <em>same</em> work commit does,
     * one bounded tick at a time — it is no longer a low-frequency policy owner standing in front
     * of a whole-store rewrite. Commit's inline clean remains the synchronous correctness fallback
     * (P7), so a store with no maintenance thread is still bounded.
     */
    public synchronized MaintenanceExecutor startMaintenance() {
        checkClosed();
        checkWritable();
        if (maintenanceShutdown) throw new DBException.StoreClosed();
        if (maintenance == null) {
            maintenance = new MaintenanceExecutor("wal");
            ownsMaintenance = true;
            checkpointHandle = maintenance.register(new WalCleanerTask(this));
        }
        return maintenance;
    }

    /** Register the cleaner task into an externally-owned executor (close cancels, does not shut down). */
    public synchronized void attachMaintenance(MaintenanceExecutor executor) {
        checkClosed();
        checkWritable();
        if (maintenanceShutdown) throw new DBException.StoreClosed();
        if (executor == null) throw new NullPointerException("executor");
        if (checkpointHandle != null) throw new IllegalStateException("maintenance already attached");
        maintenance = executor;
        ownsMaintenance = false;
        checkpointHandle = executor.register(new WalCleanerTask(this));
    }

    /**
     * One budgeted cleaning tick, called by {@link WalCleanerTask} off the executor thread.
     * Returns the bytes it wrote, or {@code -1} when there was nothing to do — which the task
     * reports as "no progress" so the executor backs off.
     *
     * <p>A cycle already in progress is continued even if the log has since dropped back under
     * target: its images are already written and paid for, so the only thing left is the mark and
     * the unlink, and abandoning it would leave the log larger than finishing it.
     */
    long maintenanceCleanStep(MaintenanceBudget budget) {
        rw.writeLock().lock();
        try {
            if (closed || readOnly) return -1;
            // Defer rather than strand a transaction's already-encoded frames (R2 §3.2, C15).
            // Deferral is always safe under P7 — the task simply retries next tick.
            if (lsnReserved()) return -1;
            // Every unit of cleaning ends in a forced section; there is no useful work to do
            // under a no-fsync budget, and buffering one would break W1/W5.
            if (!budget.mayFsync) return -1;
            // START a cycle when one is due, exactly as the commit path does. Leaving this out is
            // what made the whole R6 rewiring inert: the task fell through to a tick that had no
            // cycle to advance, returned 0 bytes, and the executor read that as progress-with-more
            // -to-come and re-ran it immediately — a maintenance thread spinning on a core while
            // the log grew untouched. Pinned by
            // the_background_task_retires_a_segment_on_its_own.
            if (cleaner == null && !beginCycleIfDueLocked()) return -1;
            return cleanTickLocked(budget);
        } catch (IOException e) {
            failClosed("WAL cleaning failed: " + file, e);
            return -1;   // unreachable: failClosed always throws
        } finally {
            rw.writeLock().unlock();
        }
    }

    /** Stop the owned executor (or cancel the attached task) BEFORE close takes the WAL write lock. */
    private void stopMaintenanceForClose() {
        MaintenanceExecutor ex;
        boolean owns;
        MaintenanceExecutor.Handle h;
        synchronized (this) {
            if (maintenanceShutdown) return; // idempotent
            maintenanceShutdown = true;      // refuses any start/attach that races this close
            ex = maintenance;
            owns = ownsMaintenance;
            h = checkpointHandle;
            checkpointHandle = null;
            maintenance = null;
        }
        if (ex == null) return;
        if (owns) ex.shutdown(true);
        else if (h != null) h.cancel();
    }

    /**
     * Sets the floor under the cleaning trigger; {@code <= 0} disables automatic cleaning entirely
     * (explicit {@link #checkpoint()} still works). See {@link #cleaningDue} for the trigger this
     * takes part in.
     */
    public void setMinLogBytes(long bytes) {
        rw.writeLock().lock();
        try {
            checkClosed();
            this.minLogBytes = bytes;
            // Both, not just the latch: the floor is the previous episode's high-water mark, and
            // leaving it behind makes the very next attempt latch again on `lowest >= floor` — so
            // raising the target would NOT resume cleaning.
            abandonEpisodeLocked();
        } finally {
            rw.writeLock().unlock();
        }
    }

    /**
     * Sets the space-amplification target: cleaning starts once the log exceeds this multiple of
     * the live data. Must be at least 1 — a factor of 0 would ask for a log smaller than the data
     * it must be able to reconstruct, which no amount of cleaning can deliver.
     */
    public void setSpaceAmplification(int factor) {
        if (factor < 1) throw new IllegalArgumentException("space amplification below 1: " + factor);
        rw.writeLock().lock();
        try {
            checkClosed();
            this.spaceAmplification = factor;
            abandonEpisodeLocked();     // see setMinLogBytes: the episode must go with it
        } finally {
            rw.writeLock().unlock();
        }
    }

    /**
     * True when cleaning has rewritten the whole log once and the trigger is <em>still</em> live —
     * the configured {@link #setSpaceAmplification} ratio is below what any log for this data can
     * achieve, so cleaning has stopped rather than spin. It clears itself when the trigger goes
     * quiet, when either setting changes, or when the log doubles.
     *
     * <p>This is the terminal "no reclaimable window" outcome, reported rather than
     * thrown: the state is a healthy fully-compacted log meeting an impossible target, and it is
     * reached on the commit path, after the durability point, where an exception would report a
     * durable commit as failed.
     */
    public boolean cleaningExhausted() {
        rw.readLock().lock();
        try { return futileAtBytes > 0; } finally { rw.readLock().unlock(); }
    }

    /** Bytes the cleaner has re-emitted, and bytes it has retired, over this store's lifetime (§5.5). */
    public long cleanerBytesWritten() {
        rw.readLock().lock();
        try { return cleanerBytesWritten; } finally { rw.readLock().unlock(); }
    }

    /** @see #cleanerBytesWritten() */
    public long cleanerBytesRetired() {
        rw.readLock().lock();
        try { return cleanerBytesRetired; } finally { rw.readLock().unlock(); }
    }

    /**
     * Sets the size past which the writer seals the active segment and rolls to the next one.
     * Test hook and tuning knob; the rollover itself always happens at a section boundary (W3),
     * so a single section larger than this simply gets a segment of its own.
     */
    public void setSegmentBytes(long bytes) {
        if (bytes < WalSegmentSet.SEG_HDR + SEC_HDR)
            throw new IllegalArgumentException("segmentBytes too small: " + bytes);
        rw.writeLock().lock();
        try {
            checkClosed();
            this.segmentBytes = bytes;
        } finally {
            rw.writeLock().unlock();
        }
    }

    /**
     * The cleaning trigger: the log is due for cleaning once it exceeds
     * {@code max(minLogBytes, spaceAmplification × liveDataBytes)}. Write lock must be held.
     *
     * <p>This replaced step 2's {@code max(autoCheckpointBytes, 2 × size-after-last-clean)}, which
     * needed a "size right after the last whole-store rewrite" basis that an incremental cleaner
     * never produces — there is no moment at which the log equals the live data. Asking the inner
     * store what it currently holds needs no such moment and is the quantity the target was always
     * a proxy for.
     *
     * <p><b>What {@code liveDataBytes} actually is, and its known bias.</b> It is
     * {@link Store#getCurrentSize()} on the inner store — allocated bytes minus reclaimed — which
     * is <em>page-granular</em>: it includes the ~512 KiB header and rounds to 1 MiB slices, so it
     * reports about 2 MiB for a store holding 200 bytes. The ratio is therefore a log-versus-store
     * -footprint ratio, exact at scale and conservative below a few MiB, where it delays cleaning
     * rather than hastening it. That direction is the safe one, but it means {@link #setMinLogBytes}
     * cannot act as an absolute cap on a tiny store: below ~2 MiB of footprint the amplification
     * term, not the floor, decides. An exact "bytes a full clean would write" figure would need
     * per-recid size tracking the identity maps do not carry; it is not worth a third map for a
     * trigger, and the bias is recorded here rather than discovered later.
     *
     * <p><b>It bounds SPACE, not WRITE amplification, and the difference is not academic.</b>
     * Cleaning strictly the oldest segment is FIFO, not cost-benefit: for a cold-head workload that
     * segment is ~100% live and re-emitting it buys nothing this cycle (the classic LFS
     * {@code 1/(1−u)} cost as {@code u → 1}). Re-emitted bytes are not bounded by the retired
     * segment's size either, since an image folds in deltas that live above it. R2 keeps
     * oldest-first as an explicit trade-off with the pathological case named rather than hidden.
     */
    private boolean cleaningDue() {
        if (minLogBytes <= 0) return false;
        return segs.logBytes() > cleaningTarget();
    }

    /** The size the log is allowed to reach. Write lock must be held. */
    private long cleaningTarget() {
        long live = inner.getCurrentSize();
        long scaled = live <= Long.MAX_VALUE / spaceAmplification
                ? spaceAmplification * live : Long.MAX_VALUE;
        return Math.max(minLogBytes, scaled);
    }

    /**
     * Opens a cleaning cycle over the oldest retirable segment IF the trigger is live. Write lock
     * must be held. Returns whether a cycle is now open.
     *
     * <p>This is the single entry point for starting one, shared by commit's inline clean and the
     * background task. It used to live only on the commit path, which meant
     * {@link #maintenanceCleanStep} could never create the state it needed and the task spun
     * reporting progress it had not made — the whole R6 rewiring did nothing.
     *
     * <h2>Episodes, and the terminal outcome §5.5 asks for</h2>
     *
     * An <b>episode</b> begins by SEALING the active segment and taking its fresh successor as
     * {@code cleanFloorSeq}. No cycle may select at or above that floor, so everything that existed
     * when the episode began is retirable and nothing the episode itself writes is: once the lowest
     * present segment reaches the floor, the episode has rewritten the whole log exactly once and is
     * over. Sealing is what makes that true — using the <em>pre-existing</em> active segment as the
     * floor left it untouched and called the result exhaustion, on a log an explicit
     * {@code checkpoint()} then compacted by 40×. It also subsumes the single-segment case: only
     * segments below the active one are retirable, so with {@code segmentBytes} above the trigger a
     * log would otherwise grow forever with no candidate at all.
     *
     * <p><b>The terminal is FUTILITY, not reaching the floor</b>, and the difference is the whole
     * point. An episode that reclaimed bytes and ended is a success; the right response is another
     * episode, which can then reclaim the traffic that arrived while it ran. Latching on
     * "reached the floor" instead suppressed cleaning after every SUCCESSFUL episode — including
     * above the hard ceiling, where a writer is supposed to be made to participate, so the ceiling
     * was not a ceiling. So the latch arms only when an episode returns
     * to its floor having <b>retired no more than it re-emitted</b>: that is the "no net progress
     * over a bounded window", measured over exactly one window rather than guessed at.
     *
     * <p>§5.5 names an explicit {@code DBException} as that terminal. A latch is used instead,
     * because the state is a healthy fully-compacted log meeting an impossible ratio rather than a
     * store-full condition, and because this runs <em>after</em> commit's durability point where
     * throwing would report a durable commit as failed. It is observable through
     * {@link #cleaningExhausted()}.
     *
     * <p>The latch is not permanent, and it must clear on <b>either side of the ratio</b>: when the
     * trigger goes quiet, when the configuration changes, once the log has grown by a further
     * <b>whole target's worth</b> since it armed — enough new traffic that a fresh episode has
     * something to work on, expressed in the trigger's own unit rather than a constant — or when the
     * <b>target itself drops materially</b>. That last one is not symmetry for its own sake: a large
     * delete lowers the live footprint and makes the images already in the log reclaimable
     * <em>without the log moving at all</em>, so waiting only on growth leaves a store that deleted
     * most of its data and then went quiet latched forever. Because a futile episode proves the log cannot shrink, the urgent path
     * respects the latch too, and that is what stops its loop from spinning forever on a log no
     * amount of cleaning can compact.
     */
    private boolean beginCycleIfDueLocked() throws IOException {
        if (!cleaningDue()) {
            // The trigger went quiet. The EPISODE is not over: keeping its floor across a dip is
            // what stops a workload hovering around the target from opening a new episode — and so
            // paying a fresh seal, a create and a directory fsync — every few commits. On a
            // pure-overwrite workload that is 75 episodes per 220 commits against 7. Its segments
            // are above the floor either way, so nothing is selected that should not be. Only the
            // futility latch is released, because a quiet trigger means the situation changed.
            clearLatchLocked();
            return false;
        }
        if (futileAtBytes > 0) {
            long room = cleaningTarget();
            long retry = futileAtBytes <= Long.MAX_VALUE - room ? futileAtBytes + room : Long.MAX_VALUE;
            // A MATERIAL drop, not any drop. The target is the inner store's footprint, and an
            // ordinary update moves it by a couple of hundred bytes in either direction as the
            // allocator reuses extents — one 60 KB update was measured moving a 3 MB target by 160.
            // Releasing on that is releasing always, which is not a latch. An eighth is a threshold
            // like any other; what is not arbitrary is that it must sit above allocator jitter and
            // below a delete large enough to make the log worth re-walking.
            long dropped = futileAtTarget - (futileAtTarget >> 3);
            boolean grew = segs.logBytes() >= retry;
            boolean shrank = room <= dropped;
            // ...and neither of those can see a state-only mass delete, which obsoletes every image
            // in the log while moving neither number. The proof is stale once as many entries have
            // been committed as the futile episode re-emitted.
            boolean churned = committedStateChanges - futileAtChanges >= futileRecords;
            if (!grew && !shrank && !churned) return false;
            clearLatchLocked();     // the floor is already 0: only a COMPLETED episode latches,
                                    // and completing clears it
        }
        if (cleanFloorSeq == 0) {
            // The seal is CLEANING's cost, not the writer's: this rollover exists only to give the
            // episode a floor, and its 36-byte successor header was invisible to every tick because
            // no tick performs it.
            long sealBefore = segs.logBytes(), sealRetired = cleanerBytesRetired;
            if (!activeSeg.empty()) rollover(nextLsn);
            chargeCleanerLocked(sealBefore, sealRetired);
            cleanFloorSeq = activeSeg.seq;
            episodeRetired = cleanerBytesRetired;
            episodeWritten = cleanerBytesWritten;
        }
        List<Segment> all = segs.segments();
        // BINARY search for the floor, not a walk. The walk is O(segments below the floor) and runs
        // at every cycle start, which is every commit or two — the same O(segments)-per-commit shape
        // an earlier fix removed from `logBytes()` and `unlinkThrough`, reintroduced by the width search
        // needing to know how many segments there are. Sequence numbers ascend, so the boundary is
        // one bisection. (Found while auditing the preceding fix.)
        int lo = 0, hi = all.size();
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (all.get(mid).seq < cleanFloorSeq) lo = mid + 1; else hi = mid;
        }
        int below = lo;
        if (below == 0 || below >= all.size()) {
            endEpisodeLocked();          // the episode has rewritten everything it could
            return false;
        }
        // One segment per cycle, or as many as the width search has reached. A wide cycle is not a
        // wider PAUSE — it is still driven in budgeted ticks — and it is the only way to amortise one
        // mark over many segments.
        if (episodeSegments == 0) episodeSegments = below;
        int width = Math.min(Math.min(Math.max(1, cycleWidth), CYCLE_WIDTH_CAP), below);
        // SATURATED = as wide as this episode is ever allowed to go, measured against the range it
        // STARTED with. Against the remainder it would be vacuous: the final cycle of a completed
        // episode is always as wide as what is left of it.
        cycleSaturated = width >= Math.min(CYCLE_WIDTH_CAP, episodeSegments);
        startCycleLocked(all.get(width - 1).seq);
        return true;
    }

    /**
     * Ends the current episode, having walked its whole range. Write lock must be held.
     *
     * <p>Called ONLY on completion, because only a completed episode says anything: it is the
     * "bounded window" §5.5's no-net-progress terminal is measured over. An episode interrupted by
     * a quiet trigger is not evidence of anything and keeps its floor.
     */
    /**
     * Did re-emitting {@code written} bytes to retire {@code retired} pay for itself? A GAIN OF AN
     * EIGHTH of what was written, not merely a positive one.
     *
     * <p>Epsilon progress is not progress. At the minimum segment size, one-at-a-time cleaning was
     * measured re-emitting 174 KB to reclaim 179 KB — a 33x write amplification for a 3% gain — and
     * a strictly-positive test calls that success, so the cleaner runs forever, the log grows at
     * traffic rate, and the terminal §5.5 requires is never reached. The threshold turns "did it
     * move at all" into "was it worth the device writes", which is the question both callers are
     * actually asking.
     */
    private static boolean paidForItself(long retired, long written) {
        return retired - written > (written >> 3);
    }

    /** Releases the futility latch, whatever armed it. Write lock must be held. */
    private void clearLatchLocked() {
        futileAtBytes = 0;
        futileAtTarget = 0;
        futileAtChanges = 0;
        futileRecords = 0;
    }

    /**
     * Abandons the current episode WITHOUT judging it — for a configuration change or an explicit
     * {@code checkpoint()}, after which nothing it observed is still about the same store. Write
     * lock must be held.
     */
    private void abandonEpisodeLocked() {
        clearLatchLocked();
        cleanFloorSeq = 0;
        episodeRetired = 0;
        episodeWritten = 0;
        episodeRecords = 0;
        episodeSegments = 0;
        cycleWidth = 1;
        lastCycleSaturated = false;
    }

    private void endEpisodeLocked() {
        boolean futile = cleanFloorSeq != 0
                && !paidForItself(cleanerBytesRetired - episodeRetired,
                                  cleanerBytesWritten - episodeWritten);
        if (futile && lastCycleSaturated) {
            futileAtBytes = Math.max(1, segs.logBytes());
            futileAtTarget = cleaningTarget();
            futileAtChanges = committedStateChanges;
            futileRecords = Math.max(1, episodeRecords);
        }
        cleanFloorSeq = 0;
        episodeRetired = 0;
        episodeWritten = 0;
        episodeRecords = 0;
        episodeSegments = 0;
        // WITH the episode, not across it. A guard that outlives what it describes is this file's
        // recurring defect — an earlier floor and terminal were both this shape — and here
        // it would let an episode that did NO work (nothing below its floor, so `futile` is trivially
        // true) arm the terminal on the strength of a previous episode's wide last cycle. Reset, so
        // arming always requires THIS episode to have concluded from a wide cycle. `cycleWidth` does
        // persist deliberately: the width a log needs is a property of the log, not of one episode.
        lastCycleSaturated = false;
    }

    /**
     * Commit's inline clean (P7). Write lock must be held, and the caller must already have
     * released any LSN reservation.
     *
     * <p>Runs bounded ticks in a loop rather than one unbounded pass, so the cost lands on the
     * commit that crossed the threshold in slices instead of as one whole-store rewrite.
     *
     * <p>Gated on the trigger ALONE, never on "a cycle is open". Continuing an open cycle here
     * regardless would mean the first commit after a background tick started one dragged it to
     * completion synchronously — moving the work back onto the commit path, which is the whole
     * thing step 3 removes. An abandoned cycle costs nothing durable: its images are forced and its
     * retired segments simply stay, so the log carries duplicates until someone finishes it, and
     * the next tick or the next live trigger does. <em>(A previous comment here claimed a fired
     * trigger necessarily stays fired. It does not: a commit adds page-rounded bytes to the store
     * and framed bytes to the log, so it can raise the threshold faster than the log.)</em>
     */
    private void autoCleanLocked() {
        try {
            if (!cleaningDue()) {
                clearLatchLocked();     // see beginCycleIfDueLocked: the floor outlives a dip
                return;
            }
            // ONE bounded slice per commit. This method runs inside commit's write-lock hold and
            // cannot release it, so a loop here is one uninterrupted hold for the whole pass: the
            // per-tick budget would bound an internal iteration while the commit that triggered it
            // still paid for all of them, consecutively, with every reader and writer waiting. That
            // is the pause step 3 exists to remove, reappearing on the fallback path.
            // Convergence does not need the loop: a slice re-emits up to 8 MiB and can
            // close a cycle, which retires a whole segment, so it reclaims far more than one commit
            // adds for any ordinary workload.
            if (cleaner != null || beginCycleIfDueLocked()) cleanTickLocked(FOREGROUND_BUDGET);
            // The exception is P7's hard ceiling. Once the log has run away — past twice its
            // target — the writer participates until it is back under, and the pause is accepted
            // deliberately: an unbounded pause is the lesser evil against an unbounded log, and
            // this is exactly the case P7 says the committing writer must run cleaning inline for.
            while (cleaningUrgent() && (cleaner != null || beginCycleIfDueLocked()))
                cleanTickLocked(FOREGROUND_BUDGET);
        } catch (IOException e) {
            failClosed("WAL cleaning failed: " + file, e);
        }
    }

    /**
     * P7's hard ceiling: the log is past twice what the trigger allows, so bounding the pause has
     * stopped being the priority. Write lock must be held.
     */
    private boolean cleaningUrgent() {
        if (minLogBytes <= 0) return false;
        long target = cleaningTarget();
        long ceiling = target <= Long.MAX_VALUE / 2 ? 2 * target : Long.MAX_VALUE;
        return segs.logBytes() > ceiling;
    }

    /**
     * {@link #checkpoint}'s body. Write lock must be held.
     *
     * <p>Rolling first is what makes this a whole-log clean: every section-bearing segment is then
     * strictly below the active one, so a single cycle whose target is {@code activeSeg.seq - 1}
     * retires all of them, and its re-emission set — every recid whose state entry is below the
     * fresh segment's first LSN — is the whole committed store. One cycle, one mark, one unlink,
     * through exactly the machinery a background tick uses.
     */
    private void cleanWholeLogLocked() {
        try {
            while (cleaner != null) cleanTickLocked(UNBOUNDED_BUDGET);   // finish a partial cycle
            long sealBefore = segs.logBytes(), sealRetired = cleanerBytesRetired;
            if (!activeSeg.empty()) rollover(nextLsn);
            chargeCleanerLocked(sealBefore, sealRetired);    // see beginCycleIfDueLocked's seal
            long target = activeSeg.seq - 1;
            // Below FIRST_SEQ there is no segment to retire: the active segment is the store's
            // first and it is empty, so the log is already as small as it can be.
            if (target < WalSegmentSet.FIRST_SEQ || segs.segments().size() < 2) return;
            startCycleLocked(target);
            while (cleaner != null) cleanTickLocked(UNBOUNDED_BUDGET);
            abandonEpisodeLocked();  // an explicit full clean re-arms the automatic one
        } catch (IOException e) {
            // IOException ONLY — the rollover above is the sole source, and a half-created segment
            // is not recoverable in place. A DBException from inside the cycle must propagate
            // instead: W10's refusal says "nothing has been deleted; the durable log is intact",
            // and failing the store closed on it would make that sentence false.
            failClosed("WAL cleaning failed: " + file, e);
        }
    }

    /**
     * A cleaning cycle in progress: retire every segment with {@code seq <= targetSeq} by
     * re-emitting, above them, a self-contained image of every record whose state still lives
     * inside them. Resumable across ticks with any budget (C11); every field is touched under the
     * WAL write lock.
     */
    private static final class Cleaner {
        /** {@code cleanedThroughSeq} the closing {@code 'K'} will attest. */
        final long targetSeq;
        /** {@code logStartLsn} the closing {@code 'K'} will attest — the successor's stated start. */
        final long logStartLsn;
        /**
         * The last LSN the retiring range accounts for, {@code logStartLsn - 1}.
         *
         * <p>Deriving the re-emission boundary from the number the mark will record — rather than
         * from the retiring segment's own {@code lastLsn} — is deliberate: it makes the writer's
         * obligation and recovery's check two readings of ONE value, and it is total over the
         * empty-segment case, where a {@code lastLsn} of 0 says nothing.
         */
        final long boundaryLsn;

        /** Phase 1 (re-emit) has walked the whole range. */
        boolean published;
        /** Phase 2 (W10) has walked it again. */
        boolean verified;

        // ---- the scan cursor, used by phase 1 and then reset and reused by phase 2 ----
        /** Index into {@code segs.segments()}; the retiring range is a prefix of it. */
        int seg;
        /** Offset of the next SECTION to enter within {@link #seg}. */
        long offset;
        /** Offset of the next ENTRY inside the section being walked, or -1 between sections. */
        long entryPos = -1;
        /** End of the section body being walked. */
        long bodyEnd;
        /** Decoder for {@link #seg}. One per segment; see {@link #SCAN_BUF}. */
        WalIn in;
        /** Reusable landing area for a section header, so the walk allocates nothing per section. */
        final byte[] secHdr = new byte[SEC_HDR];
        /** The current walk has reached the top of the retiring range. */
        boolean rangeDone;

        Cleaner(long targetSeq, long logStartLsn) {
            this.targetSeq = targetSeq;
            this.logStartLsn = logStartLsn;
            this.boundaryLsn = logStartLsn - 1;
        }

        /** Rewinds the cursor to the bottom of the range, for the second walk. */
        void rewind() {
            seg = 0;
            offset = 0;
            entryPos = -1;
            in = null;
            rangeDone = false;
        }
    }

    /**
     * Opens a cycle retiring everything at or below {@code targetSeq}. Write lock must be held, and
     * the caller must have established that a segment above {@code targetSeq} exists.
     *
     * <p><b>O(1).</b> An earlier revision computed the candidate set here by walking the whole
     * {@code stateLsn} map and sorting it, which is O(live recids) under the write lock and is a
     * pause of the kind step 3 exists to delete — for a large store it is far more work than the
     * segment being retired even contains. Candidates are now discovered by <b>walking the retiring
     * range itself</b>, one bounded unit at a time (§5.2 step 1, and §5.5's
     * {@code (segmentSeq, scanOffset)} cursor).
     *
     * <p>The walk finds every candidate and no others: R needs re-emission exactly when
     * {@code stateLsn[R] <= boundaryLsn}, that value IS the LSN of R's newest self-contained entry,
     * and the retained log begins at {@code boundaryLsn + 1} — so that entry is inside the range,
     * and R's recid therefore appears in the walk. The filter itself stays over {@code stateLsn},
     * which is what keeps a recid an in-flight transaction merely allocated out of the set: such a
     * recid has no committed entry and so no {@code stateLsn} at all. That is the step-3 blocker
     * (the predicate over {@code stateLsn}, never {@code recState}) discharged by construction
     * rather than by a filter a port has to remember.
     *
     * <p>A recid met twice in the range needs no dedup set: the first meeting publishes it and
     * raises its {@code stateLsn} above the boundary, so the second is filtered by the same test
     * that selected the first.
     *
     * <h2>Why no surviving {@code T_APPEND} can be orphaned by this</h2>
     *
     * The obvious worry is a delta ABOVE the retiring range whose base image lies INSIDE it: after
     * the unlink that delta cites a base that no longer exists. It cannot happen, and the reason is
     * an invariant of the two identities rather than a rule anyone has to remember.
     * {@code contentBaseLsn[R] <= boundary < stateLsn[R]} is unreachable, because every entry that
     * raises {@code stateLsn} either moves {@code contentBaseLsn} to the SAME LSN (a content image)
     * or clears it (a null image, a prealloc), and {@link #identityContent},
     * {@link #identityStateOnly} and {@link #identityVoid} are the only writers of either map. So a
     * delta whose base is in the range belongs to a recid whose {@code stateLsn} is also in the
     * range — i.e. to a candidate, which is re-emitted with that delta already folded into its
     * content. Replay then skips the stranded delta and the image supersedes it, which is exactly
     * what §4.2's skip audit is built to tolerate. Pinned by
     * {@code a_base_in_the_retired_segment_is_re_emitted_with_its_delta_folded_in}.
     */
    private void startCycleLocked(long targetSeq) {
        cycleRetiredAt = cleanerBytesRetired;
        cycleWrittenAt = cleanerBytesWritten;
        Segment successor = null;
        for (Segment s : segs.segments()) {
            if (s.seq > targetSeq) { successor = s; break; }
        }
        if (successor == null)
            // K4: a mark may not authorize removing its own segment, so a cycle that retires
            // everything has nowhere to record itself. Unreachable via every caller.
            throw new AssertionError("cleaning through " + targetSeq + " retires the whole log");
        cleaner = new Cleaner(targetSeq, WalSegmentSet.headerFirstLsn(successor));
    }

    /**
     * One cleaning tick: re-emit, then verify (W10), then close the cycle — as far as
     * {@code budget} allows, stopping at the first limit reached. Write lock must be held. Returns
     * the bytes written, which is what the task reports as progress.
     *
     * <p>At most ONE cycle is closed per tick, so a caller driving this in a loop always sees the
     * cycle boundary and can re-decide whether more cleaning is wanted.
     */
    private long cleanTickLocked(MaintenanceBudget budget) {
        long t0 = budget.maxNanos > 0 ? System.nanoTime() : 0;
        long written = 0;
        int records = 0;
        // Both halves of the accounting must be in the SAME unit, and the unit is FILE bytes. The
        // sections this tick appends are not what it costs the log: an append that rolls over
        // creates a segment header, and the mark that closes a cycle usually lands in a segment of
        // its own. Charging section bytes against `retired`, which sums whole `fileLen`s, reported
        // 31 bytes of progress on an episode that grew the log by 77 per live record — a treadmill
        // that could never reach its terminal because it never stopped "progressing". Measuring the
        // log itself is exact and needs no rule per writer.
        boolean closedCycle = false;
        try {
            while (cleaner != null) {
                Cleaner c = cleaner;
                long logBefore = segs.logBytes(), retiredBefore = cleanerBytesRetired;
                if (!c.published) {
                    long[] done = publishUnitLocked(c, budget, written, records);
                    records += (int) done[1];
                } else if (!c.verified) {
                    records += verifyUnitLocked(c, budget, records);
                } else {
                    finishCycleLocked(c);
                    closedCycle = true;
                    written += chargeCleanerLocked(logBefore, retiredBefore);
                    break;
                }
                written += chargeCleanerLocked(logBefore, retiredBefore);
                if (budget.maxRecords > 0 && records >= budget.maxRecords) break;
                if (budget.maxBytes > 0 && written >= budget.maxBytes) break;
                if (budget.maxNanos > 0 && System.nanoTime() - t0 >= budget.maxNanos) break;
            }
        } catch (IOException e) {
            failClosed("WAL cleaning failed: " + file, e);
        } catch (DBException refused) {
            // A unit refused — W10 caught an under-re-emission, or an identity map disagreed with
            // the inner store. The cursor has ALREADY stepped past the entry that refused (it
            // advances before the visitor runs), so a later tick would resume beyond it, find
            // nothing wrong in what remains, and write the mark: the loud refusal would become
            // exactly the silent loss it exists to prevent. Rewind, so any retry re-walks the
            // range from the bottom and reaches the same verdict — or a genuinely different one,
            // if a commit has since re-homed the recid, which makes the retirement safe for real.
            if (cleaner != null) cleaner.rewind();
            throw refused;
        }
        if (closedCycle) {
            // The cycle is closed and its whole cost is now charged, so this is the first moment its
            // net is knowable. Double on a cycle that did not pay for its own mark, HALVE on one
            // that did — not back to one. Snapping back to one was measured oscillating: the wide
            // cycle succeeded because it was wide, the narrow one that replaced it failed again, and
            // the pair averaged out to a 3% gain that no threshold could distinguish from progress.
            // Halving keeps the search near the width this log needs and still walks back down when
            // a workload stops needing it.
            long gain = cleanerBytesRetired - cycleRetiredAt - (cleanerBytesWritten - cycleWrittenAt);
            long cost = cleanerBytesWritten - cycleWrittenAt;
            // THREE bands, not two. Halving on any gain oscillated around the break-even width —
            // a trace showed widths 4 and 8 alternating forever while a single wide pass
            // would pay handsomely — because a cycle that barely pays is not evidence the width is
            // too big. So: widen when it does not pay, hold when it pays modestly, and give width
            // back only when it pays HANDSOMELY, which is what a workload that no longer needs the
            // width looks like.
            if (gain <= cost >> 3) cycleWidth = Math.min(CYCLE_WIDTH_CAP, Math.max(1, cycleWidth) * 2);
            else if (gain > cost >> 1) cycleWidth = Math.max(1, cycleWidth / 2);
            lastCycleSaturated = cycleSaturated;
        }
        return written;
    }

    /**
     * Charges one unit of cleaning to {@code cleanerBytesWritten} and returns what it charged: what
     * the log grew by, plus what the unit retired (which shrank it). Write lock must be held, and
     * nothing but the cleaner can touch the log while it does, so the difference is exactly the
     * cleaner's own appends — sections, the mark, and every segment header a rollover created.
     *
     * <p>Charged and returned per UNIT, so the tick's return value — which
     * {@code MaintenanceResult.bytesDone} publishes and {@code budget.maxBytes} is enforced
     * against — is in the same unit as the lifetime counter. Reporting section bytes to the budget
     * while counting file bytes in the terminal left the maintenance API measuring one thing and the
     * policy another.
     *
     * <p>Charges NOTHING once the store is closed: {@code appendSection} can fail the store from
     * inside a unit, and after {@code failClosed} the segment set is empty, so the delta would be a
     * large negative charge. It cannot affect a later decision on a dead handle, but a counter that
     * goes backwards is not worth leaving in place.
     */
    private long chargeCleanerLocked(long logBefore, long retiredBefore) {
        if (closed) return 0;
        long charge = segs.logBytes() - logBefore + (cleanerBytesRetired - retiredBefore);
        cleanerBytesWritten += charge;
        return charge;
    }

    /**
     * Reads the cleaner's two scans have issued over this store's lifetime. Never reset, so a test
     * takes a difference; see {@link ReadCount}.
     */
    private final ReadCount scanReads = new ReadCount();

    /** @see ReadCount */
    long testCleanerScanReads() { return scanReads.n; }

    /** Entries a single scan unit walks when the budget names no record limit. */
    private static final int SCAN_UNIT_ENTRIES = 256;

    /**
     * Window for the two scans. Small on purpose: they read entry HEADERS and seek over payloads,
     * so a replay-sized window would read a megabyte to decode ten bytes whenever entries are far
     * apart, and the "bounded unit" would not be bounded in device reads at all.
     */
    static final int SCAN_BUF = 4096;   // package-private: StoreWALCleanerScanCostTest names it

    /**
     * Walks up to {@code maxEntries} entries of the retiring range, handing each entry's recid to
     * {@code visit}, and stops early when {@code visit} returns false. Write lock must be held.
     * Returns false once the range is exhausted.
     *
     * <p>This is the single cursor both phases use, in turn, and the reason each phase is genuinely
     * budgeted: the unit is <b>an entry</b>, not a section. A section may be arbitrarily large — a
     * rollover happens only at a section boundary, so one commit can exceed {@code segmentBytes} on
     * its own — so "one section per tick" would have held the write lock for an unbounded time,
     * which is exactly the pause step 3 removes.
     *
     * <p>Payloads are <b>seeked over, not read</b> ({@link WalIn#seek}), so the cost is
     * proportional to the number of entries rather than to the bytes they carry.
     */
    private int scanUnitLocked(Cleaner c, int maxSteps, java.util.function.LongPredicate visit)
            throws IOException {
        List<Segment> all = segs.segments();
        int steps = 0;
        while (steps < maxSteps) {
            if (c.seg >= all.size() || all.get(c.seg).seq > c.targetSeq) {
                c.rangeDone = true;
                return steps;
            }
            Segment s = all.get(c.seg);
            if (c.in == null) {
                // The channel is stable for this segment's whole walk: nothing releases it until
                // the walk leaves the segment below, and the range's membership is fixed at cycle
                // start, so no other pass can touch it in between.
                c.in = new WalIn(s.channel(), SCAN_BUF, scanReads);
                // The hard bound is the SEGMENT, set once here; `rebound` then narrows the soft
                // bound per section without dropping the window.
                c.in.reset(WalSegmentSet.SEG_HDR, s.validEnd);
                c.offset = WalSegmentSet.SEG_HDR;
                c.entryPos = -1;
            }
            if (c.entryPos < 0) {                              // enter the next section
                if (c.offset >= s.validEnd) {
                    s.release();                               // keeps the descriptor count O(1)
                    c.seg++;
                    c.in = null;
                    steps++;
                    continue;
                }
                // The header is read THROUGH the window, not around it. A separate positional read
                // per section — plus the window drop that followed it — cost two syscalls per
                // section, and a log written by single-op commits is nearly all section headers:
                // the two scans issued ~148k reads to walk 34 MB, an order of magnitude more than
                // the range contains. Widening the soft bound to the segment is what lets the
                // header be read at all; it is narrowed back to the section before any entry is.
                // This walk trusts the section header — it verifies no CRC, because the section was
                // verified whole at open — so it says what it trusts. Both bounds are checked
                // BEFORE the bytes are read: the reader is now bounded by validEnd, so a header or
                // a body running past it would otherwise surface as a bare TornTail, with no
                // message, out of a scan that catches none. Two comparisons per section.
                if (s.validEnd - c.offset < SEC_HDR)
                    throw new DBException.DataCorruption("WAL section header at offset " + c.offset
                            + " in " + s.file.getName() + " does not fit before the segment's"
                            + " validated end " + s.validEnd);
                c.in.rebound(c.offset, s.validEnd);
                c.in.readFully(c.secHdr);
                int tag = c.secHdr[0] & 0xFF;
                long bodyStart = c.offset + SEC_HDR;
                long bodyLen = WalSegmentSet.be64(c.secHdr, 9);
                if (bodyLen < 0 || bodyLen > s.validEnd - bodyStart)
                    throw new DBException.DataCorruption("WAL section at offset " + c.offset
                            + " in " + s.file.getName() + " claims a " + bodyLen
                            + "-byte body, which runs past the segment's validated end "
                            + s.validEnd);
                c.bodyEnd = bodyStart + bodyLen;
                c.offset = c.bodyEnd;                          // where the NEXT section begins
                // Entering a section costs a header read and is charged like an entry. Without
                // that, a range of mark-only or empty sections is walked ENTIRELY within one unit
                // at no budgeted cost — the same unbounded-work-under-the-lock defect as the
                // per-section unit, just with metadata instead of payload.
                steps++;
                // A 'K' body carries no entries and is never passed to the entry decoder.
                if (tag == TAG_MARK) continue;
                c.entryPos = bodyStart;
                c.in.rebound(bodyStart, c.bodyEnd);   // back to the section bound, window kept
            }
            while (c.entryPos < c.bodyEnd && steps < maxSteps) {
                long recid = nextEntryRecid(c.in, s);
                c.entryPos = c.in.pos();
                steps++;
                if (!visit.test(recid)) return steps;
            }
            if (c.entryPos >= c.bodyEnd) c.entryPos = -1;      // section done
        }
        return steps;
    }

    /** Decodes one entry for its recid alone, seeking over the payload. */
    private static long nextEntryRecid(WalIn in, Segment s) {
        int type = in.readByteRaw();
        long recid = in.unpackLong();
        switch (type) {
            case T_PREALLOC, T_DELETE -> { }
            case T_RECORD -> {
                in.unpackLong();                               // capacity
                long lenPlus = in.unpackLong();
                if (lenPlus != 0) in.seek(in.pos() + lenPlus - 1);
            }
            case T_APPEND -> {
                in.unpackLong();                               // base delta
                // The length goes in a local FIRST. Written as `in.seek(in.pos() + in.unpackLong())`
                // it is wrong and quietly so: Java evaluates `in.pos()` before `unpackLong` advances
                // it, so the seek lands short by the width of the packed length and the walk
                // resumes inside the payload.
                long len = in.unpackLong();
                in.seek(in.pos() + len);
            }
            default -> throw new DBException.DataCorruption(
                    "bad WAL entry tag " + type + " in " + s.file.getName());
        }
        return recid;
    }

    /**
     * Phase 1, one bounded unit: walk the retiring range and publish, as a single {@code 'C'}
     * section, an image of every record met whose state still lives inside it. Returns
     * {@code {bytesWritten, recordsTouched}}.
     *
     * <p><b>Check, copy and publish are one serialized unit</b> — the whole method runs under the
     * WAL write lock — and that is correctness, not style. Split them and the cleaner sees R live,
     * copies image I, a committer writes update U, and the cleaner then appends a stale
     * {@code C(R,I)} <em>after</em> U: replay resurrects the old value. Note that
     * {@code StoreDirect.walSnapshot} deliberately invokes its sink after releasing the per-recid
     * lock, so that hook cannot be reused without a WAL-level barrier — this is that barrier.
     *
     * <p><b>One section per unit, and every section is forced before the next is appended</b>
     * ({@link #appendSection}). §6.1's table infers mid-log rot from "a valid section follows an
     * invalid one", which is sound only while that holds: N sections under one fsync would let
     * writeback reorder, so a crash could leave section <i>k</i> torn and <i>k+1</i> durable, and
     * recovery would call an ordinary torn tail {@code DataCorruption} and refuse to open.
     */
    private long[] publishUnitLocked(Cleaner c, MaintenanceBudget budget,
                                     long writtenSoFar, int recordsSoFar) throws IOException {
        long byteRoom = budget.maxBytes > 0 ? Math.max(1, budget.maxBytes - writtenSoFar) : 1 << 20;
        int recRoom = budget.maxRecords > 0 ? Math.max(1, budget.maxRecords - recordsSoFar)
                                            : SCAN_UNIT_ENTRIES;
        long cap = Math.min(byteRoom, 1 << 20);
        // The buffer's INITIAL size is a guess; `cap` is the break threshold and stays exact. A
        // one-byte budget must not mean a one-byte buffer that then doubles its way up to a 60 KiB
        // record.
        DataOutput2 out = new DataOutput2((int) Math.min(Math.max(cap, 4096), 1 << 16));
        long lsn = nextLsn;
        // {recid, 1 if the entry carries content} per emitted image, applied to the identities
        // only after the section is durable
        ArrayList<long[]> emitted = new ArrayList<>();
        // Recids already encoded into THIS section. A recid met again in a later section of the
        // retiring range is normally filtered by its own raised stateLsn — but the identities move
        // only once the section is durable, so within one unfinished batch that filter has not
        // fired yet, and the decoder's one-entry-per-recid-per-section rule would be violated.
        // Replay refuses such a section outright, so this is a corrupt-log bug, not a waste.
        java.util.HashSet<Long> inBatch = new java.util.HashSet<>();
        int steps = scanUnitLocked(c, recRoom, recid -> {
            if (inBatch.contains(recid)) return true;
            Long sl = stateLsn.get(recid);
            // Across units no dedup is needed: a recid re-emitted by an earlier unit has a
            // stateLsn above the boundary, exactly like one a concurrent commit re-homed. Both are
            // simply not candidates any more.
            if (sl == null || sl > c.boundaryLsn) return true;
            // Fault injection, and the only reason it exists: W10 is a check on THIS loop, so a
            // suite that cannot make this loop drop a record cannot tell a working W10 from one
            // that passes because nothing ever fails it. Dropping a recid here is precisely the
            // under-re-emission W10 is for. Off unless a test sets it.
            if (recid == testDropRecidFromPublish) return true;
            if (stagedCreated(recid))
                // A recid an in-flight transaction allocated has no committed entry and therefore
                // no stateLsn; the allocator cannot hand out a recid that is committed-live. Both
                // at once would mean inner's slot has been overwritten with a preallocation while
                // committed content is still attested — re-emitting either way would be a guess.
                throw new DBException("WAL cleaner: recid " + recid + " has committed state at LSN "
                        + sl + " and is also allocated by an in-flight transaction");
            boolean[] content = {false};
            boolean live = inner.walSnapshotOne(recid, (r, prealloc, capBytes, bytes) -> {
                if (prealloc) {
                    out.writeByte(T_PREALLOC);
                    out.packLong(r);
                } else {
                    out.writeByte(T_RECORD);
                    out.packLong(r);
                    out.packLong(capBytes);
                    if (bytes == null) out.packLong(0);
                    else {
                        out.packLong(bytes.length + 1);
                        out.write(bytes);
                    }
                    content[0] = bytes != null;
                }
            });
            if (!live)
                // stateLsn present means "committed non-void", and inner IS the committed state,
                // so this cannot happen without the identity map having diverged from the store.
                // Refuse rather than retire a segment whose contents were not re-homed.
                throw new DBException("WAL cleaner: recid " + recid + " has committed state at LSN "
                        + sl + " but the inner store holds nothing for it");
            emitted.add(new long[]{recid, content[0] ? 1 : 0});
            inBatch.add(recid);
            // An image larger than one unit's allowance still goes whole: a record cannot be split
            // across sections (§5.5's oversize-unit exception).
            return out.pos < cap;
        });
        long written = 0;
        if (!emitted.isEmpty()) {
            appendSection(TAG_IMAGE, lsn, out.buf, out.pos);
            nextLsn++;
            written = SEC_HDR + out.pos;
            // IMAGES, not entries walked. The staleness clock compares the store's committed
            // self-contained entries against the live set the futile episode had to preserve, and
            // entries walked is neither: it counts the garbage too, so a garbage-heavy log would
            // demand far more churn than its live set before retrying. Counting here rather than
            // from the unit's step count also makes a post-refusal rewind harmless — an image whose
            // identity already moved is filtered by its own raised stateLsn on the second walk.
            // (Found while auditing the preceding fix.)
            episodeRecords += emitted.size();
            // Identities move by the §4.2 row of each entry the section contains, AFTER it is
            // durable and atomically with it: a content image sets both, a prealloc sets stateLsn
            // and CLEARS the content base (a recid that was content-live and is now preallocated
            // must not keep a base a later append could be stamped from).
            for (long[] e : emitted) {
                if (e[1] == 1) identityContent(e[0], lsn);
                else identityStateOnly(e[0], lsn);
            }
        }
        if (c.rangeDone) {
            c.published = true;
            c.rewind();            // also clears rangeDone, for the verify walk
        }
        return new long[]{written, steps};
    }

    /**
     * Phase 2 — W10, one bounded unit: re-walk the retiring range and assert that every recid it
     * mentions has been re-homed above it ({@code stateLsn[R]} absent, or above
     * {@code boundaryLsn}). Write lock must be held. Returns the entries it checked.
     *
     * <p><b>A mark cannot be made self-verifying after the unlink</b>, because the evidence is
     * exactly what is being deleted: a manifest of what was re-homed cannot prove completeness,
     * since an omitted recid is omitted from the manifest too. The verifiable moment is here, while
     * the segments still exist. What it buys is that an under-re-emission — a dropped
     * {@code T_PREALLOC}, a dropped null-content record — fails loudly BEFORE the data is
     * destroyed, instead of silently until {@code rebuildFreeRecids} re-issues the recid and a
     * later allocation collides with it. §4.2's skip audit cannot see this class at all: a record
     * wholly contained in the retiring range with no surviving append leaves no entry to skip.
     *
     * <p>Chunking it across ticks is sound because the predicate is <b>monotone</b>: once
     * {@code stateLsn[R]} is absent-or-above, only a new self-contained entry at a still higher LSN
     * can change it, so an entry verified in an earlier tick stays verified.
     *
     * <p><b>Boundary, stated so it is not over-trusted: W10 is sufficient for OMISSION, not for
     * image FIDELITY.</b> It asks "was this recid re-homed?", and a cleaner that emitted a
     * CRC-valid but semantically wrong image raises {@code stateLsn} just the same and passes. Nor
     * can it see a false negative in its own oracle: if some path failed to install
     * {@code stateLsn} for a committed non-void entry, phase 1 would not select the recid and this
     * scan would read the absence as success. Selecting from the bytes rather than from the map is
     * what keeps that hole closed at the selection end.
     */
    private int verifyUnitLocked(Cleaner c, MaintenanceBudget budget, int recordsSoFar)
            throws IOException {
        int recRoom = budget.maxRecords > 0 ? Math.max(1, budget.maxRecords - recordsSoFar)
                                            : SCAN_UNIT_ENTRIES;
        int steps = scanUnitLocked(c, recRoom, recid -> {
            Long sl = stateLsn.get(recid);
            if (sl != null && sl <= c.boundaryLsn)
                throw new DBException("WAL cleaner would retire through segment " + c.targetSeq
                        + " while recid " + recid + " still has its only self-contained entry at LSN "
                        + sl + " (the log would begin at " + c.logStartLsn + "): refusing to write"
                        + " the clean mark. Nothing has been deleted; the durable log is intact.");
            return true;
        });
        if (c.rangeDone) c.verified = true;
        return steps;
    }

    /**
     * Closes a cycle: append the forced {@code 'K'}, then unlink. Write lock must be held. Returns
     * the bytes it wrote, so a tick whose only output is the mark still reports what it cost.
     *
     * <p><b>Ordering is the whole content of this method.</b> Every re-emitted image was forced as
     * it was written and every rollover sealed its predecessor with a size-persisting force, so no
     * mark ever attests bytes that were not forced (W1, and the rollover-during-re-emission trap).
     * The {@code 'K'} is forced before the unlink (W5): a failed unlink is a leak the next open
     * retries (K5/K8), never permission to advance an unproven mark. Every crash point in between
     * is state-preserving by Q5 §2.3 — before the mark the retiring segments replay and cleaning
     * simply re-runs, after it they are already superseded.
     *
     * <p>{@code logStartLsn} is the successor's <em>stated</em> start, read from its header rather
     * than computed, so the number recovery compares against is the number the writer recorded.
     */
    private long finishCycleLocked(Cleaner c) throws IOException {
        byte[] body = new byte[MARK_BODY_LEN];
        WalSegmentSet.putBe64(body, 0, c.targetSeq);
        WalSegmentSet.putBe64(body, 8, c.logStartLsn);
        appendSection(TAG_MARK, nextLsn, body, MARK_BODY_LEN);
        nextLsn++;
        long retired = 0;
        for (Segment s : segs.segments()) {
            if (s.seq > c.targetSeq) break;
            retired += s.fileLen;
        }
        segs.unlinkThrough(c.targetSeq);
        cleanerBytesRetired += retired;
        cleaner = null;
        return SEC_HDR + MARK_BODY_LEN;
    }

    /** Positional write — the channel's own position is never used, so nothing can drift. */
    private static int writeFullyAt(FileChannel ch, ByteBuffer b, long pos) throws IOException {
        int total = b.remaining();
        long p = pos;
        while (b.hasRemaining()) p += ch.write(b, p);
        return total;
    }

    private static void fsyncDir(File dir) throws IOException {
        if (dir == null) return;
        try (FileChannel dc = FileChannel.open(dir.toPath(), StandardOpenOption.READ)) {
            dc.force(true);
        }
    }

    /**
     * The one directory-fsync seam, shared with {@link WalSegmentSet}. Durable mode REQUIRES a
     * working directory fsync — Q5 §3.1 — and therefore durable mode on Windows is UNSUPPORTED
     * in v2: opening a directory as a channel is not possible there, and no port has named and
     * tested a substitute. A port must never silently skip this, which would make the
     * acknowledgement rule (a commit is durable when its section is forced AND the directory
     * entry of the segment holding it is durable) unsatisfiable while appearing to work.
     */
    static void syncDirectory(File dir) throws IOException {
        walIoEvent(WalOpKind.DIRSYNC, 0, 0, 0, 0);
        directorySync.sync(dir);
    }

    /**
     * Q5 §3.1: durable mode REQUIRES a working directory fsync, so a platform without one is
     * refused <b>at open</b>, by name, with the reason. Windows cannot open a directory as a
     * channel, and no port has named and tested a substitute.
     *
     * <p>Refusing here rather than letting {@link #fsyncDir} fail at the first segment create is
     * the whole point: the failure a port must never produce is a silently skipped fsync, and the
     * failure a user must never get is an obscure {@code IOException} from deep inside a commit.
     * There is deliberately no override — an escape hatch that skipped the fsync would make the
     * acknowledgement rule unsatisfiable while appearing to work, which is exactly what §3.1
     * forbids.
     *
     * <p>Gated on the default implementation still being installed, so a port or a test that
     * supplies a real directory-sync for its platform is not refused by a check about ours.
     *
     * <p><b>Writable opens only.</b> R-RO unlinks nothing, truncates nothing, rotates nothing and
     * never fsyncs the directory, so it makes no durability claim a missing directory fsync could
     * break. Refusing it too would make v2 stores un<em>readable</em> on Windows, which is a
     * restriction §3.1 does not ask for — durable MODE is what is unsupported.
     */
    private static void requireDurableDirectorySync() {
        if (directorySync != DEFAULT_DIRECTORY_SYNC) return;
        String os = System.getProperty("os.name", "");
        if (os.toLowerCase(java.util.Locale.ROOT).contains("win"))
            throw new DBException("StoreWAL durable mode is unsupported on " + os
                    + ": it requires an fsync of the segment directory, which this platform"
                    + " cannot express, and skipping it would make acknowledged commits"
                    + " undurable across a crash");
    }

    /** Test hook for the O(1)-descriptor invariant; see {@code WalSegmentSet.Segment}. */
    int openSegmentChannelsForTest() {
        rw.readLock().lock();
        try {
            return segs.openChannelCount();
        } finally {
            rw.readLock().unlock();
        }
    }

    static void testSetDirectorySync(DirectorySync sync) {
        directorySync = sync == null ? DEFAULT_DIRECTORY_SYNC : sync;
    }

    /** @see #publishBatchLocked — 0 (the default) injects nothing. */
    static volatile long testDropRecidFromPublish = 0;

    /**
     * Test hook: open a cleaning cycle over the OLDEST segment regardless of the trigger, so a
     * suite can drive the incremental path — the one {@link #checkpoint} does not exercise,
     * because a whole-log clean retires everything in one cycle and re-emits the entire store.
     * Returns false when there is nothing below the active segment to retire.
     */
    boolean testStartCleanCycle() {
        rw.writeLock().lock();
        try {
            checkClosed();
            checkWritable();
            if (cleaner != null) return true;
            if (lsnReserved()) return false;
            List<Segment> all = segs.segments();
            if (all.size() < 2) return false;
            startCycleLocked(all.get(0).seq);
            return true;
        } finally {
            rw.writeLock().unlock();
        }
    }

    /** Test hook: one cleaning tick under an arbitrary budget (C11). Returns the bytes written. */
    long testCleanTick(MaintenanceBudget budget) {
        rw.writeLock().lock();
        try {
            checkClosed();
            return cleaner == null ? -1 : cleanTickLocked(budget);
        } finally {
            rw.writeLock().unlock();
        }
    }

    /** Test hooks pinning the CACHED log size against a brute-force sum; see {@code sealedBytes}. */
    long testLogBytes() {
        rw.readLock().lock();
        try { return segs.logBytes(); } finally { rw.readLock().unlock(); }
    }

    /** @see #testLogBytes() */
    long testLogBytesExact() {
        rw.readLock().lock();
        try { return segs.logBytesExact(); } finally { rw.readLock().unlock(); }
    }

    /** Test hook: the current episode's floor, or 0 when no episode is in progress. */
    long testCleanFloorSeq() {
        rw.readLock().lock();
        try { return cleanFloorSeq; } finally { rw.readLock().unlock(); }
    }

    /**
     * Test hook: end the current episode as if it had retired exactly what it re-emitted, arming
     * the futility latch through the real path rather than by assignment.
     *
     * <p>A hook is needed because a genuinely futile episode is <b>not reachable</b> from this API:
     * {@code liveDataBytes} is the inner store's page-granular footprint, which always exceeds a
     * compacted log, so the trigger goes quiet before the log runs out of garbage. The latch is a
     * backstop for a denominator that may be tightened later. What the hook lets a test pin is the
     * <em>release</em> rule, which is the half that has been rewritten twice.
     */
    void testArmFutility(long recordsReEmitted) {
        rw.writeLock().lock();
        try {
            cleanFloorSeq = activeSeg.seq;
            episodeRetired = cleanerBytesRetired;
            episodeWritten = cleanerBytesWritten;
            episodeRecords = recordsReEmitted;
            lastCycleSaturated = true;  // only a whole-range cycle may arm; see endEpisodeLocked
            endEpisodeLocked();
        } finally {
            rw.writeLock().unlock();
        }
    }

    /** Test hook: segments the next cycle may retire in one go. @see #cycleWidth */
    int testCycleWidth() {
        rw.readLock().lock();
        try { return cycleWidth; } finally { rw.readLock().unlock(); }
    }

    /** Test hook: the cleaning trigger's current target, for tests that must know it is live. */
    long testCleaningTarget() {
        rw.readLock().lock();
        try { return cleaningTarget(); } finally { rw.readLock().unlock(); }
    }

    /** Test hook: is a cleaning cycle open? */
    boolean testCleaning() {
        rw.readLock().lock();
        try {
            return cleaner != null;
        } finally {
            rw.readLock().unlock();
        }
    }

    /** Test hook: drive one incremental cycle over the oldest segment to completion. */
    boolean testCleanOldestSegment() {
        if (!testStartCleanCycle()) return false;
        while (testCleaning()) testCleanTick(UNBOUNDED_BUDGET);
        return true;
    }

    // ---------- lifecycle ----------

    @Override public void close() {
        if (closed) return;
        stopMaintenanceForClose(); // before the write lock: lets an in-flight clean task finish
        rw.writeLock().lock();
        try {
            if (closed) return;
            closed = true;
            segs.close();
            inner.close();
        } finally {
            rw.writeLock().unlock();
        }
    }

    @Override public boolean isClosed() { return closed; }

    @Override public void verify() {
        rw.readLock().lock();
        try {
            checkClosed();
            inner.verify();
        } finally {
            rw.readLock().unlock();
        }
    }

    @Override public PrimitiveIterator.OfLong getAllRecids() {
        rw.readLock().lock();
        try {
            checkClosed();
            TreeSet<Long> set = new TreeSet<>();
            PrimitiveIterator.OfLong it = inner.getAllRecids();
            while (it.hasNext()) set.add(it.nextLong());
            for (var e : staged.entrySet()) {
                Staged s = e.getValue();
                if (s.deleted) set.remove(e.getKey());
                else if (s.baseSet || !s.appends.isEmpty()) set.add(e.getKey());
                else set.remove(e.getKey()); // pure prealloc
            }
            return set.stream().mapToLong(Long::longValue).iterator();
        } finally {
            rw.readLock().unlock();
        }
    }

    /** The store path as opened — the base the segment names are formed from, not a log file. */
    public File getFile() { return file; }

    /** Test/diagnostic view of the live segment set, ascending by sequence number. */
    File[] logFiles() {
        rw.readLock().lock();
        try {
            List<Segment> list = segs.segments();
            File[] out = new File[list.size()];
            for (int i = 0; i < out.length; i++) out[i] = list.get(i).file;
            return out;
        } finally {
            rw.readLock().unlock();
        }
    }

    /**
     * APPROXIMATE byte footprint, delegated to the wrapped {@link StoreDirect} {@code inner}
     * so byte-budget cache eviction ({@code HTreeCache} storeSize) actually fires on the durable
     * store. This reflects only records already flushed into {@code inner} (by the last
     * clean/commit path), not staged/uncommitted WAL bytes — an underestimate that is
     * acceptable per {@link Store#getCurrentSize()} (it still DECREASES on delete once flushed,
     * so an eviction sweep converges). Without this override {@code StoreWAL} would inherit the
     * default {@code 0} and silently disable store-size eviction. See {@link Store#getCurrentSize()}.
     */
    @Override public long getCurrentSize() {
        rw.readLock().lock();
        try {
            checkClosed();
            return inner.getCurrentSize();
        } finally {
            rw.readLock().unlock();
        }
    }
}
