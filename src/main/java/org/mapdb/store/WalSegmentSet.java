package org.mapdb.store;

import org.mapdb.DBException;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.zip.CRC32;

/**
 * The WAL's <b>multi-file namespace</b>: {@code <db>.wal.<16 lowercase hex digits>},
 * the store-level lock, and every operation that
 * changes which files exist — create, seal, unlink, directory fsync.
 *
 * <p>This class owns Q5's <b>namespace</b> and <b>segment header</b> decision tables (N and H)
 * and the writer obligations that are about files rather than bytes (W2, W5, W6, and the
 * force-flavor rule). {@link StoreWAL} owns the section and entry tables (S, K, §2.1) — the
 * split is deliberate: a port's expensive part is this state machine, not the codec,
 * and keeping it in one class is what makes it reviewable against the table.
 *
 * <pre>
 * name    := &lt;db&gt; ".wal." &lt;16 lowercase hex digits of segmentSeq&gt;
 * header  := magic "MDBS.WAL"(8) | version int32 = 3 | flags int32 = 0
 *          | segmentSeq int64 | firstLsn int64 | headerCrc int32       // 36 bytes
 * </pre>
 *
 * All integers big-endian; {@code headerCrc} is zlib CRC-32 over header bytes {@code [0,32)}.
 * The first segment of a store has {@code segmentSeq = 1}, so {@code 0} is free to mean
 * "no clean mark". Sixteen fixed hex digits make lexicographic order equal numeric order in
 * every port's directory listing; the <em>name</em> is the enumeration key and the
 * <em>header</em> is the authority, which is what catches a copied or renamed segment (N5/H7).
 *
 * <p><b>Not a general file abstraction.</b> Every method here exists because some row of the
 * decision table needs it, and the ordering constraints in the comments are the contract —
 * a port that reorders {@code create → header → force → directory fsync} (W2) produces a
 * segment that can vanish on a crash while its acknowledged commits do not.
 */
final class WalSegmentSet implements java.io.Closeable {

    /** magic(8) + version(4) + flags(4) + segmentSeq(8) + firstLsn(8) + headerCrc(4). */
    static final int SEG_HDR = 36;
    /** Bytes of the segment header covered by {@code headerCrc}. */
    static final int SEG_HDR_CRC_LEN = 32;
    static final byte[] MAGIC = {'M', 'D', 'B', 'S', '.', 'W', 'A', 'L'};
    /**
     * v3 adds {@code firstLsn} to the header and a second {@code int64} to the {@code 'K'} body.
     *
     * <p>Those two fields exist to <b>delete inference</b>. v2 asked recovery to work out whether a
     * missing segment was authorized, and where the retained log legitimately began, from
     * circumstantial evidence — LSN density, the position of the mark, the tag of the first retained
     * section. Six defects lived in that reasoning across four revisions, two of them permanent
     * bricks and two silent data loss, and the reviewers still disagreed about what a {@code 'C'} tag
     * implies. Recording the two facts directly turns every one of those questions into an equality
     * between two numbers a conforming writer wrote down.
     */
    static final int FORMAT_VERSION = 3;
    /** Sequence number of a store's first segment; 0 is reserved for "no clean mark". */
    static final long FIRST_SEQ = 1;

    /**
     * One segment file, open. {@link #validEnd} and the LSN fields are pass-1 results filled in
     * by {@link StoreWAL}; everything else is namespace state owned by this class.
     */
    static final class Segment {
        final long seq;
        final File file;
        private final boolean readOnly;
        /**
         * Opened ON DEMAND and released as soon as a recovery pass is done with this segment.
         *
         * <p>Holding one channel per segment for the store's lifetime is what a straightforward
         * implementation does, and it does not scale: nothing reads a segment after recovery — the
         * record map lives in the memory-backed inner store and only the ACTIVE segment is ever
         * appended to — while the log is allowed to reach roughly twice the live data size, so a
         * large store means thousands of open descriptors against a default {@code ulimit -n} of
         * 1024. A legitimate store would fail to open with {@code EMFILE}, and an attacker-supplied
         * directory of valid header-only segments could force it deliberately.
         */
        private FileChannel ch;
        /** The 36 header bytes, verbatim — used as an identity string in the section CRC domain. */
        final byte[] header;
        /** Scratch for the CRC domain: header bytes at [0,36), section offset at [36,44). */
        private final byte[] domain = new byte[SEG_HDR + 8];
        long fileLen;

        /** End offset of the valid section prefix (pass 1). Never below {@link #SEG_HDR}. */
        long validEnd = SEG_HDR;
        /** LSNs of the first and last accepted sections, or 0 when the segment holds none. */
        long firstLsn, lastLsn;
        /** A corruption verdict found in this segment, HELD until R4 decides it is relevant. */
        String held;

        Segment(long seq, File file, boolean readOnly, byte[] header, long fileLen) {
            this.seq = seq;
            this.file = file;
            this.readOnly = readOnly;
            this.header = header;
            this.fileLen = fileLen;
            System.arraycopy(header, 0, domain, 0, SEG_HDR);
        }

        /** The channel, opening it if this segment does not currently hold one. */
        FileChannel channel() throws IOException {
            if (ch == null) {
                ch = FileChannel.open(file.toPath(), readOnly
                        ? new StandardOpenOption[]{StandardOpenOption.READ}
                        : new StandardOpenOption[]{StandardOpenOption.READ, StandardOpenOption.WRITE});
            }
            return ch;
        }

        /**
         * Closes the channel if one is held; the segment stays usable and will reopen on demand.
         * Called as soon as a recovery pass finishes with a segment, which is what bounds the
         * descriptor count to O(1) instead of O(segments).
         *
         * <p><b>A close failure is dropped, and that is not laziness — it is the only option the
         * platform leaves.</b> {@code AbstractInterruptibleChannel.close} marks the channel closed
         * <em>before</em> delegating to {@code implCloseChannel}, so a channel whose close threw is
         * closed as far as Java is concerned: retaining the reference buys no retry, and reusing it
         * would only raise {@code ClosedChannelException} at the next read. If the underlying
         * descriptor really leaked, nothing here can reclaim it. What is bounded, then, is the
         * number of descriptors <em>this class holds</em>; {@link #openChannelCount} measures
         * exactly that, and every call site releases before the count can grow. Nothing is written
         * through these channels without a preceding force, so a lost close never loses data.
         * <em>(Raised as a LOW during review; the behaviour is unchanged and the
         * reasoning is recorded here so the next reader does not re-open it.)</em>
         */
        void release() {
            if (ch == null) return;
            try {
                ch.close();
            } catch (IOException unreclaimable) { }
            ch = null;
        }

        boolean holdsChannel() { return ch != null; }

        /**
         * Seeds {@code crc} with this section's <b>domain separator</b>:
         * {@code segmentHeader[0..28) || be64(sectionOffset)}. An ordinary
         * CRC-32 over a prefix — NOT a preloaded register, which {@code java.util.zip.CRC32}
         * cannot express and which would force every port to reimplement a private convention.
         *
         * <p>Binding the segment identity rejects a section byte-copied between segments;
         * binding the offset rejects one copied to a different offset in the same segment
         * (fixture C7). The domain intentionally includes the header's own {@code headerCrc}
         * field: the 36 bytes are an identity string, not a re-parsed structure. It therefore
         * also covers {@code firstLsn}, so a segment whose stated start is edited invalidates
         * every section CRC in it.
         */
        void crcDomain(CRC32 crc, long sectionOffset) {
            for (int i = 0; i < 8; i++) domain[SEG_HDR + i] = (byte) (sectionOffset >>> (56 - 8 * i));
            crc.update(domain, 0, domain.length);
        }

        /** True while this segment holds no accepted section (H8). */
        boolean empty() { return validEnd == SEG_HDR; }

        void close() { release(); }
    }

    private final File base;
    private final File dir;
    private final String prefix;
    private final boolean readOnly;

    private final ArrayList<Segment> segments = new ArrayList<>(); // ascending seq
    /** W6: one above the highest sequence number seen in ANY enumerated name, orphans included. */
    private long nextSeq;
    /**
     * Total {@code fileLen} of every segment EXCEPT the highest, which is the only one that grows.
     * Maintained at the two points that change which segments exist, so {@link #logBytes} is O(1).
     *
     * <p>Summing the list instead is O(segments) and {@code logBytes} is consulted on every commit
     * (the cleaning trigger) — proportional to the log, under the WAL write lock, on the hot path.
     * With the minimum segment size that is one section per segment, so it is proportional to the
     * number of committed sections. Cross-checked against a
     * brute-force sum by {@code logBytes_stays_exact_across_rollover_and_cleaning}.
     */
    private long sealedBytes;
    private FileChannel lockCh;
    private FileLock lock;

    /**
     * Opens the namespace: takes the store lock, enumerates and classifies (R0/R1), and removes
     * create-crash residue (R2). Leaves the surviving segments open and ordered; section-level
     * recovery is the caller's job.
     *
     * @param base the store path as opened, verbatim — never canonicalized and never reduced to
     *             a basename, or two opens by different paths would disagree on the namespace
     */
    WalSegmentSet(File base, boolean readOnly) throws IOException {
        this.base = base;
        this.readOnly = readOnly;
        File abs = base.getAbsoluteFile();
        this.dir = abs.getParentFile();
        this.prefix = abs.getName() + ".wal.";
        boolean ok = false;
        try {
            takeStoreLock();
            // N6: the v1 single-file log. There is no migration, and silently
            // starting a fresh segment set beside it would strand every committed transaction
            // in it — the one outcome the format break exists to prevent. Regular files only,
            // the same discipline N4 applies: a DIRECTORY at that name is not a v1 log.
            if (Files.isRegularFile(new File(abs.getPath() + ".wal").toPath(),
                    LinkOption.NOFOLLOW_LINKS))
                throw new DBException.DataCorruption(
                        "v1 single-file WAL present at " + abs.getPath() + ".wal: no migration to v2");
            classify(enumerate());
            ok = true;
        } finally {
            if (!ok) closeQuietly();
        }
    }

    /**
     * §3.1: exactly one process may run open, recovery or writing at a time. Recovery unlinks,
     * truncates and rotates, and two concurrent opens would also pick the same next sequence
     * number. v1 {@code StoreWAL} took no lock; this is new.
     */
    private void takeStoreLock() throws IOException {
        File lockFile = new File(base.getAbsoluteFile().getPath() + ".lock");
        if (readOnly) {
            // §3.1 is TWO-SIDED: a reader must be rejected while a writer holds the exclusive
            // lock, AND a writer must be rejected while a reader holds a shared one. Waiting for
            // the file to exist gave only the first half — a reader admitted with no lock at all
            // is then run over by a writer that starts afterwards, whose recovery unlinks,
            // truncates and rotates under its mid-scan feet. So CREATE the lock file when the
            // directory allows it, even though this open will not modify the store.
            try {
                lockCh = FileChannel.open(lockFile.toPath(), StandardOpenOption.CREATE,
                        StandardOpenOption.READ, StandardOpenOption.WRITE);
            } catch (IOException cannotCreate) {
                // Going lockless is the one outcome that reintroduces the race, so it needs a
                // POSITIVE reason — not merely "the create failed". An IOException here can be a
                // transient I/O error, a quota, or an ACL on this one pathname, none of which
                // imply that no writer can create the file and lock it exclusively.
                if (lockFile.exists()) {
                    // Ambiguity resolved: the file is there, so a shared lock is still attainable
                    // on a read-only channel. This is not a fallback to lockless at all.
                    lockCh = FileChannel.open(lockFile.toPath(), StandardOpenOption.READ);
                } else if (dir != null && !Files.isWritable(dir.toPath())) {
                    // Positively a read-only medium: no writer can create the lock file or a
                    // segment, so there is nothing to be excluded by and nothing to exclude.
                    return;
                } else {
                    // Inconclusive — fail closed rather than read a store that may be moving.
                    throw new DBException("cannot take a shared store lock on " + lockFile
                            + " and the directory is writable, so a writer may be running",
                            cannotCreate);
                }
            }
        } else {
            lockCh = FileChannel.open(lockFile.toPath(), StandardOpenOption.CREATE,
                    StandardOpenOption.READ, StandardOpenOption.WRITE);
        }
        try {
            lock = lockCh.tryLock(0, Long.MAX_VALUE, readOnly);
        } catch (OverlappingFileLockException e) {
            // same JVM, another StoreWAL instance: the same refusal, a different exception
            throw new DBException("WAL store " + base + " is already open in this JVM", e);
        }
        if (lock == null)
            throw new DBException("WAL store " + base + " is locked by another process");
    }

    // ---------- R0: enumerate ----------

    /**
     * R0/N4. Collects every <b>regular file</b> whose name is exactly the prefix followed by 16
     * lowercase hex digits with a non-negative {@code int64} value. Directories, symlinks,
     * uppercase hex, wrong lengths and the {@code .lock} file are not segments and are ignored —
     * ignored, not rejected, because a store directory is allowed to contain other things.
     */
    private List<long[]> enumerate() {
        ArrayList<long[]> found = new ArrayList<>(); // {seq, unused} — File resolved by name()
        String[] names = dir == null ? null : dir.list();
        if (names == null) return found;
        for (String name : names) {
            if (!name.startsWith(prefix)) continue;
            String hex = name.substring(prefix.length());
            if (hex.length() != 16) continue;
            long seq = 0;
            boolean okHex = true;
            for (int i = 0; i < 16 && okHex; i++) {
                char c = hex.charAt(i);
                int d;
                if (c >= '0' && c <= '9') d = c - '0';
                else if (c >= 'a' && c <= 'f') d = c - 'a' + 10;
                else { okHex = false; d = 0; }
                seq = (seq << 4) | d;
            }
            // uppercase hex does not match, and enumeration is case-SENSITIVE even on a
            // case-insensitive filesystem; a value >= 2^63 (negative as int64) is not a segment
            if (!okHex || seq < 0) continue;
            if (!Files.isRegularFile(new File(dir, name).toPath(), LinkOption.NOFOLLOW_LINKS)) continue;
            found.add(new long[]{seq});
        }
        found.sort(Comparator.comparingLong(a -> a[0]));
        return found;
    }

    File segmentFile(long seq) {
        return new File(dir, prefix + String.format("%016x", seq));
    }

    // ---------- R1/R2: classify, remove residue ----------

    /**
     * R1 then R2. Applies table H to every enumerated name, unlinks create-crash residue, and
     * records {@code maxObservedSeq} over ALL names — <b>including the residue it is about to
     * remove</b> (W6), so a stale directory entry can never alias a segment a later create reuses.
     *
     * <p>The asymmetry in table H is the whole point: a torn create produces an invalid
     * {@code headerCrc} with overwhelming probability, so an invalid header on the
     * <b>highest</b> name is an ordinary crash artifact, while the same bytes anywhere else are
     * corruption — something above it exists, so its creation completed once.
     */
    private void classify(List<long[]> found) throws IOException {
        long maxObserved = 0;
        for (long[] f : found) maxObserved = Math.max(maxObserved, f[0]);
        this.nextSeq = maxObserved + 1;
        if (nextSeq < 0) throw new DBException("WAL segment sequence overflow");

        long highestSeq = found.isEmpty() ? -1 : found.get(found.size() - 1)[0];
        ArrayList<Long> residue = new ArrayList<>();   // sequence numbers, so R2's unlinks are traceable
        for (long[] f : found) {
            long seq = f[0];
            // Sequence 0 is RESERVED for "no clean mark" (§4.1), so no conforming writer can ever
            // create it: FIRST_SEQ is 1 and nextSeq only increases. Rejected here, at R1, rather
            // than left to fall through — R4 happens to refuse it today only as a side effect of
            // its retained set (seq > 0) coming out empty, which is an accident, not a rule, and
            // says nothing useful about what is wrong.
            if (seq == 0)
                throw new DBException.DataCorruption("WAL segment " + segmentFile(0).getName()
                        + ": sequence 0 is reserved for \"no clean mark\" and is never a segment");
            File file = segmentFile(seq);
            FileChannel ch = FileChannel.open(file.toPath(), readOnly
                    ? new StandardOpenOption[]{StandardOpenOption.READ}
                    : new StandardOpenOption[]{StandardOpenOption.READ, StandardOpenOption.WRITE});
            try {
                long len = ch.size();
                byte[] hdr = new byte[SEG_HDR];
                String fault = readHeader(ch, len, hdr, seq);
                if (fault == null) {
                    // The channel is NOT retained: a recovery pass reopens it on demand and
                    // releases it again, so the descriptor count stays O(1) in the segment count.
                    segments.add(new Segment(seq, file, readOnly, hdr, len));
                } else if (fault.startsWith("!")) {
                    // H5/H6/H7: CRC-valid header carrying wrong content is a writer defect or a
                    // copied file, never a torn create — corruption regardless of position.
                    throw new DBException.DataCorruption("WAL segment " + file.getName()
                            + ": " + fault.substring(1));
                } else if (seq == highestSeq) {
                    residue.add(seq);                        // H1-H4, highest: create crashed
                } else {
                    throw new DBException.DataCorruption("WAL segment " + file.getName()
                            + ": " + fault + " (not the highest segment, so its create completed)");
                }
            } finally {
                try {
                    ch.close();
                } catch (IOException ignored) { }
            }
        }
        if (!readOnly && !residue.isEmpty()) {
            for (long seq : residue) {
                StoreWAL.walIoEvent(StoreWAL.WalOpKind.UNLINK, seq, 0, 0, 0);
                Files.deleteIfExists(segmentFile(seq).toPath());
            }
            fsyncDir();
        }
        for (int i = 0; i + 1 < segments.size(); i++) sealedBytes += segments.get(i).fileLen;
    }

    /**
     * Reads and validates one segment header. Returns {@code null} when it is valid, a plain
     * message for the <em>torn create</em> shapes (H1-H4, residue when highest), or a message
     * prefixed {@code "!"} for the shapes that are corruption wherever they appear (H5-H7).
     */
    private static String readHeader(FileChannel ch, long len, byte[] into, long nameSeq) throws IOException {
        if (len == 0) return "empty segment file";                                   // H1
        if (len < SEG_HDR) return "segment header truncated at " + len + " bytes";    // H2
        ByteBuffer b = ByteBuffer.wrap(into);
        long p = 0;
        while (b.hasRemaining()) {
            int n = ch.read(b, p);
            if (n < 0) return "segment header short read";
            p += n;
        }
        CRC32 crc = new CRC32();
        crc.update(into, 0, SEG_HDR_CRC_LEN);
        int stored = be32(into, SEG_HDR_CRC_LEN);
        if ((int) crc.getValue() != stored) return "segment header CRC mismatch";     // H3
        for (int i = 0; i < MAGIC.length; i++) {
            if (into[i] != MAGIC[i]) return "not a mapdb WAL segment";                // H4
        }
        int version = be32(into, 8);
        if (version != FORMAT_VERSION) return "!unsupported WAL format version " + version;   // H5
        int flags = be32(into, 12);
        if (flags != 0) return "!unknown segment flags " + flags;                             // H6
        long seq = be64(into, 16);
        if (seq != nameSeq) return "!header sequence " + seq + " does not match its name";     // H7
        if (be64(into, 24) <= 0)
            return "!header firstLsn " + be64(into, 24) + " is not a valid LSN";               // H9
        return null;
    }

    /**
     * <b>The LSN this segment's first section holds</b> — {@code nextLsn} at the moment the writer
     * created it, recorded in the header so recovery never has to infer it. A segment that holds no
     * section still states where its first one would have gone, which is exactly what separates
     * "this segment was always empty" from "its sections vanished".
     */
    static long headerFirstLsn(Segment s) { return be64(s.header, 24); }

    // ---------- the namespace mutations ----------

    /**
     * W2: {@code create → write header → force(true) → fsync the directory}, and only then may a
     * section be appended. Without the directory fsync the whole segment can vanish on a crash,
     * taking acknowledged commits with it; without the size-persisting force the header itself
     * can be lost. Returns the new active segment, appended to the set.
     */
    Segment createSegment(long firstLsn) throws IOException {
        if (readOnly) throw new DBException("read-only WAL open cannot create a segment");
        if (firstLsn <= 0) throw new DBException("segment firstLsn must be positive: " + firstLsn);
        long seq = nextSeq;
        if (seq < 0) throw new DBException("WAL segment sequence overflow");
        nextSeq = seq + 1;
        if (nextSeq < 0) throw new DBException("WAL segment sequence overflow");
        File file = segmentFile(seq);
        byte[] hdr = new byte[SEG_HDR];
        System.arraycopy(MAGIC, 0, hdr, 0, MAGIC.length);
        putBe32(hdr, 8, FORMAT_VERSION);
        putBe32(hdr, 12, 0);
        putBe64(hdr, 16, seq);
        putBe64(hdr, 24, firstLsn);
        CRC32 crc = new CRC32();
        crc.update(hdr, 0, SEG_HDR_CRC_LEN);
        putBe32(hdr, SEG_HDR_CRC_LEN, (int) crc.getValue());

        StoreWAL.walIoEvent(StoreWAL.WalOpKind.CREATE, seq, 0, 0, 0);
        FileChannel ch = FileChannel.open(file.toPath(), StandardOpenOption.CREATE_NEW,
                StandardOpenOption.READ, StandardOpenOption.WRITE);
        boolean ok = false;
        try {
            ByteBuffer b = ByteBuffer.wrap(hdr);
            long p = 0;
            StoreWAL.walIoEvent(StoreWAL.WalOpKind.SEG_HEADER, seq, 0, SEG_HDR, 0);
            while (b.hasRemaining()) p += ch.write(b, p);
            StoreWAL.walIoEvent(StoreWAL.WalOpKind.FORCE_FULL, seq, SEG_HDR, 0, 0);
            ch.force(true);   // the file's SIZE is part of the payload here: never force(false)
            fsyncDir();
            ok = true;
        } finally {
            if (!ok) {
                try {
                    ch.close();
                } catch (IOException ignored) { }
                file.delete();
            }
        }
        try {
            ch.close();          // the Segment reopens on demand, like every other one
        } catch (IOException ignored) { }
        Segment s = new Segment(seq, file, readOnly, hdr, SEG_HDR);
        s.validEnd = SEG_HDR;
        // the segment this one displaces stops growing here, so its length joins the sealed total
        if (!segments.isEmpty()) sealedBytes += segments.get(segments.size() - 1).fileLen;
        segments.add(s);
        return s;
    }

    /**
     * W5: unlink every segment at or below {@code throughSeq}, then fsync the directory. Called
     * only after the {@code 'K'} authorizing it is forced. A failed unlink is a leak that the
     * next open retries (K5/K8) — never permission to advance an unproven mark.
     *
     * <p>Ascending order is used for tidiness only. It does <b>not</b> make the crash-visible set
     * a prefix: syscall order says nothing about the order removals persist before the fsync, so
     * an interior gap is a legitimate crash image (N3/K9).
     */
    void unlinkThrough(long throughSeq) throws IOException {
        if (readOnly || throughSeq <= 0) return;
        int n = 0;
        while (n < segments.size() && segments.get(n).seq <= throughSeq) n++;
        if (n == 0) return;
        // Drop them from the live set BEFORE any delete can throw: the channels are closed here,
        // so a failed unlink must not leave a closed-but-listed segment behind for some later
        // reader to use. The files are then just a leak the next open retries (K5/K8) — never
        // permission to advance an unproven mark.
        //
        // The whole prefix goes at once. Removing index 0 repeatedly shifts the remaining elements
        // once PER SEGMENT, which makes a cleaning pass quadratic in the segment count under the
        // write lock; one sublist clear is a single shift.
        List<Segment> retiring = new ArrayList<>(segments.subList(0, n));
        segments.subList(0, n).clear();
        // RECOMPUTED, not decremented. Subtracting each removed length is correct only while the
        // highest segment is never in the prefix — true today (a mark cannot authorize removing its
        // own segment, K4, and R4 refuses a mark that retires the whole set), but it is a property
        // of two other rules rather than of this method, and getting it wrong drifts the counter
        // silently in the direction that stops cleaning. The recompute is O(remaining) inside a
        // method already O(n) for the sublist clear, so it costs nothing asymptotically.
        sealedBytes = 0;
        for (int i = 0; i + 1 < segments.size(); i++) sealedBytes += segments.get(i).fileLen;
        for (Segment s : retiring) {
            s.close();
            StoreWAL.walIoEvent(StoreWAL.WalOpKind.UNLINK, s.seq, 0, 0, 0);
            Files.deleteIfExists(s.file.toPath());
        }
        fsyncDir();
    }

    void fsyncDir() throws IOException {
        StoreWAL.syncDirectory(dir);
    }

    // ---------- accessors ----------

    List<Segment> segments() { return segments; }

    /** The highest-sequence segment with a valid header, or {@code null} for a fresh store. */
    Segment active() { return segments.isEmpty() ? null : segments.get(segments.size() - 1); }

    long nextSeq() { return nextSeq; }

    /**
     * How many segments currently hold an open channel. Steady state after recovery is at most one
     * — the active segment — and that bound is the point (see {@link Segment#ch}), so it is
     * observable rather than merely intended.
     */
    int openChannelCount() {
        int n = 0;
        for (Segment s : segments) {
            if (s.holdsChannel()) n++;
        }
        return n;
    }

    boolean readOnly() { return readOnly; }

    /** Sum of the segment files' current lengths: what the log actually costs on the device. O(1). */
    long logBytes() {
        return segments.isEmpty() ? 0 : sealedBytes + segments.get(segments.size() - 1).fileLen;
    }

    /** The same number the slow way. Test-only, to pin {@link #sealedBytes} against drift. */
    long logBytesExact() {
        long total = 0;
        for (Segment s : segments) total += s.fileLen;
        return total;
    }

    @Override public void close() {
        closeQuietly();
    }

    private void closeQuietly() {
        for (Segment s : segments) s.close();
        segments.clear();
        try {
            if (lock != null) lock.release();
        } catch (IOException ignored) { }
        try {
            if (lockCh != null) lockCh.close();
        } catch (IOException ignored) { }
        lock = null;
        lockCh = null;
    }

    // ---------- big-endian helpers ----------

    static int be32(byte[] b, int off) {
        return ((b[off] & 0xFF) << 24) | ((b[off + 1] & 0xFF) << 16)
                | ((b[off + 2] & 0xFF) << 8) | (b[off + 3] & 0xFF);
    }

    static long be64(byte[] b, int off) {
        long v = 0;
        for (int i = 0; i < 8; i++) v = (v << 8) | (b[off + i] & 0xFF);
        return v;
    }

    static void putBe32(byte[] b, int off, int v) {
        for (int i = 0; i < 4; i++) b[off + i] = (byte) (v >>> (24 - 8 * i));
    }

    static void putBe64(byte[] b, int off, long v) {
        for (int i = 0; i < 8; i++) b[off + i] = (byte) (v >>> (56 - 8 * i));
    }
}
