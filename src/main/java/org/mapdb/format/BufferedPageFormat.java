package org.mapdb.format;

import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;
import org.mapdb.ser.GroupFormat;
import org.mapdb.ser.Serializer;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.zip.CRC32C;

/**
 * R1 buffered-page format wrapper.
 *
 * <p>A FORMAT-LAYER abstraction: the store stays byte-blind — this class owns
 * the {@code int32 baseLen + base + reverse-readable delta log} page convention extracted
 * from {@code BufferTreeMap}'s original private op-tail. It is reusable by any collection
 * page (BTree leaf/dir, HTree bucket, …); the BASE image is owned by the consumer (this class
 * only frames it and operates on the delta log region), so different page types plug their
 * own base parsing/search under one delta-log codec.
 *
 * <h2>Record content layout</h2>
 * <pre>
 *   0          int32 baseLen                 (consumer-owned consolidated base follows)
 *   4          base[baseLen]
 *   4+baseLen  deltaEntry*                   (append order = oldest first)
 * </pre>
 *
 * <h2>Reverse-readable delta entry</h2>
 * <pre>
 *   0   int32  totalLen    bytes from this field through trailerLen inclusive (== entry size)
 *   4   uint8  opType      {@link #OP_PUT}=1, {@link #OP_DELETE}=2
 *   5   uint8  flags       bit0 hasValue, bit1 hasKeyHash, bit2 hasLsn, bit3 hasCrc
 *   6   [int64 lsn]        present iff hasLsn; omitted (reads as 0) while lsn==0 (see "R2 hook")
 *       [int32 keyHash]    present iff hasKeyHash (FNV-1a over the canonical key bytes);
 *                          offset 6 when the LSN is omitted, 14 when present
 *       varInt keyLen
 *       byte[] keyBytes    key serializer encoded
 *       [varInt valueLen, byte[] valueBytes]   present iff hasValue (PUT only)
 *       [int32  crc32c]    present iff hasCrc; CRC32C over bytes [entryStart, crcPos)
 *       int32  trailerLen  == totalLen; enables newest-first reverse scan
 * </pre>
 *
 * <p><b>Tail discovery uses SIZE, not CRC.</b> The authoritative log terminator is the record
 * content size handed to the read action by the store (torn appends never bump the store's
 * length word — {@code StoreDirect.appendInner}); the per-entry CRC is for bit-rot/verify only
 * ({@link #verifyEntry}), NOT for framing.
 *
 * <h2>The LSN field — an EXPERIMENTAL SEAM, not the adopted format</h2>
 * The LSN is flag-gated ({@code hasLsn}): the store supplies the LSN VALUE and this format
 * writes it while encoding — see {@link AppendContext}. There is deliberately NO "store
 * patches an offset inside opaque bytes" mechanism (that would violate P1). R2 wired the
 * seam ({@code StoreDelta.DeltaEncoder}) but froze nothing: no consumer stamps a real value
 * yet ({@code BufferTreeMap.STAMP_LSN == false}), so the field is omitted from every frame,
 * costs nothing, and point/range reads treat {@code snapshotLsn == Long.MAX_VALUE} so every
 * entry is visible.
 *
 * <p><b>The width, position and granularity are R3's to decide, and are gated.</b> The 8-byte
 * width existed only so the WAL could patch it in place; R2 removed that mechanism, so the
 * width is now free — but stamping the field as-is measured +9.7% device bytes, and the value
 * a consumer would stamp today is a PLACEMENT LSN, which makes the {@code lsn <= snapshotLsn}
 * visibility test incorrect after flush-down re-encodes a committed op. R3 must settle
 * commit-versus-placement semantics, relocation retention, and per-frame versus batch-level
 * granularity together, BEFORE any port implements this layout. No golden bytes may be frozen
 * for ports until then.
 */
public final class BufferedPageFormat<K, V> {

    public static final int OP_PUT = 1;
    public static final int OP_DELETE = 2;

    static final int F_HAS_VALUE = 1;
    static final int F_HAS_KEYHASH = 2;
    static final int F_HAS_LSN = 4;

    /** Smallest well-formed entry: totalLen(4) + op(1) + flags(1) + keyLen varint(1) + trailer(4). */
    static final int MIN_ENTRY_BYTES = 11;
    static final int F_HAS_CRC = 8;

    // ================= base fingerprint (negative-lookup accelerator, #10 on R1) =================
    //
    // A Bloom filter over the BASE keys, stored in a SELF-DESCRIBING header at the START of the
    // base region (so logStart = 4 + baseLen and the delta-log framing are entirely untouched).
    // It lets a point read return ABSENT WITHOUT binary-searching the base when the key is
    // definitely not a base key — after the (authoritative) reverse log scan finds no visible op.
    //
    // NO FALSE NEGATIVES: the filter is built over EXACTLY the base keys during a whole-image base
    // rewrite (create/split/consolidation via updateWithHeadroom, atomic old-or-new); append()
    // never touches the base region, so the filter is always consistent with the persisted base —
    // including across crash/reopen. Bloom monotonicity ⇒ a present base key ALWAYS tests "maybe";
    // only a genuine base non-member can test "absent". The filter gates ONLY the base search; the
    // log (incl. tombstones) is scanned first and authoritatively, so a deleted base key returns
    // ABSENT via the log DELETE and never reaches the filter. Correctness NEVER depends on it (P7):
    // disabled / always-search / randomized all yield identical results.
    //
    // FAIL-CLOSED: m (=fpLen*8) and k are PAGE-CARRIED, and an unknown version / any malformed or
    // out-of-region header THROWS (like the torn-frame guardrails) — never a silent skip. This code
    // is the sole writer of R1 pages (R1 is unshipped: no pre-fingerprint pages exist), and always
    // writes fpVersion=1; the version byte lets a future reader hard-fail rather than misparse.
    //
    //   base region (starts at page offset start+4, length baseLen):
    //     uint8   fpVersion   == FP_VERSION (always present)
    //     packInt fpLen       0 = no filter (dir node, empty/disabled/non-canonical key)
    //     if fpLen>0:
    //       uint8 k           number of probes (page-carried; validated 1..FP_K_MAX)
    //       byte[fpLen] bits  Bloom bit array
    //     <consumer node base follows>
    public static final int FP_VERSION = 1;
    static final int FP_K_MAX = 16;
    static final int FP_MAX_BYTES = 512;

    /**
     * Correctness-neutrality (P7) test knob. It may only ever make {@link #baseFpMightContain}
     * return {@code true} (i.e. force MORE base searching); it can NEVER synthesize a "definite
     * absent" — so results are identical in every mode. Package-private: a JVM-global switch is
     * a test instrument, not shipped API.
     */
    enum FpMode {
        /** Honor the filter (production). */ NORMAL,
        /** Never skip: always full base search (== accelerator disabled). */ FORCE_SEARCH,
        /** Randomly force a full base search (proves neutrality under a random oracle). */ RANDOM
    }

    /** Global test hook; NORMAL in production. Set via {@link #testSetFpMode}. */
    private static volatile FpMode fpMode = FpMode.NORMAL;

    /** Test hook; {@code null} restores {@link FpMode#NORMAL} (production). */
    static void testSetFpMode(FpMode mode) { fpMode = mode == null ? FpMode.NORMAL : mode; }

    /** Parsed + VALIDATED base header; reusable (fill via {@link #parseBaseHeader}). */
    public static final class BaseHeader {
        public int version, fpLen, k, bitsPos, nodeBase, hdrLen;
    }

    /** Non-WAL LSN placeholder (0 = omitted from the frame) until R2 assigns real sequence numbers. */
    public static final long LSN_UNASSIGNED = 0L;

    /** Consolidation refusal (P6): projected consolidated base exceeds the page budget → split me. */
    public static final long SPLIT_REQUIRED = Long.MIN_VALUE;

    /** Shadow/net-effect map marker for a visible DELETE (distinct from any {@code V}). */
    public static final Object TOMBSTONE = new Object();

    /**
     * R2 prerequisite hook. A WAL store supplies the durable LSN VALUE at
     * commit time; the FORMAT writes it while encoding the entry. Passing {@code null} (R1)
     * encodes {@link #LSN_UNASSIGNED}. Intentionally not wired to any store yet.
     */
    public interface AppendContext {
        long nextLsn();
    }

    /** Reusable point-probe result (allocation-free steady path: reuse one instance). */
    public static final class PointResult<V> {
        public boolean matched;   // a visible entry (PUT or DELETE) matched the key
        public boolean put;       // matched && this was a PUT (value valid); else DELETE
        public V value;

        public void clear() { matched = false; put = false; value = null; }
    }

    /** Forward (oldest→newest) decode callback. */
    public interface EntrySink<K, V> {
        void accept(int opType, K key, V value, long lsn);
    }

    private final GroupFormat<K> keyFormat;
    private final GroupFormat<V> valueFormat;
    private final Serializer<K> keyElem;
    private final Serializer<V> valueElem;
    private final Comparator<K> order;
    private final boolean writeKeyHash;
    private final boolean writeCrc;
    private final boolean canonicalKey;
    private final boolean writeBaseFp;
    private final int baseFpBitsPerKey;
    private final int baseFpK;

    public BufferedPageFormat(GroupFormat<K> keyFormat, GroupFormat<V> valueFormat,
                              boolean writeKeyHash, boolean writeCrc) {
        this(keyFormat, valueFormat, writeKeyHash, writeCrc, 0);
    }

    /**
     * @param baseFpBitsPerKey Bloom bits per base key for the negative-lookup accelerator; 0
     *                         disables it. Enabled only when the key element is canonical
     *                         ({@link Serializer#equalsBySerializedBytes()}) — the same discipline
     *                         as {@code keyHash} — so the filter is built and probed over identical
     *                         canonical key bytes.
     */
    public BufferedPageFormat(GroupFormat<K> keyFormat, GroupFormat<V> valueFormat,
                              boolean writeKeyHash, boolean writeCrc, int baseFpBitsPerKey) {
        this.keyFormat = keyFormat;
        this.valueFormat = valueFormat;
        this.keyElem = keyFormat.element();
        this.valueElem = valueFormat.element();
        this.order = keyFormat::compare;
        this.canonicalKey = keyElem.equalsBySerializedBytes();
        // keyHash is only trustworthy over canonical key bytes (equal ⟺ byte-identical);
        // for a non-canonical element serializer we cannot rely on the byte-hash and skip it.
        this.writeKeyHash = writeKeyHash && canonicalKey;
        this.writeCrc = writeCrc;
        this.baseFpBitsPerKey = baseFpBitsPerKey;
        this.writeBaseFp = baseFpBitsPerKey > 0 && canonicalKey;
        // optimal k ≈ bitsPerKey*ln2, clamped to [1, FP_K_MAX]; page-carried anyway.
        this.baseFpK = writeBaseFp
                ? Math.max(1, Math.min(FP_K_MAX, (int) Math.round(baseFpBitsPerKey * 0.6931471805599453)))
                : 0;
    }

    public Comparator<K> order() { return order; }

    // ================= framing =================

    public int readBaseLen(DataInput2 page, int pageStart) {
        page.pos(pageStart);
        int baseLen = page.readInt();
        if (baseLen < 0) throw new IllegalStateException("torn baseLen");
        return baseLen;
    }

    public static int logStart(int pageStart, int baseLen) { return pageStart + 4 + baseLen; }

    // ================= base fingerprint =================

    /** Whether this format writes a base fingerprint (enabled + canonical key). */
    public boolean baseFingerprintEnabled() { return writeBaseFp; }

    /** Filter byte length for {@code keyCount} base keys (0 = no filter). */
    int fpByteLen(int keyCount) {
        if (!writeBaseFp || keyCount <= 0) return 0;
        long bits = (long) keyCount * baseFpBitsPerKey;
        int bytes = (int) ((bits + 7) >>> 3);
        return Math.max(1, Math.min(FP_MAX_BYTES, bytes));
    }

    /**
     * Write the self-describing base header (fpVersion + fpLen + optional k+bits) at the current
     * position. Call it FIRST in the base region (before the consumer node base), so the fingerprint
     * is rebuilt over exactly the current base keys on every whole-image base rewrite (create / split
     * / consolidation). Pass {@code baseKeys == null} (directory node, or to force no filter) to emit
     * an empty header. When enabled+canonical, the filter is built from the SAME
     * {@code keyElem.serialize} canonical bytes that probes use.
     */
    public void writeBaseFp(DataOutput2 out, Object baseKeys) {
        out.writeByte(FP_VERSION);
        int n = baseKeys == null ? 0 : keyFormat.size(baseKeys);
        int fpLen = fpByteLen(n);
        out.packInt(fpLen);
        if (fpLen == 0) return;
        out.writeByte(baseFpK);
        byte[] bits = new byte[fpLen];
        int m = fpLen << 3;
        for (int i = 0; i < n; i++) {
            byte[] kb = serialize(keyElem, keyFormat.get(baseKeys, i));
            setBits(bits, m, baseFpK, kb);
        }
        out.write(bits, 0, fpLen);
    }

    /**
     * Parse + VALIDATE the base header at absolute page offset {@code headerStart} (== pageStart+4),
     * filling {@code out}. {@code baseLen} bounds the base region. Fail-closed: throws
     * {@link IllegalStateException} on an unknown version or any malformed/out-of-region field —
     * NEVER a state that could yield a wrong skip. All base-parse paths must reach the consumer node
     * base via {@code out.nodeBase} computed here.
     */
    public void parseBaseHeader(DataInput2 page, int headerStart, int baseLen, BaseHeader out) {
        if (baseLen < 2) throw new IllegalStateException("base region too short for header");
        long regionEnd = (long) headerStart + baseLen; // long: deliberate overflow-safety
        page.pos(headerStart);
        int version = page.readUnsignedByte();
        if (version != FP_VERSION) throw new IllegalStateException("unknown base header version " + version);
        int fpLen = page.unpackInt();
        if (fpLen < 0 || page.pos() > regionEnd) throw new IllegalStateException("torn fp length");
        int k = 0, bitsPos = -1;
        if (fpLen > 0) {
            if (fpLen > FP_MAX_BYTES) throw new IllegalStateException("fp too large: " + fpLen);
            k = page.readUnsignedByte();
            if (k < 1 || k > FP_K_MAX) throw new IllegalStateException("bad fp k: " + k);
            bitsPos = page.pos();
            if ((long) bitsPos + fpLen > regionEnd) throw new IllegalStateException("fp overruns base region");
        }
        int nodeBase = (fpLen > 0 ? bitsPos + fpLen : page.pos());
        // FAIL-CLOSED: the base region MUST carry a non-empty consumer node base after the fp header.
        // Rejecting nodeBase >= regionEnd is what stops a torn/header-only base from letting a
        // "definitely absent" skip fire before any real node header is parsed (there is nothing to
        // search). Every real page (even an empty leaf writes packInt(flags) ≥ 1 byte) satisfies this.
        if (nodeBase >= regionEnd) throw new IllegalStateException("empty/torn node base after fp header");
        out.version = version;
        out.fpLen = fpLen;
        out.k = k;
        out.bitsPos = bitsPos;
        out.nodeBase = nodeBase;
        out.hdrLen = nodeBase - headerStart;
    }

    /**
     * {@code true} = the key MAY be in the base (must search); {@code false} = the key is DEFINITELY
     * NOT a base key (skip the base search). No false negatives: a base member always tests true.
     * {@code probeKeyBytes} is the canonical key serialization. When there is no filter
     * ({@code h.fpLen == 0}) or under a non-NORMAL {@code fpMode} test knob, returns {@code true} (search) —
     * it can only ever force MORE searching, never a skip.
     */
    public boolean baseFpMightContain(DataInput2 page, BaseHeader h, byte[] probeKeyBytes) {
        FpMode mode = fpMode;
        if (mode == FpMode.FORCE_SEARCH) return true;
        if (h.fpLen == 0 || probeKeyBytes == null) return true;
        if (mode == FpMode.RANDOM && java.util.concurrent.ThreadLocalRandom.current().nextBoolean()) return true;
        int m = h.fpLen << 3;
        long hh = fnv64(probeKeyBytes);
        long h1 = hh & 0xFFFFFFFFL;
        long h2 = ((hh >>> 32) & 0xFFFFFFFFL) | 1L; // odd → non-degenerate double hashing
        for (int i = 0; i < h.k; i++) {
            int idx = (int) ((h1 + (long) i * h2) % m);
            page.pos(h.bitsPos + (idx >>> 3));
            if ((page.readUnsignedByte() & (1 << (idx & 7))) == 0) return false; // definitely absent
        }
        return true;
    }

    private static void setBits(byte[] bits, int m, int k, byte[] keyBytes) {
        long hh = fnv64(keyBytes);
        long h1 = hh & 0xFFFFFFFFL;
        long h2 = ((hh >>> 32) & 0xFFFFFFFFL) | 1L;
        for (int i = 0; i < k; i++) {
            int idx = (int) ((h1 + (long) i * h2) % m);
            bits[idx >>> 3] |= (byte) (1 << (idx & 7));
        }
    }

    /**
     * FNV-1a 64-bit + murmur3 fmix64 finalizer; split into two 32-bit halves for double hashing.
     * The avalanche finalizer is essential: raw FNV-1a distributes clustered/structured serialized
     * keys (e.g. adjacent big-endian longs differing only in low bytes) poorly, inflating the
     * false-positive rate. Writer ({@link #setBits}) and reader ({@link #baseFpMightContain}) both
     * call this, so they stay bit-for-bit consistent (a prerequisite for no false negatives).
     */
    static long fnv64(byte[] b) {
        long h = 0xcbf29ce484222325L;
        for (byte value : b) {
            h ^= (value & 0xFF);
            h *= 0x100000001b3L;
        }
        h ^= h >>> 33;
        h *= 0xff51afd7ed558ccdL;
        h ^= h >>> 33;
        h *= 0xc4ceb9fe1a85ec53L;
        h ^= h >>> 33;
        return h;
    }

    // ================= encode =================

    public byte[] encode(int opType, K key, V value, long lsn) {
        DataOutput2 out = new DataOutput2(48);
        encodeInto(out, opType, key, value, lsn);
        return out.copyBytes();
    }

    public byte[] encode(int opType, K key, V value, AppendContext ctx) {
        return encode(opType, key, value, ctx == null ? LSN_UNASSIGNED : ctx.nextLsn());
    }

    /** Appends one framed delta entry to {@code out}. {@code value} must be non-null iff PUT. */
    public void encodeInto(DataOutput2 out, int opType, K key, V value, long lsn) {
        if (opType != OP_PUT && opType != OP_DELETE) throw new IllegalArgumentException("opType " + opType);
        boolean hasValue = opType == OP_PUT;
        // lsn==0 (the R1 constant: no snapshots yet) is omitted from the frame — the
        // always-written 8-byte LSN was pure per-op overhead that shrank the effective
        // op-log budget and so INCREASED write amplification, against R1's goal.
        int flags = (lsn != 0 ? F_HAS_LSN : 0)
                | (hasValue ? F_HAS_VALUE : 0)
                | (writeKeyHash ? F_HAS_KEYHASH : 0)
                | (writeCrc ? F_HAS_CRC : 0);

        byte[] keyBytes = serialize(keyElem, key);
        byte[] valBytes = hasValue ? serialize(valueElem, value) : null;

        int start = out.pos;
        out.writeInt(0);            // totalLen placeholder (backpatched)
        out.writeByte(opType);
        out.writeByte(flags);
        if (lsn != 0) out.writeLong(lsn);
        if (writeKeyHash) out.writeInt(hash(keyBytes));
        out.packInt(keyBytes.length);
        out.write(keyBytes, 0, keyBytes.length);
        if (hasValue) {
            out.packInt(valBytes.length);
            out.write(valBytes, 0, valBytes.length);
        }
        int crcPos = out.pos;
        int totalLen = (crcPos - start) + (writeCrc ? 4 : 0) + 4;
        patchInt(out.buf, start, totalLen);
        if (writeCrc) {
            CRC32C crc = new CRC32C();
            crc.update(out.buf, start, crcPos - start);
            out.writeInt((int) crc.getValue());
        }
        out.writeInt(totalLen);     // trailerLen
    }

    // ================= forward decode (oldest → newest) =================

    /**
     * Decode the log region {@code [logStart, logEnd)} in append order (oldest first), calling
     * {@code sink} per entry. Used for node materialization and consolidation net-effect.
     */
    public void forEachEntryOldestFirst(DataInput2 page, int logStart, int logEnd, EntrySink<K, V> sink) {
        int pos = logStart;
        while (pos < logEnd) {
            page.pos(pos);
            int totalLen = page.readInt();
            if (totalLen < MIN_ENTRY_BYTES || pos + totalLen > logEnd) throw new IllegalStateException("torn delta entry");
            Decoded<K, V> d = decodeAt(page, pos, totalLen);
            sink.accept(d.opType, d.key, d.value, d.lsn);
            pos += totalLen;
        }
    }

    // ================= reverse newest-first point probe =================

    /**
     * Scan the log region newest-first (reverse) and STOP at the first visible match — a true
     * last-writer-wins point read. {@code probeKeyBytes} (canonical serializer)
     * selects the in-place byte-compare fast path; pass {@code null} to decode+compare.
     * {@code snapshotLsn == Long.MAX_VALUE} (R1) makes every entry visible. Returns true and
     * fills {@code out} when a PUT or DELETE matched; false when the log has no match.
     *
     * <p>Allocation: with {@code probeKeyBytes != null} a MISS/SKIP allocates nothing (byte
     * compare + {@code keyHash} reject in place). A PUT hit deserializes the
     * value; a non-canonical probe ({@code probeKeyBytes == null}) deserializes each candidate
     * key — same cost as the pre-R1 op-tail scan (P4 holds for the canonical-key miss path).
     *
     * <p>keyHash is only used as a NEGATIVE filter and only when the key element serializer is
     * canonical ({@link Serializer#equalsBySerializedBytes()}). The codebase's standing invariant
     * (BufferTreeMap op-tail scan) is that a format overriding {@link GroupFormat#compare} to be
     * coarser than element byte-equality must NOT declare its element canonical; that invariant is
     * exactly what makes the hash/byte-equality prefilter sound here.
     */
    public boolean probeNewestFirst(DataInput2 page, int logStart, int logEnd, K key,
                                    byte[] probeKeyBytes, long snapshotLsn, PointResult<V> out) {
        out.clear();
        int probeHash = (writeKeyHash && probeKeyBytes != null) ? hash(probeKeyBytes) : 0;
        int pos = logEnd;
        while (pos > logStart) {
            page.pos(pos - 4);
            int totalLen = page.readInt();
            int start = pos - totalLen;
            if (totalLen < MIN_ENTRY_BYTES || start < logStart) throw new IllegalStateException("torn delta trailer");
            page.pos(start);
            int leadLen = page.readInt();
            if (leadLen != totalLen) throw new IllegalStateException("delta len mismatch");
            int opType = page.readByte() & 0xFF;
            int flags = page.readByte() & 0xFF;
            long lsn = (flags & F_HAS_LSN) != 0 ? page.readLong() : 0;
            boolean visible = lsn <= snapshotLsn;
            int keyHash = 0;
            if ((flags & F_HAS_KEYHASH) != 0) keyHash = page.readInt();
            int keyLen = page.unpackInt();
            int keyPos = page.pos();
            boolean keyMatch;
            if (!visible) {
                keyMatch = false;
            } else if ((flags & F_HAS_KEYHASH) != 0 && probeKeyBytes != null && keyHash != probeHash) {
                keyMatch = false;                       // cheap hash reject, no key decode
            } else if (probeKeyBytes != null) {
                keyMatch = keyLen == probeKeyBytes.length && page.matchBytes(probeKeyBytes);
            } else {
                K k = keyElem.deserialize(page, keyLen);
                keyMatch = keyFormat.compare(k, key) == 0;
            }
            if (keyMatch) {
                out.matched = true;
                if ((flags & F_HAS_VALUE) != 0) {
                    page.pos(keyPos + keyLen);
                    int valLen = page.unpackInt();
                    out.value = valueElem.deserialize(page, valLen);
                    out.put = true;
                } else {
                    out.put = false;                    // DELETE: definitively absent
                }
                return true;
            }
            pos = start;
        }
        return false;
    }

    // ================= range / consolidation shadows =================

    /**
     * Materialize the newest visible op per key with key in {@code [lo,hi]} (null bound = open),
     * into {@code shadowOut} (value = {@code V} for PUT, {@link #TOMBSTONE} for DELETE). Scans
     * newest-first so the FIRST op seen per key wins; allocates only O(distinct visible keys in
     * range) — the BASE image is never touched. The log is capped by consolidation, bounding it.
     */
    public void logShadowInRange(DataInput2 page, int logStart, int logEnd, K lo, boolean loInc,
                                 K hi, boolean hiInc, long snapshotLsn, NavigableMap<K, Object> shadowOut) {
        int pos = logEnd;
        while (pos > logStart) {
            page.pos(pos - 4);
            int totalLen = page.readInt();
            int start = pos - totalLen;
            if (totalLen < MIN_ENTRY_BYTES || start < logStart) throw new IllegalStateException("torn delta trailer");
            Decoded<K, V> d = decodeAt(page, start, totalLen);
            pos = start;
            if (d.lsn > snapshotLsn) continue;
            if (!inRange(d.key, lo, loInc, hi, hiInc)) continue;
            if (shadowOut.containsKey(d.key)) continue;   // newer op already recorded (LWW)
            shadowOut.put(d.key, d.opType == OP_PUT ? d.value : TOMBSTONE);
        }
    }

    /** Whole-log net effect (newest visible op per key) for consolidation. */
    public void netEffect(DataInput2 page, int logStart, int logEnd, long snapshotLsn,
                          NavigableMap<K, Object> netOut) {
        logShadowInRange(page, logStart, logEnd, null, true, null, true, snapshotLsn, netOut);
    }

    /**
     * ASCENDING merge of a byte-side base cursor with a range-restricted shadow (log wins on tie;
     * {@link #TOMBSTONE} drops the entry). Both inputs MUST be in ascending {@code order}; the base
     * side is pulled through {@code base} lazily (no base materialization). A caller wanting
     * descending output collects this ascending stream and reverses it — cheap for a bounded range,
     * and it keeps the base cursor and shadow iterator in a single, consistent direction.
     */
    public Iterator<Map.Entry<K, V>> merge(KvCursor<K, V> base, NavigableMap<K, Object> shadow) {
        return new MergeIterator(base, shadow.entrySet().iterator(), order);
    }

    /** Byte-side base cursor supplied by the consumer (BTree leaf, HTree bucket, …). */
    public interface KvCursor<K, V> {
        boolean hasNext();
        void next();
        K key();
        V value();
    }

    private final class MergeIterator implements Iterator<Map.Entry<K, V>> {
        private final KvCursor<K, V> base;
        private final Iterator<Map.Entry<K, Object>> shadowIt;
        private final Comparator<K> cmp;
        private Map.Entry<K, Object> sEntry;
        private boolean baseHas;
        private Map.Entry<K, V> nxt;
        private boolean done;

        MergeIterator(KvCursor<K, V> base, Iterator<Map.Entry<K, Object>> shadowIt, Comparator<K> cmp) {
            this.base = base;
            this.shadowIt = shadowIt;
            this.cmp = cmp;
            baseHas = base.hasNext();
            if (baseHas) base.next();
            sEntry = shadowIt.hasNext() ? shadowIt.next() : null;
        }

        @SuppressWarnings("unchecked")
        private void advance() {
            if (done || nxt != null) return;
            while (true) {
                if (!baseHas && sEntry == null) { done = true; return; }
                int c;
                if (!baseHas) c = 1;             // only shadow left
                else if (sEntry == null) c = -1; // only base left
                else c = cmp.compare(base.key(), sEntry.getKey());
                if (c < 0) {                      // base-only key
                    nxt = entry(base.key(), base.value());
                    baseHas = base.hasNext();
                    if (baseHas) base.next();
                    return;
                } else if (c > 0) {               // shadow-only key
                    Map.Entry<K, Object> s = sEntry;
                    sEntry = shadowIt.hasNext() ? shadowIt.next() : null;
                    if (s.getValue() != TOMBSTONE) { nxt = entry(s.getKey(), (V) s.getValue()); return; }
                } else {                          // same key: shadow (log) wins
                    Map.Entry<K, Object> s = sEntry;
                    sEntry = shadowIt.hasNext() ? shadowIt.next() : null;
                    baseHas = base.hasNext();
                    if (baseHas) base.next();
                    if (s.getValue() != TOMBSTONE) { nxt = entry(s.getKey(), (V) s.getValue()); return; }
                }
            }
        }

        @Override public boolean hasNext() { advance(); return nxt != null; }

        @Override public Map.Entry<K, V> next() {
            advance();
            if (nxt == null) throw new NoSuchElementException();
            Map.Entry<K, V> e = nxt;
            nxt = null;
            return e;
        }
    }

    private static <K, V> Map.Entry<K, V> entry(K k, V v) {
        return new java.util.AbstractMap.SimpleImmutableEntry<>(k, v);
    }

    // ================= consolidation primitive =================

    /** Result of {@link #consolidate}: fresh net key/value groups, or a split-me verdict. */
    public static final class Consolidated {
        /** Merged key group, or {@code null} when {@link #splitRequired}. */
        public final Object keys;
        /** Merged value group, or {@code null} when {@link #splitRequired}. */
        public final Object values;
        public final int keyCount;
        public final boolean splitRequired;

        private Consolidated(Object keys, Object values, int keyCount, boolean splitRequired) {
            this.keys = keys;
            this.values = values;
            this.keyCount = keyCount;
            this.splitRequired = splitRequired;
        }
    }

    /**
     * Format-layer LEAF consolidation. Folds the newest visible op per key into a fresh
     * key/value group pair — retention = newest op per key, tombstones drop (no R3 snapshots).
     * Returns a {@link Consolidated} carrying the merged GROUPS (NOT node bytes: this primitive
     * is base-shape agnostic — the consumer owns node flags/link/header framing, so it composes
     * the returned groups into its own base image). When the projected key count exceeds
     * {@code maxBaseKeys} it returns {@code splitRequired} — the terminal P6 "split me"
     * outcome, never a livelock.
     *
     * <p>Only for key/value (leaf) pages: a directory base carries child recids, not values, so
     * merging keyed PUT/DELETE ops into it is nonsensical (that consolidation is the tree's job).
     * Consolidation is a cold path, so materializing the base groups here is acceptable (unlike
     * reads, which stay byte-side).
     */
    public Consolidated consolidate(Object baseKeys, Object baseVals, DataInput2 page, int logStart,
                                    int logEnd, long snapshotLsn, int maxBaseKeys) {
        TreeMap<K, Object> net = new TreeMap<>(order);
        netEffect(page, logStart, logEnd, snapshotLsn, net);

        int n = keyFormat.size(baseKeys);
        java.util.ArrayList<Object> outKeys = new java.util.ArrayList<>(n + net.size());
        java.util.ArrayList<Object> outVals = new java.util.ArrayList<>(n + net.size());
        Iterator<Map.Entry<K, Object>> opIt = net.entrySet().iterator();
        Map.Entry<K, Object> op = opIt.hasNext() ? opIt.next() : null;
        int i = 0;
        while (i < n || op != null) {
            K bk = i < n ? keyFormat.get(baseKeys, i) : null;
            int c = (bk == null) ? 1 : (op == null) ? -1 : order.compare(bk, op.getKey());
            if (c < 0) {
                outKeys.add(bk);
                outVals.add(valueFormat.get(baseVals, i));
                i++;
            } else if (c > 0) {
                if (op.getValue() != TOMBSTONE) { outKeys.add(op.getKey()); outVals.add(op.getValue()); }
                op = opIt.hasNext() ? opIt.next() : null;
            } else {
                if (op.getValue() != TOMBSTONE) { outKeys.add(op.getKey()); outVals.add(op.getValue()); }
                i++;
                op = opIt.hasNext() ? opIt.next() : null;
            }
        }
        if (outKeys.size() > maxBaseKeys) return new Consolidated(null, null, outKeys.size(), true);
        return new Consolidated(keyFormat.fromArray(outKeys.toArray()),
                valueFormat.fromArray(outVals.toArray()), outKeys.size(), false);
    }

    // ================= CRC verify (bit-rot / verify only) =================

    /**
     * Verify one entry's CRC32C over {@code [entryStart, crcPos)}. Framing (record SIZE) discovers
     * entries; this CRC only detects bit-rot and is NOT consulted on the hot read path.
     * {@code totalLen} is the entry size (leading/trailing length word). Note this is CRC32C,
     * distinct from the CRC32 the WAL uses for its sections (different checksum, different scope).
     */
    public boolean verifyEntry(DataInput2 page, int entryStart, int totalLen) {
        page.pos(entryStart + 5); // skip totalLen(4) + opType(1) → flags
        int flags = page.readByte() & 0xFF;
        if ((flags & F_HAS_CRC) == 0) return true; // no CRC stored
        int crcPos = entryStart + totalLen - 8; // 4 crc + 4 trailer
        byte[] tmp = new byte[crcPos - entryStart];
        page.pos(entryStart);
        page.readFully(tmp, 0, tmp.length);
        CRC32C crc = new CRC32C();
        crc.update(tmp, 0, tmp.length);
        page.pos(crcPos);
        int stored = page.readInt();
        return (int) crc.getValue() == stored;
    }

    // ================= helpers =================

    private static final class Decoded<K, V> {
        int opType;
        long lsn;
        K key;
        V value;
    }

    private Decoded<K, V> decodeAt(DataInput2 page, int start, int totalLen) {
        page.pos(start);
        int leadLen = page.readInt();
        if (leadLen != totalLen) throw new IllegalStateException("delta len mismatch");
        Decoded<K, V> d = new Decoded<>();
        d.opType = page.readByte() & 0xFF;
        int flags = page.readByte() & 0xFF;
        d.lsn = (flags & F_HAS_LSN) != 0 ? page.readLong() : 0;
        if ((flags & F_HAS_KEYHASH) != 0) page.readInt();
        int keyLen = page.unpackInt();
        int keyPos = page.pos();
        d.key = keyElem.deserialize(page, keyLen);
        page.pos(keyPos + keyLen);
        if ((flags & F_HAS_VALUE) != 0) {
            int valLen = page.unpackInt();
            d.value = valueElem.deserialize(page, valLen);
        }
        return d;
    }

    private boolean inRange(K key, K lo, boolean loInc, K hi, boolean hiInc) {
        if (lo != null) {
            int c = order.compare(key, lo);
            if (c < 0 || (c == 0 && !loInc)) return false;
        }
        if (hi != null) {
            int c = order.compare(key, hi);
            if (c > 0 || (c == 0 && !hiInc)) return false;
        }
        return true;
    }

    private static byte[] serialize(Serializer<?> ser, Object v) {
        DataOutput2 scratch = new DataOutput2(32);
        @SuppressWarnings("unchecked")
        Serializer<Object> s = (Serializer<Object>) ser;
        s.serialize(scratch, v);
        return scratch.copyBytes();
    }

    /** FNV-1a 32-bit over the canonical key bytes. */
    static int hash(byte[] b) {
        int h = 0x811C9DC5;
        for (byte value : b) {
            h ^= (value & 0xFF);
            h *= 0x01000193;
        }
        return h;
    }

    private static void patchInt(byte[] buf, int off, int v) {
        buf[off] = (byte) (v >>> 24);
        buf[off + 1] = (byte) (v >>> 16);
        buf[off + 2] = (byte) (v >>> 8);
        buf[off + 3] = (byte) v;
    }
}
