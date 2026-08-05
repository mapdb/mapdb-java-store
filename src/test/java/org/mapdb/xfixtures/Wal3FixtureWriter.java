package org.mapdb.xfixtures;

import org.mapdb.store.StoreWAL;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

import static org.mapdb.xfixtures.FixtureWriter.RAW;
import static org.mapdb.xfixtures.FixtureWriter.check;
import static org.mapdb.xfixtures.FixtureWriter.payload;
import static org.mapdb.xfixtures.FixtureWriter.sha256Hex;

/**
 * Stage C slice <b>C2j</b>: the java deterministic generator for the two WAL v3 accept bundles,
 * {@code wal3-java-tail} and {@code wal3-java-cleaned} (contract §5.2, §5.3, §5.3.1, §5.4).
 *
 * <pre>
 *   mvn -q test-compile
 *   java -ea -cp target/test-classes:target/classes \
 *       org.mapdb.xfixtures.Wal3FixtureWriter --out &lt;dir&gt; [--force]
 * </pre>
 *
 * Writes {@code &lt;out&gt;/wal3-java-tail/}, {@code &lt;out&gt;/wal3-java-cleaned/} (segments only,
 * base {@code x}), plus {@code fragment.tsv} and {@code layout.tsv}. Assembly, compression and
 * the {@code expect}/{@code post} rows are the sync script's job (C4), not this generator's.
 *
 * <h2>Why the {@code cleaned} workload is not §5.3's literal one</h2>
 *
 * §5.3.1 requires three retained segments, a middle one whose first two sections are
 * entry-bearing, and an active one holding exactly one section under {@code segmentBytes}.
 * §5.3's literal "T1&ndash;T3 then {@code checkpoint()}" cannot produce that, and the reason is
 * the rollover rule: {@code activeSeg.fileLen >= segmentBytes && !activeSeg.empty()} is tested
 * BEFORE a section is appended ({@code StoreWAL.java:1688}), so sections pile into a segment
 * until it crosses the limit and only the NEXT one rotates. Checkpointing after T3 makes the
 * cleaner's image cover F's 1.2 MB of live data, which overflows the segment holding it, so the
 * forced {@code 'K'} mark lands as the FIRST section of the next segment — precisely where
 * §5.3.1 row 2 forbids it.
 *
 * <p>Measured, not reasoned: {@code Wal3ShapeProbe} runs the candidates and
 * {@code todo/store-wal3/probes/dump-bundle.py} reports the witness rows executably. Variant
 * {@code spec} put the mark at segment 3 section 0; variant {@code ckpt-after-t2} moved it beside
 * the {@code 'C'} image as predicted but retained only two segments; the variant below is the one
 * that passes all seven checkable rows. §5.3.1 explicitly authorises the generator to shape the
 * workload, and every shaping transaction here is state-preserving, so §5.3's "same final
 * logical state as tail" survives verbatim — {@link #assertFinalState} is what enforces that.
 *
 * <h2>Self-checks</h2>
 *
 * The published bytes are re-parsed by {@link Seg}, a local minimal v3 decoder that verifies both
 * CRCs. It is deliberately NOT the engine's parser and NOT {@code walfmt.py}: a generator that
 * self-checks with the same code that wrote the bytes checks nothing, and the python codec is the
 * other half of a cross-check that only works if the two halves are independent. Row 6 of §5.3.1
 * ({@code fileLen < segmentBytes}) is asserted HERE and nowhere else — {@code segmentBytes} is a
 * generator setting and is not recoverable from the published bytes at all.
 */
public final class Wal3FixtureWriter {

    private Wal3FixtureWriter() {}

    // ---------- §5.1 common configuration ----------

    /** §5.1, pinned: rotates deterministically without 64 MiB fixtures. Setter floor is 61. */
    static final long SEGMENT_BYTES = 65_536;
    /**
     * §5.1: above the whole workload's byte total, so {@link StoreWAL#cleaningDue} never fires and
     * the budgeted, wall-clock-bounded auto-cleaner — which would make the bytes host-dependent —
     * cannot start. Asserted after the fact by {@link StoreWAL#cleanerBytesWritten()}.
     */
    static final long MIN_LOG_BYTES = 64L * 1024 * 1024;

    static final String TAIL_ID = "wal3-java-tail";
    static final String CLEANED_ID = "wal3-java-cleaned";
    /** §5: distinct per bundle so content differs even where recids coincide. */
    static final int TAIL_BASE = 101, CLEANED_BASE = 111;
    static final String BASE_NAME = "x";

    // ---------- the workloads ----------

    /** Recids the workload allocated. Both shapes allocate the same six, in the same order. */
    static final class Recids {
        long a, b, c, d, e, f;
    }

    private static StoreWAL open(File base) {
        StoreWAL s = new StoreWAL(base);
        s.setSegmentBytes(SEGMENT_BYTES);
        s.setMinLogBytes(MIN_LOG_BYTES);
        return s;
    }

    private static void t1(StoreWAL s, Recids r, int base) {
        r.a = s.put(payload(base + 0, 100), RAW);
        r.b = s.put(payload(base + 1, 0), RAW);
        r.c = s.put(payload(base + 2, 40), RAW);
        s.commit();
    }

    private static void t2(StoreWAL s, Recids r) {
        s.update(r.c, null, RAW);
        r.d = s.preallocate();
        s.commit();
    }

    private static void t3(StoreWAL s, Recids r, int base) {
        r.e = s.put(payload(base + 3, 256), RAW);
        r.f = s.put(payload(base + 4, 1_200_000), RAW);
        s.commit();
    }

    private static void t4(StoreWAL s, Recids r, int base) {
        s.delete(r.e, RAW);
        s.update(r.a, payload(base + 5, 120), RAW);
        s.commit();
    }

    /** §5.2's T1&ndash;T5: no cleaning ever runs, and T5 rolls back. */
    private static void tailWorkload(File base, Recids r) {
        StoreWAL s = open(base);
        try {
            t1(s, r, TAIL_BASE);
            t2(s, r);
            t3(s, r, TAIL_BASE);
            t4(s, r, TAIL_BASE);
            s.put(payload(TAIL_BASE + 6, 64), RAW);   // T5: must leave no trace
            s.rollback();
            check(s.cleanerBytesWritten() == 0,
                    "the tail shape must contain no cleaner output, but the cleaner wrote "
                            + s.cleanerBytesWritten() + " bytes");
            assertFinalState(s, r, TAIL_ID, TAIL_BASE);
        } finally {
            s.close();
        }
    }

    /**
     * §5.3's shape, with the checkpoint moved and two state-preserving shaping transactions —
     * see the class javadoc for the measurement that forces each.
     */
    private static void cleanedWorkload(File base, Recids r) {
        StoreWAL s = open(base);
        try {
            t1(s, r, CLEANED_BASE);
            t2(s, r);
            check(s.cleanerBytesWritten() == 0,
                    "auto-clean began before the explicit checkpoint: the bundle would stop at a "
                            + "machine-speed-dependent point (§5.1, §5.4 obligation 3)");
            s.checkpoint();                       // the ONLY cleaning, and it is unbudgeted
            long afterCheckpoint = s.cleanerBytesWritten();
            check(afterCheckpoint > 0, "checkpoint() wrote no image: this is not a cleaned shape");

            t3(s, r, CLEANED_BASE);               // 1.2 MB: oversizes the LOWEST retained segment

            // The middle segment's first two sections. C is null before and after, so no recid
            // and no final state changes; the second one is row 5's size-preserving T_APPEND
            // candidate (a null T_RECORD and a payload-free T_APPEND are the same four bytes),
            // and row 5 reads section index 1, so the order of these two is load-bearing.
            s.update(r.c, payload(CLEANED_BASE + 6, 48), RAW);
            s.commit();
            s.update(r.c, null, RAW);
            s.commit();

            t4(s, r, CLEANED_BASE);

            // Oversize the middle segment, then commit once more so the LAST commit lands alone
            // in a fresh segment: rollover is tested before appending, so the oversized section
            // joins the segment it overflows and only its successor rotates. Both halves rewrite
            // A and the second restores A's §5.2 content, so the final state is untouched.
            // "Only its successor rotates" is about ORDINARY appends. Cleaning also rotates, at
            // two unconditional episode seals (`:2322`, `:2494`); neither is reachable here,
            // because the sole checkpoint is already behind us and minLogBytes keeps auto-clean
            // from ever firing (§5.1). Using one of those to rotate instead would change the
            // cleaning history the whole shape is built around.
            s.update(r.a, payload(CLEANED_BASE + 7, (int) SEGMENT_BYTES), RAW);
            s.commit();
            s.update(r.a, payload(CLEANED_BASE + 5, 120), RAW);
            s.commit();

            check(s.cleanerBytesWritten() == afterCheckpoint,
                    "cleaning ran a second time after the checkpoint: §5.4 obligation 4 allows "
                            + "exactly one episode, at one prescribed boundary");
            assertFinalState(s, r, CLEANED_ID, CLEANED_BASE);
        } finally {
            s.close();
        }
    }

    /** The final logical state §5.2 pins, which §5.3 shares verbatim. */
    static void assertFinalState(StoreWAL s, Recids r, String ctx, int base) {
        FixtureWriter.assertReaderContract(s, expects(r, base), ctx);
    }

    static List<FixtureWriter.RecidExpect> expects(Recids r, int base) {
        return Arrays.asList(
                new FixtureWriter.RecidExpect("A", r.a, "live", base + 5, 120),
                new FixtureWriter.RecidExpect("B", r.b, "live", base + 1, 0),
                new FixtureWriter.RecidExpect("C", r.c, "null", base + 2, 40),
                new FixtureWriter.RecidExpect("D", r.d, "prealloc", 0, 0),
                new FixtureWriter.RecidExpect("E", r.e, "deleted", base + 3, 256),
                new FixtureWriter.RecidExpect("F", r.f, "live", base + 4, 1_200_000));
    }

    // ---------- a local, independent v3 decoder (self-check only) ----------
    //
    // Layout, from the format section of the contract and WalSegmentSet: a 36-byte segment header
    // magic[8] | version(4) | flags(4) | seq(8) | firstLsn(8) | crc32(4) with the CRC over the
    // first 32 bytes; then 25-byte section headers tag(1) | lsn(8) | bodyLen(8) | hdrCrc(4) |
    // bodyCrc(4), the header CRC over the first 17 of those bytes PLUS the 36 header bytes and
    // be64(sectionOffset) — the offset is in the domain, which is what makes a section
    // un-relocatable.

    static final int SEG_HDR = 36, SEG_HDR_CRC_LEN = 32, SEC_HDR = 25, SEC_HDR_CRC_LEN = 17;
    static final byte[] MAGIC = "MDBS.WAL".getBytes(StandardCharsets.US_ASCII);
    static final int FORMAT_VERSION = 3;
    static final int MARK_BODY_LEN = 16;

    /** One decoded section: its offset, tag, LSN and body length. */
    static final class Sec {
        final int off;
        final char tag;
        final long lsn, bodyLen;

        Sec(int off, char tag, long lsn, long bodyLen) {
            this.off = off; this.tag = tag; this.lsn = lsn; this.bodyLen = bodyLen;
        }
    }

    /** One decoded segment: both CRCs verified, every section walked. */
    static final class Seg {
        final String relName;
        final byte[] raw;
        final long seq, firstLsn;
        final List<Sec> sections = new ArrayList<>();
        /** Decoded {@code 'K'} body, or null when this segment carries no mark. */
        long markThrough = -1, markLogStart = -1;
        int markIndex = -1;

        Seg(String relName, byte[] raw) {
            this.relName = relName;
            this.raw = raw;
            check(raw.length >= SEG_HDR, relName + ": shorter than a segment header");
            check(Arrays.equals(Arrays.copyOf(raw, 8), MAGIC), relName + ": bad magic");
            check(be32(raw, 8) == FORMAT_VERSION, relName + ": version " + be32(raw, 8) + " != 3");
            check(be32(raw, 12) == 0, relName + ": nonzero header flags");
            this.seq = be64(raw, 16);
            this.firstLsn = be64(raw, 24);
            check(crc(raw, 0, SEG_HDR_CRC_LEN) == (be32(raw, 32) & 0xFFFFFFFFL),
                    relName + ": segment header CRC mismatch");
            int off = SEG_HDR;
            while (off < raw.length) {
                check(off + SEC_HDR <= raw.length, relName + ": truncated section header at " + off);
                char tag = (char) (raw[off] & 0xFF);
                check(tag == 'S' || tag == 'C' || tag == 'K',
                        relName + ": unknown section tag '" + tag + "' at " + off);
                long lsn = be64(raw, off + 1), bodyLen = be64(raw, off + 9);
                CRC32 c = domain(raw, off);
                c.update(raw, off, SEC_HDR_CRC_LEN);
                check(c.getValue() == (be32(raw, off + 17) & 0xFFFFFFFFL),
                        relName + ": section header CRC mismatch at " + off);
                // Subtract rather than add: `off + SEC_HDR + bodyLen <= raw.length` reads more
                // naturally and OVERFLOWS for a bodyLen near Long.MAX_VALUE, wrapping negative
                // and passing the very check it is — the walk would then crash on the (int) cast
                // instead of refusing. The remaining-bytes form cannot overflow, because both
                // operands are already bounded by the file length.
                check(bodyLen >= 0 && bodyLen <= raw.length - off - SEC_HDR,
                        relName + ": section body at " + off + " claims " + bodyLen
                                + " bytes, past the end of a " + raw.length + "-byte file");
                CRC32 cb = domain(raw, off);
                cb.update(raw, off + SEC_HDR, (int) bodyLen);
                check(cb.getValue() == (be32(raw, off + 21) & 0xFFFFFFFFL),
                        relName + ": section body CRC mismatch at " + off);
                if (tag == 'K') {
                    check(bodyLen == MARK_BODY_LEN,
                            relName + ": a 'K' body is " + bodyLen + " bytes, not " + MARK_BODY_LEN);
                    check(markIndex < 0, relName + ": two 'K' marks in one segment");
                    markIndex = sections.size();
                    markThrough = be64(raw, off + SEC_HDR);
                    markLogStart = be64(raw, off + SEC_HDR + 8);
                }
                sections.add(new Sec(off, tag, lsn, bodyLen));
                off += SEC_HDR + (int) bodyLen;
            }
            check(off == raw.length, relName + ": trailing bytes after the last section");
            check(!sections.isEmpty(), relName + ": a published segment with no sections");
        }

        Sec first() { return sections.get(0); }
        Sec last() { return sections.get(sections.size() - 1); }
    }

    private static long crc(byte[] b, int off, int len) {
        CRC32 c = new CRC32();
        c.update(b, off, len);
        return c.getValue();
    }

    /**
     * A CRC primed with a section's domain — all 36 header bytes then {@code be64(sectionOffset)},
     * fed BEFORE the section's own bytes. The offset being inside the domain is what makes a
     * section un-relocatable, and getting the order wrong is why this decoder is written out
     * rather than shared with the writer.
     */
    private static CRC32 domain(byte[] raw, int sectionOff) {
        CRC32 c = new CRC32();
        c.update(raw, 0, SEG_HDR);
        c.update(be64bytes(sectionOff), 0, 8);
        return c;
    }

    private static long be32(byte[] b, int off) {
        return ((long) (b[off] & 0xFF) << 24) | ((b[off + 1] & 0xFF) << 16)
                | ((b[off + 2] & 0xFF) << 8) | (b[off + 3] & 0xFF);
    }

    private static long be64(byte[] b, int off) {
        long v = 0;
        for (int i = 0; i < 8; i++) v = (v << 8) | (b[off + i] & 0xFFL);
        return v;
    }

    private static byte[] be64bytes(long v) {
        byte[] out = new byte[8];
        for (int i = 7; i >= 0; i--) { out[i] = (byte) v; v >>>= 8; }
        return out;
    }

    /** {@code String.format("%016x", seq)} — hex, NOT decimal (§3). */
    static String segmentName(long seq) {
        return BASE_NAME + ".wal." + String.format("%016x", seq);
    }

    // ---------- enumeration and structural self-checks ----------

    /** The published namespace, decoded, ordered by sequence. Refuses anything but segments. */
    private static List<Seg> readNamespace(File dir, String ctx) throws IOException {
        File[] all = dir.listFiles();
        check(all != null, ctx + ": namespace directory vanished: " + dir);
        List<Seg> segs = new ArrayList<>();
        for (File f : all) {
            check(f.isFile(), ctx + ": unexpected non-file in the namespace: " + f);
            check(!f.getName().equals(BASE_NAME + ".lock"),
                    ctx + ": the lock sidecar must be removed before enumeration (§2.2)");
            check(f.getName().matches("x\\.wal\\.[0-9a-f]{16}"),
                    ctx + ": scratch or foreign file in the namespace: " + f.getName()
                            + " (§5.4 obligation 7)");
            segs.add(new Seg(f.getName(), Files.readAllBytes(f.toPath())));
        }
        segs.sort((p, q) -> Long.compare(p.seq, q.seq));
        for (Seg g : segs)
            check(g.relName.equals(segmentName(g.seq)),
                    ctx + ": " + g.relName + " does not match %016x of its own sequence " + g.seq);
        return segs;
    }

    /** §5.4 obligation 7, shared by both shapes: names, firstLsn equalities, LSN order. */
    private static void checkCommon(List<Seg> segs, String ctx) {
        check(segs.size() >= 2, ctx + ": " + segs.size() + " segment(s); both shapes need ≥2");
        long prevLsn = Long.MIN_VALUE;
        for (int i = 0; i < segs.size(); i++) {
            Seg g = segs.get(i);
            check(g.firstLsn == g.first().lsn, ctx + ": " + g.relName + " states firstLsn "
                    + g.firstLsn + ", its first section holds " + g.first().lsn);
            if (i > 0) check(g.seq == segs.get(i - 1).seq + 1,
                    ctx + ": sequences are not contiguous: " + segs.get(i - 1).seq + " then " + g.seq);
            for (Sec sec : g.sections) {
                check(sec.lsn > prevLsn, ctx + ": LSN " + sec.lsn + " at " + g.relName + ":"
                        + sec.off + " does not follow " + prevLsn);
                prevLsn = sec.lsn;
            }
        }
    }

    /** §5.2's generator self-check. */
    private static void checkTail(List<Seg> segs) {
        checkCommon(segs, TAIL_ID);
        check(segs.get(0).seq == 1, TAIL_ID + ": sequences must start at 1, not " + segs.get(0).seq);
        for (Seg g : segs)
            for (Sec sec : g.sections)
                check(sec.tag == 'S', TAIL_ID + ": " + g.relName + ":" + sec.off + " is tag '"
                        + sec.tag + "'; an uncleaned log carries only 'S'");
    }

    /**
     * §5.3's self-check and FIVE of §5.3.1's six witness rows, returned as the layout index
     * §5.3.1 asks the generator to publish.
     *
     * <p>Which five, stated precisely because "checks §5.3.1" would be a false claim. Rows 1, 2,
     * 3, 4 and 6 are checked here. **Row 5 is not** — it asks whether the middle segment's second
     * section holds an entry admitting a size-preserving {@code T_APPEND} rewrite, which means
     * decoding the entry stream and searching for a replacement encoding of exactly the same
     * length. That is `derive._stranded_append`'s job and reimplementing it here would be a
     * second entry codec to keep in step with the first. So the shaping transaction that exists
     * for row 5 ({@link #cleanedWorkload}'s {@code update(C, ...)} pair) has its necessity
     * established by `xcheck_bundles.py`, not by this class or by the java gate.
     *
     * <p>Rows 1-5 are also re-derived independently by {@code derive.check_witnesses} from the
     * same bytes, so the four checked in both places are checked by two codecs. Row 6 exists
     * ONLY here: {@code segmentBytes} is a generator setting and leaves no trace in the bytes.
     */
    private static Map<String, Long> checkCleaned(List<Seg> segs) {
        checkCommon(segs, CLEANED_ID);

        // exactly one valid mark, and K4
        Seg markSeg = null;
        for (Seg g : segs) {
            if (g.markIndex < 0) continue;
            check(markSeg == null, CLEANED_ID + ": two segments carry a 'K' mark");
            markSeg = g;
        }
        check(markSeg != null, CLEANED_ID + ": no 'K' mark; checkpoint() must force one");
        long through = markSeg.markThrough, logStart = markSeg.markLogStart;
        long markLsn = markSeg.sections.get(markSeg.markIndex).lsn;
        check(through > 0 && through < markSeg.seq, CLEANED_ID + ": K4 violated: cleanedThroughSeq "
                + through + " is not in (0, " + markSeg.seq + ")");
        check(logStart > 0 && logStart <= markLsn,
                CLEANED_ID + ": logStartLsn " + logStart + " is not in (0, " + markLsn + "]");

        List<Seg> retained = new ArrayList<>();
        for (Seg g : segs) if (g.seq > through) retained.add(g);
        // Cardinality BEFORE indexing: an image retaining nothing is refused for row 1,
        // which is what is wrong with it, and not with an IndexOutOfBoundsException
        // thrown on the way to saying so.
        check(retained.size() == 3, CLEANED_ID + ": §5.3.1 row 1 requires exactly three retained "
                + "segments; this bundle retains " + retained.size());
        check(retained.get(0).seq > 1,
                CLEANED_ID + ": the retained floor must be above segment 1 (§5.3)");
        Seg lowest = retained.get(0), middle = retained.get(1), active = retained.get(2);
        check(active.seq == segs.get(segs.size() - 1).seq,
                CLEANED_ID + ": the highest retained segment is not the highest segment");

        // §5.3: a 'C' image before the mark and an 'S' after it, both within the namespace
        check(markSeg == lowest, CLEANED_ID + ": the mark sits in segment " + markSeg.seq
                + "; §5.3.1 row 2 forbids it in the middle retained segment, so it must be in the "
                + "lowest one (" + lowest.seq + ") beside the 'C' image");
        boolean imageBefore = false;
        for (int i = 0; i < markSeg.markIndex; i++)
            if (markSeg.sections.get(i).tag == 'C') imageBefore = true;
        check(imageBefore, CLEANED_ID + ": no 'C' image precedes the mark");
        check(active.last().tag == 'S', CLEANED_ID + ": no 'S' section follows the mark");

        // row 4: the lowest retained segment's stated firstLsn IS the mark's floor, and the
        // namespace is dense — checkCommon proved ascent, this proves no gaps.
        check(lowest.firstLsn == logStart, CLEANED_ID + ": §5.3.1 row 4: the lowest retained "
                + "segment states firstLsn " + lowest.firstLsn + ", the mark attests " + logStart);
        long expect = logStart;
        for (Seg g : retained)
            for (Sec sec : g.sections) {
                check(sec.lsn == expect, CLEANED_ID + ": §5.3.1 row 4: LSNs are not dense across "
                        + "the retained set: expected " + expect + " at " + g.relName + ":"
                        + sec.off + ", found " + sec.lsn);
                expect++;
            }

        // row 2
        check(middle.sections.size() >= 2, CLEANED_ID + ": §5.3.1 row 2: the middle retained "
                + "segment carries " + middle.sections.size() + " section(s), fewer than two");
        for (int i = 0; i < 2; i++)
            check(middle.sections.get(i).tag != 'K', CLEANED_ID + ": §5.3.1 row 2: section " + i
                    + " of the middle retained segment is the mark; both must be entry-bearing");

        // row 3
        check(active.sections.size() == 1, CLEANED_ID + ": §5.3.1 row 3: the active segment carries "
                + active.sections.size() + " sections, not one");

        // row 6 — checkable HERE ONLY: segmentBytes is a generator setting, not a published byte.
        check(active.raw.length < SEGMENT_BYTES, CLEANED_ID + ": §5.3.1 row 6: the active segment "
                + "is " + active.raw.length + " bytes, not under segmentBytes " + SEGMENT_BYTES
                + "; Q8's appended record would force a rollover and there would be no section "
                + "to assert");

        return selectorIndex(segs, through);
    }

    /**
     * §5.3.1's layout index: every segment selector in {@code catalogue.SEGMENT_SELECTORS} that
     * resolves to EXACTLY ONE segment, and what it resolved to.
     *
     * <p>Exactly one is the whole point. A recipe addresses its target by selector and the
     * deriver refuses to pick between candidates, so a selector resolving to zero or to two makes
     * the cell that uses it unbuildable — and, worse, a selector that resolves to the wrong single
     * segment produces a cell labelled `reject` that is really an accept. So the index records
     * resolution as a SET: a selector missing here is a selector this bundle cannot host, which
     * is as much of a fact as the ones present, and the gate compares both directions.
     *
     * <p>Mirrors {@code derive._segment_candidates} deliberately and independently. That is the
     * cross-check: two implementations, one reading its own workload's output through its own
     * decoder and one reading the published bytes through {@code walfmt}, must name the same file
     * for each selector.
     */
    private static Map<String, Long> selectorIndex(List<Seg> segs, long through) {
        List<Seg> retained = new ArrayList<>();
        for (Seg g : segs) if (g.seq > through) retained.add(g);
        long highest = segs.get(segs.size() - 1).seq;

        Map<String, List<Long>> cand = new LinkedHashMap<>();
        cand.put("lowest_retained", retained.isEmpty()
                ? new ArrayList<>() : new ArrayList<>(List.of(retained.get(0).seq)));
        List<Long> mid = new ArrayList<>(), single = new ArrayList<>();
        for (int i = 1; i < retained.size(); i++) {
            Seg g = retained.get(i);
            if (g.seq != highest) mid.add(g.seq);
            if (g.sections.size() == 1) single.add(g.seq);
        }
        cand.put("middle_retained", mid);
        cand.put("single_section_retained", single);
        cand.put("highest", new ArrayList<>(List.of(highest)));
        List<Long> mark = new ArrayList<>();
        for (Seg g : segs) if (g.markIndex >= 0) mark.add(g.seq);
        cand.put("mark", mark);

        Map<String, Long> out = new LinkedHashMap<>();
        for (Map.Entry<String, List<Long>> e : cand.entrySet())
            if (e.getValue().size() == 1) out.put(e.getKey(), e.getValue().get(0));
        return out;
    }

    /**
     * Grades an arbitrary namespace directory against §5.3's self-check and the FIVE §5.3.1 rows
     * {@link #checkCleaned} can decide — 1, 2, 3, 4 and 6, never 5 — exactly as the generator
     * grades its own output. Throws {@link AssertionError} naming the first row that fails.
     *
     * <p>"Six" is what this javadoc said until the C2j review, and it was the same false claim
     * {@code checkCleaned}'s had already been corrected for: a fix applied to one of two comments
     * describing the same method is a fix that did not happen.
     *
     * <p>Exists so a candidate workload can be FALSIFIED rather than only confirmed: the test
     * runs the shape probe's rejected variants through this and requires each to be refused for
     * the reason claimed. Without it, a generator whose shaping is unnecessary and a generator
     * whose shaping is essential are indistinguishable.
     */
    public static Map<String, Long> gradeCleaned(File dir) throws IOException {
        Files.deleteIfExists(new File(dir, BASE_NAME + ".lock").toPath());
        return checkCleaned(readNamespace(dir, CLEANED_ID));
    }

    /**
     * A one-line structural summary of any namespace: where the mark sits, and how the retained
     * set is shaped around it.
     *
     * <p>The companion to {@link #gradeCleaned}. Grading reports the FIRST row a candidate
     * workload violates, which is not always the row that workload is interesting for — §5.3's
     * literal one loses on row 1 (two retained segments) long before anything looks at row 2,
     * even though row 2 is the reason it can never be fixed by adding a segment. This reports the
     * position itself, so a falsification test can name the actual defect.
     */
    public static String describeShape(File dir) throws IOException {
        Files.deleteIfExists(new File(dir, BASE_NAME + ".lock").toPath());
        List<Seg> segs = readNamespace(dir, "describeShape");
        long through = 0;
        String mark = "none";
        for (Seg g : segs)
            if (g.markIndex >= 0) {
                through = g.markThrough;
                mark = g.seq + ":" + g.markIndex;
            }
        List<Long> retained = new ArrayList<>();
        for (Seg g : segs) if (g.seq > through) retained.add(g.seq);
        Seg active = segs.get(segs.size() - 1);
        return "mark=" + mark + " retained=" + retained
                + " activeSections=" + active.sections.size();
    }

    // ---------- emission ----------

    /** relName -> bytes, in sequence order: the map §5.4 obligation 8 compares across runs. */
    private static Map<String, byte[]> imageOf(List<Seg> segs) {
        Map<String, byte[]> out = new LinkedHashMap<>();
        for (Seg g : segs) out.put(g.relName, g.raw);
        return out;
    }

    private static String describe(Map<String, byte[]> image) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, byte[]> e : image.entrySet())
            sb.append(e.getKey()).append('\t').append(e.getValue().length)
              .append('\t').append(sha256Hex(e.getValue())).append('\n');
        return sb.toString();
    }

    /** One shape's finished product: its published bytes, its recids and its layout index. */
    private static final class Bundle {
        final String id;
        final Map<String, byte[]> image;
        final Recids recids;
        final Map<String, Long> layout;

        Bundle(String id, Map<String, byte[]> image, Recids recids, Map<String, Long> layout) {
            this.id = id; this.image = image; this.recids = recids; this.layout = layout;
        }
    }

    private interface Workload {
        void run(File base, Recids r);
    }

    /**
     * Runs one shape into a scratch directory, self-checks it, and returns it.
     *
     * <p>§5.4 obligation 1: the base is EMPTY, because the directory is created here and wiped
     * first. Obligation 5: the store is closed by the workload's own {@code finally} before
     * anything reads the files — a snapshot of an open store is not the published image.
     */
    private static Bundle produce(File scratch, String id, Workload w, boolean cleaned)
            throws IOException {
        wipe(scratch);
        Files.createDirectories(scratch.toPath());
        Recids r = new Recids();
        w.run(new File(scratch, BASE_NAME), r);
        Files.deleteIfExists(new File(scratch, BASE_NAME + ".lock").toPath());
        List<Seg> segs = readNamespace(scratch, id);
        Map<String, Long> layout;
        if (cleaned) {
            layout = checkCleaned(segs);
        } else {
            checkTail(segs);
            layout = selectorIndex(segs, 0);   // no mark, so nothing is superseded
        }
        // reopen: the same reader contract every accept cell will run, then prove the reopen
        // published nothing new — §5.5 says no accept-bundle cell mutates, and all 12 of this
        // bundle's accept cells rest on that.
        Map<String, byte[]> image = imageOf(segs);
        StoreWAL s = open(new File(scratch, BASE_NAME));
        try {
            assertFinalState(s, r, id + " reopen", cleaned ? CLEANED_BASE : TAIL_BASE);
        } finally {
            s.close();
        }
        Files.deleteIfExists(new File(scratch, BASE_NAME + ".lock").toPath());
        check(describe(imageOf(readNamespace(scratch, id))).equals(describe(image)),
                id + ": a clean reopen changed the published bytes, so §5.5's "
                        + "\"no ACCEPT bundle cell mutates\" is false for this bundle");
        return new Bundle(id, image, r, layout);
    }

    /**
     * Produces one shape TWICE into separate scratch directories and refuses to publish unless
     * the complete relName&rarr;bytes maps agree (§5.4 obligation 8).
     *
     * <p>This is necessary and not sufficient, and the difference matters: two runs in ONE process
     * share every JVM-wide seed, so this catches a workload that reads a clock or a directory
     * listing and would NOT catch one that depends on an identity hash code. The sync script's
     * separate obligation to invoke each generator twice, in two processes, is what covers that;
     * this check is the cheap half that fails at the generator rather than three slices later.
     */
    private static Bundle produceTwice(File root, String id, Workload w, boolean cleaned)
            throws IOException {
        Bundle b1 = produce(new File(root, ".run1-" + id), id, w, cleaned);
        Bundle b2 = produce(new File(root, ".run2-" + id), id, w, cleaned);
        String d1 = describe(b1.image), d2 = describe(b2.image);
        check(d1.equals(d2), id + " is NOT deterministic across two runs:\n" + d1 + "--\n" + d2);
        check(b1.layout.equals(b2.layout), id + ": the layout index differs across two runs: "
                + b1.layout + " vs " + b2.layout);
        return b1;
    }

    private static void publish(File out, Bundle b) throws IOException {
        File dest = new File(out, b.id);
        wipe(dest);
        Files.createDirectories(dest.toPath());
        for (Map.Entry<String, byte[]> e : b.image.entrySet())
            Files.write(new File(dest, e.getKey()).toPath(), e.getValue());
    }

    // ---------- fragment.tsv and layout.tsv ----------

    private static String fragment(Bundle tail, Bundle cleaned) {
        String commit = FixtureWriter.gitHeadOrUnknown();
        StringBuilder sb = new StringBuilder();
        sb.append("# xfixtures fragment written by org.mapdb.xfixtures.Wal3FixtureWriter (C2j).\n");
        sb.append("# The sync script merges fragments, appends the gzSha256 column to file rows\n");
        sb.append("# and adds the expect/post rows from catalogue.py.\n");
        for (Bundle b : new Bundle[]{tail, cleaned}) {
            sb.append("fixture\t").append(b.id).append("\twal3-namespace\tjava\t").append(commit).append('\n');
            // §2: file rows sorted numerically by segment sequence — imageOf preserves that order.
            for (Map.Entry<String, byte[]> e : b.image.entrySet())
                sb.append("file\t").append(b.id).append('\t').append(e.getKey()).append('\t')
                  .append(e.getValue().length).append('\t').append(sha256Hex(e.getValue())).append('\n');
            int base = b == tail ? TAIL_BASE : CLEANED_BASE;
            for (FixtureWriter.RecidExpect x : expects(b.recids, base))
                sb.append("recid\t").append(b.id).append('\t').append(x.label).append('\t')
                  .append(x.recid).append('\t').append(x.state).append('\t')
                  .append(x.payloadId).append('\t').append(x.len).append('\n');
        }
        return sb.toString();
    }

    /**
     * §5.3.1's layout index: each witness named by the segment sequence it resolved to, as
     * {@code symbol} rows in §10.1's shape.
     *
     * <p>What makes it more than decoration: these values come from THIS generator's own decoder
     * and its own knowledge of the workload, and {@code derive.resolve_symbols} resolves the same
     * names independently from the published bytes. The gate compares them. A row nothing reads
     * is a claim nothing checked (§10.1), so this file is written to be read.
     */
    private static String layout(Bundle tail, Bundle cleaned) {
        StringBuilder sb = new StringBuilder();
        sb.append("# §5.3.1 layout index written by org.mapdb.xfixtures.Wal3FixtureWriter (C2j).\n");
        sb.append("# symbol <fixtureId> <@segmentSelector> <relName>, one row per selector that\n");
        sb.append("# resolves to exactly one segment. An ABSENT selector is a claim too: this\n");
        sb.append("# bundle cannot host a recipe that addresses it. Cross-checked against\n");
        sb.append("# derive._segment_candidates, both directions, by xcheck_bundles.py.\n");
        for (Bundle b : new Bundle[]{tail, cleaned})
            for (Map.Entry<String, Long> e : b.layout.entrySet())
                sb.append("symbol\t").append(b.id).append("\t@").append(e.getKey()).append('\t')
                  .append(segmentName(e.getValue())).append('\n');
        return sb.toString();
    }

    // ---------- CLI ----------

    private static void wipe(File dir) throws IOException {
        if (!dir.exists()) return;
        File[] kids = dir.listFiles();
        if (kids != null) for (File f : kids) wipe(f);
        Files.delete(dir.toPath());
    }

    public static void main(String[] args) throws IOException {
        File out = null;
        boolean force = false, quiet = false;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--out":
                    if (++i == args.length) usage("--out needs a directory argument");
                    out = new File(args[i]);
                    break;
                case "--force":
                    force = true;
                    break;
                case "--quiet":     // the gate invokes this generator five times per run
                    quiet = true;
                    break;
                default:
                    usage("unknown argument: " + args[i]);
            }
        }
        if (out == null) usage("--out is required");
        if (out.exists() && !out.isDirectory()) usage("--out is not a directory: " + out);
        String[] existing = out.list();
        if (existing != null && existing.length > 0 && !force)
            usage("output directory not empty (use --force to overwrite): " + out);
        Files.createDirectories(out.toPath());

        Bundle tail = produceTwice(out, TAIL_ID, (base, r) -> tailWorkload(base, r), false);
        Bundle cleaned = produceTwice(out, CLEANED_ID, (base, r) -> cleanedWorkload(base, r), true);
        for (String id : new String[]{TAIL_ID, CLEANED_ID}) {
            wipe(new File(out, ".run1-" + id));
            wipe(new File(out, ".run2-" + id));
        }
        publish(out, tail);
        publish(out, cleaned);
        String frag = fragment(tail, cleaned);
        Files.write(new File(out, "fragment.tsv").toPath(), frag.getBytes(StandardCharsets.UTF_8));
        Files.write(new File(out, "layout.tsv").toPath(),
                layout(tail, cleaned).getBytes(StandardCharsets.UTF_8));

        // §5.4 obligation 7 applied to the OUTPUT directory, not just to the scratch namespaces.
        // A `--force` rerun over a directory holding anything else republishes the two bundles
        // and leaves the rest in place, and the sync script would then pick up whatever was
        // there. Checked rather than deleted: `--out` is a path the caller chose, and silently
        // emptying it is a bigger risk than refusing to publish into it.
        List<String> stray = new ArrayList<>(Arrays.asList(out.list()));
        stray.removeAll(Arrays.asList(TAIL_ID, CLEANED_ID, "fragment.tsv", "layout.tsv"));
        check(stray.isEmpty(), "the output directory also holds " + stray + "; a generator's "
                + "output directory must contain exactly the two bundles and the two sidecars, "
                + "because everything in it is what the sync script will consume");
        if (!quiet) for (Bundle b : new Bundle[]{tail, cleaned}) {
            System.out.println("wrote " + b.id + "/ (" + b.image.size() + " segments):");
            System.out.print(describe(b.image).replaceAll("(?m)^", "  "));
            System.out.println("  layout " + b.layout);
        }
    }

    private static void usage(String problem) {
        System.err.println(problem);
        System.err.println("usage: java -ea -cp target/test-classes:target/classes "
                + Wal3FixtureWriter.class.getName() + " --out <dir> [--force] [--quiet]");
        System.exit(2);
        throw new IllegalStateException("unreachable");
    }
}
