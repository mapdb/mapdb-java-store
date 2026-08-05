package org.mapdb.xfixtures;

import org.mapdb.store.Wal3Decode;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;
import java.util.zip.GZIPInputStream;

import static org.mapdb.xfixtures.FixtureWriter.check;

/**
 * Writes {@code GOLDEN-BODY.tsv}: the DECODED BODY of every section of the schema-v2 sample
 * bundles, as this engine reads it.
 *
 * <p><b>Why Java authors this file and python does not.</b> {@code GOLDEN-DECODE.tsv} pins
 * framing, and says so in its own header: {@code walfmt.py} is a structural codec, and
 * reimplementing store record semantics there would be a fifth implementation nobody reviews. So
 * the contract (§11.2) settles body agreement engine-against-engine with the frozen Java reader
 * as the authority — which is what this is. The rust and zig readers of C3r/C3z decode the same
 * bytes and compare against this file; Java is authoritative by construction, because it wrote it.
 *
 * <p><b>{@code lenPlus} is emitted RAW, and that is the point of the whole file.</b>
 * {@code T_RECORD} encodes {@code packLong(data == null ? 0 : data.length + 1)}, so
 * {@code lenPlus == 0} is NULL content and {@code lenPlus == 1} is zero-length content. A dump
 * that emitted a decoded length would collapse the two into {@code 0} and pin the collapse — and
 * two readers that both collapsed it would agree forever. The {@code contentSha256} column is
 * {@code -} for NULL and the sha of the empty string for zero-length, so the two cases differ in
 * BOTH columns and no single-column bug can hide one as the other. The sample carries one of
 * each (recid 12 and recid 11), which is why C3s regenerated the corpus before this file existed:
 * a distinction no fixture contains cannot be checked however the dump is shaped.
 *
 * <p><b>It checks its own decode rather than trusting it.</b> Every record's content must be
 * {@code payload(id, len)} for the id recovered from its own first byte — the payload function is
 * invertible, so a mis-parsed entry stream produces bytes that do not fit it — and the set of
 * recids the entries mention must equal the set the manifest names. Without those, a decoder that
 * read the packed-long continuation bit the wrong way round would emit a self-consistent file
 * full of nonsense, and the first thing to notice would be C3r failing for the wrong reason.
 *
 * <pre>
 *   mvn -q -o test-compile
 *   java -ea -cp target/test-classes:target/classes \
 *       org.mapdb.xfixtures.Wal3BodyDump &lt;out.tsv&gt;
 * </pre>
 *
 * The emitted file is checked in twice — {@code todo/store-cross/sample-v2/} is the source of
 * truth, {@code src/test/resources/xfixtures-v2/} the distributed copy — and
 * {@code XFixtureConformanceTest.sample_v2_body_matches_golden_body} re-derives it and compares,
 * so the emitter is gated by the same rule as C3s's python generators: a generator that is only
 * ever run to produce the file it is then validated against agrees with itself about anything.
 */
public final class Wal3BodyDump {

    private Wal3BodyDump() {}

    static final String RESOURCE_ROOT = "/xfixtures-v2/";
    static final String FILE_NAME = "GOLDEN-BODY.tsv";

    private static final String HEADER = String.join("\n",
            "# The DECODED BODIES of every pinned schema-v2 sample section, as the FROZEN JAVA",
            "# READER reads them — contract §11.2's engine-against-engine half.",
            "#",
            "#   sec  <bundle> <relName> <index> <tag> <entryCount>",
            "#   ent  <bundle> <relName> <index> <ord> <kind> <recid> <cap> <lenPlus> <contentSha256>",
            "#   mark <bundle> <relName> <index> <cleanedThroughSeq> <logStartLsn>",
            "#",
            "# GOLDEN-DECODE.tsv pins FRAMING and deliberately stops there: walfmt.py is a",
            "# structural codec, and store record semantics written in python would be a fifth",
            "# implementation no one reviews. This file is the other half, and Java authors it",
            "# because Java is the reference for what a body MEANS.",
            "#",
            "# lenPlus IS RAW, NOT A LENGTH. `lenPlus == 0` is NULL content; `lenPlus == 1` is",
            "# ZERO-LENGTH content (StoreWAL.applySection). A reader that decodes lenPlus into a",
            "# length collapses the two, and two readers that both collapse it agree forever.",
            "# contentSha256 is `-` for NULL and the empty-string sha for zero-length, so the two",
            "# differ in both columns. The sample contains one of each: recid 12 and recid 11.",
            "#",
            "# `-` means the column does not apply to that entry kind. cap is emitted because a",
            "# reader must decode it to find the next entry at all; leaving it out would be a",
            "# field the comparison never reaches.",
            "#",
            "# Regenerate with mapdb-java-store's org.mapdb.xfixtures.Wal3BodyDump; the java suite",
            "# re-derives it and fails on drift.",
            "");

    /** One row of the dump, kept whole so the file's sort order is a property of the text. */
    private static void row(StringBuilder sb, Object... cells) {
        for (int i = 0; i < cells.length; i++) {
            if (i > 0) sb.append('\t');
            sb.append(cells[i]);
        }
        sb.append('\n');
    }

    /**
     * Renders the whole file from the classpath sample.
     *
     * <p>Deliberately reads the same resources the conformance suite runs, through the same
     * manifest reader and the same SHA-256 gate: a dump taken from bytes that were never checked
     * against their pins would describe whatever happened to be on disk.
     */
    public static String render() throws IOException {
        XFixtureManifest.Loaded loaded = XFixtureManifest.load(RESOURCE_ROOT);
        check(loaded.version == 2, "the sample is schema v" + loaded.version + ", not v2");
        XFixtureManifest.V2 m = loaded.v2;

        List<XFixtureManifest.V2.FileRow> files = new ArrayList<>(m.files);
        files.sort(Comparator.comparing((XFixtureManifest.V2.FileRow f) -> f.fixtureId)
                .thenComparing(f -> f.relName));

        StringBuilder sb = new StringBuilder(HEADER);
        String bundle = null;
        TreeSet<Long> seenRecids = new TreeSet<>();
        for (XFixtureManifest.V2.FileRow f : files) {
            if (!f.fixtureId.equals(bundle)) {
                if (bundle != null) checkRecidsAgainstManifest(m, bundle, seenRecids);
                bundle = f.fixtureId;
                seenRecids = new TreeSet<>();
            }
            String where = f.fixtureId + "/" + f.relName;
            Wal3Decode.Segment seg = Wal3Decode.decode(gunzip(f), where);
            check(seg.trailing == 0, where + ": " + seg.trailing + " bytes follow the last section");
            for (Wal3Decode.Section s : seg.sections) {
                if (s.tag == 'K') {
                    long[] mark = Wal3Decode.mark(s, where);
                    checkMark(mark, seg.header.seq, s.lsn, where + " section " + s.index);
                    row(sb, "sec", f.fixtureId, f.relName, s.index, s.tag, "-");
                    row(sb, "mark", f.fixtureId, f.relName, s.index, mark[0], mark[1]);
                    continue;
                }
                List<Wal3Decode.Entry> entries = Wal3Decode.entries(s, where);
                row(sb, "sec", f.fixtureId, f.relName, s.index, s.tag, entries.size());
                for (int i = 0; i < entries.size(); i++) {
                    Wal3Decode.Entry e = entries.get(i);
                    seenRecids.add(e.recid);
                    row(sb, "ent", f.fixtureId, f.relName, s.index, i, e.kind(), e.recid,
                            e.cap < 0 ? "-" : e.cap,
                            e.lenPlus < 0 ? "-" : e.lenPlus,
                            contentSha(e, where + " section " + s.index + " entry " + i));
                }
            }
        }
        check(bundle != null, "the sample has no file rows");
        checkRecidsAgainstManifest(m, bundle, seenRecids);
        return sb.toString();
    }

    /**
     * The content column, and the decode's own self-check.
     *
     * <p>{@code payload(id, len)[i] == (i * 131 + id) & 0xff} is invertible from its first byte, so
     * rebuilding it from the recovered id and comparing is a total check that these bytes really
     * are a payload this corpus issued — which they cannot be if the entry stream was framed
     * wrongly. A decoder that read the packed-long continuation bit the wrong way round lands
     * mid-payload and fails here, rather than emitting a plausible file for the ports to chase.
     */
    private static String contentSha(Wal3Decode.Entry e, String where) {
        if (!e.isRecord() || e.lenPlus == 0) {
            check(e.content == null, where + ": non-record or null entry carries content");
            if (e.isRecord()) check(e.cap == 0, where + ": a NULL record's cap must be 0, not " + e.cap);
            return "-";
        }
        byte[] c = e.content;
        check(c.length == e.lenPlus - 1,
                where + ": content is " + c.length + " bytes but lenPlus says " + (e.lenPlus - 1));
        checkCap(e.cap, c.length, where);
        if (c.length > 0) {
            int id = c[0] & 0xFF;
            check(Arrays.equals(c, FixtureWriter.payload(id, c.length)),
                    where + ": the " + c.length + " content bytes are not payload(" + id + ", "
                            + c.length + ") — this entry stream was not framed the way the writer "
                            + "wrote it");
        }
        return FixtureWriter.sha256Hex(c);
    }

    /**
     * The independent witness for the emitted {@code cap} column.
     *
     * <p>Nothing else in the slice observes {@code cap}: the real engine's replay consumes it and
     * exposes only the resulting record, and the golden comparison grades the column against a
     * file this same code wrote. So an emitter that consumed the varint correctly and then printed
     * a fabricated number would go unnoticed — the C3j review named exactly that hole. This is
     * {@code StoreWAL.capValid}'s rule restated over what the dump can see: a plain capacity is
     * 16-aligned and leaves room for the 4-byte header, and 0 means "oversize, stored linked".
     */
    private static void checkCap(long cap, int len, String where) {
        if (cap == 0) return;                    // linked/oversize content; the size rule is the engine's
        check(cap >= 4L + len && (cap & 15) == 0,
                where + ": cap " + cap + " is not a valid capacity for " + len + " content bytes "
                        + "(must be 16-aligned and at least " + (4L + len) + ")");
    }

    /**
     * The independent witness for the two {@code 'K'} mark longs, which are otherwise
     * indistinguishable once decoded.
     *
     * <p>These are {@code StoreWAL}'s own S8/K4 rules. They matter here because the fields are
     * both longs in one 16-byte body: a decoder that returned them in the other order would emit a
     * self-consistent file, and the C3j review measured that nothing else in the slice notices.
     * The sample's mark is {@code (cleanedThroughSeq=2, logStartLsn=9)} in a segment of sequence 4,
     * so a swap makes {@code through} 9 and trips K4 immediately.
     */
    private static void checkMark(long[] mark, long segSeq, long lsn, String where) {
        check(mark[0] > 0, where + ": cleanedThroughSeq is " + mark[0]);
        check(mark[0] < segSeq, where + ": a mark in segment " + segSeq + " authorizes removing "
                + "segment " + mark[0] + ", including itself (K4)");
        check(mark[1] > 0 && mark[1] <= lsn, where + ": logStartLsn " + mark[1]
                + " is not an LSN at or below the mark's own " + lsn + " (S8)");
    }

    /**
     * Cross-checks the recids the entry stream mentions against the ones the manifest names.
     *
     * <p>The manifest's rows were folded from these same bytes by an independent (python) reader,
     * so this is a real second opinion on the recid decode and not a restatement: the two agree
     * only if both unpacked the same varints.
     *
     * <p><b>The relation is ONE-WAY, and that is not laxness.</b> Plan §5 forbids asserting that a
     * log contains only the recids the manifest names — §5.2's rolled-back put need only be
     * invisible through the API, and {@code wal3-java-tail} already carries recids beyond the ones
     * §5.2 describes. So the check is that every recid the manifest names is WITNESSED in the
     * decoded history, never the reverse. An earlier revision used set equality, which quietly
     * asserted the forbidden direction; it passed only because these three bundles happen to have
     * equal sets, so it was a §5 violation waiting for the first legal fixture to break it.
     */
    private static void checkRecidsAgainstManifest(XFixtureManifest.V2 m, String fixtureId,
                                                   TreeSet<Long> seen) {
        List<FixtureWriter.RecidExpect> rows = m.recids.get(fixtureId);
        check(rows != null && !rows.isEmpty(), fixtureId + ": no recid rows to cross-check against");
        TreeSet<Long> want = new TreeSet<>();
        for (FixtureWriter.RecidExpect r : rows) want.add(r.recid);
        TreeSet<Long> missing = new TreeSet<>(want);
        missing.removeAll(seen);
        check(missing.isEmpty(), fixtureId + ": the manifest names recids " + missing
                + " that the decoded entry stream never mentions");
    }

    private static byte[] gunzip(XFixtureManifest.V2.FileRow f) throws IOException {
        byte[] gz;
        try (InputStream in = Wal3BodyDump.class.getResourceAsStream(RESOURCE_ROOT + f.blobName())) {
            check(in != null, "fixture resource missing: " + RESOURCE_ROOT + f.blobName());
            gz = in.readAllBytes();
        }
        check(f.gzSha.equals(FixtureWriter.sha256Hex(gz)), f.blobName() + ": compressed SHA-256 mismatch");
        ByteArrayOutputStream raw = new ByteArrayOutputStream();
        try (GZIPInputStream gin = new GZIPInputStream(new ByteArrayInputStream(gz))) {
            gin.transferTo(raw);
        }
        byte[] bytes = raw.toByteArray();
        check(bytes.length == f.rawLen, f.relName + ": uncompressed length mismatch");
        check(f.rawSha.equals(FixtureWriter.sha256Hex(bytes)), f.relName + ": uncompressed SHA-256 mismatch");
        return bytes;
    }

    public static void main(String[] args) throws Exception {
        check(args.length == 1, "usage: Wal3BodyDump <out.tsv>");
        File out = new File(args[0]);
        Files.write(out.toPath(), render().getBytes(StandardCharsets.UTF_8));
        System.out.println("wrote " + out + " (" + out.length() + " bytes)");
    }
}
