package org.mapdb.xfixtures;

import org.junit.After;
import org.junit.Test;
import org.mapdb.DBException;
import org.mapdb.TmpFiles;
import org.mapdb.store.Store;
import org.mapdb.store.StoreDirect;
import org.mapdb.store.StoreWAL;
import org.mapdb.store.Wal3Decode;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.zip.GZIPInputStream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Cross-port conformance harness: runs every {@code engine == java} cell of the checked-in
 * fixture manifests against this engine.
 *
 * <p>The fixtures pin the CURRENT state of an UNSTABLE format so that silent divergence between
 * the store engines is detected. Cross-engine openability today is an implementation fact, not a
 * supported feature; any format change regenerates the fixtures as part of that change (see
 * {@link FixtureWriter} and the sync script in the planning repo).
 *
 * <p><b>Two manifests, one reader (Stage C, C3).</b> {@code /xfixtures/} holds the live schema-v1
 * fixture tree the suite has always run. {@code /xfixtures-v2/} holds a static schema-v2 sample
 * over the wal3 golden bundles. Both go through {@link XFixtureManifest#load}, which dispatches on
 * the {@code version} row alone. They are separate resource roots on purpose: C6 flips the live
 * tree to v2 as a <em>data</em> commit, and that is only safe if this reader already accepts both
 * schemas, proven by both being exercised at once. {@link #manifest_dispatches_on_the_version_row}
 * pins the dispatch itself, because the two schemas' {@code expect} rows have the same arity and
 * different columns.
 *
 * <p><b>Schema-v1 flow.</b> Gunzip every fixture file once into a session temp dir verifying
 * length and SHA-256, then run each cell in a fresh private directory over its own working copy:
 * accept cells get {@code verify()} + the full per-recid reader contract + exact
 * {@code getAllRecids}; reject cells must fail to open with {@link DBException.DataCorruption} —
 * via {@link StoreDirect} for {@code direct} rows, via {@link StoreWAL} on the BASE path for
 * {@code wal} rows (the fixture placed at {@code <base>.wal} hits the N6 no-migration boundary).
 * Either way the working copy must stay byte-identical and nothing beyond {@code .lock} sidecars
 * may appear.
 *
 * <p><b>Schema-v2 flow.</b> A fixture is a whole namespace — several file rows, blobs named
 * {@code <fixtureId>.<relName>.gz} — and a cell names a {@code mode} ({@code rw} or {@code ro}) as
 * well as a verdict. Post-open state is asserted from the manifest's own {@code post} rows rather
 * than from a blanket rule: a file a post row names must match that disposition, and every other
 * file must be byte-unchanged (the D6 post-cardinality amendment). On top of the cells,
 * {@link #sample_v2_framing_matches_golden_decode} compares this engine's decode of every sample
 * segment against {@code GOLDEN-DECODE.tsv}, which pins what the bytes MEAN where the manifest's
 * SHA-256 columns pin only which bytes were read.
 *
 * <p>All temp state lives in {@link TmpFiles}-owned directories so nothing leaks into
 * {@code java.io.tmpdir}.
 */
public class XFixtureConformanceTest {

    private static final String V1_ROOT = "/xfixtures/";
    private static final String V2_ROOT = "/xfixtures-v2/";

    private final List<File> dirs = new ArrayList<>();

    @After public void cleanup() {
        for (File d : dirs) TmpFiles.delete(d);
        dirs.clear();
    }

    private File tempDir(String prefix) throws IOException {
        File d = TmpFiles.tempDir(prefix);
        dirs.add(d);
        return d;
    }

    // ---------- shared resource plumbing ----------

    private static byte[] resource(String path) throws IOException {
        try (InputStream in = XFixtureConformanceTest.class.getResourceAsStream(path)) {
            if (in == null) fail("classpath resource " + path + " is missing");
            return in.readAllBytes();
        }
    }

    /** Gunzips a blob and checks all three of its pinned identities before it is ever opened. */
    private static byte[] gunzipChecked(String root, String blobName, String relName,
                                        long rawLen, String rawSha, String gzSha) throws IOException {
        byte[] gz = resource(root + blobName);
        assertEquals(blobName + ": compressed SHA-256 mismatch", gzSha, FixtureWriter.sha256Hex(gz));
        ByteArrayOutputStream raw = new ByteArrayOutputStream();
        try (GZIPInputStream gin = new GZIPInputStream(new ByteArrayInputStream(gz))) {
            gin.transferTo(raw);
        }
        byte[] bytes = raw.toByteArray();
        assertEquals(relName + ": uncompressed length mismatch", rawLen, bytes.length);
        assertEquals(relName + ": uncompressed SHA-256 mismatch", rawSha, FixtureWriter.sha256Hex(bytes));
        return bytes;
    }

    private static TreeSet<String> listNames(File dir) {
        String[] names = dir.list();
        assertTrue("cell dir vanished: " + dir, names != null);
        return new TreeSet<>(List.of(names));
    }

    // ---------- schema v1 ----------

    /** Gunzips every v1 fixture file once, verifying gz and raw SHA-256 plus raw length up front. */
    private static Map<String, File> gunzipAllV1(XFixtureManifest.V1 m, File session) throws IOException {
        Map<String, File> pristine = new HashMap<>();
        for (XFixtureManifest.V1.FileRow f : m.files) {
            byte[] bytes = gunzipChecked(V1_ROOT, f.relName + ".gz", f.relName, f.rawLen, f.rawSha, f.gzSha);
            File out = new File(session, f.relName);
            Files.write(out.toPath(), bytes);
            pristine.put(f.relName, out);
        }
        return pristine;
    }

    private void runV1Cell(XFixtureManifest.V1 m, XFixtureManifest.V1.Expect e, File pristine)
            throws IOException {
        String ctx = "v1 cell[" + e.fixtureId + " java " + e.verdict + " " + e.opener + "]";
        // Schema v1 knows exactly two openers; anything else is a manifest error, not a skip.
        assertTrue(ctx + ": unsupported opener " + e.opener,
                "direct".equals(e.opener) || "wal".equals(e.opener));

        File cell = tempDir("xfcell");
        File work = new File(cell, e.placeAs);
        Files.copy(pristine.toPath(), work.toPath());
        byte[] before = Files.readAllBytes(work.toPath());
        TreeSet<String> namesBefore = listNames(cell);

        // direct rows carry the file path in openArg; java wal rows carry the store BASE path
        // (the fixture sits at placeAs = <base>.wal, where the N6 no-migration boundary sees it).
        File openTarget = new File(cell, e.openArg);
        switch (e.verdict) {
            case "accept": {
                // Schema v1 has no java accept-wal rows (the W fixtures are port-format v1).
                assertEquals(ctx + ": unsupported accept opener", "direct", e.opener);
                StoreDirect s = new StoreDirect(openTarget);
                try {
                    List<FixtureWriter.RecidExpect> expects = m.recids.get(e.fixtureId);
                    assertTrue(ctx + ": accept fixture has no recid rows", expects != null && !expects.isEmpty());
                    FixtureWriter.assertReaderContract(s, expects, ctx);
                } finally {
                    s.close();
                }
                break;
            }
            case "reject":
                // Schema v1 has no mode column; every v1 cell is a writable open.
                assertRejected(ctx, e.opener, "rw", openTarget);
                break;
            default:
                fail(ctx + ": unknown verdict " + e.verdict);
        }

        // the working copy must be byte-identical after the cell, whatever the verdict
        assertArrayEquals(ctx + ": working copy bytes changed", before, Files.readAllBytes(work.toPath()));
        // .lock sidecars are allowed to appear; anything else new is a failure
        for (String name : listNames(cell)) {
            if (namesBefore.contains(name)) continue;
            assertTrue(ctx + ": unexpected new file " + name, name.endsWith(".lock"));
        }
        TmpFiles.delete(cell);
        dirs.remove(cell);
    }

    /**
     * A reject cell must fail with the engine's corruption class. For {@code wal} rows
     * {@code WalSegmentSet}'s N6 check throws on the bare {@code <base>.wal} file BEFORE any
     * namespace mutation (only the {@code .lock} sidecar precedes it), so the byte-unchanged and
     * no-new-files assertions still hold for that arm.
     */
    private static void assertRejected(String ctx, String opener, String mode, File openTarget) {
        try {
            Store s;
            if ("direct".equals(opener)) {
                assertEquals(ctx + ": the direct opener has no read-only mode here", "rw", mode);
                s = new StoreDirect(openTarget);
            } else {
                // The mode is honoured, because `ro` is a different code path: `openReadOnly` runs
                // recovery without acting on it, so a bundle it accepts and a writable open refuses
                // (or the reverse) is exactly the divergence a reject cell would exist to pin.
                s = "ro".equals(mode) ? StoreWAL.openReadOnly(openTarget) : new StoreWAL(openTarget);
            }
            s.close();
            fail(ctx + ": expected DBException.DataCorruption, but the store opened");
        } catch (DBException.DataCorruption expected) {
            // the engine's corruption class, per the contract
        }
    }

    @Test public void manifest_cells_conform() throws Exception {
        XFixtureManifest.Loaded loaded = XFixtureManifest.load(V1_ROOT);
        assertEquals("the live fixture tree is not schema v1 — if C6 flipped it, this test moves "
                + "to the v2 executor rather than being relaxed", 1, loaded.version);
        XFixtureManifest.V1 m = loaded.v1;
        File session = tempDir("xfixtures-session");
        Map<String, File> pristine = gunzipAllV1(m, session);
        int ran = 0;
        for (XFixtureManifest.V1.Expect e : m.expects) {
            if (!"java".equals(e.engine)) continue;
            runV1Cell(m, e, pristine.get(m.fileOf(e.fixtureId).relName));
            ran++;
        }
        assertTrue("manifest contains no java expect rows", ran > 0);
    }

    // ---------- schema v2 ----------

    /**
     * Gunzips every v2 fixture file once into {@code <session>/<fixtureId>/<relName>}.
     *
     * <p>The per-fixture subdirectory is load-bearing, not tidiness: all three sample bundles name
     * their segments {@code x.wal.0000000000000001} and up, so a flat map keyed by {@code relName}
     * would have them overwrite each other and every cell would then run against whichever bundle
     * happened to be gunzipped last.
     */
    private static Map<String, File> gunzipAllV2(XFixtureManifest.V2 m, File session) throws IOException {
        Map<String, File> pristine = new HashMap<>();
        for (XFixtureManifest.V2.FileRow f : m.files) {
            byte[] bytes = gunzipChecked(V2_ROOT, f.blobName(), f.relName, f.rawLen, f.rawSha, f.gzSha);
            File dir = new File(session, f.fixtureId);
            assertTrue("cannot create " + dir, dir.isDirectory() || dir.mkdirs());
            File out = new File(dir, f.relName);
            Files.write(out.toPath(), bytes);
            pristine.put(f.fixtureId + "/" + f.relName, out);
        }
        return pristine;
    }

    private void runV2Cell(XFixtureManifest.V2 m, XFixtureManifest.V2.Expect e,
                           Map<String, File> pristine) throws IOException {
        String ctx = "v2 cell[" + e.fixtureId + " java " + e.mode + " " + e.verdict + " " + e.opener + "]";
        assertEquals(ctx + ": this reader executes only the wal3 opener", "wal3", e.opener);

        File cell = tempDir("xfv2cell");
        Map<String, byte[]> inputs = new LinkedHashMap<>();
        for (XFixtureManifest.V2.FileRow f : m.filesOf(e.fixtureId)) {
            File work = new File(cell, f.relName);
            Files.copy(pristine.get(f.fixtureId + "/" + f.relName).toPath(), work.toPath());
            inputs.put(f.relName, Files.readAllBytes(work.toPath()));
        }

        File base = new File(cell, e.openArg);
        switch (e.verdict) {
            case "accept": {
                StoreWAL s = "ro".equals(e.mode) ? StoreWAL.openReadOnly(base) : new StoreWAL(base);
                try {
                    List<FixtureWriter.RecidExpect> expects = m.recids.get(e.fixtureId);
                    assertTrue(ctx + ": accept fixture has no recid rows",
                            expects != null && !expects.isEmpty());
                    FixtureWriter.assertReaderContract(s, expects, ctx);
                } finally {
                    s.close();
                }
                break;
            }
            case "reject":
                // No reject cell reaches this arm from the C3 sample (C4's derived bundles are the
                // first). It is implemented anyway: a verdict the executor cannot run must fail
                // loudly, and `default` below would be the only thing catching it otherwise. The
                // mode is passed through rather than assumed `rw`, so a future `ro reject` cell is
                // actually probed read-only.
                assertRejected(ctx, e.opener, e.mode, base);
                break;
            default:
                fail(ctx + ": unknown verdict " + e.verdict);
        }

        assertPostState(ctx, m, e, cell, inputs);
        TmpFiles.delete(cell);
        dirs.remove(cell);
    }

    /**
     * Checks the cell directory against the manifest's {@code post} rows for this (engine, mode).
     *
     * <p>The rule is two-sided, which is the whole point of the D6 post-cardinality amendment: a
     * file a post row names must match that disposition exactly, and every OTHER input file must
     * be byte-unchanged. A one-sided check — "the named files are right" — would pass a store that
     * rewrote every segment it was handed.
     */
    private static void assertPostState(String ctx, XFixtureManifest.V2 m,
                                        XFixtureManifest.V2.Expect e, File cell,
                                        Map<String, byte[]> inputs) throws IOException {
        List<XFixtureManifest.V2.Post> posts = m.postsOf(e.fixtureId, "java", e.mode);
        assertTrue(ctx + ": no post rows — an accept cell that asserts nothing about the directory "
                + "it just opened is not a check", !posts.isEmpty());
        TreeSet<String> named = new TreeSet<>();
        for (XFixtureManifest.V2.Post p : posts) {
            String where = ctx + " post[" + p.relName + " " + p.verb + "]";
            assertTrue(where + ": two post rows for one file", named.add(p.relName));
            File f = new File(cell, p.relName);
            byte[] was = inputs.get(p.relName);
            switch (p.verb) {
                case "unchanged":
                    assertTrue(where + ": names a file that was not an input", was != null);
                    assertTrue(where + ": file is gone", f.isFile());
                    assertArrayEquals(where + ": bytes changed", was, Files.readAllBytes(f.toPath()));
                    break;
                case "deleted":
                    assertTrue(where + ": names a file that was not an input", was != null);
                    assertTrue(where + ": file is still there", !f.exists());
                    break;
                case "created":
                    assertTrue(where + ": names a file that already existed as an input", was == null);
                    // fall through to the content check
                case "truncated":
                case "modified": {
                    // Reached by fall-through from `created`, which has already asserted the file
                    // was NOT an input; when named directly, these two verbs mean the opposite and
                    // must say so, or a newly created file mislabelled `modified` passes.
                    if (!"created".equals(p.verb))
                        assertTrue(where + ": names a file that was not an input", was != null);
                    assertTrue(where + ": file is missing", f.isFile());
                    byte[] now = Files.readAllBytes(f.toPath());
                    assertEquals(where + ": length", p.length, now.length);
                    assertEquals(where + ": SHA-256", p.sha, FixtureWriter.sha256Hex(now));
                    break;
                }
                default:
                    fail(where + ": unknown disposition verb");
            }
        }
        // Everything the manifest did NOT name: inputs must be untouched, and no other file may
        // have appeared. `x.lock` is not exempt here — the sample pins it with a post row, so an
        // engine that stopped creating it would fail rather than quietly pass a blanket allowance.
        for (Map.Entry<String, byte[]> in : inputs.entrySet()) {
            if (named.contains(in.getKey())) continue;
            File f = new File(cell, in.getKey());
            assertTrue(ctx + ": input " + in.getKey() + " is gone and no post row says so", f.isFile());
            assertArrayEquals(ctx + ": input " + in.getKey() + " changed and no post row says so",
                    in.getValue(), Files.readAllBytes(f.toPath()));
        }
        for (String name : listNames(cell)) {
            assertTrue(ctx + ": unexpected new file " + name,
                    inputs.containsKey(name) || named.contains(name));
        }
    }

    @Test public void sample_v2_cells_conform() throws Exception {
        XFixtureManifest.Loaded loaded = XFixtureManifest.load(V2_ROOT);
        assertEquals("the static sample must be schema v2 — it exists to exercise that path",
                2, loaded.version);
        XFixtureManifest.V2 m = loaded.v2;
        File session = tempDir("xfixtures-v2-session");
        Map<String, File> pristine = gunzipAllV2(m, session);

        // WHAT SHOULD RUN, derived from a DIFFERENT row type than the one that says what will.
        // An earlier revision only checked that the cells it happened to run covered both modes,
        // and the C3j review measured the consequence: deleting one `expect` row and its `post`
        // row left the suite green, because another fixture still supplied that mode. A count or a
        // mode set is a projection of the already-truncated input and cannot see the deletion. The
        // `fixture` rows can: every declared fixture owes one java cell per mode, so a missing
        // `expect` row now contradicts the fixture row that is still there.
        TreeSet<String> want = new TreeSet<>();
        for (String fixtureId : m.fixtureKinds.keySet())
            for (String mode : XFixtureManifest.MODES) want.add(fixtureId + "/" + mode);
        assertTrue("the v2 sample declares no fixtures", !want.isEmpty());

        TreeSet<String> ran = new TreeSet<>();
        for (XFixtureManifest.V2.Expect e : m.expects) {
            if (!"java".equals(e.engine)) continue;
            runV2Cell(m, e, pristine);
            assertTrue("two java cells for " + e.fixtureId + "/" + e.mode,
                    ran.add(e.fixtureId + "/" + e.mode));
        }
        assertEquals("the java cells that ran are not the ones the fixture rows call for",
                want, ran);
    }

    // ---------- framing: the engine's decode against GOLDEN-DECODE.tsv ----------

    /**
     * Compares this engine's decode of every sample segment against the checked-in framing pins.
     *
     * <p>{@code MANIFEST.tsv}'s SHA-256 columns attest WHICH BYTES were read and say nothing about
     * the parse: a reader that framed sections wrongly would match every hash. This is the other
     * half — header fields, and every section's offset, tag, LSN, body length and both CRCs.
     *
     * <p>The comparison is set-equality in both directions, so a pin for a file that was never
     * decoded fails just as loudly as a section that has no pin. {@link Wal3Decode} validates both
     * CRCs while it decodes, so the CRC columns are checked twice over, from opposite ends: the
     * decoder recomputes them from the bytes, and the row asserts the stored value.
     */
    @Test public void sample_v2_framing_matches_golden_decode() throws Exception {
        XFixtureManifest.Loaded loaded = XFixtureManifest.load(V2_ROOT);
        XFixtureManifest.V2 m = loaded.v2;
        File session = tempDir("xfixtures-v2-framing");
        Map<String, File> pristine = gunzipAllV2(m, session);

        Map<String, String> want = goldenDecodeRows();
        Map<String, String> got = new TreeMap<>();
        for (XFixtureManifest.V2.FileRow f : m.files) {
            String where = f.fixtureId + "/" + f.relName;
            byte[] bytes = Files.readAllBytes(pristine.get(where).toPath());
            Wal3Decode.Segment seg = Wal3Decode.decode(bytes, where);
            assertEquals(where + ": " + seg.trailing + " bytes follow the last whole section — a "
                    + "pinned golden segment with an unaccounted tail is not fully described by "
                    + "its pins", 0, seg.trailing);
            String key = f.fixtureId + "\t" + f.relName;
            got.put("hdr\t" + key, seg.header.version + "\t" + seg.header.flags + "\t"
                    + seg.header.seq + "\t" + seg.header.firstLsn + "\t" + Wal3Decode.hex32(seg.header.headerCrc));
            for (Wal3Decode.Section s : seg.sections)
                got.put("sec\t" + key + "\t" + s.index, s.offset + "\t" + s.tag + "\t" + s.lsn + "\t"
                        + s.bodyLen + "\t" + Wal3Decode.hex32(s.hdrCrc) + "\t" + Wal3Decode.hex32(s.bodyCrc));
        }
        assertEquals("GOLDEN-DECODE.tsv keys", want.keySet(), got.keySet());
        for (Map.Entry<String, String> w : want.entrySet())
            assertEquals("GOLDEN-DECODE.tsv row " + w.getKey().replace('\t', ' '),
                    w.getValue(), got.get(w.getKey()));
    }

    /**
     * Reads {@code GOLDEN-DECODE.tsv} into {@code key -> value} rows.
     *
     * <pre>
     *   hdr &lt;bundle&gt; &lt;relName&gt; | &lt;version&gt; &lt;flags&gt; &lt;seq&gt; &lt;firstLsn&gt; &lt;headerCrc&gt;
     *   sec &lt;bundle&gt; &lt;relName&gt; &lt;index&gt; | &lt;off&gt; &lt;tag&gt; &lt;lsn&gt; &lt;bodyLen&gt; &lt;hdrCrc&gt; &lt;bodyCrc&gt;
     * </pre>
     */
    private static Map<String, String> goldenDecodeRows() throws IOException {
        Map<String, String> out = new TreeMap<>();
        for (String line : new String(resource(V2_ROOT + "GOLDEN-DECODE.tsv"), StandardCharsets.UTF_8)
                .split("\n", -1)) {
            if (line.isBlank() || line.startsWith("#")) continue;
            String[] t = line.split("\t", -1);
            String key, value;
            switch (t[0]) {
                case "hdr":
                    assertEquals("bad hdr row: " + line, 8, t.length);
                    key = "hdr\t" + t[1] + "\t" + t[2];
                    value = t[3] + "\t" + t[4] + "\t" + t[5] + "\t" + t[6] + "\t" + t[7];
                    break;
                case "sec":
                    assertEquals("bad sec row: " + line, 10, t.length);
                    key = "sec\t" + t[1] + "\t" + t[2] + "\t" + t[3];
                    value = t[4] + "\t" + t[5] + "\t" + t[6] + "\t" + t[7] + "\t" + t[8] + "\t" + t[9];
                    break;
                default:
                    throw new AssertionError("unknown GOLDEN-DECODE.tsv row type: " + line);
            }
            assertTrue("duplicate GOLDEN-DECODE.tsv row: " + line, out.put(key, value) == null);
        }
        assertTrue("GOLDEN-DECODE.tsv is empty", !out.isEmpty());
        return out;
    }

    /**
     * Re-derives {@code GOLDEN-BODY.tsv} from the sample bytes and compares it to the checked-in
     * file, line by line.
     *
     * <p>This engine AUTHORS that file — it is the §11.2 authority on what a body means, and the
     * rust and zig readers are graded against it — so the check here is drift, not agreement:
     * nothing in this repo could disagree with the emitter, and a comparison of a generator's
     * output to a fresh run of the same generator would pass for any corpus at all. What makes it
     * more than that is what {@link Wal3BodyDump} asserts while rendering (every record's content
     * must fit the invertible payload function, and the recid set must match the manifest's
     * python-folded rows) and what the sample's own cells assert about the same bytes through the
     * real engine.
     */
    @Test public void sample_v2_body_matches_golden_body() throws Exception {
        String want = new String(resource(V2_ROOT + Wal3BodyDump.FILE_NAME), StandardCharsets.UTF_8);
        String got = Wal3BodyDump.render();
        String[] w = want.split("\n", -1), g = got.split("\n", -1);
        for (int i = 0; i < Math.min(w.length, g.length); i++)
            assertEquals(Wal3BodyDump.FILE_NAME + " line " + (i + 1), w[i], g[i]);
        assertEquals(Wal3BodyDump.FILE_NAME + ": line count", w.length, g.length);
        // A dump that emitted no rows would match an empty checked-in file and read as green.
        assertTrue(Wal3BodyDump.FILE_NAME + " pins no entries", want.contains("\nent\t"));
        // The two cases the file exists to keep apart must BOTH be in it: `lenPlus 0` with no
        // content sha, and `lenPlus 1` with the empty-string sha. Losing either from the corpus
        // would leave this comparison unable to fail on the distinction it was built for.
        assertTrue(Wal3BodyDump.FILE_NAME + " pins no NULL-content record",
                want.contains("\tRECORD\t12\t0\t0\t-\n"));
        assertTrue(Wal3BodyDump.FILE_NAME + " pins no zero-length record",
                want.contains("\t1\te3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n"));
    }

    // ---------- the dispatch itself ----------

    /**
     * Pins the version dispatch, which is the one thing in this reader that cannot be checked by
     * running it: v1 and v2 {@code expect} rows are both seven fields, so a reader that keyed on
     * arity would parse either file without complaint and put {@code mode} where {@code verdict}
     * belongs. These cases read a column that moved and assert it landed in the right field.
     */
    @Test public void manifest_dispatches_on_the_version_row() {
        XFixtureManifest.Loaded v1 = XFixtureManifest.parse(String.join("\n",
                "version\t1",
                "fixture\tf\tdirect\tjava\tc",
                "expect\tf\tjava\treject\tdirect\tf.db\tf.db") + "\n");
        assertEquals(1, v1.version);
        assertTrue("v1 parse produced a v2 model", v1.v2 == null);
        assertEquals("reject", v1.v1.expects.get(0).verdict);
        assertEquals("f.db", v1.v1.expects.get(0).placeAs);

        XFixtureManifest.Loaded v2 = XFixtureManifest.parse(String.join("\n",
                "version\t2",
                "fixture\tf\twal3-namespace\tjava\tc",
                "expect\tf\tjava\tro\taccept\twal3\tx") + "\n");
        assertEquals(2, v2.version);
        assertTrue("v2 parse produced a v1 model", v2.v1 == null);
        // The columns that moved. `ro` in the verdict slot is exactly the misread an arity-keyed
        // dispatcher makes, and it is caught here rather than by a downstream vocabulary check.
        assertEquals("ro", v2.v2.expects.get(0).mode);
        assertEquals("accept", v2.v2.expects.get(0).verdict);
        assertEquals("wal3", v2.v2.expects.get(0).opener);

        refused("an unsupported schema version", "version\t3\n");
        refused("a manifest whose first data line is not a version row",
                "fixture\tf\tdirect\tjava\tc\n");
    }

    /**
     * Every reader in this workstream owes a proof that it REFUSES an unknown row type, because
     * the python validator that gates the grammar cannot test a Java parser: a reader that skipped
     * a row type it did not recognise would pass every cell of every manifest it was ever given
     * while ignoring whatever that row asserted.
     */
    @Test public void unknown_row_types_are_refused() {
        refused("an unknown v1 row type", "version\t1\nsomething\tf\tx\n");
        refused("an unknown v2 row type", "version\t2\nsomething\tf\tx\n");
        // A row type from the OTHER schema is unknown too: v1 has no `post`, v2 has no ... nothing
        // it lacks, so only this direction can be shown, and it is the direction C6 will exercise.
        refused("a v2 post row inside a v1 manifest",
                "version\t1\npost\tf\tjava\trw\tx.lock\tunchanged\n");
        refused("a wrong-arity v2 expect row",
                "version\t2\nfixture\tf\twal3-namespace\tjava\tc\nexpect\tf\tjava\tro\taccept\twal3\n");
        refused("an out-of-vocabulary v2 mode",
                "version\t2\nfixture\tf\twal3-namespace\tjava\tc\n"
                        + "expect\tf\tjava\tnope\taccept\twal3\tx\n");
        refused("an out-of-vocabulary v2 engine",
                "version\t2\nfixture\tf\twal3-namespace\tjava\tc\n"
                        + "expect\tf\tocaml\tro\taccept\twal3\tx\n");
        refused("an empty field", "version\t2\nfixture\tf\twal3-namespace\t\tc\n");
        refused("a non-canonical integer",
                "version\t2\nfixture\tf\twal3-namespace\tjava\tc\nrecid\tf\tr1\t007\tlive\t1\t2\n");
        refused("a relName that escapes the cell directory",
                "version\t2\nfixture\tf\twal3-namespace\tjava\tc\n"
                        + "file\tf\t../../etc/passwd\t1\t" + "0".repeat(64) + "\t" + "0".repeat(64) + "\n");
        refused("a duplicate java cell",
                "version\t2\nfixture\tf\twal3-namespace\tjava\tc\n"
                        + "expect\tf\tjava\tro\taccept\twal3\tx\n"
                        + "expect\tf\tjava\tro\treject\twal3\tx\n");
        refused("two rows for one recid",
                "version\t2\nfixture\tf\twal3-namespace\tjava\tc\n"
                        + "recid\tf\tr1\t1\tlive\t1\t2\nrecid\tf\tr2\t1\tnull\t0\t0\n");
        refused("a recidrange whose span would be expanded one entry at a time",
                "version\t2\nfixture\tf\twal3-namespace\tjava\tc\n"
                        + "recidrange\tf\tr\t1\t9223372036854775807\tlive\t1\t2\n");
    }

    /**
     * {@code bytes} is a KNOWN v2 row type that this reader refuses because it cannot execute one
     * yet — which is a different claim from "unknown row type", and the C3j review was right that
     * filing it under that heading muddled the proof.
     *
     * <p>The row asserts specific bytes at an offset of a derived fixture, and no derived fixture
     * exists before C4. The three ways to handle that are: execute it (impossible — there is
     * nothing to execute it against, and an untested executor is worse than none), ignore it
     * (forbidden: a silently skipped assertion is the defect this whole workstream is built to
     * prevent), or refuse. Refusing means C4 cannot land a {@code bytes} row without also teaching
     * this reader to run it, which is the outcome worth having.
     */
    @Test public void a_bytes_row_is_refused_until_c4_can_execute_it() {
        refused("a v2 bytes row", "version\t2\nfixture\tf\twal3-namespace\tjava\tc\n"
                + "bytes\tf\tjava\trw\tx.wal.0000000000000001\t0\tdeadbeef\n");
    }

    /**
     * Every file distributed under {@code /xfixtures-v2/} must be one the manifest or the golden
     * tables account for.
     *
     * <p>Without this, an extra {@code .gz} that no row mentions rides along unnoticed: the suite
     * checks the files it was TOLD about and never asks what else is there. That makes the copied
     * sample merely sufficient rather than complete, and C4's sync step needs it complete to
     * compare two runs' trees.
     */
    @Test public void the_v2_resource_tree_has_nothing_unexplained() throws Exception {
        java.net.URL root = XFixtureConformanceTest.class.getResource(V2_ROOT);
        assertTrue("resource root " + V2_ROOT + " is missing", root != null);
        // Tests run from an exploded target/test-classes; if that ever changes this must be
        // rewritten rather than silently skipped, so it fails instead of returning early.
        assertEquals("the v2 resources are not on an exploded classpath — this check needs rewriting",
                "file", root.getProtocol());
        TreeSet<String> present = listNames(new File(root.toURI()));

        XFixtureManifest.V2 m = XFixtureManifest.load(V2_ROOT).v2;
        TreeSet<String> expected = new TreeSet<>(List.of(
                "MANIFEST.tsv", "GOLDEN-DECODE.tsv", Wal3BodyDump.FILE_NAME));
        for (XFixtureManifest.V2.FileRow f : m.files) expected.add(f.blobName());
        assertEquals(V2_ROOT + " holds files no row accounts for (or is missing one)",
                expected, present);
    }

    /**
     * Asserts the parser refuses {@code manifest}.
     *
     * <p>The accepted/refused flag is not decoration around a {@code fail()} in the try block:
     * junit's {@code fail} throws {@link AssertionError} too, so a {@code catch (AssertionError)}
     * would swallow the very failure that reports the parser accepted bad input, and this check
     * would then pass no matter what the parser did.
     */
    private static void refused(String what, String manifest) {
        boolean accepted;
        AssertionError refusal = null;
        try {
            XFixtureManifest.parse(manifest);
            accepted = true;
        } catch (AssertionError e) {
            accepted = false;
            refusal = e;
        }
        assertTrue("the manifest reader accepted " + what, !accepted);
        assertTrue("refusal carried no message: " + what, refusal.getMessage() != null);
    }
}
