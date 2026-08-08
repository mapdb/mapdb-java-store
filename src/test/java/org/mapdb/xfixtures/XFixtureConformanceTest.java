package org.mapdb.xfixtures;

import org.junit.After;
import org.junit.Test;
import org.mapdb.TmpFiles;
import org.mapdb.store.Wal3Decode;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Cross-port conformance harness: runs every {@code engine == java} cell of the checked-in
 * schema-v2 sample against this engine (Stage C, <b>C7j</b> — schema-v1 retired).
 *
 * <p>The fixtures pin the CURRENT state of an UNSTABLE format so that silent divergence between
 * the store engines is detected. Cross-engine openability today is an implementation fact, not a
 * supported feature; any format change regenerates the fixtures as part of that change.
 *
 * <p><b>Schema v2 only.</b> {@code /xfixtures-v2/} holds the static schema-v2 sample over the wal3
 * golden bundles. The dual v1/v2 dispatch and the schema-v1 tree retired at C7j once the corpus
 * root was green (C6). The frozen corpus runs in {@link XFixtureCorpusTest}.
 *
 * <p>A fixture is a whole namespace — several file rows, blobs named
 * {@code <fixtureId>.<relName>.gz} — and a cell names a {@code mode} ({@code rw} or {@code ro}) as
 * well as a verdict. Post-open state is asserted from the manifest's own {@code post} rows rather
 * than from a blanket rule. On top of the cells,
 * {@link #sample_v2_framing_matches_golden_decode} compares this engine's decode of every sample
 * segment against {@code GOLDEN-DECODE.tsv}, which pins what the bytes MEAN where the manifest's
 * SHA-256 columns pin only which bytes were read.
 *
 * <p>All temp state lives in {@link TmpFiles}-owned directories so nothing leaks into
 * {@code java.io.tmpdir}.
 */
public class XFixtureConformanceTest {

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
        return XFixtureV2Executor.resource(path);
    }

    private static TreeSet<String> listNames(File dir) {
        return XFixtureV2Executor.listNames(dir);
    }

    // ---------- schema v2 ----------

    /** Gunzips every v2 sample file once into {@code <session>/<fixtureId>/<relName>}. */
    private static File gunzipAllV2(XFixtureManifest.V2 m, File session) throws IOException {
        XFixtureV2Executor.gunzipAll(m, V2_ROOT, session);
        return session;
    }

    /**
     * The static sample's cells, through the shared v2 executor.
     *
     * <p>The sample is the {@code v2-core} profile — no {@code applies}, {@code action},
     * {@code bytes} or {@code reopen} rows — and it runs under exactly the same rules as the
     * corpus. An earlier draft gave the corpus a relaxed accept rule the sample did not get; both
     * C5j reviewers showed that was a deletion rather than the disjunction the plan asked for, so
     * there is no per-root knob left to state.
     */
    @Test public void sample_v2_cells_conform() throws Exception {
        XFixtureManifest.Loaded loaded = XFixtureManifest.load(V2_ROOT);
        assertEquals("the static sample must be schema v2 — it exists to exercise that path",
                2, loaded.version);
        XFixtureManifest.V2 m = loaded.v2;
        File session = tempDir("xfixtures-v2-session");
        gunzipAllV2(m, session);
        XFixtureV2Executor x = new XFixtureV2Executor(m, session);

        // The sample is v2-core, in BOTH directions. A root that grew an oracle row would be
        // running assertions this test's rules never bought, and — since C5 moved the profile
        // split into the grammar — that is a refusal rather than a widening.
        assertTrue("the static sample carries an oracle row; it is v2-core through C7",
                m.applies.isEmpty() && m.actions.isEmpty() && m.bytes.isEmpty()
                        && m.reopens.isEmpty());

        // WHAT SHOULD RUN, derived from a DIFFERENT row type than the one that says what will.
        // An earlier revision only checked that the cells it happened to run covered both modes,
        // and the C3j review measured the consequence: deleting one `expect` row and its `post`
        // row left the suite green, because another fixture still supplied that mode. A count or a
        // mode set is a projection of the already-truncated input and cannot see the deletion. The
        // `fixture` rows can: every declared fixture owes one java cell per mode, so a missing
        // `expect` row now contradicts the fixture row that is still there. (The corpus cannot use
        // this rule — its cell set is legitimately partial — which is what `applies` is for.)
        TreeSet<String> want = new TreeSet<>();
        for (String fixtureId : m.fixtureKinds.keySet())
            for (String mode : XFixtureManifest.MODES) want.add(fixtureId + "/" + mode);
        assertTrue("the v2 sample declares no fixtures", !want.isEmpty());

        TreeSet<String> ran = new TreeSet<>();
        for (XFixtureManifest.V2.Expect e : m.expects) {
            if (!"java".equals(e.engine)) continue;
            File cell = tempDir("xfv2cell");
            x.runCell(e, cell);
            assertTrue("two java cells for " + e.fixtureId + "/" + e.mode,
                    ran.add(e.fixtureId + "/" + e.mode));
            TmpFiles.delete(cell);
            dirs.remove(cell);
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
        gunzipAllV2(m, session);

        Map<String, String> want = goldenDecodeRows();
        Map<String, String> got = new TreeMap<>();
        for (XFixtureManifest.V2.FileRow f : m.files) {
            String where = f.fixtureId + "/" + f.relName;
            byte[] bytes = Files.readAllBytes(
                    new File(new File(session, f.fixtureId), f.relName).toPath());
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

    // ---------- the version gate (C7j) ----------

    /**
     * Schema version 1 is retired; schema version 2 is the only accepted form.
     *
     * <p>The historical reason the version row is a hard gate, not an arity guess: v1 and v2
     * {@code expect} rows were both seven fields with different columns. A reader that keyed on
     * field count would put {@code mode} where {@code verdict} belongs. These cases pin that v2
     * columns land in the right fields and that v1 (and any other version) is refused by name.
     */
    @Test public void manifest_accepts_only_schema_v2() {
        XFixtureManifest.Loaded v2 = XFixtureManifest.parse(String.join("\n",
                "version\t2",
                "fixture\tf\twal3-namespace\tjava\tc",
                "expect\tf\tjava\tro\taccept\twal3\tx") + "\n");
        assertEquals(2, v2.version);
        assertTrue("v2 parse produced no model", v2.v2 != null);
        // The columns that moved at the v1→v2 flip. `ro` in the verdict slot is exactly the
        // misread an arity-keyed dispatcher makes, and it is caught here rather than by a
        // downstream vocabulary check.
        assertEquals("ro", v2.v2.expects.get(0).mode);
        assertEquals("accept", v2.v2.expects.get(0).verdict);
        assertEquals("wal3", v2.v2.expects.get(0).opener);

        refused("retired schema version 1",
                "version\t1\nfixture\tf\tdirect\tjava\tc\nexpect\tf\tjava\treject\tdirect\tf.db\tf.db\n");
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
        refused("an unknown v2 row type", "version\t2\nsomething\tf\tx\n");
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
     * The four C5 oracle rows are PARSED, and every field that is a grammar is checked.
     *
     * <p>Through C4 a {@code bytes} row was refused outright, because no derived fixture existed to
     * execute one against and a silently skipped assertion is the defect this workstream is built
     * to prevent. C5j gives it an input — {@code /xfixtures-v2-corpus/} — so the refusal is
     * replaced by execution ({@link XFixtureCorpusTest}) and by these grammar cases, which are the
     * half execution cannot reach: the corpus carries exactly one of each row type, so no shape it
     * does not happen to have is graded by running it.
     */
    @Test public void the_c5_oracle_rows_are_parsed_and_their_grammars_checked() {
        XFixtureManifest.V2 m = XFixtureManifest.parse(String.join("\n",
                "version\t2",
                "fixture\tf\twal3-namespace\tjava\tc",
                "applies\tf\tjava\trw",
                "expect\tf\tjava\trw\taccept\twal3\tx",
                "action\tf\tjava\trw\tcommit_one_record\top=put,payload_id=161,serializer=raw",
                "bytes\tf\tjava\trw\tx.wal.0000000000000004\t187\t8000000000000000",
                "reopen\tf\tjava\trw\tS2",
                "family\tf\tjava\trw\tS2") + "\n").v2;
        assertEquals("rw", m.applies.get(0).mode);
        assertEquals("commit_one_record", m.actions.get(0).verb);
        assertEquals(187, m.bytes.get(0).offset);
        assertEquals("8000000000000000", m.bytes.get(0).hex);
        assertEquals("S2", m.reopens.get(0).family);
        assertEquals("S2", m.families.get(0).family);

        String head = "version\t2\nfixture\tf\twal3-namespace\tjava\tc\n";
        refused("a duplicate applies row", head + "applies\tf\tjava\trw\napplies\tf\tjava\trw\n");
        refused("a duplicate action row for one verb", head
                + "action\tf\tjava\trw\tv\ta=1\naction\tf\tjava\trw\tv\ta=2\n");
        refused("a duplicate reopen row",
                head + "reopen\tf\tjava\trw\tS2\nreopen\tf\tjava\trw\tS9\n");
        refused("a duplicate family row",
                head + "family\tf\tjava\trw\tS2\nfamily\tf\tjava\trw\tS9\n");
        refused("a duplicate bytes row at one offset", head
                + "bytes\tf\tjava\trw\tx\t1\taa\nbytes\tf\tjava\trw\tx\t1\tbb\n");
        // `catalogue.render_action_args` sorts the keys and pins the value character class. A
        // reader that accepted any order would accept a manifest python refuses, and the two roots
        // would then disagree about what the same cell says.
        refused("action arguments out of sorted order",
                head + "action\tf\tjava\trw\tv\tb=1,a=2\n");
        refused("a repeated action argument key", head + "action\tf\tjava\trw\tv\ta=1,a=2\n");
        refused("an action argument that is not k=v", head + "action\tf\tjava\trw\tv\tabc\n");
        refused("an action argument key outside [a-z][a-z0-9_]*",
                head + "action\tf\tjava\trw\tv\tA=1\n");
        refused("an action argument value with a comma in it",
                head + "action\tf\tjava\trw\tv\ta=1;b,c=2\n");
        refused("an empty action argument value", head + "action\tf\tjava\trw\tv\ta=\n");
        refused("an odd-length bytes value", head + "bytes\tf\tjava\trw\tx\t0\tabc\n");
        refused("an uppercase bytes value", head + "bytes\tf\tjava\trw\tx\t0\tAB\n");
        refused("an empty bytes value", head + "bytes\tf\tjava\trw\tx\t0\t\n");
        refused("a bytes offset that is not a canonical integer",
                head + "bytes\tf\tjava\trw\tx\t007\tab\n");
        refused("a bytes relName that escapes the cell directory",
                head + "bytes\tf\tjava\trw\t../x\t0\tab\n");
        refused("an out-of-vocabulary engine on an applies row",
                head + "applies\tf\tocaml\trw\n");
        refused("a wrong-arity reopen row", head + "reopen\tf\tjava\trw\n");
        refused("a wrong-arity family row", head + "family\tf\tjava\trw\n");
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
