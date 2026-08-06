package org.mapdb.xfixtures;

import org.junit.After;
import org.junit.Test;
import org.mapdb.DBException;
import org.mapdb.TmpFiles;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Runs the schema-v2 <b>preflight corpus</b> root against this engine (Stage C, slice C5j).
 *
 * <p>{@code /xfixtures-v2-corpus/} is a byte-identical copy of the {@code root}-marked files of
 * {@code todo/store-cross/preflight-v2/} — twelve files: {@code MANIFEST.tsv} and one blob per
 * {@code file} row, and nothing else (C5 plan §4c). It is the {@code v2-oracle} profile: it carries
 * {@code applies}, {@code action}, {@code bytes} and {@code reopen} rows, all four of which this
 * engine EXECUTES. The static {@code /xfixtures-v2/} sample stays {@code v2-core} and is untouched
 * by C5; {@link XFixtureConformanceTest} still owns it, through the same executor.
 *
 * <h2>What makes the copy more than a copy</h2>
 * Three roots hand-copied between four repositories is how the static sample survives today only by
 * luck. {@link #the_corpus_root_matches_todos_sealed_tree} re-derives
 * {@code freeze_v2.dist_seal(files, "java")} from the resource directory — the file set, every
 * size, every content hash, under todo's own preimage grammar — and compares it to the constant
 * todo's gate re-derives from {@code FROZEN.tsv} on every run. Neither side can move without the
 * other going red, and the comparison is in CI rather than in a review note.
 *
 * <p>That seal is also the corpus's <b>completeness</b> authority, which is what buys
 * {@link XFixtureV2Executor.Rules#sealedRoot} its relaxed accept rule: a {@code recid},
 * {@code action} or {@code reopen} row that goes missing changes {@code MANIFEST.tsv} and is caught
 * whatever kind of row it was. The sample root has no such authority and keeps the strict rule.
 *
 * <h2>The deletion campaign</h2>
 * Nine checks were removed one at a time and the suite required to go red for the reason each
 * names — the reopen execution, the bytes execution, the action execution, the action with the
 * bytes assertion neutralised beside it (so the {@code modified:279} post row is what fires), the
 * consumption accountant's call site, the {@code ro} mode SELECTION, the opener dispatch, the
 * two-sided file-set rule, and the S2 message predicate. All nine killed.
 *
 * <p>Three of them had no natural input and are green with the check deleted unless something
 * supplies one: the accountant (every row is consumed today), the file-set rule (every file is
 * named today) and the reader contract (nothing observes a leaf assertion). The first two are
 * given DOCTORED inputs by tests in this file; the third cannot be — deleting the last assertion
 * in a chain is undetectable by construction — so what is proved instead is that it FIRES, which
 * is a different claim and is labelled as one.
 */
public class XFixtureCorpusTest {

    static final String ROOT = "/xfixtures-v2-corpus/";

    /**
     * {@code freeze_v2.PREFLIGHT_DIST_SEALS["java"]}, and the referent is
     * {@code todo/store-cross/preflight-v2/} — never this directory. A constant regenerated from
     * the tree it grades certifies that the tree equals itself.
     *
     * <p>Regenerate with {@code python3 todo/store-cross/freeze_v2.py --dist-seals --preflight}.
     */
    static final String DIST_SEAL =
            "8888492b074a202328a5ccf17e1024e7067a0e045030e804ea50234f3d8cd2c7";

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

    private static XFixtureManifest.V2 manifest() throws IOException {
        XFixtureManifest.Loaded loaded = XFixtureManifest.load(ROOT);
        assertEquals("the corpus root must be schema v2", 2, loaded.version);
        return loaded.v2;
    }

    // ------------------------------------------------------------------ the cells

    /**
     * Every {@code applies} row addressed to java, run, and exactly those.
     *
     * <p>The cardinality rule is the C5 replacement for "every fixture × every mode": the corpus's
     * cell set is legitimately partial — {@code reject-wal3-d1-barebase} has no java rows at all,
     * and {@code reject-wal3-segment-at-direct} has no {@code ro} row — and an executor that
     * required the full product would refuse the corpus. {@code applies} says which cells exist.
     *
     * <p>Two row types emitted from one catalogue is a pair that moves together, so this also
     * requires {@code applies == expect} per cell, in both directions. That check is deliberately
     * absent from {@code manifest_v2.py} — there both sets are compared to the catalogue a few
     * lines apart, so a third comparison could only fire after one of those already had. An engine
     * has no catalogue, so for an engine the disagreement is the only detectable inconsistency, and
     * without it a manifest could have this suite run a cell it holds no verdict for.
     */
    @Test public void corpus_cells_conform() throws Exception {
        XFixtureManifest.V2 m = manifest();
        File session = tempDir("xfcorpus-session");
        XFixtureV2Executor.gunzipAll(m, ROOT, session);
        XFixtureV2Executor x = new XFixtureV2Executor(m, XFixtureV2Executor.Rules.sealedRoot(),
                session);

        TreeSet<String> want = new TreeSet<>();
        for (XFixtureManifest.V2.Applies a : m.applies)
            if ("java".equals(a.engine)) want.add(a.fixtureId + "/" + a.mode);
        assertTrue("the corpus declares no java applies rows", !want.isEmpty());

        TreeSet<String> expects = new TreeSet<>();
        for (XFixtureManifest.V2.Expect e : m.expects)
            if ("java".equals(e.engine)) expects.add(e.fixtureId + "/" + e.mode);
        assertEquals("the java `applies` rows and the java `expect` rows are different sets",
                want, expects);

        TreeSet<String> ran = new TreeSet<>();
        for (XFixtureManifest.V2.Expect e : m.expects) {
            if (!"java".equals(e.engine)) continue;
            File cell = tempDir("xfcorpuscell");
            x.runCell(e, cell);
            assertTrue("two java cells for " + e.fixtureId + "/" + e.mode,
                    ran.add(e.fixtureId + "/" + e.mode));
            TmpFiles.delete(cell);
            dirs.remove(cell);
        }
        assertEquals("the java cells that ran are not the ones `applies` calls for", want, ran);

        // The oracle rows exist and are addressed to cells that ran. Without this the whole C5j
        // execution path could be absent from the corpus and every assertion above would still
        // pass — the shape §7 of the C5 plan exists to stop, and the one both round-3 reviewers
        // found revision 3 had shipped for rust and zig.
        int actions = 0, bytes = 0, reopens = 0;
        for (XFixtureManifest.V2.Action a : m.actions) if ("java".equals(a.engine)) actions++;
        for (XFixtureManifest.V2.Bytes b : m.bytes) if ("java".equals(b.engine)) bytes++;
        for (XFixtureManifest.V2.Reopen r : m.reopens) if ("java".equals(r.engine)) reopens++;
        assertEquals("the corpus carries no `action` row for java, so this engine's action "
                + "executor has no input at all", 1, actions);
        assertEquals("the corpus carries no `bytes` row for java", 1, bytes);
        assertEquals("the corpus carries no `reopen` row for java", 1, reopens);
    }

    // -------------------------------------------------------------- the §3.11 mutant

    /**
     * Routing the {@code direct} cell through the WAL opener must turn this suite RED.
     *
     * <p>{@code reject-wal3-segment-at-direct} publishes a v3 segment as the bare file {@code x} and
     * expects {@code reject}/{@code direct}. The C5 plan §3.11 expected the discrimination to be the
     * lock sidecar; for java it is the VERDICT, and both halves of the plan's reasoning were
     * measured false here — see {@link XFixtureV2Executor.Dispatch}. java's WAL opener ACCEPTS a
     * regular file at the base path (that is the D1 divergence, which is why this fixture has no
     * java rows) and mints a fresh segment, so a misrouted cell fails on "expected
     * DataCorruption, but the store opened" long before any file-set rule looks at it.
     *
     * <p>Asserting that here is the plan's requirement either way: a deletion that merely restored
     * an {@code opener == "wal3"} refusal would prove parser branching and nothing about this
     * engine.
     */
    @Test public void a_direct_cell_sent_to_the_wal_opener_goes_red() throws Exception {
        XFixtureManifest.V2 m = manifest();
        File session = tempDir("xfcorpus-mutant");
        XFixtureV2Executor.gunzipAll(m, ROOT, session);
        XFixtureV2Executor x = new XFixtureV2Executor(m, XFixtureV2Executor.Rules.sealedRoot(),
                session);

        XFixtureManifest.V2.Expect direct = null;
        for (XFixtureManifest.V2.Expect e : m.expects)
            if ("java".equals(e.engine) && "direct".equals(e.opener)) direct = e;
        assertTrue("the corpus has no java `direct` cell, so this mutant grades nothing",
                direct != null);

        // The control: through the opener the manifest names, the cell passes and leaves exactly
        // the two files the manifest accounts for — `x` and the lock java's StoreDirect takes.
        File cell = tempDir("xfcorpusmutant");
        x.runCell(direct, cell, XFixtureV2Executor.Dispatch.BY_MANIFEST);
        assertEquals("the direct cell's file set", new TreeSet<>(List.of("x", "x.lock")),
                XFixtureV2Executor.listNames(cell));

        File cell2 = tempDir("xfcorpusmutant2");
        AssertionError caught = null;
        try {
            x.runCell(direct, cell2, XFixtureV2Executor.Dispatch.ALWAYS_WAL3);
        } catch (AssertionError err) {
            caught = err;
        }
        assertTrue("the direct cell passed through the WAL opener, so the opener column is "
                + "decoration on this engine", caught != null);
        assertTrue("it went red for the wrong reason: " + caught.getMessage(),
                caught.getMessage() != null
                        && caught.getMessage().contains("but the store opened"));
        // …and it opened far enough to write. Named separately from the verdict because the two
        // are independent facts and a future java that refused D1 would keep this one.
        assertTrue("the misrouted open left no fresh segment behind, so this mutant is measuring "
                + "less than it says: " + XFixtureV2Executor.listNames(cell2),
                XFixtureV2Executor.listNames(cell2).contains("x.wal.0000000000000001"));
    }

    // ------------------------------------------------------------- ro, both directions

    /**
     * The read-only refusal is not vacuous: the same write, on the same fixture, through the two
     * handles, with opposite outcomes.
     *
     * <p>C3z's review found the general shape this closes — {@code mode} was parsed,
     * vocabulary-checked and used to pick an opener, and then nothing observed the difference, so
     * every {@code ro} cell in java and rust was a writable open wearing a label. Asserting only the
     * refusal would leave the same hole one step along: a {@code put} that failed for an unrelated
     * reason in BOTH modes would satisfy it. So the {@code rw} half runs here too, and it is the
     * same call.
     *
     * <p>This runs outside the cell executor deliberately — the {@code rw} write mutates the
     * directory, and doing it inside a cell would author a post state no manifest row describes.
     */
    @Test public void the_read_only_write_refusal_discriminates() throws Exception {
        XFixtureManifest.V2 m = manifest();
        File session = tempDir("xfcorpus-ro");
        XFixtureV2Executor.gunzipAll(m, ROOT, session);
        String fid = "wal3-java-cleaned";

        File rw = tempDir("xfcorpus-rw-probe");
        long recid;
        org.mapdb.store.StoreWAL w = new org.mapdb.store.StoreWAL(
                new File(stage(m, fid, session, rw), "x"));
        try {
            recid = w.put(new byte[] {1, 2, 3}, org.mapdb.store.Wal3Actions.RAW);
            w.commit();
        } finally {
            w.close();
        }
        assertTrue("the writable handle refused the write, so the read-only half below proves "
                + "nothing about the mode", recid > 0);

        File ro = tempDir("xfcorpus-ro-probe");
        org.mapdb.store.StoreWAL r = org.mapdb.store.StoreWAL.openReadOnly(
                new File(stage(m, fid, session, ro), "x"));
        DBException refusal = null;
        try {
            r.put(new byte[] {1, 2, 3}, org.mapdb.store.Wal3Actions.RAW);
        } catch (DBException x) {
            refusal = x;
        } finally {
            r.close();
        }
        assertTrue("the read-only handle ACCEPTED the write", refusal != null);
        assertTrue("the refusal does not name the mode: " + refusal.getMessage(),
                refusal.getMessage().contains("open read-only"));
    }

    private static File stage(XFixtureManifest.V2 m, String fid, File session, File into)
            throws IOException {
        for (XFixtureManifest.V2.FileRow f : m.filesOf(fid))
            Files.copy(new File(new File(session, fid), f.relName).toPath(),
                    new File(into, f.relName).toPath());
        return into;
    }

    // ------------------------------------------- the two rules with no natural input
    //
    // Deleting either of the two checks below leaves the whole suite GREEN, because the corpus
    // contains no input that reaches them: every oracle row IS consumed and every file the engine
    // creates IS named by a post row. That is lesson (i) — a rule can be correct, directly tested
    // and never called — and the answer C3z settled on is a DOCTORED input, not an argument. Both
    // cases below doctor the manifest, run the real executor over the real bytes, and require the
    // rule to fire. Delete the rule and these two go red.

    /** The corpus manifest, with {@code edits} applied to its text, re-parsed. */
    private static XFixtureManifest.V2 doctored(java.util.function.UnaryOperator<String> edit)
            throws IOException {
        String text = new String(XFixtureV2Executor.resource(ROOT + "MANIFEST.tsv"),
                StandardCharsets.UTF_8);
        String out = edit.apply(text);
        assertTrue("the doctoring changed nothing, so this case grades the same manifest twice",
                !out.equals(text));
        return XFixtureManifest.parse(out).v2;
    }

    /**
     * An oracle row addressed to a cell whose arm has no handler for it must FAIL the cell.
     *
     * <p>The grammar permits an {@code action} row on a {@code reject} cell; the catalogue never
     * emits one, and the reject arm never opens a store to run it against. Without the accountant
     * that row is parsed, addressed, and silently dropped — which is precisely "parses" wearing
     * "executes"'s clothes.
     */
    @Test public void an_oracle_row_no_arm_can_run_fails_the_cell() throws Exception {
        XFixtureManifest.V2 m = doctored(t -> t.replace(
                "expect\treject-wal3-segment-at-direct\tjava\trw\treject\tdirect\tx\n",
                "expect\treject-wal3-segment-at-direct\tjava\trw\treject\tdirect\tx\n"
                        + "action\treject-wal3-segment-at-direct\tjava\trw\tcommit_one_record"
                        + "\top=put,payload_id=1,payload_len=1,recid_label=Z,serializer=raw\n"));
        File session = tempDir("xfcorpus-owed");
        XFixtureV2Executor.gunzipAll(m, ROOT, session);
        XFixtureV2Executor x = new XFixtureV2Executor(m, XFixtureV2Executor.Rules.sealedRoot(),
                session);
        XFixtureManifest.V2.Expect direct = javaCell(m, "reject-wal3-segment-at-direct", "rw");
        AssertionError caught = null;
        try {
            x.runCell(direct, tempDir("xfcorpus-owedcell"));
        } catch (AssertionError err) {
            caught = err;
        }
        assertTrue("the executor was handed an action it never ran and reported green", caught != null);
        assertTrue("it failed for another reason: " + caught.getMessage(),
                caught.getMessage().contains("no handler consumed"));
    }

    /**
     * A file the engine creates that no {@code post} row names must fail the cell.
     *
     * <p>The two-sided file-set rule is what makes the post oracle more than "the named files are
     * right"; a one-sided reading would pass a store that rewrote or littered everything it was not
     * asked about. Deleting the {@code x.lock} row is how that side gets an input: the lock is
     * still created, and now nothing accounts for it.
     *
     * <p>The cell is {@code div-wal3-lsn-exhausted java rw} because it is the only java cell with
     * TWO post rows. Doctoring a single-row cell would leave it with none and trip the "a cell that
     * asserts nothing is not a check" guard instead — an input that trips several checks measures
     * the first one only (lesson h), and this case is about the second.
     */
    @Test public void a_file_no_post_row_names_fails_the_cell() throws Exception {
        String row = "post\tdiv-wal3-lsn-exhausted\tjava\trw\tx.lock\tcreated:0:"
                + "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n";
        XFixtureManifest.V2 m = doctored(t -> t.replace(row, ""));
        File session = tempDir("xfcorpus-unnamed");
        XFixtureV2Executor.gunzipAll(m, ROOT, session);
        XFixtureV2Executor x = new XFixtureV2Executor(m, XFixtureV2Executor.Rules.sealedRoot(),
                session);
        AssertionError caught = null;
        try {
            x.runCell(javaCell(m, "div-wal3-lsn-exhausted", "rw"), tempDir("xfcorpus-unnamedcell"));
        } catch (AssertionError err) {
            caught = err;
        }
        assertTrue("the engine created x.lock, no row named it, and the cell passed", caught != null);
        assertTrue("it failed for another reason: " + caught.getMessage(),
                caught.getMessage().contains("unexpected new file x.lock"));
    }

    /**
     * The reader contract is NON-VACUOUS on this corpus — a different claim from the two above,
     * and the honest one for a leaf assertion.
     *
     * <p>Deleting {@code assertReaderContract} leaves the suite green, and it always will: nothing
     * observes the last assertion in a chain, so a deletion mutant is the wrong instrument. What
     * can be shown is that the assertion FIRES — that {@code wal3-java-cleaned}'s six recid rows
     * are compared against a real read rather than carried past it. Record A holds
     * {@code payload(116, 120)}; this says 117 and the cell must refuse.
     */
    @Test public void the_reader_contract_is_not_vacuous() throws Exception {
        XFixtureManifest.V2 m = doctored(t -> t.replace(
                "recid\twal3-java-cleaned\tA\t1\tlive\t116\t120\n",
                "recid\twal3-java-cleaned\tA\t1\tlive\t117\t120\n"));
        File session = tempDir("xfcorpus-recid");
        XFixtureV2Executor.gunzipAll(m, ROOT, session);
        XFixtureV2Executor x = new XFixtureV2Executor(m, XFixtureV2Executor.Rules.sealedRoot(),
                session);
        AssertionError caught = null;
        try {
            x.runCell(javaCell(m, "wal3-java-cleaned", "rw"), tempDir("xfcorpus-recidcell"));
        } catch (AssertionError err) {
            caught = err;
        }
        assertTrue("record A's content was compared against nothing", caught != null);
        assertTrue("it failed for another reason: " + caught.getMessage(),
                caught.getMessage().contains("recid=1"));
    }

    private static XFixtureManifest.V2.Expect javaCell(XFixtureManifest.V2 m, String fid,
                                                       String mode) {
        for (XFixtureManifest.V2.Expect e : m.expects)
            if (fid.equals(e.fixtureId) && "java".equals(e.engine) && mode.equals(e.mode)) return e;
        fail("no java " + mode + " cell for " + fid);
        return null;
    }

    // ------------------------------------------------------------- the family predicate

    /**
     * The {@code reopen} family predicate must DISCRIMINATE, which the corpus alone cannot show.
     *
     * <p>Exactly one {@code reopen} row exists and its family is {@code S2}, so a predicate that
     * accepted any corruption at all would pass every cell — lesson (g), a comparison can only see
     * the variation its inputs contain. S9 is the immediate neighbour: the very next branch of the
     * same scan, the same exception class, a different rule. It must not match.
     */
    @Test public void the_reopen_family_predicate_discriminates() {
        XFixtureV2Executor.assertFamily("S2 control", "S2", new DBException.DataCorruption(
                "WAL segment x.wal.4: section LSN -9223372036854775808 at offset 187 does not "
                        + "follow 9223372036854775807"));

        refusedFamily("S9's refusal, which is the next branch of the same scan", "S2",
                new DBException.DataCorruption(
                        "section LSNs must be consecutive: 12 follows 9"));
        refusedFamily("a corruption verdict from another rule entirely", "S2",
                new DBException.DataCorruption("WAL file x.wal is not a v3 segment"));
        refusedFamily("an operational failure wearing the right words", "S2",
                new DBException("section LSN 1 at offset 2 does not follow 0"));
        refusedFamily("a family this engine has no predicate for", "R4-floor",
                new DBException.DataCorruption("anything at all"));
    }

    private static void refusedFamily(String what, String family, Throwable t) {
        try {
            XFixtureV2Executor.assertFamily("probe", family, t);
        } catch (AssertionError expected) {
            return;
        }
        fail("the family predicate accepted " + what);
    }

    // ------------------------------------------------------------- the accountant

    /**
     * The consumption accountant, unit-tested.
     *
     * <p>It is the only thing that makes "executes" distinguishable from "parses and drops" for the
     * {@code bytes} and {@code reopen} rows: the whole-file {@code post} hash subsumes a
     * byte-at-offset assertion, and nothing at all observes a dropped {@code reopen}. Deleting the
     * reopen call in the executor is red only because of this.
     */
    @Test public void an_unconsumed_oracle_row_is_a_failure() {
        Object a = new Object(), b = new Object();
        XFixtureV2Executor.Consumption ok = new XFixtureV2Executor.Consumption("ctx");
        ok.owe("action x", a);
        ok.owe("reopen S2", b);
        ok.consume("action x", a);
        ok.consume("reopen S2", b);
        ok.requireAllConsumed();

        XFixtureV2Executor.Consumption dropped = new XFixtureV2Executor.Consumption("ctx");
        dropped.owe("action x", a);
        dropped.owe("reopen S2", b);
        dropped.consume("action x", a);
        refuses("a row no handler consumed", dropped::requireAllConsumed);

        XFixtureV2Executor.Consumption twice = new XFixtureV2Executor.Consumption("ctx");
        twice.owe("action x", a);
        twice.consume("action x", a);
        refuses("the same row consumed twice", () -> twice.consume("action x", a));

        XFixtureV2Executor.Consumption never = new XFixtureV2Executor.Consumption("ctx");
        refuses("a row consumed that was never owed", () -> never.consume("action x", a));

        XFixtureV2Executor.Consumption other = new XFixtureV2Executor.Consumption("ctx");
        other.owe("action x", a);
        refuses("the key consumed with a different row object", () -> other.consume("action x", b));
    }

    private static void refuses(String what, Runnable r) {
        try {
            r.run();
        } catch (AssertionError expected) {
            return;
        }
        fail("the accountant accepted " + what);
    }

    // ------------------------------------------------------------- the root itself

    /**
     * The corpus root holds {@code MANIFEST.tsv} plus one blob per {@code file} row and nothing
     * else (C5 plan §4c).
     *
     * <p>No golden tables and no post blobs: {@code GOLDEN-DECODE.tsv} and {@code GOLDEN-BODY.tsv}
     * belong to the static sample and stay there, and post-state blobs are not distributed to any
     * engine — java produces its own by running the cell.
     */
    @Test public void the_corpus_root_has_nothing_unexplained() throws Exception {
        XFixtureManifest.V2 m = manifest();
        TreeSet<String> expected = new TreeSet<>(List.of("MANIFEST.tsv"));
        for (XFixtureManifest.V2.FileRow f : m.files) expected.add(f.blobName());
        assertEquals(ROOT + " holds files no `file` row accounts for (or is missing one)",
                expected, XFixtureV2Executor.listNames(XFixtureV2Executor.rootDir(ROOT)));
    }

    /**
     * This root is byte-identical to todo's sealed tree.
     *
     * <p>Re-derives {@code freeze_v2.dist_seal(files, "java")} — todo's own preimage grammar,
     * restricted to the {@code root}-marked files an engine actually receives — from what is on the
     * classpath, and compares it to the constant. The file SET comes from the directory listing
     * rather than from {@code MANIFEST.tsv}, so this and
     * {@link #the_corpus_root_has_nothing_unexplained} do not consult the same source: a blob added
     * to the tree moves the seal even if no row mentions it.
     *
     * <p>What it does not certify, stated because the whole-artifact seal does certify it:
     * provenance. The four repo commits and {@code sync_v2.py}'s digest are in
     * {@code PREFLIGHT_SEAL}'s preimage and not in this one — they are not properties of the
     * distributed bytes and this repository has no way to check them.
     */
    @Test public void the_corpus_root_matches_todos_sealed_tree() throws Exception {
        File dir = XFixtureV2Executor.rootDir(ROOT);
        StringBuilder pre = new StringBuilder("mapdb-xfixtures-dist\tv1\nengine\tjava\n");
        TreeSet<String> names = XFixtureV2Executor.listNames(dir);
        assertTrue(ROOT + " is empty", !names.isEmpty());
        for (String n : names) {
            byte[] b = Files.readAllBytes(new File(dir, n).toPath());
            pre.append("file\t").append(n).append('\t').append(b.length).append('\t')
                    .append(FixtureWriter.sha256Hex(b)).append("\troot\n");
        }
        assertEquals("this root is not todo/store-cross/preflight-v2/'s `root` slice. Regenerate "
                        + "with `freeze_v2.py --dist-seals --preflight`, and copy the TREE too — a "
                        + "constant updated alone certifies whatever is here",
                DIST_SEAL,
                FixtureWriter.sha256Hex(pre.toString().getBytes(StandardCharsets.UTF_8)));
    }
}
