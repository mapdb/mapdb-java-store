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
 * <p><b>What the seal does NOT buy.</b> It proves copy fidelity — that these are todo's bytes —
 * and nothing about whether the manifest expresses enough semantics. The first draft of this slice
 * used it to justify dropping the "an accept cell must assert something" guard for this root, and
 * both reviewers refused: one stripped {@code wal3-java-cleaned}'s recid rows and watched an
 * assert-nothing accept cell pass. The guard is back, as the disjunction plan §5.3 item 5 asked
 * for, and it is now the SAME rule for both roots ({@link XFixtureV2Executor#requireSomeOracle}).
 *
 * <h2>The mutation campaign</h2>
 * A set of NAMED cases, in {@code scratchpad/mut.py} + {@code mutants.sh}. Each case mutates one
 * named site — by deleting it, replacing it, or moving it — or a named combination of sites, and
 * the suite must then go red for the reason that case names. Every case kills; the runner exits
 * non-zero if any survives, mis-kills, or fails to apply, so the count and the result are read from
 * a run rather than asserted here. Three attempts to state them in prose were wrong three different
 * ways.
 *
 * <p><b>It is a named campaign, not an exhaustive sweep,</b> and the difference is not pedantry —
 * an earlier version of this paragraph claimed the sweep and round 4 disproved it by deleting
 * statements in this class that nothing red-flags. What is true is narrower: every check the
 * campaign names has a red that names it.
 *
 * <p><b>Most of those checks are green when deleted unless something supplies an input.</b>
 * They are closed with DOCTORED manifests
 * routed through the PRODUCTION path — never by calling the check directly, a mistake this slice
 * made twice, which proves the method and leaves its call unobserved. Where a check's red is
 * unreachable from any conforming corpus it gets a direct firing probe instead
 * ({@link #the_read_only_write_probe_fires}, {@link #the_reopen_family_predicate_discriminates}).
 *
 * <p><b>The residue is the leaf problem</b>: a statement no other statement depends on is
 * invisible to deletion, and the last assertion in any chain is one. It is pushed DOWN rather than
 * eliminated, by collecting outcomes and comparing them once — the cells probed
 * ({@link XFixtureV2Executor#readOnlyHandlesProbed}), that probe's own inputs
 * ({@link #the_read_only_write_probe_fires}), the two openers' verdicts and file sets
 * ({@link #a_direct_cell_sent_to_the_wal_opener_goes_red}) — so <b>one comparison per group</b> is
 * unobserved rather than every statement in it. Reviewers measured each group as it was collapsed:
 * deleting a group's comparison is suite-green, and where two groups guard each other a mutant
 * over one catches the other's loss. {@link #the_reader_contract_is_not_vacuous} is the one check
 * that proves FIRING rather than deletion, and says so.
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
            "3c0442fb100cdcbbafbccb753420950a04c3ee28a9efbe51f00a8bffa7102415";

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
        runEveryJavaCell(manifest());
    }

    /**
     * The whole suite-level flow: run every java cell, then apply every rule that is about the SET
     * of cells rather than about one of them.
     *
     * <p>Extracted so the doctored cases below go through <b>this</b> code rather than calling the
     * rules directly. That distinction is the entire finding both C5j reviewers made, and the first
     * repair round reproduced it: a test that calls {@code requireEveryOracleRowAddressesARunCell}
     * itself proves the METHOD and leaves its CALL unobserved, so deleting the call from the suite
     * stayed green. Every doctored manifest now enters here.
     */
    private void runEveryJavaCell(XFixtureManifest.V2 m) throws IOException {
        File session = tempDir("xfcorpus-session");
        XFixtureV2Executor.gunzipAll(m, ROOT, session);
        XFixtureV2Executor x = new XFixtureV2Executor(m, session);

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

        // The other half of contract §2.3's consumption rule. Per-cell accounting owes only the
        // rows addressed to the cell being run, so a row addressed to a cell that does not exist
        // is owed by nobody — both reviewers proved that independently and both suites stayed
        // green with the oracle dropped.
        x.requireEveryOracleRowAddressesARunCell(ran);

        // …and the ro write probe really ran on every ro cell. Deleting the call inside the
        // executor leaves this set empty, which is the red that call did not have.
        TreeSet<String> roCells = new TreeSet<>();
        for (XFixtureManifest.V2.Expect e : m.expects)
            if ("java".equals(e.engine) && "ro".equals(e.mode) && "accept".equals(e.verdict))
                roCells.add(e.fixtureId + "/" + e.mode);
        assertTrue("the corpus has no java ro accept cell, so the read-only probe has no input",
                !roCells.isEmpty());
        assertEquals("the ro cells whose read-only handle was probed with a write",
                roCells, x.readOnlyHandlesProbed);

        // The oracle rows exist and are addressed to cells that ran. Without this the whole C5j
        // execution path could be absent from the corpus and every assertion above would still
        // pass — the shape §7 of the C5 plan exists to stop, and the one both round-3 reviewers
        // found revision 3 had shipped for rust and zig.
        // Counted over the rows addressed to cells that RAN, not over every java-addressed row.
        // The difference is what stops these pins from masking the addressing check above: an
        // orphan row added to the manifest would otherwise move a count to 2 and fire HERE, so
        // every doctored addressing case would report killed while proving a different rule
        // (lesson h — an input that trips several checks measures the first).
        int actions = 0, bytes = 0, reopens = 0;
        for (XFixtureManifest.V2.Action a : m.actions)
            if ("java".equals(a.engine) && ran.contains(a.fixtureId + "/" + a.mode)) actions++;
        for (XFixtureManifest.V2.Bytes b : m.bytes)
            if ("java".equals(b.engine) && ran.contains(b.fixtureId + "/" + b.mode)) bytes++;
        for (XFixtureManifest.V2.Reopen r : m.reopens)
            if ("java".equals(r.engine) && ran.contains(r.fixtureId + "/" + r.mode)) reopens++;
        assertEquals("the corpus carries no `action` row for java, so this engine's action "
                + "executor has no input at all", 1, actions);
        assertEquals("the corpus carries no `bytes` row for java", 1, bytes);
        // TWO since C5t: Q8's `S2` row after the action, plus the `direct-magic` row plan §3.12
        // derives for `reject-wal3-segment-at-direct` — the first reopen row this engine runs on a
        // REJECT cell, where it grades the refusal's family and that the refusal is STABLE.
        assertEquals("the corpus carries no `reopen` row for java", 2, reopens);
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
        XFixtureV2Executor x = new XFixtureV2Executor(m, session);

        XFixtureManifest.V2.Expect direct = null;
        for (XFixtureManifest.V2.Expect e : m.expects)
            if ("java".equals(e.engine) && "direct".equals(e.opener)) direct = e;
        assertTrue("the corpus has no java `direct` cell, so this mutant grades nothing",
                direct != null);

        // ONE comparison for all four facts this mutant rests on: the control's verdict and file
        // set, and the misrouted open's. They are independent — a future java that refused D1 would
        // keep the file sets and lose the verdicts — but each assertion written separately is a
        // leaf the whole gate can lose without noticing, and rounds 8 and 9 measured three of them
        // here one at a time. Collapsing them leaves one leaf for the group instead of four.
        final XFixtureManifest.V2.Expect cellRow = direct;
        File cell = tempDir("xfcorpusmutant");
        String control = outcomeOf(() -> x.runCell(cellRow, cell, XFixtureV2Executor.Dispatch.BY_MANIFEST))
                + " " + XFixtureV2Executor.listNames(cell);
        File cell2 = tempDir("xfcorpusmutant2");
        String misrouted = outcomeOf(() -> x.runCell(cellRow, cell2, XFixtureV2Executor.Dispatch.ALWAYS_WAL3))
                + " " + XFixtureV2Executor.listNames(cell2);
        assertEquals("the direct cell through each opener: verdict, then what it left behind",
                List.of("PASSED [x, x.lock]",
                        "RED(but the store opened) [x, x.lock, x.wal.0000000000000001]"),
                List.of(control, misrouted));
    }

    /** {@code PASSED}, or {@code RED(<the phrase the caller is entitled to>)} for a refusal. */
    private static String outcomeOf(RunsCell r) {
        try {
            r.run();
            return "PASSED";
        } catch (AssertionError e) {
            String m = String.valueOf(e.getMessage());
            return "RED(" + (m.contains("but the store opened") ? "but the store opened" : m) + ")";
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private interface RunsCell { void run() throws IOException; }

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

    /**
     * The read-only probe's assertion FIRES — which no corpus input can show.
     *
     * <p>A conforming engine refuses the write, so the red side is unreachable from the corpus and
     * the assertion could be deleted with the whole gate green while
     * {@link XFixtureV2Executor#readOnlyHandlesProbed} still attested the probe "ran". So the method
     * is handed the two inputs the corpus cannot produce: a WRITABLE handle, and a handle that
     * refuses for the wrong reason. This is the treatment {@code assertFamily} already gets.
     *
     * <p><b>The reds are COLLECTED and compared as an ordered list, not asserted one at a
     * time.</b> Round 4 is why: both reviewers deleted an individual probe call — and the {@code isEmpty} assertions
     * beside them — and watched the gate stay green at 2,590 tests, because nothing observed that a
     * test arm was still there. A statement that no other statement depends on is invisible to
     * deletion, however many assertions it contains. Comparing the collected outcomes makes each
     * input observable: drop either call and the list is short.
     */
    @Test public void the_read_only_write_probe_fires() throws Exception {
        XFixtureManifest.V2 m = manifest();
        File session = tempDir("xfcorpus-rofire");
        XFixtureV2Executor.gunzipAll(m, ROOT, session);
        XFixtureV2Executor x = new XFixtureV2Executor(m, session);
        XFixtureManifest.V2.Expect ro = javaCell(m, "wal3-java-cleaned", "ro");
        List<String> reds = new ArrayList<>();

        org.mapdb.store.StoreWAL w = new org.mapdb.store.StoreWAL(
                new File(stage(m, "wal3-java-cleaned", session, tempDir("xfrofire-rw")), "x"));
        try {
            reds.add(redOf(() -> x.assertWriteRefused("probe", ro, w)));
        } finally {
            w.close();
        }
        // A handle that refuses for a DIFFERENT reason: closed, and not read-only, so the refusal
        // is `StoreClosed` and its message cannot name the mode.
        org.mapdb.store.StoreWAL closed = new org.mapdb.store.StoreWAL(
                new File(stage(m, "wal3-java-cleaned", session, tempDir("xfrofire-closed")), "x"));
        closed.close();
        reds.add(redOf(() -> x.assertWriteRefused("probe", ro, closed)));

        assertEquals("the probe's two inputs and the red each must produce",
                List.of("ACCEPTED", "WRONG-REASON"), reds);
        assertTrue("the probe recorded a cell it had just refused — the recording must stay "
                + "downstream of the assertion", x.readOnlyHandlesProbed.isEmpty());
    }

    /**
     * Classifies the red one probe input produced, so the two can be compared as an ORDERED list —
     * the order is what binds {@code ACCEPTED} to the writable input and {@code WRONG-REASON} to the
     * closed one, which a set would lose.
     *
     * <p>Classification is by substring, so an unrelated {@code AssertionError} carrying either
     * phrase would be labelled as the wanted red. Sound for the two inputs here — both lambdas call
     * only {@code assertWriteRefused}, and the store's own failures are {@code DBException}s caught
     * inside it — and stated rather than left implied, because it is a provenance claim about the
     * message and not about where it came from.
     */
    private static String redOf(Runnable r) {
        try {
            r.run();
            return "NO-RED";
        } catch (AssertionError e) {
            String msg = String.valueOf(e.getMessage());
            if (msg.contains("the write was ACCEPTED")) return "ACCEPTED";
            if (msg.contains("refused with:")) return "WRONG-REASON";
            return "OTHER: " + msg;
        }
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
        XFixtureV2Executor x = new XFixtureV2Executor(m, session);
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
        XFixtureV2Executor x = new XFixtureV2Executor(m, session);
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
        XFixtureV2Executor x = new XFixtureV2Executor(m, session);
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

    /**
     * An oracle row addressed to a cell this engine never runs must fail the suite.
     *
     * <p>Both C5j reviewers found this independently and both proved it: codex moved Q8's
     * {@code bytes} row to {@code reject-wal3-d1-barebase} (a real fixture whose java cells are in
     * {@code EXPECT_EXCEPTIONS}) and fable moved the {@code reopen} row to the direct cell's absent
     * {@code ro} mode. Per-cell accounting is blind to both, because it owes only the rows addressed
     * to the cell being run. All four addressed row types get their own doctored input, because the
     * check is one loop per type and round 2 deleted two of them green when only the {@code bytes}
     * shape had a red — including the exact shape one reviewer had filed in round 1.
     */
    @Test public void an_oracle_row_addressed_to_an_absent_cell_fails_the_suite() throws Exception {
        // One doctored input per ROW TYPE, because the check is one loop per row type and round 2
        // deleted two of them green when only the `bytes` shape had a red. Each case ADDS an orphan
        // row rather than moving a real one: moving the action away leaves the cell 186 bytes and
        // the `bytes` range check fires first, which would report KILLED while proving a different
        // rule (lesson h). The expected message names the row type, so no case can pass on another
        // loop's refusal.
        String absent = "reject-wal3-d1-barebase";   // a real fixture with no java cell at all
        refusesSuite("a bytes row addressed to a cell java never runs",
                doctored(t -> t + "bytes\t" + absent + "\tjava\trw\tx\t0\tab\n"),
                "bytes " + absent + "/rw");
        refusesSuite("a reopen row addressed to a cell java never runs",
                doctored(t -> t + "reopen\t" + absent + "\tjava\tro\tS2\n"),
                "reopen " + absent + "/ro");
        refusesSuite("an action row addressed to a cell java never runs",
                doctored(t -> t + "action\t" + absent + "\tjava\trw\tcommit_one_record"
                        + "\top=put,payload_id=1,payload_len=1,recid_label=Z,serializer=raw\n"),
                "action " + absent + "/rw");
        // …and `post`, the fourth addressed row type — named by contract §2.3 since round 2, and
        // droppable in silence on both sides of the fence before it was.
        refusesSuite("a post row addressed to a cell java never runs",
                doctored(t -> t + "post\t" + absent + "\tjava\tro\tz.lock\tunchanged\n"),
                "post " + absent + "/ro");
    }

    /**
     * The {@code bytes} row's VALUE is compared, not merely computed.
     *
     * <p>Codex deleted the equality and watched the whole gate pass: the handler read the range,
     * consumed the row and compared it to nothing, because the whole-file post hash covers the same
     * bytes and masks the deletion. The row's expected value is doctored to the same LENGTH and a
     * different value, so the post hash cannot fire and only the equality can.
     */
    @Test public void the_bytes_rows_value_is_compared() throws Exception {
        XFixtureManifest.V2 m = doctored(t -> t.replace("\t187\t8000000000000000",
                "\t187\t0000000000000000"));
        File session = tempDir("xfcorpus-hex");
        XFixtureV2Executor.gunzipAll(m, ROOT, session);
        AssertionError caught = null;
        try {
            new XFixtureV2Executor(m, session).runCell(
                    javaCell(m, "div-wal3-lsn-exhausted", "rw"), tempDir("xfcorpus-hexcell"));
        } catch (AssertionError err) {
            caught = err;
        }
        assertTrue("the asserted bytes were read and compared to nothing", caught != null);
        assertTrue("it failed for another reason: " + caught.getMessage(),
                caught.getMessage().contains("the asserted bytes"));
    }

    /**
     * The {@code reopen} row's FAMILY is graded, not merely the fact that something was thrown.
     *
     * <p>Codex deleted the {@code assertFamily} call and watched the gate pass — the reopen still
     * had to fail, the row was still consumed, and the manifest's family was ignored. Doctoring the
     * family to one this engine has no predicate for is the input that call never had: the reopen
     * fails as S2 either way, so only a graded family can tell the two manifests apart.
     */
    @Test public void the_reopen_rows_family_is_graded() throws Exception {
        XFixtureManifest.V2 m = doctored(t -> t.replace(
                "reopen\tdiv-wal3-lsn-exhausted\tjava\trw\tS2",
                "reopen\tdiv-wal3-lsn-exhausted\tjava\trw\tR4-floor"));
        File session = tempDir("xfcorpus-fam");
        XFixtureV2Executor.gunzipAll(m, ROOT, session);
        AssertionError caught = null;
        try {
            new XFixtureV2Executor(m, session).runCell(
                    javaCell(m, "div-wal3-lsn-exhausted", "rw"), tempDir("xfcorpus-famcell"));
        } catch (AssertionError err) {
            caught = err;
        }
        assertTrue("the reopen was graded as 'it threw something'", caught != null);
        assertTrue("it failed for another reason: " + caught.getMessage(),
                caught.getMessage().contains("has no predicate in this engine"));
    }

    /**
     * On a REJECT cell the family is graded on the cell's OWN refusal, before the reopen.
     *
     * <p>codex round 1 finding 2: C5t's first draft graded the family only on the reopen, which is a
     * WRITABLE open whatever the cell's mode was. Every {@code mode=ro} row was therefore graded by
     * a retry in the other mode, and a store that refuses read-only for one reason and writable for
     * another passed.
     *
     * <p><b>The corpus alone cannot show the fix.</b> Both reference openers refuse the same way
     * twice, so deleting the first grading leaves the reopen's — same family, same predicate, gate
     * green. What separates them is WHERE the red comes from: this doctored family reds at
     * {@code family[...]} if the cell's own refusal was graded and at {@code reopen[...]} if only
     * the second open was. Asserting the prefix is what makes the deletion visible.
     */
    @Test public void the_reject_arms_own_refusal_is_graded() throws Exception {
        XFixtureManifest.V2 m = doctored(t -> t.replace(
                "reopen\treject-wal3-segment-at-direct\tjava\trw\tdirect-magic",
                "reopen\treject-wal3-segment-at-direct\tjava\trw\tR4-floor"));
        File session = tempDir("xfcorpus-first");
        XFixtureV2Executor.gunzipAll(m, ROOT, session);
        AssertionError caught = null;
        try {
            new XFixtureV2Executor(m, session).runCell(
                    javaCell(m, "reject-wal3-segment-at-direct", "rw"), tempDir("xfcorpus-fcell"));
        } catch (AssertionError err) {
            caught = err;
        }
        assertTrue("the reject arm's own refusal was graded by nothing", caught != null);
        assertTrue("it failed for another reason: " + caught.getMessage(),
                caught.getMessage().contains("has no predicate in this engine"));
        assertTrue("the family was graded on the REOPEN, not on the cell's own refusal: "
                + caught.getMessage(), caught.getMessage().contains("family[R4-floor]"));
    }

    /**
     * An accept cell that asserts nothing must be refused — the C3j guard, restored.
     *
     * <p>The first draft of this slice deleted it for the sealed root and called the distribution
     * seal its replacement. Both reviewers refused that and proved it with this exact input: strip
     * {@code wal3-java-cleaned}'s six recid rows and the cell passes on nothing but the universal
     * {@code x.lock} post row. The seal proves copy fidelity; assertion adequacy is a different
     * property and artifact identity cannot buy it.
     */
    @Test public void an_accept_cell_that_asserts_nothing_is_refused() throws Exception {
        XFixtureManifest.V2 m = doctored(t -> {
            StringBuilder out = new StringBuilder();
            for (String line : t.split("\n", -1)) {
                if (line.startsWith("recid\twal3-java-cleaned\t")) continue;
                out.append(line).append('\n');
            }
            return out.substring(0, out.length() - 1);
        });
        File session = tempDir("xfcorpus-bare");
        XFixtureV2Executor.gunzipAll(m, ROOT, session);
        AssertionError caught = null;
        try {
            new XFixtureV2Executor(m, session).runCell(
                    javaCell(m, "wal3-java-cleaned", "rw"), tempDir("xfcorpus-barecell"));
        } catch (AssertionError err) {
            caught = err;
        }
        assertTrue("a writable accept cell with no oracle at all passed", caught != null);
        assertTrue("it failed for another reason: " + caught.getMessage(),
                caught.getMessage().contains("asserts nothing about the store it opened"));

        // …and the disjunction is not vacuous in the other direction: the SAME stripped fixture
        // passes in `ro`, where the read-only write refusal is the claim. Without this, "an accept
        // cell must assert something" and "ro is exempt" would be indistinguishable.
        new XFixtureV2Executor(m, session).runCell(
                javaCell(m, "wal3-java-cleaned", "ro"), tempDir("xfcorpus-barecell-ro"));
    }

    /**
     * An {@code applies} row that goes missing while its {@code expect} row stays must fail.
     *
     * <p>Codex deleted BOTH set equalities in {@link #corpus_cells_conform} and the suite stayed
     * green — together they are the whole of plan §5.3 item 6, and neither had an input. The two
     * row types come from one catalogue and agree by construction, so only a doctored manifest can
     * separate them.
     */
    @Test public void an_applies_row_missing_its_expect_fails() throws Exception {
        XFixtureManifest.V2 m = doctored(t ->
                t.replace("applies\twal3-java-cleaned\tjava\tro\n", ""));
        boolean stillExpected = false;
        for (XFixtureManifest.V2.Expect e : m.expects)
            if ("wal3-java-cleaned".equals(e.fixtureId) && "java".equals(e.engine)
                    && "ro".equals(e.mode)) stillExpected = true;
        assertTrue("the expect row must survive, or this proves only that a row was deleted",
                stillExpected);
        refusesSuite("an applies row deleted while its expect row stays", m,
                "different sets");
    }

    /** Runs the whole java suite over a doctored manifest and requires it to refuse, by reason. */
    private void refusesSuite(String what, XFixtureManifest.V2 m, String because) throws IOException {
        AssertionError caught = null;
        try {
            runEveryJavaCell(m);
        } catch (AssertionError err) {
            caught = err;
        }
        assertTrue("the suite accepted " + what, caught != null);
        assertTrue("it failed for another reason: " + caught.getMessage(),
                caught.getMessage() != null && caught.getMessage().contains(because));
    }

    /**
     * The oracle count pins fire when the corpus stops feeding an executor.
     *
     * <p>They are C5 plan §7's defence against "an execution path absent from the corpus with every
     * assertion still passing" — the defect C5s's round 3 shipped for two engines — and round 3
     * found all three deletable with the gate green and no mutant naming them.
     *
     * <p><b>The `action` pin carried an "unreachable red" label in round 3 and both round-4
     * reviewers falsified it with the same input.</b> The label reasoned cell by cell — remove the
     * action row and the CELL dies at the bytes range check, and every deeper stripping dies at the
     * post row, the reopen or the oracle guard in turn. All true, and all beside the point: strip
     * the WHOLE CELL and every cell-level death is escaped. That is also the pin's own stated
     * threat, an execution path vanishing from a regenerated corpus — so the label was not just
     * wrong, it was wrong about the case the pin exists for.
     */
    @Test public void the_oracle_count_pins_fire() throws Exception {
        refusesSuite("a corpus with no java `bytes` row",
                doctored(t -> dropRows(t, "bytes\tdiv-wal3-lsn-exhausted\tjava\trw\t")),
                "carries no `bytes` row for java");
        refusesSuite("a corpus with no java `reopen` row",
                doctored(t -> dropRows(t, "reopen\tdiv-wal3-lsn-exhausted\tjava\trw\t")),
                "carries no `reopen` row for java");
        // Q8's rw cell removed COHERENTLY — its applies, expect, both posts and all three oracle
        // rows — so `applies == expect` still holds, no row is orphaned, the other four java cells
        // run clean, and the action pin is the first thing left that can notice.
        refusesSuite("a corpus with no java `action` row",
                doctored(t -> {
                    String out = t;
                    for (String pfx : new String[] {
                            "applies\tdiv-wal3-lsn-exhausted\tjava\trw",
                            "expect\tdiv-wal3-lsn-exhausted\tjava\trw\t",
                            "post\tdiv-wal3-lsn-exhausted\tjava\trw\t",
                            "action\tdiv-wal3-lsn-exhausted\tjava\trw\t",
                            "bytes\tdiv-wal3-lsn-exhausted\tjava\trw\t",
                            "reopen\tdiv-wal3-lsn-exhausted\tjava\trw\t"})
                        out = dropRows(out, pfx);
                    return out;
                }),
                "carries no `action` row for java");
    }

    /**
     * A {@code post} row addressed to a cell that RUNS is consumed and graded — the other half of
     * codex's round-3 finding 1, and the {@code unchanged} verb's first input.
     *
     * <p>Round 2 gave {@code post} a suite-wide addressing check and no per-cell debt, so a handler
     * that skipped a row addressed to a real cell was masked: the two-sided unnamed-input rule
     * independently re-verifies the same file, and "parses and drops" reported green. The per-cell
     * accountant now owes every post row.
     *
     * <p>Both directions, because a verb that always holds is not a check: the same {@code unchanged}
     * row is true of a segment the cell leaves alone and false of the one Q8's action grows.
     */
    @Test public void an_unchanged_post_row_is_graded() throws Exception {
        String cell = "post\tdiv-wal3-lsn-exhausted\tjava\trw\t";
        // TRUE: the low segment is not touched by the commit.
        runEveryJavaCell(doctored(t -> t + cell + "x.wal.0000000000000002\tunchanged\n"));
        // FALSE: the active segment is exactly what the action appends to. Its real row is
        // `modified:279:…`, so this REPLACES it — adding a second row for one file is a duplicate
        // the parser refuses, which would prove the parser rather than the verb.
        refusesSuite("an `unchanged` row over the segment the action grew",
                doctored(t -> t.replaceAll(
                        "post\tdiv-wal3-lsn-exhausted\tjava\trw\tx\\.wal\\.0000000000000004\tmodified:[^\n]*",
                        "post\tdiv-wal3-lsn-exhausted\tjava\trw\tx.wal.0000000000000004\tunchanged")),
                "bytes changed");
    }

    private static String dropRows(String text, String prefix) {
        StringBuilder out = new StringBuilder();
        for (String line : text.split("\n", -1)) {
            if (line.startsWith(prefix)) continue;
            out.append(line).append('\n');
        }
        return out.substring(0, out.length() - 1);
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
     * <p>A predicate that accepted any corruption at all would pass every cell — lesson (g), a
     * comparison can only see the variation its inputs contain. S9 is the immediate neighbour of
     * S2: the very next branch of the same scan, the same exception class, a different rule. It
     * must not match.
     *
     * <p><b>C5t added the second family and with it the pairing</b> (plan §3.12). Until then this
     * engine had exactly one {@code reopen} row, so "the family is read from the row" and "every
     * family means DataCorruption" were indistinguishable here. {@code direct-magic} is now the
     * other one, and the four cases at the end take both families against both refusals: the
     * corpus varies in neither direction, so a predicate that swapped them would pass every cell
     * with only these to notice.
     */
    @Test public void the_reopen_family_predicate_discriminates() {
        XFixtureV2Executor.assertFamily("S2 control", "S2", new DBException.DataCorruption(
                "WAL segment x.wal.4: section LSN -9223372036854775808 at offset 187 does not "
                        + "follow 9223372036854775807"));

        refusedFamily("S9's refusal, which is the next branch of the same scan", "S2",
                new DBException.DataCorruption("WAL segment x.wal.4: section LSNs must be "
                        + "consecutive: 12 at offset 187 after 9"));
        refusedFamily("a corruption verdict from another rule entirely", "S2",
                new DBException.DataCorruption("WAL file x.wal is not a v3 segment"));
        // The WHOLE S2 message, on a non-corruption class — so the class predicate is the only
        // thing that can refuse it. Round 3 caught the earlier form omitting the
        // "WAL segment <name>: " prefix, which meant the message predicate rejected it and the
        // class predicate could be deleted with the gate green: one check masking another on the
        // same input, which is lesson (h) inside a discrimination battery.
        refusedFamily("an operational failure wearing the right words", "S2",
                new DBException("WAL segment x.wal.4: section LSN 1 at offset 2 "
                        + "does not follow 0"));
        refusedFamily("a family this engine has no predicate for", "R4-floor",
                new DBException.DataCorruption("anything at all"));
        // The pattern matches WHOLE. The first draft used find() on an unanchored fragment while
        // its comment claimed anchoring, and codex demonstrated the gap with exactly this shape.
        refusedFamily("the S2 wording embedded in an unrelated message", "S2",
                new DBException.DataCorruption("prefix: WAL segment x: section LSN 1 at offset 2 "
                        + "does not follow 0; suffix"));

        // C5t's second family, and the pair that makes the family READING falsifiable.
        Throwable magic = new DBException.DataCorruption("not a mapdb StoreDirect file (bad magic)");
        Throwable s2 = new DBException.DataCorruption("WAL segment x.wal.4: section LSN 1 at "
                + "offset 2 does not follow 0");
        XFixtureV2Executor.assertFamily("direct-magic control", "direct-magic", magic);
        refusedFamily("an S2 corruption verdict presented as direct-magic", "direct-magic", s2);
        refusedFamily("StoreDirect's bad magic presented as S2", "S2", magic);
        // The neighbour INSIDE StoreDirect.initOpen: the length check one line above the magic
        // one. `reject-wal3-segment-at-direct` is 1,200,509 bytes so it never trips that branch,
        // and a predicate that accepted it would be saying "the direct opener refused somehow".
        refusedFamily("the direct opener's OTHER structural refusal", "direct-magic",
                new DBException.DataCorruption("store file smaller than the header page"));
        refusedFamily("an operational failure wearing the magic words", "direct-magic",
                new DBException("not a mapdb StoreDirect file (bad magic)"));
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
     * <p>It is the only thing that makes "executes" distinguishable from "parses and drops" for
     * three of the four addressed row types — every one except {@code action}, which has a failure
     * of its own. The whole-file {@code post} hash subsumes a byte-at-offset assertion, the
     * two-sided unnamed-input rule silently re-verifies a file whose {@code unchanged} row was
     * dropped, and nothing at all observes a dropped {@code reopen}.
     *
     * <p><b>Not "only because of this".</b> An earlier version of this sentence said the accountant
     * was the sole red for all three, and a reviewer disproved it by mutation: delete the reopen
     * call AND the accountant and the suite is still red, at
     * {@link #the_reopen_rows_family_is_graded}, because that doctored case was added later. The
     * accountant is what made the drop visible when there was nothing else; it is no longer alone,
     * and saying otherwise is a claim about the rest of the suite that nothing maintains.
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
        fail("accepted " + what);
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
