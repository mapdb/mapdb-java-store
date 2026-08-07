package org.mapdb.xfixtures;

import org.mapdb.DBException;
import org.mapdb.store.Store;
import org.mapdb.store.StoreDirect;
import org.mapdb.store.StoreWAL;
import org.mapdb.store.Wal3Actions;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.regex.Pattern;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Runs one schema-v2 cell against this engine — the single executor for BOTH v2 roots.
 *
 * <p><b>Why one class and not two.</b> {@code /xfixtures-v2/} is the static {@code v2-core} sample
 * and {@code /xfixtures-v2-corpus/} is the {@code v2-oracle} preflight root; they differ in which
 * rows they carry, not in what a cell means. Two executors would be two implementations of the
 * post-state rule, the opener dispatch and the reader contract, and this workstream has already
 * shipped the consequence twice — a fix applied to one of two copies is a fix that did not happen
 * (C2j's B-finding). And there is now NO per-root knob at all: the first draft gave the corpus a
 * relaxed accept rule that the sample did not get, and both reviewers showed the relaxation was a
 * deletion with a rationalisation attached. {@link #requireSomeOracle} is the disjunction plan
 * §5.3 item 5 asked for, it admits every cell either root has, and it is the same rule for both.
 *
 * <h2>The cell, in order</h2>
 * <ol>
 *   <li>copy every {@code file} row of the fixture into a private directory;</li>
 *   <li>open through the opener the {@code expect} row names, in the mode it names;</li>
 *   <li>for an {@code accept} cell: run the {@code action} rows through {@link Wal3Actions},
 *       assert the reader contract if the fixture carries {@code recid} rows, and — in
 *       {@code ro} — assert that a write is refused;</li>
 *   <li>close, and <b>capture</b> the directory: every assertion after this point reads the
 *       capture, never the disk, because step 6 opens the store again;</li>
 *   <li>grade the {@code bytes} rows against the capture, then the {@code post} rows and the
 *       two-sided file-set rule;</li>
 *   <li>if a {@code reopen} row is addressed here, open again and require the named family.</li>
 * </ol>
 *
 * <h2>Consumption accounting, in two halves</h2>
 * Every {@code action}, {@code bytes}, {@code reopen} and {@code post} row addressed to the cell
 * being run must be consumed by a handler ({@link Consumption}). Of those four rows only the
 * {@code action} has a failure of its own — the whole-file {@code post} hash subsumes a
 * byte-at-offset assertion, a dropped {@code unchanged} row is silently re-verified by the two-sided
 * unnamed-input rule, and <em>nothing at all</em> observes a dropped {@code reopen}. The accountant
 * is what made each of those visible; it is not the only red for all of them today, because later
 * rounds added doctored cases that also fire.
 *
 * <p>That is only half the rule, and shipping it as the whole rule is what both C5j reviewers
 * broke independently: an accountant built per cell cannot see a row addressed to a cell that does
 * not exist. {@link #requireEveryOracleRowAddressesARunCell} is the other half, and contract §2.3
 * needs both to be true of an engine.
 */
final class XFixtureV2Executor {

    /**
     * How to choose the opener. {@link #BY_MANIFEST} is the only value production uses;
     * {@link #ALWAYS_WAL3} exists so the C5 plan §3.11 mutant can be RUN rather than described.
     *
     * <p>That mutant is the one that matters for {@code reject-wal3-segment-at-direct}. The C5 plan
     * §3.11 reasoned the discrimination would be the LOCK — a wrongly dispatched cell reaching the
     * WAL opener leaves a stray {@code x.lock} the file-set rule refuses. <b>For java neither half
     * of that is true, and both halves were measured.</b> java's {@code StoreDirect} leaves the
     * same {@code x.lock}, so the sidecar tells the two openers apart not at all; and java has no
     * D1 refusal — a regular file at the base path is exactly the divergence
     * {@code reject-wal3-d1-barebase} exists for, and it is in {@code EXPECT_EXCEPTIONS} because
     * java ACCEPTS one. So the misrouted cell does not "also reject": it opens, mints
     * {@code x.wal.0000000000000001}, and fails on the verdict itself.
     *
     * <p>§3.11's lock argument is the PORTS' mechanism, where D1 is a refusal. java's discriminator
     * is stronger and this test asserts the one java actually has.
     */
    enum Dispatch { BY_MANIFEST, ALWAYS_WAL3 }

    /** The one engine this executor speaks for. Named once so no sibling can drift from it. */
    static final String ENGINE = "java";

    private final XFixtureManifest.V2 m;
    private final File session;

    /**
     * The {@code (fixtureId, mode)} of every {@code ro} accept cell whose read-only handle was
     * actually probed with a write.
     *
     * <p>This exists so the probe is not a LEAF. Both reviewers deleted
     * {@code if (ro) assertWriteRefused(...)} and watched the whole gate stay green — nothing
     * observed the call, and the standalone discriminating test opens {@code openReadOnly} itself,
     * so it could not see the executor skipping it. A set the caller compares against the cells it
     * ran turns the deletion into an empty set and a red gate. It is bookkeeping, and it is the
     * cheapest honest answer to "a rule can be correct, directly tested, and never called".
     */
    final TreeSet<String> readOnlyHandlesProbed = new TreeSet<>();

    XFixtureV2Executor(XFixtureManifest.V2 m, File session) {
        this.m = m;
        this.session = session;
    }

    /**
     * Every {@code action}/{@code bytes}/{@code reopen}/{@code post} row addressed to java must name
     * a cell this engine actually runs.
     *
     * <p>Per-cell consumption cannot see this, and both reviewers proved it independently: the
     * accountant is built from the rows addressed to the cell BEING RUN, so a row addressed to a
     * {@code (fixture, mode)} with no {@code expect} row is owed by nobody, consumed by nobody and
     * graded by nobody. Codex moved Q8's {@code bytes} row to {@code reject-wal3-d1-barebase}
     * (a real fixture with no java cell, by {@code EXPECT_EXCEPTIONS}) and fable moved the
     * {@code reopen} row to the direct cell's absent {@code ro} mode; both suites stayed green with
     * the oracle silently dropped. Contract §2.3 says an addressed row no handler consumed is a
     * failure, and this is the half of that sentence per-cell accounting cannot reach.
     *
     * @param ran the {@code fixtureId + "/" + mode} of every java cell that was executed
     */
    void requireEveryOracleRowAddressesARunCell(java.util.Set<String> ran) {
        TreeSet<String> orphans = new TreeSet<>();
        for (XFixtureManifest.V2.Action a : m.actions)
            if (ENGINE.equals(a.engine) && !ran.contains(a.fixtureId + "/" + a.mode))
                orphans.add("action " + a.fixtureId + "/" + a.mode + " " + a.verb);
        for (XFixtureManifest.V2.Bytes b : m.bytes)
            if (ENGINE.equals(b.engine) && !ran.contains(b.fixtureId + "/" + b.mode))
                orphans.add("bytes " + b.fixtureId + "/" + b.mode + " " + b.relName);
        for (XFixtureManifest.V2.Reopen r : m.reopens)
            if (ENGINE.equals(r.engine) && !ran.contains(r.fixtureId + "/" + r.mode))
                orphans.add("reopen " + r.fixtureId + "/" + r.mode + " " + r.family);
        // `post` is the FOURTH addressed row type. Round 2 found nothing on either side of the
        // fence caught one addressed to a cell no engine runs; §2.3 now names it, and it has a
        // per-cell debt as well (round 3 — the orphan case is this loop's, the dropped-by-its-own
        // -handler case is the accountant's).
        for (XFixtureManifest.V2.Post p : m.posts)
            if (ENGINE.equals(p.engine) && !ran.contains(p.fixtureId + "/" + p.mode))
                orphans.add("post " + p.fixtureId + "/" + p.mode + " " + p.relName);
        assertTrue("oracle rows addressed to java whose cell this engine never ran, so no "
                + "accountant could ever owe them: " + orphans, orphans.isEmpty());
    }

    // ------------------------------------------------------------------ one cell

    void runCell(XFixtureManifest.V2.Expect e, File cell) throws IOException {
        runCell(e, cell, Dispatch.BY_MANIFEST);
    }

    void runCell(XFixtureManifest.V2.Expect e, File cell, Dispatch dispatch) throws IOException {
        String ctx = "v2 cell[" + e.fixtureId + " " + ENGINE + " " + e.mode + " " + e.verdict + " "
                + e.opener + "]";

        // Every oracle row addressed to this cell, and nothing else. Rows are REMOVED as they are
        // consumed; what is left at the end is a claim the executor was handed and dropped.
        Consumption owed = new Consumption(ctx);
        for (XFixtureManifest.V2.Action a : m.actionsOf(e.fixtureId, ENGINE, e.mode))
            owed.owe("action " + a.verb, a);
        for (XFixtureManifest.V2.Bytes b : m.bytesOf(e.fixtureId, ENGINE, e.mode))
            owed.owe("bytes " + b.relName + "@" + b.offset, b);
        for (XFixtureManifest.V2.Reopen r : m.reopensOf(e.fixtureId, ENGINE, e.mode))
            owed.owe("reopen " + r.family, r);
        // `post` too. Round 2 added it to the SUITE-WIDE half only, and codex showed the gap that
        // leaves: a post row addressed to a cell that RUNS could be skipped by its handler and the
        // two-sided unnamed-input rule would independently re-verify the same file, masking the
        // drop. The orphan case is the suite-wide check's; this is the other half.
        for (XFixtureManifest.V2.Post p : m.postsOf(e.fixtureId, ENGINE, e.mode))
            owed.owe("post " + p.relName, p);

        Map<String, byte[]> inputs = new LinkedHashMap<>();
        for (XFixtureManifest.V2.FileRow f : m.filesOf(e.fixtureId)) {
            File work = new File(cell, f.relName);
            Files.copy(new File(new File(session, f.fixtureId), f.relName).toPath(),
                    work.toPath());
            inputs.put(f.relName, Files.readAllBytes(work.toPath()));
        }

        String opener = dispatch == Dispatch.ALWAYS_WAL3 ? "wal3" : e.opener;
        File target = new File(cell, e.openArg);
        // The cell's OWN refusal, on a reject cell. Null on an accept cell, where there is none
        // and the reopen row (Q8's) is graded alone.
        Throwable firstRefusal = null;
        switch (e.verdict) {
            case "accept" -> runAccept(ctx, e, opener, target, owed);
            case "reject" -> firstRefusal = assertRejected(ctx, opener, e.mode, target);
            default -> fail(ctx + ": unknown verdict " + e.verdict);
        }

        // THE CAPTURE. Taken before the reopen, because the reopen is an open: it happens not to
        // rewrite a segment today, and "happens not to" is not a property to hash a corpus
        // against — the C4 probe made the same call for the same reason.
        Map<String, byte[]> after = capture(cell);
        assertBytesRows(ctx, e, after, owed);
        assertPostState(ctx, e, after, inputs, owed);
        assertReopen(ctx, e, opener, target, owed, firstRefusal);
        owed.requireAllConsumed();
    }

    private void runAccept(String ctx, XFixtureManifest.V2.Expect e, String opener, File target,
                           Consumption owed) {
        assertEquals(ctx + ": an accept cell through a non-wal3 opener is a shape no corpus has "
                + "and no executor here implements", "wal3", opener);
        boolean ro = "ro".equals(e.mode);
        StoreWAL s = ro ? StoreWAL.openReadOnly(target) : new StoreWAL(target);
        try {
            for (XFixtureManifest.V2.Action a : m.actionsOf(e.fixtureId, ENGINE, e.mode)) {
                // Deliberately NOT wrapped: a store that opened and then failed its action is a
                // different fact from one that refused to open, and collapsing the two lets a
                // broken action be read as the cell's verdict.
                Wal3Actions.run(s, a.verb, Wal3Actions.parseArgs(a.argSpec));
                owed.consume("action " + a.verb, a);
            }
            List<FixtureWriter.RecidExpect> expects = m.recids.get(e.fixtureId);
            boolean has = expects != null && !expects.isEmpty();
            requireSomeOracle(ctx, e, has);
            if (has) FixtureWriter.assertReaderContract(s, expects, ctx);
            if (ro) assertWriteRefused(ctx, e, s);
        } finally {
            s.close();
        }
    }

    /**
     * An accept cell must assert SOMETHING about the store it just opened — the C3j guard, as the
     * disjunction plan §5.3 item 5 asked for.
     *
     * <p>The first draft of this slice deleted the guard for the corpus and called the distribution
     * seal its replacement. Both reviewers refused that, and proving them right took one doctored
     * manifest: strip {@code wal3-java-cleaned}'s six recid rows and its accept cell passes with
     * nothing but the universal {@code x.lock} post row behind it. <b>The seal proves copy fidelity
     * and the guard proves assertion adequacy</b>; artifact identity cannot buy a semantic
     * property, and after that deletion nothing on either side of the fence enforced it — there is
     * no such rule in {@code manifest_v2.py} either.
     *
     * <p>The disjunction admits every cell the corpus actually has, which is why the deletion was
     * never forced:
     * <ul>
     *   <li>{@code recid} rows — the logical-state claim ({@code wal3-java-cleaned} rw and ro, and
     *       all three sample fixtures);</li>
     *   <li>an {@code action} row — the cell commits and a post oracle grades the result
     *       ({@code div-wal3-lsn-exhausted} java rw);</li>
     *   <li>a {@code reopen} row — the store's permanent unopenability is the claim;</li>
     *   <li>{@code mode == ro} — the read-only write refusal below is an executable claim, and on
     *       {@code div-wal3-lsn-exhausted} java ro it is the whole point: java accepts an image
     *       both ports refuse, and refuses the write. The first draft called that cell one that
     *       "carries neither", which skipped the probe it does carry.</li>
     *   <li>a {@code post} row that says the open CHANGED the tree — {@code modified},
     *       {@code truncated} or {@code deleted}. <b>The staged run found this one</b>, and no
     *       preflight root could have: {@code mut-wal3-torn-tail} java rw carries no recid row, no
     *       action and no reopen, and what it asserts is a post state — the tail truncated to the
     *       last valid section end, which is §6.2a's own obligation. A byte-exact statement of
     *       what recovery left behind is an assertion about the store, not an absence of one.</li>
     * </ul>
     *
     *   <li>a verdict that DIFFERS from another engine's on the same fixture and mode. <b>The
     *       staged run found this one too</b>, on the next cell of the same shape once the
     *       {@code mut-wal3-torn-tail} arm let it get that far — which is lesson (h) at corpus
     *       scale: fixing the first red is what reveals the second.
     *       {@code div-wal3-entry-recid0} java rw carries only the universal {@code x.lock}
     *       post row, and the catalogue says why in as many words: "java: open and close ONLY —
     *       no logical-state claim, because the reference's behaviour here is undefined
     *       ({@code recidToOffset} computes {@code recid - 1}) and pinning it would freeze an
     *       accident." The claim IS the verdict — java opens an image both ports refuse — and it
     *       is graded, because a java that started refusing would fail the {@code expect} row.
     *       Demanding more of a {@code div-} cell means demanding that undefined reference
     *       behaviour be frozen as contract, which is the one thing this corpus must not do.</li>
     * </ul>
     *
     * <p><b>{@code created} and {@code unchanged} are deliberately NOT in that last arm.</b> Every
     * wal3 cell carries the universal {@code x.lock created} row, so admitting {@code created}
     * would make the guard vacuous — it would admit the very cell the doctored proof above uses,
     * which is how this guard is shown to bite at all. {@code unchanged} is the two-sided rule's
     * default statement and asserts that the open did nothing.
     */
    private void requireSomeOracle(String ctx, XFixtureManifest.V2.Expect e, boolean hasRecids) {
        boolean mutationClaimed = false;
        for (XFixtureManifest.V2.Post p : m.postsOf(e.fixtureId, ENGINE, e.mode))
            mutationClaimed |= "modified".equals(p.verb) || "truncated".equals(p.verb)
                    || "deleted".equals(p.verb);
        // A DIVERGENCE: some other engine reaches a different verdict on this fixture and mode.
        // Read from the `expect` rows already in the manifest — the divergence needs no column of
        // its own, because it IS the disagreement between two rows that are both already there.
        //
        // THIS ARM IS JAVA'S ALONE, and the asymmetry is deliberate rather than an oversight the
        // ports have yet to fix. rust and zig carried it for one day; round 4 measured it and
        // found it unreachable in both. Every divergent (fixture, mode) group in the corpus —
        // `div-wal3-lsn-exhausted`, `div-wal3-entry-recid0`, `div-wal3-packlong-overlong` — is
        // java ACCEPT against ports REJECT, and this guard runs on the accept arm only, so a
        // port's copy was `false` on every input it has. It was deleted there under the rule this
        // slice keeps applying: a check no input can reach goes. Each port states the same thing
        // at the site where its arm used to be, so the asymmetry is discoverable from either side.
        //
        // Two java cells actually depend on it: `div-wal3-entry-recid0 rw` and
        // `div-wal3-packlong-overlong rw`. Round 4's qualification is worth keeping — the staged
        // run's red-without/green-with shows the arm is NECESSARY, not that it is SOUND, and what
        // it proves is narrower than the other five arms: that the cell's VERDICT discriminates,
        // not that the cell observed anything. No alternative was available, since a `reopen` row
        // on an accept cell can only express a refusal.
        boolean divergent = false;
        for (XFixtureManifest.V2.Expect o : m.expects)
            divergent |= o.fixtureId.equals(e.fixtureId) && o.mode.equals(e.mode)
                    && !o.engine.equals(ENGINE) && !o.verdict.equals(e.verdict);
        boolean any = hasRecids
                || !m.actionsOf(e.fixtureId, ENGINE, e.mode).isEmpty()
                || !m.reopensOf(e.fixtureId, ENGINE, e.mode).isEmpty()
                || "ro".equals(e.mode)
                || mutationClaimed
                || divergent;
        assertTrue(ctx + ": an accept cell with no recid rows, no action, no reopen, no post row "
                + "claiming a change, no engine disagreeing about the verdict and a writable "
                + "handle asserts nothing about the store it opened, which is not a check", any);
    }

    /**
     * D7's read-only mode is observable, in the direction that matters: a write through the
     * {@code ro} handle must be refused.
     *
     * <p>C3z's review found the general shape of what this closes — {@code mode} was parsed,
     * vocabulary-checked and used to select an opener, and then NOTHING observed the difference,
     * so every {@code ro} cell in java and rust was an ordinary writable open wearing a label.
     *
     * <p>The other direction is not asserted here but is not missing: {@code div-wal3-lsn-exhausted}
     * java {@code rw} runs {@code commit_one_record} — the same operation, through the same
     * {@link Wal3Actions#run} — and its {@code post} row records the segment growing by the
     * committed section. So the pair "rw commits, ro refuses" is carried by the corpus, and a
     * refusal that fired in both modes would fail that cell.
     */
    void assertWriteRefused(String ctx, XFixtureManifest.V2.Expect e, StoreWAL s) {
        DBException refusal = null;
        boolean accepted;
        try {
            s.put(new byte[] {1, 2, 3}, Wal3Actions.RAW);
            accepted = true;
        } catch (DBException x) {
            accepted = false;
            refusal = x;
        }
        // ONE assertion, not two. The campaign's own runner showed why: on a writable handle
        // `refusal` is null, so deleting the "accepted" half made the message half fire (or NPE) —
        // two statements that could only ever be killed by each other, which is a pair of checks
        // where the code has one claim. The claim is "the write was refused AND the refusal names
        // the mode", and it is now one statement with one red.
        String outcome = accepted ? "the write was ACCEPTED"
                : "refused with: " + refusal.getMessage();
        assertTrue(ctx + ": the probe accepted a writable handle or a refusal that does not name "
                        + "the read-only mode — " + outcome,
                !accepted && refusal.getMessage() != null
                        && refusal.getMessage().contains("open read-only"));
        // LAST, and inside this method rather than beside its call. Round 2 proved the difference:
        // with the recording at the call site, deleting `assertWriteRefused(...)` alone and keeping
        // the `add` left the whole gate green — the set observed that the bookkeeping ran, not that
        // the probe did.
        //
        // Round 3 proved the NEXT layer, and both reviewers found it: the assertions above can be
        // VACATED rather than skipped. No corpus input can reach their red — a conforming engine
        // refuses the write — so deleting either left 2,587 tests green while the recording still
        // attested that the probe "ran". They are given a red by
        // XFixtureCorpusTest#the_read_only_write_probe_fires, which hands this method a writable
        // handle and a wrong-reason refusal and COMPARES the collected reds — a list, so deleting
        // either input is visible too, which round 4 found it was not. Same treatment as
        // assertFamily, for the same reason.
        //
        // The ordering below is downstream of the assertion because the AssertionError propagates,
        // not because anything enforces the line order; the probe's own isEmpty check is what
        // notices if the two are swapped.
        readOnlyHandlesProbed.add(e.fixtureId + "/" + e.mode);
    }

    /**
     * A reject cell must fail with the engine's corruption class, through the named opener.
     *
     * <p>Returns the refusal, which the caller hands to {@link #assertReopen} — that is where the
     * cell's {@code reopen} row grades its FAMILY. Held rather than graded here because §3.11's
     * mutant (the direct cell dispatched to the wal3 opener) trips both the family check and the
     * post-row rule it was written to prove, and lesson (h) says such an input measures whichever
     * fires first.
     */
    private static Throwable assertRejected(String ctx, String opener, String mode, File target) {
        Throwable t = refusalOf(ctx, opener, mode, target);
        assertTrue(ctx + ": expected DBException.DataCorruption, but the store opened", t != null);
        assertTrue(ctx + ": refused with " + t.getClass().getName() + ": " + t.getMessage(),
                t instanceof DBException.DataCorruption);
        return t;
    }

    /** Opens and returns the refusal, or {@code null} if the store opened (and was closed). */
    private static Throwable refusalOf(String ctx, String opener, String mode, File target) {
        try {
            Store s;
            if ("direct".equals(opener)) {
                assertEquals(ctx + ": the direct opener has no read-only mode here", "rw", mode);
                s = new StoreDirect(target);
            } else {
                s = "ro".equals(mode) ? StoreWAL.openReadOnly(target) : new StoreWAL(target);
            }
            s.close();
            return null;
        } catch (DBException x) {
            return x;
        }
    }

    // ------------------------------------------------------------------- bytes

    /**
     * Grades every {@code bytes} row against the CAPTURED post bytes (contract §2.3).
     *
     * <p>It is never a pre-open patch: Q8's input segment is 186 bytes and the assertion is at
     * offset 187, so a pre-open reading is not merely wrong, it is out of range. An assertion whose
     * range cannot be reached is a failure, never a skip.
     */
    private void assertBytesRows(String ctx, XFixtureManifest.V2.Expect e,
                                 Map<String, byte[]> after, Consumption owed) {
        for (XFixtureManifest.V2.Bytes b : m.bytesOf(e.fixtureId, ENGINE, e.mode)) {
            String where = ctx + " bytes[" + b.relName + "@" + b.offset + "]";
            byte[] now = after.get(b.relName);
            assertTrue(where + ": names a file the cell directory does not hold", now != null);
            int len = b.hex.length() / 2;
            assertTrue(where + ": the range ends at " + (b.offset + len) + " and the post state is "
                    + now.length + " bytes", b.offset + len <= now.length);
            StringBuilder got = new StringBuilder(b.hex.length());
            for (int i = 0; i < len; i++) {
                int v = now[(int) b.offset + i] & 0xFF;
                got.append(Character.forDigit(v >>> 4, 16)).append(Character.forDigit(v & 0xF, 16));
            }
            assertEquals(where + ": the asserted bytes", b.hex, got.toString());
            owed.consume("bytes " + b.relName + "@" + b.offset, b);
        }
    }

    // ------------------------------------------------------------------ reopen

    private void assertReopen(String ctx, XFixtureManifest.V2.Expect e, String opener,
                              File target, Consumption owed, Throwable first) {
        for (XFixtureManifest.V2.Reopen r : m.reopensOf(e.fixtureId, ENGINE, e.mode)) {
            String where = ctx + " reopen[" + r.family + "]";
            // THE CELL'S OWN REFUSAL FIRST, where there was one. C5t's first draft graded the
            // family on the reopen alone and threw the first refusal away; codex round 1 finding 2
            // is why it does not. The reopen is a WRITABLE open whatever the cell's mode was, so
            // every mode=ro row was graded on a retry in the OTHER mode — a store that refuses
            // read-only for one reason and writable for another passed, and so did a stateful one
            // that got it wrong once and right on retry. The arm the corpus names is the first
            // open; the second is the stability check.
            //
            // On an ACCEPT cell — Q8 — `first` is null and the reopen is the only grading there
            // is, because the cell's own open succeeded.
            if (first != null) assertFamily(ctx + " family[" + r.family + "]", r.family, first);
            // A reopen is a WRITABLE open whatever the cell's own mode was: the claim is that the
            // store is permanently unopenable, and a read-only probe would be a weaker one.
            // Through the cell's OWN opener, not a hard-coded `wal3`. Until C5t only Q8 had a
            // reopen row and Q8 is a wal3 cell, so the constant was right by accident;
            // `reject-wal3-segment-at-direct` carries one now, and sending it to the WAL opener
            // would grade a `direct-magic` family against a refusal StoreDirect never made.
            Throwable t = refusalOf(where, opener, "rw", target);
            assertTrue(where + ": the store opened again", t != null);
            assertFamily(where, r.family, t);
            owed.consume("reopen " + r.family, r);
        }
    }

    /**
     * S2 is {@code lsn <= seg.lastLsn} on a section HEADER ({@code StoreWAL.java:679-682}), wrapped
     * by {@code hold}'s {@code "WAL segment <name>: "} prefix.
     *
     * <p>Matched WHOLE, with {@link java.util.regex.Matcher#matches}. The first draft used
     * {@code find()} on an unanchored fragment and its comment claimed it was anchored; codex
     * demonstrated the gap — a message with the S2 wording embedded in unrelated text passed. The
     * refusal this grades is one line of the reference and its whole form is knowable, so matching
     * the whole form is what the check should say.
     *
     * <p><b>The NAME is opaque.</b> It was {@code [^:]+}, which forbids a legal segment filename
     * containing a colon and so refuses a genuine refusal about one — codex round 2, the same
     * defect round 1 found in rust's D1 predicate, in a third place. The rest of the sentence is
     * fixed and the name is whatever lies between two fixed markers, so the name group is reluctant
     * and the marker that ends it is spelled out in full: only {@code ": section LSN "} can, and
     * {@code matches()} anchors both ends.
     *
     * <p><b>{@code [\s\S]+?}, not {@code .+?}</b> — the SAME defect a third time, and round 2's own
     * repair introduced it. Java's {@code .} does not match a line terminator unless DOTALL is on,
     * and a Unix filename may contain a newline, so {@code .+?} traded a hidden no-colon constraint
     * for a hidden no-newline one. The character class round 2 deleted, {@code [^:]+}, matched
     * newlines fine. Opaque means opaque: the name group must exclude nothing at all, and only the
     * markers on either side may end it.
     *
     * <p>The OFFSET is unsigned and the two LSNs are not, and that asymmetry is the engine's: an LSN
     * is {@code long} on disk and a CRC-valid section may legitimately carry a negative one, while
     * an offset is a count of bytes into a file. A message with {@code at offset -2} is not this
     * refusal reworded — it is not a refusal this engine writes at all.
     */
    static final Pattern S2 = Pattern.compile(
            "WAL segment [\\s\\S]+?: section LSN -?\\d+ at offset \\d+ does not follow -?\\d+");

    /**
     * Asserts a refusal belongs to the named contract family.
     *
     * <p>The family is read from the manifest row, never hard-coded, so editing
     * {@code catalogue.reopen} stops the run instead of being graded against a constant this file
     * happens to agree with. A family this engine has no predicate for is a <b>failure</b>: the
     * alternative is a green cell whose reopen was checked by nothing.
     *
     * <p>Matching on the message is weaker than zig's typed {@code Diag.reason} and is what java
     * has — {@code DBException.DataCorruption} is also D1's class, N6's and every other
     * writer-defect verdict's, so the class alone identifies no rule. The pattern is anchored on
     * the words the S2 arm alone produces, and {@link XFixtureCorpusTest} proves it does not match
     * its immediate neighbour S9, which the corpus itself never varies.
     */
    static void assertFamily(String where, String family, Throwable t) {
        if ("direct-magic".equals(family)) {
            // `StoreDirect.initOpen`'s magic check, matched WHOLE and named exactly.
            //
            // The draft of this arm accepted the length refusal beside it, reasoning that a WAL
            // segment might be shorter than the header page. MEASURED instead: the published
            // segment is 1,200,509 bytes, so it clears `PAGE_SIZE` by three orders of magnitude
            // and reaches the magic word — in all three engines. A disjunction with an
            // unreachable half is a predicate that cannot say what it refuses, which is the whole
            // objection to grading a reject cell by "it threw something".
            assertTrue(where + ": direct-magic is a corruption verdict, got "
                            + t.getClass().getName() + ": " + t.getMessage(),
                    t instanceof DBException.DataCorruption);
            assertEquals(where + ": not StoreDirect's bad-magic refusal",
                    "not a mapdb StoreDirect file (bad magic)", t.getMessage());
            return;
        }
        if ("S2".equals(family)) {
            assertTrue(where + ": S2 is a corruption verdict, got " + t.getClass().getName()
                    + ": " + t.getMessage(), t instanceof DBException.DataCorruption);
            assertTrue(where + ": not the S2 rule's refusal: " + t.getMessage(),
                    t.getMessage() != null && S2.matcher(t.getMessage()).matches());
            return;
        }
        fail(where + ": error family " + family + " has no predicate in this engine. Refusing "
                + "rather than accepting any refusal at all — an unimplemented family graded as "
                + "'it threw something' is the check not running");
    }

    // -------------------------------------------------------------- post state

    private static Map<String, byte[]> capture(File cell) throws IOException {
        Map<String, byte[]> out = new LinkedHashMap<>();
        String[] names = cell.list();
        assertTrue("cell dir vanished: " + cell, names != null);
        java.util.Arrays.sort(names);
        for (String n : names) {
            File f = new File(cell, n);
            assertTrue(cell + ": " + n + " is not a regular file", f.isFile());
            out.put(n, Files.readAllBytes(f.toPath()));
        }
        return out;
    }

    /**
     * Checks the capture against this cell's {@code post} rows.
     *
     * <p>Two-sided, which is the D6 post-cardinality amendment: a file a post row names must match
     * that disposition exactly, and every OTHER input must be byte-unchanged with nothing new
     * beside it. A one-sided check would pass a store that rewrote every segment it was handed.
     *
     * <p><b>The C5 relaxation §3.10 asked for is not here, because C5j measured it away.</b> The
     * plan expected {@code reject-wal3-segment-at-direct} to carry no post rows —
     * {@code catalogue.universal_overrides} exempted non-{@code wal3} cells from the
     * {@code x.lock} row on the reasoning that the lock belongs to {@code WalSegmentSet}. It
     * belongs to the store: java's {@code StoreDirect} takes its own {@code <db>.lock} before it
     * opens the volume, so the refused file leaves a sidecar too. The catalogue now says so
     * ({@code DIRECT_OPENER_LOCKS}) and every java cell has a post row again, so the original
     * guard stands unweakened. Whether the ports need the relaxation is what C5r and C5z measure.
     *
     * <p><b>Recorded rather than implied</b> (codex, C5j review): across both roots the java
     * {@code post} rows exercise {@code created} and {@code modified} only. {@code unchanged},
     * {@code deleted} and {@code truncated} are parsed, branch here, and are executed by nothing —
     * parsed vocabulary is not execution coverage. It blocks no C5j cell, and the inputs that would
     * close it are C5t's torn-tail dispositions, which is where the six {@code truncated}/
     * {@code deleted} shapes actually live.
     */
    private void assertPostState(String ctx, XFixtureManifest.V2.Expect e,
                                 Map<String, byte[]> after, Map<String, byte[]> inputs,
                                 Consumption owed) {
        List<XFixtureManifest.V2.Post> posts = m.postsOf(e.fixtureId, ENGINE, e.mode);
        assertTrue(ctx + ": no post rows — a cell that asserts nothing about the directory it just "
                + "opened is not a check", !posts.isEmpty());
        TreeSet<String> named = new TreeSet<>();
        for (XFixtureManifest.V2.Post p : posts) {
            String where = ctx + " post[" + p.relName + " " + p.verb + "]";
            assertTrue(where + ": two post rows for one file", named.add(p.relName));
            byte[] was = inputs.get(p.relName);
            byte[] now = after.get(p.relName);
            switch (p.verb) {
                case "unchanged" -> {
                    assertTrue(where + ": names a file that was not an input", was != null);
                    assertTrue(where + ": file is gone", now != null);
                    assertArrayEquals(where + ": bytes changed", was, now);
                }
                case "deleted" -> {
                    assertTrue(where + ": names a file that was not an input", was != null);
                    assertTrue(where + ": file is still there", now == null);
                }
                case "created", "truncated", "modified" -> {
                    // `created` means it was NOT an input; the other two mean it was. Saying so
                    // per verb rather than sharing one branch is what stops a newly created file
                    // mislabelled `modified` from passing.
                    if ("created".equals(p.verb)) {
                        assertTrue(where + ": names a file that already existed as an input",
                                was == null);
                    } else {
                        assertTrue(where + ": names a file that was not an input", was != null);
                    }
                    assertTrue(where + ": file is missing", now != null);
                    assertEquals(where + ": length", p.length, now.length);
                    assertEquals(where + ": SHA-256", p.sha, FixtureWriter.sha256Hex(now));
                }
                default -> fail(where + ": unknown disposition verb");
            }
            // AFTER the disposition was asserted, never before: a row consumed on entry would be
            // accounted for by a handler that had not yet graded it.
            owed.consume("post " + p.relName, p);
        }
        for (Map.Entry<String, byte[]> in : inputs.entrySet()) {
            if (named.contains(in.getKey())) continue;
            byte[] now = after.get(in.getKey());
            assertTrue(ctx + ": input " + in.getKey() + " is gone and no post row says so",
                    now != null);
            assertArrayEquals(ctx + ": input " + in.getKey() + " changed and no post row says so",
                    in.getValue(), now);
        }
        for (String name : after.keySet()) {
            assertTrue(ctx + ": unexpected new file " + name,
                    inputs.containsKey(name) || named.contains(name));
        }
    }

    // ------------------------------------------------------ resource plumbing
    //
    // Shared by both v2 roots and by the v1 flow, for the reason at the top of this file: a
    // second copy of "gunzip it and check its three pinned identities" is a second thing to fix.

    static byte[] resource(String path) throws IOException {
        try (java.io.InputStream in = XFixtureV2Executor.class.getResourceAsStream(path)) {
            if (in == null) fail("classpath resource " + path + " is missing");
            return in.readAllBytes();
        }
    }

    /** Gunzips a blob and checks all three of its pinned identities before it is ever opened. */
    static byte[] gunzipChecked(String root, String blobName, String relName, long rawLen,
                                String rawSha, String gzSha) throws IOException {
        byte[] gz = resource(root + blobName);
        assertEquals(blobName + ": compressed SHA-256 mismatch", gzSha, FixtureWriter.sha256Hex(gz));
        java.io.ByteArrayOutputStream raw = new java.io.ByteArrayOutputStream();
        try (java.util.zip.GZIPInputStream gin = new java.util.zip.GZIPInputStream(
                new java.io.ByteArrayInputStream(gz))) {
            gin.transferTo(raw);
        }
        byte[] bytes = raw.toByteArray();
        assertEquals(relName + ": uncompressed length mismatch", rawLen, bytes.length);
        assertEquals(relName + ": uncompressed SHA-256 mismatch", rawSha,
                FixtureWriter.sha256Hex(bytes));
        return bytes;
    }

    /**
     * Gunzips every v2 fixture file once into {@code <session>/<fixtureId>/<relName>}.
     *
     * <p>The per-fixture subdirectory is load-bearing, not tidiness: bundles name their segments
     * {@code x.wal.0000000000000002} and up, so a flat map keyed by {@code relName} would have them
     * overwrite each other and every cell would run against whichever was gunzipped last.
     */
    static void gunzipAll(XFixtureManifest.V2 m, String root, File session) throws IOException {
        for (XFixtureManifest.V2.FileRow f : m.files) {
            byte[] bytes = gunzipChecked(root, f.blobName(), f.relName, f.rawLen, f.rawSha, f.gzSha);
            File dir = new File(session, f.fixtureId);
            assertTrue("cannot create " + dir, dir.isDirectory() || dir.mkdirs());
            Files.write(new File(dir, f.relName).toPath(), bytes);
        }
    }

    static TreeSet<String> listNames(File dir) {
        String[] names = dir.list();
        assertTrue("cell dir vanished: " + dir, names != null);
        return new TreeSet<>(List.of(names));
    }

    /**
     * The exploded directory a resource root lives in.
     *
     * <p>Fails rather than skipping if the tests ever run from a jar: a completeness check that
     * quietly returns early when it cannot look is the defect it exists to catch.
     */
    static File rootDir(String root) throws Exception {
        java.net.URL url = XFixtureV2Executor.class.getResource(root);
        assertTrue("resource root " + root + " is missing", url != null);
        assertEquals("the resources at " + root + " are not on an exploded classpath — this check "
                + "needs rewriting, not skipping", "file", url.getProtocol());
        return new File(url.toURI());
    }

    // ------------------------------------------------------- the accountant

    /**
     * The oracle rows one cell owes, and which of them a handler actually ran.
     *
     * <p>Package-private and separately unit-tested, because it is the ONE mechanism standing
     * between "executes" and "parses and drops" for three of the four addressed oracle row types —
     * every one except {@code action}, which has a failure of its own.
     */
    static final class Consumption {
        private final String ctx;
        private final Map<String, Object> owed = new LinkedHashMap<>();
        private final LinkedHashSet<String> done = new LinkedHashSet<>();

        Consumption(String ctx) { this.ctx = ctx; }

        void owe(String key, Object row) {
            assertTrue(ctx + ": two oracle rows share the key " + key, owed.put(key, row) == null);
        }

        void consume(String key, Object row) {
            assertTrue(ctx + ": consumed " + key + ", which was never owed", owed.containsKey(key));
            assertTrue(ctx + ": consumed " + key + " with a different row object",
                    owed.get(key) == row);
            assertTrue(ctx + ": consumed " + key + " twice", done.add(key));
        }

        void requireAllConsumed() {
            List<String> dropped = new ArrayList<>();
            for (String k : owed.keySet()) if (!done.contains(k)) dropped.add(k);
            assertTrue(ctx + ": oracle rows addressed to this cell that no handler consumed: "
                    + dropped + ". A parsed-and-dropped assertion is a green cell that checked "
                    + "nothing", dropped.isEmpty());
        }
    }
}
