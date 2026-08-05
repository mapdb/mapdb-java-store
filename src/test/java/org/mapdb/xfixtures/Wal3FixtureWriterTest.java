package org.mapdb.xfixtures;

import org.junit.Test;
import org.mapdb.TmpFiles;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Gate for {@link Wal3FixtureWriter} (slice C2j).
 *
 * <p>The generator asserts contract §5.2/§5.3/§5.3.1/§5.4 about its own output while it runs, so
 * this test's first job is simply to RUN it: an assertion in a program nobody invokes is not a
 * check. The C2-entry review made that the standing rule for this workstream — a measured number
 * whose script no longer exists is not evidence — and a generator invoked only by the sync script,
 * in the planning repo, on the day of the cutover, is the same defect one slice later.
 *
 * <p>Its second job is the part the generator cannot do for itself. Two of the properties below
 * are asserted HERE and nowhere in the generator, because a program cannot catch its own blind
 * spot by re-reading its own output: that the two shapes really are DIFFERENT shapes, and that
 * the §5.3.1 witnesses each depend on something — falsify the workload and the corresponding
 * witness must fail. The python half ({@code todo/store-cross/xcheck_bundles.py}) re-derives
 * every structural claim from the published bytes with an independent codec.
 */
public class Wal3FixtureWriterTest {

    private File out() throws IOException {
        return TmpFiles.tempDir("wal3-c2j");
    }

    private static void generate(File dir) throws IOException {
        Wal3FixtureWriter.main(new String[]{"--out", dir.getAbsolutePath(), "--force", "--quiet"});
    }

    /**
     * The whole generator, end to end. Every §5.2/§5.3/§5.3.1/§5.4 self-check inside it is
     * exercised by this one call; the assertions below are about the PRODUCT.
     */
    @Test
    public void generatorProducesBothBundles() throws IOException {
        File dir = out();
        generate(dir);

        for (String id : new String[]{Wal3FixtureWriter.TAIL_ID, Wal3FixtureWriter.CLEANED_ID}) {
            File b = new File(dir, id);
            assertTrue(id + " was not published", b.isDirectory());
            String[] segs = b.list();
            assertTrue(id + " published " + Arrays.toString(segs) + "; both shapes need ≥2 segments",
                    segs != null && segs.length >= 2);
            for (String s : segs)
                assertTrue(id + ": " + s + " is not a %016x segment name",
                        s.matches("x\\.wal\\.[0-9a-f]{16}"));
        }
        // §5.3: the cleaned bundle's retained floor is above segment 1, which is the shape v1
        // could not express — and the reason this bundle exists at all.
        assertTrue("the cleaned bundle must not contain segment 1",
                !new File(dir, Wal3FixtureWriter.CLEANED_ID + "/x.wal.0000000000000001").exists());

        // §5.4 obligation 7: no scratch survives, and nothing but the two bundles and the two
        // sidecars is published.
        List<String> published = new ArrayList<>(Arrays.asList(dir.list()));
        published.sort(null);
        assertEquals("unexpected files in the output directory",
                Arrays.asList("fragment.tsv", "layout.tsv",
                        Wal3FixtureWriter.CLEANED_ID, Wal3FixtureWriter.TAIL_ID),
                published);
    }

    /**
     * §5.4 obligation 7 for the output directory: a `--force` rerun over a directory holding
     * anything else must be REFUSED, not quietly republished around.
     */
    @Test
    public void refusesToPublishBesideStrayFiles() throws IOException {
        File dir = out();
        generate(dir);
        Files.write(new File(dir, "leftover.tsv").toPath(), new byte[]{1});
        try {
            generate(dir);
            fail("the generator published into a directory holding a stray file; the sync script "
                    + "consumes everything in that directory");
        } catch (AssertionError e) {
            assertTrue("refused, but not for the stray file: " + e.getMessage(),
                    e.getMessage() != null && e.getMessage().contains("leftover.tsv"));
        }
    }

    /**
     * §5.4 obligation 8, ACROSS PROCESSES, and this time literally.
     *
     * <p>The generator compares two runs internally, but both live in ONE JVM and share every
     * JVM-wide seed, so an output depending on an identity hash code, a hash-map iteration order
     * or a lazily initialised static would agree with itself and still differ between runs. This
     * test's first draft called two {@code main()}s in the surefire JVM "across processes"; the
     * C2j review's finding 4 is that a name is a claim, and this one was false. It now spawns a
     * real JVM running the documented CLI.
     *
     * <p>The comparison is over the COMPLETE published tree — both bundles and both sidecars —
     * because {@code fragment.tsv} does not exist yet when {@code produceTwice} runs and is
     * therefore the one obligation the generator structurally cannot assert about itself.
     */
    @Test
    public void twoProcessesAgreeByteForByte() throws Exception {
        File a = generateInAChildProcess(), b = generateInAChildProcess();
        for (String id : new String[]{Wal3FixtureWriter.TAIL_ID, Wal3FixtureWriter.CLEANED_ID})
            assertEquals(id + " is not deterministic across two processes",
                    describe(new File(a, id)), describe(new File(b, id)));
        for (String side : new String[]{"fragment.tsv", "layout.tsv"})
            assertEquals(side + " is not deterministic across two processes",
                    read(new File(a, side)), read(new File(b, side)));
        // The whole tree, not just the parts named above: a generator that grew a third output
        // would otherwise be compared by nothing.
        assertEquals("the published trees differ", describeTree(a), describeTree(b));
    }

    /**
     * Runs the documented generator CLI in a SEPARATE JVM and returns its output directory.
     *
     * <p>Same command the class javadoc publishes and the sync script will run, built from this
     * JVM's own {@code java.home} and classpath so it needs no build-tool cooperation.
     */
    private File generateInAChildProcess() throws Exception {
        File dir = out();
        String java = new File(new File(System.getProperty("java.home"), "bin"), "java").getPath();
        Process p = new ProcessBuilder(java, "-ea",
                "-cp", System.getProperty("java.class.path"),
                Wal3FixtureWriter.class.getName(),
                "--out", dir.getAbsolutePath(), "--force", "--quiet")
                .redirectErrorStream(true)
                .start();
        String output = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals("the generator child process failed:\n" + output, 0, p.waitFor());
        return dir;
    }

    /** The two shapes must be genuinely different, or one of them is testing nothing. */
    @Test
    public void theTwoShapesDiffer() throws IOException {
        File dir = out();
        generate(dir);
        assertTrue("the tail and cleaned bundles are byte-identical, so one of them is redundant",
                !describe(new File(dir, Wal3FixtureWriter.TAIL_ID))
                        .equals(describe(new File(dir, Wal3FixtureWriter.CLEANED_ID))));
        Map<String, String> layout = layout(dir);
        // §5.3.1 rows 1-3, restated as the index they produce. The tail shape has no middle
        // retained segment because it has no mark and therefore only two segments; the cleaned
        // shape must have all three positions.
        assertEquals("x.wal.0000000000000003",
                layout.get(Wal3FixtureWriter.CLEANED_ID + " @middle_retained"));
        assertEquals("x.wal.0000000000000004",
                layout.get(Wal3FixtureWriter.CLEANED_ID + " @single_section_retained"));
        assertEquals("x.wal.0000000000000002",
                layout.get(Wal3FixtureWriter.CLEANED_ID + " @mark"));
        assertTrue("the tail shape must host no `mark` selector",
                !layout.containsKey(Wal3FixtureWriter.TAIL_ID + " @mark"));
    }

    /**
     * The published bundles open cleanly in a fresh directory and mutate nothing.
     *
     * <p>This is §5.5's claim, and all 36 accept cells rest on it: "no ACCEPT bundle cell
     * mutates", so no cell carries a {@code created:}/{@code truncated:} override. The generator
     * checks it for its scratch copy; this checks it for the copy that was actually published.
     */
    @Test
    public void publishedBundlesOpenWithoutMutating() throws IOException {
        File dir = out();
        generate(dir);
        for (String id : new String[]{Wal3FixtureWriter.TAIL_ID, Wal3FixtureWriter.CLEANED_ID}) {
            File cell = out();
            File src = new File(dir, id);
            for (String rel : src.list())
                Files.copy(new File(src, rel).toPath(), new File(cell, rel).toPath());
            String before = describe(cell);
            StoreWAL s = new StoreWAL(new File(cell, "x"));
            try {
                s.verify();
            } finally {
                s.close();
            }
            Files.deleteIfExists(new File(cell, "x.lock").toPath());
            assertEquals(id + ": a clean rw open mutated the published bundle, so §5.5 is false "
                    + "for it and its accept cells need file-set overrides", before, describe(cell));
        }
    }

    /**
     * The three REJECTED candidate workloads, measured.
     *
     * <p>What this pins, stated narrowly because the name it first carried
     * ("witnessesDependOnTheShaping") claimed more than the inputs deliver: all three of these
     * lose §5.3.1 <b>row 1</b>. They are the history of how §5.3's table was arrived at, not a
     * per-witness falsification — two of them change the checkpoint's position and the third
     * tests a rewrite that was proposed and rejected, and none of them removes a single adopted
     * shaping step. The C2j review's finding 6 is exactly this workstream's recurring defect, a
     * test whose name claims a property its inputs never exercise.
     *
     * <p>The per-step falsifiers are {@link #eachShapingStepIsLoadBearing} and
     * {@link #rowFiveIsInvisibleToThisGenerator}; the per-WITNESS-ROW falsifiers are
     * {@code derive.py --self-test}'s, which mutates the model once per row, declares each case's
     * exact consequent failure set, and refuses to let a row exist without a case.
     */
    @Test
    public void rejectedCandidateWorkloadsMeasured() throws IOException {
        // §5.3 as literally written: checkpoint after T3, no shaping. The cleaner's image covers
        // F's 1.2 MB, which overflows the segment holding it, so the forced mark lands as section
        // 0 of the NEXT segment. That is the finding that moved the checkpoint: adding segments
        // cannot repair it, because whichever segment then becomes the middle one opens with the
        // 'K' that §5.3.1 row 2 forbids there.
        assertEquals("§5.3's literal workload no longer puts the mark where the plan measured it",
                "mark=3:0 retained=[2, 3] activeSections=2", shapeOf("spec"));
        expectRefusal("spec", "row 1 requires exactly three retained segments");

        // The checkpoint moved: the mark is now section 1 of the LOWEST retained segment, beside
        // the 'C' image, which is what row 2 needs. Two retained segments is what is left to fix.
        assertEquals("moving the checkpoint no longer puts the mark beside the 'C' image",
                "mark=2:1 retained=[2, 3] activeSections=1", shapeOf("ckpt-after-t2"));
        expectRefusal("ckpt-after-t2", "row 1 requires exactly three retained segments");

        // The plan's own first proposal for the third segment: `update(F, own content)` was
        // expected to "force the rotation into a single-section active segment". It does not.
        // Rollover is tested BEFORE the append, so the 1.2 MB section joined the segment it
        // overflowed and nothing rotated — the active segment ends with FOUR sections, and the
        // retained count never moves off two.
        assertEquals("the plan's rejected proposal no longer fails the way it was measured to",
                "mark=2:1 retained=[2, 3] activeSections=4", shapeOf("ckpt-after-t2-shaped"));
        expectRefusal("ckpt-after-t2-shaped", "row 1 requires exactly three retained segments");
    }

    /**
     * The one shaping step this class CANNOT justify, made executable rather than argued.
     *
     * <p>`shapeC` exists for §5.3.1 row 5, and row 5 is the row {@link Wal3FixtureWriter} does not
     * check — deciding it means decoding the entry stream and searching for a size-preserving
     * replacement encoding. So dropping `shapeC` produces a bundle this generator's own grading
     * ACCEPTS, and only `derive.check_witnesses` refuses:
     *
     * <pre>
     *   FAIL stranded-append-candidate: §5.3.1 row 5: no entry in the selected section admits a
     *   stranded T_APPEND: entry 0 (recid 1): a later section touches it, which would clear the
     *   skip set before the audit
     * </pre>
     *
     * This test asserts the java side's BLINDNESS, which is the only part of that it can own. If
     * it ever starts failing, the generator has grown a row-5 check and this test should become
     * an `expectRefusal` instead — which is a better outcome, not a regression.
     */
    @Test
    public void rowFiveIsInvisibleToThisGenerator() throws IOException {
        File dir = probe("shaped-no-C");
        assertEquals("dropping shapeC no longer produces the shape this test is about",
                "mark=2:1 retained=[2, 3, 4] activeSections=1", Wal3FixtureWriter.describeShape(dir));
        Wal3FixtureWriter.gradeCleaned(dir);   // accepted: rows 1,2,3,4,6 all hold without shapeC
    }

    /**
     * Each ADOPTED shaping step, removed on its own, and what it costs.
     *
     * <p>This is the test {@link #rejectedCandidateWorkloadsMeasured} was mistakenly named for.
     * The rotation pair is two commits because rollover is tested BEFORE a section is appended,
     * and each half is removed separately rather than the pair as a unit — removing the pair
     * together would show only that "some rotation is needed", which is not the claim §5.3 makes.
     *
     * <p>The remaining adopted step, {@code shapeC}, is falsified in
     * {@link #rowFiveIsInvisibleToThisGenerator}: it cannot be falsified here, because the row it
     * exists for is the one this generator cannot see.
     */
    @Test
    public void eachShapingStepIsLoadBearing() throws IOException {
        // No rotation at all: the log never opens a third segment, so `middle` and `active` are
        // the same file and row 1 is lost.
        assertEquals("dropping the rotation pair no longer produces the shape this test pins",
                "mark=2:1 retained=[2, 3] activeSections=3", shapeOf("shaped-no-rotate"));
        expectRefusal("shaped-no-rotate", "row 1 requires exactly three retained segments");

        // Only the half that CROSSES segmentBytes. This is the case the pair exists for: the
        // oversized section joins the segment it overflows, so nothing has rotated yet.
        assertEquals("the oversized commit now rotates on its own, which would make the second "
                        + "half of the rotation pair unnecessary — read StoreWAL.java:1688 before "
                        + "changing this expectation",
                "mark=2:1 retained=[2, 3] activeSections=4", shapeOf("shaped-half-rotate"));
        expectRefusal("shaped-half-rotate", "row 1 requires exactly three retained segments");
    }

    /** relPath -> length + sha for every file under {@code root}, recursively. */
    private static String describeTree(File root) throws IOException {
        List<String> rows = new ArrayList<>();
        collect(root, root.toPath(), rows);
        rows.sort(null);
        return String.join("", rows);
    }

    private static void collect(File dir, java.nio.file.Path root, List<String> into)
            throws IOException {
        File[] kids = dir.listFiles();
        if (kids == null) return;
        for (File f : kids) {
            if (f.isDirectory()) {
                collect(f, root, into);
                continue;
            }
            byte[] raw = Files.readAllBytes(f.toPath());
            into.add(root.relativize(f.toPath()) + "\t" + raw.length + "\t"
                    + FixtureWriter.sha256Hex(raw) + "\n");
        }
    }

    private String shapeOf(String variant) throws IOException {
        return Wal3FixtureWriter.describeShape(probe(variant));
    }

    private File probe(String variant) throws IOException {
        File dir = out();
        Wal3ShapeProbe.main(new String[]{"--out", dir.getAbsolutePath(), "--variant", variant});
        return dir;
    }

    /** The variant must be REFUSED by the generator's own §5.3.1 grading, for the stated reason. */
    private void expectRefusal(String variant, String expected) throws IOException {
        File dir = probe(variant);
        AssertionError refusal = null;
        try {
            Wal3FixtureWriter.gradeCleaned(dir);
        } catch (AssertionError e) {
            refusal = e;
        }
        if (refusal == null)
            fail("variant " + variant + " was expected to violate §5.3.1 (" + expected
                    + ") and it satisfied every row instead — either the shaping this generator "
                    + "does is unnecessary, or this expectation is stale");
        if (refusal.getMessage() == null || !refusal.getMessage().contains(expected))
            throw new AssertionError("variant " + variant + " was refused, but not for the "
                    + "reason claimed.\n  expected a message containing: " + expected
                    + "\n  got: " + refusal.getMessage(), refusal);
    }

    // ---------- helpers ----------

    private static String describe(File dir) throws IOException {
        String[] names = dir.list();
        Arrays.sort(names);
        StringBuilder sb = new StringBuilder();
        for (String n : names) {
            byte[] raw = Files.readAllBytes(new File(dir, n).toPath());
            sb.append(n).append('\t').append(raw.length).append('\t')
              .append(FixtureWriter.sha256Hex(raw)).append('\n');
        }
        return sb.toString();
    }

    private static String read(File f) throws IOException {
        return new String(Files.readAllBytes(f.toPath()), StandardCharsets.UTF_8);
    }

    /** {@code "<fixtureId> @<selector>" -> relName} from the published {@code layout.tsv}. */
    private static Map<String, String> layout(File dir) throws IOException {
        Map<String, String> out = new LinkedHashMap<>();
        for (String line : read(new File(dir, "layout.tsv")).split("\n")) {
            if (line.isEmpty() || line.startsWith("#")) continue;
            String[] f = line.split("\t");
            assertEquals("layout.tsv row arity", 4, f.length);
            assertEquals("layout.tsv row type", "symbol", f[0]);
            assertEquals("layout.tsv claims " + f[1] + " " + f[2] + " twice",
                    null, out.put(f[1] + " " + f[2], f[3]));
        }
        return out;
    }
}
