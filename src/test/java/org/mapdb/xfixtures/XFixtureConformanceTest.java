package org.mapdb.xfixtures;

import org.junit.After;
import org.junit.Test;
import org.mapdb.DBException;
import org.mapdb.TmpFiles;
import org.mapdb.store.Store;
import org.mapdb.store.StoreDirect;
import org.mapdb.store.StoreWAL;

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
import java.util.TreeSet;
import java.util.zip.GZIPInputStream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Cross-port conformance harness (Stages 1 and 2): runs every {@code engine == java} cell of
 * the checked-in fixture manifest against this engine.
 *
 * <p>The fixtures pin the CURRENT state of an UNSTABLE format so that silent divergence
 * between the store engines is detected. Cross-engine openability today is an implementation
 * fact, not a supported feature; any format change regenerates the fixtures as part of that
 * change (see {@link FixtureWriter} and the sync script in the planning repo).
 *
 * <p>Flow per the Stage-1/2 contracts: load {@code xfixtures/MANIFEST.tsv} from the classpath
 * (HARD failure when absent — a missing manifest means the fixture sync step never ran for
 * this checkout — or when its version is not 1), gunzip every fixture file once into a
 * session temp dir verifying length and SHA-256, then run each cell in a fresh private
 * directory over its own working copy: accept cells get {@code verify()} + the full
 * per-recid reader contract + exact {@code getAllRecids}; reject cells must fail to open
 * with {@link DBException.DataCorruption} — via {@link StoreDirect} for {@code direct} rows,
 * via {@link StoreWAL} on the BASE path for {@code wal} rows (the fixture placed at
 * {@code <base>.wal} hits the N6 no-migration boundary). Either way the working copy must
 * stay byte-identical and nothing beyond {@code .lock} sidecars may appear. All temp state
 * lives in {@link TmpFiles}-owned directories so nothing leaks into {@code java.io.tmpdir}.
 */
public class XFixtureConformanceTest {

    private static final String RESOURCE_ROOT = "/xfixtures/";

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

    // ---------- manifest model ----------

    private static final class FileRow {
        String fixtureId, relName, rawSha, gzSha;
        long rawLen;
    }

    private static final class Expect {
        String fixtureId, engine, verdict, opener, placeAs, openArg;
    }

    private static final class Manifest {
        final Map<String, String> fixtureKinds = new LinkedHashMap<>();
        final List<FileRow> files = new ArrayList<>();
        final List<Expect> expects = new ArrayList<>();
        final Map<String, List<FixtureWriter.RecidExpect>> recids = new HashMap<>();

        /** Stages 1 and 2: every fixture has exactly one file row (asserted while resolving). */
        FileRow fileOf(String fixtureId) {
            FileRow found = null;
            for (FileRow f : files) {
                if (!f.fixtureId.equals(fixtureId)) continue;
                assertTrue("fixture " + fixtureId + " has more than one file row (Stages 1-2 forbid that)",
                        found == null);
                found = f;
            }
            assertTrue("fixture " + fixtureId + " has no file row", found != null);
            return found;
        }
    }

    /** Parses MANIFEST.tsv; hard-fails on absence, wrong version and unknown row types. */
    private static Manifest loadManifest() throws IOException {
        byte[] bytes;
        try (InputStream in = XFixtureConformanceTest.class.getResourceAsStream(RESOURCE_ROOT + "MANIFEST.tsv")) {
            if (in == null)
                fail("classpath resource " + RESOURCE_ROOT + "MANIFEST.tsv is missing — "
                        + "the fixture sync step was never run for this checkout");
            bytes = in.readAllBytes();
        }
        Manifest m = new Manifest();
        boolean versionSeen = false;
        for (String line : new String(bytes, StandardCharsets.UTF_8).split("\n", -1)) {
            if (line.isBlank() || line.startsWith("#")) continue;
            String[] t = line.split("\t", -1);
            if (!versionSeen) { // the first data line MUST be the version row
                assertTrue("first manifest data line is not 'version\\t1': " + line,
                        t.length == 2 && t[0].equals("version") && t[1].equals("1"));
                versionSeen = true;
                continue;
            }
            switch (t[0]) {
                case "fixture":
                    assertEquals("bad fixture row: " + line, 5, t.length);
                    assertTrue("duplicate fixture row: " + line,
                            !m.fixtureKinds.containsKey(t[1]));
                    m.fixtureKinds.put(t[1], t[2]);
                    break;
                case "file": {
                    assertEquals("bad file row: " + line, 6, t.length);
                    FileRow f = new FileRow();
                    f.fixtureId = t[1];
                    f.relName = t[2];
                    f.rawLen = Long.parseLong(t[3]);
                    f.rawSha = t[4];
                    f.gzSha = t[5];
                    m.files.add(f);
                    break;
                }
                case "expect": {
                    assertEquals("bad expect row: " + line, 7, t.length);
                    Expect e = new Expect();
                    e.fixtureId = t[1];
                    e.engine = t[2];
                    e.verdict = t[3];
                    e.opener = t[4];
                    e.placeAs = t[5];
                    e.openArg = t[6];
                    m.expects.add(e);
                    break;
                }
                case "recid":
                    assertEquals("bad recid row: " + line, 7, t.length);
                    m.recids.computeIfAbsent(t[1], k -> new ArrayList<>()).add(new FixtureWriter.RecidExpect(
                            t[2], Long.parseLong(t[3]), t[4], Integer.parseInt(t[5]), Integer.parseInt(t[6])));
                    break;
                case "recidrange": {
                    assertEquals("bad recidrange row: " + line, 8, t.length);
                    long from = Long.parseLong(t[3]), to = Long.parseLong(t[4]);
                    assertTrue("empty recidrange row: " + line, from <= to);
                    int base = Integer.parseInt(t[6]), len = Integer.parseInt(t[7]);
                    List<FixtureWriter.RecidExpect> list = m.recids.computeIfAbsent(t[1], k -> new ArrayList<>());
                    for (long r = from; r <= to; r++)
                        list.add(new FixtureWriter.RecidExpect(t[2], r, t[5], (int) (base + r - from), len));
                    break;
                }
                case "edit":
                    assertEquals("bad edit row: " + line, 6, t.length);
                    // reject-derivation provenance; the referenced files are consumed pre-edited
                    break;
                default:
                    fail("unknown manifest row type: " + line);
            }
        }
        assertTrue("manifest has no version row", versionSeen);
        return m;
    }

    // ---------- fixture decompression ----------

    /** Gunzips every fixture file once, verifying gz and raw SHA-256 plus raw length up front. */
    private static Map<String, File> gunzipAll(Manifest m, File session) throws IOException {
        Map<String, File> pristine = new HashMap<>();
        for (FileRow f : m.files) {
            byte[] gz;
            try (InputStream in = XFixtureConformanceTest.class.getResourceAsStream(RESOURCE_ROOT + f.relName + ".gz")) {
                if (in == null) fail("fixture resource missing: " + RESOURCE_ROOT + f.relName + ".gz");
                gz = in.readAllBytes();
            }
            assertEquals(f.relName + ".gz: compressed SHA-256 mismatch", f.gzSha, FixtureWriter.sha256Hex(gz));
            ByteArrayOutputStream raw = new ByteArrayOutputStream();
            try (GZIPInputStream gin = new GZIPInputStream(new ByteArrayInputStream(gz))) {
                gin.transferTo(raw);
            }
            byte[] bytes = raw.toByteArray();
            assertEquals(f.relName + ": uncompressed length mismatch", f.rawLen, bytes.length);
            assertEquals(f.relName + ": uncompressed SHA-256 mismatch", f.rawSha, FixtureWriter.sha256Hex(bytes));
            File out = new File(session, f.relName);
            Files.write(out.toPath(), bytes);
            pristine.put(f.relName, out);
        }
        return pristine;
    }

    // ---------- cell execution ----------

    private void runCell(Manifest m, Expect e, File pristine) throws IOException {
        String ctx = "cell[" + e.fixtureId + " java " + e.verdict + " " + e.opener + "]";
        // Stage 2 knows exactly two openers; anything else is a manifest error, not a skip.
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
                // Stage 2 has no java accept-wal rows (the W fixtures are port-format v1).
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
                try {
                    // wal: WalSegmentSet's N6 check throws DataCorruption on the bare <base>.wal
                    // file BEFORE any namespace mutation (only the .lock sidecar precedes it), so
                    // the byte-unchanged and no-new-files assertions below hold for this arm too.
                    Store s = "wal".equals(e.opener)
                            ? new StoreWAL(openTarget)
                            : new StoreDirect(openTarget);
                    s.close();
                    fail(ctx + ": expected DBException.DataCorruption, but the store opened");
                } catch (DBException.DataCorruption expected) {
                    // the engine's corruption class, per the contract
                }
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

    private static TreeSet<String> listNames(File dir) {
        String[] names = dir.list();
        assertTrue("cell dir vanished: " + dir, names != null);
        return new TreeSet<>(List.of(names));
    }

    // ---------- the suite ----------

    @Test public void manifest_cells_conform() throws Exception {
        Manifest m = loadManifest();
        File session = tempDir("xfixtures-session");
        Map<String, File> pristine = gunzipAll(m, session);
        int ran = 0;
        for (Expect e : m.expects) {
            if (!"java".equals(e.engine)) continue;
            runCell(m, e, pristine.get(m.fileOf(e.fixtureId).relName));
            ran++;
        }
        assertTrue("manifest contains no java expect rows", ran > 0);
    }
}
