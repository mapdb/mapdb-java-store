package org.mapdb.xfixtures;

import org.junit.After;
import org.junit.Test;
import org.mapdb.DBException;
import org.mapdb.TmpFiles;
import org.mapdb.store.StoreDirect;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * StoreDirect cross-port harness (Stage C, <b>C7j</b> residual).
 *
 * <p>Contract §9 retires the WAL schema-v1 tree and its skip lists; it does
 * <b>not</b> retire the StoreDirect accept images ({@code direct-v1-*}) or the
 * shared malformed-StoreDirect reject images. Those lived under the same
 * schema-v1 root and would have vanished with it, so C7 keeps them here as a
 * dedicated schema-v2 root ({@code /xfixtures-direct/}) with a harness that
 * does not reintroduce the dual v1/v2 dispatch.
 *
 * <p>The v2 WAL executor refuses a non-{@code wal3} accept opener by design
 * (corpus shape); this harness is the StoreDirect counterpart.
 */
public class XFixtureDirectTest {

    private static final String ROOT = "/xfixtures-direct/";

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

    @Test public void store_direct_cross_port_cells_conform() throws Exception {
        XFixtureManifest.Loaded loaded = XFixtureManifest.load(ROOT);
        assertEquals("the StoreDirect residual root must be schema v2", 2, loaded.version);
        XFixtureManifest.V2 m = loaded.v2;
        File session = tempDir("xf-direct-session");
        XFixtureV2Executor.gunzipAll(m, ROOT, session);

        int accepts = 0, rejects = 0;
        for (XFixtureManifest.V2.Expect e : m.expects) {
            if (!"java".equals(e.engine)) continue;
            assertEquals("this harness only runs the direct opener", "direct", e.opener);
            assertEquals("StoreDirect has no read-only cell in this harness", "rw", e.mode);

            File cell = tempDir("xf-direct-cell");
            // place every file of the fixture
            for (XFixtureManifest.V2.FileRow f : m.files) {
                if (!f.fixtureId.equals(e.fixtureId)) continue;
                File src = new File(new File(session, f.fixtureId), f.relName);
                File dst = new File(cell, f.relName);
                Files.copy(src.toPath(), dst.toPath());
            }
            File target = new File(cell, e.openArg);
            byte[] before = Files.readAllBytes(target.toPath());
            TreeSet<String> namesBefore = XFixtureV2Executor.listNames(cell);

            if ("accept".equals(e.verdict)) {
                StoreDirect s = new StoreDirect(target);
                try {
                    List<FixtureWriter.RecidExpect> expects = m.recids.get(e.fixtureId);
                    assertTrue(e.fixtureId + ": accept fixture has no recid rows",
                            expects != null && !expects.isEmpty());
                    FixtureWriter.assertReaderContract(s, expects, "direct " + e.fixtureId);
                } finally {
                    s.close();
                }
                accepts++;
            } else if ("reject".equals(e.verdict)) {
                assertRefusedOnOpen(e.fixtureId, () -> new StoreDirect(target));
                rejects++;
            } else {
                fail("unknown verdict " + e.verdict);
            }

            assertArrayEquals(e.fixtureId + ": working copy bytes changed",
                    before, Files.readAllBytes(target.toPath()));
            for (String name : XFixtureV2Executor.listNames(cell)) {
                if (namesBefore.contains(name)) continue;
                assertTrue(e.fixtureId + ": unexpected new file " + name, name.endsWith(".lock"));
            }
        }
        assertEquals("missing a StoreDirect accept cell (3 writers × this reader)", 3, accepts);
        assertEquals("missing a StoreDirect reject cell (4 shared malformed images)", 4, rejects);
    }

    /** How a cell opens its store; anything the reject probe is allowed to grade. */
    interface Opener { AutoCloseable open(); }

    /**
     * The reject probe: <b>only the OPEN is graded</b>.
     *
     * <p>The close used to sit inside the same {@code try}, so a {@code DataCorruption} thrown
     * while CLOSING a store that had opened successfully was caught as "the store refused" and the
     * cell went green on the opposite outcome (review r1). A malformed image that opens and then
     * fails on the way out is not a refusal — it is an engine that accepted a file the contract
     * says every engine must reject, which is the finding this harness exists to make.
     *
     * <p>A handle that did open is closed outside the graded region and its outcome is discarded,
     * because the cell is already failing on the open and reporting the close would name the wrong
     * event. {@link #the_reject_probe_grades_only_the_open} is the red.
     */
    static void assertRefusedOnOpen(String ctx, Opener opener) {
        AutoCloseable opened;
        try {
            opened = opener.open();
        } catch (DBException.DataCorruption expected) {
            return;                 // contract: the shared malformed image is refused by every engine
        }
        try {
            opened.close();
        } catch (Exception whileFailingAnyway) {
            // deliberately dropped; the fail() below is the finding
        }
        fail(ctx + ": expected DBException.DataCorruption, but opened");
    }

    /**
     * The probe's own diagonal: refusing ON OPEN passes, and the two ways of NOT refusing on open
     * both fail — including the one the corpus cannot produce, a store that opens and then throws
     * the corruption verdict from {@code close()}.
     *
     * <p>That case is exactly what the old shape counted as a reject, and no fixture in the tree
     * exhibits it, so a doctored opener is the only input that can see it. Restore the old
     * arrangement (open and close inside one {@code try}) and the second case here goes green.
     */
    @Test public void the_reject_probe_grades_only_the_open() {
        assertRefusedOnOpen("refused on open", () -> {
            throw new DBException.DataCorruption("not a mapdb StoreDirect file (bad magic)");
        });
        refuses("a store that OPENED and threw the corruption verdict from close()",
                () -> assertRefusedOnOpen("closes badly", () -> () -> {
                    throw new DBException.DataCorruption("not a mapdb StoreDirect file (bad magic)");
                }));
        refuses("a store that opened and closed cleanly",
                () -> assertRefusedOnOpen("opens fine", () -> () -> { }));
    }

    private static void refuses(String what, Runnable probe) {
        try {
            probe.run();
        } catch (AssertionError expected) {
            return;
        }
        fail("the reject probe accepted " + what);
    }
}
