package org.mapdb.store;

import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * On-volume geometry pins + long-stack conformance (mapdb3 StoreDirectTest
 * constants()/recid2Offset()/longStack_putTake lineage): the header layout is an
 * on-disk contract, recid→offset must be exact across the zero-page and full-page
 * boundaries, and the long stack must be strictly LIFO through chunk growth,
 * chunk release and file reopen.
 */
public class StoreDirectLongStackTest {

    private final List<File> files = new ArrayList<>();

    private File newFile() throws IOException {
        File f = Files.createTempFile("mapdb5-longstack", ".db").toFile();
        f.delete();
        files.add(f);
        return f;
    }

    @After public void cleanup() {
        for (File f : files) f.delete();
        files.clear();
    }

    // ---------- header geometry is an on-disk contract: pin it ----------

    @Test public void header_constants_pinned() {
        assertEquals(1 << 20, StoreDirect.PAGE_SIZE);
        assertEquals(64, StoreDirect.O_FREE_RECID_STACK);
        assertEquals(72, StoreDirect.O_FREE_DATA_STACKS);
        assertEquals(0xFFFD, StoreDirect.MAX_CAP_UNITS);
        assertEquals(524_336, StoreDirect.HEAD_END);
        assertEquals(StoreDirect.HEAD_END, StoreDirect.ZERO_PAGE_LINK);
        assertEquals(524_352, StoreDirect.ZERO_SLOTS_START);
        assertEquals(65_528, StoreDirect.RECIDS_PER_ZERO_PAGE);
        assertEquals(131_070, StoreDirect.RECIDS_PER_PAGE);
        assertEquals(72, StoreDirect.masterLinkOffset(1));
        assertEquals(StoreDirect.HEAD_END - 8, StoreDirect.masterLinkOffset(StoreDirect.MAX_CAP_UNITS));
        assertEquals(IndexVal.MAX_CAPACITY - 12, StoreDirect.MAX_CHUNK_DATA);
    }

    // ---------- recid -> index slot offset math ----------

    @Test public void recid_to_offset_across_page_boundaries() {
        StoreDirect s = new StoreDirect();
        try {
            long rpz = StoreDirect.RECIDS_PER_ZERO_PAGE;
            long rpp = StoreDirect.RECIDS_PER_PAGE;
            s.testEnsureIndexCapacity(rpz + 2 * rpp + 5); // forces two overflow pages
            long[] pages = s.testIndexPages();
            assertEquals(3, pages.length);

            // zero page: first and last slot
            assertEquals(StoreDirect.ZERO_SLOTS_START, s.recidToOffset(1));
            assertEquals(StoreDirect.PAGE_SIZE - 8, s.recidToOffset(rpz));
            // page 1: first and last slot
            assertEquals(pages[0] + 16, s.recidToOffset(rpz + 1));
            assertEquals(pages[0] + StoreDirect.PAGE_SIZE - 8, s.recidToOffset(rpz + rpp));
            // page 2 boundary
            assertEquals(pages[1] + 16, s.recidToOffset(rpz + rpp + 1));
            assertEquals(pages[1] + StoreDirect.PAGE_SIZE - 8, s.recidToOffset(rpz + 2 * rpp));
            assertEquals(pages[2] + 16, s.recidToOffset(rpz + 2 * rpp + 1));

            // slots advance by exactly 8 within a page
            for (long recid : new long[]{2, rpz - 1, rpz + 2, rpz + rpp - 1, rpz + rpp + 2}) {
                assertEquals("adjacent slots 8 bytes apart, recid=" + recid,
                        8, s.recidToOffset(recid + 1) - s.recidToOffset(recid));
            }
            // exhaustive: every recid in the two boundary windows maps to a unique in-range slot
            java.util.HashSet<Long> seen = new java.util.HashSet<>();
            for (long recid = rpz - 3; recid <= rpz + rpp + 3; recid++) {
                long off = s.recidToOffset(recid);
                assertTrue("slot in bounds, recid=" + recid, off >= StoreDirect.ZERO_SLOTS_START);
                assertEquals("slot aligned, recid=" + recid, 0, off & 7);
                assertTrue("slot not in a page header, recid=" + recid, (off % StoreDirect.PAGE_SIZE) >= 16
                        || off < StoreDirect.PAGE_SIZE);
                assertTrue("duplicate slot for recid " + recid, seen.add(off));
            }
        } finally {
            s.close();
        }
    }

    // ---------- long stack: LIFO under randomized put/take, oracle-checked ----------

    @Test public void long_stack_lifo_fuzz_single_stack() {
        StoreDirect s = new StoreDirect();
        try {
            long master = StoreDirect.masterLinkOffset(7);
            Random rnd = new Random(4242);
            ArrayDeque<Long> oracle = new ArrayDeque<>();
            for (int op = 0; op < 20_000; op++) {
                if (oracle.isEmpty() || rnd.nextInt(100) < 55) {
                    long v = Parity.p1set(((rnd.nextLong() & 0xFFFF_FFF0L) | 16));
                    s.testLongStackPut(master, v);
                    oracle.addLast(v);
                } else {
                    assertEquals("LIFO pop mismatch at op " + op,
                            (long) oracle.removeLast(), s.testLongStackTake(master));
                }
            }
            while (!oracle.isEmpty()) {
                assertEquals((long) oracle.removeLast(), s.testLongStackTake(master));
            }
            assertEquals("stack must drain to empty", 0, s.testLongStackTake(master));
            // all fuzz chunks got recycled through the free lists: full accounting holds
            s.verify();
        } finally {
            s.close();
        }
    }

    @Test public void long_stack_lifo_fuzz_multiple_stacks() {
        StoreDirect s = new StoreDirect();
        try {
            long[] masters = {
                    StoreDirect.O_FREE_RECID_STACK,
                    StoreDirect.masterLinkOffset(1),
                    StoreDirect.masterLinkOffset(255),
                    StoreDirect.masterLinkOffset(StoreDirect.MAX_CAP_UNITS),
            };
            Random rnd = new Random(777);
            @SuppressWarnings("unchecked")
            ArrayDeque<Long>[] oracle = new ArrayDeque[masters.length];
            for (int i = 0; i < masters.length; i++) oracle[i] = new ArrayDeque<>();
            for (int op = 0; op < 30_000; op++) {
                int i = rnd.nextInt(masters.length);
                if (oracle[i].isEmpty() || rnd.nextInt(100) < 55) {
                    long v = Parity.p1set(((rnd.nextLong() & 0x3FF_FFFF_FFF0L) | 16));
                    s.testLongStackPut(masters[i], v);
                    oracle[i].addLast(v);
                } else {
                    assertEquals("stack " + i + " LIFO mismatch at op " + op,
                            (long) oracle[i].removeLast(), s.testLongStackTake(masters[i]));
                }
            }
            for (int i = 0; i < masters.length; i++) {
                while (!oracle[i].isEmpty())
                    assertEquals((long) oracle[i].removeLast(), s.testLongStackTake(masters[i]));
                assertEquals(0, s.testLongStackTake(masters[i]));
            }
            s.verify();
        } finally {
            s.close();
        }
    }

    // ---------- long stack content survives close/reopen ----------

    @Test public void long_stack_persists_across_reopen() throws IOException {
        File f = newFile();
        long master = StoreDirect.masterLinkOffset(33);
        ArrayDeque<Long> oracle = new ArrayDeque<>();
        Random rnd = new Random(99);
        StoreDirect s = new StoreDirect(f);
        try {
            for (int i = 0; i < 700; i++) { // several chunks worth
                long v = Parity.p1set(((rnd.nextLong() & 0xFFFF_FFF0L) | 16));
                s.testLongStackPut(master, v);
                oracle.addLast(v);
            }
        } finally {
            s.close();
        }
        StoreDirect s2 = new StoreDirect(f);
        try {
            while (!oracle.isEmpty()) {
                assertEquals("post-reopen LIFO mismatch",
                        (long) oracle.removeLast(), s2.testLongStackTake(master));
            }
            assertEquals(0, s2.testLongStackTake(master));
            s2.verify();
        } finally {
            s2.close();
        }
    }
}
