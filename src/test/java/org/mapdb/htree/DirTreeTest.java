package org.mapdb.htree;

import org.junit.Test;
import org.mapdb.store.Store;
import org.mapdb.store.StoreByteArray;
import org.mapdb.store.StoreOnHeap;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Direct tests of the {@link DirTree} algorithm (§0.10): deterministic lazy-split
 * paths plus a random fuzz vs a HashMap oracle. Runs on both an object store
 * (StoreOnHeap: aliasing/mutation discipline) and a serializing store
 * (StoreByteArray: DIR_SER round-trips).
 */
public class DirTreeTest {

    private static final int DIR_SHIFT = 7;
    private static final int LEVELS = 4; // root call passes level = LEVELS - 1
    private static final long INDEX_MASK = (1L << (LEVELS * DIR_SHIFT)) - 1;

    private interface TreeCase {
        void run(Store store);
    }

    private static void onBothStores(TreeCase c) {
        for (Store store : new Store[]{new StoreOnHeap(), new StoreByteArray()}) {
            c.run(store);
            store.verify();
            store.close();
        }
    }

    /**
     * §0.10 targeted split ladder: indices share the top-level slot and diverge at
     * each successively lower level, driving every treePutSub chain depth. All
     * previously inserted indices must survive every split step.
     */
    @Test
    public void targetedSplits() {
        onBothStores(store -> {
            long root = store.put(DirTree.dirEmpty(), DirTree.DIR_SER);

            long[] indices = {
                    0,                          // terminal in root slot 0
                    1,                          // same slots at levels 3..1, splits down to level 0
                    1L << 7,                    // diverges at level 1
                    1L << 14,                   // diverges at level 2
                    1L << 21,                   // diverges at level 3 (top)
                    (5L << 21) | (3L << 14),
                    (5L << 21) | (9L << 7),     // shares top slot with previous
                    (5L << 21) | (9L << 7) | 1, // shares levels 3..1 with previous
                    INDEX_MASK,                 // max index
            };

            Map<Long, Long> oracle = new HashMap<>();
            for (long idx : indices) {
                long value = idx + 7777; // any non-zero recid-like value
                DirTree.treePut(DIR_SHIFT, root, store, LEVELS - 1, idx, value);
                oracle.put(idx, value);
                for (Map.Entry<Long, Long> e : oracle.entrySet()) {
                    assertEquals("treeGet " + e.getKey() + " after inserting " + idx,
                            (long) e.getValue(),
                            DirTree.treeGet(DIR_SHIFT, root, store, LEVELS - 1, e.getKey()));
                }
            }

            // absent indices sharing slots with present ones must miss
            for (long absent : new long[]{2, 3L << 7, (5L << 21) | (9L << 7) | 2, INDEX_MASK - 1}) {
                assertEquals("absent " + absent, 0,
                        DirTree.treeGet(DIR_SHIFT, root, store, LEVELS - 1, absent));
            }

            // overwrite value under an existing index
            DirTree.treePut(DIR_SHIFT, root, store, LEVELS - 1, 1, 99999);
            assertEquals(99999, DirTree.treeGet(DIR_SHIFT, root, store, LEVELS - 1, 1));
            oracle.put(1L, 99999L);

            // treeFold visits exactly the oracle's entries
            Map<Long, Long> folded = new HashMap<>();
            DirTree.treeFold(root, store, LEVELS - 1, (index, value) -> folded.put(index, value));
            assertEquals(oracle, folded);
        });
    }

    @Test
    public void emptyTree() {
        onBothStores(store -> {
            long root = store.put(DirTree.dirEmpty(), DirTree.DIR_SER);
            assertEquals(0, DirTree.treeGet(DIR_SHIFT, root, store, LEVELS - 1, 0));
            assertEquals(0, DirTree.treeGet(DIR_SHIFT, root, store, LEVELS - 1, INDEX_MASK));
            DirTree.treeFold(root, store, LEVELS - 1,
                    (index, value) -> assertTrue("empty tree visited " + index, false));
        });
    }

    @Test
    public void fuzzVsHashMap() {
        onBothStores(store -> {
            long root = store.put(DirTree.dirEmpty(), DirTree.DIR_SER);
            Map<Long, Long> oracle = new HashMap<>();
            Random rnd = new Random(0xD1B7BEE);

            for (int i = 0; i < 50_000; i++) {
                long idx = rnd.nextLong() & INDEX_MASK;
                if (rnd.nextInt(100) < 70) { // put
                    long value = 1 + (rnd.nextLong() & 0xFFFFFFFFL);
                    DirTree.treePut(DIR_SHIFT, root, store, LEVELS - 1, idx, value);
                    oracle.put(idx, value);
                } else { // get
                    Long expected = oracle.get(idx);
                    assertEquals("get[" + i + "] idx=" + idx,
                            expected == null ? 0 : expected,
                            DirTree.treeGet(DIR_SHIFT, root, store, LEVELS - 1, idx));
                }
            }

            // full verification: every oracle entry gettable, fold matches exactly
            for (Map.Entry<Long, Long> e : oracle.entrySet()) {
                assertEquals((long) e.getValue(),
                        DirTree.treeGet(DIR_SHIFT, root, store, LEVELS - 1, e.getKey()));
            }
            Map<Long, Long> folded = new HashMap<>();
            DirTree.treeFold(root, store, LEVELS - 1, (index, value) -> folded.put(index, value));
            assertEquals(oracle, folded);
        });
    }

    // ================= Tier-1: treeRemove (collapsing) / treeClear / treeIsEmpty =================

    /**
     * Targeted removal over the split ladder: indices share upper slots and diverge
     * low, so the subdir chains built by treePutSub must COLLAPSE step by step as
     * entries are removed — after every removal the removed index misses, a repeat
     * remove returns false, and every survivor still resolves (a mis-collapse would
     * orphan or mis-route survivors). onBothStores' store.verify() closes the loop
     * on freed records.
     */
    @Test
    public void targetedRemoveCollapsing() {
        onBothStores(store -> {
            long root = store.put(DirTree.dirEmpty(), DirTree.DIR_SER);

            long[] indices = {
                    0,                          // terminal in root slot 0
                    1,                          // same slots at levels 3..1, splits down to level 0
                    1L << 7,                    // diverges at level 1
                    1L << 14,                   // diverges at level 2
                    1L << 21,                   // diverges at level 3 (top)
                    (5L << 21) | (3L << 14),
                    (5L << 21) | (9L << 7),     // shares top slot with previous
                    (5L << 21) | (9L << 7) | 1, // shares levels 3..1 with previous
                    INDEX_MASK,                 // max index
            };

            Map<Long, Long> oracle = new HashMap<>();
            for (long idx : indices) {
                DirTree.treePut(DIR_SHIFT, root, store, LEVELS - 1, idx, idx + 7777);
                oracle.put(idx, idx + 7777);
            }
            assertFalse(DirTree.treeIsEmpty(root, store));

            // removing ABSENT indices that share slots with present ones is a no-op
            for (long absent : new long[]{2, 3L << 7, (5L << 21) | (9L << 7) | 2, INDEX_MASK - 1}) {
                assertFalse("absent " + absent,
                        DirTree.treeRemove(DIR_SHIFT, root, store, LEVELS - 1, absent));
                assertEquals(oracle.size(), foldCount(store, root));
            }

            // remove one at a time (insertion order walks every collapse depth)
            for (long idx : indices) {
                assertTrue("remove " + idx,
                        DirTree.treeRemove(DIR_SHIFT, root, store, LEVELS - 1, idx));
                oracle.remove(idx);
                assertEquals("removed " + idx + " must miss", 0,
                        DirTree.treeGet(DIR_SHIFT, root, store, LEVELS - 1, idx));
                assertFalse("double remove " + idx,
                        DirTree.treeRemove(DIR_SHIFT, root, store, LEVELS - 1, idx));
                for (Map.Entry<Long, Long> e : oracle.entrySet()) {
                    assertEquals("survivor " + e.getKey() + " after removing " + idx,
                            (long) e.getValue(),
                            DirTree.treeGet(DIR_SHIFT, root, store, LEVELS - 1, e.getKey()));
                }
            }

            // fully drained: root record survives, content collapsed back to empty,
            // and every subdir record was freed (root is the only live record)
            assertTrue(DirTree.treeIsEmpty(root, store));
            assertEquals(2, store.get(root, DirTree.DIR_SER).length);
            assertEquals("treeRemove leaked dir records", 1, liveRecidCount(store));

            // collapse-then-regrow: the ladder re-splits cleanly on the same root
            for (long idx : indices) {
                DirTree.treePut(DIR_SHIFT, root, store, LEVELS - 1, idx, idx + 1);
            }
            for (long idx : indices) {
                assertEquals(idx + 1, DirTree.treeGet(DIR_SHIFT, root, store, LEVELS - 1, idx));
            }
        });
    }

    /** Root stability regression: with exactly two terminals in the ROOT, removing one
     *  must NOT delete/collapse the root record (§0.6) — the survivor stays resolvable
     *  through the same root recid. (mapdb3's level convention never hits this case;
     *  ours does, hence the explicit topLevel guard in the terminal branch.) */
    @Test
    public void rootWithTwoTerminalsSurvivesRemove() {
        onBothStores(store -> {
            long root = store.put(DirTree.dirEmpty(), DirTree.DIR_SER);
            // distinct ROOT slots (bits [21,28)): both stay terminal in the root
            long a = 1L << 21, b = 2L << 21;
            DirTree.treePut(DIR_SHIFT, root, store, LEVELS - 1, a, 111);
            DirTree.treePut(DIR_SHIFT, root, store, LEVELS - 1, b, 222);

            assertTrue(DirTree.treeRemove(DIR_SHIFT, root, store, LEVELS - 1, a));
            // root record still there, holding the lone survivor as a plain terminal
            assertEquals(222, DirTree.treeGet(DIR_SHIFT, root, store, LEVELS - 1, b));
            assertEquals(4, store.get(root, DirTree.DIR_SER).length);

            assertTrue(DirTree.treeRemove(DIR_SHIFT, root, store, LEVELS - 1, b));
            assertTrue(DirTree.treeIsEmpty(root, store));
            assertEquals(0, DirTree.treeGet(DIR_SHIFT, root, store, LEVELS - 1, b));
        });
    }

    /** Random put/remove/get fuzz vs a HashMap oracle; narrow indices force deep
     *  chains at shared slots, so collapse paths run constantly. */
    @Test
    public void fuzzWithRemoves() {
        onBothStores(store -> {
            long root = store.put(DirTree.dirEmpty(), DirTree.DIR_SER);
            Map<Long, Long> oracle = new HashMap<>();
            Random rnd = new Random(0xDECAF5EEDL);

            for (int i = 0; i < 60_000; i++) {
                // 3/4 narrow (deep shared chains), 1/4 across the whole index space
                long idx = (rnd.nextInt(4) > 0 ? rnd.nextInt(1 << 12) : rnd.nextLong()) & INDEX_MASK;
                int roll = rnd.nextInt(100);
                if (roll < 50) { // put
                    long value = 1 + (rnd.nextLong() & 0xFFFFFFFFL);
                    DirTree.treePut(DIR_SHIFT, root, store, LEVELS - 1, idx, value);
                    oracle.put(idx, value);
                } else if (roll < 85) { // remove
                    assertEquals("remove[" + i + "] idx=" + idx,
                            oracle.remove(idx) != null,
                            DirTree.treeRemove(DIR_SHIFT, root, store, LEVELS - 1, idx));
                } else { // get
                    Long expected = oracle.get(idx);
                    assertEquals("get[" + i + "] idx=" + idx,
                            expected == null ? 0 : expected,
                            DirTree.treeGet(DIR_SHIFT, root, store, LEVELS - 1, idx));
                }
            }

            // full sweep: every oracle entry resolvable, fold matches exactly
            for (Map.Entry<Long, Long> e : oracle.entrySet()) {
                assertEquals((long) e.getValue(),
                        DirTree.treeGet(DIR_SHIFT, root, store, LEVELS - 1, e.getKey()));
            }
            Map<Long, Long> folded = new HashMap<>();
            DirTree.treeFold(root, store, LEVELS - 1, (index, value) -> folded.put(index, value));
            assertEquals(oracle, folded);
            assertEquals(oracle.isEmpty(), DirTree.treeIsEmpty(root, store));

            // drain everything through treeRemove: tree must collapse to empty
            for (Long idx : new java.util.ArrayList<>(oracle.keySet())) {
                assertTrue(DirTree.treeRemove(DIR_SHIFT, root, store, LEVELS - 1, idx));
            }
            assertTrue(DirTree.treeIsEmpty(root, store));
            assertEquals(2, store.get(root, DirTree.DIR_SER).length);
            assertEquals("fuzz drain leaked dir records", 1, liveRecidCount(store));
        });
    }

    /** treeClear frees the whole tree in one pass: visitor sees exactly the live
     *  entries, root content resets in place, and the tree is reusable. */
    @Test
    public void clearVisitsAllAndResets() {
        onBothStores(store -> {
            long root = store.put(DirTree.dirEmpty(), DirTree.DIR_SER);

            // clear on an empty tree: no visits, root intact
            DirTree.treeClear(root, store, LEVELS - 1,
                    (index, value) -> assertTrue("empty tree visited " + index, false));
            assertTrue(DirTree.treeIsEmpty(root, store));

            Map<Long, Long> oracle = new HashMap<>();
            Random rnd = new Random(0xC1EA5);
            for (int i = 0; i < 5_000; i++) {
                long idx = rnd.nextLong() & INDEX_MASK;
                long value = 1 + (rnd.nextLong() & 0xFFFFFFFFL);
                DirTree.treePut(DIR_SHIFT, root, store, LEVELS - 1, idx, value);
                oracle.put(idx, value);
            }

            Map<Long, Long> visited = new HashMap<>();
            DirTree.treeClear(root, store, LEVELS - 1, (index, value) -> visited.put(index, value));
            assertEquals("clear visits exactly the live entries", oracle, visited);
            assertTrue(DirTree.treeIsEmpty(root, store));
            assertEquals(2, store.get(root, DirTree.DIR_SER).length);
            assertEquals("treeClear leaked dir records", 1, liveRecidCount(store));
            for (Long idx : oracle.keySet()) {
                assertEquals(0, DirTree.treeGet(DIR_SHIFT, root, store, LEVELS - 1, idx));
            }

            // reusable after clear
            DirTree.treePut(DIR_SHIFT, root, store, LEVELS - 1, 42, 4242);
            assertEquals(4242, DirTree.treeGet(DIR_SHIFT, root, store, LEVELS - 1, 42));
        });
    }

    private static int foldCount(Store store, long root) {
        int[] n = new int[1];
        DirTree.treeFold(root, store, LEVELS - 1, (index, value) -> n[0]++);
        return n[0];
    }

    /** Live records in the store — the dir-record leak gate. */
    private static long liveRecidCount(Store store) {
        long n = 0;
        for (java.util.PrimitiveIterator.OfLong it = store.getAllRecids(); it.hasNext(); it.nextLong()) n++;
        return n;
    }
}
