package org.mapdb.btree;

import org.junit.Test;
import org.mapdb.DBException;
import org.mapdb.ser.LongFormat;
import org.mapdb.store.StoreOnHeap;

import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Hard-corruption backstop for a stale/mismatched {@code leftEdges} at a root grow
 * (the guard at {@code propagateSplit}'s root-grow branch — the same guard fix 2's
 * tx refresh keeps a rollback from tripping). The zig/rust spec require this to be a
 * HARD {@link DBException.DataCorruption} plus {@code poisoned = true}, NEVER an
 * assert-only path — so the throw must not depend on {@code -ea}, and the map must
 * fail fast on every later op.
 *
 * <p>A controlled mismatch is forced by reflectively overwriting {@code leftEdges}
 * with a wrong-length array (the same reflection technique
 * {@link BTreeMapLockProtocolTest} uses), then triggering a root grow whose
 * {@code level} disagrees with that length.
 */
public class BTreeMapLeftEdgesMismatchTest {

    private static void setLeftEdges(BTreeMap<?, ?> map, long[] bogus) {
        try {
            Field f = BTreeMap.class.getDeclaredField("leftEdges");
            f.setAccessible(true);
            f.set(map, bogus);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private static boolean isPoisoned(BTreeMap<?, ?> map) {
        try {
            Field f = BTreeMap.class.getDeclaredField("poisoned");
            f.setAccessible(true);
            return f.getBoolean(map);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    @Test
    public void leftEdgesLevelMismatchIsHardCorruptionAndPoisons() {
        StoreOnHeap store = new StoreOnHeap();
        BTreeMap<Long, Long> map =
                BTreeMap.create(store, LongFormat.INSTANCE, LongFormat.INSTANCE, 4);
        // Fill the single-leaf root to capacity WITHOUT splitting: leftEdges = [rootLeaf].
        for (long k = 1; k <= 4; k++) map.put(k, k * 10);

        // Force a stale structural cache: a 3-level array while the tree is height 1. The
        // next root grow ascends to level 1 and finds leftEdges.length (3) != level (1).
        setLeftEdges(map, new long[]{999L, 998L, 997L});

        // Overflow the root leaf → leaf split → root grow → mismatch. This must be an
        // explicit thrown DataCorruption (NOT an AssertionError): the throw is
        // unconditional code, so it fires the same with -ea on or off.
        try {
            map.put(5L, 50L);
            fail("expected a hard DataCorruption on the leftEdges/level mismatch");
        } catch (DBException.DataCorruption expected) {
            assertTrue("mismatch message: " + expected.getMessage(),
                    expected.getMessage().contains("leftEdges/level mismatch"));
        } catch (AssertionError ae) {
            fail("mismatch must be a thrown DataCorruption, not an assertion: " + ae);
        }

        // The map is now poisoned...
        assertTrue("map must be poisoned after the failed root grow", isPoisoned(map));

        // ...so every SUBSEQUENT op fails fast through checkPoison (a read here).
        try {
            map.get(1L);
            fail("expected a poisoned map to reject reads");
        } catch (DBException.DataCorruption poison) {
            assertTrue("poison message: " + poison.getMessage(),
                    poison.getMessage().contains("poisoned"));
        }
        // ...and writes.
        try {
            map.put(6L, 60L);
            fail("expected a poisoned map to reject writes");
        } catch (DBException.DataCorruption poison) {
            assertTrue(poison.getMessage().contains("poisoned"));
        }
    }

    /** Sanity: {@code leftEdges} is exactly the field name reflected above. */
    @Test
    public void leftEdgesFieldExists() throws NoSuchFieldException {
        Field f = BTreeMap.class.getDeclaredField("leftEdges");
        assertEquals(long[].class, f.getType());
    }
}
