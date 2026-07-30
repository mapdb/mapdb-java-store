package org.mapdb.btree;

import org.junit.Test;
import org.mapdb.ser.LongFormat;
import org.mapdb.ser.Serializer;
import org.mapdb.ser.Serializers;
import org.mapdb.store.Store;
import org.mapdb.store.StoreOnHeap;

import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;

/**
 * Root-growth identity gate (parity with the Rust
 * {@code crafted_fake_root_descendant_does_not_replace_root} and the Zig
 * "crafted: fake root descendant does not replace root" test).
 *
 * <p>A checksum-valid node falsely flagged {@code LEFT|RIGHT} (root shape) must NOT be
 * treated as the root when it later splits — doing so would grow a NEW root from its
 * halves alone and orphan the genuine root and its siblings (silent data loss). Root
 * growth is gated on authoritative root-pointer identity
 * ({@link BTreeMap#isCurrentRoot}) AND the node flags, not the flags alone.
 *
 * <p>Both split call sites are exercised: {@link #craftedFakeRootLeafDoesNotReplaceRoot}
 * pins the LEAF-split gate and {@link #craftedFakeRootDirectoryDoesNotReplaceRoot} pins
 * the DIRECTORY-split gate inside {@code propagateSplit}. Both assert the actual root
 * POINTER VALUE ({@code store.get(rrr)}) is unchanged — a regression to a flags-only
 * gate either replaces that pointer or (via the leftEdges/level backstop) throws.
 */
public class BTreeMapRootGrowGateTest {

    @SuppressWarnings("unchecked")
    private static Serializer<BTreeMap.Node> nodeSerOf(BTreeMap<?, ?> map) {
        try {
            Field f = BTreeMap.class.getDeclaredField("nodeSer");
            f.setAccessible(true);
            return (Serializer<BTreeMap.Node>) f.get(map);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    /** A nodeSer bound to LongFormat/LongFormat, for writing the crafted nodes. */
    private static Serializer<BTreeMap.Node> longNodeSer() {
        return nodeSerOf(BTreeMap.create(new StoreOnHeap(),
                LongFormat.INSTANCE, LongFormat.INSTANCE, 4));
    }

    // ============================================================ leaf-split gate
    @Test
    public void craftedFakeRootLeafDoesNotReplaceRoot() {
        Store store = new StoreOnHeap();
        Serializer<BTreeMap.Node> nodeSer = longNodeSer();

        long a = store.preallocate();
        long c = store.preallocate();
        long root = store.preallocate();

        // A: crafted LEFT|RIGHT leaf (a "fake root"), full at maxNodeSize=4.
        BTreeMap.Node nodeA = new BTreeMap.Node(
                BTreeMap.LEFT | BTreeMap.RIGHT, 0L,
                new long[]{10, 20, 30, 40}, new long[]{100, 200, 300, 400}, null);
        // C: real rightmost sibling holding data that must NOT be lost.
        BTreeMap.Node nodeC = new BTreeMap.Node(
                BTreeMap.RIGHT, 0L, new long[]{100}, new long[]{1000}, null);
        // R: the genuine root, routes <=50 to A and >50 to C.
        BTreeMap.Node nodeR = new BTreeMap.Node(
                BTreeMap.DIR | BTreeMap.LEFT | BTreeMap.RIGHT, 0L,
                new long[]{50}, new long[]{a, c}, null);
        store.update(a, nodeA, nodeSer);
        store.update(c, nodeC, nodeSer);
        store.update(root, nodeR, nodeSer);
        long rrr = store.put(root, Serializers.LONG);

        BTreeMap<Long, Long> map =
                BTreeMap.open(store, rrr, LongFormat.INSTANCE, LongFormat.INSTANCE, 4);

        // Inserting 45 (routed to A) overflows A → LEAF split. Flags-only would replace
        // the root and lose C; the identity gate propagates the separator into R instead.
        map.put(45L, 450L);

        assertEquals("sibling C must not be orphaned", Long.valueOf(1000L), map.get(100L));
        assertEquals(Long.valueOf(100L), map.get(10L));
        assertEquals(Long.valueOf(400L), map.get(40L));
        assertEquals(Long.valueOf(450L), map.get(45L));
        // The genuine root pointer VALUE is unchanged (no bogus root replacement).
        assertEquals(Long.valueOf(root), store.get(rrr, Serializers.LONG));
    }

    // ============================================================ directory-split gate
    @Test
    public void craftedFakeRootDirectoryDoesNotReplaceRoot() {
        Store store = new StoreOnHeap();
        Serializer<BTreeMap.Node> nodeSer = longNodeSer();

        // Leaves under the crafted directory D. Only L0 is ever dereferenced (buildLeftEdges
        // walks child0, and the put below routes into L0). L1..L4 are valid recids that D's
        // child array carries but that this test never loads.
        long l0 = store.preallocate();
        long l1 = store.preallocate();
        long l2 = store.preallocate();
        long l3 = store.preallocate();
        long l4 = store.preallocate();
        long d = store.preallocate();   // crafted LEFT|RIGHT directory ("fake root")
        long e = store.preallocate();   // genuine right subtree that must not be orphaned
        long root = store.preallocate();

        // L0: leftmost leaf under D (LEFT, not RIGHT → carries an upper fence = 100), full.
        BTreeMap.Node nodeL0 = new BTreeMap.Node(
                BTreeMap.LEFT, l1,
                new long[]{10, 20, 30, 40}, new long[]{100, 200, 300, 400}, new long[]{100});
        // D: crafted DIRECTORY falsely flagged LEFT|RIGHT (root shape), full at 4 keys.
        // A RIGHT dir has keysLen+1 children, so 4 keys → 5 children.
        BTreeMap.Node nodeD = new BTreeMap.Node(
                BTreeMap.DIR | BTreeMap.LEFT | BTreeMap.RIGHT, 0L,
                new long[]{100, 200, 300, 400}, new long[]{l0, l1, l2, l3, l4}, null);
        // E: genuine rightmost subtree (a leaf) holding data that must NOT be lost.
        BTreeMap.Node nodeE = new BTreeMap.Node(
                BTreeMap.RIGHT, 0L, new long[]{1000}, new long[]{10000}, null);
        // R: the genuine root, routes <=500 to D and >500 to E.
        BTreeMap.Node nodeR = new BTreeMap.Node(
                BTreeMap.DIR | BTreeMap.LEFT | BTreeMap.RIGHT, 0L,
                new long[]{500}, new long[]{d, e}, null);
        store.update(l0, nodeL0, nodeSer);
        store.update(d, nodeD, nodeSer);
        store.update(e, nodeE, nodeSer);
        store.update(root, nodeR, nodeSer);
        long rrr = store.put(root, Serializers.LONG);

        BTreeMap<Long, Long> map =
                BTreeMap.open(store, rrr, LongFormat.INSTANCE, LongFormat.INSTANCE, 4);

        // Inserting 25 routes R→D→L0 and overflows L0. Its separator (20) propagates into D,
        // overflowing D (5 keys) → DIRECTORY split. Flags-only would grow a new root from D's
        // halves (replacing the pointer / orphaning E); the identity gate propagates D's
        // separator into the genuine root R instead.
        map.put(25L, 250L);

        // E and the genuine root survive.
        assertEquals("right subtree E must not be orphaned", Long.valueOf(10000L), map.get(1000L));
        assertEquals(Long.valueOf(100L), map.get(10L)); // L0' still reachable
        assertEquals(Long.valueOf(250L), map.get(25L)); // the split-off right half reachable
        // The genuine root pointer VALUE is unchanged (no bogus root replacement).
        assertEquals(Long.valueOf(root), store.get(rrr, Serializers.LONG));
    }
}
