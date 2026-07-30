package org.mapdb.btree;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mapdb.ser.LongFormat;
import org.mapdb.store.StoreWAL;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Transactional structural-generation refresh (parity with the Rust port's fix and the Zig
 * port's "wal: rollback then regrow keeps left_edges consistent" /
 * "wal: repeated rollback cycles advance generation" tests).
 *
 * <p>A transactional-store {@code rollback()} can shrink the tree height out-of-band
 * while an open {@link BTreeMap} keeps its longer, append-only {@code leftEdges} cache.
 * A later root grow would then append onto that stale array (whose entries name
 * deleted/reused recids) — or, thanks to the {@code leftEdges/level} mismatch backstop,
 * fail hard with a spurious {@code DataCorruption}. The refresh keyed on the store's
 * {@link org.mapdb.store.Store#structuralGeneration()} rebuilds the cache once after
 * each rollback so a post-rollback regrow succeeds.
 *
 * <p>PRE-FIX: without the refresh, the first put after the rollback below throws
 * {@code DBException.DataCorruption("… leftEdges/level mismatch …")} deterministically —
 * the committed baseline is height 1 (leftEdges length 1) while the stale cache is
 * several levels tall, so the first regrow trips the mismatch guard.
 */
public class BTreeMapTxRegrowTest {

    private File file;

    @Before
    public void setUp() throws IOException {
        file = File.createTempFile("btree-tx-regrow", ".wal");
        file.delete(); // StoreWAL will (re)create it
    }

    @After
    public void tearDown() {
        if (file != null) file.delete();
    }

    @Test
    public void rollbackThenRegrowKeepsLeftEdgesConsistent() {
        final long rrr;
        {
            StoreWAL store = new StoreWAL(file);
            // counter-enabled so sizeLong() reads the tx-visible O(1) count (which also
            // reverts on rollback), matching the rust/zig ports.
            BTreeMap<Long, Long> map =
                    BTreeMap.create(store, LongFormat.INSTANCE, LongFormat.INSTANCE, 4, true);
            store.commit(); // committed baseline: empty map, height 1

            final int n = 200; // maxNodeSize=4 → this grows the tree several levels
            for (long k = 0; k < n; k++) map.put(k, k);
            assertEquals(n, map.sizeLong());

            store.rollback();
            assertEquals("rollback reverts to the committed empty baseline", 0, map.sizeLong());

            // Regrow past several root grows onto the now-shrunk tree. Pre-fix this throws
            // DataCorruption on the first regrow; post-fix leftEdges is rebuilt and it works.
            for (long k = 0; k < n; k++) map.put(k, k * 10);
            assertEquals(n, map.sizeLong());

            long i = 0;
            for (Iterator<Map.Entry<Long, Long>> it = map.entryIterator(); it.hasNext(); i++) {
                Map.Entry<Long, Long> e = it.next();
                assertEquals(Long.valueOf(i), e.getKey());
                assertEquals(Long.valueOf(i * 10), e.getValue());
            }
            assertEquals(n, i);

            store.commit();
            rrr = map.rootRecidRecid();
            store.close();
        }

        // Reopen: the committed post-regrow tree must be intact and correct.
        StoreWAL store2 = new StoreWAL(file);
        store2.verify();
        BTreeMap<Long, Long> m2 =
                BTreeMap.open(store2, rrr, LongFormat.INSTANCE, LongFormat.INSTANCE, 4);
        assertEquals(200, m2.sizeLong());
        for (long k = 0; k < 200; k++) assertEquals(Long.valueOf(k * 10), m2.get(k));
        store2.close();
    }

    @Test
    public void repeatedRollbackCyclesAdvanceGeneration() {
        final long rrr;
        {
            StoreWAL store = new StoreWAL(file);
            BTreeMap<Long, Long> map =
                    BTreeMap.create(store, LongFormat.INSTANCE, LongFormat.INSTANCE, 4, true);
            store.commit();

            // Each cycle grows the tree, then rolls it back to the empty baseline. Every
            // rollback advances structuralGeneration(), and the next cycle's first put
            // must rebuild the stale leftEdges before it grows again.
            for (int cycle = 0; cycle < 4; cycle++) {
                final int n = 80 + cycle * 40;
                for (long k = 0; k < n; k++) map.put(k, k);
                assertEquals(n, map.sizeLong());
                store.rollback();
                assertEquals(0, map.sizeLong());
            }

            final int n = 300;
            for (long k = 0; k < n; k++) map.put(k, k + 7);
            store.commit();
            assertEquals(n, map.sizeLong());
            rrr = map.rootRecidRecid();
            store.close();
        }

        StoreWAL store2 = new StoreWAL(file);
        store2.verify();
        BTreeMap<Long, Long> m2 =
                BTreeMap.open(store2, rrr, LongFormat.INSTANCE, LongFormat.INSTANCE, 4);
        assertEquals(300, m2.sizeLong());
        for (long k = 0; k < 300; k++) assertEquals(Long.valueOf(k + 7), m2.get(k));
        // full forward scan matches the oracle exactly (no stray/duplicate keys)
        Iterator<Map.Entry<Long, Long>> it = m2.entryIterator();
        for (long k = 0; k < 300; k++) {
            assertTrue(it.hasNext());
            Map.Entry<Long, Long> e = it.next();
            assertEquals(Long.valueOf(k), e.getKey());
            assertEquals(Long.valueOf(k + 7), e.getValue());
        }
        assertFalse(it.hasNext());
        store2.close();
    }
}
