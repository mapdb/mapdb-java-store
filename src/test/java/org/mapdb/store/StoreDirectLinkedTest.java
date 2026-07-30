package org.mapdb.store;

import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.PrimitiveIterator;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Linked (oversize) records: chunk-boundary round trips, the plain/linked
 * transition at exactly MAX_CAPACITY, the append-on-linked-is-REFUSED invariant,
 * and linked records through the WAL commit/checkpoint/
 * replay path.
 */
public class StoreDirectLinkedTest {

    /** Largest content that still fits a plain record. */
    private static final int MAX_PLAIN = IndexVal.MAX_CAPACITY - 4;

    private final List<File> files = new ArrayList<>();

    private File newFile() throws IOException {
        File f = Files.createTempFile("mapdb-linked", ".db").toFile();
        f.delete();
        files.add(f);
        return f;
    }

    @After public void cleanup() {
        for (File f : files) {
            f.delete();
            org.mapdb.store.WalTestKit.deleteStore(f);
        }
        files.clear();
    }

    private static byte[] bytes(int size, long seed) {
        byte[] b = new byte[size];
        long s = seed * 0x9E3779B97F4A7C15L + 1;
        for (int i = 0; i < size; i++) {
            s ^= s << 13; s ^= s >>> 7; s ^= s << 17;
            b[i] = (byte) s;
        }
        return b;
    }

    @Test public void boundary_sizes_roundtrip() {
        StoreDirect s = new StoreDirect();
        try {
            int mcd = StoreDirect.MAX_CHUNK_DATA;
            int[] sizes = {
                    MAX_PLAIN,          // largest plain
                    MAX_PLAIN + 1,      // smallest linked
                    mcd, mcd + 1,       // around one full chunk of data
                    2 * mcd - 1, 2 * mcd, 2 * mcd + 1,
                    3 * mcd + 17,
            };
            for (int size : sizes) {
                byte[] v = bytes(size, size);
                long r = s.put(v, Fixtures.RAW);
                assertArrayEquals("size=" + size, v, s.get(r, Fixtures.RAW));
                s.verify();
                byte[] v2 = bytes(size + 3, size * 31L);
                s.update(r, v2, Fixtures.RAW);
                assertArrayEquals("update size=" + size, v2, s.get(r, Fixtures.RAW));
                s.verify();
                s.delete(r, Fixtures.RAW);
                s.verify();
            }
        } finally {
            s.close();
        }
    }

    /**
     * A zero-length append is a no-op returning the current size even on a linked record —
     * appending nothing needs no capacity, so refusal would make the zero-length case
     * shape-dependent. Paired with {@code StoreWALLinkedAppendTest}: StoreWAL used to stage
     * an empty append here and then trip its own "commit append refused" assertion when
     * {@code inner.append} refused it at commit-apply.
     */
    @Test public void zero_length_append_on_linked_is_noop() {
        StoreDirect s = new StoreDirect();
        try {
            byte[] v = bytes(MAX_PLAIN + 100, 1);
            long r = s.put(v, Fixtures.RAW);
            assertEquals("linked records expose no append capacity", 0, s.capacityRemaining(r));
            assertEquals("zero-length append returns current size", v.length,
                    s.append(r, new byte[0], 0, 0));
            assertEquals("non-empty append is still REFUSED",
                    StoreDelta.REFUSED, s.append(r, new byte[]{1}, 0, 1));
            assertArrayEquals("content untouched", v, s.get(r, Fixtures.RAW));
            s.verify();

            // and on a null (preallocated) record it must not establish anything
            long p = s.preallocate();
            assertEquals(0, s.append(p, new byte[0], 0, 0));
            assertEquals("still null-content", 0, s.capacityRemaining(p));
            s.verify();
        } finally {
            s.close();
        }
    }

    @Test public void append_on_linked_always_refused() {
        StoreDirect s = new StoreDirect();
        try {
            byte[] v = bytes(MAX_PLAIN + 100, 1);
            long r = s.put(v, Fixtures.RAW);
            assertEquals("linked records expose no append capacity", 0, s.capacityRemaining(r));
            assertEquals(StoreDelta.REFUSED, s.append(r, new byte[]{1}, 0, 1));
            assertArrayEquals("content untouched by refused append", v, s.get(r, Fixtures.RAW));
            s.verify();

            // consolidation via update is the escape hatch: shrink to plain, appends work again
            byte[] small = bytes(100, 2);
            s.updateWithHeadroom(r, small, Fixtures.RAW, 64);
            assertTrue(s.capacityRemaining(r) >= 64);
            assertEquals(small.length + 3, s.append(r, new byte[]{1, 2, 3}, 0, 3));
            s.verify();
        } finally {
            s.close();
        }
    }

    @Test public void plain_at_exact_cap_stays_plain() {
        StoreDirect s = new StoreDirect();
        try {
            byte[] v = bytes(MAX_PLAIN, 3);
            long r = s.put(v, Fixtures.RAW);
            assertEquals("exactly-at-cap record is plain and full", 0, s.capacityRemaining(r));
            assertEquals(StoreDelta.REFUSED, s.append(r, new byte[]{1}, 0, 1));
            assertArrayEquals(v, s.get(r, Fixtures.RAW));
            s.verify();
        } finally {
            s.close();
        }
    }

    @Test public void read_dispatch_and_cas_on_linked() {
        StoreDirect s = new StoreDirect();
        try {
            byte[] v = bytes(2_500_000, 4);
            long r = s.put(v, Fixtures.RAW);

            Fixtures.Capture c = new Fixtures.Capture();
            s.read(r, c);
            assertArrayEquals("read() assembles the full chain", v, c.bytes);

            byte[] v2 = bytes(1_800_000, 5);
            assertTrue("CAS with matching linked value must succeed",
                    s.compareAndSwap(r, v, v2, Fixtures.RAW));
            assertArrayEquals(v2, s.get(r, Fixtures.RAW));
            assertTrue("CAS vs stale value must fail",
                    !s.compareAndSwap(r, v, bytes(10, 6), Fixtures.RAW));
            s.verify();

            boolean found = false;
            PrimitiveIterator.OfLong it = s.getAllRecids();
            while (it.hasNext()) found |= it.nextLong() == r;
            assertTrue("linked recid listed in getAllRecids", found);
        } finally {
            s.close();
        }
    }

    @Test public void linked_space_reclaimed_on_delete() {
        StoreDirect s = new StoreDirect();
        try {
            long tail = -1;
            for (int i = 0; i < 8; i++) {
                long r = s.put(bytes(2_000_000, i), Fixtures.RAW);
                s.delete(r, Fixtures.RAW);
                if (i == 0) tail = s.testFileTail();
                else assertEquals("linked chunks must be recycled, not leaked", tail, s.testFileTail());
                s.verify();
            }
        } finally {
            s.close();
        }
    }

    @Test public void append_establish_oversize_creates_linked() {
        StoreDirect s = new StoreDirect();
        try {
            long r = s.preallocate();
            byte[] big = bytes(MAX_PLAIN + 12345, 7);
            assertEquals("first append may establish an oversize record",
                    big.length, s.append(r, big, 0, big.length));
            assertArrayEquals(big, s.get(r, Fixtures.RAW));
            assertEquals(StoreDelta.REFUSED, s.append(r, new byte[]{1}, 0, 1));
            s.verify();
        } finally {
            s.close();
        }
    }

    @Test public void wal_linked_commit_checkpoint_reopen() throws IOException {
        File f = newFile();
        byte[] big = bytes(2_345_678, 8);
        byte[] small = bytes(777, 9);
        long rBig, rSmall;
        StoreWAL w = new StoreWAL(f);
        try {
            rBig = w.put(big, Fixtures.RAW);
            rSmall = w.put(small, Fixtures.RAW);
            w.commit();
            assertArrayEquals(big, w.get(rBig, Fixtures.RAW));
            w.verify();
            w.checkpoint(); // snapshot section must carry the oversize record (cap=0 framing)
            assertArrayEquals(big, w.get(rBig, Fixtures.RAW));
            w.verify();
        } finally {
            w.close();
        }
        StoreWAL w2 = new StoreWAL(f);
        try {
            assertArrayEquals("linked record after checkpoint+reopen", big, w2.get(rBig, Fixtures.RAW));
            assertArrayEquals(small, w2.get(rSmall, Fixtures.RAW));
            assertEquals("append on a linked record stays refused after replay",
                    StoreDelta.REFUSED, w2.append(rBig, new byte[]{1}, 0, 1));
            w2.verify();
        } finally {
            w2.close();
        }
    }

    @Test public void wal_replay_of_unCheckpointed_linked_commit() throws IOException {
        File f = newFile();
        byte[] big = bytes(1_500_000, 10);
        long r;
        StoreWAL w = new StoreWAL(f);
        try {
            r = w.put(big, Fixtures.RAW);
            w.commit(); // plain section replay path (no checkpoint)
        } finally {
            w.close();
        }
        StoreWAL w2 = new StoreWAL(f);
        try {
            assertArrayEquals(big, w2.get(r, Fixtures.RAW));
            w2.verify();
        } finally {
            w2.close();
        }
    }
}
