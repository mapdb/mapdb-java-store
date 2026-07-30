package org.mapdb.ported;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mapdb.DBException;
import org.mapdb.ser.Serializer;
import org.mapdb.ser.Serializers;
import org.mapdb.store.Store;
import org.mapdb.store.StoreByteArray;
import org.mapdb.store.StoreDirect;
import org.mapdb.store.StoreOnHeap;
import org.mapdb.store.StoreWAL;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Store-level scenarios ported from the historical suites that the Store TCK
 * did NOT already cover, run across every store dialect (Direct / ByteArray / OnHeap / WAL).
 *
 * Gap filled: the TCK exercises tiny fixed-value records only. The old suites hammered
 * boundary sizes, records around the old 64 KiB linked-record threshold, and records up
 * to the ~1 MiB plain-record cap — all of which this engine must handle as plain records.
 */
@RunWith(Parameterized.class)
public class PortedStoreSuite {

    @Parameterized.Parameters(name = "{0}")
    public static List<String> stores() {
        return Arrays.asList("Direct", "ByteArray", "OnHeap", "WAL");
    }

    @Parameterized.Parameter public String storeName;

    private final List<Store> opened = new ArrayList<>();
    private final List<File> tmpFiles = new ArrayList<>();

    private Store fresh() throws Exception {
        Store s;
        switch (storeName) {
            case "Direct" -> s = new StoreDirect();
            case "ByteArray" -> s = new StoreByteArray();
            case "OnHeap" -> s = new StoreOnHeap();
            case "WAL" -> {
                File f = File.createTempFile("mapdb-ported", ".wal");
                f.delete();
                tmpFiles.add(f);
                s = new StoreWAL(f);
            }
            default -> throw new AssertionError(storeName);
        }
        opened.add(s);
        return s;
    }

    @After public void tearDown() {
        for (Store s : opened) {
            try { if (!s.isClosed()) s.close(); } catch (Throwable ignore) {}
        }
        opened.clear();
        for (File f : tmpFiles) { try { f.delete(); } catch (Throwable ignore) {} }
        tmpFiles.clear();
    }

    private void assertGetVoid(Runnable r) {
        try { r.run(); fail("expected DBException.GetVoid"); }
        catch (DBException.GetVoid expected) { /* ok */ }
    }

    // ================================================================
    // boundary sizes — ported from mapdb3 StoreTest.test_sizes /
    // StoreDirectTest.small_ser_size + mapdb2 EngineTest.large_record.
    // Adapted: mapdb3 reopened the file (StoreDirect is heap-index here, no
    // reopen) and switched to linked records above ~64 KiB — this engine keeps every
    // size below the cap as a single plain record. Max size clamped to MAX_CONTENT.
    // ================================================================

    @Test public void boundary_sizes_roundtrip() throws Exception {
        int[] sizes = {
                0, 1, 2, 3, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129,
                255, 256, 257, 1000, 4096, 65535, 65536, 100000, 262144, Ported.MAX_CONTENT
        };
        Store s = fresh();
        for (int size : sizes) {
            byte[] a = Ported.bytes(size, size * 31L + 1);
            long r = s.put(a, Ported.RAW);
            assertArrayEquals("put/get size=" + size, a, s.get(r, Ported.RAW));
            s.verify();
            s.commit();
            assertArrayEquals("get after commit size=" + size, a, s.get(r, Ported.RAW));

            byte[] b = Ported.bytes(size, size * 31L + 2);
            s.update(r, b, Ported.RAW);
            assertArrayEquals("update same size=" + size, b, s.get(r, Ported.RAW));
            s.verify();

            if (size <= 70000) { // skip CAS on the huge ones to bound memory/time
                if (size > 0) { // at size 0 every array is the empty array (equal): no "wrong" expected exists
                    byte[] wrong = b.clone();
                    wrong[0] ^= 0xFF; // guaranteed != current (b) and != a
                    assertFalse("CAS wrong-expected size=" + size,
                            s.compareAndSwap(r, wrong, a, Ported.RAW));
                }
                assertTrue("CAS match size=" + size, s.compareAndSwap(r, b, a, Ported.RAW));
                assertArrayEquals(a, s.get(r, Ported.RAW));
            }

            s.delete(r, Ported.RAW);
            assertGetVoid(() -> s.get(r, Ported.RAW));
            s.verify();
        }
    }

    // Many records of varied size coexisting — ported from mapdb2
    // EngineTest.insert_many_reopen_check (reopen dropped; sizes clamped under cap).
    // Exercises the allocator / slice-donation logic under a mixed-size workload and
    // asserts no record overlaps another (via verify()).
    @Test public void many_records_varying_sizes() throws Exception {
        Store s = fresh();
        final int N = 400;
        final int MAXSIZE = 60000;
        Random rnd = new Random(0);
        Map<Long, byte[]> ref = new HashMap<>();
        for (int i = 0; i < N; i++) {
            byte[] b = Ported.bytes(rnd.nextInt(MAXSIZE), rnd.nextInt());
            ref.put(s.put(b, Ported.RAW), b);
        }
        s.verify();
        for (Map.Entry<Long, byte[]> e : ref.entrySet())
            assertArrayEquals(e.getValue(), s.get(e.getKey(), Ported.RAW));

        // update half to fresh random sizes (forces in-place vs relocate mix)
        int i = 0;
        for (Long recid : new ArrayList<>(ref.keySet())) {
            if ((i++ & 1) == 0) {
                byte[] b = Ported.bytes(rnd.nextInt(MAXSIZE), rnd.nextInt());
                s.update(recid, b, Ported.RAW);
                ref.put(recid, b);
            }
        }
        s.verify();
        for (Map.Entry<Long, byte[]> e : ref.entrySet())
            assertArrayEquals(e.getValue(), s.get(e.getKey(), Ported.RAW));

        // delete a quarter, survivors intact
        i = 0;
        for (Long recid : new ArrayList<>(ref.keySet())) {
            if ((i++ & 3) == 0) { s.delete(recid, Ported.RAW); ref.remove(recid); }
        }
        s.verify();
        for (Map.Entry<Long, byte[]> e : ref.entrySet())
            assertArrayEquals(e.getValue(), s.get(e.getKey(), Ported.RAW));
    }

    // Multiple large records with interleaved update/delete — ported from mapdb3
    // StoreTest.large_record_delete2 + large_record_update. Adapted: sizes stay under
    // MAX_CONTENT (mapdb3 used up to ~10 MB linked records). verify() after each step
    // catches allocator overlap/leak regressions.
    @Test public void large_records_interleaved() throws Exception {
        Store s = fresh();
        byte[] b1 = Ported.bytes(200000, 1);
        byte[] b2 = Ported.bytes(220000, 2);
        long r1 = s.put(b1, Ported.RAW);
        s.verify();
        long r2 = s.put(b2, Ported.RAW);
        s.verify();
        assertArrayEquals(b1, s.get(r1, Ported.RAW));
        assertArrayEquals(b2, s.get(r2, Ported.RAW));

        // grow r1 (relocate), r2 untouched
        byte[] b1b = Ported.bytes(210000, 3);
        s.update(r1, b1b, Ported.RAW);
        assertArrayEquals(b1b, s.get(r1, Ported.RAW));
        assertArrayEquals(b2, s.get(r2, Ported.RAW));
        s.verify();

        // shrink r1 sharply, r2 untouched
        byte[] b1c = Ported.bytes(28001, 4);
        s.update(r1, b1c, Ported.RAW);
        assertArrayEquals(b1c, s.get(r1, Ported.RAW));
        assertArrayEquals(b2, s.get(r2, Ported.RAW));
        s.verify();

        s.delete(r1, Ported.RAW);
        assertArrayEquals(b2, s.get(r2, Ported.RAW));
        s.verify();
        s.delete(r2, Ported.RAW);
        s.verify();
    }

    // CAS must use serializer.equals (logical), not reference equality — ported from
    // mapdb2 EngineTest.cas_uses_serializer. The TCK only CASes boxed Longs (identity
    // via the small-value cache), so this distinct-but-equal byte[] clone is a real gap.
    @Test public void cas_uses_serializer_equals() throws Exception {
        Store s = fresh();
        byte[] data = Ported.bytes(1024, 7);
        long r = s.put(data, Ported.RAW);
        byte[] data2 = Ported.bytes(100, 8);
        // expected is a DISTINCT array, equal only by content
        assertTrue(s.compareAndSwap(r, data.clone(), data2.clone(), Ported.RAW));
        assertArrayEquals(data2, s.get(r, Ported.RAW));
        // content-mismatched expected must fail even though lengths match
        assertFalse(s.compareAndSwap(r, Ported.bytes(100, 9), Ported.bytes(100, 10), Ported.RAW));
        assertArrayEquals(data2, s.get(r, Ported.RAW));
        s.verify();
    }

    // CAS a live record to null then back — ported from mapdb2 EngineTest.cas_delete +
    // mapdb3 StoreTest.cas_delete. TCK's cas_prealloc starts from P; this starts from a
    // put() LIVE record, covering the live→null CAS transition (record stays allocated,
    // becomes null-content, not deleted).
    @Test public void cas_live_to_null_and_back() throws Exception {
        Store s = fresh();
        Serializer<Long> LONG = Serializers.LONG;
        long r = s.put(1L, LONG);
        assertTrue(s.compareAndSwap(r, 1L, null, LONG));
        assertNull(s.get(r, LONG));
        s.verify();
        // still a live (null-content) record — appears in getAllRecids, not GetVoid
        assertTrue(recids(s).contains(r));
        assertTrue(s.compareAndSwap(r, null, 2L, LONG));
        assertEquals(Long.valueOf(2L), s.get(r, LONG));
        s.verify();
    }

    // Genuine 0-byte LIVE record via a zero-emitting serializer — ported from mapdb2
    // EngineTest.zero_size_serializer. Distinct from null; never produced by the
    // length-prefixed Serializers.STRING the TCK uses, so this path is otherwise untested.
    @Test public void zero_size_live_record() throws Exception {
        Store s = fresh();
        Serializer<String> ser = Ported.ZERO_STR;
        long r = s.put("", ser);
        assertEquals("", s.get(r, ser));            // 0-byte live record reads back ""
        assertTrue(recids(s).contains(r));          // it IS a live record (not prealloc)
        s.update(r, "a", ser);
        assertEquals("a", s.get(r, ser));
        assertTrue(s.compareAndSwap(r, "a", "", ser));
        assertEquals("", s.get(r, ser));
        s.update(r, "abc", ser);
        assertEquals("abc", s.get(r, ser));
        s.update(r, "", ser);
        assertEquals("", s.get(r, ser));
        s.verify();
        // a distinct null record stays null (0-byte live != null)
        long rn = s.put(null, ser);
        assertNull(s.get(rn, ser));
        assertEquals("", s.get(r, ser));
        s.verify();
    }

    private static List<Long> recids(Store s) {
        List<Long> out = new ArrayList<>();
        PrimitiveIterator.OfLong it = s.getAllRecids();
        while (it.hasNext()) out.add(it.nextLong());
        return out;
    }
}
