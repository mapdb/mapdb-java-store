package org.mapdb.store;

import org.junit.After;
import org.junit.Test;
import org.mapdb.DBException;
import org.mapdb.io.DataInput2;
import org.mapdb.ser.Serializer;
import org.mapdb.ser.Serializers;

import java.util.ArrayList;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.TreeSet;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Store contract test kit — <b>the normative statement of the {@link Store} edge-case
 * semantics</b>, plus the cases ported from mapdb3's {@code StoreTest.kt}. Every concrete
 * store runs the identical suite via {@link #createStore()}; a store that passes this
 * conforms, and a behaviour not pinned here is not part of the contract.
 */
public abstract class StoreTCK {

    private final List<Store> opened = new ArrayList<>();

    /** Open a fresh, empty store. */
    protected abstract Store createStore();

    /** OnHeap/ByteArray/Direct reuse freed recids immediately (LIFO/FIFO); WAL defers to commit. */
    protected boolean reusesRecidsImmediately() { return true; }

    protected final Store openStore() {
        Store s = createStore();
        opened.add(s);
        return s;
    }

    @After public void tearDown() {
        try {
            for (Store s : opened) {
                try { if (!s.isClosed()) s.close(); } catch (Throwable ignore) {}
            }
            opened.clear();
        } finally {
            cleanup();
        }
    }

    /**
     * Subclass hook for post-close resource cleanup (temp files), as in the map TCKs. A subclass
     * that deletes its files from an {@code @After} of its own instead would delete them
     * <em>before</em> this one runs — JUnit runs the subclass's hooks first — and so would unlink a
     * live store's lock file out from under it.
     */
    protected void cleanup() { }

    // ---------- helpers ----------

    private static final Serializer<Long> LONG = Serializers.LONG;

    private void assertGetVoid(Runnable r) {
        try {
            r.run();
            fail("expected DBException.GetVoid");
        } catch (DBException.GetVoid expected) {
            // ok
        }
    }

    private void assertStoreClosed(Runnable r) {
        try {
            r.run();
            fail("expected DBException.StoreClosed");
        } catch (DBException.StoreClosed expected) {
            // ok
        }
    }

    private static long neverAllocated() { return 99_999L; }

    private static List<Long> recids(Store s) {
        List<Long> out = new ArrayList<>();
        PrimitiveIterator.OfLong it = s.getAllRecids();
        while (it.hasNext()) out.add(it.nextLong());
        return out;
    }

    // ---------- read() probe actions ----------

    /** Reads the stored long from either representation; onNull must not fire for these tests. */
    static final class LongReader implements RecordRead {
        @Override public long onBytes(DataInput2 in, int size) { return in.readLong(); }
        @Override public long onObject(Object record) { return (Long) record; }
        @Override public long onNull() { throw new AssertionError("onNull unexpectedly dispatched"); }
    }

    /** Returns a caller-defined sentinel from every branch; proves opaque pass-through. */
    static final class Sentinel implements RecordRead {
        final long v;
        Sentinel(long v) { this.v = v; }
        @Override public long onBytes(DataInput2 in, int size) { return v; }
        @Override public long onObject(Object record) { return v; }
        @Override public long onNull() { return v; }
    }

    /** Records which branch dispatched. */
    static final class Probe implements RecordRead {
        boolean bytes, obj, nul;
        @Override public long onBytes(DataInput2 in, int size) { bytes = true; return 100; }
        @Override public long onObject(Object record) { obj = true; return 200; }
        @Override public long onNull() { nul = true; return 300; }
    }

    // ================= record-state table: state N (never allocated) =================

    @Test public void state_N_all_ops_getvoid() {
        Store s = openStore();
        long r = neverAllocated();
        assertGetVoid(() -> s.get(r, LONG));
        assertGetVoid(() -> s.read(r, new Probe()));
        assertGetVoid(() -> s.update(r, 1L, LONG));
        assertGetVoid(() -> s.compareAndSwap(r, 1L, 2L, LONG));
        assertGetVoid(() -> s.delete(r, LONG));
        s.verify();
    }

    // ================= state P (preallocated / null content) =================

    @Test public void state_P_semantics() {
        Store s = openStore();
        long r = s.preallocate();
        s.verify();
        assertNull(s.get(r, LONG));                 // get -> null
        assertEquals(300L, s.read(r, new Probe())); // read -> onNull
        assertFalse(s.compareAndSwap(r, 1L, 2L, LONG)); // CAS vs null: mismatch
        s.verify();
        assertTrue(s.compareAndSwap(r, null, 2L, LONG)); // CAS vs null: match, fills -> L
        assertEquals(Long.valueOf(2L), s.get(r, LONG));
        s.verify();
    }

    @Test public void state_P_update_fills() {
        Store s = openStore();
        long r = s.preallocate();
        s.update(r, 7L, LONG);
        assertEquals(Long.valueOf(7L), s.get(r, LONG));
        s.verify();
    }

    @Test public void state_P_delete_to_D() {
        Store s = openStore();
        long r = s.preallocate();
        s.delete(r, LONG);
        assertGetVoid(() -> s.get(r, LONG));
        s.verify();
    }

    // ================= state L (live) =================

    @Test public void state_L_semantics() {
        Store s = openStore();
        long r = s.put(1L, LONG);
        s.verify();
        assertEquals(Long.valueOf(1L), s.get(r, LONG));
        s.update(r, 2L, LONG);                       // update replaces
        assertEquals(Long.valueOf(2L), s.get(r, LONG));
        s.verify();
        assertFalse(s.compareAndSwap(r, 99L, 3L, LONG));
        assertTrue(s.compareAndSwap(r, 2L, 3L, LONG));
        assertEquals(Long.valueOf(3L), s.get(r, LONG));
        s.verify();
        s.delete(r, LONG);
        assertGetVoid(() -> s.get(r, LONG));
        s.verify();
    }

    // ================= state D (deleted) =================

    @Test public void state_D_all_ops_getvoid() {
        Store s = openStore();
        long r = s.put(1L, LONG);
        s.delete(r, LONG);
        assertGetVoid(() -> s.get(r, LONG));
        assertGetVoid(() -> s.read(r, new Probe()));
        assertGetVoid(() -> s.update(r, 1L, LONG));
        assertGetVoid(() -> s.compareAndSwap(r, 1L, 2L, LONG));
        assertGetVoid(() -> s.delete(r, LONG));
        s.verify();
    }

    // ================= ported mapdb3 StoreTest.kt cases =================

    @Test public void get_non_existent() {
        Store s = openStore();
        assertGetVoid(() -> s.get(1L, LONG));
        s.verify();
    }

    @Test public void preallocate_cas() {
        Store s = openStore();
        long r = s.preallocate();
        s.verify();
        assertFalse(s.compareAndSwap(r, 1L, 2L, LONG));
        assertTrue(s.compareAndSwap(r, null, 2L, LONG));
        assertEquals(Long.valueOf(2L), s.get(r, LONG));
        s.verify();
    }

    @Test public void preallocate_get_update_delete_update_get() {
        Store s = openStore();
        long r = s.preallocate();
        s.verify();
        assertNull(s.get(r, LONG));
        s.update(r, 1L, LONG);
        assertEquals(Long.valueOf(1L), s.get(r, LONG));
        s.delete(r, LONG);
        assertGetVoid(() -> s.get(r, LONG));
        s.verify();
        assertGetVoid(() -> s.update(r, 1L, LONG));
        s.verify();
    }

    @Test public void cas_prealloc() {
        Store s = openStore();
        long r = s.preallocate();
        assertTrue(s.compareAndSwap(r, null, 1L, LONG));
        s.verify();
        assertEquals(Long.valueOf(1L), s.get(r, LONG));
        assertTrue(s.compareAndSwap(r, 1L, null, LONG));
        s.verify();
        assertNull(s.get(r, LONG));
        s.verify();
    }

    @Test public void cas_prealloc_delete() {
        Store s = openStore();
        long r = s.preallocate();
        s.delete(r, LONG);
        assertGetVoid(() -> s.compareAndSwap(r, null, 1L, LONG));
        s.verify();
    }

    @Test public void get_deleted() {
        Store s = openStore();
        long r = s.put(1L, LONG);
        s.verify();
        s.delete(r, LONG);
        assertGetVoid(() -> s.get(r, LONG));
        s.verify();
    }

    @Test public void update_deleted() {
        Store s = openStore();
        long r = s.put(1L, LONG);
        s.delete(r, LONG);
        assertGetVoid(() -> s.update(r, 2L, LONG));
        s.verify();
    }

    @Test public void double_delete() {
        Store s = openStore();
        long r = s.put(1L, LONG);
        s.delete(r, LONG);
        assertGetVoid(() -> s.delete(r, LONG));
        s.verify();
    }

    @Test public void nosize_array() {
        Store s = openStore();
        Serializer<byte[]> ser = Serializers.BYTE_ARRAY;
        byte[] b = new byte[0];
        long r = s.put(b, ser);
        assertArrayEquals(b, s.get(r, ser));
        b = new byte[]{1, 2, 3};
        s.update(r, b, ser);
        assertArrayEquals(b, s.get(r, ser));
        s.verify();
        b = new byte[0];
        s.update(r, b, ser);
        assertArrayEquals(b, s.get(r, ser));
        s.verify();
        s.delete(r, ser);
        Serializer<byte[]> fser = ser;
        assertGetVoid(() -> s.get(r, fser));
        s.verify();
    }

    @Test public void empty_update_commit() {
        Store s = openStore();
        Serializer<String> ser = Serializers.STRING;
        long r = s.put("", ser);
        assertEquals("", s.get(r, ser));
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 50; i++) {
            sb.append("abcdefghij").append(i);
            String v = sb.toString();
            s.update(r, v, ser);
            assertEquals(v, s.get(r, ser));
            s.commit();
            assertEquals(v, s.get(r, ser));
            s.verify();
        }
    }

    @Test public void delete_reuse() {
        Store s = openStore();
        Serializer<String> ser = Serializers.STRING;
        long r = s.put("hello world", ser);
        s.delete(r, ser);
        assertGetVoid(() -> s.get(r, ser));
        long r2 = s.put("second value", ser);
        if (reusesRecidsImmediately()) {
            assertEquals("freed recid must be reused (LIFO/FIFO)", r, r2);
        }
        s.verify();
    }

    // ================= new Store4 core cases =================

    @Test public void recid_zero_never_returned() {
        Store s = openStore();
        for (int i = 0; i < 200; i++) {
            long a = s.put((long) i, LONG);
            long b = s.preallocate();
            assertTrue("put returned recid 0", a != 0);
            assertTrue("preallocate returned recid 0", b != 0);
        }
        s.verify();
    }

    @Test public void getAllRecids_excludes_prealloc() {
        Store s = openStore();
        long p = s.preallocate();
        long live = s.put(1L, LONG);
        long nullRec = s.put(null, LONG); // explicit-null content is a record, not prealloc
        List<Long> ids = recids(s);
        assertTrue("live recid missing", ids.contains(live));
        assertTrue("null-content recid missing", ids.contains(nullRec));
        assertFalse("prealloc recid must be excluded", ids.contains(p));
        // filling the prealloc record includes it
        s.update(p, 5L, LONG);
        assertTrue("filled prealloc must now appear", recids(s).contains(p));
        s.verify();
    }

    @Test public void read_dispatch_onNull_for_prealloc() {
        Store s = openStore();
        long r = s.preallocate();
        Probe p = new Probe();
        long ret = s.read(r, p);
        assertTrue("onNull expected for P record", p.nul);
        assertFalse(p.bytes);
        assertFalse(p.obj);
        assertEquals(300L, ret);
        s.verify();
    }

    @Test public void read_dispatch_onNull_for_explicit_null() {
        Store s = openStore();
        long r = s.put(null, LONG);
        Probe p = new Probe();
        s.read(r, p);
        assertTrue("onNull expected for explicit-null record", p.nul);
        assertFalse(p.bytes);
        assertFalse(p.obj);
        s.verify();
    }

    @Test public void read_dispatch_live_and_value_passthrough() {
        Store s = openStore();
        long[] values = {0L, 42L, -1L, -123456789L, Long.MAX_VALUE, Long.MIN_VALUE};
        for (long v : values) {
            long r = s.put(v, LONG);
            Probe p = new Probe();
            s.read(r, p);
            assertTrue("live record must dispatch onBytes or onObject", p.bytes ^ p.obj);
            assertFalse(p.nul);
            // value read back bit-exactly through the representation
            assertEquals(v, s.read(r, new LongReader()));
        }
        s.verify();
    }

    @Test public void read_return_value_passthrough_bit_exact() {
        Store s = openStore();
        long r = s.put(1L, LONG);
        long[] sentinels = {0L, -1L, 7L, Long.MAX_VALUE, Long.MIN_VALUE, 0x8000000000000000L, -42L};
        for (long sent : sentinels) {
            assertEquals(sent, s.read(r, new Sentinel(sent)));
        }
        s.verify();
    }

    @Test public void read_getvoid_states_do_not_run_action() {
        Store s = openStore();
        // never allocated
        Probe p1 = new Probe();
        assertGetVoid(() -> s.read(neverAllocated(), p1));
        assertFalse(p1.bytes || p1.obj || p1.nul);
        // deleted
        long r = s.put(1L, LONG);
        s.delete(r, LONG);
        Probe p2 = new Probe();
        assertGetVoid(() -> s.read(r, p2));
        assertFalse(p2.bytes || p2.obj || p2.nul);
        s.verify();
    }

    @Test public void null_record_distinct_from_empty_bytearray() {
        Store s = openStore();
        Serializer<byte[]> ser = Serializers.BYTE_ARRAY;
        long rNull = s.put(null, ser);
        long rEmpty = s.put(new byte[0], ser);
        assertNull("null record must read back null", s.get(rNull, ser));
        byte[] empty = s.get(rEmpty, ser);
        assertNull("... yet get on null stays null", s.get(rNull, ser));
        org.junit.Assert.assertNotNull("empty byte[] must be non-null", empty);
        assertEquals(0, empty.length);
        // both are records (not prealloc): appear in getAllRecids
        List<Long> ids = recids(s);
        assertTrue(ids.contains(rNull));
        assertTrue(ids.contains(rEmpty));
        s.verify();
    }

    @Test public void cas_with_null_on_prealloc() {
        Store s = openStore();
        long r = s.preallocate();
        assertFalse("CAS(recid, value, x) on P must fail", s.compareAndSwap(r, 5L, 6L, LONG));
        assertTrue("CAS(recid, null, x) on P must succeed", s.compareAndSwap(r, null, 6L, LONG));
        assertEquals(Long.valueOf(6L), s.get(r, LONG));
        s.verify();
    }

    @Test public void close_then_ops_throw_StoreClosed() {
        Store s = openStore();
        long r = s.put(1L, LONG);
        s.close();
        assertTrue(s.isClosed());
        assertStoreClosed(s::preallocate);
        assertStoreClosed(() -> s.put(1L, LONG));
        assertStoreClosed(() -> s.get(r, LONG));
        assertStoreClosed(() -> s.read(r, new Probe()));
        assertStoreClosed(() -> s.update(r, 2L, LONG));
        assertStoreClosed(() -> s.compareAndSwap(r, 1L, 2L, LONG));
        assertStoreClosed(() -> s.delete(r, LONG));
        assertStoreClosed(s::commit);
        assertStoreClosed(s::verify);
        assertStoreClosed(s::getAllRecids);
    }

    /** Store is AutoCloseable with an unchecked close(): the block below must need no catch. */
    @Test public void try_with_resources_closes() {
        Store outside;
        try (Store s = openStore()) {
            s.put(1L, LONG);
            assertFalse(s.isClosed());
            outside = s;
        }
        assertTrue("try-with-resources must have closed the store", outside.isClosed());
    }

    // ================= misc round trips =================

    @Test public void put_get_roundtrip_strings() {
        Store s = openStore();
        Serializer<String> ser = Serializers.STRING;
        long r = s.put("aaaad9009", ser);
        assertEquals("aaaad9009", s.get(r, ser));
        s.update(r, "da8898fe89w98fw98f9", ser);
        assertEquals("da8898fe89w98fw98f9", s.get(r, ser));
        s.verify();
        s.delete(r, ser);
        assertGetVoid(() -> s.get(r, ser));
        s.verify();
    }

    @Test public void getAllRecids_sorted_and_complete() {
        Store s = openStore();
        TreeSet<Long> expected = new TreeSet<>();
        for (int i = 0; i < 30; i++) expected.add(s.put((long) i, LONG));
        // delete a few
        long[] arr = expected.stream().mapToLong(Long::longValue).toArray();
        for (int i = 0; i < arr.length; i += 5) { s.delete(arr[i], LONG); expected.remove(arr[i]); }
        s.verify();
        assertEquals(new ArrayList<>(expected), recids(s));
    }
}
