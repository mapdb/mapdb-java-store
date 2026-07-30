package org.mapdb.db;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;
import org.mapdb.DBException;
import org.mapdb.btree.BTreeMap;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;
import org.mapdb.ser.ByteArrayFormat;
import org.mapdb.ser.GroupFormat;
import org.mapdb.ser.LongFormat;
import org.mapdb.ser.ObjectArrayFormat;
import org.mapdb.ser.Serializer;
import org.mapdb.ser.Serializers;
import org.mapdb.store.Store;
import org.mapdb.store.StoreDirect;

/**
 * Edge-case coverage for the ported {@link DB}/{@link DBMaker} facade. Complements
 * {@code DBSmokeTest} (no overlap): init/ownership policy, custom-codec reopen,
 * structural depth + reopen survival, atomic contention, lifecycle semantics,
 * large catalogs, and byte-array codec round-trips.
 */
public class DBEdgeCaseTest {

    // ---- helpers ----------------------------------------------------------

    /** Allocates a fresh (non-existent) temp file path plus its WAL checkpoint sidecar. */
    private static File freshFile() throws Exception {
        File f = File.createTempFile("mapdb-edge", ".db");
        f.delete();
        return f;
    }

    private static void cleanup(File f) {
        f.delete();
        org.mapdb.store.WalTestKit.deleteStore(f);
    }

    /** A non-registered String serializer (delegates to the singleton but is a distinct object). */
    private static Serializer<String> customStringSer() {
        return new Serializer<String>() {
            @Override public void serialize(DataOutput2 out, String v) { Serializers.STRING.serialize(out, v); }
            @Override public String deserialize(DataInput2 in, int size) { return Serializers.STRING.deserialize(in, size); }
        };
    }

    // ======================================================================
    // 1. Init policy / store ownership
    // ======================================================================

    @Test public void freshStoresInitializeWithEmptyCatalog() {
        for (DB db : new DB[]{ DBMaker.memoryDB().make(), DBMaker.memoryDirectDB().make(),
                DBMaker.heapDB().make(), DBMaker.memoryByteArrayDB().make() }) {
            try {
                assertTrue(db.getNameCatalog().isEmpty());
                assertFalse(db.getAllNames().iterator().hasNext());
            } finally {
                db.close();
            }
        }
    }

    @Test public void reopenStoreWithValidCatalogWorks() throws Exception {
        File f = freshFile();
        try {
            DB db = DBMaker.fileDB(f).transactionEnable().make();
            db.atomicLong("counter", 7L).create();
            db.commit();
            db.close();

            DB db2 = DBMaker.fileDB(f).transactionEnable().make();
            assertTrue(db2.exists("counter"));
            assertEquals(7L, db2.atomicLong("counter").open().get());
            db2.close();
        } finally {
            cleanup(f);
        }
    }

    @Test public void dbOverPollutedStoreRecid1FailsCleanly() {
        // A different low-level writer occupies recid 1 with a non-catalog record.
        Store raw = new StoreDirect(false, true);
        long recid = raw.put(123456789L, Serializers.LONG);
        raw.commit();
        assertEquals("premise: first put must land on the catalog recid", DB.RECID_CATALOG, recid);
        try {
            DBException ex = assertThrows(DBException.class, () -> new DB(raw, true));
            assertTrue("expected WrongConfiguration or DataCorruption, got " + ex.getClass().getSimpleName(),
                    ex instanceof DBException.WrongConfiguration || ex instanceof DBException.DataCorruption);
        } finally {
            raw.close();
        }
    }

    @Test public void dbOverStorePollutedWithBytesFailsCleanly() {
        Store raw = new StoreDirect(false, true);
        long recid = raw.put(new byte[]{1, 2, 3}, Serializers.BYTE_ARRAY);
        raw.commit();
        assertEquals(DB.RECID_CATALOG, recid);
        try {
            assertThrows(DBException.class, () -> new DB(raw, true));
        } finally {
            raw.close();
        }
    }

    // ======================================================================
    // 2. Custom (unregistered) codec reopen
    // ======================================================================

    @Test public void treeMapCustomFormatRequiresResupplyOnReopen() throws Exception {
        File f = freshFile();
        GroupFormat<String> customFmt = new ObjectArrayFormat<>(customStringSer());
        assertNull("premise: format must be unregistered", SerializerRegistry.groupFormatId(customFmt));
        try {
            DB db = DBMaker.fileDB(f).transactionEnable().make();
            BTreeMap<Long, String> m = db.treeMap("t", LongFormat.INSTANCE, customFmt).create();
            m.put(1L, "one");
            m.put(2L, "two");
            db.commit();
            db.close();

            DB db2 = DBMaker.fileDB(f).transactionEnable().make();
            // Opening without re-supplying the custom value format (null format) must fail.
            assertThrows(DBException.WrongConfiguration.class, () -> db2.treeMap("t").open());
            // Re-supplying only the (registered) key format still leaves the value custom -> fail.
            assertThrows(DBException.WrongConfiguration.class,
                    () -> db2.<Long, String>treeMap("t").keySerializer(LongFormat.INSTANCE).open());
            // untyped get() cannot re-derive the custom codec either.
            assertThrows(DBException.WrongConfiguration.class, () -> db2.get("t"));

            BTreeMap<Long, String> reopened =
                    db2.<Long, String>treeMap("t").keySerializer(LongFormat.INSTANCE)
                            .valueSerializer(new ObjectArrayFormat<>(customStringSer())).open();
            assertEquals("one", reopened.get(1L));
            assertEquals("two", reopened.get(2L));
            db2.close();
        } finally {
            cleanup(f);
        }
    }

    @Test public void hashMapCustomSerializerRequiresResupplyOnReopen() throws Exception {
        File f = freshFile();
        Serializer<String> customVal = customStringSer();
        assertNull(SerializerRegistry.serializerId(customVal));
        try {
            DB db = DBMaker.fileDB(f).transactionEnable().make();
            ConcurrentMap<String, String> m = db.hashMap("h", Serializers.STRING, customVal).create();
            m.put("k", "v");
            db.commit();
            db.close();

            DB db2 = DBMaker.fileDB(f).transactionEnable().make();
            assertThrows(DBException.WrongConfiguration.class, () -> db2.hashMap("h").open());
            assertThrows(DBException.WrongConfiguration.class, () -> db2.get("h"));

            ConcurrentMap<String, String> reopened =
                    db2.<String, String>hashMap("h").valueSerializer(customStringSer()).open();
            assertEquals("v", reopened.get("k"));
            db2.close();
        } finally {
            cleanup(f);
        }
    }

    @Test public void atomicVarCustomSerializerRequiresResupplyOnReopen() throws Exception {
        File f = freshFile();
        Serializer<String> customSer = customStringSer();
        assertNull(SerializerRegistry.serializerId(customSer));
        try {
            DB db = DBMaker.fileDB(f).transactionEnable().make();
            db.atomicVar("v", customSer, "hello").create();
            db.commit();
            db.close();

            DB db2 = DBMaker.fileDB(f).transactionEnable().make();
            assertThrows(DBException.WrongConfiguration.class, () -> db2.atomicVar("v").open());
            assertThrows(DBException.WrongConfiguration.class, () -> db2.get("v"));

            Atomic.Var<String> reopened = db2.<String>atomicVar("v").serializer(customStringSer()).open();
            assertEquals("hello", reopened.get());
            db2.close();
        } finally {
            cleanup(f);
        }
    }

    // ======================================================================
    // 3. Set structural depth + reopen
    // ======================================================================

    @Test public void treeSetDeepSplitsSurviveReopen() throws Exception {
        File f = freshFile();
        final int N = 600;
        try {
            DB db = DBMaker.fileDB(f).transactionEnable().make();
            NavigableSet<Long> ts = db.treeSet("ts", LongFormat.INSTANCE).maxNodeSize(8).create();
            // insert in shuffled-ish order to exercise splits at many positions
            for (long i = 0; i < N; i++) ts.add((i * 37) % N);
            assertEquals(N, ts.size());
            assertFalse("re-adding an existing element returns false", ts.add(0L));
            assertTrue("adding a new element returns true", ts.add((long) N));
            ts.remove((long) N);

            assertEquals((Long) 0L, ts.first());
            assertEquals((Long) (long) (N - 1), ts.last());
            assertTrue(ts.contains(123L));
            assertTrue(ts.remove(123L));
            assertFalse(ts.contains(123L));
            assertFalse(ts.remove(123L));
            assertEquals(N - 1, ts.size());
            db.commit();
            db.close();

            DB db2 = DBMaker.fileDB(f).transactionEnable().make();
            NavigableSet<Long> ts2 = db2.treeSet("ts", LongFormat.INSTANCE).open();
            assertEquals(N - 1, ts2.size());
            assertEquals((Long) 0L, ts2.first());
            assertEquals((Long) (long) (N - 1), ts2.last());
            assertFalse(ts2.contains(123L));
            assertTrue(ts2.contains(122L));
            // verify strictly ascending ordering across the whole set
            long prev = java.lang.Long.MIN_VALUE;
            for (long v : ts2) { assertTrue("ordering broken", v > prev); prev = v; }
            db2.close();
        } finally {
            cleanup(f);
        }
    }

    @Test public void hashSetManyEntriesSurviveReopen() throws Exception {
        File f = freshFile();
        final int N = 2000;
        try {
            DB db = DBMaker.fileDB(f).transactionEnable().make();
            Set<Long> hs = db.hashSet("hs", Serializers.LONG).create();
            for (long i = 0; i < N; i++) assertTrue(hs.add(i));
            for (long i = 0; i < N; i++) assertFalse("duplicate add must be false", hs.add(i));
            assertEquals(N, hs.size());
            assertTrue(hs.remove(500L));
            assertFalse(hs.contains(500L));
            assertEquals(N - 1, hs.size());
            db.commit();
            db.close();

            DB db2 = DBMaker.fileDB(f).transactionEnable().make();
            Set<Long> hs2 = db2.hashSet("hs", Serializers.LONG).open();
            assertEquals(N - 1, hs2.size());
            assertFalse(hs2.contains(500L));
            for (long i = 0; i < N; i++) {
                if (i == 500L) continue;
                assertTrue("missing element " + i, hs2.contains(i));
            }
            db2.close();
        } finally {
            cleanup(f);
        }
    }

    // ======================================================================
    // 4. Atomics under contention on multiple stores
    // ======================================================================

    @Test public void atomicLongUnderContentionMemory() throws Exception {
        assertAtomicLongContention(DBMaker.memoryDB().make());
    }

    @Test public void atomicLongUnderContentionMemoryDirect() throws Exception {
        assertAtomicLongContention(DBMaker.memoryDirectDB().make());
    }

    private static void assertAtomicLongContention(DB db) throws Exception {
        final int threads = 6, iters = 10_000;
        try {
            final Atomic.Long counter = db.atomicLong("c", 0L).create();
            // makers return the same cached instance per name
            assertTrue(counter == db.atomicLong("c").open());
            final CountDownLatch start = new CountDownLatch(1);
            List<Thread> ts = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                Thread th = new Thread(() -> {
                    try { start.await(); } catch (InterruptedException ignore) {}
                    for (int i = 0; i < iters; i++) counter.incrementAndGet();
                });
                ts.add(th);
                th.start();
            }
            start.countDown();
            for (Thread th : ts) th.join();
            assertEquals((long) threads * iters, counter.get());
        } finally {
            db.close();
        }
    }

    @Test public void atomicVarCompareAndSetLoopSingleThreaded() {
        DB db = DBMaker.memoryDB().make();
        try {
            Atomic.Var<String> v = db.atomicVar("v", Serializers.STRING, "a").create();
            assertTrue(v.compareAndSet("a", "b"));
            assertFalse(v.compareAndSet("a", "c"));
            assertEquals("b", v.get());
            // getAndSet is a CAS loop
            assertEquals("b", v.getAndSet("d"));
            assertEquals("d", v.get());
        } finally {
            db.close();
        }
    }

    // ======================================================================
    // 5. createOrOpen / create / open semantics; delete; rename
    // ======================================================================

    @Test public void createOpenSemanticsAcrossReopen() throws Exception {
        File f = freshFile();
        try {
            DB db = DBMaker.fileDB(f).transactionEnable().make();
            assertThrows("open of a non-existent name fails",
                    DBException.WrongConfiguration.class, () -> db.atomicLong("x").open());
            db.atomicLong("x", 1L).createOrOpen(); // creates
            assertThrows("create of an existing name fails",
                    DBException.WrongConfiguration.class, () -> db.atomicLong("x").create());
            assertEquals(1L, db.atomicLong("x").createOrOpen().get()); // opens
            db.commit();
            db.close();

            DB db2 = DBMaker.fileDB(f).transactionEnable().make();
            assertThrows(DBException.WrongConfiguration.class, () -> db2.atomicLong("x").create());
            assertEquals(1L, db2.atomicLong("x").open().get());
            db2.close();
        } finally {
            cleanup(f);
        }
    }

    @Test public void deleteFreesAtomicAndAllowsRecreateWithFreshValue() {
        DB db = DBMaker.memoryDB().make();
        try {
            db.atomicLong("c", 5L).create();
            assertTrue(db.exists("c"));
            assertTrue(db.delete("c"));
            assertFalse(db.exists("c"));
            assertNull(db.getType("c"));
            assertFalse("second delete returns false", db.delete("c"));
            // recreate with a fresh initial value
            Atomic.Long recreated = db.atomicLong("c", 42L).create();
            assertEquals(42L, recreated.get());
        } finally {
            db.close();
        }
    }

    @Test public void renameOntoExistingNameThrows() {
        DB db = DBMaker.memoryDB().make();
        try {
            db.atomicLong("a").create();
            db.atomicLong("b").create();
            assertThrows(DBException.WrongConfiguration.class, () -> db.rename("a", "b"));
            // both still present and untouched
            assertTrue(db.exists("a"));
            assertTrue(db.exists("b"));
        } finally {
            db.close();
        }
    }

    @Test public void renameNonExistentNameThrows() {
        DB db = DBMaker.memoryDB().make();
        try {
            assertThrows(DBException.WrongConfiguration.class, () -> db.rename("nope", "x"));
        } finally {
            db.close();
        }
    }

    // ======================================================================
    // 6. Large-ish catalog
    // ======================================================================

    @Test public void manyNamedAtomicLongsSurviveReopen() throws Exception {
        File f = freshFile();
        final int N = 500;
        try {
            DB db = DBMaker.fileDB(f).transactionEnable().make();
            for (int i = 0; i < N; i++) db.atomicLong("al" + i, i).create();
            db.commit();
            db.close();

            DB db2 = DBMaker.fileDB(f).transactionEnable().make();
            int count = 0;
            for (String n : db2.getAllNames()) { assertTrue(n.startsWith("al")); count++; }
            assertEquals(N, count);
            for (int i = 0; i < N; i++) {
                assertTrue(db2.exists("al" + i));
                assertEquals(i, db2.atomicLong("al" + i).open().get());
            }
            db2.close();
        } finally {
            cleanup(f);
        }
    }

    // ======================================================================
    // 7. Byte-array codec correctness
    // ======================================================================

    @Test public void treeMapByteArrayKeysAndValuesRoundTrip() throws Exception {
        File f = freshFile();
        try {
            DB db = DBMaker.fileDB(f).transactionEnable().make();
            BTreeMap<byte[], byte[]> m =
                    db.treeMap("b", ByteArrayFormat.INSTANCE, ByteArrayFormat.INSTANCE).create();
            for (int i = 0; i < 200; i++) {
                m.put(new byte[]{(byte) (i >>> 8), (byte) i}, new byte[]{(byte) i, 99});
            }
            assertArrayEquals(new byte[]{50, 99}, m.get(new byte[]{0, 50}));
            db.commit();
            db.close();

            DB db2 = DBMaker.fileDB(f).transactionEnable().make();
            BTreeMap<byte[], byte[]> m2 =
                    db2.treeMap("b", ByteArrayFormat.INSTANCE, ByteArrayFormat.INSTANCE).open();
            for (int i = 0; i < 200; i++) {
                assertArrayEquals(new byte[]{(byte) i, 99}, m2.get(new byte[]{(byte) (i >>> 8), (byte) i}));
            }
            // ordering: first key is the all-zero prefix (i==0)
            assertArrayEquals(new byte[]{0, 0}, m2.firstKey());
            db2.close();
        } finally {
            cleanup(f);
        }
    }

    // NOTE: byte[] KEYS are deliberately not used here. The DB facade's hashMap maker
    // builds HTreeMap with the default objectHasher() (identity hash for arrays) and
    // exposes no content Hasher, so content-equal byte[] key lookups miss. byte[] as a
    // VALUE round-trips fine; byte[] keys belong on treeMap (ByteArrayFormat.compare).
    @Test public void hashMapStringKeyByteArrayValueRoundTrip() throws Exception {
        File f = freshFile();
        try {
            DB db = DBMaker.fileDB(f).transactionEnable().make();
            ConcurrentMap<String, byte[]> m =
                    db.hashMap("h", Serializers.STRING, Serializers.BYTE_ARRAY).create();
            m.put("k", new byte[]{4, 5, 6});
            m.put("empty", new byte[]{});
            assertArrayEquals(new byte[]{4, 5, 6}, m.get("k"));
            assertArrayEquals(new byte[]{}, m.get("empty"));
            db.commit();
            db.close();

            DB db2 = DBMaker.fileDB(f).transactionEnable().make();
            ConcurrentMap<String, byte[]> m2 =
                    db2.hashMap("h", Serializers.STRING, Serializers.BYTE_ARRAY).open();
            assertArrayEquals(new byte[]{4, 5, 6}, m2.get("k"));
            assertArrayEquals(new byte[]{}, m2.get("empty"));
            assertNull(m2.get("absent"));
            db2.close();
        } finally {
            cleanup(f);
        }
    }
}
