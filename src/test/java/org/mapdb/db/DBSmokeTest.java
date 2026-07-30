package org.mapdb.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.junit.Test;
import org.mapdb.DBException;
import org.mapdb.btree.BTreeMap;
import org.mapdb.indextree.IndexTreeList;
import org.mapdb.indextree.IndexTreeLongLongMap;
import org.mapdb.ser.LongFormat;
import org.mapdb.ser.Serializers;
import org.mapdb.ser.StringGroupFormat;

public class DBSmokeTest {

    @Test public void catalogAtRecid1() {
        DB db = DBMaker.memoryDB().make();
        assertEquals(1L, DB.RECID_CATALOG);
        assertTrue(db.getNameCatalog().isEmpty());
        db.close();
    }

    @Test public void hashMapRoundTrip() {
        DB db = DBMaker.memoryDB().make();
        ConcurrentMap<String, Long> m = db.hashMap("h", Serializers.STRING, Serializers.LONG).create();
        m.put("a", 1L);
        m.put("b", 2L);
        assertEquals((Long) 1L, m.get("a"));
        assertTrue(db.exists("h"));
        assertEquals("HashMap", db.getType("h"));
        db.close();
    }

    @Test public void treeMapRoundTrip() {
        DB db = DBMaker.memoryDB().make();
        BTreeMap<Long, String> m = db.treeMap("t", LongFormat.INSTANCE, StringGroupFormat.INSTANCE).create();
        for (long i = 0; i < 100; i++) m.put(i, "v" + i);
        assertEquals("v50", m.get(50L));
        assertEquals((Long) 0L, m.firstKey());
        assertEquals((Long) 99L, m.lastKey());
        db.close();
    }

    @Test public void setsAddThroughView() {
        DB db = DBMaker.memoryDB().make();
        Set<String> hs = db.hashSet("hs", Serializers.STRING).create();
        assertTrue(hs.add("x"));
        assertFalse(hs.add("x"));
        assertTrue(hs.contains("x"));
        assertEquals(1, hs.size());

        NavigableSet<Long> ts = db.treeSet("ts", LongFormat.INSTANCE).create();
        ts.add(3L); ts.add(1L); ts.add(2L);
        assertEquals((Long) 1L, ts.first());
        assertEquals((Long) 3L, ts.last());
        assertEquals(3, ts.size());
        assertTrue(ts.contains(2L));
        db.close();
    }

    @Test public void atomics() {
        DB db = DBMaker.memoryDB().make();
        Atomic.Long al = db.atomicLong("al", 10L).create();
        assertEquals(10L, al.get());
        assertEquals(11L, al.incrementAndGet());
        assertTrue(al.compareAndSet(11L, 20L));
        assertFalse(al.compareAndSet(11L, 30L));

        Atomic.Integer ai = db.atomicInteger("ai").create();
        assertEquals(0, ai.get());
        assertEquals(5, ai.addAndGet(5));

        Atomic.Boolean ab = db.atomicBoolean("ab", true).create();
        assertTrue(ab.get());
        ab.set(false);
        assertFalse(ab.get());

        Atomic.String as = db.atomicString("as").create();
        assertNull(as.get());
        as.set("hi");
        assertEquals("hi", as.get());

        Atomic.Var<String> av = db.atomicVar("av", Serializers.STRING, "init").create();
        assertEquals("init", av.get());
        av.set("next");
        assertEquals("next", av.get());
        db.close();
    }

    @Test public void indexTreeCollections() {
        DB db = DBMaker.memoryDB().make();
        IndexTreeList<String> list = db.indexTreeList("l", Serializers.STRING).create();
        list.add("a"); list.add("b"); list.add("c");
        assertEquals(3, list.size());
        assertEquals("b", list.get(1));

        IndexTreeLongLongMap m = db.indexTreeLongLongMap("ll").create();
        m.put(1L, 100L);
        assertEquals(100L, m.get(1L));
        db.close();
    }

    @Test public void persistAcrossReopen() throws Exception {
        File f = File.createTempFile("mapdb-db", ".db");
        f.delete();
        try {
            DB db = DBMaker.fileDB(f).transactionEnable().make();
            BTreeMap<Long, String> t = db.treeMap("t", LongFormat.INSTANCE, StringGroupFormat.INSTANCE).create();
            t.put(7L, "seven");
            db.hashMap("h", Serializers.STRING, Serializers.LONG).create().put("k", 42L);
            db.atomicLong("counter", 99L).create();
            db.commit();
            db.close();

            DB db2 = DBMaker.fileDB(f).transactionEnable().make();
            assertTrue(db2.exists("t"));
            BTreeMap<Long, String> t2 = db2.treeMap("t", LongFormat.INSTANCE, StringGroupFormat.INSTANCE).open();
            assertEquals("seven", t2.get(7L));
            ConcurrentMap<String, Long> h2 = db2.hashMap("h", Serializers.STRING, Serializers.LONG).open();
            assertEquals((Long) 42L, h2.get("k"));
            assertEquals(99L, db2.atomicLong("counter").open().get());
            // untyped get() dispatch
            @SuppressWarnings("unchecked")
            BTreeMap<Long, String> t3 = (BTreeMap<Long, String>) db2.get("t");
            assertEquals("seven", t3.get(7L));
            db2.close();
        } finally {
            f.delete();
            org.mapdb.store.WalTestKit.deleteStore(f);
        }
    }

    @Test public void createOrOpenReturnsSameInstance() {
        DB db = DBMaker.memoryDB().make();
        Object a = db.treeMap("t", LongFormat.INSTANCE, StringGroupFormat.INSTANCE).createOrOpen();
        Object b = db.treeMap("t", LongFormat.INSTANCE, StringGroupFormat.INSTANCE).createOrOpen();
        assertTrue("createOrOpen must return the cached singleton", a == b);
        db.close();
    }

    @Test public void createTwiceFails() {
        DB db = DBMaker.memoryDB().make();
        db.atomicLong("x").create();
        assertThrows(DBException.WrongConfiguration.class, () -> db.atomicLong("x").create());
        db.close();
    }

    @Test public void typeMismatchFails() {
        DB db = DBMaker.memoryDB().make();
        db.atomicLong("x").create();
        assertThrows(DBException.WrongConfiguration.class,
                () -> db.hashMap("x", Serializers.STRING, Serializers.LONG).open());
        db.close();
    }

    @Test public void renameAndDelete() {
        DB db = DBMaker.memoryDB().make();
        db.hashMap("old", Serializers.STRING, Serializers.LONG).create().put("k", 1L);
        db.rename("old", "new");
        assertFalse(db.exists("old"));
        assertTrue(db.exists("new"));
        ConcurrentMap<String, Long> m = db.hashMap("new", Serializers.STRING, Serializers.LONG).open();
        assertEquals((Long) 1L, m.get("k"));

        assertTrue(db.delete("new"));
        assertFalse(db.exists("new"));
        assertFalse(db.delete("new"));
        db.close();
    }

    @Test public void rollbackClearsCache() {
        File tmp;
        try { tmp = File.createTempFile("mapdb-rb", ".db"); tmp.delete(); }
        catch (Exception e) { throw new RuntimeException(e); }
        try {
            DB db = DBMaker.fileDB(tmp).transactionEnable().make();
            db.atomicLong("committed", 1L).create();
            db.commit();
            db.atomicLong("uncommitted", 2L).create();
            assertTrue(db.exists("uncommitted"));
            db.rollback();
            assertFalse(db.exists("uncommitted"));
            assertTrue(db.exists("committed"));
            db.close();
        } finally {
            tmp.delete();
            org.mapdb.store.WalTestKit.deleteStore(tmp);
        }
    }

    @Test public void rollbackUnsupportedOnNonTx() {
        DB db = DBMaker.memoryDB().make();
        assertThrows(UnsupportedOperationException.class, db::rollback);
        db.close();
    }

    @Test public void badNameRejected() {
        DB db = DBMaker.memoryDB().make();
        assertThrows(DBException.WrongConfiguration.class,
                () -> db.atomicLong("bad#name").create());
        db.close();
    }

    @Test public void transactionEnableRejectedForMemory() {
        assertThrows(DBException.WrongConfiguration.class,
                () -> DBMaker.memoryDB().transactionEnable().make());
    }

    @Test public void getAllNames() {
        DB db = DBMaker.memoryDB().make();
        db.atomicLong("a").create();
        db.atomicLong("b").create();
        int count = 0;
        for (String n : db.getAllNames()) count++;
        assertEquals(2, count);
        db.close();
    }

    @Test public void byteArrayKeysWithContentHasher() throws Exception {
        File f = File.createTempFile("mapdb-hash", ".db");
        f.delete();
        try {
            DB db = DBMaker.fileDB(f).transactionEnable().make();
            ConcurrentMap<byte[], Long> m = db.hashMap("b", Serializers.BYTE_ARRAY, Serializers.LONG)
                    .hasher(org.mapdb.hash.Hashers.mixing(java.util.Arrays::hashCode))
                    .create();
            m.put(new byte[]{1, 2, 3}, 42L);
            assertEquals("content-equal key must hit", (Long) 42L, m.get(new byte[]{1, 2, 3}));
            db.commit();
            db.close();

            DB db2 = DBMaker.fileDB(f).transactionEnable().make();
            // must re-supply the same custom hasher
            assertThrows(DBException.WrongConfiguration.class,
                    () -> db2.hashMap("b", Serializers.BYTE_ARRAY, Serializers.LONG).open());
            ConcurrentMap<byte[], Long> m2 = db2.hashMap("b", Serializers.BYTE_ARRAY, Serializers.LONG)
                    .hasher(org.mapdb.hash.Hashers.mixing(java.util.Arrays::hashCode))
                    .open();
            assertEquals((Long) 42L, m2.get(new byte[]{1, 2, 3}));
            db2.close();
        } finally {
            f.delete();
            org.mapdb.store.WalTestKit.deleteStore(f);
        }
    }

    @Test public void closeIsIdempotent() {
        DB db = DBMaker.memoryDB().make();
        db.close();
        db.close();
        assertTrue(db.isClosed());
        assertThrows(DBException.StoreClosed.class, () -> db.atomicLong("x").create());
    }
}
