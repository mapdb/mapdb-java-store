package org.mapdb.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.junit.Test;
import org.mapdb.DBException;
import org.mapdb.btree.BufferTreeMap;
import org.mapdb.hash.Hashers;
import org.mapdb.htree.HTreeMap48;
import org.mapdb.ser.LongFormat;
import org.mapdb.ser.Serializers;
import org.mapdb.ser.StringGroupFormat;

/** DB facade coverage for engines that previously existed only at the low-level API. */
public class DBExistingEnginesTest {

    @Test public void bufferTreeMapCreateBulkAndDispatch() {
        DB db = DBMaker.memoryDB().make();
        BufferTreeMap<Long, String> map = db
                .bufferTreeMap("buffered", LongFormat.INSTANCE, StringGroupFormat.INSTANCE)
                .maxNodeSize(8).bufferBytes(96).leafHeadroom(32).create();
        for (long i = 0; i < 200; i++) map.putOnly(i, "v" + i);
        assertEquals("v99", map.get(99L));
        assertEquals("BufferTreeMap", db.getType("buffered"));
        assertTrue(db.get("buffered") == map);

        TreeMap<Long, String> source = new TreeMap<>();
        for (long i = 0; i < 300; i++) source.put(i, "b" + i);
        BufferTreeMap<Long, String> bulk = db
                .bufferTreeMap("bulk", LongFormat.INSTANCE, StringGroupFormat.INSTANCE)
                .maxNodeSize(8).bufferBytes(96).leafHeadroom(0).createFrom(source);
        assertEquals(300, bulk.size());
        assertEquals("b222", bulk.get(222L));
        db.verify();
        db.close();
    }

    @Test public void bufferTreeMapPersistsAndOpensReadOnly() throws Exception {
        File file = File.createTempFile("mapdb5-buffer-db", ".db");
        file.delete();
        try {
            DB db = DBMaker.fileDB(file).make();
            BufferTreeMap<Long, String> map = db
                    .bufferTreeMap("buffered", LongFormat.INSTANCE, StringGroupFormat.INSTANCE)
                    .maxNodeSize(8).bufferBytes(96).leafHeadroom(32).create();
            for (long i = 0; i < 100; i++) map.putOnly(i, "v" + i);
            db.commit();
            db.close();

            DB ro = DBMaker.fileDB(file).readOnly().make();
            BufferTreeMap<Long, String> reopened = ro
                    .bufferTreeMap("buffered", LongFormat.INSTANCE, StringGroupFormat.INSTANCE).open();
            assertEquals("v42", reopened.get(42L));
            assertThrows(UnsupportedOperationException.class, () -> reopened.putOnly(101L, "blocked"));
            ro.close();
        } finally {
            file.delete();
            org.mapdb.store.WalTestKit.deleteStore(file);
        }
    }

    @Test public void bufferTreeRejectsNonDeltaHeapStore() {
        DB db = DBMaker.heapDB().make();
        assertThrows(DBException.WrongConfiguration.class, () -> db
                .bufferTreeMap("buffered", LongFormat.INSTANCE, StringGroupFormat.INSTANCE).create());
        db.close();
    }

    @Test public void appendOnlyDbFactoriesSupportNamedCollections() {
        for (DB db : new DB[]{DBMaker.memoryAppendOnlyDB().make(),
                DBMaker.memoryAppendOnlyDirectDB().make()}) {
            try {
                db.hashMap("hash", Serializers.STRING, Serializers.LONG).create().put("a", 1L);
                BufferTreeMap<Long, String> buffered = db
                        .bufferTreeMap("buffered", LongFormat.INSTANCE, StringGroupFormat.INSTANCE)
                        .maxNodeSize(8).bufferBytes(64).create();
                buffered.putOnly(1L, "one");
                assertEquals((Long) 1L, db.hashMap("hash", Serializers.STRING, Serializers.LONG).open().get("a"));
                assertEquals("one", buffered.get(1L));
                db.verify();
            } finally {
                db.close();
            }
        }
    }

    @Test public void hashMap48PersistsWithBuiltinHasherAndUntypedDispatch() throws Exception {
        File file = File.createTempFile("mapdb5-hash48-db", ".db");
        file.delete();
        try {
            DB db = DBMaker.fileDB(file).transactionEnable().make();
            HTreeMap48<Long, String> map = db
                    .hashMap48("wide", Serializers.LONG, Serializers.STRING)
                    .hasher(Hashers.LONG64).hashSeed(111L).create();
            for (long i = 0; i < 1000; i++) map.put(i, "v" + i);
            db.commit();
            db.close();

            DB reopened = DBMaker.fileDB(file).transactionEnable().make();
            @SuppressWarnings("unchecked")
            HTreeMap48<Long, String> map2 = (HTreeMap48<Long, String>) reopened.get("wide");
            assertEquals("v777", map2.get(777L));
            map2.put(1000L, "v1000");
            reopened.close();
        } finally {
            file.delete();
            org.mapdb.store.WalTestKit.deleteStore(file);
        }
    }

    @Test public void hashMap48BulkBuildViaDbMaker() {
        long seed = 987654321L;
        List<Map.Entry<Long, String>> entries = new ArrayList<>();
        for (long i = 0; i < 500; i++) {
            entries.add(new AbstractMap.SimpleImmutableEntry<>(i, "v" + i));
        }
        entries.sort((a, b) -> Long.compareUnsigned(
                Hashers.LONG64.hash(a.getKey(), seed),
                Hashers.LONG64.hash(b.getKey(), seed)));

        DB db = DBMaker.memoryDB().make();
        HTreeMap48<Long, String> map = db
                .hashMap48("wide", Serializers.LONG, Serializers.STRING)
                .hasher(Hashers.LONG64).hashSeed(seed)
                .createFromSortedByHash(entries.iterator());
        assertEquals(500, map.size());
        assertEquals("v333", map.get(333L));
        assertTrue(db.get("wide") == map);
        db.verify();
        db.close();
    }

    @Test public void hashSet48CreateAndDispatch() {
        DB db = DBMaker.memoryDB().make();
        Set<Long> set = db.hashSet48("wideSet", Serializers.LONG)
                .hasher(Hashers.LONG64).hashSeed(42L).create();
        for (long i = 0; i < 500; i++) assertTrue(set.add(i));
        assertEquals(500, set.size());
        assertTrue(set.contains(321L));
        assertTrue(db.get("wideSet") == set);
        db.verify();
        db.close();
    }
}
