package org.mapdb.db;

import org.mapdb.TmpFiles;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.mapdb.DBException;
import org.mapdb.btree.BTreeMap;
import org.mapdb.ser.LongFormat;
import org.mapdb.ser.StringGroupFormat;
import org.mapdb.store.StoreByteArray;

/** MapDB 3 parity coverage for DB catalog utilities and streaming BTree creation. */
public class DBParityUtilitiesTest {

    @Test public void lowLevelBTreeStreamingSink() {
        StoreByteArray store = new StoreByteArray();
        BTreeMap.Sink<Long, String> sink = BTreeMap.createFromSink(
                store, LongFormat.INSTANCE, StringGroupFormat.INSTANCE, 8, true);
        for (long i = 0; i < 500; i++) sink.put(i, "v" + i);
        BTreeMap<Long, String> map = sink.create();
        assertEquals(500L, map.sizeLong());
        assertTrue(map.counterRecid() > 0);
        assertEquals("v321", map.get(321L));
        assertThrows(IllegalStateException.class, () -> sink.put(501L, "late"));
        assertThrows(IllegalStateException.class, sink::create);
        store.verify();
        store.close();
    }

    @Test public void lowLevelSinkRejectsUnsortedInput() {
        StoreByteArray store = new StoreByteArray();
        BTreeMap.Sink<Long, String> sink = BTreeMap.createFromSink(
                store, LongFormat.INSTANCE, StringGroupFormat.INSTANCE, 8);
        sink.put(2L, "two");
        assertThrows(DBException.NotSorted.class, () -> sink.put(1L, "one"));
        store.close();
    }

    @Test public void dbStreamingSinkPersistsCounterAndListeners() throws Exception {
        File file = TmpFiles.tempFile("mapdb-tree-sink", ".db");
        file.delete();
        try {
            List<String> events = new ArrayList<>();
            DB db = DBMaker.fileDB(file).transactionEnable().make();
            DB.TreeMapMaker<Long, String>.DbSink sink = db
                    .treeMap("tree", LongFormat.INSTANCE, StringGroupFormat.INSTANCE)
                    .maxNodeSize(8)
                    .counterEnable()
                    .modificationListener((key, oldValue, newValue) ->
                            events.add(key + ":" + oldValue + ":" + newValue))
                    .createFromSink();
            for (long i = 0; i < 300; i++) sink.put(i, "v" + i);
            BTreeMap<Long, String> map = sink.create();
            assertTrue(events.isEmpty()); // bulk input predates the live listener
            assertEquals(300L, map.sizeLong());
            map.put(300L, "v300");
            assertEquals(java.util.Collections.singletonList("300:null:v300"), events);
            db.commit();
            db.close();

            DB reopened = DBMaker.fileDB(file).transactionEnable().make();
            BTreeMap<Long, String> map2 = reopened
                    .treeMap("tree", LongFormat.INSTANCE, StringGroupFormat.INSTANCE).open();
            assertEquals(301L, map2.sizeLong());
            assertEquals("v222", map2.get(222L));
            reopened.close();
        } finally {
            file.delete();
            org.mapdb.store.WalTestKit.deleteStore(file);
        }
    }

    @Test public void catalogIntrospectionAndVerification() {
        DB db = DBMaker.memoryByteArrayDB().make();
        BTreeMap<Long, String> map = db
                .treeMap("tree", LongFormat.INSTANCE, StringGroupFormat.INSTANCE).create();
        Atomic.Long atomic = db.atomicLong("counter", 42L).create();

        assertEquals("tree", db.getNameForObject(map));
        assertEquals("counter", db.getNameForObject(atomic));
        assertNull(db.getNameForObject(new Object()));
        Map<String, Object> all = db.getAll();
        assertTrue(all.get("tree") == map);
        assertTrue(all.get("counter") == atomic);
        assertEquals("TreeMap", db.nameCatalogParamsFor("tree").get("tree#type"));
        assertTrue(db.nameCatalogVerifyGetMessages().isEmpty());
        db.verify();

        java.util.TreeMap<String, String> corrupted =
                new java.util.TreeMap<>(db.getNameCatalog());
        corrupted.remove("tree#rootRecidRecid");
        db.getStore().update(DB.RECID_CATALOG, corrupted, DB.CATALOG_SER);
        List<String> messages = db.nameCatalogVerifyGetMessages();
        assertFalse(messages.isEmpty());
        assertTrue(messages.get(0).contains("rootRecidRecid"));
        assertThrows(DBException.VerifyFailed.class, db::verify);
        db.close();
    }
}
