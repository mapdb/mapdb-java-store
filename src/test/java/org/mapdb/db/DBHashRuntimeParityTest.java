package org.mapdb.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.io.File;

import org.junit.Test;
import org.mapdb.btree.BTreeMap;
import org.mapdb.hash.Hashers;
import org.mapdb.htree.HTreeMap48;
import org.mapdb.ser.LongFormat;
import org.mapdb.ser.Serializers;
import org.mapdb.ser.StringGroupFormat;

/** DB-maker wiring for MapDB 3 modification listeners and value loaders. */
public class DBHashRuntimeParityTest {

    @Test public void plainAndExternalRuntimeOptions() {
        DB db = DBMaker.memoryDB().make();
        for (boolean external : new boolean[]{false, true}) {
            List<String> events = new ArrayList<>();
            String name = external ? "external" : "plain";
            ConcurrentMap<String, Long> map = db
                    .hashMap(name, Serializers.STRING, Serializers.LONG)
                    .valueInline(!external)
                    .valueLoader(key -> key.equals("loaded") ? 42L : null)
                    .modificationListener((key, oldValue, newValue, triggered) ->
                            events.add(key + ":" + oldValue + ":" + newValue + ":" + triggered))
                    .create();
            assertNull(map.put("a", 1L));
            assertEquals(Long.valueOf(1L), map.put("a", 2L));
            assertEquals(Long.valueOf(2L), map.remove("a"));
            assertEquals(Long.valueOf(42L), map.get("loaded"));
            assertEquals(java.util.Arrays.asList(
                    "a:null:1:false", "a:1:2:false", "a:2:null:false",
                    "loaded:null:42:false"), events);
        }
        db.close();
    }

    @Test public void cacheEvictionListenerIsMarkedTriggered() {
        DB db = DBMaker.memoryDB().make();
        List<String> events = new ArrayList<>();
        ConcurrentMap<String, Long> cache = db
                .hashMap("cache", Serializers.STRING, Serializers.LONG)
                .layout(0, 4, 8)
                .expireMaxSize(1)
                .modificationListener((key, oldValue, newValue, triggered) ->
                        events.add(key + ":" + oldValue + ":" + newValue + ":" + triggered))
                .create();
        cache.put("a", 1L);
        cache.put("b", 2L);
        cache.put("c", 3L); // foreground sweep evicts the oldest entry
        assertTrue(events.contains("a:1:null:true"));
        assertFalse(events.contains("a:1:null:false"));
        db.close();
    }

    @Test public void runtimeOptionsAttachOnCachedOpen() {
        DB db = DBMaker.memoryDB().make();
        ConcurrentMap<String, Long> map = db
                .hashMap("map", Serializers.STRING, Serializers.LONG).create();
        List<String> events = new ArrayList<>();
        ConcurrentMap<String, Long> same = db
                .hashMap("map", Serializers.STRING, Serializers.LONG)
                .modificationListener((key, oldValue, newValue, triggered) ->
                        events.add(key + ":" + newValue))
                .valueLoader(key -> 9L)
                .open();
        assertTrue(map == same);
        assertEquals(Long.valueOf(9L), map.get("missing"));
        assertEquals(java.util.Collections.singletonList("missing:9"), events);
        db.close();
    }

    @Test public void hash48AndTreeMapCompatibilityListeners() {
        DB db = DBMaker.memoryDB().make();
        List<String> events = new ArrayList<>();
        HTreeMap48<Long, String> wide = db
                .hashMap48("wide", Serializers.LONG, Serializers.STRING)
                .hasher(Hashers.LONG64)
                .valueLoader(key -> "loaded" + key)
                .modificationListener((key, oldValue, newValue, triggered) ->
                        events.add("h:" + key + ":" + triggered))
                .create();
        assertEquals("loaded7", wide.get(7L));

        BTreeMap<Long, String> tree = db
                .treeMap("tree", LongFormat.INSTANCE, StringGroupFormat.INSTANCE)
                .modificationListener((key, oldValue, newValue, triggered) ->
                        events.add("t:" + key + ":" + triggered))
                .create();
        tree.put(1L, "one");
        assertEquals(java.util.Arrays.asList("h:7:false", "t:1:false"), events);
        db.close();
    }

    @Test public void hashCounterPersistsAndTracksViewMutations() throws Exception {
        File file = File.createTempFile("mapdb-hash-counter", ".db");
        file.delete();
        try {
            DB db = DBMaker.fileDB(file).transactionEnable().make();
            ConcurrentMap<String, Long> map = db
                    .hashMap("counted", Serializers.STRING, Serializers.LONG)
                    .counterEnable().create();
            map.put("a", 1L);
            map.put("b", 2L);
            map.put("a", 3L);
            assertEquals(2, map.size());
            assertTrue(map.keySet().remove("b"));
            assertEquals(1, map.size());
            db.commit();
            db.close();

            DB reopened = DBMaker.fileDB(file).transactionEnable().make();
            @SuppressWarnings("unchecked")
            ConcurrentMap<String, Long> map2 = (ConcurrentMap<String, Long>) reopened.get("counted");
            assertEquals(1, map2.size());
            map2.clear();
            assertEquals(0, map2.size());
            reopened.close();
        } finally {
            file.delete();
            org.mapdb.store.WalTestKit.deleteStore(file);
        }
    }

    @Test public void hashSetCounterPersistsAndTracksIteratorMutations() throws Exception {
        File file = File.createTempFile("mapdb-hash-set-counter", ".db");
        file.delete();
        try {
            DB db = DBMaker.fileDB(file).transactionEnable().make();
            Set<String> set = db.hashSet("counted", Serializers.STRING)
                    .counterEnable().create();
            set.add("a");
            set.add("b");
            set.add("a");
            assertEquals(2, set.size());
            java.util.Iterator<String> iterator = set.iterator();
            iterator.next();
            iterator.remove();
            assertEquals(1, set.size());
            db.commit();
            db.close();

            DB reopened = DBMaker.fileDB(file).transactionEnable().make();
            @SuppressWarnings("unchecked")
            Set<String> set2 = (Set<String>) reopened.get("counted");
            assertEquals(1, set2.size());
            set2.clear();
            assertEquals(0, set2.size());
            reopened.close();
        } finally {
            file.delete();
            org.mapdb.store.WalTestKit.deleteStore(file);
        }
    }
}
