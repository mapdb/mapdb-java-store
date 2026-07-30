package org.mapdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.mapdb.btree.BTreeMap;
import org.mapdb.htree.HTreeMap;
import org.mapdb.ser.LongFormat;
import org.mapdb.ser.Serializers;
import org.mapdb.ser.StringGroupFormat;
import org.mapdb.store.Store;
import org.mapdb.store.StoreOnHeap;

public class MapExtraTest {
    @Test public void htreeAndBtreeExposeSharedExtras() {
        Store store = new StoreOnHeap(true);
        List<MapExtra<Long, String>> maps = new ArrayList<>();
        maps.add(HTreeMap.create(store, Serializers.LONG, Serializers.STRING, 0, 4, 8));
        maps.add(BTreeMap.create(store, LongFormat.INSTANCE, StringGroupFormat.INSTANCE, 8));
        for (MapExtra<Long, String> map : maps) {
            assertTrue(map.putIfAbsentBoolean(1L, "one"));
            assertFalse(map.putIfAbsentBoolean(1L, "other"));
            assertEquals(1L, map.sizeLong());
            List<Long> keys = new ArrayList<>();
            List<String> values = new ArrayList<>();
            map.forEachKey(keys::add);
            map.forEachValue(values::add);
            assertEquals(java.util.Collections.singletonList(1L), keys);
            assertEquals(java.util.Collections.singletonList("one"), values);
            assertFalse(map.isClosed());
        }
        store.close();
        assertTrue(maps.get(0).isClosed());
        assertTrue(maps.get(1).isClosed());
    }
}
