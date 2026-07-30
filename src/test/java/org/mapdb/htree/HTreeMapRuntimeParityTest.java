package org.mapdb.htree;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.mapdb.ser.Serializers;
import org.mapdb.store.StoreByteArray;

/** MapDB 3 runtime listener, clear-mode and value-loader parity. */
public class HTreeMapRuntimeParityTest {

    @Test public void modificationEventsAndClearModes() {
        StoreByteArray store = new StoreByteArray();
        HTreeMap<String, Long> map = HTreeMap.create(
                store, Serializers.STRING, Serializers.LONG);
        List<String> events = new ArrayList<>();
        map.modificationListenerAdd((key, oldValue, newValue, triggered) ->
                events.add(key + ":" + oldValue + ":" + newValue + ":" + triggered));

        assertNull(map.put("a", 1L));
        assertEquals(Long.valueOf(1L), map.put("a", 2L));
        assertEquals(Long.valueOf(2L), map.putIfAbsent("a", 3L));
        assertFalse(map.remove("a", 9L));
        assertTrue(map.replace("a", 2L, 4L));
        assertEquals(Long.valueOf(4L), map.remove("a"));
        assertEquals(java.util.Arrays.asList(
                "a:null:1:false", "a:1:2:false", "a:2:4:false", "a:4:null:false"), events);

        events.clear();
        map.put("b", 1L);
        map.clearWithoutNotification();
        assertEquals(java.util.Collections.singletonList("b:null:1:false"), events);
        events.clear();
        map.put("c", 2L);
        events.clear();
        map.clearWithExpire();
        assertEquals(java.util.Collections.singletonList("c:2:null:true"), events);
        store.close();
    }

    @Test public void valueLoaderPromotesMissExactlyOnce() {
        StoreByteArray store = new StoreByteArray();
        HTreeMap<String, Long> map = HTreeMap.create(
                store, Serializers.STRING, Serializers.LONG);
        int[] calls = {0};
        map.valueLoader(key -> {
            calls[0]++;
            return key.equals("load") ? 42L : null;
        });
        assertEquals(Long.valueOf(42L), map.get("load"));
        assertEquals(Long.valueOf(42L), map.get("load"));
        assertNull(map.get("absent"));
        assertEquals(2, calls[0]); // once for load, once for the distinct absent key
        assertEquals(1, map.size());
        store.close();
    }

    @Test public void crossMapListenersRunOutsideSegmentLocks() throws Exception {
        StoreByteArray store = new StoreByteArray();
        HTreeMap<String, Long> left = HTreeMap.create(store, Serializers.STRING, Serializers.LONG);
        HTreeMap<String, Long> right = HTreeMap.create(store, Serializers.STRING, Serializers.LONG);
        CountDownLatch bothCallbacks = new CountDownLatch(2);
        left.modificationListenerAdd((key, oldValue, newValue, triggered) -> {
            if (!key.equals("start-left")) return;
            bothCallbacks.countDown();
            try { assertTrue(bothCallbacks.await(5, TimeUnit.SECONDS)); }
            catch (InterruptedException e) { throw new AssertionError(e); }
            right.put("from-left", 1L);
        });
        right.modificationListenerAdd((key, oldValue, newValue, triggered) -> {
            if (!key.equals("start-right")) return;
            bothCallbacks.countDown();
            try { assertTrue(bothCallbacks.await(5, TimeUnit.SECONDS)); }
            catch (InterruptedException e) { throw new AssertionError(e); }
            left.put("from-right", 1L);
        });
        Thread a = new Thread(() -> left.put("start-left", 1L));
        Thread b = new Thread(() -> right.put("start-right", 1L));
        a.start(); b.start();
        a.join(10_000); b.join(10_000);
        assertFalse("left mutation deadlocked", a.isAlive());
        assertFalse("right mutation deadlocked", b.isAlive());
        assertEquals(Long.valueOf(1), left.get("from-right"));
        assertEquals(Long.valueOf(1), right.get("from-left"));
        store.close();
    }
}
