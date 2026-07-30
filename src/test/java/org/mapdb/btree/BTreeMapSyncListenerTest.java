package org.mapdb.btree;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.mapdb.SynchronousMapModificationListener;
import org.mapdb.ser.LongFormat;
import org.mapdb.ser.StringGroupFormat;
import org.mapdb.store.Store;
import org.mapdb.store.StoreOnHeap;

/**
 * A THROWING synchronous listener runs while the map holds the covering leaf's spin node lock,
 * after the mutation and counter update commit. It must never leak the node lock (a leaked spin
 * lock hangs every later op on that leaf forever) nor desync the size counter — this drills the
 * put-update, non-split insert, SPLITTING insert, remove and replace fire points, on inline and
 * external-value maps. Follow-up ops run in a bounded-join thread so a lock-leak regression
 * fails fast instead of hanging the build.
 */
public class BTreeMapSyncListenerTest {

    /** Sync listener that throws once when armed, then disarms. */
    private static final class ArmedThrow implements SynchronousMapModificationListener<Long, String> {
        final AtomicBoolean armed = new AtomicBoolean();
        @Override public void modify(Long key, String oldValue, String newValue, boolean triggered) {
            if (armed.getAndSet(false)) throw new IllegalStateException("listener boom");
        }
    }

    @Test public void throwingListenerLeavesInlineMapUsable() throws Exception {
        Store store = new StoreOnHeap(true);
        BTreeMap<Long, String> map = BTreeMap.create(store,
                LongFormat.INSTANCE, StringGroupFormat.INSTANCE, 4, true);
        drill(store, map);
    }

    @Test public void throwingListenerLeavesExternalValueMapUsable() throws Exception {
        Store store = new StoreOnHeap(true);
        BTreeMap<Long, String> map = BTreeMap.createExternalValues(store,
                LongFormat.INSTANCE, StringGroupFormat.INSTANCE, 4, true);
        drill(store, map);
    }

    private static void drill(Store store, BTreeMap<Long, String> map) throws Exception {
        ArmedThrow listener = new ArmedThrow();
        map.modificationListenerAdd(listener);
        map.put(0L, "v0");
        map.put(1L, "v1");

        // non-split insert (leaf below maxNodeSize=4)
        expectBoom(listener, () -> map.put(2L, "v2"));
        assertEquals("v2", map.get(2L));
        assertConsistent(map, 3);
        assertCompletes(() -> map.put(2L, "v2b")); // SAME leaf: node lock must not be leaked
        assertEquals("v2b", map.get(2L));

        // put over an existing key (update branch)
        expectBoom(listener, () -> map.put(0L, "v0b"));
        assertEquals("v0b", map.get(0L));
        assertConsistent(map, 3);
        assertCompletes(() -> map.put(0L, "v0c"));

        // SPLITTING insert: 5th key overflows maxNodeSize=4 and splits the ROOT leaf. The
        // listener's throwable is rethrown only AFTER separator/root propagation completes, so
        // the tree must stay fully operable — including the LATER split of the right half B,
        // which under a skipped root propagation would spin forever waiting for the level-1
        // left edge that was never created.
        map.put(3L, "v3");
        expectBoom(listener, () -> map.put(4L, "v4"));
        assertEquals("v4", map.get(4L));
        assertConsistent(map, 5);
        assertCompletes(() -> map.put(5L, "v5")); // fills B (split left B={2,3,4})
        assertCompletes(() -> map.put(6L, "v6")); // overflows B: forces B's own split+propagation
        assertCompletes(() -> map.put(7L, "v7"));
        assertEquals("v5", map.get(5L));
        assertEquals("v6", map.get(6L));
        assertEquals("v7", map.get(7L));
        assertConsistent(map, 8);
        map.remove(6L);
        map.remove(7L);
        assertConsistent(map, 6);

        // remove
        expectBoom(listener, () -> map.remove(5L));
        assertNull(map.get(5L));
        assertConsistent(map, 5);
        assertCompletes(() -> map.put(5L, "back"));
        assertConsistent(map, 6);

        // replace
        expectBoom(listener, () -> map.replace(1L, "v1b"));
        assertEquals("v1b", map.get(1L));
        assertConsistent(map, 6);
        assertCompletes(() -> map.remove(1L));
        assertConsistent(map, 5);

        store.verify();
        store.close();
    }

    /** A throwing sync listener must not hide the event from LATER sync listeners. */
    @Test public void throwingSyncListenerStillDeliversToLaterSyncListener() {
        Store store = new StoreOnHeap(true);
        BTreeMap<Long, String> map = BTreeMap.create(store,
                LongFormat.INSTANCE, StringGroupFormat.INSTANCE, 4, true);
        java.util.List<String> later = new java.util.ArrayList<>();
        map.modificationListenerAdd((SynchronousMapModificationListener<Long, String>)
                (key, oldValue, newValue, triggered) -> {
            throw new IllegalStateException("listener boom");
        });
        map.modificationListenerAdd((SynchronousMapModificationListener<Long, String>)
                (key, oldValue, newValue, triggered) ->
                        later.add(key + ":" + oldValue + ":" + newValue));
        try {
            map.put(1L, "v1");
            fail("armed listener did not throw");
        } catch (IllegalStateException expected) {
            assertEquals("listener boom", expected.getMessage());
        }
        assertEquals(java.util.Collections.singletonList("1:null:v1"), later);
        assertEquals("v1", map.get(1L));
        store.close();
    }

    /** Same continuation for ordinary (deferred) listeners in fireModified. */
    @Test public void throwingDeferredListenerStillDeliversToLaterListener() {
        Store store = new StoreOnHeap(true);
        BTreeMap<Long, String> map = BTreeMap.create(store,
                LongFormat.INSTANCE, StringGroupFormat.INSTANCE, 4, true);
        java.util.List<String> later = new java.util.ArrayList<>();
        map.modificationListenerAdd((key, oldValue, newValue, triggered) -> {
            throw new IllegalStateException("listener boom");
        });
        map.modificationListenerAdd((key, oldValue, newValue, triggered) ->
                later.add(key + ":" + oldValue + ":" + newValue));
        try {
            map.put(1L, "v1");
            fail("armed listener did not throw");
        } catch (IllegalStateException expected) {
            assertEquals("listener boom", expected.getMessage());
        }
        assertEquals(java.util.Collections.singletonList("1:null:v1"), later);
        assertEquals("v1", map.get(1L));
        store.close();
    }

    private static void expectBoom(ArmedThrow listener, Runnable op) {
        listener.armed.set(true);
        try {
            op.run();
            fail("armed listener did not throw");
        } catch (IllegalStateException expected) {
            assertEquals("listener boom", expected.getMessage());
        }
    }

    /** sizeLong (the O(1) counter) must equal the iterated entry count and the expectation. */
    private static void assertConsistent(BTreeMap<Long, String> map, long expectedSize) {
        assertEquals("counter desynced", expectedSize, map.sizeLong());
        long iterated = 0;
        for (Iterator<Map.Entry<Long, String>> it = map.entryIterator(); it.hasNext(); it.next())
            iterated++;
        assertEquals("counter vs iteration", expectedSize, iterated);
    }

    /** Run {@code op} in a fresh thread with a BOUNDED join: a leaked node lock spins forever,
     *  and this must fail the test rather than hang the build. */
    private static void assertCompletes(Runnable op) throws Exception {
        AtomicReference<Throwable> err = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try { op.run(); } catch (Throwable e) { err.set(e); }
        });
        t.setDaemon(true);
        t.start();
        t.join(30_000);
        if (t.isAlive()) fail("operation did not complete within 30s (leaked node lock?)");
        if (err.get() != null) throw new AssertionError(err.get());
    }
}
