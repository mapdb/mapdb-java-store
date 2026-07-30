package org.mapdb.htree;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.PrimitiveIterator;

import org.junit.Test;
import org.mapdb.SynchronousMapModificationListener;
import org.mapdb.ser.Serializers;
import org.mapdb.store.StoreByteArray;

/**
 * Listener failure semantics: a throwing listener must not abort clear() before later
 * segments are processed, must not hide events from later listeners, and must not skip
 * deferred delivery for the mutation that threw.
 */
public class HTreeListenerFailureTest {

    private static final int N = 200; // enough keys to populate many segments

    /** Sync listener that throws on every removal event. */
    private static <K, V> SynchronousMapModificationListener<K, V> syncThrower() {
        return (key, oldValue, newValue, triggered) -> {
            throw new RuntimeException("sync boom");
        };
    }

    private static void expectBoom(Runnable op, String message) {
        try {
            op.run();
            fail("expected listener throwable to propagate");
        } catch (RuntimeException e) {
            assertEquals(message, e.getMessage());
        }
    }

    @Test public void clearSurvivesThrowingSyncListener() {
        StoreByteArray store = new StoreByteArray();
        HTreeMap<String, Long> map = HTreeMap.create(store, Serializers.STRING, Serializers.LONG);
        for (long i = 0; i < N; i++) map.put("k" + i, i);
        map.modificationListenerAdd(syncThrower());
        expectBoom(map::clear, "sync boom");
        assertTrue("all segments must be cleared despite the throwing listener", map.isEmpty());
        store.close();
    }

    /** Ordinary listeners fire inside unlockWrite's deferEnd — a throw there must not
     *  abort the remaining segments either. */
    @Test public void clearSurvivesThrowingDeferredListener() {
        StoreByteArray store = new StoreByteArray();
        HTreeMap<String, Long> map = HTreeMap.create(store, Serializers.STRING, Serializers.LONG);
        for (long i = 0; i < N; i++) map.put("k" + i, i);
        map.modificationListenerAdd((key, oldValue, newValue, triggered) -> {
            throw new RuntimeException("deferred boom");
        });
        expectBoom(map::clear, "deferred boom");
        assertTrue(map.isEmpty());
        store.close();
    }

    @Test public void clearSurvivesThrowingSyncListener48() {
        StoreByteArray store = new StoreByteArray();
        HTreeMap48<String, Long> map =
                HTreeMap48.create(store, Serializers.STRING, Serializers.LONG);
        for (long i = 0; i < N; i++) map.put("k" + i, i);
        map.modificationListenerAdd(syncThrower());
        expectBoom(map::clear, "sync boom");
        assertTrue(map.isEmpty());
        store.close();
    }

    @Test public void clearSurvivesThrowingSyncListenerExternalNoRecidLeak() {
        StoreByteArray store = new StoreByteArray();
        HTreeMapExternal<String, Long> map =
                HTreeMapExternal.create(store, Serializers.STRING, Serializers.LONG);
        long baseline = recidCount(store); // roots + header only
        for (long i = 0; i < N; i++) map.put("k" + i, i);
        map.modificationListenerAdd(syncThrower());
        expectBoom(map::clear, "sync boom");
        assertTrue(map.isEmpty());
        assertEquals("external value/bucket records must not leak", baseline, recidCount(store));
        store.close();
    }

    @Test public void clearSurvivesThrowingSyncListenerCache() {
        StoreByteArray store = new StoreByteArray();
        HTreeCache<String, Long> map = HTreeCache.create(store,
                Serializers.STRING, Serializers.LONG, 0 /* no TTL */, false, 1_000_000);
        for (long i = 0; i < N; i++) map.put("k" + i, i);
        map.modificationListenerAdd(syncThrower());
        expectBoom(map::clear, "sync boom");
        assertEquals("all segments (and their counters) must be cleared", 0L, map.sizeLong());
        for (long i = 0; i < N; i++) assertNull(map.get("k" + i));
        store.close();
    }

    /** A throwing sync listener must not hide the event from later sync listeners, nor
     *  skip the deferred delivery to ordinary listeners (syncOnly=false path). */
    @Test public void syncThrowStillDeliversToLaterSyncAndDeferredListeners() {
        StoreByteArray store = new StoreByteArray();
        HTreeMap<String, Long> map = HTreeMap.create(store, Serializers.STRING, Serializers.LONG);
        List<String> laterSync = new ArrayList<>();
        List<String> deferred = new ArrayList<>();
        map.modificationListenerAdd(syncThrower());
        map.modificationListenerAdd((SynchronousMapModificationListener<String, Long>)
                (key, oldValue, newValue, triggered) ->
                        laterSync.add(key + ":" + oldValue + ":" + newValue));
        map.modificationListenerAdd((key, oldValue, newValue, triggered) ->
                deferred.add(key + ":" + oldValue + ":" + newValue));
        expectBoom(() -> map.put("k", 1L), "sync boom");
        assertEquals(java.util.Collections.singletonList("k:null:1"), laterSync);
        assertEquals(java.util.Collections.singletonList("k:null:1"), deferred);
        assertEquals(Long.valueOf(1L), map.get("k"));
        store.close();
    }

    /** deferEnd must deliver ALL queued events to ALL listeners even when one throws
     *  mid-batch, and must leave the deferral ThreadLocal cleared. */
    @Test public void deferEndContinuesPastThrowingListener() {
        MapRuntime<String, Long> runtime = new MapRuntime<>();
        List<String> first = new ArrayList<>();
        List<String> second = new ArrayList<>();
        runtime.add((key, oldValue, newValue, triggered) -> {
            first.add(key);
            if (key.equals("e1")) throw new RuntimeException("boom e1");
        });
        runtime.add((key, oldValue, newValue, triggered) -> second.add(key));
        runtime.deferBegin();
        runtime.fireDeferred("e1", 1L, null, false);
        runtime.fireDeferred("e2", 2L, null, false);
        try {
            runtime.deferEnd();
            fail("expected listener throwable to propagate");
        } catch (RuntimeException e) {
            assertEquals("boom e1", e.getMessage());
        }
        assertEquals(java.util.Arrays.asList("e1", "e2"), first);
        assertEquals(java.util.Arrays.asList("e1", "e2"), second);
        runtime.deferBegin(); // ThreadLocal was cleared: a fresh window must open cleanly
        runtime.deferEnd();
    }

    /** When a sync listener AND a deferred listener both throw for the same mutation, the
     *  SYNC throwable (the one already propagating from the try body) must stay primary —
     *  the deferred one (raised by deferEnd inside unlockWrite's finally) is suppressed
     *  behind it, not allowed to mask it. */
    @Test public void deferredThrowIsSuppressedBehindSyncThrow() {
        StoreByteArray store = new StoreByteArray();
        HTreeMap<String, Long> map = HTreeMap.create(store, Serializers.STRING, Serializers.LONG);
        map.modificationListenerAdd(syncThrower());
        map.modificationListenerAdd((key, oldValue, newValue, triggered) -> {
            throw new RuntimeException("deferred boom");
        });
        try {
            map.put("k", 1L);
            fail("expected listener throwable to propagate");
        } catch (RuntimeException e) {
            assertEquals("sync boom", e.getMessage());
            assertEquals(1, e.getSuppressed().length);
            assertEquals("deferred boom", e.getSuppressed()[0].getMessage());
        }
        assertEquals(Long.valueOf(1L), map.get("k")); // the mutation itself committed
        store.close();
    }

    /** A listener that rethrows one PRE-CONSTRUCTED exception instance for every event must
     *  not trip addSuppressed's self-suppression IllegalArgumentException — every delivery
     *  still happens and the shared instance propagates (with nothing suppressed). */
    @Test public void sharedExceptionInstanceDoesNotSelfSuppress() {
        StoreByteArray store = new StoreByteArray();
        HTreeMap<String, Long> map = HTreeMap.create(store, Serializers.STRING, Serializers.LONG);
        for (long i = 0; i < 20; i++) map.put("k" + i, i);
        RuntimeException shared = new RuntimeException("shared boom");
        List<String> delivered = new ArrayList<>();
        map.modificationListenerAdd((SynchronousMapModificationListener<String, Long>)
                (key, oldValue, newValue, triggered) -> { throw shared; });
        map.modificationListenerAdd((SynchronousMapModificationListener<String, Long>)
                (key, oldValue, newValue, triggered) -> delivered.add(key));
        try {
            map.clear();
            fail("expected the shared exception to propagate");
        } catch (RuntimeException e) {
            assertTrue("shared instance must propagate, not IllegalArgumentException",
                    e == shared);
            assertEquals(0, e.getSuppressed().length);
        }
        assertEquals("every removal must still reach the second listener", 20, delivered.size());
        assertTrue(map.isEmpty());
        store.close();
    }

    /** A STORE failure mid-clear must ABORT the loop (unlike a listener failure): the
     *  current segment is inconsistent, so continuing would destructively erase healthy
     *  later segments. No further deletes may run after the failing one. */
    @Test public void storeFailureAbortsClear() {
        FailingDeleteStore store = new FailingDeleteStore(new StoreByteArray());
        HTreeMap<String, Long> map = HTreeMap.create(store, Serializers.STRING, Serializers.LONG);
        for (long i = 0; i < N; i++) map.put("k" + i, i);
        store.deleteFailsAfter = 1; // second delete throws
        try {
            map.clear();
            fail("expected the store failure to propagate");
        } catch (RuntimeException e) {
            assertEquals("store boom", e.getMessage());
        }
        assertEquals("no destructive work may continue past a store failure",
                2, store.deleteCalls); // the successful one + the failing one
        assertTrue("later segments must keep their entries", !map.isEmpty());
    }

    /** One PRE-CONSTRUCTED exception instance used as BOTH an earlier segment's listener
     *  failure and a later segment's store mutation failure: the clear-abort path must not
     *  self-suppress (IllegalArgumentException) when attaching the recorded listener failure
     *  to the identical mutation throwable — the shared instance propagates as primary. */
    @Test public void sharedInstanceAcrossListenerAndStoreFailure() {
        RuntimeException shared = new RuntimeException("shared boom");
        FailingDeleteStore store = new FailingDeleteStore(new StoreByteArray());
        HTreeMap<String, Long> map = HTreeMap.create(store, Serializers.STRING, Serializers.LONG);
        for (long i = 0; i < N; i++) map.put("k" + i, i);
        // the listener throws shared for the FIRST segment's removal batch; from then on the
        // store's next delete (the NEXT segment's mutation phase) throws the SAME instance
        java.util.concurrent.atomic.AtomicBoolean armed =
                new java.util.concurrent.atomic.AtomicBoolean();
        map.modificationListenerAdd((SynchronousMapModificationListener<String, Long>)
                (key, oldValue, newValue, triggered) -> { armed.set(true); throw shared; });
        store.deleteThrows = () -> armed.get() ? shared : null;
        try {
            map.clear();
            fail("expected the shared exception to propagate");
        } catch (RuntimeException e) {
            assertTrue("shared instance must propagate as primary", e == shared);
            assertEquals(0, e.getSuppressed().length);
        }
        assertTrue("mutation failure must abort the loop", !map.isEmpty());
    }

    /** Store decorator whose {@code delete} starts throwing after a set number of calls,
     *  or throws whatever {@code deleteThrows} supplies (null = don't throw). */
    private static final class FailingDeleteStore implements org.mapdb.store.Store {
        final org.mapdb.store.Store d;
        int deleteFailsAfter = Integer.MAX_VALUE;
        int deleteCalls;
        java.util.function.Supplier<RuntimeException> deleteThrows;

        FailingDeleteStore(org.mapdb.store.Store d) { this.d = d; }

        @Override public <R> void delete(long recid, org.mapdb.ser.Serializer<R> ser) {
            deleteCalls++;
            if (deleteCalls > deleteFailsAfter) throw new RuntimeException("store boom");
            if (deleteThrows != null) {
                RuntimeException e = deleteThrows.get();
                if (e != null) throw e;
            }
            d.delete(recid, ser);
        }

        @Override public long preallocate() { return d.preallocate(); }
        @Override public <R> long put(R record, org.mapdb.ser.Serializer<R> ser) {
            return d.put(record, ser);
        }
        @Override public <R> R get(long recid, org.mapdb.ser.Serializer<R> ser) {
            return d.get(recid, ser);
        }
        @Override public long read(long recid, org.mapdb.store.RecordRead action) {
            return d.read(recid, action);
        }
        @Override public <R> void update(long recid, R record, org.mapdb.ser.Serializer<R> ser) {
            d.update(recid, record, ser);
        }
        @Override public <R> boolean compareAndSwap(long recid, R expected, R newRecord,
                org.mapdb.ser.Serializer<R> ser) {
            return d.compareAndSwap(recid, expected, newRecord, ser);
        }
        @Override public void commit() { d.commit(); }
        @Override public void close() { d.close(); }
        @Override public boolean isClosed() { return d.isClosed(); }
        @Override public void verify() { d.verify(); }
        @Override public PrimitiveIterator.OfLong getAllRecids() { return d.getAllRecids(); }
    }

    private static long recidCount(StoreByteArray store) {
        long count = 0;
        for (PrimitiveIterator.OfLong it = store.getAllRecids(); it.hasNext(); it.nextLong())
            count++;
        return count;
    }
}
