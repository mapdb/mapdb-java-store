package org.mapdb.store;

import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.mapdb.TmpFiles;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;
import org.mapdb.ser.Serializer;
import org.mapdb.ser.Serializers;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Proves the DeadlockAsserts guards fire: a RecordRead action or a Serializer that
 * calls back into the store while the store holds a lock must trip an
 * AssertionError instead of risking a lock-order deadlock. Requires -ea.
 */
public class DeadlockAssertsTest {

    @Before public void assertionsEnabled() {
        Assume.assumeTrue("run with -ea", DeadlockAsserts.ENABLED);
    }

    private static Store walStore() {
        try {
            File f = TmpFiles.tempFile("mapdb-deadlock", ".wal");
            f.delete();
            f.deleteOnExit();
            return new StoreWAL(f);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // ---- RecordRead action calling back into the store ----

    private void actionCallbackTrips(Store store) {
        long recid = store.put(1L, Serializers.LONG);
        boolean tripped = false;
        try {
            store.read(recid, new RecordRead() {
                @Override public long onBytes(DataInput2 in, int size) {
                    store.get(recid, Serializers.LONG); // forbidden reentry
                    return 0;
                }

                @Override public long onObject(Object record) {
                    store.get(recid, Serializers.LONG); // forbidden reentry
                    return 0;
                }
            });
        } catch (AssertionError e) {
            tripped = e.getMessage() != null && e.getMessage().contains("called back into the store");
        }
        assertTrue("action callback must trip the A3 assertion", tripped);
        // store must remain usable: the failed acquire did not corrupt lock state
        assertEquals((Long) 1L, store.get(recid, Serializers.LONG));
    }

    @Test public void actionCallback_onHeap() { actionCallbackTrips(new StoreOnHeap()); }
    @Test public void actionCallback_byteArray() { actionCallbackTrips(new StoreByteArray()); }
    @Test public void actionCallback_direct() { actionCallbackTrips(new StoreDirect()); }
    @Test public void actionCallback_wal() { actionCallbackTrips(walStore()); }
    @Test public void actionCallback_noLockModeStillTrips() { actionCallbackTrips(new StoreDirect(false, false)); }

    // ---- Serializer calling back into the store ----

    /** LONG codec whose deserialize/equals re-enters the store (both are invoked under locks). */
    private static final class EvilSerializer implements Serializer<Long> {
        Store store;
        long recid;

        @Override public void serialize(DataOutput2 out, Long value) { out.writeLong(value); }

        @Override public Long deserialize(DataInput2 in, int size) {
            if (store != null) store.get(recid, Serializers.LONG); // forbidden reentry
            return in.readLong();
        }

        @Override public boolean equals(Long a, Long b) {
            if (store != null) store.get(recid, Serializers.LONG); // forbidden reentry
            return a.equals(b);
        }
    }

    /** get() deserializes under the segment/global read lock — byte-backed stores. */
    private void serializerDeserializeTrips(Store store) {
        EvilSerializer evil = new EvilSerializer();
        long recid = store.put(7L, evil); // serialize runs outside locks: no trip
        evil.store = store;
        evil.recid = recid;
        boolean tripped = false;
        try {
            store.get(recid, evil);
        } catch (AssertionError e) {
            tripped = e.getMessage() != null && e.getMessage().contains("called back into the store");
        }
        assertTrue("serializer.deserialize callback must trip the A3 assertion", tripped);
    }

    @Test public void serializerCallback_byteArray() { serializerDeserializeTrips(new StoreByteArray()); }
    @Test public void serializerCallback_direct() { serializerDeserializeTrips(new StoreDirect()); }

    @Test public void serializerCallback_walStagedRead() {
        // uncommitted record: WAL deserializes the staged merge under its global lock
        Store store = walStore();
        serializerDeserializeTrips(store);
    }

    /** StoreOnHeap never deserializes, but CAS runs serializer.equals under the write lock. */
    @Test public void serializerEqualsCallback_onHeap() {
        StoreOnHeap store = new StoreOnHeap();
        EvilSerializer evil = new EvilSerializer();
        long recid = store.put(7L, evil);
        evil.store = store;
        evil.recid = recid;
        boolean tripped = false;
        try {
            store.compareAndSwap(recid, 7L, 8L, evil);
        } catch (AssertionError e) {
            tripped = e.getMessage() != null && e.getMessage().contains("called back into the store");
        }
        assertTrue("serializer.equals callback in CAS must trip the A3 assertion", tripped);
    }
}
