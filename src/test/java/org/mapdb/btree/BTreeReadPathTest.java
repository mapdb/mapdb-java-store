package org.mapdb.btree;

import org.junit.Test;
import org.mapdb.ser.LongFormat;
import org.mapdb.ser.Serializer;
import org.mapdb.ser.Serializers;
import org.mapdb.store.RecordRead;
import org.mapdb.store.Store;
import org.mapdb.store.StoreDirect;
import org.mapdb.store.StoreOnHeap;

import java.util.PrimitiveIterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Proves the push-down read architecture: {@code get()} traverses nodes through the
 * store's push-down {@link Store#read} primitive on BOTH dialects
 *  - heap store -> {@code onObject} handle,
 *  - byte store -> {@code onBytes} handle,
 * and never materializes a node via the slow-path {@link Store#get}.
 */
public class BTreeReadPathTest {

    /** (a) onObject path: heap store read works. */
    @Test
    public void heapGetWorks() {
        StoreOnHeap store = new StoreOnHeap();
        BTreeMap<Long, Long> m = BTreeMap.create(store, LongFormat.INSTANCE, LongFormat.INSTANCE, 8);
        for (long k = 0; k < 5_000; k++) m.put(k, k * 2);
        for (long k = 0; k < 5_000; k++) assertEquals(Long.valueOf(k * 2), m.get(k));
        store.close();
    }

    /** (b) onBytes path: direct store read works. */
    @Test
    public void directGetWorks() {
        StoreDirect store = new StoreDirect();
        BTreeMap<Long, Long> m = BTreeMap.create(store, LongFormat.INSTANCE, LongFormat.INSTANCE, 8);
        for (long k = 0; k < 5_000; k++) m.put(k, k * 2);
        for (long k = 0; k < 5_000; k++) assertEquals(Long.valueOf(k * 2), m.get(k));
        store.close();
    }

    /**
     * (c) A forwarding decorator counts read() vs node-materializing get() calls.
     * A map.get() must drive node traversal exclusively through read() (push-down);
     * it must never deserialize a node via store.get. The single get() the read path
     * DOES issue is the root-pointer indirection (a plain Long, serializer == LONG),
     * which (P2: get is the slow path, allowed off the node hot-path) is not
     * a node materialization — the decorator excludes it. The put path may use
     * store.get freely and is not measured.
     */
    @Test
    public void getUsesOnlyPushDownReadsForNodes() {
        StoreDirect inner = new StoreDirect();
        CountingStore counting = new CountingStore(inner);

        BTreeMap<Long, Long> m = BTreeMap.create(counting, LongFormat.INSTANCE, LongFormat.INSTANCE, 8);
        for (long k = 0; k < 20_000; k++) m.put(k, k + 1); // deep, multi-level tree

        // measure a batch of point lookups
        counting.reset();
        for (long k = 0; k < 20_000; k++) {
            assertEquals(Long.valueOf(k + 1), m.get(k));
        }

        assertTrue("push-down read() must be used for node traversal; reads=" + counting.reads,
                counting.reads > 0);
        // every lookup on a multi-level tree needs >1 hop -> at least one read per get
        assertTrue("expected >= one read per get", counting.reads >= 20_000);
        assertEquals("no node may be materialized via store.get on the read path",
                0, counting.nodeGets);

        counting.close();
    }

    // ---------- forwarding Store that counts reads and node-materializing gets ----------

    private static final class CountingStore implements Store {
        private final Store d;
        long reads = 0;
        long nodeGets = 0; // get() calls that deserialize something other than the Long root pointer

        CountingStore(Store delegate) { this.d = delegate; }

        void reset() { reads = 0; nodeGets = 0; }

        @Override public long read(long recid, RecordRead action) {
            reads++;
            return d.read(recid, action);
        }

        @Override public <R> R get(long recid, Serializer<R> serializer) {
            if (serializer != Serializers.LONG) nodeGets++;
            return d.get(recid, serializer);
        }

        // ---- pure delegation ----
        @Override public long preallocate() { return d.preallocate(); }
        @Override public void preallocate(int count, long[] into) { d.preallocate(count, into); }
        @Override public <R> long put(R record, Serializer<R> serializer) { return d.put(record, serializer); }
        @Override public <R> void update(long recid, R record, Serializer<R> serializer) { d.update(recid, record, serializer); }
        @Override public <R> boolean compareAndSwap(long recid, R e, R n, Serializer<R> s) { return d.compareAndSwap(recid, e, n, s); }
        @Override public <R> void delete(long recid, Serializer<R> serializer) { d.delete(recid, serializer); }
        @Override public void commit() { d.commit(); }
        @Override public void close() { d.close(); }
        @Override public boolean isClosed() { return d.isClosed(); }
        @Override public void verify() { d.verify(); }
        @Override public PrimitiveIterator.OfLong getAllRecids() { return d.getAllRecids(); }
    }
}
