package org.mapdb.btree;

import org.junit.Test;
import org.mapdb.ser.LongFormat;
import org.mapdb.ser.Serializer;
import org.mapdb.store.RecordRead;
import org.mapdb.store.Store;
import org.mapdb.store.StoreByteArray;
import org.mapdb.store.StoreDelta;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Bulk-build certification: wrap a store in an instrumented
 * decorator and run the pump shape, asserting
 * <ul>
 * <li>every recid receives exactly ONE content write (B3/B4 — preallocate →
 *     update-fill, no rewrite of finalized nodes),</li>
 * <li>allocation is monotonic on a fresh store (B2 — recids and therefore data
 *     lay out in key order).</li>
 * </ul>
 */
public class PumpCertificationTest {

    /** StoreDelta decorator counting content writes per recid and allocation order. */
    static final class InstrumentedStore implements StoreDelta {
        final StoreDelta target = new StoreByteArray();
        final HashMap<Long, Integer> contentWrites = new HashMap<>();
        final ArrayList<Long> allocationOrder = new ArrayList<>();

        private void countWrite(long recid) {
            contentWrites.merge(recid, 1, Integer::sum);
        }

        @Override public long preallocate() {
            long recid = target.preallocate();
            allocationOrder.add(recid);
            return recid;
        }

        @Override public <R> long put(R record, Serializer<R> serializer) {
            long recid = target.put(record, serializer);
            allocationOrder.add(recid);
            countWrite(recid);
            return recid;
        }

        @Override public <R> R get(long recid, Serializer<R> serializer) { return target.get(recid, serializer); }
        @Override public long read(long recid, RecordRead action) { return target.read(recid, action); }

        @Override public <R> void update(long recid, R record, Serializer<R> serializer) {
            countWrite(recid);
            target.update(recid, record, serializer);
        }

        @Override public <R> boolean compareAndSwap(long recid, R expected, R newRecord, Serializer<R> serializer) {
            countWrite(recid);
            return target.compareAndSwap(recid, expected, newRecord, serializer);
        }

        @Override public <R> void delete(long recid, Serializer<R> serializer) { target.delete(recid, serializer); }
        @Override public void commit() { target.commit(); }
        @Override public void close() { target.close(); }
        @Override public boolean isClosed() { return target.isClosed(); }
        @Override public void verify() { target.verify(); }
        @Override public PrimitiveIterator.OfLong getAllRecids() { return target.getAllRecids(); }
        @Override public boolean isThreadSafe() { return target.isThreadSafe(); }

        @Override public long append(long recid, byte[] data, int offset, int len) {
            long r = target.append(recid, data, offset, len);
            if (r != StoreDelta.REFUSED) countWrite(recid);
            return r;
        }

        @Override public long capacityRemaining(long recid) { return target.capacityRemaining(recid); }

        @Override public <R> void updateWithHeadroom(long recid, R record, Serializer<R> serializer, int headroom) {
            countWrite(recid);
            target.updateWithHeadroom(recid, record, serializer, headroom);
        }

        void assertCertified() {
            // B3/B4: exactly one content write per live recid, none written twice
            PrimitiveIterator.OfLong recids = getAllRecids();
            long live = 0;
            while (recids.hasNext()) {
                long recid = recids.nextLong();
                assertEquals("content writes for recid " + recid,
                        Integer.valueOf(1), contentWrites.get(recid));
                live++;
            }
            assertEquals("every written recid is live", live, contentWrites.size());
            // B2: monotonic allocation on a fresh store
            for (int i = 1; i < allocationOrder.size(); i++) {
                assertTrue("allocation not monotonic at " + i,
                        allocationOrder.get(i) > allocationOrder.get(i - 1));
            }
        }
    }

    private static Iterator<Map.Entry<Long, Long>> ascending(long n) {
        return new Iterator<>() {
            long i = 0;
            @Override public boolean hasNext() { return i < n; }
            @Override public Map.Entry<Long, Long> next() {
                long k = i++;
                return new AbstractMap.SimpleImmutableEntry<>(k, k * 7 + 1);
            }
        };
    }

    @Test
    public void btreePumpWritesEachNodeExactlyOnce() {
        InstrumentedStore store = new InstrumentedStore();
        BTreeMap<Long, Long> m = BTreeMap.createFromSorted(store,
                LongFormat.INSTANCE, LongFormat.INSTANCE, 8, ascending(10_000));
        store.verify();
        store.assertCertified();
        assertEquals(10_000L, m.sizeLong());
    }

    @Test
    public void bufferTreePumpWritesEachNodeExactlyOnce() {
        InstrumentedStore store = new InstrumentedStore();
        BufferTreeMap<Long, Long> m = BufferTreeMap.createFromSorted(store,
                LongFormat.INSTANCE, LongFormat.INSTANCE, 8, 256, ascending(10_000));
        store.verify();
        store.assertCertified();
        assertEquals(10_000L, m.sizeLong());
    }

    /** The pump never leaks a preallocated recid: every reserved recid is filled. */
    @Test
    public void noDanglingPreallocations() {
        for (long n : new long[]{0, 1, 6, 7, 36, 1000}) {
            InstrumentedStore store = new InstrumentedStore();
            BTreeMap.createFromSorted(store, LongFormat.INSTANCE, LongFormat.INSTANCE, 8, ascending(n));
            List<Long> allocated = store.allocationOrder;
            assertEquals("every allocated recid written exactly once (n=" + n + ")",
                    allocated.size(), store.contentWrites.size());
        }
    }
}
