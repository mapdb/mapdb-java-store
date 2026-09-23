package org.mapdb.btree;

import org.junit.Test;
import org.mapdb.TmpFiles;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;
import org.mapdb.ser.GroupCursor;
import org.mapdb.ser.GroupFormat;
import org.mapdb.ser.LongFormat;
import org.mapdb.ser.ObjectArrayFormat;
import org.mapdb.ser.Serializer;
import org.mapdb.ser.Serializers;
import org.mapdb.store.StoreDirect;
import org.mapdb.store.StoreWAL;

import java.io.File;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Regression guard for the byte-side point get (S1). On a format with
 * {@code supportsBinary()}, {@code GetAction.onBytes} searches the key group
 * with {@code binarySearch} and, on a hit, reads the value with {@code binaryGet}.
 * {@code GroupFormat.deserialize} runs only when that format declares no binary
 * support. A present-key get of an inline value still does not deserialize the
 * node as objects.
 */
public class BTreeDirectGetNoDeserializeTest {

    private static final int KEYS = 16;
    /** Large enough that 16 keys stay in the root leaf, so one get is one node. */
    private static final int MAX_NODE = 32;

    @Test
    public void directBinaryGetDoesNotDeserialize() {
        StoreDirect store = new StoreDirect();
        try {
            AtomicInteger deserialize = new AtomicInteger();
            AtomicInteger binary = new AtomicInteger();
            CountingFormat<Long> keys = new CountingFormat<>(LongFormat.INSTANCE, deserialize, binary);
            CountingFormat<Long> values = new CountingFormat<>(LongFormat.INSTANCE, deserialize, binary);
            BTreeMap<Long, Long> map = BTreeMap.create(store, keys, values, MAX_NODE);
            putKeys(map);
            // StoreDirect writes the node into the volume on put. Same durability
            // point as BTreeReadPathTest.directGetWorks: no separate commit.
            assertByteSideGet(map, deserialize, binary);
        } finally {
            store.close();
        }
    }

    @Test
    public void walCommittedGetDoesNotDeserialize() throws Exception {
        File file = TmpFiles.tempFile("btree-direct-get", ".wal");
        file.delete();
        StoreWAL wal = new StoreWAL(file);
        try {
            AtomicInteger deserialize = new AtomicInteger();
            AtomicInteger binary = new AtomicInteger();
            CountingFormat<Long> keys = new CountingFormat<>(LongFormat.INSTANCE, deserialize, binary);
            CountingFormat<Long> values = new CountingFormat<>(LongFormat.INSTANCE, deserialize, binary);
            BTreeMap<Long, Long> map = BTreeMap.create(wal, keys, values, MAX_NODE);
            putKeys(map);
            // Uncommitted WAL reads the staged bytes through onBytes.
            assertByteSideGet(map, deserialize, binary);
            wal.commit();
            // After commit the same get reads the inner StoreDirect record.
            assertByteSideGet(map, deserialize, binary);
        } finally {
            wal.close();
            TmpFiles.delete(file);
        }
    }

    /**
     * Negative control. {@code ObjectArrayFormat} is the objects-only side of the
     * same {@code supportsBinary()} test, so a get must call {@code deserialize}.
     * A counter that nothing increments cannot pass this test.
     */
    @Test
    public void objectFormatGetDoesDeserialize() {
        StoreDirect store = new StoreDirect();
        try {
            AtomicInteger deserialize = new AtomicInteger();
            AtomicInteger binary = new AtomicInteger();
            CountingFormat<Long> keys = new CountingFormat<>(
                    new ObjectArrayFormat<>(Serializers.LONG), deserialize, binary);
            BTreeMap<Long, Long> map = BTreeMap.create(store, keys, LongFormat.INSTANCE, MAX_NODE);
            putKeys(map);
            deserialize.set(0);
            binary.set(0);
            assertEquals(Long.valueOf(100), map.get(0L));
            assertNull(map.get(-1L));
            assertTrue("objects-only key search must deserialize the key group; deserialize="
                    + deserialize.get(), deserialize.get() > 0);
        } finally {
            store.close();
        }
    }

    private static void putKeys(BTreeMap<Long, Long> map) {
        for (long k = 0; k < KEYS; k++) map.put(k, k + 100);
    }

    private static void assertByteSideGet(BTreeMap<Long, Long> map,
                                           AtomicInteger deserialize, AtomicInteger binary) {
        deserialize.set(0);
        binary.set(0);
        assertEquals(Long.valueOf(100), map.get(0L));
        assertNull(map.get(-1L));
        assertEquals("binary key search must not deserialize the group", 0, deserialize.get());
        assertTrue("byte-side search must run; binary=" + binary.get(), binary.get() > 0);
    }

    /**
     * Test double over a real {@link GroupFormat}. Counts {@code deserialize}
     * against {@code binarySearch} and {@code binaryGet}, which is the byte side
     * {@code GetAction.onBytes} calls. No production counter.
     */
    private static final class CountingFormat<A> implements GroupFormat<A> {
        final GroupFormat<A> inner;
        final AtomicInteger deserialize;
        final AtomicInteger binary;

        CountingFormat(GroupFormat<A> inner, AtomicInteger deserialize, AtomicInteger binary) {
            this.inner = inner;
            this.deserialize = deserialize;
            this.binary = binary;
        }

        @Override public Serializer<A> element() { return inner.element(); }
        @Override public Object empty() { return inner.empty(); }
        @Override public int size(Object group) { return inner.size(group); }
        @Override public A get(Object group, int pos) { return inner.get(group, pos); }
        @Override public int search(Object group, A key) { return inner.search(group, key); }
        @Override public int compare(A a, A b) { return inner.compare(a, b); }
        @Override public Comparator<A> comparator() { return inner.comparator(); }
        @Override public Object insert(Object group, int pos, A newValue) { return inner.insert(group, pos, newValue); }
        @Override public Object set(Object group, int pos, A newValue) { return inner.set(group, pos, newValue); }
        @Override public Object delete(Object group, int pos) { return inner.delete(group, pos); }
        @Override public Object copyRange(Object group, int from, int to) { return inner.copyRange(group, from, to); }
        @Override public Object fromArray(Object[] values) { return inner.fromArray(values); }
        @Override public void serialize(DataOutput2 out, Object group) { inner.serialize(out, group); }

        @Override public Object deserialize(DataInput2 in, int size) {
            deserialize.incrementAndGet();
            return inner.deserialize(in, size);
        }

        @Override public boolean supportsBinary() { return inner.supportsBinary(); }

        @Override public int binarySearch(A key, DataInput2 in, int size) {
            binary.incrementAndGet();
            return inner.binarySearch(key, in, size);
        }

        @Override public A binaryGet(DataInput2 in, int size, int pos) {
            binary.incrementAndGet();
            return inner.binaryGet(in, size, pos);
        }

        @Override public boolean supportsRangeCursor() { return inner.supportsRangeCursor(); }

        @Override public GroupCursor<A> rangeCursor(DataInput2 in, int size, int from, int to) {
            return inner.rangeCursor(in, size, from, to);
        }
    }
}
