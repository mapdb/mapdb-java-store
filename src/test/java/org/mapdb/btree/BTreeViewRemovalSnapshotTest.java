package org.mapdb.btree;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mapdb.TmpFiles;
import org.mapdb.ser.LongFormat;
import org.mapdb.store.Store;
import org.mapdb.store.StoreDirect;
import org.mapdb.store.StoreWAL;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class BTreeViewRemovalSnapshotTest {
    @Parameterized.Parameters(name = "wal={0}, external={1}, descending={2}, bounded={3}")
    public static Collection<Object[]> cases() {
        Collection<Object[]> result = new ArrayList<>();
        for (boolean wal : new boolean[]{false, true})
            for (boolean external : new boolean[]{false, true})
                for (boolean descending : new boolean[]{false, true})
                    for (boolean bounded : new boolean[]{false, true})
                        result.add(new Object[]{wal, external, descending, bounded});
        return result;
    }

    private final boolean wal, external, descending, bounded;

    public BTreeViewRemovalSnapshotTest(boolean wal, boolean external, boolean descending, boolean bounded) {
        this.wal = wal;
        this.external = external;
        this.descending = descending;
        this.bounded = bounded;
    }

    private File file;

    private Store openStore() throws IOException {
        file = TmpFiles.tempFile("btree-removal-snapshot", wal ? ".wal" : ".sd1");
        file.delete();
        return wal ? new StoreWAL(file) : new StoreDirect(file);
    }

    private BTreeMap<Long, Long> create(Store store) {
        return external
                ? BTreeMap.createExternalValues(store, LongFormat.INSTANCE, LongFormat.INSTANCE, 8, true)
                : BTreeMap.create(store, LongFormat.INSTANCE, LongFormat.INSTANCE, 8, true);
    }

    private NavigableMap<Long, Long> range(NavigableMap<Long, Long> map) {
        // Nested bounds preserve the excluded edge keys.
        if (bounded) map = map.subMap(8L, true, 120L, true).subMap(16L, false, 112L, false);
        return descending ? map.descendingMap() : map.descendingMap().descendingMap();
    }

    private static Collection<?> projection(Map<Long, Long> map, int kind) {
        return kind == 0 ? map.entrySet() : kind == 1 ? map.keySet() : map.values();
    }

    @Test public void iteratorRemovalKeepsCursorAndCorrectContents() throws Exception {
        for (int kind = 0; kind < 3; kind++) {
            try (Store store = openStore()) {
                BTreeMap<Long, Long> map = create(store);
                TreeMap<Long, Long> oracle = new TreeMap<>();
                for (long key = 0; key < 128; key++) { map.put(key, key); oracle.put(key, key); }
                OrderedNavigableView<Long, Long> actual = (OrderedNavigableView<Long, Long>) range(map);
                AtomicInteger cursors = new AtomicInteger();
                // Count calls into the real adapter; all traversal and mutation
                // still execute BTreeMap production methods.
                @SuppressWarnings("unchecked")
                OrderedMapAdapter<Long, Long> adapter = (OrderedMapAdapter<Long, Long>) Proxy.newProxyInstance(
                        OrderedMapAdapter.class.getClassLoader(), new Class<?>[]{OrderedMapAdapter.class},
                        (proxy, method, args) -> {
                            if (method.getName().equals(descending ? "descendingEntryIterator" : "entryIterator"))
                                cursors.incrementAndGet();
                            try { return method.invoke(actual.a, args); }
                            catch (InvocationTargetException failure) { throw failure.getCause(); }
                        });
                OrderedNavigableView<Long, Long> counted = new OrderedNavigableView<>(adapter,
                        actual.lo, actual.loInc, actual.hi, actual.hiInc, actual.descending);
                Iterator<?> it = projection(counted, kind).iterator();
                Iterator<?> reference = projection(range(oracle), kind).iterator();
                assertThrows(IllegalStateException.class, it::remove);
                int removed = 0;
                while (reference.hasNext()) {
                    assertTrue(it.hasNext());
                    Object expected = reference.next();
                    assertEquals(expected, it.next());
                    // Exercise prefetched state across own removal.
                    assertEquals(reference.hasNext(), it.hasNext());
                    long key = kind == 0 ? (Long) ((Map.Entry<?, ?>) expected).getKey() : (Long) expected;
                    if (key % 3 != 0) {
                        it.remove(); reference.remove(); removed++;
                        assertThrows(IllegalStateException.class, it::remove);
                    }
                }
                assertFalse(it.hasNext());
                assertThrows(java.util.NoSuchElementException.class, it::next);
                assertEquals(oracle, map);
                assertEquals(oracle.size(), map.sizeLong());
                assertEquals("descending snapshots survive own removal; ascending retains reseek",
                        descending ? 1 : 1 + removed, cursors.get());
                store.commit(); store.verify();
            } finally { TmpFiles.delete(file); }
        }
    }

    @Test public void projectionRemoveIfDrainsOnlyRange() throws Exception {
        for (int kind = 0; kind < 3; kind++) {
            try (Store store = openStore()) {
                BTreeMap<Long, Long> map = create(store);
                TreeMap<Long, Long> oracle = new TreeMap<>();
                for (long key = 0; key < 128; key++) { map.put(key, key); oracle.put(key, key); }
                assertEquals(projection(range(oracle), kind).removeIf(value -> true),
                        projection(range(map), kind).removeIf(value -> true));
                assertFalse(projection(range(map), kind).removeIf(value -> true));
                assertEquals(oracle, map);
                assertEquals(oracle.size(), map.sizeLong());
                store.commit(); store.verify();
            } finally { TmpFiles.delete(file); }
        }
    }

    private BTreeMap<Long, Long> reopen(Store store, long root, long counter) {
        return external
                ? BTreeMap.openExternalValues(store, root, LongFormat.INSTANCE, LongFormat.INSTANCE, 8, counter)
                : BTreeMap.open(store, root, LongFormat.INSTANCE, LongFormat.INSTANCE, 8, counter);
    }

    @Test public void removalsAndFullDrainSurviveReopen() throws Exception {
        TreeMap<Long, Long> oracle = new TreeMap<>();
        long root, counter;
        try {
            try (Store store = openStore()) {
                BTreeMap<Long, Long> map = create(store);
                root = map.rootRecidRecid(); counter = map.counterRecid();
                for (long key = 0; key < 128; key++) { map.put(key, key); oracle.put(key, key); }
                Iterator<Long> actual = range(map).keySet().iterator();
                Iterator<Long> expected = range(oracle).keySet().iterator();
                while (expected.hasNext()) {
                    long key = expected.next();
                    assertEquals(Long.valueOf(key), actual.next());
                    assertEquals(expected.hasNext(), actual.hasNext());
                    if (key % 3 != 0) { actual.remove(); expected.remove(); }
                }
                assertFalse(actual.hasNext());
                assertEquals(oracle, map);
                store.commit();
            }
            try (Store store = wal ? new StoreWAL(file) : new StoreDirect(file)) {
                BTreeMap<Long, Long> map = reopen(store, root, counter);
                assertEquals(oracle, map);
                assertEquals(oracle.size(), map.sizeLong());
                assertTrue(map.descendingKeySet().removeIf(key -> true));
                assertTrue(map.isEmpty());
                store.commit();
            }
            try (Store store = wal ? new StoreWAL(file) : new StoreDirect(file)) {
                BTreeMap<Long, Long> map = reopen(store, root, counter);
                assertTrue(map.isEmpty());
                assertEquals(0, map.sizeLong());
                assertFalse(map.descendingMap().entrySet().iterator().hasNext());
                store.verify();
            }
        } finally { TmpFiles.delete(file); }
    }

}
