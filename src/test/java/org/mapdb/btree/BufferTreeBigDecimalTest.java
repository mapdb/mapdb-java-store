package org.mapdb.btree;

import org.junit.Test;
import org.mapdb.TmpFiles;
import org.mapdb.ser.GroupFormat;
import org.mapdb.ser.ObjectArrayFormat;
import org.mapdb.ser.Serializers;
import org.mapdb.store.StoreWAL;

import java.io.File;
import java.math.BigDecimal;
import java.util.Map;

import static org.junit.Assert.*;

public class BufferTreeBigDecimalTest {
    private static final GroupFormat<BigDecimal> KEYS = new ObjectArrayFormat<>(Serializers.BIG_DECIMAL);
    private static final GroupFormat<String> VALUES = new ObjectArrayFormat<>(Serializers.STRING);

    private static void assertEquivalent(Map<BigDecimal, String> buffered, Map<BigDecimal, String> tree,
                                         String expected) {
        assertEquals(tree.size(), buffered.size());
        for (String key : new String[]{"1.0", "1.00", "1.000"}) {
            BigDecimal decimal = new BigDecimal(key);
            assertEquals(expected, tree.get(decimal));
            assertEquals(tree.get(decimal), buffered.get(decimal));
        }
    }

    @Test public void numericallyEqualKeysSurviveUpdatesFlushAndReopen() throws Exception {
        File file = TmpFiles.tempFile("buffer-decimal", ".wal");
        file.delete();
        try {
            long bufferedRoot;
            long treeRoot;
            try (StoreWAL store = new StoreWAL(file)) {
                BufferTreeMap<BigDecimal, String> buffered = BufferTreeMap.create(store, KEYS, VALUES, 8, 128);
                BTreeMap<BigDecimal, String> tree = BTreeMap.create(store, KEYS, VALUES, 8);
                bufferedRoot = buffered.rootRecidRecid();
                treeRoot = tree.rootRecidRecid();
                assertEquals(tree.put(new BigDecimal("1.0"), "a"), buffered.put(new BigDecimal("1.0"), "a"));
                assertEquals(tree.put(new BigDecimal("1.00"), "b"), buffered.put(new BigDecimal("1.00"), "b"));
                assertEquivalent(buffered, tree, "b");
                buffered.flushAll();
                assertEquivalent(buffered, tree, "b");
                store.commit();
            }
            try (StoreWAL store = new StoreWAL(file)) {
                BufferTreeMap<BigDecimal, String> buffered = BufferTreeMap.open(store, bufferedRoot, KEYS, VALUES, 8, 128);
                BTreeMap<BigDecimal, String> tree = BTreeMap.open(store, treeRoot, KEYS, VALUES, 8);
                assertEquivalent(buffered, tree, "b");
                assertEquals(tree.put(new BigDecimal("1.000"), "c"), buffered.put(new BigDecimal("1.000"), "c"));
                assertEquivalent(buffered, tree, "c");
                assertEquals(tree.remove(new BigDecimal("1.00")), buffered.remove(new BigDecimal("1.00")));
                assertEquivalent(buffered, tree, null);
                assertTrue(buffered.isEmpty());
                buffered.flushAll();
                store.commit();
            }
            try (StoreWAL store = new StoreWAL(file)) {
                assertTrue(BufferTreeMap.open(store, bufferedRoot, KEYS, VALUES, 8, 128).isEmpty());
                assertTrue(BTreeMap.open(store, treeRoot, KEYS, VALUES, 8).isEmpty());
                store.verify();
            }
        } finally {
            TmpFiles.delete(file);
        }
    }
}
