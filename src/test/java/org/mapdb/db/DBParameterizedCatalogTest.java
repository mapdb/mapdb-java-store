package org.mapdb.db;

import org.mapdb.TmpFiles;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

import java.io.File;

import org.junit.Test;
import org.mapdb.btree.BTreeMap;
import org.mapdb.ser.ArraySerializer;
import org.mapdb.ser.CompressionSerializer;
import org.mapdb.ser.ColumnarValueFormat;
import org.mapdb.ser.LongFormat;
import org.mapdb.ser.ObjectArrayFormat;
import org.mapdb.ser.Serializers;
import org.mapdb.ser.StringGroupFormat;
import org.mapdb.ser.TupleComponent;
import org.mapdb.ser.TupleFormat;

/** Automatic catalog reconstruction for supported parameterized codecs. */
public class DBParameterizedCatalogTest {
    private static volatile boolean componentInitialized;
    public static final class ComponentWithInitializer {
        static { componentInitialized = true; }
    }

    @Test public void parameterizedCodecsReopenWithoutResupply() throws Exception {
        File file = TmpFiles.tempFile("mapdb-parameterized", ".db");
        file.delete();
        try {
            DB db = DBMaker.fileDB(file).transactionEnable().make();

            TupleFormat tuple = TupleFormat.of(
                    TupleComponent.STRING, TupleComponent.LONG, TupleComponent.INT);
            BTreeMap<Object[], String> tuples = db
                    .treeMap("tuples", tuple, StringGroupFormat.INSTANCE).create();
            tuples.put(new Object[]{"tenant", 7L, 1}, "one");
            tuples.put(new Object[]{"tenant", 7L, 2}, "two");

            CompressionSerializer<String> compressed =
                    new CompressionSerializer<>(Serializers.STRING, 6);
            BTreeMap<Long, String> compressedValues = db
                    .treeMap("compressed", LongFormat.INSTANCE,
                            new ObjectArrayFormat<>(compressed)).create();
            compressedValues.put(1L, "abcdefghij".repeat(1000));

            BTreeMap<Long, Object[]> columnar = db.treeMap("columnar", LongFormat.INSTANCE,
                    ColumnarValueFormat.of(ColumnarValueFormat.ColumnType.LONG,
                            ColumnarValueFormat.ColumnType.INT)).create();
            columnar.put(1L, new Object[]{9L, 4});

            ArraySerializer<String> stringArrays =
                    new ArraySerializer<>(String.class, Serializers.STRING);
            db.atomicVar("array", stringArrays, new String[]{"a", "b"}).create();
            db.atomicVar("compressedVar", compressed, "payload".repeat(100)).create();
            db.commit();
            db.close();

            DB reopened = DBMaker.fileDB(file).transactionEnable().make();
            @SuppressWarnings("unchecked")
            BTreeMap<Object[], String> tuples2 = (BTreeMap<Object[], String>) reopened.get("tuples");
            assertEquals("two", tuples2.get(new Object[]{"tenant", 7L, 2}));
            @SuppressWarnings("unchecked")
            BTreeMap<Long, String> compressed2 =
                    (BTreeMap<Long, String>) reopened.get("compressed");
            assertEquals("abcdefghij".repeat(1000), compressed2.get(1L));
            @SuppressWarnings("unchecked")
            BTreeMap<Long, Object[]> columnar2 =
                    (BTreeMap<Long, Object[]>) reopened.get("columnar");
            assertArrayEquals(new Object[]{9L, 4}, columnar2.get(1L));
            assertArrayEquals(new String[]{"a", "b"},
                    (String[]) ((Atomic.Var<?>) reopened.get("array")).get());
            assertEquals("payload".repeat(100),
                    ((Atomic.Var<?>) reopened.get("compressedVar")).get());
            assertTrue(reopened.nameCatalogVerifyGetMessages().isEmpty());
            reopened.close();
        } finally {
            file.delete();
            org.mapdb.store.WalTestKit.deleteStore(file);
        }
    }

    @Test public void malformedDescriptorsAreRejectedWithoutClassInitialization() {
        assertNull(SerializerRegistry.groupFormatById("OBJECT_ARRAY:%%%"));
        String component = java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(
                ComponentWithInitializer.class.getName().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        String nested = java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(
                "STRING".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        assertFalse(componentInitialized);
        assertTrue(SerializerRegistry.serializerById("ARRAY:" + component + ":" + nested)
                instanceof ArraySerializer);
        assertFalse("descriptor resolution initialized the component class", componentInitialized);
    }
}
