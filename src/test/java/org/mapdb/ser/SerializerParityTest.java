package org.mapdb.ser;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Test;
import org.mapdb.db.Atomic;
import org.mapdb.db.DB;
import org.mapdb.db.DBMaker;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

/** Round-trip and catalog-reopen coverage for serializers ported from MapDB 3. */
public class SerializerParityTest {

    private static <A> A roundTrip(Serializer<A> serializer, A value) {
        DataOutput2 out = new DataOutput2();
        serializer.serialize(out, value);
        return serializer.deserialize(new DataInput2.ByteArray(out.copyBytes(), 0), out.pos);
    }

    @Test public void scalarAndPackedRoundTrips() {
        assertEquals(Boolean.TRUE, roundTrip(Serializers.BOOLEAN, true));
        assertEquals(Byte.valueOf((byte) -128), roundTrip(Serializers.BYTE, (byte) -128));
        assertEquals(Float.valueOf(-0.0f), roundTrip(Serializers.FLOAT, -0.0f));
        assertEquals(Double.valueOf(Double.NaN), roundTrip(Serializers.DOUBLE, Double.NaN));
        for (int value : new int[]{Integer.MIN_VALUE, -1, 0, 1, 127, 128, Integer.MAX_VALUE})
            assertEquals(Integer.valueOf(value), roundTrip(Serializers.INTEGER_PACKED, value));
        for (long value : new long[]{Long.MIN_VALUE, -1, 0, 1, 127, 128, Long.MAX_VALUE})
            assertEquals(Long.valueOf(value), roundTrip(Serializers.LONG_PACKED, value));
    }

    @Test public void primitiveArrayRoundTrips() {
        assertArrayEquals(new boolean[]{true, false, true, true, false, false, false, true, true},
                roundTrip(Serializers.BOOLEAN_ARRAY,
                        new boolean[]{true, false, true, true, false, false, false, true, true}));
        assertArrayEquals(new char[]{0, 'x', Character.MAX_VALUE},
                roundTrip(Serializers.CHAR_ARRAY, new char[]{0, 'x', Character.MAX_VALUE}));
        assertArrayEquals(new short[]{Short.MIN_VALUE, 0, Short.MAX_VALUE},
                roundTrip(Serializers.SHORT_ARRAY, new short[]{Short.MIN_VALUE, 0, Short.MAX_VALUE}));
        assertArrayEquals(new int[]{Integer.MIN_VALUE, 0, Integer.MAX_VALUE},
                roundTrip(Serializers.INT_ARRAY, new int[]{Integer.MIN_VALUE, 0, Integer.MAX_VALUE}));
        assertArrayEquals(new long[]{Long.MIN_VALUE, 0, Long.MAX_VALUE},
                roundTrip(Serializers.LONG_ARRAY, new long[]{Long.MIN_VALUE, 0, Long.MAX_VALUE}));
        assertArrayEquals(new float[]{-0.0f, 1.5f, Float.NaN},
                roundTrip(Serializers.FLOAT_ARRAY, new float[]{-0.0f, 1.5f, Float.NaN}), 0f);
        assertArrayEquals(new double[]{-0.0d, 1.5d, Double.NaN},
                roundTrip(Serializers.DOUBLE_ARRAY, new double[]{-0.0d, 1.5d, Double.NaN}), 0d);
    }

    @Test public void objectLikeAndNoSizeRoundTrips() {
        assertArrayEquals(new byte[]{1, 2, 3},
                roundTrip(Serializers.BYTE_ARRAY_NOSIZE, new byte[]{1, 2, 3}));
        assertEquals("žluťoučký", roundTrip(Serializers.STRING_NOSIZE, "žluťoučký"));
        assertEquals("plain ASCII", roundTrip(Serializers.STRING_ASCII, "plain ASCII"));
        String interned = roundTrip(Serializers.STRING_INTERN, new String("intern-me"));
        assertTrue(interned == "intern-me");
        assertEquals(Long.valueOf(123456789L), roundTrip(Serializers.RECID, 123456789L));
        assertArrayEquals(new long[]{1L, 2L, Long.MAX_VALUE},
                roundTrip(Serializers.RECID_ARRAY, new long[]{1L, 2L, Long.MAX_VALUE}));
        assertEquals(new BigInteger("-123456789012345678901234567890"),
                roundTrip(Serializers.BIG_INTEGER,
                        new BigInteger("-123456789012345678901234567890")));
        assertEquals(new BigDecimal("-1234567890.0012300"),
                roundTrip(Serializers.BIG_DECIMAL, new BigDecimal("-1234567890.0012300")));
        assertEquals(new Date(123456789L), roundTrip(Serializers.DATE, new Date(123456789L)));
        assertEquals(String.class, roundTrip(Serializers.CLASS, String.class));
        assertEquals(int.class, roundTrip(Serializers.CLASS, int.class));

        ArrayList<String> list = new ArrayList<>(Arrays.asList("a", "b", "c"));
        assertEquals(list, roundTrip(Serializers.JAVA, list));
        ArraySerializer<String> array = new ArraySerializer<>(String.class, Serializers.STRING);
        assertArrayEquals(new String[]{"a", "b"}, roundTrip(array, new String[]{"a", "b"}));
    }

    @Test public void compressionWrapperRoundTripsLargeAndEmptyValues() {
        CompressionSerializer<String> strings = new CompressionSerializer<>(Serializers.STRING);
        String value = "abcdefghij".repeat(10_000);
        assertEquals(value, roundTrip(strings, value));
        CompressionSerializer<byte[]> raw = new CompressionSerializer<>(Serializers.BYTE_ARRAY_NOSIZE);
        assertArrayEquals(new byte[0], roundTrip(raw, new byte[0]));
    }

    @Test public void parameterizedSerializersRejectHostileLengthsBeforeAllocation() {
        DataOutput2 arrayFrame = new DataOutput2();
        arrayFrame.packInt(Integer.MAX_VALUE);
        ArraySerializer<String> array = new ArraySerializer<>(String.class, Serializers.STRING);
        try {
            array.deserialize(new DataInput2.ByteArray(arrayFrame.copyBytes(), 0), arrayFrame.pos);
            throw new AssertionError("hostile array length accepted");
        } catch (IllegalArgumentException expected) { }

        DataOutput2 compressedFrame = new DataOutput2();
        compressedFrame.packInt(Integer.MAX_VALUE);
        compressedFrame.packInt(0);
        try {
            new CompressionSerializer<>(Serializers.STRING).deserialize(
                    new DataInput2.ByteArray(compressedFrame.copyBytes(), 0), compressedFrame.pos);
            throw new AssertionError("hostile decompressed length accepted");
        } catch (IllegalArgumentException expected) { }
    }

    @Test public void registeredSerializersReopenThroughUntypedCatalog() throws Exception {
        File file = File.createTempFile("mapdb-serializer-parity", ".db");
        file.delete();
        try {
            DB db = DBMaker.fileDB(file).transactionEnable().make();
            db.atomicVar("float", Serializers.FLOAT, 1.25f).create();
            db.atomicVar("ints", Serializers.INT_ARRAY, new int[]{1, 2, 3}).create();
            db.atomicVar("decimal", Serializers.BIG_DECIMAL, new BigDecimal("1.2300")).create();
            Map<String, Integer> object = new LinkedHashMap<>();
            object.put("one", 1);
            db.atomicVar("java", Serializers.JAVA, object).create();
            db.commit();
            db.close();

            DB reopened = DBMaker.fileDB(file).transactionEnable().make();
            assertEquals(Float.valueOf(1.25f), ((Atomic.Var<?>) reopened.get("float")).get());
            assertArrayEquals(new int[]{1, 2, 3},
                    (int[]) ((Atomic.Var<?>) reopened.get("ints")).get());
            assertEquals(new BigDecimal("1.2300"), ((Atomic.Var<?>) reopened.get("decimal")).get());
            assertEquals(object, ((Atomic.Var<?>) reopened.get("java")).get());
            assertTrue(reopened.nameCatalogVerifyGetMessages().isEmpty());
            reopened.close();
        } finally {
            file.delete();
            org.mapdb.store.WalTestKit.deleteStore(file);
        }
    }
}
