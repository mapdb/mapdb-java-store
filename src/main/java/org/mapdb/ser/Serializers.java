package org.mapdb.ser;

import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.nio.charset.StandardCharsets;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.UUID;

/** Built-in element serializers. */
public final class Serializers {

    private Serializers() {}

    public static final Serializer<Short> SHORT = new Serializer<>() {
        @Override public void serialize(DataOutput2 out, Short value) {
            short v = value;
            out.writeByte(v >>> 8);
            out.writeByte(v);
        }
        @Override public Short deserialize(DataInput2 in, int size) {
            int hi = in.readUnsignedByte();
            int lo = in.readUnsignedByte();
            return (short) ((hi << 8) | lo);
        }
        @Override public int fixedSize() { return 2; }
        @Override public int compare(Short a, Short b) { return Short.compare(a, b); }
        @Override public boolean equalsBySerializedBytes() { return true; }
        @Override public boolean naturalOrder() { return true; }
    };

    /**
     * Unsigned 16-bit {@code char}. Its natural order ({@link Character#compare}) is
     * plain unsigned 0..65535 and equals the big-endian 2-byte wire order directly — no
     * sign flip, UNLIKE {@link #SHORT}.
     */
    public static final Serializer<Character> CHAR = new Serializer<>() {
        @Override public void serialize(DataOutput2 out, Character value) {
            char v = value;
            out.writeByte(v >>> 8);
            out.writeByte(v);
        }
        @Override public Character deserialize(DataInput2 in, int size) {
            int hi = in.readUnsignedByte();
            int lo = in.readUnsignedByte();
            return (char) ((hi << 8) | lo);
        }
        @Override public int fixedSize() { return 2; }
        @Override public int compare(Character a, Character b) { return Character.compare(a, b); }
        @Override public boolean equalsBySerializedBytes() { return true; }
        @Override public boolean naturalOrder() { return true; }
    };

    /**
     * 16-byte {@link UUID}: {@code mostSigBits} then {@code leastSigBits}, each a
     * big-endian signed long. Order is {@link UUID#compareTo}, i.e. SIGNED comparison on
     * msb then lsb (not unsigned/lexicographic byte order); the encoding preserves it, so
     * byte-side code compares stored halves as signed longs in place.
     */
    public static final Serializer<UUID> UUID = new Serializer<>() {
        @Override public void serialize(DataOutput2 out, UUID value) {
            out.writeLong(value.getMostSignificantBits());
            out.writeLong(value.getLeastSignificantBits());
        }
        @Override public UUID deserialize(DataInput2 in, int size) {
            long msb = in.readLong();
            long lsb = in.readLong();
            return new UUID(msb, lsb);
        }
        @Override public int fixedSize() { return 16; }
        @Override public int compare(UUID a, UUID b) { return a.compareTo(b); }
        @Override public boolean equalsBySerializedBytes() { return true; }
        @Override public boolean naturalOrder() { return true; }
    };

    public static final Serializer<Long> LONG = new Serializer<>() {
        @Override public void serialize(DataOutput2 out, Long value) { out.writeLong(value); }
        @Override public Long deserialize(DataInput2 in, int size) { return in.readLong(); }
        @Override public int fixedSize() { return 8; }
        @Override public int compare(Long a, Long b) { return Long.compare(a, b); }
        @Override public boolean equalsBySerializedBytes() { return true; }
        @Override public boolean naturalOrder() { return true; }
    };

    public static final Serializer<Integer> INTEGER = new Serializer<>() {
        @Override public void serialize(DataOutput2 out, Integer value) { out.writeInt(value); }
        @Override public Integer deserialize(DataInput2 in, int size) { return in.readInt(); }
        @Override public int fixedSize() { return 4; }
        @Override public int compare(Integer a, Integer b) { return Integer.compare(a, b); }
        @Override public boolean equalsBySerializedBytes() { return true; }
        @Override public boolean naturalOrder() { return true; }
    };

    /**
     * UTF-8 with packInt length framing. Domain: WELL-FORMED strings (no unpaired
     * surrogates). Java's UTF-8 encoder replaces ill-formed UTF-16 with {@code '?'},
     * so such strings do not round-trip ("\uD800" stores as "?") — a lossiness
     * inherent to this encoding, predating and independent of
     * {@link #equalsBySerializedBytes()}, which is declared on the same domain:
     * within well-formed strings UTF-8 is a bijection, so byte equality is exactly
     * value equality. Callers needing arbitrary ill-formed strings need a
     * UTF-16-code-unit-injective serializer instead.
     */
    public static final Serializer<String> STRING = new Serializer<>() {
        @Override public void serialize(DataOutput2 out, String value) {
            byte[] b = value.getBytes(StandardCharsets.UTF_8);
            out.packInt(b.length);
            out.write(b);
        }

        @Override public String deserialize(DataInput2 in, int size) {
            int len = in.unpackInt();
            byte[] b = new byte[len];
            in.readFully(b);
            return new String(b, StandardCharsets.UTF_8);
        }

        @Override public int compare(String a, String b) { return a.compareTo(b); }
        @Override public boolean equalsBySerializedBytes() { return true; } // over well-formed strings; see class doc
        @Override public boolean naturalOrder() { return true; }
    };

    public static final Serializer<byte[]> BYTE_ARRAY = new Serializer<>() {
        @Override public void serialize(DataOutput2 out, byte[] value) {
            out.packInt(value.length);
            out.write(value);
        }

        @Override public byte[] deserialize(DataInput2 in, int size) {
            byte[] b = new byte[in.unpackInt()];
            in.readFully(b);
            return b;
        }

        @Override public int compare(byte[] a, byte[] b) { return java.util.Arrays.compare(a, b); }
        @Override public boolean equals(byte[] a, byte[] b) { return java.util.Arrays.equals(a, b); }
        @Override public boolean equalsBySerializedBytes() { return true; }
    };

    /**
     * {@link #BYTE_ARRAY} with UNSIGNED lexicographic order ({@code memcmp} /
     * {@link java.util.Arrays#compareUnsigned(byte[], byte[])}) — the sane storage
     * order: it equals the byte order of the serialized form, so byte-side code can
     * compare stored bytes in place without sign-adjusting each one. Same wire format
     * and equality as BYTE_ARRAY; ONLY the order differs. {@link ByteArrayFormat}
     * uses this as its element rather than silently changing BYTE_ARRAY's
     * (signed {@code Arrays.compare}) order under existing maps.
     */
    public static final Serializer<byte[]> BYTE_ARRAY_UNSIGNED = new Serializer<>() {
        @Override public void serialize(DataOutput2 out, byte[] value) {
            BYTE_ARRAY.serialize(out, value);
        }

        @Override public byte[] deserialize(DataInput2 in, int size) {
            return BYTE_ARRAY.deserialize(in, size);
        }

        @Override public int compare(byte[] a, byte[] b) { return java.util.Arrays.compareUnsigned(a, b); }
        @Override public boolean equals(byte[] a, byte[] b) { return java.util.Arrays.equals(a, b); }
        @Override public boolean equalsBySerializedBytes() { return true; }
    };

    public static final Serializer<Boolean> BOOLEAN = new Serializer<>() {
        @Override public void serialize(DataOutput2 out, Boolean value) { out.writeByte(value ? 1 : 0); }
        @Override public Boolean deserialize(DataInput2 in, int size) {
            int value = in.readUnsignedByte();
            if (value > 1) throw new IllegalArgumentException("invalid boolean byte " + value);
            return value != 0;
        }
        @Override public int fixedSize() { return 1; }
        @Override public int compare(Boolean a, Boolean b) { return Boolean.compare(a, b); }
        @Override public boolean naturalOrder() { return true; }
        @Override public boolean equalsBySerializedBytes() { return true; }
    };

    public static final Serializer<Byte> BYTE = new Serializer<>() {
        @Override public void serialize(DataOutput2 out, Byte value) { out.writeByte(value); }
        @Override public Byte deserialize(DataInput2 in, int size) { return in.readByte(); }
        @Override public int fixedSize() { return 1; }
        @Override public int compare(Byte a, Byte b) { return Byte.compare(a, b); }
        @Override public boolean naturalOrder() { return true; }
        @Override public boolean equalsBySerializedBytes() { return true; }
    };

    public static final Serializer<Float> FLOAT = new Serializer<>() {
        @Override public void serialize(DataOutput2 out, Float value) {
            out.writeInt(Float.floatToIntBits(value));
        }
        @Override public Float deserialize(DataInput2 in, int size) {
            return Float.intBitsToFloat(in.readInt());
        }
        @Override public int fixedSize() { return 4; }
        @Override public int compare(Float a, Float b) { return Float.compare(a, b); }
        @Override public boolean naturalOrder() { return true; }
        @Override public boolean equalsBySerializedBytes() { return true; }
    };

    public static final Serializer<Double> DOUBLE = new Serializer<>() {
        @Override public void serialize(DataOutput2 out, Double value) {
            out.writeLong(Double.doubleToLongBits(value));
        }
        @Override public Double deserialize(DataInput2 in, int size) {
            return Double.longBitsToDouble(in.readLong());
        }
        @Override public int fixedSize() { return 8; }
        @Override public int compare(Double a, Double b) { return Double.compare(a, b); }
        @Override public boolean naturalOrder() { return true; }
        @Override public boolean equalsBySerializedBytes() { return true; }
    };

    /** Packed two's-complement integer; negative values use five bytes. */
    public static final Serializer<Integer> INTEGER_PACKED = new Serializer<>() {
        @Override public void serialize(DataOutput2 out, Integer value) { out.packInt(value); }
        @Override public Integer deserialize(DataInput2 in, int size) { return in.unpackInt(); }
        @Override public int compare(Integer a, Integer b) { return Integer.compare(a, b); }
        @Override public boolean naturalOrder() { return true; }
        @Override public boolean equalsBySerializedBytes() { return true; }
    };

    /** Packed two's-complement long; negative values use ten bytes. */
    public static final Serializer<Long> LONG_PACKED = new Serializer<>() {
        @Override public void serialize(DataOutput2 out, Long value) { out.packLong(value); }
        @Override public Long deserialize(DataInput2 in, int size) { return in.unpackLong(); }
        @Override public int compare(Long a, Long b) { return Long.compare(a, b); }
        @Override public boolean naturalOrder() { return true; }
        @Override public boolean equalsBySerializedBytes() { return true; }
    };

    /** Raw record bytes with no inner length prefix; requires a non-negative available size. */
    public static final Serializer<byte[]> BYTE_ARRAY_NOSIZE = new Serializer<>() {
        @Override public void serialize(DataOutput2 out, byte[] value) { out.write(value); }
        @Override public byte[] deserialize(DataInput2 in, int size) {
            if (size < 0) throw new IllegalArgumentException("BYTE_ARRAY_NOSIZE requires record size");
            byte[] value = new byte[size];
            in.readFully(value);
            return value;
        }
        @Override public boolean equals(byte[] a, byte[] b) { return java.util.Arrays.equals(a, b); }
        @Override public int compare(byte[] a, byte[] b) { return java.util.Arrays.compare(a, b); }
        @Override public boolean equalsBySerializedBytes() { return true; }
    };

    /** UTF-8 occupying the entire record, without an inner length prefix. */
    public static final Serializer<String> STRING_NOSIZE = new Serializer<>() {
        @Override public void serialize(DataOutput2 out, String value) {
            out.write(value.getBytes(StandardCharsets.UTF_8));
        }
        @Override public String deserialize(DataInput2 in, int size) {
            if (size < 0) throw new IllegalArgumentException("STRING_NOSIZE requires record size");
            byte[] value = new byte[size];
            in.readFully(value);
            return new String(value, StandardCharsets.UTF_8);
        }
        @Override public int compare(String a, String b) { return a.compareTo(b); }
        @Override public boolean naturalOrder() { return true; }
        @Override public boolean equalsBySerializedBytes() { return true; }
    };

    /** Seven-bit ASCII string; rejects characters outside U+0000..U+007F. */
    public static final Serializer<String> STRING_ASCII = new Serializer<>() {
        @Override public void serialize(DataOutput2 out, String value) {
            out.packInt(value.length());
            for (int i = 0; i < value.length(); i++) {
                char c = value.charAt(i);
                if (c > 0x7F) throw new IllegalArgumentException("non-ASCII character at " + i);
                out.writeByte(c);
            }
        }
        @Override public String deserialize(DataInput2 in, int size) {
            int length = in.unpackInt();
            char[] chars = new char[length];
            for (int i = 0; i < length; i++) chars[i] = (char) in.readUnsignedByte();
            return new String(chars);
        }
        @Override public int compare(String a, String b) { return a.compareTo(b); }
        @Override public boolean naturalOrder() { return true; }
        @Override public boolean equalsBySerializedBytes() { return true; }
    };

    /** Standard UTF-8 string codec whose deserialized values are interned. */
    public static final Serializer<String> STRING_INTERN = new Serializer<>() {
        @Override public void serialize(DataOutput2 out, String value) { STRING.serialize(out, value); }
        @Override public String deserialize(DataInput2 in, int size) {
            return STRING.deserialize(in, size).intern();
        }
        @Override public int compare(String a, String b) { return a.compareTo(b); }
        @Override public boolean naturalOrder() { return true; }
        @Override public boolean equalsBySerializedBytes() { return true; }
    };

    /** Positive record identifier encoded as a packed long. */
    public static final Serializer<Long> RECID = new Serializer<>() {
        @Override public void serialize(DataOutput2 out, Long value) {
            if (value <= 0) throw new IllegalArgumentException("recid must be positive");
            out.packLong(value);
        }
        @Override public Long deserialize(DataInput2 in, int size) {
            long value = in.unpackLong();
            if (value <= 0) throw new IllegalArgumentException("invalid recid " + value);
            return value;
        }
        @Override public int compare(Long a, Long b) { return Long.compare(a, b); }
        @Override public boolean naturalOrder() { return true; }
        @Override public boolean equalsBySerializedBytes() { return true; }
    };

    public static final Serializer<long[]> RECID_ARRAY = new Serializer<>() {
        @Override public void serialize(DataOutput2 out, long[] value) {
            out.packInt(value.length);
            for (long recid : value) RECID.serialize(out, recid);
        }
        @Override public long[] deserialize(DataInput2 in, int size) {
            long[] value = new long[in.unpackInt()];
            for (int i = 0; i < value.length; i++) value[i] = RECID.deserialize(in, -1);
            return value;
        }
        @Override public boolean equals(long[] a, long[] b) { return java.util.Arrays.equals(a, b); }
        @Override public boolean equalsBySerializedBytes() { return true; }
    };

    public static final Serializer<char[]> CHAR_ARRAY = new Serializer<>() {
        @Override public void serialize(DataOutput2 out, char[] value) {
            out.packInt(value.length);
            for (char v : value) { out.writeByte(v >>> 8); out.writeByte(v); }
        }
        @Override public char[] deserialize(DataInput2 in, int size) {
            char[] value = new char[in.unpackInt()];
            for (int i = 0; i < value.length; i++)
                value[i] = (char) ((in.readUnsignedByte() << 8) | in.readUnsignedByte());
            return value;
        }
        @Override public boolean equals(char[] a, char[] b) { return java.util.Arrays.equals(a, b); }
        @Override public boolean equalsBySerializedBytes() { return true; }
    };

    public static final Serializer<short[]> SHORT_ARRAY = new Serializer<>() {
        @Override public void serialize(DataOutput2 out, short[] value) {
            out.packInt(value.length);
            for (short v : value) { out.writeByte(v >>> 8); out.writeByte(v); }
        }
        @Override public short[] deserialize(DataInput2 in, int size) {
            short[] value = new short[in.unpackInt()];
            for (int i = 0; i < value.length; i++)
                value[i] = (short) ((in.readUnsignedByte() << 8) | in.readUnsignedByte());
            return value;
        }
        @Override public boolean equals(short[] a, short[] b) { return java.util.Arrays.equals(a, b); }
        @Override public boolean equalsBySerializedBytes() { return true; }
    };

    public static final Serializer<int[]> INT_ARRAY = new Serializer<>() {
        @Override public void serialize(DataOutput2 out, int[] value) {
            out.packInt(value.length);
            for (int v : value) out.writeInt(v);
        }
        @Override public int[] deserialize(DataInput2 in, int size) {
            int[] value = new int[in.unpackInt()];
            for (int i = 0; i < value.length; i++) value[i] = in.readInt();
            return value;
        }
        @Override public boolean equals(int[] a, int[] b) { return java.util.Arrays.equals(a, b); }
        @Override public boolean equalsBySerializedBytes() { return true; }
    };

    public static final Serializer<long[]> LONG_ARRAY = new Serializer<>() {
        @Override public void serialize(DataOutput2 out, long[] value) {
            out.packInt(value.length);
            for (long v : value) out.writeLong(v);
        }
        @Override public long[] deserialize(DataInput2 in, int size) {
            long[] value = new long[in.unpackInt()];
            for (int i = 0; i < value.length; i++) value[i] = in.readLong();
            return value;
        }
        @Override public boolean equals(long[] a, long[] b) { return java.util.Arrays.equals(a, b); }
        @Override public boolean equalsBySerializedBytes() { return true; }
    };

    public static final Serializer<float[]> FLOAT_ARRAY = new Serializer<>() {
        @Override public void serialize(DataOutput2 out, float[] value) {
            out.packInt(value.length);
            for (float v : value) out.writeInt(Float.floatToIntBits(v));
        }
        @Override public float[] deserialize(DataInput2 in, int size) {
            float[] value = new float[in.unpackInt()];
            for (int i = 0; i < value.length; i++) value[i] = Float.intBitsToFloat(in.readInt());
            return value;
        }
        @Override public boolean equals(float[] a, float[] b) { return java.util.Arrays.equals(a, b); }
        @Override public boolean equalsBySerializedBytes() { return true; }
    };

    public static final Serializer<double[]> DOUBLE_ARRAY = new Serializer<>() {
        @Override public void serialize(DataOutput2 out, double[] value) {
            out.packInt(value.length);
            for (double v : value) out.writeLong(Double.doubleToLongBits(v));
        }
        @Override public double[] deserialize(DataInput2 in, int size) {
            double[] value = new double[in.unpackInt()];
            for (int i = 0; i < value.length; i++) value[i] = Double.longBitsToDouble(in.readLong());
            return value;
        }
        @Override public boolean equals(double[] a, double[] b) { return java.util.Arrays.equals(a, b); }
        @Override public boolean equalsBySerializedBytes() { return true; }
    };

    public static final Serializer<boolean[]> BOOLEAN_ARRAY = new Serializer<>() {
        @Override public void serialize(DataOutput2 out, boolean[] value) {
            out.packInt(value.length);
            for (int offset = 0; offset < value.length; offset += 8) {
                int bits = 0;
                for (int bit = 0; bit < 8 && offset + bit < value.length; bit++)
                    if (value[offset + bit]) bits |= 1 << bit;
                out.writeByte(bits);
            }
        }
        @Override public boolean[] deserialize(DataInput2 in, int size) {
            boolean[] value = new boolean[in.unpackInt()];
            for (int offset = 0; offset < value.length; offset += 8) {
                int bits = in.readUnsignedByte();
                for (int bit = 0; bit < 8 && offset + bit < value.length; bit++)
                    value[offset + bit] = (bits & (1 << bit)) != 0;
            }
            return value;
        }
        @Override public boolean equals(boolean[] a, boolean[] b) { return java.util.Arrays.equals(a, b); }
        @Override public boolean equalsBySerializedBytes() { return true; }
    };

    public static final Serializer<BigInteger> BIG_INTEGER = new Serializer<>() {
        @Override public void serialize(DataOutput2 out, BigInteger value) {
            BYTE_ARRAY.serialize(out, value.toByteArray());
        }
        @Override public BigInteger deserialize(DataInput2 in, int size) {
            return new BigInteger(BYTE_ARRAY.deserialize(in, -1));
        }
        @Override public int compare(BigInteger a, BigInteger b) { return a.compareTo(b); }
        @Override public boolean naturalOrder() { return true; }
        @Override public boolean equalsBySerializedBytes() { return true; }
    };

    public static final Serializer<BigDecimal> BIG_DECIMAL = new Serializer<>() {
        @Override public void serialize(DataOutput2 out, BigDecimal value) {
            BYTE_ARRAY.serialize(out, value.unscaledValue().toByteArray());
            out.packInt(value.scale());
        }
        @Override public BigDecimal deserialize(DataInput2 in, int size) {
            return new BigDecimal(new BigInteger(BYTE_ARRAY.deserialize(in, -1)), in.unpackInt());
        }
        @Override public int compare(BigDecimal a, BigDecimal b) { return a.compareTo(b); }
        @Override public boolean naturalOrder() { return true; }
        @Override public boolean equalsBySerializedBytes() { return true; }
    };

    public static final Serializer<Date> DATE = new Serializer<>() {
        @Override public void serialize(DataOutput2 out, Date value) { out.writeLong(value.getTime()); }
        @Override public Date deserialize(DataInput2 in, int size) { return new Date(in.readLong()); }
        @Override public int fixedSize() { return 8; }
        @Override public int compare(Date a, Date b) { return a.compareTo(b); }
        @Override public boolean naturalOrder() { return true; }
        @Override public boolean equalsBySerializedBytes() { return true; }
    };

    public static final Serializer<Class<?>> CLASS = new Serializer<>() {
        @Override public void serialize(DataOutput2 out, Class<?> value) {
            STRING.serialize(out, value.getName());
        }
        @Override public Class<?> deserialize(DataInput2 in, int size) {
            String name = STRING.deserialize(in, -1);
            switch (name) {
                case "boolean": return boolean.class;
                case "byte": return byte.class;
                case "char": return char.class;
                case "short": return short.class;
                case "int": return int.class;
                case "long": return long.class;
                case "float": return float.class;
                case "double": return double.class;
                case "void": return void.class;
                default: break;
            }
            try {
                return Class.forName(name, false, Thread.currentThread().getContextClassLoader());
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("class not found: " + name, e);
            }
        }
        @Override public int compare(Class<?> a, Class<?> b) { return a.getName().compareTo(b.getName()); }
    };

    /**
     * Java ObjectStream codec, length-framed so it is safe inside larger records.
     * Deserialize only trusted database files: ObjectInputStream may instantiate
     * application classes with unsafe deserialization behavior.
     */
    public static final Serializer<Object> JAVA = new Serializer<>() {
        @Override public void serialize(DataOutput2 out, Object value) {
            try {
                java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
                try (java.io.ObjectOutputStream objects = new java.io.ObjectOutputStream(bytes)) {
                    objects.writeObject(value);
                }
                byte[] data = bytes.toByteArray();
                out.packInt(data.length);
                out.write(data);
            } catch (java.io.IOException e) {
                throw new IllegalArgumentException("Java serialization failed", e);
            }
        }
        @Override public Object deserialize(DataInput2 in, int size) {
            byte[] data = new byte[in.unpackInt()];
            in.readFully(data);
            try (java.io.ObjectInputStream objects = new java.io.ObjectInputStream(
                    new java.io.ByteArrayInputStream(data))) {
                return objects.readObject();
            } catch (java.io.IOException | ClassNotFoundException e) {
                throw new IllegalArgumentException("Java deserialization failed", e);
            }
        }
    };
}
