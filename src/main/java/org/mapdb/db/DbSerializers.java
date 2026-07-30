package org.mapdb.db;

import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;
import org.mapdb.ser.Serializer;

/**
 * Small element serializers the DB layer needs on top of
 * {@link org.mapdb.ser.Serializers}.
 */
public final class DbSerializers {

    private DbSerializers() {}

    /** UTF-8 string that also encodes {@code null} (one presence byte + packInt-framed bytes). */
    public static final Serializer<String> STRING_NULLABLE = new Serializer<>() {
        @Override public void serialize(DataOutput2 out, String value) {
            if (value == null) { out.writeByte(0); return; }
            out.writeByte(1);
            byte[] b = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            out.packInt(b.length);
            out.write(b);
        }
        @Override public String deserialize(DataInput2 in, int size) {
            if (in.readByte() == 0) return null;
            byte[] b = new byte[in.unpackInt()];
            in.readFully(b);
            return new String(b, java.nio.charset.StandardCharsets.UTF_8);
        }
        @Override public int compare(String a, String b) {
            if (a == null) return b == null ? 0 : -1;
            if (b == null) return 1;
            return a.compareTo(b);
        }
        @Override public boolean equalsBySerializedBytes() { return true; }
    };

    /** Backward-compatible alias for the public built-in codec. */
    public static final Serializer<Boolean> BOOLEAN = org.mapdb.ser.Serializers.BOOLEAN;
}
