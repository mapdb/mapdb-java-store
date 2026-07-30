package org.mapdb.ser;

import java.lang.reflect.Array;
import java.util.Arrays;

import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

/** Length-framed object-array serializer backed by an element serializer. */
public final class ArraySerializer<A> implements Serializer<A[]> {
    private static final int MAX_ARRAY_LENGTH = 16_000_000;

    private final Class<A> componentType;
    private final Serializer<A> elementSerializer;

    public ArraySerializer(Class<A> componentType, Serializer<A> elementSerializer) {
        if (componentType == null) throw new NullPointerException("componentType");
        if (elementSerializer == null) throw new NullPointerException("elementSerializer");
        this.componentType = componentType;
        this.elementSerializer = elementSerializer;
    }

    public Class<A> componentType() { return componentType; }
    public Serializer<A> elementSerializer() { return elementSerializer; }

    @Override public void serialize(DataOutput2 out, A[] value) {
        if (value.length > MAX_ARRAY_LENGTH)
            throw new IllegalArgumentException("array length exceeds " + MAX_ARRAY_LENGTH);
        out.packInt(value.length);
        for (A element : value) elementSerializer.serialize(out, element);
    }

    @SuppressWarnings("unchecked")
    @Override public A[] deserialize(DataInput2 in, int size) {
        int start = in.pos();
        int length = in.unpackInt();
        if (length < 0 || length > MAX_ARRAY_LENGTH)
            throw new IllegalArgumentException("invalid array length " + length);
        int fixed = elementSerializer.fixedSize();
        if (size >= 0 && fixed > 0) {
            long remaining = (long) size - (in.pos() - start);
            if (remaining < 0 || (long) length * fixed > remaining)
                throw new IllegalArgumentException("array length exceeds record frame");
        }
        A[] value = (A[]) Array.newInstance(componentType, length);
        for (int i = 0; i < value.length; i++) value[i] = elementSerializer.deserialize(in, -1);
        return value;
    }

    @Override public boolean equals(A[] a, A[] b) { return Arrays.equals(a, b); }
    @Override public boolean equalsBySerializedBytes() {
        return elementSerializer.equalsBySerializedBytes();
    }
}
