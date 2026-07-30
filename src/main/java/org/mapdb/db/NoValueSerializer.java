package org.mapdb.db;

import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;
import org.mapdb.ser.Serializer;

/**
 * A zero-byte value serializer used to back Sets on top of the Map primitives
 * (a {@code Set<E>} is a {@code Map<E, NONE>} whose value column costs nothing).
 *
 * <p>{@link #serialize} writes nothing; {@link #deserialize} consumes nothing and
 * returns the shared {@link #NONE} singleton, regardless of the {@code size} hint.
 * Wrapped in an {@link org.mapdb.ser.ObjectArrayFormat} it also serves as a
 * no-value {@link org.mapdb.ser.GroupFormat} for BTreeMap-backed sorted sets.
 */
public final class NoValueSerializer implements Serializer<Object> {

    /** The single logical value every set entry maps to. */
    public static final Object NONE = new Object() {
        @Override public String toString() { return "NONE"; }
    };

    public static final NoValueSerializer INSTANCE = new NoValueSerializer();

    private NoValueSerializer() {}

    @Override public void serialize(DataOutput2 out, Object value) { /* zero bytes */ }

    @Override public Object deserialize(DataInput2 in, int size) { return NONE; }

    @Override public int fixedSize() { return 0; }

    @Override public int compare(Object a, Object b) { return 0; }

    @Override public boolean equals(Object a, Object b) { return true; }

    @Override public boolean equalsBySerializedBytes() { return true; }
}
