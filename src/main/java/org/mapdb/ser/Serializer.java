package org.mapdb.ser;

import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.util.Objects;

/**
 * Element codec. Store-facing: record content is produced/consumed through this.
 * Also provides ordering and logical equality (used by compareAndSwap).
 */
public interface Serializer<A> {

    void serialize(DataOutput2 out, A value);

    /**
     * @param size total bytes available for this value, or -1 when the value is
     *             framed inside a larger stream and must self-delimit.
     */
    A deserialize(DataInput2 in, int size);

    /** Fixed serialized size in bytes, or -1 if variable. */
    default int fixedSize() { return -1; }

    /** Hint for output buffer sizing; -1 = unknown. */
    default int sizeHint() { return fixedSize(); }

    @SuppressWarnings({"unchecked", "rawtypes"})
    default int compare(A a, A b) { return ((Comparable) a).compareTo(b); }

    default boolean equals(A a, A b) { return Objects.equals(a, b); }

    /**
     * True iff {@link #compare} is exactly the key type's natural {@link Comparable}
     * ordering — so a {@link java.util.SortedMap} keyed by this serializer may report a
     * {@code null} comparator (JDK convention). Default {@code false}: a serializer whose
     * {@code compare} is natural (Long/Integer/String) overrides to {@code true}; one with
     * a custom order (unsigned {@code byte[]}, reversed, case-insensitive) leaves it false
     * so the map exposes a non-null comparator instead of falsely claiming natural order.
     */
    default boolean naturalOrder() { return false; }

    /**
     * True iff serialization is CANONICAL: two values are equal (per {@link #equals})
     * exactly when their serialized forms are byte-identical. Lets readers test
     * equality by comparing serialized bytes in place — no deserialization
     * (BufferTreeMap op-tail scan). Declare true only when both
     * directions hold over the serializer's ROUND-TRIPPABLE DOMAIN: equal values
     * always encode to the same bytes AND distinct values never collide. Values a
     * serializer already stores lossily (e.g. ill-formed strings under
     * {@link Serializers#STRING}) are outside that domain: they conflate on write
     * regardless, and byte equality simply observes the stored identity.
     */
    default boolean equalsBySerializedBytes() { return false; }
}
