package org.mapdb.ser;

import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.util.Comparator;

/**
 * Format for a group of values (e.g. the sorted key array of a BTree node).
 * The Store4 evolution of mapdb3's GroupSerializer: the format owns BOTH the
 * representation and the access algorithm.
 *
 * Two dialects:
 *  - object side: operations on an opaque materialized group (long[], Object[], ...)
 *  - byte side:   operations executed directly against serialized bytes, used by
 *    read-actions without materializing the group.
 *
 * The group object is OPAQUE — callers must never cast it (mapdb3 lesson).
 * Mutating ops are copy-on-write: they return a NEW group.
 *
 * No silent fallbacks: if {@link #supportsBinary()} is false,
 * byte-side methods throw and the caller must deserialize explicitly.
 */
public interface GroupFormat<A> {

    Serializer<A> element();

    // ---- object side ----

    Object empty();

    int size(Object group);

    A get(Object group, int pos);

    /** Binary-search semantics: index if found, else -(insertionPoint+1). */
    int search(Object group, A key);

    /**
     * Total order over keys, shared with {@link #search}. Invariant (spec-btree-map
     * item A): {@code compare(a, b) == 0} iff {@link #search}{@code (group, a)} treats
     * {@code a} as the SAME key as {@code b} — i.e. {@code compare}, {@link #search}
     * and {@link #binarySearch} are ONE coherent order. The default delegates to the
     * element serializer's {@link Serializer#compare}; a format whose stored node
     * layout is ordered differently from {@code element().compare} MUST override this
     * together with {@code search}/{@code binarySearch} so they never diverge.
     */
    default int compare(A a, A b) { return element().compare(a, b); }

    /**
     * Comparator matching {@link #compare}, or {@code null} when this format orders by
     * the keys' NATURAL {@link Comparable} ordering — so a map built on it can return
     * {@code null} from {@link java.util.SortedMap#comparator()} (JDK convention for
     * natural order). Derived from the element serializer's {@link Serializer#naturalOrder}:
     * natural-order elements (Long/Int/String) → {@code null}; anything else (e.g. the
     * unsigned {@code byte[]} order of {@link ByteArrayFormat}, or a custom element
     * serializer) → a non-null comparator delegating to {@link #compare}, so the map never
     * falsely advertises natural order. A format whose stored order differs from
     * {@code element().compare} must override this alongside {@code compare}/{@code search}.
     */
    default Comparator<A> comparator() {
        return element().naturalOrder() ? null : this::compare;
    }

    Object insert(Object group, int pos, A newValue);

    Object set(Object group, int pos, A newValue);

    Object delete(Object group, int pos);

    Object copyRange(Object group, int from, int to);

    Object fromArray(Object[] values);

    // ---- wire ----

    /** Writes exactly the group elements; count is stored externally by the caller. */
    void serialize(DataOutput2 out, Object group);

    Object deserialize(DataInput2 in, int size);

    // ---- byte side ----

    default boolean supportsBinary() { return false; }

    /**
     * Search directly in serialized bytes. Input is positioned at the start of the
     * group; on return it MUST be positioned at the end of the group (so callers can
     * continue parsing what follows). Returns binary-search encoding like {@link #search}.
     */
    default int binarySearch(A key, DataInput2 in, int size) {
        throw new UnsupportedOperationException("format does not support binary access: " + getClass().getName());
    }

    /**
     * Extract one element directly from serialized bytes. Input positioned at group
     * start; on return positioned at group end.
     */
    default A binaryGet(DataInput2 in, int size, int pos) {
        throw new UnsupportedOperationException("format does not support binary access: " + getClass().getName());
    }

    // ---- byte side: sequential / range cursor (spec-missing #10) ----

    /**
     * Capability flag for {@link #rangeCursor}. Defaults to {@link #supportsBinary()}: every
     * binary format gets the (correct) default cursor for free, and a non-binary format
     * ({@code supportsBinary()==false}) reports {@code false} so {@code rangeCursor} throws.
     */
    default boolean supportsRangeCursor() { return supportsBinary(); }

    /**
     * Open a forward sequential {@link GroupCursor} over positions {@code [from, to)} of the
     * serialized group, yielding elements in stored (key) order WITHOUT materializing the whole
     * group. {@code in} MUST be positioned at group start; {@code size} is the element count
     * (supplied externally, exactly as for {@link #binarySearch}/{@link #binaryGet}).
     *
     * <p>On exhaustion (after {@link GroupCursor#next()} first returns {@code false}) {@code in}
     * is left at group END — including for an empty group ({@code size == 0}) and an empty range
     * ({@code from == to}). A caller that stops the scan early has no such guarantee (see
     * {@link GroupCursor}).
     *
     * <p>The default is a correctness fallback built on {@link #binaryGet} (it re-seeks to group
     * start per element, so a full scan is O(n · binaryGet)); formats with sequential wire
     * layouts (delta, columnar) override it with a single-pass decode. {@code from}/{@code to}
     * must satisfy {@code 0 <= from <= to <= size}.
     */
    default GroupCursor<A> rangeCursor(DataInput2 in, int size, int from, int to) {
        if (!supportsRangeCursor())
            throw new UnsupportedOperationException("format does not support range cursor: " + getClass().getName());
        if (from < 0 || from > to || to > size)
            throw new IndexOutOfBoundsException("from=" + from + " to=" + to + " size=" + size);
        final int start = in.pos();
        return new GroupCursor<A>() {
            int idx = from - 1;
            A cur;
            boolean exhausted;

            @Override public boolean next() {
                if (exhausted) return false;
                idx++;
                if (idx >= to) {
                    exhausted = true;
                    cur = null;
                    in.pos(start);
                    // snap input to group end (exact for empty groups too)
                    if (size == 0) deserialize(in, 0);       // advances over any empty-group header
                    else binaryGet(in, size, size - 1);      // leaves input at group end
                    return false;
                }
                in.pos(start);
                cur = binaryGet(in, size, idx);              // leaves input at group end; reset on next call
                return true;
            }

            @Override public int index() { return idx; }

            @Override public A value() { return cur; }
        };
    }
}
