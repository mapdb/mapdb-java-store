package org.mapdb.ser;

/**
 * Forward byte-side cursor over a serialized {@link GroupFormat} group, yielding elements in
 * stored (key) order WITHOUT materializing the whole group. This is the "one real interface
 * gap" for scan-heavy / columnar workloads (spec-missing #10, roadmap R7): a caller that only
 * wants to walk a value range — or one column, via
 * {@link ColumnarValueFormat#columnCursor} — reads bytes once, forward, instead of calling
 * {@link GroupFormat#binaryGet} per position (which for delta/prefix/columnar layouts re-decodes
 * from the group start each time, i.e. O(n²) for a full scan).
 *
 * <p>Obtained from {@link GroupFormat#rangeCursor}. Typical use:
 * <pre>{@code
 *   GroupCursor<A> c = format.rangeCursor(in, size, from, to);
 *   while (c.next()) { consume(c.index(), c.value()); }
 *   // c is now exhausted: `in` is positioned at the group's end
 * }</pre>
 *
 * <p><b>Positioning contract.</b> The backing {@link org.mapdb.io.DataInput2} is guaranteed to
 * be left at group END only after the cursor is EXHAUSTED (after {@link #next()} first returns
 * {@code false}) — including for an empty group and an empty range. A caller that abandons the
 * scan early must NOT assume the input is positioned for parsing following fields.
 */
public interface GroupCursor<A> {

    /** Advance to the next element in range; {@code false} once the range is exhausted. */
    boolean next();

    /** 0-based absolute index of the current element within the group (valid after {@code next()==true}). */
    int index();

    /** Decode and return the current element (valid after {@code next()==true}). */
    A value();
}
