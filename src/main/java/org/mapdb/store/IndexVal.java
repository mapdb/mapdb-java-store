package org.mapdb.store;

/**
 * Capacity-based index-value encoding:
 *
 * <pre>
 * bit 63..48  capacityUnits (16 bits) — capacity = capacityUnits * 16 bytes
 * bit 47..4   offset (44 bits, 16-aligned)
 * bit 3       linked   (oversize records stored as chunk chains)
 * bit 2       prealloc (P state)
 * bit 1       archive  (reserved)
 * bit 0       parity   (parity1 over the whole slot once stored on-volume)
 * </pre>
 *
 * Record data layout at offset: 4-byte used-length header, then content.
 * Isolated in this class so alternative layouts (e.g. mapdb3-classic 16-bit size
 * field with small-record inlining) can ship as sibling StoreDirect variants
 * behind the same TCK.
 */
final class IndexVal {

    static final long MOFFSET = 0x0000FFFFFFFFFFF0L;

    static final int FLAG_LINKED = 8;
    static final int FLAG_PREALLOC = 4;
    static final int FLAG_ARCHIVE = 2;

    /** capacityUnits sentinel: record content is null (P state iff FLAG_PREALLOC set). */
    static final int CAP_NULL = 0xFFFF;
    /** capacityUnits sentinel: recid deleted (tombstone). */
    static final int CAP_DELETED = 0xFFFE;
    static final int CAP_MAX_UNITS = 0xFFFD;
    /** Max plain-record capacity incl. 4-byte header: ~1 MiB - 48. */
    static final int MAX_CAPACITY = CAP_MAX_UNITS * 16;

    private IndexVal() {}

    static long compose(int capUnits, long offset, int flags) {
        assert (offset & ~MOFFSET) == 0 : "offset not 16-aligned or out of range: " + offset;
        return ((long) capUnits << 48) | offset | flags;
    }

    static int capUnits(long iv) { return (int) (iv >>> 48); }

    static long offset(long iv) { return iv & MOFFSET; }

    static boolean isPrealloc(long iv) { return (iv & FLAG_PREALLOC) != 0; }

    static boolean isLinked(long iv) { return (iv & FLAG_LINKED) != 0; }

    static int roundUp16(int n) { return (n + 15) & ~15; }
}
