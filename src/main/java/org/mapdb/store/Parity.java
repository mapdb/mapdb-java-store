package org.mapdb.store;

import org.mapdb.DBException;

/**
 * Bit-parity encodings for on-volume pointers/counters (mapdb3 DataIO lineage,
 * simplified). The low N bits of the stored long carry a checksum of the payload
 * bits, sized to whatever alignment frees them: parity1 for 2-aligned payloads
 * (index values, recids shifted left by one, offsets shifted right by three),
 * parity4 for 16-aligned ones (dataTail, long-stack links), parity16 for page
 * pointers (1 MB aligned).
 *
 * A raw stored value of 0 always FAILS its parity check, so "never written /
 * lost update" is distinguishable from every legitimately stored value —
 * including the encoded 0 used for empty links ({@code pNset(0) == 1}).
 */
final class Parity {

    private Parity() {}

    /** {@code v} must have bit 0 clear; the result has an odd total bit count. */
    static long p1set(long v) {
        assert (v & 1) == 0 : "parity1 payload uses bit 0";
        return v | ((Long.bitCount(v) + 1) & 1);
    }

    /** Validates and strips parity1; throws {@link DBException.DataCorruption} when broken. */
    static long p1get(long v) {
        if ((Long.bitCount(v) & 1) != 1)
            throw new DBException.DataCorruption("parity1 broken: 0x" + Long.toHexString(v));
        return v & ~1L;
    }

    /** {@code v} must have the low 4 bits clear. */
    static long p4set(long v) {
        assert (v & 0xF) == 0 : "parity4 payload uses low 4 bits";
        return v | ((Long.bitCount(v) + 1) & 0xF);
    }

    static long p4get(long v) {
        long x = v & ~0xFL;
        if ((v & 0xF) != ((Long.bitCount(x) + 1) & 0xF))
            throw new DBException.DataCorruption("parity4 broken: 0x" + Long.toHexString(v));
        return x;
    }

    /** {@code v} must have the low 16 bits clear. */
    static long p16set(long v) {
        assert (v & 0xFFFF) == 0 : "parity16 payload uses low 16 bits";
        return v | ((Long.bitCount(v) + 1) & 0xFFFF);
    }

    static long p16get(long v) {
        long x = v & ~0xFFFFL;
        if ((v & 0xFFFF) != ((Long.bitCount(x) + 1) & 0xFFFF))
            throw new DBException.DataCorruption("parity16 broken: 0x" + Long.toHexString(v));
        return x;
    }
}
