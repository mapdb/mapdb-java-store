package org.mapdb.stress;

import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;
import org.mapdb.ser.Serializer;
import org.mapdb.store.RecordRead;

import java.util.Arrays;
import java.util.zip.CRC32;

/**
 * Shared helpers for the large-scale stress suite (NOT a JUnit test; no {@code *Test}
 * name so surefire ignores it). Provides:
 *  - a RAW opaque-bytes serializer matching the store's content model (no length prefix),
 *  - self-validating payloads carrying (recid, version, CRC32) for torn-read detection,
 *  - throughput/summary printing.
 *
 * Sizes in every test scale by the {@code stress.scale} system property (default 1.0);
 * run at e.g. -Dstress.scale=0.01 to shake out bugs at 1% before a full-scale run.
 */
final class StressSupport {

    private StressSupport() {}

    /** Global size multiplier for quick smoke runs. */
    static final double SCALE = Double.parseDouble(System.getProperty("stress.scale", "1.0"));

    /** Store under test for the buffer-tree harnesses: -Dbuffertree.store=direct|appendonly. */
    static final String STORE_KIND = System.getProperty("buffertree.store", "direct");

    static org.mapdb.store.StoreDelta newBufferTreeStore() {
        switch (STORE_KIND) {
            case "direct": return new org.mapdb.store.StoreDirect(false);
            case "appendonly": return new org.mapdb.store.StoreAppendOnly(false);
            default: throw new IllegalArgumentException("unknown buffertree.store: " + STORE_KIND);
        }
    }

    static int scaled(long n) { return (int) Math.max(1, Math.round(n * SCALE)); }

    static long scaledL(long n) { return Math.max(1, Math.round(n * SCALE)); }

    /**
     * Opaque byte content, exactly as the store's delta model wants it: no length
     * framing, so appends concatenate cleanly and {@code read()} size == total content.
     */
    static final Serializer<byte[]> RAW = new Serializer<>() {
        @Override public void serialize(DataOutput2 out, byte[] v) { out.write(v); }
        @Override public byte[] deserialize(DataInput2 in, int size) {
            byte[] b = new byte[size];
            in.readFully(b);
            return b;
        }
    };

    /** read() action returning the record's content length; -1 for a null record. */
    static final RecordRead LEN_ACTION = new RecordRead() {
        @Override public long onBytes(DataInput2 in, int size) { return size; }
        @Override public long onNull() { return -1L; }
    };

    // ---- self-validating payload: [0..8)=recid [8..16)=version [16..20)=crc [20..BASE)=filler ----

    static final int BASE = 64;

    static byte[] payload(long recid, long version) { return payload(recid, version, BASE); }

    static byte[] payload(long recid, long version, int baseLen) {
        byte[] b = new byte[baseLen];
        putLong(b, 0, recid);
        putLong(b, 8, version);
        long s = recid * 0x9E3779B97F4A7C15L ^ version * 0xD1B54A32D192ED03L;
        for (int i = 20; i < baseLen; i++) {
            s = s * 6364136223846793005L + 1442695040888963407L;
            b[i] = (byte) (s >>> 56);
        }
        putInt(b, 16, crc32(b, baseLen));
        return b;
    }

    static int crc32(byte[] b, int baseLen) {
        CRC32 c = new CRC32();
        c.update(b, 0, 16);              // recid + version
        c.update(b, 20, baseLen - 20);   // filler (skip the crc slot itself)
        return (int) c.getValue();
    }

    /** Byte-exact large payload: fill deterministic from recid, no header. */
    static byte[] largePayload(long recid, int len) {
        byte[] b = new byte[len];
        long s = recid * 0x9E3779B97F4A7C15L + 0x243F6A8885A308D3L;
        for (int i = 0; i < len; i++) {
            s = s * 6364136223846793005L + 1442695040888963407L;
            b[i] = (byte) (s >>> 56);
        }
        return b;
    }

    /** Small 16-byte self-describing payload: recid at [0], derived tail at [8]. */
    static byte[] smallPayload(long recid) {
        byte[] b = new byte[16];
        putLong(b, 0, recid);
        putLong(b, 8, recid * 0x9E3779B97F4A7C15L ^ 0xABCDEF0123456789L);
        return b;
    }

    static long smallRecidOf(byte[] b) { return getLong(b, 0); }

    static void putLong(byte[] b, int off, long v) {
        for (int i = 0; i < 8; i++) b[off + i] = (byte) (v >>> (56 - 8 * i));
    }

    static long getLong(byte[] b, int off) {
        long r = 0;
        for (int i = 0; i < 8; i++) r = (r << 8) | (b[off + i] & 0xFF);
        return r;
    }

    static void putInt(byte[] b, int off, int v) {
        b[off] = (byte) (v >>> 24);
        b[off + 1] = (byte) (v >>> 16);
        b[off + 2] = (byte) (v >>> 8);
        b[off + 3] = (byte) v;
    }

    static int getInt(byte[] b, int off) {
        return ((b[off] & 0xFF) << 24) | ((b[off + 1] & 0xFF) << 16) | ((b[off + 2] & 0xFF) << 8) | (b[off + 3] & 0xFF);
    }

    /** Reusable, per-thread zero-copy validating read action (torn-read detector). */
    static final class Validator implements RecordRead {
        long expected;
        boolean sawNull;
        private final byte[] buf = new byte[BASE];
        private final CRC32 crc = new CRC32();

        @Override public long onBytes(DataInput2 in, int size) {
            sawNull = false;
            if (size < BASE) throw new AssertionError("short read: size=" + size + " recid=" + expected);
            in.readFully(buf, 0, BASE);
            long recid = getLong(buf, 0);
            if (recid != expected)
                throw new AssertionError("recid mismatch: content=" + recid + " expected=" + expected);
            crc.reset();
            crc.update(buf, 0, 16);
            crc.update(buf, 20, BASE - 20);
            int calc = (int) crc.getValue();
            int stored = getInt(buf, 16);
            if (calc != stored)
                throw new AssertionError("CRC mismatch recid=" + expected + " stored=" + stored + " calc=" + calc);
            return size;
        }

        @Override public long onNull() { sawNull = true; return -1L; }
    }

    // ---- reporting ----

    static long usedHeapMB() {
        Runtime r = Runtime.getRuntime();
        return (r.totalMemory() - r.freeMemory()) / (1024 * 1024);
    }

    static void summary(String name, long ops, long nanos, long bytes) {
        double sec = nanos / 1e9;
        System.out.printf(
            "[STRESS] %-46s ops=%,15d  wall=%8.2fs  ops/sec=%,13.0f  data=%7.2f GB  heapUsed=%,6d MB%n",
            name, ops, sec, sec > 0 ? ops / sec : 0.0, bytes / 1e9, usedHeapMB());
    }

    static void phase(String name, long ops, long nanos) {
        double sec = nanos / 1e9;
        System.out.printf("[STRESS]   %-44s ops=%,15d  wall=%8.2fs  ops/sec=%,13.0f%n",
            name, ops, sec, sec > 0 ? ops / sec : 0.0);
    }

    /** Skip a @Test unless -Dstress.only names its tag (comma list). */
    static void requireTag(String tag) {
        String only = System.getProperty("stress.only");
        if (only != null && !Arrays.asList(only.split(",")).contains(tag))
            org.junit.Assume.assumeTrue("skipped (stress.only=" + only + ")", false);
    }

    static long durationMs() {
        return Long.parseLong(System.getProperty("stress.durationMs", "30000"));
    }
}
