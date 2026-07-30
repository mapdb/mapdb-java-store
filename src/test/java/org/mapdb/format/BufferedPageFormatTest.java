package org.mapdb.format;

import org.junit.Test;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;
import org.mapdb.ser.LongFormat;
import org.mapdb.ser.Serializers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for the R1 {@link BufferedPageFormat}. Exercises the format-layer
 * primitives in isolation: reverse-readable framed entry codec, true newest-first reverse probe
 * (LWW), keyHash skip, bounded log shadow, byte-side range merge with NO base materialization,
 * consolidation + SPLIT_REQUIRED, and CRC32C bit-rot verification (framing is size-based, not CRC).
 */
public class BufferedPageFormatTest {

    private static BufferedPageFormat<Long, Long> fmt(boolean keyHash, boolean crc) {
        return new BufferedPageFormat<>(LongFormat.INSTANCE, LongFormat.INSTANCE, keyHash, crc);
    }

    /** A log-only page: {@code [int32 baseLen=0][entries...]}. Returns the raw bytes. */
    private static byte[] logPage(BufferedPageFormat<Long, Long> f, List<int[]> ops, List<long[]> kv) {
        DataOutput2 out = new DataOutput2(128);
        out.writeInt(0); // baseLen = 0 (no base image)
        for (int i = 0; i < ops.size(); i++) {
            int op = ops.get(i)[0];
            long[] e = kv.get(i);
            f.encodeInto(out, op, e[0], op == BufferedPageFormat.OP_PUT ? e[1] : null, BufferedPageFormat.LSN_UNASSIGNED);
        }
        return out.copyBytes();
    }

    // ---- 1. framed entry codec round-trips oldest→newest ----
    @Test public void encodeDecodeRoundTrip() {
        BufferedPageFormat<Long, Long> f = fmt(true, true);
        List<int[]> ops = List.of(new int[]{1}, new int[]{2}, new int[]{1});
        List<long[]> kv = List.of(new long[]{5, 50}, new long[]{6}, new long[]{7, 70});
        byte[] page = logPage(f, ops, kv);
        DataInput2 in = new DataInput2.ByteArray(page, 0);

        List<String> seen = new ArrayList<>();
        f.forEachEntryOldestFirst(in, 4, page.length,
                (opType, k, v, lsn) -> {
                    assertEquals("lsn placeholder is 0 in R1", 0L, lsn);
                    seen.add(opType + ":" + k + ":" + v);
                });
        assertEquals(List.of("1:5:50", "2:6:null", "1:7:70"), seen);
    }

    // ---- 2. reverse newest-first probe = LWW; PUT/DELETE newest wins ----
    @Test public void reverseProbeNewestWins() {
        BufferedPageFormat<Long, Long> f = fmt(true, true);
        BufferedPageFormat.PointResult<Long> pr = new BufferedPageFormat.PointResult<>();

        // key 5: PUT 50, PUT 55, DELETE  → newest DELETE wins (absent)
        byte[] p1 = logPage(f, List.of(new int[]{1}, new int[]{1}, new int[]{2}),
                List.of(new long[]{5, 50}, new long[]{5, 55}, new long[]{5}));
        assertTrue(f.probeNewestFirst(new DataInput2.ByteArray(p1, 0), 4, p1.length, 5L, probe(5L), Long.MAX_VALUE, pr));
        assertTrue("matched", pr.matched);
        assertFalse("newest op is DELETE", pr.put);

        // key 5: PUT 50, DELETE, PUT 99 → newest PUT wins (99)
        byte[] p2 = logPage(f, List.of(new int[]{1}, new int[]{2}, new int[]{1}),
                List.of(new long[]{5, 50}, new long[]{5}, new long[]{5, 99}));
        assertTrue(f.probeNewestFirst(new DataInput2.ByteArray(p2, 0), 4, p2.length, 5L, probe(5L), Long.MAX_VALUE, pr));
        assertTrue(pr.put);
        assertEquals((Long) 99L, pr.value);

        // absent key → no match
        assertFalse(f.probeNewestFirst(new DataInput2.ByteArray(p2, 0), 4, p2.length, 123L, probe(123L), Long.MAX_VALUE, pr));
    }

    // ---- 3. reverse scan STOPS at first match (newest key read first) ----
    @Test public void reverseScanStopsAtFirstMatch() {
        BufferedPageFormat<Long, Long> f = fmt(false, false); // no keyHash → matchBytes per entry
        List<int[]> ops = new ArrayList<>();
        List<long[]> kv = new ArrayList<>();
        for (long k = 0; k < 20; k++) { ops.add(new int[]{1}); kv.add(new long[]{k, k * 10}); }
        byte[] page = logPage(f, ops, kv);
        BufferedPageFormat.PointResult<Long> pr = new BufferedPageFormat.PointResult<>();

        Counting newest = new Counting(page);
        assertTrue(f.probeNewestFirst(newest, 4, page.length, 19L, probe(19L), Long.MAX_VALUE, pr));
        Counting oldest = new Counting(page);
        assertTrue(f.probeNewestFirst(oldest, 4, page.length, 0L, probe(0L), Long.MAX_VALUE, pr));
        // newest key is the LAST-appended entry → first in reverse → far fewer frames read
        assertTrue("newest match stops early (" + newest.readInts + " vs " + oldest.readInts + ")",
                newest.readInts * 4 < oldest.readInts);
    }

    // ---- 4. keyHash rejects non-matching keys without a byte compare ----
    @Test public void keyHashSkipsByteCompare() {
        List<int[]> ops = new ArrayList<>();
        List<long[]> kv = new ArrayList<>();
        for (long k = 0; k < 20; k++) { ops.add(new int[]{1}); kv.add(new long[]{k, k}); }
        BufferedPageFormat.PointResult<Long> pr = new BufferedPageFormat.PointResult<>();

        BufferedPageFormat<Long, Long> withHash = fmt(true, false);
        byte[] hp = logPage(withHash, ops, kv);
        Counting h = new Counting(hp);
        assertFalse(withHash.probeNewestFirst(h, 4, hp.length, 999L, probe(999L), Long.MAX_VALUE, pr));

        BufferedPageFormat<Long, Long> noHash = fmt(false, false);
        byte[] np = logPage(noHash, ops, kv);
        Counting n = new Counting(np);
        assertFalse(noHash.probeNewestFirst(n, 4, np.length, 999L, probe(999L), Long.MAX_VALUE, pr));

        // hash gate: absent key with a mismatching hash → matchBytes almost never called
        assertTrue("keyHash suppresses matchBytes: " + h.matchBytes, h.matchBytes <= 1);
        assertEquals("no-hash format compares every key's bytes", 20, n.matchBytes);
    }

    // ---- 5. bounded log shadow: newest op per key, restricted to range ----
    @Test public void logShadowInRange() {
        BufferedPageFormat<Long, Long> f = fmt(true, true);
        byte[] page = logPage(f,
                List.of(new int[]{1}, new int[]{1}, new int[]{1}, new int[]{2}, new int[]{1}),
                List.of(new long[]{1, 10}, new long[]{5, 50}, new long[]{5, 55}, new long[]{7}, new long[]{9, 90}));
        NavigableMap<Long, Object> shadow = new TreeMap<>();
        f.logShadowInRange(new DataInput2.ByteArray(page, 0), 4, page.length, 3L, true, 8L, true, Long.MAX_VALUE, shadow);
        // in [3,8]: key 5 → newest PUT 55; key 7 → tombstone. keys 1 and 9 out of range.
        assertEquals(2, shadow.size());
        assertEquals(55L, shadow.get(5L));
        assertEquals(BufferedPageFormat.TOMBSTONE, shadow.get(7L));
    }

    // ---- 6. byte-side range merge materializes ONLY the log shadow, never the base ----
    @Test public void rangeMergeNoBaseMaterialization() {
        BufferedPageFormat<Long, Long> f = fmt(true, true);
        // log: PUT 4->400, DELETE 6
        byte[] page = logPage(f, List.of(new int[]{1}, new int[]{2}),
                List.of(new long[]{4, 400}, new long[]{6}));
        NavigableMap<Long, Object> shadow = new TreeMap<>();
        f.logShadowInRange(new DataInput2.ByteArray(page, 0), 4, page.length, null, true, null, true, Long.MAX_VALUE, shadow);

        // base = {2:20,4:40,6:60,8:80}; a cursor that FORBIDS whole-group materialization
        long[] baseK = {2, 4, 6, 8}, baseV = {20, 40, 60, 80};
        Cursor cur = new Cursor(baseK, baseV);
        List<String> merged = new ArrayList<>();
        var it = f.merge(cur, shadow);
        while (it.hasNext()) { Map.Entry<Long, Long> e = it.next(); merged.add(e.getKey() + "=" + e.getValue()); }
        // 4 overridden to 400, 6 deleted, 2 and 8 from base
        assertEquals(List.of("2=20", "4=400", "8=80"), merged);
    }

    // ---- 7. consolidation folds newest op per key; oversize → SPLIT_REQUIRED ----
    @Test public void consolidateAndSplitRequired() {
        BufferedPageFormat<Long, Long> f = fmt(true, true);
        Object baseK = LongFormat.INSTANCE.fromArray(new Object[]{1L, 3L, 5L});
        Object baseV = LongFormat.INSTANCE.fromArray(new Object[]{10L, 30L, 50L});
        // ops: UPDATE 3->33, DELETE 5, INSERT 4->44
        byte[] page = logPage(f, List.of(new int[]{1}, new int[]{2}, new int[]{1}),
                List.of(new long[]{3, 33}, new long[]{5}, new long[]{4, 44}));
        DataInput2 in = new DataInput2.ByteArray(page, 0);

        BufferedPageFormat.Consolidated c = f.consolidate(baseK, baseV, in, 4, page.length, Long.MAX_VALUE, 100);
        assertFalse(c.splitRequired);
        // net: 1->10 (base), 3->33 (op), 5 deleted, 4->44 (op) => keys {1,3,4}
        assertEquals(3, c.keyCount);
        List<String> got = new ArrayList<>();
        for (int i = 0; i < LongFormat.INSTANCE.size(c.keys); i++)
            got.add(LongFormat.INSTANCE.get(c.keys, i) + "=" + LongFormat.INSTANCE.get(c.values, i));
        assertEquals(List.of("1=10", "3=33", "4=44"), got);

        // maxBaseKeys below result size → SPLIT_REQUIRED
        BufferedPageFormat.Consolidated split =
                f.consolidate(baseK, baseV, new DataInput2.ByteArray(page, 0), 4, page.length, Long.MAX_VALUE, 2);
        assertTrue(split.splitRequired);
        assertNull(split.keys);
    }

    // ---- 8. CRC32C detects bit-rot; framing is size-based, not CRC ----
    @Test public void crcVerifyDetectsBitRot() {
        BufferedPageFormat<Long, Long> f = fmt(true, true);
        byte[] page = logPage(f, List.of(new int[]{1}), List.of(new long[]{5, 50}));
        DataInput2 in = new DataInput2.ByteArray(page, 0);
        int totalLen = new DataInput2.ByteArray(page, 4).readInt();
        assertTrue(f.verifyEntry(in, 4, totalLen));
        // flip a payload byte inside the entry (value region) → CRC fails, but size framing intact
        page[page.length - 6] ^= 0x7F;
        assertFalse(f.verifyEntry(new DataInput2.ByteArray(page, 0), 4, totalLen));
    }

    // ---- 9. base fingerprint: NO false negatives (every present base key tests "maybe") ----
    @Test public void baseFingerprintNoFalseNegatives() {
        BufferedPageFormat<Long, Long> f = fmtFp(8);
        assertTrue(f.baseFingerprintEnabled());
        int n = 300;
        Long[] keys = new Long[n];
        for (int i = 0; i < n; i++) keys[i] = (long) (i * 7 + 1);
        Object baseKeys = LongFormat.INSTANCE.fromArray(keys);
        byte[] page = fpPage(f, baseKeys);

        DataInput2 in = new DataInput2.ByteArray(page, 0);
        int baseLen = in.readInt();
        BufferedPageFormat.BaseHeader h = new BufferedPageFormat.BaseHeader();
        f.parseBaseHeader(in, 4, baseLen, h);
        assertTrue("filter present", h.fpLen > 0);

        // EXHAUSTIVE: every present key MUST test "maybe" — no false negative ever.
        for (Long k : keys)
            assertTrue("present key " + k + " wrongly reported absent", f.baseFpMightContain(in, h, probe(k)));

        // absent keys: mostly "definitely absent"; measure a loose false-positive bound.
        int fp = 0, trials = 6000;
        for (long k = 1_000_000; k < 1_000_000 + trials; k++)
            if (f.baseFpMightContain(in, h, probe(k))) fp++;
        double rate = fp / (double) trials;
        assertTrue("fp rate too high: " + rate, rate < 0.15);
    }

    // ---- 10. self-describing header: fpLen=0 ⇒ always maybe; unknown version ⇒ hard fail ----
    @Test public void baseFingerprintSelfDescribing() {
        // disabled format (bitsPerKey=0) writes version + fpLen=0 ⇒ baseFpMightContain always true
        BufferedPageFormat<Long, Long> off = fmt(true, true);
        assertFalse(off.baseFingerprintEnabled());
        byte[] page = fpPage(off, LongFormat.INSTANCE.fromArray(new Object[]{1L, 2L, 3L}));
        DataInput2 in = new DataInput2.ByteArray(page, 0);
        int baseLen = in.readInt();
        BufferedPageFormat.BaseHeader h = new BufferedPageFormat.BaseHeader();
        off.parseBaseHeader(in, 4, baseLen, h);
        assertEquals(0, h.fpLen);
        assertTrue(off.baseFpMightContain(in, h, probe(999L))); // no filter ⇒ must search

        // unknown version byte ⇒ throws (never a silent skip / misparse)
        byte[] bad = {0, 0, 0, 1, (byte) 2}; // baseLen=1, version=2
        try {
            new BufferedPageFormat<>(LongFormat.INSTANCE, LongFormat.INSTANCE, true, true, 8)
                    .parseBaseHeader(new DataInput2.ByteArray(bad, 0), 4, 1, new BufferedPageFormat.BaseHeader());
            org.junit.Assert.fail("expected unknown-version throw");
        } catch (IllegalStateException expected) { /* fail-closed */ }
    }

    // ---- 11. header round-trips: node base is reachable exactly after the fp header ----
    @Test public void baseHeaderRoundTripNodeBase() {
        BufferedPageFormat<Long, Long> f = fmtFp(8);
        Object baseKeys = LongFormat.INSTANCE.fromArray(new Object[]{2L, 4L, 6L, 8L});
        // build [int32 baseLen][fp header][node-base marker bytes]
        DataOutput2 fpOut = new DataOutput2(64);
        f.writeBaseFp(fpOut, baseKeys);
        byte[] fp = fpOut.copyBytes();
        byte[] marker = {(byte) 0xAB, (byte) 0xCD, (byte) 0xEF};
        DataOutput2 out = new DataOutput2(fp.length + marker.length + 4);
        out.writeInt(fp.length + marker.length);
        out.write(fp, 0, fp.length);
        out.write(marker, 0, marker.length);
        byte[] page = out.copyBytes();

        DataInput2 in = new DataInput2.ByteArray(page, 0);
        int baseLen = in.readInt();
        BufferedPageFormat.BaseHeader h = new BufferedPageFormat.BaseHeader();
        f.parseBaseHeader(in, 4, baseLen, h);
        assertEquals("hdrLen", fp.length, h.hdrLen);
        assertEquals("nodeBase abs offset", 4 + fp.length, h.nodeBase);
        in.pos(h.nodeBase);
        assertEquals((byte) 0xAB, in.readByte()); // node base begins exactly here
    }

    // ---- 12. malformed header (fp overruns base region) ⇒ hard fail, never a skip ----
    @Test public void baseFingerprintMalformedThrows() {
        // baseLen=3, version=1, fpLen=100 (0xE4 packed), k=6 → bits would run past the region
        byte[] bad = {0, 0, 0, 3, 1, (byte) 0xE4, 6};
        try {
            fmtFp(8).parseBaseHeader(new DataInput2.ByteArray(bad, 0), 4, 3, new BufferedPageFormat.BaseHeader());
            org.junit.Assert.fail("expected overrun throw");
        } catch (IllegalStateException expected) { /* fail-closed */ }
    }

    // ---- 13. parseBaseHeader is FAIL-CLOSED on every malformed shape (throws, never a wrong skip) ----
    @Test public void parseBaseHeaderFailClosed() {
        BufferedPageFormat<Long, Long> f = fmtFp(8);
        // (a) baseLen too short for even the version+fpLen bytes
        assertThrows(f, new byte[]{0, 0, 0, 1, 1}, 1);
        // (b) header-only base: fp fills the whole base region, NO node base after it (nodeBase==end)
        DataOutput2 fpOut = new DataOutput2(64);
        f.writeBaseFp(fpOut, LongFormat.INSTANCE.fromArray(new Object[]{1L, 2L, 3L}));
        byte[] fp = fpOut.copyBytes();
        DataOutput2 ho = new DataOutput2(fp.length + 4);
        ho.writeInt(fp.length);
        ho.write(fp, 0, fp.length);
        assertThrows(f, ho.copyBytes(), fp.length);
        // (c) k == 0 (fpLen=8 via packInt 0x88), then k byte 0, plus padding + a node byte
        assertThrows(f, new byte[]{0, 0, 0, 12, 1, (byte) 0x88, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 12);
        // (d) k > FP_K_MAX
        assertThrows(f, new byte[]{0, 0, 0, 12, 1, (byte) 0x88, 17, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 12);
        // (e) fpLen > FP_MAX_BYTES (600 = packLong 0x04 0xD8)
        assertThrows(f, new byte[]{0, 0, 0, 8, 1, 0x04, (byte) 0xD8, 6, 0, 0, 0, 0}, 8);
    }

    private static void assertThrows(BufferedPageFormat<Long, Long> f, byte[] page, int baseLen) {
        try {
            f.parseBaseHeader(new DataInput2.ByteArray(page, 0), 4, baseLen, new BufferedPageFormat.BaseHeader());
            org.junit.Assert.fail("expected fail-closed throw");
        } catch (IllegalStateException expected) { /* fail-closed: a validated format error */ }
    }

    // ---- P7: FpMode can only ever force MORE searching (never synthesize a definite-absent) ----
    @Test public void fpModeNeverManufacturesAbsent() {
        BufferedPageFormat<Long, Long> f = fmtFp(8);
        Object baseKeys = LongFormat.INSTANCE.fromArray(new Object[]{10L, 20L, 30L});
        byte[] page = fpPage(f, baseKeys);
        DataInput2 in = new DataInput2.ByteArray(page, 0);
        int baseLen = in.readInt();
        BufferedPageFormat.BaseHeader h = new BufferedPageFormat.BaseHeader();
        f.parseBaseHeader(in, 4, baseLen, h);
        try {
            for (BufferedPageFormat.FpMode mode : new BufferedPageFormat.FpMode[]{
                    BufferedPageFormat.FpMode.FORCE_SEARCH, BufferedPageFormat.FpMode.RANDOM}) {
                BufferedPageFormat.testSetFpMode(mode);
                // FORCE_SEARCH/RANDOM may only add "maybe"; a definite-absent (false) can only come
                // from a real bit miss under NORMAL, so under these modes a known absent may report
                // either — but a known PRESENT must still always be "maybe".
                for (int i = 0; i < 500; i++)
                    assertTrue(f.baseFpMightContain(in, h, probe(20L)));
            }
        } finally {
            BufferedPageFormat.testSetFpMode(BufferedPageFormat.FpMode.NORMAL);
        }
    }

    // ---- helpers ----

    private static BufferedPageFormat<Long, Long> fmtFp(int bitsPerKey) {
        return new BufferedPageFormat<>(LongFormat.INSTANCE, LongFormat.INSTANCE, true, true, bitsPerKey);
    }

    /**
     * {@code [int32 baseLen][fp header][1 node-base byte]} — mirrors the production shape (a base
     * region always carries a non-empty consumer node base after the fp header).
     */
    private static byte[] fpPage(BufferedPageFormat<Long, Long> f, Object baseKeys) {
        DataOutput2 fpOut = new DataOutput2(256);
        f.writeBaseFp(fpOut, baseKeys);
        byte[] fp = fpOut.copyBytes();
        DataOutput2 out = new DataOutput2(fp.length + 5);
        out.writeInt(fp.length + 1);        // baseLen = fp header + 1 node-base byte
        out.write(fp, 0, fp.length);
        out.writeByte(0);                   // dummy node base (non-empty)
        return out.copyBytes();
    }

    private static byte[] probe(long k) {
        DataOutput2 o = new DataOutput2(8);
        Serializers.LONG.serialize(o, k);
        return o.copyBytes();
    }

    /** In-memory base cursor that FAILS if asked to materialize the whole group. */
    private static final class Cursor implements BufferedPageFormat.KvCursor<Long, Long> {
        final long[] k, v;
        int i = -1;
        Cursor(long[] k, long[] v) { this.k = k; this.v = v; }
        @Override public boolean hasNext() { return i + 1 < k.length; }
        @Override public void next() { i++; }
        @Override public Long key() { return k[i]; }
        @Override public Long value() { return v[i]; }
    }

    /** DataInput2 wrapper counting {@code readInt} (frame scans) and {@code matchBytes} (key compares). */
    private static final class Counting extends DataInput2 {
        final DataInput2.ByteArray d;
        int readInts = 0, matchBytes = 0;
        Counting(byte[] b) { d = new DataInput2.ByteArray(b, 0); }
        @Override public int pos() { return d.pos(); }
        @Override public void pos(int p) { d.pos(p); }
        @Override public byte readByte() { return d.readByte(); }
        @Override public void readFully(byte[] b, int o, int l) { d.readFully(b, o, l); }
        @Override public void skipBytes(int n) { d.skipBytes(n); }
        @Override public int readInt() { readInts++; return d.readInt(); }
        @Override public boolean matchBytes(byte[] e) { matchBytes++; return d.matchBytes(e); }
    }
}
