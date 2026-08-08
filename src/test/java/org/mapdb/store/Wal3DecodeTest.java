package org.mapdb.store;

import org.junit.Test;
import org.mapdb.io.DataOutput2;

import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The {@link Wal3Decode} battery: hand-built segments in shapes the pinned C3 corpus does not
 * contain.
 *
 * <p><b>Why this exists.</b> The C3 framing and body comparisons run against three checked-in
 * bundles, and those bundles are uniform in ways that make several of the decoder's computations
 * unfalsifiable by them. The C3j review measured three such mutants surviving the whole suite:
 *
 * <ul>
 *   <li>{@code h.flags = 0} instead of decoding the field — every sample flags word IS zero;</li>
 *   <li>sorting each section's entries by recid before returning them — every sample section is
 *       already in ascending recid order, so the sort is a no-op and the "ordered entry stream"
 *       the body dump publishes was never observed to be ordered;</li>
 *   <li>swapping the two {@code 'K'} mark longs — nothing downstream of the decoder distinguishes
 *       them.</li>
 * </ul>
 *
 * <p>This is the same answer C3s reached for the python fold: a comparison can only see the inputs
 * it did not compute, so the shapes the corpus lacks get synthetic inputs built from the writer's
 * own encoders ({@link WalTestKit}, {@link DataOutput2#packLong}) rather than from transcribed
 * bytes. Each case below either kills one of those mutants or exercises a refusal the corpus can
 * never reach, because it is byte-pinned VALID.
 */
public class Wal3DecodeTest {

    // ---------- builders, from the writer's own encoders ----------

    private static byte[] record(long recid, long cap, byte[] content) {
        DataOutput2 o = new DataOutput2(32);
        o.writeByte(StoreWAL.T_RECORD);
        o.packLong(recid);
        o.packLong(cap);
        o.packLong(content == null ? 0 : content.length + 1);
        byte[] head = java.util.Arrays.copyOf(o.buf, o.pos);
        return content == null ? head : WalTestKit.concat(head, content);
    }

    private static byte[] simple(int tag, long recid) {
        DataOutput2 o = new DataOutput2(16);
        o.writeByte(tag);
        o.packLong(recid);
        return java.util.Arrays.copyOf(o.buf, o.pos);
    }

    /** A one-section segment carrying {@code body} under {@code tag}. */
    private static byte[] segment(long seq, long lsn, char tag, byte[] body) {
        byte[] header = WalTestKit.segmentHeader(seq, lsn);
        return WalTestKit.concat(header, WalTestKit.section(header, WalSegmentSet.SEG_HDR, tag, lsn, body));
    }

    private static Wal3Decode.Section only(byte[] seg) {
        Wal3Decode.Segment d = Wal3Decode.decode(seg, "synthetic");
        assertEquals("expected exactly one section", 1, d.sections.size());
        assertEquals("unexpected trailing bytes", 0, d.trailing);
        return d.sections.get(0);
    }

    private static void refused(String what, Runnable r) {
        boolean accepted;
        AssertionError err = null;
        try {
            r.run();
            accepted = true;
        } catch (AssertionError e) {
            accepted = false;
            err = e;
        }
        assertTrue("the decoder accepted " + what, !accepted);
        assertTrue("refusal of " + what + " carried no message", err.getMessage() != null);
    }

    // ---------- the cases the corpus cannot express ----------

    /**
     * Entries come back in WIRE order, not recid order.
     *
     * <p>Kills the measured sort-by-recid mutant. Every section of the pinned corpus happens to be
     * in ascending recid order, so ordering was published by the body dump and checked by nothing;
     * these three entries are deliberately descending.
     */
    @Test public void entries_keep_wire_order() {
        byte[] body = WalTestKit.concat(
                record(9, 16, new byte[]{1}),
                simple(StoreWAL.T_PREALLOC, 4),
                record(2, 16, new byte[]{2}));
        List<Wal3Decode.Entry> got = Wal3Decode.entries(only(segment(1, 1, 'S', body)), "synthetic");
        assertEquals(3, got.size());
        assertEquals("first entry", 9, got.get(0).recid);
        assertEquals("second entry", 4, got.get(1).recid);
        assertEquals("third entry", 2, got.get(2).recid);
        assertEquals("PREALLOC", got.get(1).kind());
    }

    /**
     * A NULL record and a ZERO-LENGTH record decode differently in BOTH observable ways.
     *
     * <p>The corpus does now carry one of each (C3s regenerated it for exactly this), but the two
     * live in different bundles and different sections; here they are adjacent, so a decoder that
     * collapsed them could not pass by accident of layout.
     */
    @Test public void null_and_zero_length_records_are_distinct() {
        byte[] body = WalTestKit.concat(record(1, 0, null), record(2, 16, new byte[0]));
        List<Wal3Decode.Entry> got = Wal3Decode.entries(only(segment(1, 1, 'S', body)), "synthetic");
        assertEquals(0, got.get(0).lenPlus);
        assertTrue("a NULL record must carry no content array at all", got.get(0).content == null);
        assertEquals(1, got.get(1).lenPlus);
        assertArrayEquals("a zero-length record carries an EMPTY array, not null",
                new byte[0], got.get(1).content);
    }

    /**
     * The two {@code 'K'} mark longs come back in the documented order.
     *
     * <p>Kills the swap mutant: the sample's real mark is {@code (2, 9)}, and nothing downstream of
     * the decoder tells the fields apart. Here they are far apart and unmistakable.
     */
    @Test public void mark_fields_are_in_order() {
        byte[] header = WalTestKit.segmentHeader(7, 5);
        byte[] seg = WalTestKit.concat(header,
                WalTestKit.mark(header, WalSegmentSet.SEG_HDR, 5, 3, 4));
        long[] mark = Wal3Decode.mark(only(seg), "synthetic");
        assertEquals("cleanedThroughSeq comes first", 3, mark[0]);
        assertEquals("logStartLsn comes second", 4, mark[1]);
        refused("a mark body of the wrong length",
                () -> Wal3Decode.mark(only(segment(1, 1, 'K', new byte[8])), "synthetic"));
    }

    /**
     * Header fields are DECODED, not assumed.
     *
     * <p>Kills the {@code flags = 0} mutant. Every segment this format has ever written carries
     * {@code flags == 0} ({@code WalSegmentSet} line 446 writes the constant), so no corpus can
     * ever falsify that field's decode — only a hand-built header can.
     */
    @Test public void header_fields_are_decoded() {
        byte[] header = WalTestKit.segmentHeader(0x0102030405060708L, 0x1112131415161718L);
        WalSegmentSet.putBe32(header, 12, 0x2A);       // a flags word no writer emits
        WalTestKit.resealSegmentHeader(header);
        byte[] seg = WalTestKit.concat(header,
                WalTestKit.section(header, WalSegmentSet.SEG_HDR, 'S', 1, simple(StoreWAL.T_DELETE, 3)));
        Wal3Decode.Segment d = Wal3Decode.decode(seg, "synthetic");
        assertEquals(WalSegmentSet.FORMAT_VERSION, d.header.version);
        assertEquals(0x2A, d.header.flags);
        assertEquals(0x0102030405060708L, d.header.seq);
        assertEquals(0x1112131415161718L, d.header.firstLsn);
        assertEquals(WalSegmentSet.SEG_HDR, d.sections.get(0).offset);
    }

    /** Section offsets advance by {@code SEC_HDR + bodyLen}, and each is CRC-bound to its offset. */
    @Test public void section_offsets_advance() {
        byte[] header = WalTestKit.segmentHeader(1, 1);
        byte[] a = WalTestKit.section(header, WalSegmentSet.SEG_HDR, 'S', 1, simple(StoreWAL.T_DELETE, 1));
        int second = WalSegmentSet.SEG_HDR + a.length;
        byte[] b = WalTestKit.section(header, second, 'S', 2, simple(StoreWAL.T_DELETE, 2));
        Wal3Decode.Segment d = Wal3Decode.decode(WalTestKit.concat(header, a, b), "synthetic");
        assertEquals(2, d.sections.size());
        assertEquals(WalSegmentSet.SEG_HDR, d.sections.get(0).offset);
        assertEquals(second, d.sections.get(1).offset);
        assertEquals(1, d.sections.get(0).lsn);
        assertEquals(2, d.sections.get(1).lsn);
        // The domain separator is the offset itself, so the same section bytes at a different
        // offset must NOT verify — the property fixture C7 tests, asserted here on the decoder.
        byte[] moved = WalTestKit.concat(header, b);
        refused("a section whose bytes were checksummed for another offset",
                () -> Wal3Decode.decode(moved, "synthetic"));
    }

    // ---------- refusals a byte-pinned VALID corpus can never reach ----------

    @Test public void a_damaged_segment_is_refused() {
        byte[] good = segment(1, 1, 'S', simple(StoreWAL.T_DELETE, 1));

        byte[] shortSeg = java.util.Arrays.copyOf(good, WalSegmentSet.SEG_HDR - 1);
        refused("a segment shorter than its header", () -> Wal3Decode.decode(shortSeg, "synthetic"));

        byte[] badMagic = good.clone();
        badMagic[3] ^= 1;
        refused("a bad magic", () -> Wal3Decode.decode(badMagic, "synthetic"));

        byte[] badVersion = good.clone();
        WalSegmentSet.putBe32(badVersion, 8, WalSegmentSet.FORMAT_VERSION + 1);
        WalTestKit.resealSegmentHeader(badVersion);
        refused("a future format version", () -> Wal3Decode.decode(badVersion, "synthetic"));

        byte[] badHeaderCrc = good.clone();
        WalSegmentSet.putBe32(badHeaderCrc, WalSegmentSet.SEG_HDR_CRC_LEN, 0xDEADBEEF);
        refused("a wrong header CRC", () -> Wal3Decode.decode(badHeaderCrc, "synthetic"));

        byte[] badTag = good.clone();
        badTag[WalSegmentSet.SEG_HDR] = 'Z';
        refused("an unknown section tag", () -> Wal3Decode.decode(badTag, "synthetic"));

        byte[] badSecHdrCrc = good.clone();
        WalSegmentSet.putBe32(badSecHdrCrc, WalSegmentSet.SEG_HDR + 17, 0xDEADBEEF);
        refused("a wrong section header CRC", () -> Wal3Decode.decode(badSecHdrCrc, "synthetic"));

        byte[] badBodyCrc = good.clone();
        WalSegmentSet.putBe32(badBodyCrc, WalSegmentSet.SEG_HDR + 21, 0xDEADBEEF);
        refused("a wrong section body CRC", () -> Wal3Decode.decode(badBodyCrc, "synthetic"));
    }

    /**
     * A torn tail is REPORTED, not swallowed.
     *
     * <p>The engine truncates crash residue, which is correct for a store opening a live
     * namespace. A golden sample with bytes after its last whole section is a different thing —
     * it means the pins do not describe all of the file — so this decoder counts them and lets
     * the caller assert zero.
     */
    @Test public void a_torn_tail_is_reported() {
        byte[] seg = WalTestKit.concat(segment(1, 1, 'S', simple(StoreWAL.T_DELETE, 1)), new byte[7]);
        Wal3Decode.Segment d = Wal3Decode.decode(seg, "synthetic");
        assertEquals(1, d.sections.size());
        assertEquals(7, d.trailing);
    }

    /** An entry stream that runs off the end of its body is refused rather than truncated. */
    @Test public void a_truncated_entry_stream_is_refused() {
        // lenPlus claims 100 content bytes; the body holds none.
        DataOutput2 o = new DataOutput2(16);
        o.writeByte(StoreWAL.T_RECORD);
        o.packLong(1);
        o.packLong(112);
        o.packLong(101);
        byte[] body = java.util.Arrays.copyOf(o.buf, o.pos);
        refused("a record whose content runs past the section body",
                () -> Wal3Decode.entries(only(segment(1, 1, 'S', body)), "synthetic"));

        refused("an unknown entry tag",
                () -> Wal3Decode.entries(only(segment(1, 1, 'S', simple(99, 1))), "synthetic"));
        // Truncated T_APPEND (tag+recid only) fails mid-delta, not as "unknown".
        refused("a truncated T_APPEND entry",
                () -> Wal3Decode.entries(only(segment(1, 1, 'S', simple(StoreWAL.T_APPEND, 1))),
                        "synthetic"));
        // delta must be in [1, lsn-1]; at section LSN 1 no legal delta exists.
        DataOutput2 badDelta = new DataOutput2(16);
        badDelta.writeByte(StoreWAL.T_APPEND);
        badDelta.packLong(1);
        badDelta.packLong(1); // delta=1 with lsn=1 → outside [1, 0]
        badDelta.packLong(0);
        byte[] badDeltaBody = java.util.Arrays.copyOf(badDelta.buf, badDelta.pos);
        refused("a T_APPEND with delta outside [1, lsn-1]",
                () -> Wal3Decode.entries(only(segment(1, 1, 'S', badDeltaBody)), "synthetic"));
        // Legal delta, overlong len: claims more payload than remains (C9a §4.3).
        DataOutput2 overLen = new DataOutput2(16);
        overLen.writeByte(StoreWAL.T_APPEND);
        overLen.packLong(1);
        overLen.packLong(1); // delta=1, section LSN 5 → ok
        overLen.packLong(100); // len, no payload bytes follow
        byte[] overLenBody = java.util.Arrays.copyOf(overLen.buf, overLen.pos);
        refused("a T_APPEND whose len overruns the section body",
                () -> Wal3Decode.entries(only(segment(1, 5, 'S', overLenBody)), "synthetic"));
    }

    /** Well-formed T_APPEND decodes delta, baseLsn, len and payload (C9a / O1). */
    @Test public void append_entries_decode_four_fields() {
        DataOutput2 o = new DataOutput2(32);
        o.writeByte(StoreWAL.T_APPEND);
        o.packLong(7);
        o.packLong(1); // delta
        o.packLong(3); // len
        o.write(new byte[]{10, 20, 30});
        byte[] body = java.util.Arrays.copyOf(o.buf, o.pos);
        // section LSN 5 → baseLsn = 5 - 1 = 4
        List<Wal3Decode.Entry> es = Wal3Decode.entries(only(segment(1, 5, 'S', body)), "synthetic");
        assertEquals(1, es.size());
        Wal3Decode.Entry e = es.get(0);
        assertEquals("APPEND", e.kind());
        assertEquals(7, e.recid);
        assertEquals(1, e.delta);
        assertEquals(4, e.baseLsn);
        assertEquals(3, e.appendLen);
        assertArrayEquals(new byte[]{10, 20, 30}, e.content);
    }

    /** {@code 'C'} decodes exactly like {@code 'S'}: {@code StoreWAL} gives it no special handling. */
    @Test public void image_sections_decode_like_ordinary_ones() {
        byte[] body = WalTestKit.concat(record(5, 16, new byte[]{7}), simple(StoreWAL.T_DELETE, 6));
        List<Wal3Decode.Entry> s = Wal3Decode.entries(only(segment(1, 1, 'S', body)), "synthetic");
        List<Wal3Decode.Entry> c = Wal3Decode.entries(only(segment(1, 1, 'C', body)), "synthetic");
        assertEquals(s.size(), c.size());
        for (int i = 0; i < s.size(); i++) {
            assertEquals(s.get(i).kind(), c.get(i).kind());
            assertEquals(s.get(i).recid, c.get(i).recid);
            assertEquals(s.get(i).lenPlus, c.get(i).lenPlus);
        }
        refused("entries() on a mark section",
                () -> Wal3Decode.entries(only(segment(1, 1, 'K', new byte[16])), "synthetic"));
    }
}
