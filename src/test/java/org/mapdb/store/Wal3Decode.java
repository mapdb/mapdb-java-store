package org.mapdb.store;

import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/**
 * A standalone, CRC-validating decoder for one WAL v3 segment, for the cross-engine C3 checks.
 *
 * <p><b>Why this exists at all, given the engine already parses these bytes.</b> The engine's
 * scan is inline and private in three separate methods of {@link StoreWAL}, and it exists to
 * <em>apply</em> sections, not to report them. C3 needs the decoded <em>values</em> — the header
 * fields, every section's offset and both CRCs, and the entry stream of each body — so that
 * {@code GOLDEN-DECODE.tsv} (framing, python-authored) and {@code GOLDEN-BODY.tsv} (bodies,
 * authored here) can be compared field by field. Exposing them from the engine would mean
 * editing frozen main code to serve a test; decoding them here does not.
 *
 * <p><b>It lives in {@code org.mapdb.store} to borrow the engine's constants rather than
 * transcribe them.</b> {@link WalSegmentSet#SEG_HDR}, {@link WalSegmentSet#MAGIC},
 * {@link WalSegmentSet#FORMAT_VERSION}, {@link StoreWAL#SEC_HDR},
 * {@link StoreWAL#SEC_HDR_CRC_LEN} and the {@code T_*} entry tags are all package-private, and a
 * decoder that re-declared them would agree with a stale copy of the format instead of with the
 * reference. The three tag bytes {@code 'S'}/{@code 'C'}/{@code 'K'} are the exception — they are
 * private in {@link StoreWAL} — so they are written out here and cross-checked in
 * {@link #TAGS}'s comment against the layout javadoc.
 *
 * <p><b>Exactly what it validates, and what it does not.</b> It checks the magic, the format
 * version, the header CRC, both section CRCs (domain-separated by
 * {@code segmentHeader(36) || be64(sectionOffset)}, per {@code WalSegmentSet.Segment.crcDomain}),
 * the section tag vocabulary, and every entry-stream bound — and it throws on any failure, so it
 * cannot report a framing that the bytes do not support.
 *
 * <p>It is <b>NOT</b> a re-implementation of the engine's acceptance rules, and an earlier version
 * of this paragraph wrongly implied it was. It does not require {@code flags == 0}, does not check
 * the header sequence against the file name, does not enforce dense or increasing section LSNs or
 * cross-segment LSN linkage, does not range-check the {@code 'K'} mark fields, does not apply
 * {@code capValid}, and does not enforce one entry per recid per section. The engine does all of
 * that ({@code WalSegmentSet:408-416}, {@code StoreWAL:679-718,1142-1148,1282-1286}), and the C3
 * suite opens every one of these same bundles through the engine in the same run — so those rules
 * have a witness, just not this one. Where a decoded field would otherwise be graded only against
 * a file this codebase also wrote, the witness is stated explicitly at the point of use
 * ({@code Wal3BodyDump.checkCap}, {@code checkMark}).
 *
 * <p>It is deliberately NOT a recovery implementation either: it does not tolerate a torn tail, it
 * reports one — see {@link Segment#trailing} — because a static golden sample with unaccounted
 * trailing bytes means the pins do not describe all of its bytes.
 */
public final class Wal3Decode {

    private Wal3Decode() {}

    /**
     * Section tags. Private in {@link StoreWAL} ({@code TAG_SECTION}/{@code TAG_IMAGE}/
     * {@code TAG_MARK}), so restated here; {@link StoreWAL}'s class javadoc §"segment layout" is
     * the source. An unknown tag is refused rather than passed through, because a reader that
     * skips a tag it does not know silently omits whatever that section carried.
     */
    static final String TAGS = "SCK";

    /** The 16-byte {@code 'K'} body: {@code cleanedThroughSeq(8) | logStartLsn(8)}. */
    static final int MARK_BODY_LEN = 16;

    public static final class Header {
        public int version, flags, headerCrc;
        public long seq, firstLsn;
    }

    public static final class Section {
        public int index, hdrCrc, bodyCrc;
        public long offset, lsn, bodyLen;
        public char tag;
        public byte[] body;
    }

    public static final class Entry {
        /** One of {@link StoreWAL#T_PREALLOC}, {@code T_RECORD}, {@code T_APPEND}, {@code T_DELETE}. */
        public int tag;
        public long recid;
        /** {@code T_RECORD} only, else -1. */
        public long cap = -1;
        /**
         * {@code T_RECORD} only, else -1, and emitted RAW on purpose: {@code lenPlus == 0} is
         * NULL content and {@code lenPlus == 1} is zero-length content
         * ({@code StoreWAL.applySection}), and a decoder that reports a length collapses the two.
         */
        public long lenPlus = -1;
        /** {@code T_RECORD} with {@code lenPlus > 0} only, else null. Never confuse with empty. */
        public byte[] content;

        /**
         * The entry kind as the golden body dump names it.
         *
         * <p>{@link #tag} holds the wire value, but {@code StoreWAL}'s {@code T_*} constants are
         * package-private, so a caller outside {@code org.mapdb.store} cannot compare against them
         * and would have to hard-code 1/2/4 — a transcription of the format that would not move
         * with it. These two methods are the supported way to ask.
         */
        public String kind() {
            switch (tag) {
                case StoreWAL.T_PREALLOC: return "PREALLOC";
                case StoreWAL.T_RECORD: return "RECORD";
                case StoreWAL.T_DELETE: return "DELETE";
                case StoreWAL.T_APPEND: return "APPEND";
                default: throw new AssertionError("entry tag " + tag + " has no name");
            }
        }

        public boolean isRecord() { return tag == StoreWAL.T_RECORD; }
    }

    public static final class Segment {
        public Header header;
        public final List<Section> sections = new ArrayList<>();
        /** Bytes after the last whole section. Nonzero means a torn tail; the C3 sample has none. */
        public long trailing;
    }

    private static void req(boolean ok, String msg) {
        if (!ok) throw new AssertionError(msg);
    }

    private static int be32(byte[] b, int off) { return WalSegmentSet.be32(b, off); }

    private static long be64(byte[] b, int off) { return WalSegmentSet.be64(b, off); }

    /** The 44-byte CRC domain for a section at {@code sectionOffset}: header bytes then the offset. */
    private static CRC32 domain(byte[] seg, long sectionOffset) {
        byte[] d = new byte[WalSegmentSet.SEG_HDR + 8];
        System.arraycopy(seg, 0, d, 0, WalSegmentSet.SEG_HDR);
        for (int i = 0; i < 8; i++) d[WalSegmentSet.SEG_HDR + i] = (byte) (sectionOffset >>> (56 - 8 * i));
        CRC32 crc = new CRC32();
        crc.update(d, 0, d.length);
        return crc;
    }

    /** Decodes one whole segment file. {@code where} names it in every failure message. */
    public static Segment decode(byte[] seg, String where) {
        Segment out = new Segment();
        req(seg.length >= WalSegmentSet.SEG_HDR,
                where + ": " + seg.length + " bytes is shorter than the " + WalSegmentSet.SEG_HDR
                        + "-byte segment header");
        for (int i = 0; i < WalSegmentSet.MAGIC.length; i++)
            req(seg[i] == WalSegmentSet.MAGIC[i], where + ": bad magic at byte " + i);

        Header h = new Header();
        h.version = be32(seg, 8);
        h.flags = be32(seg, 12);
        h.seq = be64(seg, 16);
        h.firstLsn = be64(seg, 24);
        h.headerCrc = be32(seg, WalSegmentSet.SEG_HDR_CRC_LEN);
        req(h.version == WalSegmentSet.FORMAT_VERSION,
                where + ": format version " + h.version + ", expected " + WalSegmentSet.FORMAT_VERSION);
        CRC32 hc = new CRC32();
        hc.update(seg, 0, WalSegmentSet.SEG_HDR_CRC_LEN);
        req(h.headerCrc == (int) hc.getValue(),
                where + ": segment header CRC mismatch, stored " + hex32(h.headerCrc)
                        + " computed " + hex32((int) hc.getValue()));
        out.header = h;

        long pos = WalSegmentSet.SEG_HDR;
        while (seg.length - pos >= StoreWAL.SEC_HDR) {
            int off = (int) pos;
            Section s = new Section();
            s.index = out.sections.size();
            s.offset = pos;
            s.tag = (char) (seg[off] & 0xFF);
            s.lsn = be64(seg, off + 1);
            s.bodyLen = be64(seg, off + 9);
            s.hdrCrc = be32(seg, off + 17);
            s.bodyCrc = be32(seg, off + 21);
            String at = where + " section " + s.index + " at " + s.offset;
            req(TAGS.indexOf(s.tag) >= 0, at + ": unknown section tag " + describeTag(s.tag));
            // Subtraction form, so an adversarial bodyLen cannot overflow the comparison.
            req(s.bodyLen >= 0 && s.bodyLen <= seg.length - pos - StoreWAL.SEC_HDR,
                    at + ": bodyLen " + s.bodyLen + " does not fit the remaining "
                            + (seg.length - pos - StoreWAL.SEC_HDR) + " bytes");

            CRC32 hcrc = domain(seg, pos);
            hcrc.update(seg, off, StoreWAL.SEC_HDR_CRC_LEN);
            req(s.hdrCrc == (int) hcrc.getValue(), at + ": section header CRC mismatch, stored "
                    + hex32(s.hdrCrc) + " computed " + hex32((int) hcrc.getValue()));

            s.body = new byte[(int) s.bodyLen];
            System.arraycopy(seg, off + StoreWAL.SEC_HDR, s.body, 0, s.body.length);
            CRC32 bcrc = domain(seg, pos);
            bcrc.update(s.body, 0, s.body.length);
            req(s.bodyCrc == (int) bcrc.getValue(), at + ": section body CRC mismatch, stored "
                    + hex32(s.bodyCrc) + " computed " + hex32((int) bcrc.getValue()));

            out.sections.add(s);
            pos += StoreWAL.SEC_HDR + s.bodyLen;
        }
        out.trailing = seg.length - pos;
        return out;
    }

    private static String describeTag(char tag) {
        return tag >= 0x20 && tag < 0x7F ? "'" + tag + "'" : "0x" + Integer.toHexString(tag);
    }

    /** The 8-digit lowercase-hex form the pinned golden files use for a CRC32. */
    public static String hex32(int v) { return String.format("%08x", v); }

    // ---------- bodies ----------

    /**
     * The ordered entry stream of an {@code 'S'} or {@code 'C'} body.
     *
     * <p>Mirrors {@code StoreWAL.applySection}'s decode exactly, including its packed-long form
     * (7 bits per byte, high bit SET on the LAST byte — the inverse of the usual convention, and
     * the one place a port is most likely to diverge). {@code 'C'} is decoded identically to
     * {@code 'S'}: {@code StoreWAL} states it "is semantically identical to 'S' and gets no
     * special handling".
     */
    public static List<Entry> entries(Section s, String where) {
        req(s.tag == 'S' || s.tag == 'C',
                where + ": entries() called on a " + describeTag(s.tag) + " section");
        List<Entry> out = new ArrayList<>();
        Cursor in = new Cursor(s.body, where + " section " + s.index);
        while (in.pos < s.body.length) {
            Entry e = new Entry();
            e.tag = in.u8();
            e.recid = in.packed();
            switch (e.tag) {
                case StoreWAL.T_PREALLOC:
                case StoreWAL.T_DELETE:
                    break;
                case StoreWAL.T_RECORD:
                    e.cap = in.packed();
                    e.lenPlus = in.packed();
                    if (e.lenPlus != 0) {
                        long len = e.lenPlus - 1;
                        req(len >= 0 && len <= s.body.length - in.pos,
                                in.at() + ": record length " + len + " does not fit the section body");
                        e.content = in.bytes((int) len);
                    }
                    break;
                case StoreWAL.T_APPEND:
                    // Reachable format, unreachable corpus: no bundle C3 grades carries one, so
                    // there is nothing to check a decode of it against. Refusing names the gap;
                    // decoding it into columns no pinned file has would only look like coverage.
                    throw new AssertionError(in.at() + ": T_APPEND entry — the C3 body dump has no "
                            + "columns for it and no fixture exercises it; extend both together");
                default:
                    throw new AssertionError(in.at() + ": unknown entry tag " + e.tag);
            }
            out.add(e);
        }
        return out;
    }

    /** The two fields of a {@code 'K'} mark body: {@code {cleanedThroughSeq, logStartLsn}}. */
    public static long[] mark(Section s, String where) {
        req(s.tag == 'K', where + ": mark() called on a " + describeTag(s.tag) + " section");
        req(s.bodyLen == MARK_BODY_LEN,
                where + " section " + s.index + ": mark body is " + s.bodyLen + " bytes, not "
                        + MARK_BODY_LEN);
        return new long[]{be64(s.body, 0), be64(s.body, 8)};
    }

    /** A bounds-checked reader over one section body; every overrun names its section. */
    private static final class Cursor {
        final byte[] b;
        final String where;
        int pos;

        Cursor(byte[] b, String where) { this.b = b; this.where = where; }

        String at() { return where + " offset " + pos + " in body"; }

        int u8() {
            req(pos < b.length, at() + ": read past the end of the section body");
            return b[pos++] & 0xFF;
        }

        long packed() {
            long ret = 0;
            int v;
            do {
                v = u8();
                ret = (ret << 7) | (v & 0x7F);
            } while ((v & 0x80) == 0);
            return ret;
        }

        byte[] bytes(int n) {
            req(n <= b.length - pos, at() + ": " + n + " bytes read past the end of the section body");
            byte[] out = new byte[n];
            System.arraycopy(b, pos, out, 0, n);
            pos += n;
            return out;
        }
    }
}
