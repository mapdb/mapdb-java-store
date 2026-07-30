package org.mapdb.store;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Byte-level helpers for the WAL <b>segment set</b>.
 *
 * <p>Every crash/corruption test in this package used to do surgery on one file at a known
 * offset. Under v2 the log is {@code <db>.wal.<16 hex>}, section CRCs are domain-separated by
 * {@code segmentHeader(28) || be64(sectionOffset)}, and "delete the log" means removing a set of
 * files. Those three facts are easy to get subtly wrong in a test — a section resealed with the
 * v1 CRC recipe simply fails its checksum, and the test then passes for the wrong reason. So the
 * recipe lives here once.
 */
public final class WalTestKit {

    private WalTestKit() { }

    static final int SEG_HDR = WalSegmentSet.SEG_HDR;
    static final int SEC_HDR = StoreWAL.SEC_HDR;

    // ---------- the namespace ----------

    static File segment(File base, long seq) {
        File abs = base.getAbsoluteFile();
        return new File(abs.getParentFile(), abs.getName() + ".wal." + String.format("%016x", seq));
    }

    /** Every segment of this store, ascending by sequence number. */
    public static File[] segments(File base) {
        File abs = base.getAbsoluteFile();
        File dir = abs.getParentFile();
        String prefix = abs.getName() + ".wal.";
        String[] names = dir == null ? null : dir.list();
        List<File> out = new ArrayList<>();
        if (names != null) {
            for (String n : names) {
                if (n.startsWith(prefix) && n.length() == prefix.length() + 16) out.add(new File(dir, n));
            }
        }
        out.sort(Comparator.comparing(File::getName));
        return out.toArray(new File[0]);
    }

    /** The only segment, asserting there is exactly one. */
    static File onlySegment(File base) {
        File[] all = segments(base);
        if (all.length != 1) throw new AssertionError("expected 1 segment, found " + all.length
                + ": " + Arrays.toString(all));
        return all[0];
    }

    /** Total bytes of the segment set — the log's size on disk. */
    public static long logBytes(File base) {
        long n = 0;
        for (File f : segments(base)) n += f.length();
        return n;
    }

    /** Removes the store and every sidecar it owns, so the next open sees a fresh namespace. */
    public static void deleteStore(File base) {
        base.delete();
        new File(base.getPath() + ".lock").delete();
        for (File f : segments(base)) f.delete();
    }

    static byte[] read(File f) {
        try {
            return Files.readAllBytes(f.toPath());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static void write(File f, byte[] bytes) {
        try {
            Files.write(f.toPath(), bytes);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // ---------- segment headers ----------

    /** A valid segment header for {@code seq}, stating that its first section holds LSN 1. */
    static byte[] segmentHeader(long seq) {
        return segmentHeader(seq, 1);
    }

    /**
     * A valid segment header for {@code seq} stating that its first section holds
     * {@code firstLsn}. R4 checks that number against the newest mark and against the neighbouring
     * segments, so a hand-built image has to state it as the writer would have.
     */
    static byte[] segmentHeader(long seq, long firstLsn) {
        byte[] h = new byte[SEG_HDR];
        System.arraycopy(WalSegmentSet.MAGIC, 0, h, 0, 8);
        WalSegmentSet.putBe32(h, 8, WalSegmentSet.FORMAT_VERSION);
        WalSegmentSet.putBe32(h, 12, 0);
        WalSegmentSet.putBe64(h, 16, seq);
        WalSegmentSet.putBe64(h, 24, firstLsn);
        resealSegmentHeader(h);
        return h;
    }

    /** Recomputes {@code headerCrc} in place, so a doctored header stays CRC-valid (H5-H7). */
    static void resealSegmentHeader(byte[] seg) {
        CRC32 c = new CRC32();
        c.update(seg, 0, WalSegmentSet.SEG_HDR_CRC_LEN);
        WalSegmentSet.putBe32(seg, WalSegmentSet.SEG_HDR_CRC_LEN, (int) c.getValue());
    }

    // ---------- sections ----------

    /**
     * Seeds a CRC with the §6.2 domain separator for a section at {@code off} of this segment:
     * the 28 header bytes verbatim followed by the big-endian section offset.
     */
    private static CRC32 domain(byte[] seg, int off) {
        CRC32 c = new CRC32();
        c.update(seg, 0, SEG_HDR);
        byte[] o = new byte[8];
        WalSegmentSet.putBe64(o, 0, off);
        c.update(o, 0, 8);
        return c;
    }

    static long bodyLen(byte[] seg, int off) {
        return ByteBuffer.wrap(seg).getLong(off + 9);
    }

    static int sectionEnd(byte[] seg, int off) {
        return (int) (off + SEC_HDR + bodyLen(seg, off));
    }

    /** Offset of section {@code n} (0-based) inside a segment image. */
    static int sectionOffset(byte[] seg, int n) {
        int off = SEG_HDR;
        for (int i = 0; i < n; i++) off = sectionEnd(seg, off);
        return off;
    }

    /** Number of whole sections in a segment image. */
    static int sectionCount(byte[] seg) {
        int off = SEG_HDR, n = 0;
        while (off + SEC_HDR <= seg.length && sectionEnd(seg, off) <= seg.length) {
            off = sectionEnd(seg, off);
            n++;
        }
        return n;
    }

    static char tagOf(byte[] seg, int off) { return (char) (seg[off] & 0xFF); }

    static long lsnOf(byte[] seg, int off) { return ByteBuffer.wrap(seg).getLong(off + 1); }

    /** Recomputes the section's header CRC in place — for tests that doctor tag/lsn/bodyLen. */
    static void resealSectionHeader(byte[] seg, int off) {
        CRC32 c = domain(seg, off);
        c.update(seg, off, StoreWAL.SEC_HDR_CRC_LEN);
        ByteBuffer.wrap(seg).putInt(off + 17, (int) c.getValue());
    }

    /** Recomputes the section's body CRC in place — for tests that doctor entry bytes. */
    static void resealSectionBody(byte[] seg, int off) {
        CRC32 c = domain(seg, off);
        int start = off + SEC_HDR;
        c.update(seg, start, (int) bodyLen(seg, off));
        ByteBuffer.wrap(seg).putInt(off + 21, (int) c.getValue());
    }

    /**
     * A fully valid section, checksummed for {@code off} of the segment whose header is
     * {@code segHeader}. Moving the returned bytes to any other offset or segment invalidates
     * them — which is exactly the property fixture C7 tests.
     */
    static byte[] section(byte[] segHeader, int off, char tag, long lsn, byte[] body) {
        byte[] out = new byte[SEC_HDR + body.length];
        ByteBuffer bb = ByteBuffer.wrap(out);
        bb.put((byte) tag).putLong(lsn).putLong(body.length);
        System.arraycopy(body, 0, out, SEC_HDR, body.length);
        CRC32 h = domain(segHeader, off);
        h.update(out, 0, StoreWAL.SEC_HDR_CRC_LEN);
        CRC32 b = domain(segHeader, off);
        b.update(body, 0, body.length);
        bb.putInt(17, (int) h.getValue()).putInt(21, (int) b.getValue());
        return out;
    }

    /**
     * A {@code 'K'} clean mark authorizing removal of every segment at or below {@code through},
     * and attesting that the retained log then begins at {@code logStartLsn}.
     */
    static byte[] mark(byte[] segHeader, int off, long lsn, long through, long logStartLsn) {
        byte[] body = new byte[16];
        WalSegmentSet.putBe64(body, 0, through);
        WalSegmentSet.putBe64(body, 8, logStartLsn);
        return section(segHeader, off, 'K', lsn, body);
    }

    static byte[] concat(byte[]... parts) {
        int n = 0;
        for (byte[] p : parts) n += p.length;
        byte[] out = new byte[n];
        int at = 0;
        for (byte[] p : parts) {
            System.arraycopy(p, 0, out, at, p.length);
            at += p.length;
        }
        return out;
    }
}
