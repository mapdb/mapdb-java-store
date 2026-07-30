package org.mapdb.store;

import org.junit.After;
import org.junit.Test;
import org.mapdb.DBException;
import org.mapdb.io.DataOutput2;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * WAL on-disk format v2 specifics at the SECTION level: length-prefixed CRC-in-header sections
 * inside a segment, LSN density, mid-log-corruption detection (vs silent torn-tail truncation),
 * and refusal of every pre-v2 image. The segment-set namespace itself — tables N, H and K — is
 * {@link StoreWALSegmentSetTest}.
 */
public class StoreWALFormatV2Test {

    private final List<File> files = new ArrayList<>();

    private File newFile(String tag) {
        try {
            File f = Files.createTempFile("mapdb5-wal-fmt-" + tag, ".wal").toFile();
            f.delete();
            files.add(f);
            return f;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @After public void cleanup() {
        for (File f : files) WalTestKit.deleteStore(f);
        files.clear();
    }

    /** Two records committed in two sections; returns {segLenAfterCommit1, recid1, recid2}. */
    private long[] twoCommits(File f, byte[] v1, byte[] v2) {
        StoreWAL s = new StoreWAL(f);
        try {
            long r1 = s.put(v1, Fixtures.RAW);
            s.commit();
            long len1 = WalTestKit.onlySegment(f).length();
            long r2 = s.put(v2, Fixtures.RAW);
            s.commit();
            return new long[]{len1, r1, r2};
        } finally {
            s.close();
        }
    }

    @Test public void segment_starts_with_magic_version_and_its_own_sequence_number() {
        File f = newFile("magic");
        new StoreWAL(f).close();
        File seg = WalTestKit.onlySegment(f);
        assertEquals("the first segment of a store is 1, so 0 can mean 'no clean mark'",
                WalTestKit.segment(f, 1).getName(), seg.getName());
        byte[] head = WalTestKit.read(seg);
        assertTrue(head.length >= WalTestKit.SEG_HDR);
        assertArrayEquals(new byte[]{'M', 'D', 'B', '5', '.', 'W', 'A', 'L'},
                java.util.Arrays.copyOf(head, 8));
        assertEquals("format version", 3, ByteBuffer.wrap(head).getInt(8));
        assertEquals("flags", 0, ByteBuffer.wrap(head).getInt(12));
        assertEquals("segmentSeq", 1L, ByteBuffer.wrap(head).getLong(16));
        // v3: the header states the LSN its first section will hold, so recovery checks the log's
        // lower bound instead of inferring it. A fresh store's first segment starts at LSN 1.
        assertEquals("firstLsn", 1L, ByteBuffer.wrap(head).getLong(24));
    }

    /**
     * H5. The version must be checked on a header that is otherwise VALID — a doctored version
     * with a stale CRC is just H3 (torn create) and would be swept away as residue, so a test
     * that skips the reseal passes for the wrong reason.
     */
    @Test public void unsupported_version_refuses_to_open() {
        File f = newFile("ver");
        twoCommits(f, Fixtures.payload(1, 1, 4), Fixtures.payload(2, 1, 4));
        File seg = WalTestKit.onlySegment(f);
        byte[] all = WalTestKit.read(seg);
        ByteBuffer.wrap(all).putInt(8, 99);
        WalTestKit.resealSegmentHeader(all);
        WalTestKit.write(seg, all);
        try {
            new StoreWAL(f).close();
            fail("expected refusal of an unknown format version");
        } catch (DBException.DataCorruption expected) {
            assertTrue(expected.getMessage().contains("version"));
        }
    }

    // ================= corruption vs torn tail =================

    @Test public void corrupt_last_section_is_a_torn_tail_and_truncates() {
        File f = newFile("tail");
        byte[] v1 = Fixtures.payload(1, 1, 8);
        long[] r = twoCommits(f, v1, Fixtures.payload(2, 1, 8));
        File seg = WalTestKit.onlySegment(f);
        byte[] all = WalTestKit.read(seg);
        all[all.length - 1] ^= 0x5A; // body byte of the LAST section
        WalTestKit.write(seg, all);

        StoreWAL s = new StoreWAL(f);
        try {
            assertArrayEquals(v1, s.get(r[1], Fixtures.RAW)); // commit 1 survives
            try {
                s.get(r[2], Fixtures.RAW);
                fail("expected GetVoid: torn last section must be discarded");
            } catch (DBException.GetVoid expected) { /* ok */ }
            assertEquals("torn section truncated off", r[0], seg.length());
            // W7: an ACTUAL truncation forces and then rotates, so later appends never reuse
            // the torn segment's checksum domain.
            assertEquals("post-truncation rotate", 2, WalTestKit.segments(f).length);
            assertEquals(WalTestKit.SEG_HDR, WalTestKit.segment(f, 2).length());
        } finally {
            s.close();
        }
    }

    @Test public void corrupt_mid_log_section_raises_data_corruption() {
        File f = newFile("midlog");
        long[] r = twoCommits(f, Fixtures.payload(1, 1, 8), Fixtures.payload(2, 1, 8));
        File seg = WalTestKit.onlySegment(f);
        byte[] all = WalTestKit.read(seg);
        // flip a body byte of the FIRST section (just before commit 1's end); a valid
        // section follows, so this is bit rot, not a torn tail — must NOT be silently dropped
        all[(int) r[0] - 1] ^= 0x5A;
        WalTestKit.write(seg, all);
        try {
            new StoreWAL(f).close();
            fail("expected DataCorruption for mid-log CRC failure");
        } catch (DBException.DataCorruption expected) { /* ok */ }
    }

    @Test public void lsn_regression_with_valid_crc_raises_data_corruption() {
        File f = newFile("lsn");
        long[] r = twoCommits(f, Fixtures.payload(1, 1, 8), Fixtures.payload(2, 1, 8));
        File seg = WalTestKit.onlySegment(f);
        byte[] all = WalTestKit.read(seg);
        // rewrite section 2's LSN to 1 (same as section 1) and recompute its header CRC
        // so the section itself validates: replay must reject the non-increasing sequence
        int sec2 = (int) r[0];
        ByteBuffer.wrap(all).putLong(sec2 + 1, 1L);
        WalTestKit.resealSectionHeader(all, sec2);
        WalTestKit.write(seg, all);
        try {
            new StoreWAL(f).close();
            fail("expected DataCorruption for LSN regression");
        } catch (DBException.DataCorruption expected) { /* ok */ }
    }

    /**
     * S9. LSNs are dense by construction, so a section carrying a FUTURE LSN is as much a
     * missing-sections signal as a regressed one — this is the rule that makes a cleaner whose
     * {@code 'C'} sections vanished wholly detectable, rather than a silent authorization to
     * delete the only surviving copy of that data.
     */
    @Test public void lsn_gap_with_valid_crc_raises_data_corruption() {
        File f = newFile("lsngap");
        long[] r = twoCommits(f, Fixtures.payload(1, 1, 8), Fixtures.payload(2, 1, 8));
        File seg = WalTestKit.onlySegment(f);
        byte[] all = WalTestKit.read(seg);
        int sec2 = (int) r[0];
        ByteBuffer.wrap(all).putLong(sec2 + 1, 7L); // 1 then 7: a gap, not a regression
        WalTestKit.resealSectionHeader(all, sec2);
        WalTestKit.write(seg, all);
        try {
            new StoreWAL(f).close();
            fail("expected DataCorruption for an LSN gap");
        } catch (DBException.DataCorruption expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("consecutive"));
        }
    }

    @Test public void corrupt_mid_log_header_raises_data_corruption() {
        File f = newFile("midhdr");
        long[] r = twoCommits(f, Fixtures.payload(1, 1, 8), Fixtures.payload(2, 1, 8));
        File seg = WalTestKit.onlySegment(f);
        byte[] all = WalTestKit.read(seg);
        // flip the FIRST section's tag byte: its header CRC fails, but the declared
        // bodyLen still points at the valid section 2 — bit rot, must not truncate
        all[WalTestKit.SEG_HDR] ^= 0x01;
        WalTestKit.write(seg, all);
        try {
            new StoreWAL(f).close();
            fail("expected DataCorruption for mid-log header damage (r=" + r[0] + ")");
        } catch (DBException.DataCorruption expected) { /* ok */ }
    }

    /**
     * A CRC-valid section carrying a T_RECORD whose capacity field is garbage (smaller
     * than header+content) must be rejected as corruption at decode time — the cap is
     * applied via an (int) cast and drives allocator arithmetic, so it may never pass
     * through unvalidated even when the section checksum matches.
     */
    @Test public void invalid_capacity_in_crc_valid_section_raises_data_corruption() {
        File f = newFile("badcap");
        StoreWAL s = new StoreWAL(f);
        try {
            long r = s.put(Fixtures.payload(1, 1, 8), Fixtures.RAW); // 16 bytes content
            s.updateWithHeadroom(r, Fixtures.payload(1, 1, 8), Fixtures.RAW, 12); // cap = 32 -> packLong 0xA0
            s.commit();
        } finally {
            s.close();
        }
        File seg = WalTestKit.onlySegment(f);
        byte[] all = WalTestKit.read(seg);
        // body = [T_RECORD][recid pack][cap pack][lenPlus pack][content]; cap byte at body+2
        int sec = WalTestKit.SEG_HDR;
        int capAt = sec + WalTestKit.SEC_HDR + 2;
        if ((all[capAt] & 0xFF) != 0xA0) fail("layout drifted: cap byte=" + (all[capAt] & 0xFF));
        all[capAt] = (byte) 0x90; // cap 32 -> 16 < 4+16: invalid, but re-seal the body CRC
        WalTestKit.resealSectionBody(all, sec);
        WalTestKit.write(seg, all);
        try {
            new StoreWAL(f).close();
            fail("expected DataCorruption for garbage capacity");
        } catch (DBException.DataCorruption expected) { /* ok */ }
    }

    /**
     * A legitimately torn LAST section may contain arbitrary user bytes — including a
     * copied/crafted WAL section image — and its damaged header's bodyLen may point
     * right at it. That must stay a torn tail (truncate, reopen fine) unless the image
     * carries exactly the next expected LSN; anything else would turn a recoverable
     * crash into a refuse-to-open.
     *
     * <p>The embedded image is built for the offset it actually occupies, so it really is a
     * fully valid section under the §6.2 domain — otherwise the test would pass on the CRC
     * check and never reach the LSN rule it exists to pin.
     */
    @Test public void torn_last_section_with_embedded_fake_section_still_truncates() {
        File f = newFile("fake-embed");
        byte[] v1 = Fixtures.payload(1, 1, 8);
        long[] r = twoCommits(f, v1, Fixtures.payload(2, 1, 8)); // sections lsn=1, lsn=2
        File seg = WalTestKit.onlySegment(f);
        byte[] all = WalTestKit.read(seg);
        byte[] segHeader = java.util.Arrays.copyOf(all, WalTestKit.SEG_HDR);

        // append a "torn" third section: header with bodyLen=0 whose hdrCrc is broken,
        // followed by payload bytes that ARE a fully valid section image with a future
        // (but not exactly-next) LSN. lastLsn at the damage point is 2 => exact-next is 4.
        ByteBuffer damaged = ByteBuffer.allocate(WalTestKit.SEC_HDR);
        damaged.put((byte) 'S').putLong(3L).putLong(0L).putInt(0xDEAD).putInt(0xBEEF); // bad hdrCrc
        byte[] fake = WalTestKit.section(segHeader, all.length + WalTestKit.SEC_HDR, 'S', 99, new byte[0]);
        byte[] torn = WalTestKit.concat(all, damaged.array(), fake);
        WalTestKit.write(seg, torn);

        StoreWAL s = new StoreWAL(f); // must open: torn tail, NOT DataCorruption
        try {
            assertArrayEquals(v1, s.get(r[1], Fixtures.RAW));
            assertEquals("torn tail truncated at the damaged section", all.length, (int) seg.length());
        } finally {
            s.close();
        }
    }

    // ================= recid reuse across delete + replay =================

    @Test public void deleted_then_reused_recid_is_not_double_allocated_after_reopen() {
        File f = newFile("reuse");
        byte[] a = Fixtures.payload(1, 1, 8);
        byte[] b = Fixtures.payload(2, 2, 8);
        long x, x2;
        StoreWAL s = new StoreWAL(f);
        try {
            x = s.put(a, Fixtures.RAW);
            s.commit();
            s.delete(x, Fixtures.RAW);
            s.commit();
            x2 = s.put(b, Fixtures.RAW); // allocator legitimately reuses the freed recid
            s.commit();
            assertEquals("precondition: recid reused", x, x2);
        } finally {
            s.close();
        }

        StoreWAL s2 = new StoreWAL(f);
        try {
            assertArrayEquals(b, s2.get(x, Fixtures.RAW));
            // the killer: replay pushed x onto the free list at the T_DELETE and must
            // NOT hand it out again now that a later section revived it
            for (int i = 0; i < 8; i++) {
                long fresh = s2.put(Fixtures.payload(9, i, 4), Fixtures.RAW);
                assertTrue("live recid " + x + " re-allocated as " + fresh, fresh != x);
            }
            s2.commit();
            assertArrayEquals("live record clobbered by re-allocation", b, s2.get(x, Fixtures.RAW));
            s2.verify();
        } finally {
            s2.close();
        }
    }

    @Test public void deleted_then_repreallocated_recid_survives_reopen() {
        File f = newFile("reuse-p");
        byte[] fill = Fixtures.payload(3, 3, 8);
        long x, x2;
        StoreWAL s = new StoreWAL(f);
        try {
            x = s.put(Fixtures.payload(1, 1, 8), Fixtures.RAW);
            s.commit();
            s.delete(x, Fixtures.RAW);
            s.commit();
            x2 = s.preallocate(); // reuses the freed recid, committed as T_PREALLOC
            s.commit();
            assertEquals("precondition: recid reused for prealloc", x, x2);
        } finally {
            s.close();
        }

        StoreWAL s2 = new StoreWAL(f);
        try {
            // the re-preallocation was committed: filling it must work, not GetVoid
            s2.update(x, fill, Fixtures.RAW);
            s2.commit();
            assertArrayEquals(fill, s2.get(x, Fixtures.RAW));
            s2.verify();
        } finally {
            s2.close();
        }
    }

    // ================= pre-v2 images are refused, never migrated =================

    /** Legacy (milestone-1) framing: entries, then COMMIT tag (8) + CRC32 over the section bytes. */
    private static byte[] legacySection(long recid, int cap, byte[] content) {
        DataOutput2 out = new DataOutput2(64);
        out.writeByte(2); // T_RECORD
        out.packLong(recid);
        out.packLong(cap);
        out.packLong(content.length + 1);
        out.write(content);
        CRC32 crc = new CRC32();
        crc.update(out.buf, 0, out.pos);
        out.writeByte(8); // legacy T_COMMIT
        out.writeInt((int) crc.getValue());
        return java.util.Arrays.copyOf(out.buf, out.pos);
    }

    /**
     * N6. A v1 log is a single file named {@code <db>.wal}, and it is a DIFFERENT format, not an
     * older dialect: {@code T_APPEND} lost its base field and the log lost its segment identity.
     * Starting a fresh segment set beside it would strand every committed transaction in it, so
     * its mere presence refuses the open — whatever its contents.
     */
    @Test public void v1_single_file_log_is_refused_not_migrated() {
        File f = newFile("v1");
        byte[] c1 = Fixtures.payload(1, 1, 8);
        WalTestKit.write(new File(f.getPath() + ".wal"),
                WalTestKit.concat(legacySection(1, 32, c1), legacySection(2, 48, Fixtures.payload(2, 1, 24))));
        try {
            new StoreWAL(f).close();
            fail("expected refusal of a v1 single-file log");
        } catch (DBException.DataCorruption expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("no migration"));
        } finally {
            new File(f.getPath() + ".wal").delete();
        }
    }
}
