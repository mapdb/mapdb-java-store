package org.mapdb.store;

import org.junit.After;
import org.junit.Test;
import org.mapdb.btree.BTreeMap;
import org.mapdb.btree.BufferTreeMap;
import org.mapdb.ser.LongFormat;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * WHERE the WAL bytes go, by entry type — the diagnostic that decides how much of R2's
 * claim is real for each workload.
 *
 * <p>{@link WalWriteAmplificationTest} measures total device bytes per op; this one splits
 * that total into {@code T_RECORD} (full record images) vs {@code T_APPEND} (delta bytes) vs
 * framing overhead. The distinction is load-bearing: bytes that are already {@code T_APPEND}
 * are ALREADY delta-granular, so a delta-aware WAL cannot shrink them. Only the
 * {@code T_RECORD} share is addressable — either by not collapsing appends into images
 * (design §2.4) or, for a collection that rewrites whole nodes, by adopting R1 pages.
 *
 * <p>Parses the v1 format directly (magic + 25-byte section header + packLong-framed
 * entries, {@code StoreWAL.java:30-51}) rather than instrumenting the writer, so it measures
 * what actually reached the device.
 */
public class WalCompositionTest {

    private static final int NODE_SIZE = 256;
    private static final int BUFFER_BYTES = 4096;

    private static final int SEG_HDR = WalTestKit.SEG_HDR;
    private static final int SEC_HDR = WalTestKit.SEC_HDR;
    private static final int T_PREALLOC = 1, T_RECORD = 2, T_APPEND = 3, T_DELETE = 4;

    private final List<File> files = new ArrayList<>();

    private File newFile(String tag) {
        try {
            File f = Files.createTempFile("mapdb5-walcomp-" + tag, ".wal").toFile();
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

    // ------------------------------------------------------------------ v1 log parser

    /** Byte totals attributed to each entry class, plus framing. */
    static final class Composition {
        long sections, sectionHeaderBytes;
        long recordEntries, recordPayload, recordFraming;
        long appendEntries, appendPayload, appendFraming;
        long preallocEntries, deleteEntries, smallEntryBytes;

        long total() {
            return sectionHeaderBytes + recordPayload + recordFraming
                    + appendPayload + appendFraming + smallEntryBytes;
        }
    }

    /** Length of a packLong encoding (7 bits/byte, terminator has the high bit SET). */
    private static int packLen(long v) {
        int n = 1;
        long x = v >>> 7;
        while (x != 0) { n++; x >>>= 7; }
        return n;
    }

    /** Parses from {@code fromOffset} to EOF, attributing every byte. */
    private static Composition parse(File f, long fromOffset) throws IOException {
        // These harnesses stay well inside one segment (default rollover is 64 MiB), and
        // onlySegment() fails loudly rather than silently measuring a fraction of the log
        // if that ever stops being true.
        byte[] all = WalTestKit.read(WalTestKit.onlySegment(f));
        ByteBuffer bb = ByteBuffer.wrap(all).order(ByteOrder.BIG_ENDIAN);
        Composition c = new Composition();
        int pos = (int) Math.max(fromOffset, SEG_HDR);
        while (pos + SEC_HDR <= all.length) {
            int tag = all[pos] & 0xFF;
            long bodyLen = bb.getLong(pos + 9);
            if (bodyLen < 0 || pos + SEC_HDR + bodyLen > all.length) break;   // torn tail
            c.sections++;
            c.sectionHeaderBytes += SEC_HDR;
            int bodyStart = pos + SEC_HDR;
            int bodyEnd = (int) (bodyStart + bodyLen);
            if (tag == 'C') {
                // checkpoint snapshot: attribute wholesale, it is a full-image rewrite
                c.recordPayload += bodyLen;
            } else {
                int p = bodyStart;
                while (p < bodyEnd) {
                    int t = all[p] & 0xFF;
                    p++;
                    long[] r = new long[1];
                    p = unpack(all, p, r);
                    long recid = r[0];
                    switch (t) {
                        case T_RECORD -> {
                            p = unpack(all, p, r);
                            long cap = r[0];
                            p = unpack(all, p, r);
                            long lenPlus = r[0];
                            long len = lenPlus == 0 ? 0 : lenPlus - 1;
                            c.recordEntries++;
                            c.recordPayload += len;
                            c.recordFraming += 1 + packLen(recid) + packLen(cap) + packLen(lenPlus);
                            p += len;
                        }
                        case T_APPEND -> {
                            p = unpack(all, p, r);
                            long baseDelta = r[0]; // v2: LSN of the base image, as a delta
                            p = unpack(all, p, r);
                            long len = r[0];
                            c.appendEntries++;
                            c.appendPayload += len;
                            c.appendFraming += 1 + packLen(recid) + packLen(baseDelta) + packLen(len);
                            p += len;
                        }
                        case T_PREALLOC -> { c.preallocEntries++; c.smallEntryBytes += 1 + packLen(recid); }
                        case T_DELETE -> { c.deleteEntries++; c.smallEntryBytes += 1 + packLen(recid); }
                        default -> throw new IllegalStateException("bad tag " + t + " at " + p);
                    }
                }
            }
            pos = bodyEnd;
        }
        return c;
    }

    private static int unpack(byte[] a, int p, long[] out) {
        long ret = 0;
        int v;
        do { v = a[p++] & 0xFF; ret = (ret << 7) | (v & 0x7F); } while ((v & 0x80) == 0);
        out[0] = ret;
        return p;
    }

    // ------------------------------------------------------------------ report

    private void print(String title, Map<String, Composition> bySubject) {
        StringBuilder sb = new StringBuilder("\n=== " + title + " ===\n");
        sb.append(String.format("%-30s %10s %12s %12s %12s %10s%n",
                "subject", "sections", "IMAGE bytes", "DELTA bytes", "framing", "image %"));
        bySubject.forEach((k, c) -> {
            long framing = c.sectionHeaderBytes + c.recordFraming + c.appendFraming + c.smallEntryBytes;
            double imagePct = 100.0 * c.recordPayload / Math.max(1, c.total());
            sb.append(String.format("%-30s %10d %12d %12d %12d %9.1f%%%n",
                    k, c.sections, c.recordPayload, c.appendPayload, framing, imagePct));
        });
        System.out.println(sb);
    }

    // ------------------------------------------------------------------ subjects

    @Test
    public void where_the_bytes_go_random_updates() throws Exception {
        final int entries = 20_000;
        final int ops = 5_000;
        Map<String, Composition> out = new LinkedHashMap<>();

        {
            File f = newFile("bt");
            StoreWAL s = new StoreWAL(f, false, true);
            long from;
            try {
                BufferTreeMap<Long, Long> map =
                        BufferTreeMap.create(s, LongFormat.INSTANCE, LongFormat.INSTANCE, NODE_SIZE, BUFFER_BYTES);
                for (long i = 0; i < entries; i++) map.put(i, i);
                s.commit();
                s.checkpoint();
                from = WalTestKit.onlySegment(f).length();
                Random rnd = new Random(42);
                for (int i = 0; i < ops; i++) { map.put((long) rnd.nextInt(entries), (long) i); s.commit(); }
            } finally { s.close(); }
            out.put("BufferTreeMap (R1 append)", parse(f, from));
        }
        {
            File f = newFile("plain");
            StoreWAL s = new StoreWAL(f, false, true);
            long from;
            try {
                BTreeMap<Long, Long> map =
                        BTreeMap.create(s, LongFormat.INSTANCE, LongFormat.INSTANCE, NODE_SIZE);
                for (long i = 0; i < entries; i++) map.put(i, i);
                s.commit();
                s.checkpoint();
                from = WalTestKit.onlySegment(f).length();
                Random rnd = new Random(42);
                for (int i = 0; i < ops; i++) { map.put((long) rnd.nextInt(entries), (long) i); s.commit(); }
            } finally { s.close(); }
            out.put("BTreeMap (full node image)", parse(f, from));
        }
        {
            File f = newFile("collapse");
            StoreWAL s = new StoreWAL(f, false, true);
            long from;
            try {
                byte[] base = new byte[4000];
                long recid = s.put(base, Fixtures.RAW);
                s.commit();
                s.checkpoint();
                from = WalTestKit.onlySegment(f).length();
                byte[] d = new byte[32];
                for (int i = 0; i < 2000; i++) {
                    base[0] = (byte) i;
                    s.updateWithHeadroom(recid, base, Fixtures.RAW, 96);
                    d[0] = (byte) i;
                    s.append(recid, d, 0, d.length);
                    s.commit();
                }
            } finally { s.close(); }
            out.put("record: rewrite+append", parse(f, from));
        }
        print("WAL byte composition (IMAGE = T_RECORD payload, DELTA = T_APPEND payload)", out);
    }

    /**
     * Leaf headroom sweep against DEVICE BYTES — the metric {@code DEFAULT_LEAF_HEADROOM}
     * was never swept on. Its javadoc records a sweep on put throughput, live footprint and
     * read shapes (BufferTreeMap.java:178-188) and settled on 128 bytes, which makes leaves
     * consolidate often. Every consolidation is a full node image in the WAL, so the R1
     * default may be spending device bytes to buy CPU — precisely the trade R2 exists to
     * re-examine. This prints the image/delta split per headroom.
     */
    @Test
    public void leaf_headroom_vs_device_bytes() throws Exception {
        final int entries = 20_000;
        final int ops = 5_000;
        Map<String, Composition> out = new LinkedHashMap<>();

        for (int headroom : new int[]{0, 128, 512, 2048, 8192}) {
            File f = newFile("lh-" + headroom);
            StoreWAL s = new StoreWAL(f, false, true);
            long from;
            try {
                BufferTreeMap<Long, Long> map = BufferTreeMap.create(
                        s, LongFormat.INSTANCE, LongFormat.INSTANCE, NODE_SIZE, BUFFER_BYTES, headroom);
                for (long i = 0; i < entries; i++) map.put(i, i);
                s.commit();
                s.checkpoint();
                from = WalTestKit.onlySegment(f).length();
                Random rnd = new Random(42);
                for (int i = 0; i < ops; i++) { map.put((long) rnd.nextInt(entries), (long) i); s.commit(); }
            } finally { s.close(); }
            Composition c = parse(f, from);
            out.put("leafHeadroom=" + headroom
                    + String.format(" (%.0f B/op)", c.total() / (double) ops), c);
        }
        print("leaf headroom vs device bytes — BufferTreeMap, 5000 random updates", out);
    }
}
