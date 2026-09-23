package org.mapdb.store;

import org.mapdb.TmpFiles;
import org.mapdb.btree.BufferTreeMap;
import org.mapdb.ser.LongFormat;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Random;

/**
 * S4 measurement: {@link BufferTreeMap} leaf headroom against device bytes,
 * put throughput, and {@link Store#getCurrentSize()} on one workload.
 *
 * <p>Not a JUnit test and not a dependency of {@code mvn test}. It does not
 * write {@link BufferTreeMap#DEFAULT_LEAF_HEADROOM}. The device window is the
 * July 2026 protocol ({@code WalCompositionTest.leaf_headroom_vs_device_bytes}):
 * maxNodeSize 256, bufferBytes 4096, 20_000 sequential puts, one commit, a
 * checkpoint, then 5_000 random updates committed one at a time. The foot
 * window is the same update protocol at maxNodeSize 32 and 100_000 keys, where
 * a 1 MiB page can no longer hide the reserved leaf capacity.
 *
 * <p>Run: {@code java -cp target/classes:target/test-classes org.mapdb.store.LeafHeadroomSweep}
 * Optional args: {@code device}, {@code foot}, or {@code both} (default), then
 * one headroom from {@code 0, 128, 512, 2048, 8192}.
 */
public final class LeafHeadroomSweep {

    private static final int[] HEADROOMS = {0, 128, 512, 2048, 8192};
    private static final int BUFFER_BYTES = 4096;
    private static final int REPEATS = 3;

    /** July device-byte protocol. maxNodeSize matches {@code WalCompositionTest}. */
    private static final Window DEVICE = new Window("device", 256, 20_000, 5_000);
    /**
     * Same updates, smaller nodes, more keys. Leaf headroom is reserved store
     * capacity; 20_000 keys at node 256 stay inside one or two 1 MiB pages.
     */
    private static final Window FOOT = new Window("foot", 32, 100_000, 5_000);

    private static final int SEG_HDR = WalTestKit.SEG_HDR;
    private static final int SEC_HDR = WalTestKit.SEC_HDR;
    private static final int T_PREALLOC = 1, T_RECORD = 2, T_APPEND = 3, T_DELETE = 4;

    public static void main(String[] args) throws Exception {
        if (BufferTreeMap.DEFAULT_LEAF_HEADROOM != 128) {
            throw new IllegalStateException(
                    "DEFAULT_LEAF_HEADROOM is " + BufferTreeMap.DEFAULT_LEAF_HEADROOM + ", expected 128");
        }
        String which = args.length == 0 ? "both" : args[0];
        int only = -1;
        if (args.length > 2) {
            System.err.println("usage: LeafHeadroomSweep [device|foot|both] [headroom]");
            System.exit(2);
        }
        if (args.length == 2) {
            only = Integer.parseInt(args[1]);
            boolean known = false;
            for (int h : HEADROOMS) if (h == only) known = true;
            if (!known) throw new IllegalArgumentException("headroom not in the sweep: " + only);
        }
        if (!which.equals("device") && !which.equals("foot") && !which.equals("both")) {
            System.err.println("usage: LeafHeadroomSweep [device|foot|both] [headroom]");
            System.exit(2);
        }
        System.out.println("default_leaf_headroom\t" + BufferTreeMap.DEFAULT_LEAF_HEADROOM);
        System.out.println("buffer_bytes\t" + BUFFER_BYTES);
        System.out.println("repeats\t" + REPEATS);
        System.out.println("java\t" + System.getProperty("java.version"));
        System.out.println("row\twindow\theadroom\trepeat\tfill_ns\tupdate_ns\tget_ns"
                + "\tlive_after_ckpt\tlive_after_updates\twal_bytes\timage_bytes\tdelta_bytes"
                + "\tframing\tbytes_per_op");
        if (which.equals("device") || which.equals("both")) runWindow(DEVICE, only);
        if (which.equals("foot") || which.equals("both")) runWindow(FOOT, only);
    }

    private static void runWindow(Window w, int only) throws Exception {
        for (int headroom : HEADROOMS) {
            if (only >= 0 && headroom != only) continue;
            measure(w, headroom, -1); // warmup, discarded
            long[] wal = new long[REPEATS];
            for (int r = 0; r < REPEATS; r++) {
                Sample s = measure(w, headroom, r);
                wal[r] = s.walBytes;
                System.out.printf("row\t%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%.1f%n",
                        w.name, headroom, r, s.fillNs, s.updateNs, s.getNs,
                        s.liveAfterCkpt, s.liveAfterUpdates, s.walBytes, s.imageBytes, s.deltaBytes,
                        s.framing, s.walBytes / (double) w.ops);
            }
            for (int r = 1; r < REPEATS; r++) {
                if (wal[r] != wal[0]) {
                    throw new IllegalStateException(
                            w.name + " headroom " + headroom + " wal bytes differ: " + Arrays.toString(wal));
                }
            }
        }
    }

    private static Sample measure(Window w, int headroom, int repeat) throws Exception {
        File f = TmpFiles.tempFile("mapdb-lh-" + w.name + "-" + headroom + "-" + repeat, ".db");
        f.delete();
        StoreWAL s = new StoreWAL(f, false, true);
        try {
            BufferTreeMap<Long, Long> map = BufferTreeMap.create(
                    s, LongFormat.INSTANCE, LongFormat.INSTANCE, w.maxNode, BUFFER_BYTES, headroom);
            long t0 = System.nanoTime();
            for (long i = 0; i < w.entries; i++) map.put(i, i);
            s.commit();
            long fillNs = System.nanoTime() - t0;
            s.checkpoint();
            long liveAfterCkpt = s.getCurrentSize();
            long from = WalTestKit.onlySegment(f).length();

            Random rnd = new Random(42);
            long t1 = System.nanoTime();
            for (int i = 0; i < w.ops; i++) {
                map.put((long) rnd.nextInt(w.entries), (long) i);
                s.commit();
            }
            long updateNs = System.nanoTime() - t1;
            long liveAfterUpdates = s.getCurrentSize();
            long mid = WalTestKit.onlySegment(f).length();

            Random gets = new Random(99);
            long t2 = System.nanoTime();
            long sink = 0;
            for (int i = 0; i < w.ops; i++) {
                Long v = map.get((long) gets.nextInt(w.entries));
                if (v == null) throw new IllegalStateException("missing key");
                sink += v;
            }
            long getNs = System.nanoTime() - t2;
            if (sink == Long.MIN_VALUE) throw new IllegalStateException("unreachable");

            s.close();
            s = null;
            long end = WalTestKit.onlySegment(f).length();
            if (end != mid) {
                throw new IllegalStateException("gets changed the log: " + mid + " -> " + end);
            }
            Composition c = parse(f, from);
            long walBytes = end - from;
            if (c.total() != walBytes) {
                throw new IllegalStateException(
                        "parser total " + c.total() + " != file growth " + walBytes);
            }
            long framing = c.sectionHeaderBytes + c.recordFraming + c.appendFraming + c.smallEntryBytes;
            return new Sample(fillNs, updateNs, getNs, liveAfterCkpt, liveAfterUpdates,
                    walBytes, c.recordPayload, c.appendPayload, framing);
        } finally {
            if (s != null) s.close();
            TmpFiles.delete(f);
        }
    }

    private record Window(String name, int maxNode, int entries, int ops) {}

    private record Sample(long fillNs, long updateNs, long getNs,
                          long liveAfterCkpt, long liveAfterUpdates,
                          long walBytes, long imageBytes, long deltaBytes, long framing) {}

    /** Byte totals attributed to each WAL entry class, plus framing. */
    private static final class Composition {
        long sectionHeaderBytes;
        long recordPayload, recordFraming;
        long appendPayload, appendFraming;
        long smallEntryBytes;

        long total() {
            return sectionHeaderBytes + recordPayload + recordFraming
                    + appendPayload + appendFraming + smallEntryBytes;
        }
    }

    /** Same v1 walk as {@code WalCompositionTest.parse}. */
    private static Composition parse(File f, long fromOffset) throws IOException {
        byte[] all = WalTestKit.read(WalTestKit.onlySegment(f));
        ByteBuffer bb = ByteBuffer.wrap(all).order(ByteOrder.BIG_ENDIAN);
        Composition c = new Composition();
        int pos = (int) Math.max(fromOffset, SEG_HDR);
        while (pos + SEC_HDR <= all.length) {
            int tag = all[pos] & 0xFF;
            long bodyLen = bb.getLong(pos + 9);
            if (bodyLen < 0 || pos + SEC_HDR + bodyLen > all.length) break;
            c.sectionHeaderBytes += SEC_HDR;
            int bodyStart = pos + SEC_HDR;
            int bodyEnd = (int) (bodyStart + bodyLen);
            if (tag == 'C') {
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
                            c.recordPayload += len;
                            c.recordFraming += 1 + packLen(recid) + packLen(cap) + packLen(lenPlus);
                            p += len;
                        }
                        case T_APPEND -> {
                            p = unpack(all, p, r);
                            long baseDelta = r[0];
                            p = unpack(all, p, r);
                            long len = r[0];
                            c.appendPayload += len;
                            c.appendFraming += 1 + packLen(recid) + packLen(baseDelta) + packLen(len);
                            p += len;
                        }
                        case T_PREALLOC, T_DELETE -> c.smallEntryBytes += 1 + packLen(recid);
                        default -> throw new IllegalStateException("bad tag " + t + " at " + p);
                    }
                }
            }
            pos = bodyEnd;
        }
        return c;
    }

    private static int packLen(long v) {
        int n = 1;
        long x = v >>> 7;
        while (x != 0) { n++; x >>>= 7; }
        return n;
    }

    private static int unpack(byte[] a, int p, long[] out) {
        long ret = 0;
        int v;
        do { v = a[p++] & 0xFF; ret = (ret << 7) | (v & 0x7F); } while ((v & 0x80) == 0);
        out[0] = ret;
        return p;
    }
}
