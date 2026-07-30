package org.mapdb.store;

import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Streaming-replay specifics of {@link StoreWAL}: replay decodes the log through a
 * fixed-size window with long positions, so every framing element (tag, packed longs,
 * payloads, the 4-byte CRC) must decode correctly when split across window refills,
 * and torn tails must truncate to the newest surviving commit regardless of where the
 * cut lands relative to a window edge. Adapts mapdb3's {@code WALTruncate} /
 * {@code cut_broken_end} invariants to this engine's WAL framing.
 */
public class StoreWALStreamReplayTest {

    private final List<File> files = new ArrayList<>();

    private File newFile(String tag) {
        try {
            File f = Files.createTempFile("mapdb-wal-stream-" + tag, ".wal").toFile();
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

    private static TreeMap<Long, byte[]> snapshot(Store s) {
        TreeMap<Long, byte[]> snap = new TreeMap<>();
        PrimitiveIterator.OfLong it = s.getAllRecids();
        while (it.hasNext()) {
            long r = it.nextLong();
            snap.put(r, s.get(r, Fixtures.RAW));
        }
        return snap;
    }

    private static void assertState(Store s, TreeMap<Long, byte[]> snap) {
        TreeMap<Long, byte[]> actual = snapshot(s);
        assertEquals("recid set differs", snap.keySet(), actual.keySet());
        for (Long r : snap.keySet()) {
            assertArrayEquals("content differs at recid=" + r, snap.get(r), actual.get(r));
        }
    }

    private final List<Long> commitLens = new ArrayList<>();
    private final List<TreeMap<Long, byte[]>> commitSnaps = new ArrayList<>();

    /** Committed workload with payload sizes drawn from {@code sizes}; records lens + oracles. */
    private void buildWorkload(File f, long seed, int commits, int[] sizes) {
        commitLens.clear();
        commitSnaps.clear();
        Random rnd = new Random(seed);
        StoreWAL s = new StoreWAL(f);
        try {
            List<Long> live = new ArrayList<>();
            int version = 1;
            for (int c = 0; c < commits; c++) {
                int ops = 2 + rnd.nextInt(3);
                for (int i = 0; i < ops; i++) {
                    int size = sizes[rnd.nextInt(sizes.length)];
                    int pick = live.isEmpty() ? 0 : rnd.nextInt(4);
                    switch (pick) {
                        case 0 -> live.add(s.put(Fixtures.payload(live.size(), version++, size), Fixtures.RAW));
                        case 1 -> s.updateWithHeadroom(live.get(rnd.nextInt(live.size())),
                                Fixtures.payload(c, version++, size), Fixtures.RAW, 32);
                        case 2 -> {
                            long r = live.get(rnd.nextInt(live.size()));
                            s.updateWithHeadroom(r, Fixtures.payload(c, version++, 2), Fixtures.RAW, size + 16);
                            s.append(r, Fixtures.payload(c, version++, Math.max(0, size - 8)), 0, size);
                        }
                        default -> {
                            if (live.size() > 1) s.delete(live.remove(rnd.nextInt(live.size())), Fixtures.RAW);
                        }
                    }
                }
                s.commit();
                commitLens.add(WalTestKit.onlySegment(f).length());
                commitSnaps.add(snapshot(s));
            }
        } finally {
            s.close();
        }
    }

    private TreeMap<Long, byte[]> survivingSnapshot(long truncLen) {
        TreeMap<Long, byte[]> best = new TreeMap<>();
        for (int i = 0; i < commitLens.size(); i++) {
            if (commitLens.get(i) <= truncLen) best = commitSnaps.get(i);
        }
        return best;
    }

    // ================= tiny window: every offset, every refill edge =================

    /**
     * Replay window of 3 bytes: every multi-byte element (packed recids, lengths,
     * payloads, stored CRC) is forced across refills. Truncation at EVERY byte offset
     * must recover exactly the newest wholly-surviving commit and never throw.
     */
    @Test public void tiny_window_truncation_sweep_at_every_offset() throws IOException {
        File src = newFile("tiny-src");
        buildWorkload(src, 0x7EA5EED, 5, new int[]{0, 1, 7, 40});
        byte[] all = WalTestKit.read(WalTestKit.onlySegment(src));

        File copy = newFile("tiny-copy");
        for (int t = 0; t <= all.length; t++) {
            WalTestKit.deleteStore(copy);
            WalTestKit.write(WalTestKit.segment(copy, 1), java.util.Arrays.copyOf(all, t));
            StoreWAL s = null;
            try {
                s = new StoreWAL(copy, false, true, 3); // 3-byte replay window
                s.verify();
                assertState(s, survivingSnapshot(t));
                long surviving = survivingLen(t);
                // once at least one commit survives, the torn tail past it must be gone;
                // below that the file is header-only or legacy-migrated (length varies)
                if (surviving > 0) assertEquals("torn tail must be truncated off",
                        surviving, WalTestKit.segment(copy, 1).length());
            } catch (RuntimeException e) {
                fail("truncation at offset " + t + " threw " + e);
            } finally {
                if (s != null) s.close();
            }
        }
    }

    private long survivingLen(long truncLen) {
        long best = 0;
        for (long len : commitLens) {
            if (len <= truncLen) best = len;
        }
        return best;
    }

    // ================= payloads larger than the window =================

    /**
     * Records several times larger than the replay window (1 KiB window, up to ~64 KiB
     * payloads): bulk payload reads span many refills; window-boundary-straddling
     * headers and CRCs decode correctly; results are byte-identical to a replay with
     * the default 1 MiB window.
     */
    @Test public void payloads_larger_than_window_replay_byte_exact() {
        File f = newFile("large-rec");
        buildWorkload(f, 0xBADC0DE, 6, new int[]{1023, 1024, 1025, 4096, 65_000});
        TreeMap<Long, byte[]> oracle = commitSnaps.get(commitSnaps.size() - 1);

        for (int win : new int[]{1024, 4096, StoreWAL.DEFAULT_REPLAY_BUF}) {
            StoreWAL s = new StoreWAL(f, false, true, win);
            try {
                s.verify();
                assertState(s, oracle);
            } finally {
                s.close();
            }
        }
    }

    // ================= WALTruncate port: random cuts on a multi-MB log =================

    /**
     * mapdb3 {@code WALTruncate} invariant at real scale: build a multi-megabyte log
     * (many window refills with the default 1 MiB window), cut it at random offsets,
     * reopen, and require exactly the newest wholly-surviving commit's state.
     */
    @Test public void random_truncation_on_multi_megabyte_log() throws IOException {
        File src = newFile("mb-src");
        buildWorkload(src, 0xCAFE, 24, new int[]{20_000, 60_000, 120_000});
        byte[] all = WalTestKit.read(WalTestKit.onlySegment(src));
        if (all.length < 2 * StoreWAL.DEFAULT_REPLAY_BUF)
            fail("workload too small to cross window refills: " + all.length);

        Random rnd = new Random(42);
        File copy = newFile("mb-copy");
        for (int i = 0; i < 40; i++) {
            int t = switch (i) {
                case 0 -> 0;
                case 1 -> all.length;
                case 2 -> all.length - 1;
                case 3 -> StoreWAL.DEFAULT_REPLAY_BUF;     // exactly one window
                case 4 -> StoreWAL.DEFAULT_REPLAY_BUF - 1; // one byte short of it
                case 5 -> StoreWAL.DEFAULT_REPLAY_BUF + 1;
                default -> rnd.nextInt(all.length + 1);
            };
            WalTestKit.deleteStore(copy);
            WalTestKit.write(WalTestKit.segment(copy, 1), java.util.Arrays.copyOf(all, t));
            StoreWAL s = null;
            try {
                s = new StoreWAL(copy);
                s.verify();
                assertState(s, survivingSnapshot(t));
            } catch (RuntimeException e) {
                fail("truncation at offset " + t + " threw " + e);
            } finally {
                if (s != null) s.close();
            }
        }
    }
}
