package org.mapdb.store;

import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

/**
 * Allocator fuzz with verify() as the oracle (mapdb3 StoreDirect_LongStackAllocTest
 * lineage): randomized put/update/delete/get over mixed sizes — including linked
 * records — against a HashMap reference, with full-store verify() sweeps and two
 * close/reopen cycles mid-run. Any double-free, lost extent, overlap or free-list
 * corruption surfaces as a VerifyFailed or a content mismatch.
 */
public class StoreDirectAllocFuzzTest {

    private static final int OPS = 3_000;

    private final List<File> files = new ArrayList<>();

    @After public void cleanup() {
        for (File f : files) f.delete();
        files.clear();
    }

    private static byte[] bytes(int size, long seed) {
        byte[] b = new byte[size];
        long s = seed * 0x9E3779B97F4A7C15L + 1;
        for (int i = 0; i < size; i++) {
            s ^= s << 13; s ^= s >>> 7; s ^= s << 17;
            b[i] = (byte) s;
        }
        return b;
    }

    private static int pickSize(Random rnd) {
        int r = rnd.nextInt(100);
        if (r < 70) return rnd.nextInt(400);
        if (r < 95) return 400 + rnd.nextInt(30_000);
        // linked territory: straddle the plain/linked boundary up to ~1.4 MB
        return IndexVal.MAX_CAPACITY - 50 + rnd.nextInt(400_000);
    }

    @Test public void alloc_fuzz_with_reopen() throws IOException {
        long seed = Long.getLong("fuzz.seed", 0xA110CA7EL);
        System.out.println("StoreDirectAllocFuzzTest seed=" + Long.toHexString(seed) + " ops=" + OPS);
        File f = Files.createTempFile("mapdb-allocfuzz", ".db").toFile();
        f.delete();
        files.add(f);

        Random rnd = new Random(seed);
        Map<Long, byte[]> ref = new HashMap<>();
        StoreDirect s = new StoreDirect(f);
        try {
            for (int op = 0; op < OPS; op++) {
                int kind = rnd.nextInt(100);
                if (kind < 40 || ref.isEmpty()) {
                    byte[] v = rnd.nextInt(20) == 0 ? null : bytes(pickSize(rnd), op);
                    long r = s.put(v, Fixtures.RAW);
                    ref.put(r, v);
                } else {
                    Long r = pickRecid(ref, rnd);
                    if (kind < 65) {
                        byte[] v = rnd.nextInt(20) == 0 ? null : bytes(pickSize(rnd), op ^ 0x5555);
                        s.update(r, v, Fixtures.RAW);
                        ref.put(r, v);
                    } else if (kind < 85) {
                        s.delete(r, Fixtures.RAW);
                        ref.remove(r);
                    } else {
                        assertArrayEquals("get mismatch at op " + op, ref.get(r), s.get(r, Fixtures.RAW));
                    }
                }
                if (op % 250 == 249) s.verify();
                if (op == 1000 || op == 2000) {
                    s.verify();
                    s.close();
                    s = new StoreDirect(f);
                    s.verify();
                    checkAll(s, ref, op);
                }
            }
            s.verify();
            checkAll(s, ref, -1);
            s.close();
            s = new StoreDirect(f);
            s.verify();
            checkAll(s, ref, -2);
        } finally {
            try { if (!s.isClosed()) s.close(); } catch (Throwable ignore) { }
        }
    }

    private static Long pickRecid(Map<Long, byte[]> ref, Random rnd) {
        int idx = rnd.nextInt(ref.size());
        for (Long r : ref.keySet()) {
            if (idx-- == 0) return r;
        }
        fail("unreachable");
        return null;
    }

    private static void checkAll(StoreDirect s, Map<Long, byte[]> ref, int op) {
        for (Map.Entry<Long, byte[]> e : ref.entrySet()) {
            assertArrayEquals("content mismatch recid=" + e.getKey() + " at op " + op,
                    e.getValue(), s.get(e.getKey(), Fixtures.RAW));
        }
    }
}
