package org.mapdb.store;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * {@link StoreAppendOnly} compaction: bounded footprint, content preservation
 * across moves (including append headroom), accounting reconciliation
 * ({@code verify()} checks totalLive/sliceLive exactly), and the wait-free
 * reader contract while slices retire mid-read (GC-based reclamation: readers
 * that lose the race to {@link ByteBufferVol#dataInputOrNull} retry against the
 * re-pointed entry; winners read intact stale bytes).
 *
 * All tests use a tiny {@code minGarbageBytes} so sweeps fire constantly at
 * unit-test data sizes; the default 64 MB floor keeps compaction out of the
 * ordinary TCK/fuzz paths.
 */
public class StoreAppendOnlyCompactionTest {

    private static final long MB = 1 << 20;

    /** Rewriting one large record forever must not grow the footprint (the original failure mode). */
    @Test public void footprintBounded() {
        StoreAppendOnly s = new StoreAppendOnly(false, 2 * MB);
        try {
            byte[] big = Fixtures.payload(1, 0, 100_000);
            long recid = s.put(big, Fixtures.RAW);
            for (int v = 1; v <= 1000; v++) {
                big = Fixtures.payload(1, v, 100_000);
                s.update(recid, big, Fixtures.RAW);
            }
            assertTrue("address space consumed: " + s.allocatedBytes(), s.allocatedBytes() > 90 * MB);
            assertTrue("footprint bounded: " + s.occupiedBytes(), s.occupiedBytes() < 8 * MB);
            assertArrayEquals(big, s.get(recid, Fixtures.RAW));
            s.verify();
        } finally {
            s.close();
        }
    }

    /** Random put/update/append/delete against an oracle map, with sweeps firing constantly. */
    @Test public void contentsSurviveCompaction() {
        StoreAppendOnly s = new StoreAppendOnly(false, MB / 4);
        try {
            Random rnd = new Random(42);
            Map<Long, byte[]> oracle = new HashMap<>();
            long[] recids = new long[300];
            for (int i = 0; i < recids.length; i++) {
                byte[] p = Fixtures.payload(i, 0, rnd.nextInt(2000));
                recids[i] = s.put(p, Fixtures.RAW);
                oracle.put(recids[i], p);
            }
            for (int op = 0; op < 20_000; op++) {
                int i = rnd.nextInt(recids.length);
                long recid = recids[i];
                byte[] cur = recid == 0 ? null : oracle.get(recid);
                switch (rnd.nextInt(cur == null ? 1 : 3)) {
                    case 0: { // (re)write, sometimes with headroom
                        byte[] p = Fixtures.payload(i, op, rnd.nextInt(2000));
                        if (cur == null) {
                            recids[i] = recid = s.put(p, Fixtures.RAW);
                        } else if (rnd.nextBoolean()) {
                            s.update(recid, p, Fixtures.RAW);
                        } else {
                            s.updateWithHeadroom(recid, p, Fixtures.RAW, rnd.nextInt(256));
                        }
                        oracle.put(recid, p);
                        break;
                    }
                    case 1: { // append within provisioned capacity (REFUSED is legal — oracle only on success)
                        byte[] extra = Fixtures.payload(i, op, rnd.nextInt(64));
                        if (s.append(recid, extra, 0, extra.length) != StoreDelta.REFUSED) {
                            byte[] merged = new byte[cur.length + extra.length];
                            System.arraycopy(cur, 0, merged, 0, cur.length);
                            System.arraycopy(extra, 0, merged, cur.length, extra.length);
                            oracle.put(recid, merged);
                        }
                        break;
                    }
                    case 2: { // delete, clearing the slot so recid reuse cannot alias two slots
                        s.delete(recid, Fixtures.RAW);
                        oracle.remove(recid);
                        recids[i] = 0;
                        break;
                    }
                }
                if (op % 1000 == 0) s.verify();
            }
            s.compact(); // explicit sweep on top of the auto ones
            s.verify();
            long checked = 0;
            for (Map.Entry<Long, byte[]> e : oracle.entrySet()) {
                assertArrayEquals("recid " + e.getKey(), e.getValue(), s.get(e.getKey(), Fixtures.RAW));
                checked++;
            }
            assertTrue(checked > 100); // the oracle kept a real population
            assertTrue("footprint bounded: " + s.occupiedBytes(), s.occupiedBytes() < 8 * MB);
        } finally {
            s.close();
        }
    }

    /** Capacity (and therefore append headroom) must survive the copy-move. */
    @Test public void headroomSurvivesMove() {
        StoreAppendOnly s = new StoreAppendOnly(false, Long.MAX_VALUE); // manual compaction only
        try {
            byte[] base = Fixtures.payload(7, 0, 100);
            long recid = s.put(base, Fixtures.RAW);
            s.updateWithHeadroom(recid, base, Fixtures.RAW, 512);
            long capBefore = s.capacityRemaining(recid);
            assertTrue(capBefore >= 512);

            // make the record's slice majority-dead, then force the move
            long junk = s.put(Fixtures.payload(8, 0, 800_000), Fixtures.RAW);
            s.update(junk, Fixtures.payload(8, 1, 800_000), Fixtures.RAW);
            long offBefore = s.allocatedBytes();
            s.compact();
            s.verify();
            assertTrue("compaction ran", s.allocatedBytes() > offBefore || s.occupiedBytes() < s.allocatedBytes());

            byte[] merged = base;
            for (int a = 0; a < 4; a++) { // headroom still appendable after the move
                byte[] extra = Fixtures.payload(7, a + 1, 64);
                assertNotEquals("append " + a + " refused", StoreDelta.REFUSED,
                        s.append(recid, extra, 0, extra.length));
                byte[] m = new byte[merged.length + extra.length];
                System.arraycopy(merged, 0, m, 0, merged.length);
                System.arraycopy(extra, 0, m, merged.length, extra.length);
                merged = m;
            }
            assertArrayEquals(merged, s.get(recid, Fixtures.RAW));
            assertEquals(capBefore - 4 * (8 + 64), s.capacityRemaining(recid));
            s.verify();
        } finally {
            s.close();
        }
    }

    /**
     * Readers hammer get()/read() while the single writer rewrites versioned
     * payloads with compaction firing constantly. Every observed value must be
     * an internally consistent (slot, version, deterministic fill) payload —
     * a moved-or-retired extent must never yield mixed bytes.
     */
    @Test public void concurrentReadersDuringCompaction() throws InterruptedException {
        StoreAppendOnly s = new StoreAppendOnly(false, MB / 4);
        try {
            final int slots = 128;
            final long[] recids = new long[slots];
            for (int i = 0; i < slots; i++) recids[i] = s.put(Fixtures.payload(i, 0, 512), Fixtures.RAW);

            AtomicReference<Throwable> failure = new AtomicReference<>();
            long deadline = System.nanoTime() + 1_500_000_000L; // ~1.5s
            Thread writer = new Thread(() -> {
                Random rnd = new Random(7);
                int version = 1;
                try {
                    while (System.nanoTime() < deadline && failure.get() == null) {
                        int i = rnd.nextInt(slots);
                        s.update(recids[i], Fixtures.payload(i, version++, 512), Fixtures.RAW);
                    }
                } catch (Throwable t) { failure.compareAndSet(null, t); }
            }, "compaction-writer");

            Thread[] readers = new Thread[8];
            for (int r = 0; r < readers.length; r++) {
                final long seed = 100 + r;
                readers[r] = new Thread(() -> {
                    Random rnd = new Random(seed);
                    Fixtures.Capture capture = new Fixtures.Capture();
                    try {
                        while (System.nanoTime() < deadline && failure.get() == null) {
                            int i = rnd.nextInt(slots);
                            byte[] b = rnd.nextBoolean()
                                    ? s.get(recids[i], Fixtures.RAW)
                                    : (s.read(recids[i], capture) >= 0 ? capture.bytes : null);
                            checkPayload(i, b);
                        }
                    } catch (Throwable t) { failure.compareAndSet(null, t); }
                }, "compaction-reader-" + r);
            }

            writer.start();
            for (Thread t : readers) t.start();
            writer.join();
            for (Thread t : readers) t.join();
            if (failure.get() != null) throw new AssertionError("concurrent compaction failed", failure.get());
            s.verify();
            assertTrue("compaction actually ran", s.allocatedBytes() > s.occupiedBytes());
        } finally {
            s.close();
        }
    }

    /** Payload self-check: fill bytes must match the (slot, version) header exactly. */
    private static void checkPayload(int slot, byte[] b) {
        if (b == null) throw new AssertionError("null payload for slot " + slot);
        int gotSlot = ((b[0] & 0xFF) << 24) | ((b[1] & 0xFF) << 16) | ((b[2] & 0xFF) << 8) | (b[3] & 0xFF);
        int version = ((b[4] & 0xFF) << 24) | ((b[5] & 0xFF) << 16) | ((b[6] & 0xFF) << 8) | (b[7] & 0xFF);
        if (gotSlot != slot) throw new AssertionError("slot mismatch: " + gotSlot + " != " + slot);
        byte[] expect = Fixtures.payload(slot, version, b.length - 8);
        assertArrayEquals("torn payload slot=" + slot + " version=" + version, expect, b);
    }
}
