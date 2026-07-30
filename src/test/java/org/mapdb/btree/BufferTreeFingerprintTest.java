package org.mapdb.btree;

import org.junit.After;
import org.junit.Test;
import org.mapdb.format.BufferedPageFormat;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;
import org.mapdb.ser.GroupCursor;
import org.mapdb.ser.GroupFormat;
import org.mapdb.ser.LongFormat;
import org.mapdb.ser.Serializer;
import org.mapdb.store.StoreByteArray;
import org.mapdb.store.StoreDelta;
import org.mapdb.store.StoreDirect;
import org.mapdb.store.StoreWAL;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests for the R1 base-fingerprint negative-lookup accelerator
 * ({@link BufferedPageFormat} base filter, spec-missing #10). Proves: correctness is neutral to the
 * accelerator (P7: NORMAL / DISABLED / RANDOM give identical results on the SAME physical pages); no
 * present key is ever reported absent (no false negatives); a definite base-miss point read SKIPS the
 * leaf base binary search (measurable via a counting group format); the fingerprint is rebuilt on
 * consolidation; and it survives reopen over StoreDirect(file) + StoreWAL.
 */
public class BufferTreeFingerprintTest {

    private static final GroupFormat<Long> LF = LongFormat.INSTANCE;

    @After public void resetMode() { BufferedPageFormat.fpMode = BufferedPageFormat.FpMode.NORMAL; }

    /** Random put/remove workload into m mirrored by a TreeMap oracle. */
    private static TreeMap<Long, Long> fuzz(BufferTreeMap<Long, Long> m, long seed, int ops, int keySpace) {
        TreeMap<Long, Long> oracle = new TreeMap<>();
        Random r = new Random(seed);
        for (int i = 0; i < ops; i++) {
            long k = r.nextInt(keySpace);
            if (r.nextInt(4) == 0) { m.remove(k); oracle.remove(k); }
            else { long v = r.nextLong(); m.put(k, v); oracle.put(k, v); }
        }
        return oracle;
    }

    // ---- P7: identical results under NORMAL / FORCE_SEARCH / RANDOM on the SAME pages ----
    @Test public void resultsIdenticalAcrossFpModes() throws IOException {
        File wal = File.createTempFile("bt-fp-modes", ".wal");
        wal.delete();
        try {
            for (StoreDelta s : new StoreDelta[]{new StoreDirect(false), new StoreByteArray(), new StoreWAL(wal)}) {
                BufferTreeMap<Long, Long> m = BufferTreeMap.create(s, LF, LF, 16, 128);
                TreeMap<Long, Long> oracle = fuzz(m, 424242L, 8000, 2500);
                // one physical tree; only the READ decision changes per mode.
                for (BufferedPageFormat.FpMode mode : BufferedPageFormat.FpMode.values()) {
                    BufferedPageFormat.fpMode = mode;
                    for (long k = -50; k < 3000; k++)   // present AND absent probes
                        assertEquals("store=" + s.getClass().getSimpleName() + " mode=" + mode + " k=" + k,
                                oracle.get(k), m.get(k));
                }
                BufferedPageFormat.fpMode = BufferedPageFormat.FpMode.NORMAL;
            }
        } finally { wal.delete(); }
    }

    // ---- no false negatives: after a heavy fuzz, EVERY present key returns its value ----
    @Test public void noFalseNegativeManySeeds() {
        for (long seed : new long[]{1, 7, 99, 12345, 0xBEEFL, 2026_07_08L}) {
            BufferTreeMap<Long, Long> m = BufferTreeMap.create(new StoreDirect(false), LF, LF, 8, 96);
            TreeMap<Long, Long> oracle = fuzz(m, seed, 6000, 1500);
            for (Map.Entry<Long, Long> e : oracle.entrySet())
                assertEquals("present key wrongly absent seed=" + seed + " k=" + e.getKey(),
                        e.getValue(), m.get(e.getKey()));
        }
    }

    // ---- measurable skip: definite base-misses skip the leaf base search (all but Bloom FPs) ----
    @Test public void definiteMissSkipsBaseSearch() {
        CountingLongFormat cf = new CountingLongFormat();
        // large maxNodeSize + few keys ⇒ the root stays a SINGLE leaf (no dir routing to muddy counts)
        BufferTreeMap<Long, Long> m = BufferTreeMap.create(new StoreDirect(false), cf, LF, 1024, 8192);
        for (long k = 0; k < 200; k += 2) m.put(k, k * 11);   // even keys 0..198
        m.flushAll();                                          // consolidate → keys in base, log empty

        final int N = 1000;                                   // odd keys 20001..21999 — all absent
        // NORMAL: only Bloom false-positives reach the base search.
        BufferedPageFormat.fpMode = BufferedPageFormat.FpMode.NORMAL;
        cf.binarySearches = 0;
        for (int i = 0; i < N; i++) assertNull(m.get(20001L + 2L * i));
        int normalSearches = cf.binarySearches;

        // FORCE_SEARCH (== accelerator disabled): EVERY miss searches the base (one leaf each).
        BufferedPageFormat.fpMode = BufferedPageFormat.FpMode.FORCE_SEARCH;
        cf.binarySearches = 0;
        for (int i = 0; i < N; i++) assertNull(m.get(20001L + 2L * i));
        int forceSearches = cf.binarySearches;

        assertEquals("disabled accelerator searches every miss", N, forceSearches);
        assertTrue("accelerator must skip the vast majority of definite misses (" + normalSearches
                + "/" + N + ")", normalSearches <= N / 5);

        // a present key always searches (fp says "maybe" for a real base member — no false negative)
        BufferedPageFormat.fpMode = BufferedPageFormat.FpMode.NORMAL;
        cf.binarySearches = 0;
        assertEquals((Long) (10L * 11), m.get(10L));
        assertTrue("present key must search the base", cf.binarySearches >= 1);
    }

    // ---- fingerprint is rebuilt on consolidation and after deletes of base keys ----
    @Test public void rebuiltOnConsolidationAndDeletes() {
        BufferTreeMap<Long, Long> m = BufferTreeMap.create(new StoreDirect(false), LF, LF, 8, 64);
        TreeMap<Long, Long> oracle = new TreeMap<>();
        for (long k = 0; k < 3000; k++) { m.put(k, k); oracle.put(k, k); }
        m.flushAll(); // rebuild fp over the full base

        // delete a swath of BASE keys (log DELETEs), then re-consolidate → fp rebuilt WITHOUT them
        for (long k = 0; k < 3000; k += 3) { m.remove(k); oracle.remove(k); }
        m.flushAll();

        for (long k = -10; k < 3200; k++)
            assertEquals("k=" + k, oracle.get(k), m.get(k));

        // HIGH-4: a base key deleted via a log tombstone reads absent even though its bits are set
        // in the (pre-delete) fingerprint — the log DELETE is authoritative before the fp is consulted.
        BufferTreeMap<Long, Long> m2 = BufferTreeMap.create(new StoreDirect(false), LF, LF, 8, 64);
        m2.put(42L, 4200L);
        m2.put(43L, 4300L);
        m2.flushAll();            // 42 and 43 now base keys; their bits are set in the fp
        m2.remove(42L);           // log DELETE (base key 42 stays in base, fp still "maybe" for it)
        assertNull("deleted base key must read absent (log-first)", m2.get(42L));
        assertEquals("live base key still found", (Long) 4300L, m2.get(43L));
    }

    // ---- fingerprint survives reopen over StoreDirect(file) and StoreWAL ----
    @Test public void survivesReopen() throws IOException {
        reopenRoundTrip(true);
        reopenRoundTrip(false);
    }

    private void reopenRoundTrip(boolean wal) throws IOException {
        File f = File.createTempFile("bt-fp-reopen", wal ? ".wal" : ".db");
        f.delete();
        try {
            TreeMap<Long, Long> oracle = new TreeMap<>();
            long rrr;
            {
                StoreDelta s = wal ? new StoreWAL(f) : new StoreDirect(f);
                BufferTreeMap<Long, Long> m = BufferTreeMap.create(s, LF, LF, 16, 96);
                rrr = m.rootRecidRecid();
                Random r = new Random(9);
                for (int i = 0; i < 12000; i++) { long k = r.nextInt(6000); long v = r.nextLong(); m.put(k, v); oracle.put(k, v); }
                for (int i = 0; i < 2000; i++) { long k = r.nextInt(6000); m.remove(k); oracle.remove(k); }
                m.flushAll();            // consolidate → fingerprints written into the base images
                s.commit();
                s.close();
            }
            {
                StoreDelta s = wal ? new StoreWAL(f) : new StoreDirect(f);
                BufferTreeMap<Long, Long> m = BufferTreeMap.open(s, rrr, LF, LF, 16, 96);
                // present keys found (no false negative on the persisted fp) AND absent keys correct
                for (Map.Entry<Long, Long> e : oracle.entrySet())
                    assertEquals(e.getValue(), m.get(e.getKey()));
                for (long k = 6000; k < 6500; k++) assertNull(m.get(k)); // definitely absent
                s.close();
            }
        } finally { f.delete(); }
    }

    // ================= counting group format (measures base binary searches) =================

    /** Delegates to {@link LongFormat} but counts {@link #binarySearch} calls (the base read path). */
    private static final class CountingLongFormat implements GroupFormat<Long> {
        int binarySearches;
        private final GroupFormat<Long> d = LongFormat.INSTANCE;

        @Override public Serializer<Long> element() { return d.element(); }
        @Override public Object empty() { return d.empty(); }
        @Override public int size(Object g) { return d.size(g); }
        @Override public Long get(Object g, int p) { return d.get(g, p); }
        @Override public int search(Object g, Long k) { return d.search(g, k); }
        @Override public int compare(Long a, Long b) { return d.compare(a, b); }
        @Override public Comparator<Long> comparator() { return d.comparator(); }
        @Override public Object insert(Object g, int p, Long v) { return d.insert(g, p, v); }
        @Override public Object set(Object g, int p, Long v) { return d.set(g, p, v); }
        @Override public Object delete(Object g, int p) { return d.delete(g, p); }
        @Override public Object copyRange(Object g, int from, int to) { return d.copyRange(g, from, to); }
        @Override public Object fromArray(Object[] v) { return d.fromArray(v); }
        @Override public void serialize(DataOutput2 out, Object g) { d.serialize(out, g); }
        @Override public Object deserialize(DataInput2 in, int size) { return d.deserialize(in, size); }
        @Override public boolean supportsBinary() { return d.supportsBinary(); }
        @Override public int binarySearch(Long key, DataInput2 in, int size) {
            binarySearches++;
            return d.binarySearch(key, in, size);
        }
        @Override public Long binaryGet(DataInput2 in, int size, int pos) { return d.binaryGet(in, size, pos); }
        @Override public boolean supportsRangeCursor() { return d.supportsRangeCursor(); }
        @Override public GroupCursor<Long> rangeCursor(DataInput2 in, int size, int from, int to) {
            return d.rangeCursor(in, size, from, to);
        }
    }
}
