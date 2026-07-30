package org.mapdb.btree;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mapdb.ser.ByteArrayFormat;
import org.mapdb.ser.GroupFormat;
import org.mapdb.ser.LongFormat;
import org.mapdb.ser.ObjectArrayFormat;
import org.mapdb.ser.Serializers;
import org.mapdb.store.StoreDelta;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Abstract TCK for {@link BufferTreeMap}. Concrete subclasses bind {@link #openStore()}
 * to a specific {@link StoreDelta} dialect (byte-array / direct / WAL); StoreOnHeap is
 * unsupported so there is no OnHeap subclass. Every case runs unchanged across all three.
 *
 * <p>Because writes are BLIND MESSAGES (put/remove return void), correctness is validated
 * exclusively through the read surface — get / containsKey / entryIterator / sizeLong.
 * Buffer-tree specific cases additionally assert that the map reads IDENTICALLY before and
 * after {@link BufferTreeMap#flushAll()} (buffered vs consolidated state), and that
 * flushAll is idempotent.
 */
public abstract class BufferTreeMapTCK {

    /** A fresh, empty delta store. Subclasses may allocate resources (temp files) here. */
    protected abstract StoreDelta openStore();

    protected StoreDelta store;

    @Before
    public void setUp() {
        store = openStore();
    }

    @After
    public void tearDown() {
        try {
            if (store != null && !store.isClosed()) {
                store.verify();
                store.close();
            }
        } finally {
            cleanup();
        }
    }

    /** Subclass hook for post-close resource cleanup (temp files). */
    protected void cleanup() {}

    // ---------- format helpers ----------

    private BufferTreeMap<Long, Long> longMap(int maxNodeSize, int bufferBytes) {
        return BufferTreeMap.create(store, LongFormat.INSTANCE, LongFormat.INSTANCE, maxNodeSize, bufferBytes);
    }

    private BufferTreeMap<String, String> stringMap(int maxNodeSize, int bufferBytes) {
        return BufferTreeMap.create(store,
                new ObjectArrayFormat<>(Serializers.STRING),
                new ObjectArrayFormat<>(Serializers.STRING), maxNodeSize, bufferBytes);
    }

    private BufferTreeMap<String, String> stringMapBinary(int maxNodeSize, int bufferBytes) {
        return BufferTreeMap.create(store,
                org.mapdb.ser.StringGroupFormat.INSTANCE,
                org.mapdb.ser.StringGroupFormat.INSTANCE, maxNodeSize, bufferBytes);
    }

    // ---------- oracle-comparison helpers ----------

    /** Full read-surface comparison against a TreeMap oracle: point gets, containsKey,
     *  sizeLong, and ordered/complete iteration. */
    private static <K, V> void assertMatches(BufferTreeMap<K, V> m, TreeMap<K, V> oracle) {
        assertEquals("sizeLong", (long) oracle.size(), m.sizeLong());
        for (Map.Entry<K, V> e : oracle.entrySet()) {
            assertEquals("get " + e.getKey(), e.getValue(), m.get(e.getKey()));
            assertTrue("containsKey " + e.getKey(), m.containsKey(e.getKey()));
        }
        Iterator<Map.Entry<K, V>> it = m.entryIterator();
        Iterator<Map.Entry<K, V>> ot = oracle.entrySet().iterator();
        while (ot.hasNext()) {
            assertTrue("map iterator ended early", it.hasNext());
            Map.Entry<K, V> me = it.next();
            Map.Entry<K, V> oe = ot.next();
            assertEquals(oe.getKey(), me.getKey());
            assertEquals(oe.getValue(), me.getValue());
        }
        assertFalse("map iterator has extra entries", it.hasNext());
    }

    /** Core buffer-tree invariant: the map reads identically with buffers live, after
     *  flushAll, and after a second flushAll (idempotence). */
    private static <K, V> void assertMatchesAcrossFlush(BufferTreeMap<K, V> m, TreeMap<K, V> oracle) {
        assertMatches(m, oracle);       // buffered state
        m.flushAll();
        assertMatches(m, oracle);       // consolidated state must read identically
        m.flushAll();                   // idempotence: second flush is a no-op
        assertMatches(m, oracle);
    }

    // ---------- bulk build (pump / createFromSorted) ----------

    private static Iterator<Map.Entry<Long, Long>> ascending(long n) {
        return new Iterator<>() {
            long i = 0;
            @Override public boolean hasNext() { return i < n; }
            @Override public Map.Entry<Long, Long> next() {
                long k = i++;
                return new java.util.AbstractMap.SimpleImmutableEntry<>(k, k * 7 + 1);
            }
        };
    }

    /** Node-boundary sizes around the default fill (maxNodeSize 8 → fill 6). */
    @Test
    public void pumpBoundarySizes() {
        for (long n : new long[]{0, 1, 2, 5, 6, 7, 12, 13, 35, 36, 37, 1000}) {
            BufferTreeMap<Long, Long> m = BufferTreeMap.createFromSorted(store,
                    LongFormat.INSTANCE, LongFormat.INSTANCE, 8, 256, ascending(n));
            store.verify();
            TreeMap<Long, Long> oracle = new TreeMap<>();
            for (long k = 0; k < n; k++) oracle.put(k, k * 7 + 1);
            assertMatches(m, oracle);
            assertNull(m.get(-1L));
            assertNull(m.get(n + 1000));
            // reopen from the persisted root pointer
            assertMatches(BufferTreeMap.open(store, m.rootRecidRecid(),
                    LongFormat.INSTANCE, LongFormat.INSTANCE, 8, 256), oracle);
        }
    }

    @Test
    public void pumpLarge() {
        final long N = 50_000;
        BufferTreeMap<Long, Long> m = BufferTreeMap.createFromSorted(store,
                LongFormat.INSTANCE, LongFormat.INSTANCE, 8, 512, ascending(N));
        store.verify();
        TreeMap<Long, Long> oracle = new TreeMap<>();
        for (long k = 0; k < N; k++) oracle.put(k, k * 7 + 1);
        assertMatches(m, oracle);
    }

    /** A pumped tree must accept blind messages exactly like an incrementally
     *  built one: appends land in the provisioned headroom of pumped nodes, and
     *  flushes/splits of pump-filled nodes preserve the read surface. */
    @Test
    public void pumpThenMutate() {
        final long N = 5_000;
        TreeMap<Long, Long> oracle = new TreeMap<>();
        for (long k = 0; k < 2 * N; k += 2) oracle.put(k, k * 7 + 1);
        BufferTreeMap<Long, Long> m = BufferTreeMap.createFromSorted(store,
                LongFormat.INSTANCE, LongFormat.INSTANCE, 8, 256,
                oracle.entrySet().iterator());

        for (long k = 1; k < 2 * N; k += 2) { // fresh odd keys split pump-filled leaves
            m.put(k, -k);
            oracle.put(k, -k);
        }
        for (long k = 0; k < 2 * N; k += 100) { // overwrite pumped entries
            m.put(k, k + 5);
            oracle.put(k, k + 5);
        }
        for (long k = 0; k < 2 * N; k += 7) { // tombstones over pumped entries
            m.remove(k);
            oracle.remove(k);
        }
        store.verify();
        assertMatchesAcrossFlush(m, oracle);
    }

    @Test
    public void pumpEmptyThenUsable() {
        BufferTreeMap<Long, Long> m = BufferTreeMap.createFromSorted(store,
                LongFormat.INSTANCE, LongFormat.INSTANCE, 8, 64,
                java.util.Collections.emptyIterator());
        assertEquals(0L, m.sizeLong());
        m.put(1L, 10L);
        assertEquals(Long.valueOf(10L), m.get(1L));
        m.remove(1L);
        assertNull(m.get(1L));
        assertEquals(0L, m.sizeLong());
    }

    @Test
    public void pumpRejectsUnsortedAndDuplicates() {
        List<Map.Entry<Long, Long>> dup = List.of(
                Map.entry(1L, 1L), Map.entry(2L, 2L), Map.entry(2L, 3L));
        try {
            BufferTreeMap.createFromSorted(store, LongFormat.INSTANCE, LongFormat.INSTANCE, 8, 64, dup.iterator());
            fail("expected NotSorted for duplicate key");
        } catch (org.mapdb.DBException.NotSorted expected) { /* ok */ }

        List<Map.Entry<Long, Long>> desc = List.of(Map.entry(5L, 1L), Map.entry(4L, 2L));
        try {
            BufferTreeMap.createFromSorted(store, LongFormat.INSTANCE, LongFormat.INSTANCE, 8, 64, desc.iterator());
            fail("expected NotSorted for descending keys");
        } catch (org.mapdb.DBException.NotSorted expected) { /* ok */ }
    }

    /** String pump over both the object-array and the binary group format,
     *  then buffered mutations on top. */
    @Test
    public void pumpStrings() {
        TreeMap<String, String> oracle = new TreeMap<>();
        Random rnd = new Random(11);
        for (int i = 0; i < 5_000; i++) {
            String k = randomString(rnd, 1 + rnd.nextInt(12));
            oracle.put(k, "v-" + k);
        }
        List<GroupFormat<String>> formats = List.of(
                new ObjectArrayFormat<>(Serializers.STRING),
                org.mapdb.ser.StringGroupFormat.INSTANCE);
        for (GroupFormat<String> f : formats) {
            TreeMap<String, String> o = new TreeMap<>(oracle);
            BufferTreeMap<String, String> m = BufferTreeMap.createFromSorted(store, f, f, 6, 512,
                    o.entrySet().iterator());
            store.verify();
            assertMatches(m, o);
            for (int i = 0; i < 500; i++) {
                assertNull(m.get("ABSENT-" + i));
            }
            // buffered mutations over the pumped tree
            List<String> keys = new ArrayList<>(o.keySet());
            for (int i = 0; i < keys.size(); i += 5) {
                m.put(keys.get(i), "updated");
                o.put(keys.get(i), "updated");
            }
            for (int i = 0; i < keys.size(); i += 9) {
                m.remove(keys.get(i));
                o.remove(keys.get(i));
            }
            assertMatchesAcrossFlush(m, o);
        }
    }

    // =====================================================================

    @Test
    public void emptyMap() {
        BufferTreeMap<Long, Long> m = longMap(8, 64);
        assertNull(m.get(1L));
        assertFalse(m.containsKey(1L));
        assertEquals(0L, m.sizeLong());
        assertFalse(m.entryIterator().hasNext());
        // remove of an absent key is legal (blind tombstone consolidates away)
        m.remove(1L);
        assertNull(m.get(1L));
        assertEquals(0L, m.sizeLong());
        m.flushAll();
        assertEquals(0L, m.sizeLong());
        assertFalse(m.entryIterator().hasNext());
    }

    @Test
    public void singleEntryLifecycle() {
        BufferTreeMap<Long, Long> m = longMap(8, 64);

        m.put(10L, 100L);
        assertEquals(Long.valueOf(100L), m.get(10L));
        assertTrue(m.containsKey(10L));
        assertEquals(1L, m.sizeLong());

        // overwrite: LWW in buffer, no old value returned
        m.put(10L, 200L);
        assertEquals(Long.valueOf(200L), m.get(10L));
        assertEquals(1L, m.sizeLong());

        // survives flush identically
        m.flushAll();
        assertEquals(Long.valueOf(200L), m.get(10L));
        assertEquals(1L, m.sizeLong());

        // remove (blind), then absent
        m.remove(10L);
        assertNull(m.get(10L));
        assertFalse(m.containsKey(10L));
        m.remove(10L); // remove-of-absent legal
        assertEquals(0L, m.sizeLong());
        assertFalse(m.entryIterator().hasNext());

        m.flushAll();
        assertEquals(0L, m.sizeLong());
        assertFalse(m.entryIterator().hasNext());
    }

    // ---------- bulk insert (forces deep splits) ----------

    private void bulkInsertOrder(long[] keys, int maxNodeSize, int bufferBytes) {
        final int N = keys.length;
        BufferTreeMap<Long, Long> m = longMap(maxNodeSize, bufferBytes);
        TreeMap<Long, Long> oracle = new TreeMap<>();

        for (long k : keys) {
            m.put(k, k * 7 + 1);
            oracle.put(k, k * 7 + 1);
        }
        assertEquals(N, m.sizeLong());

        // every key retrievable while buffers are still live
        for (long k : keys) {
            assertEquals("get " + k, Long.valueOf(k * 7 + 1), m.get(k));
            assertTrue(m.containsKey(k));
        }

        // iteration strictly ascending and complete, before and after flush
        assertAscendingComplete(m, N);
        m.flushAll();
        assertAscendingComplete(m, N);
        assertMatches(m, oracle);
    }

    private static void assertAscendingComplete(BufferTreeMap<Long, Long> m, long expectedCount) {
        Iterator<Map.Entry<Long, Long>> it = m.entryIterator();
        long prev = Long.MIN_VALUE;
        long count = 0;
        while (it.hasNext()) {
            Map.Entry<Long, Long> e = it.next();
            assertTrue("iteration not strictly ascending at " + e.getKey(), e.getKey() > prev);
            assertEquals(Long.valueOf(e.getKey() * 7 + 1), e.getValue());
            prev = e.getKey();
            count++;
        }
        assertEquals(expectedCount, count);
    }

    @Test
    public void bulkAscending() {
        long[] keys = new long[20_000];
        for (int i = 0; i < keys.length; i++) keys[i] = i;
        bulkInsertOrder(keys, 8, 64);
    }

    @Test
    public void bulkDescending() {
        long[] keys = new long[20_000];
        for (int i = 0; i < keys.length; i++) keys[i] = keys.length - 1 - i;
        bulkInsertOrder(keys, 8, 64);
    }

    @Test
    public void bulkShuffled() {
        long[] keys = new long[20_000];
        for (int i = 0; i < keys.length; i++) keys[i] = i;
        Random rnd = new Random(42);
        for (int i = keys.length - 1; i > 0; i--) {
            int j = rnd.nextInt(i + 1);
            long t = keys[i]; keys[i] = keys[j]; keys[j] = t;
        }
        bulkInsertOrder(keys, 8, 64);
    }

    @Test
    public void bulkTinyBufferChurn() {
        // smallest legal node + buffer: constant flush/split churn on the write path
        long[] keys = new long[20_000];
        for (int i = 0; i < keys.length; i++) keys[i] = i;
        Random rnd = new Random(9);
        for (int i = keys.length - 1; i > 0; i--) {
            int j = rnd.nextInt(i + 1);
            long t = keys[i]; keys[i] = keys[j]; keys[j] = t;
        }
        bulkInsertOrder(keys, 4, 48);
    }

    // ---------- long-running seeded fuzz vs TreeMap oracle ----------

    /** Fuzz across several (maxNodeSize, bufferBytes) combos, incl. tiny ones that force
     *  constant flush/split churn. Blind writes: correctness checked purely by reads. */
    @Test
    public void fuzzAgainstTreeMap() {
        int[][] combos = {
                {4, 48},   // tiniest legal: churn central
                {8, 64},
                {16, 128},
                {32, 256},
        };
        for (int[] combo : combos) {
            fuzzOnce(combo[0], combo[1]);
        }
    }

    private void fuzzOnce(int maxNodeSize, int bufferBytes) {
        // each combo gets its own fresh store so a prior combo cannot mask a bug
        StoreDelta local = openStore();
        try {
            final long seed = 0x5EEDC0FFEEL ^ (((long) maxNodeSize << 20) | bufferBytes);
            System.out.println("[BufferTreeMapTCK.fuzzAgainstTreeMap] store="
                    + local.getClass().getSimpleName()
                    + " maxNodeSize=" + maxNodeSize + " bufferBytes=" + bufferBytes + " seed=" + seed);
            Random rnd = new Random(seed);
            final int KEYSPACE = 8_000;
            final int OPS = 60_000;

            BufferTreeMap<Long, Long> m = BufferTreeMap.create(local,
                    LongFormat.INSTANCE, LongFormat.INSTANCE, maxNodeSize, bufferBytes);
            TreeMap<Long, Long> oracle = new TreeMap<>();

            for (int i = 0; i < OPS; i++) {
                long key = rnd.nextInt(KEYSPACE);
                int roll = rnd.nextInt(100);
                if (roll < 60) { // put
                    long val = rnd.nextLong();
                    oracle.put(key, val);
                    m.put(key, val);
                } else if (roll < 85) { // remove (may be absent)
                    oracle.remove(key);
                    m.remove(key);
                } else { // get: blind writes verified via reads
                    assertEquals("get[" + i + "] key=" + key + " combo=("
                            + maxNodeSize + "," + bufferBytes + ")", oracle.get(key), m.get(key));
                }
                // occasional flush mid-stream so consolidation interleaves with buffered writes
                if ((i & 16383) == 16383) {
                    m.flushAll();
                    assertEquals("size after mid flush", (long) oracle.size(), m.sizeLong());
                }
            }

            assertMatchesAcrossFlush(m, oracle);
        } finally {
            if (!local.isClosed()) {
                local.verify();
                local.close();
            }
        }
    }

    // ---------- remove-heavy then reinsert (leaf-coverage regression) ----------

    @Test
    public void removeHeavyThenReinsert() {
        BufferTreeMap<Long, Long> m = longMap(8, 64);
        TreeMap<Long, Long> oracle = new TreeMap<>();
        final int N = 4000;

        for (long k = 0; k <= N; k++) { m.put(k, k); oracle.put(k, k); }

        // remove every key divisible by 3 (blind)
        for (long k = 0; k <= N; k++) {
            if (k % 3 == 0) { m.remove(k); oracle.remove(k); }
        }
        for (long k = 0; k <= N; k++) {
            if (k % 3 == 0) assertNull("should be removed " + k, m.get(k));
            else assertEquals(Long.valueOf(k), m.get(k));
        }

        // reinsert the removed keys with a distinct value
        for (long k = 0; k <= N; k++) {
            if (k % 3 == 0) { m.put(k, k + 1_000_000); oracle.put(k, k + 1_000_000); }
        }

        assertMatchesAcrossFlush(m, oracle);
    }

    // ---------- String keys and values ----------

    @Test
    public void stringKeysAndValues() {
        BufferTreeMap<String, String> m = stringMap(6, 64);
        TreeMap<String, String> oracle = new TreeMap<>();
        Random rnd = new Random(7);
        final int N = 8_000;

        for (int i = 0; i < N; i++) {
            String k = randomString(rnd, 1 + rnd.nextInt(12));
            String v = randomString(rnd, 1 + rnd.nextInt(20));
            oracle.put(k, v);
            m.put(k, v);
        }
        assertMatchesAcrossFlush(m, oracle);
    }

    /** Same coverage but exercises the binary {@link org.mapdb.ser.StringGroupFormat}
     *  read path (binarySearch/binaryGet over serialized bytes). */
    @Test
    public void stringKeysAndValuesBinary() {
        BufferTreeMap<String, String> m = stringMapBinary(6, 64);
        TreeMap<String, String> oracle = new TreeMap<>();
        Random rnd = new Random(7);
        final int N = 8_000;

        for (int i = 0; i < N; i++) {
            String k = randomString(rnd, 1 + rnd.nextInt(12));
            String v = randomString(rnd, 1 + rnd.nextInt(20));
            oracle.put(k, v);
            m.put(k, v);
        }

        assertMatches(m, oracle);
        // absent keys (different alphabet region) resolve to null in both buffered/flushed state
        for (int i = 0; i < 1000; i++) assertNull(m.get("ABSENT-" + i));
        m.flushAll();
        for (int i = 0; i < 1000; i++) assertNull(m.get("ABSENT-" + i));
        assertMatches(m, oracle);
    }

    private static String randomString(Random rnd, int len) {
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) sb.append((char) ('a' + rnd.nextInt(26)));
        return sb.toString();
    }

    // ---------- mixed formats: Long key (binary) + String value (non-binary) ----------

    @Test
    public void mixedLongKeyStringValue() {
        GroupFormat<Long> kf = LongFormat.INSTANCE;
        GroupFormat<String> vf = new ObjectArrayFormat<>(Serializers.STRING);
        BufferTreeMap<Long, String> m = BufferTreeMap.create(store, kf, vf, 6, 64);

        TreeMap<Long, String> oracle = new TreeMap<>();
        Random rnd = new Random(99);
        final int N = 8_000;
        for (int i = 0; i < N; i++) {
            long k = rnd.nextInt(N * 2);
            String v = "v" + k + "-" + rnd.nextInt(1000);
            oracle.put(k, v);
            m.put(k, v);
        }

        assertMatches(m, oracle);
        List<Long> got = new ArrayList<>();
        Iterator<Map.Entry<Long, String>> it = m.entryIterator();
        while (it.hasNext()) got.add(it.next().getKey());
        assertEquals(new ArrayList<>(oracle.keySet()), got);

        m.flushAll();
        assertMatches(m, oracle);
    }

    // ---------- buffer-specific: interleaved put/remove of one key, no flush ----------

    /** A single key churned put/remove/put... many times, staying entirely in buffers.
     *  Each op is verified immediately by get; LWW must resolve to the last write. */
    @Test
    public void interleavedPutRemoveSameKeyNoFlush() {
        BufferTreeMap<Long, Long> m = longMap(8, 64);
        final long KEY = 12345L;
        Random rnd = new Random(3);
        boolean present = false;
        long expected = 0;

        for (int i = 0; i < 5_000; i++) {
            if (rnd.nextBoolean()) {
                expected = rnd.nextLong();
                m.put(KEY, expected);
                present = true;
            } else {
                m.remove(KEY);
                present = false;
            }
            // get after every op — buffer-tail LWW must answer correctly
            if (present) {
                assertEquals("op " + i, Long.valueOf(expected), m.get(KEY));
                assertTrue(m.containsKey(KEY));
                assertEquals(1L, m.sizeLong());
            } else {
                assertNull("op " + i, m.get(KEY));
                assertFalse(m.containsKey(KEY));
                assertEquals(0L, m.sizeLong());
            }
        }

        // final state consistent across flush
        if (present) {
            assertEquals(Long.valueOf(expected), m.get(KEY));
            m.flushAll();
            assertEquals(Long.valueOf(expected), m.get(KEY));
            assertEquals(1L, m.sizeLong());
        } else {
            m.flushAll();
            assertNull(m.get(KEY));
            assertEquals(0L, m.sizeLong());
        }
    }

    /** Many distinct keys each churned put/remove several times with NO flush, so buffers
     *  at multiple levels hold overlapping ops for the same keys. get + iteration must
     *  reflect only the last op per key. */
    @Test
    public void interleavedManyKeysUnflushedMultiLevel() {
        BufferTreeMap<Long, Long> m = longMap(4, 48); // tiny -> deep tree, buffers at many levels
        TreeMap<Long, Long> oracle = new TreeMap<>();
        Random rnd = new Random(555);
        final int KEYSPACE = 2_000;

        for (int i = 0; i < 40_000; i++) {
            long k = rnd.nextInt(KEYSPACE);
            if (rnd.nextInt(100) < 55) {
                long v = rnd.nextLong();
                m.put(k, v); oracle.put(k, v);
            } else {
                m.remove(k); oracle.remove(k);
            }
        }
        // verify WITHOUT flushing first — this is the un-flushed multi-level read
        assertMatches(m, oracle);
        // then confirm flush doesn't change the answer
        m.flushAll();
        assertMatches(m, oracle);
    }

    /** flushAll called on an already-flushed map, repeatedly, is a stable no-op. */
    @Test
    public void flushAllIdempotence() {
        BufferTreeMap<Long, Long> m = longMap(8, 64);
        TreeMap<Long, Long> oracle = new TreeMap<>();
        Random rnd = new Random(2024);
        for (int i = 0; i < 10_000; i++) {
            long k = rnd.nextInt(5_000);
            if (rnd.nextInt(100) < 70) { long v = rnd.nextLong(); m.put(k, v); oracle.put(k, v); }
            else { m.remove(k); oracle.remove(k); }
        }
        m.flushAll();
        assertMatches(m, oracle);
        m.flushAll();
        assertMatches(m, oracle);
        m.flushAll();
        assertMatches(m, oracle);
    }

    // ---------- null rejection ----------

    @Test
    public void putNullKeyRejected() {
        BufferTreeMap<Long, Long> m = longMap(8, 64);
        try {
            m.put(null, 1L);
            fail("expected NullPointerException");
        } catch (NullPointerException expected) { /* ok */ }
    }

    @Test
    public void putNullValueRejected() {
        BufferTreeMap<Long, Long> m = longMap(8, 64);
        try {
            m.put(1L, null);
            fail("expected NullPointerException");
        } catch (NullPointerException expected) { /* ok */ }
    }

    @Test
    public void removeNullKeyRejected() {
        BufferTreeMap<Long, Long> m = longMap(8, 64);
        try {
            m.remove(null);
            fail("expected NullPointerException");
        } catch (NullPointerException expected) { /* ok */ }
    }

    // =====================================================================
    // java.util.Map / SortedMap interface conformance (the newly-added surface;
    // NOT a ConcurrentMap). Oracle = java.util.TreeMap; views are ASCENDING so
    // sequences are compared in order. Runs across every StoreDelta dialect.
    // =====================================================================

    /** Ascending-order comparison of every collection view against a SortedMap oracle,
     *  plus size/isEmpty/equals/hashCode/containsValue. */
    private static void assertAscendingViews(Map<Long, Long> m, SortedMap<Long, Long> oracle) {
        assertEquals("size", oracle.size(), m.size());
        assertEquals("isEmpty", oracle.isEmpty(), m.isEmpty());
        assertEquals("keySet order", new ArrayList<>(oracle.keySet()), new ArrayList<>(m.keySet()));
        assertEquals("values order", new ArrayList<>(oracle.values()), new ArrayList<>(m.values()));
        assertEquals("entrySet order", new ArrayList<>(oracle.entrySet()), new ArrayList<>(m.entrySet()));
        assertEquals("entrySet.size", oracle.size(), m.entrySet().size());
        assertEquals("keySet.size", oracle.size(), m.keySet().size());
        assertEquals("values.size", oracle.size(), m.values().size());
        for (Map.Entry<Long, Long> e : oracle.entrySet()) {
            assertTrue("keySet.contains " + e.getKey(), m.keySet().contains(e.getKey()));
            assertTrue("containsValue " + e.getValue(), m.containsValue(e.getValue()));
        }
        assertFalse("containsValue absent", m.containsValue(Long.MIN_VALUE + 12345L));
        assertTrue("map.equals(oracle)", m.equals(oracle));
        assertTrue("oracle.equals(map)", oracle.equals(m));
        assertEquals("hashCode", oracle.hashCode(), m.hashCode());
    }

    @Test
    public void mapSizeAndIsEmpty() {
        BufferTreeMap<Long, Long> m = longMap(8, 64);
        assertEquals(0, m.size());
        assertEquals(0L, m.sizeLong());
        assertTrue(m.isEmpty());
        for (long k = 0; k < 1_000; k++) m.put(k, k * 7);
        assertEquals(1_000, m.size());
        assertEquals((long) m.size(), m.sizeLong());
        assertFalse(m.isEmpty());
        for (long k = 0; k < 400; k++) m.remove(k);
        assertEquals(600, m.size());
        assertEquals((long) m.size(), m.sizeLong());
    }

    @Test
    public void collectionViewsAscendingMatchOracle() {
        BufferTreeMap<Long, Long> m = longMap(8, 64);
        TreeMap<Long, Long> oracle = new TreeMap<>();
        for (long k = 0; k < 1_500; k++) { m.put(k, k * 7 + 1); oracle.put(k, k * 7 + 1); }
        assertAscendingViews(m, oracle);           // buffered
        m.flushAll();
        assertAscendingViews(m, oracle);           // consolidated reads identically
    }

    /**
     * View mutation on a single-node map (large leaf headroom, so no flush/split occurs
     * mid-iteration): iterator.remove, keySet.removeAll, keySet/values.retainAll,
     * entrySet.remove(entry), keySet.contains — every mutation reflects in the live map.
     * A big leafHeadroom keeps every op in the one root-leaf buffer, so the live DFS
     * cursor a bulk-remove iterates is never structurally disturbed.
     */
    @Test
    public void viewMutationReflectsInMap() {
        // maxNodeSize 64 > 40 entries, leafHeadroom 8192 holds all buffered ops: one node.
        BufferTreeMap<Long, Long> m = BufferTreeMap.create(
                store, LongFormat.INSTANCE, LongFormat.INSTANCE, 64, 4096, 8192);
        TreeMap<Long, Long> oracle = new TreeMap<>();
        for (long k = 0; k < 40; k++) { m.put(k, k * 2); oracle.put(k, k * 2); }

        // entrySet().iterator().remove()
        Iterator<Map.Entry<Long, Long>> eit = m.entrySet().iterator();
        Long removedKey = eit.next().getKey();
        eit.remove();
        oracle.remove(removedKey);
        assertFalse(m.containsKey(removedKey));
        try { eit.remove(); fail("double remove must throw IllegalStateException"); }
        catch (IllegalStateException expected) { /* ok */ }

        assertTrue(m.keySet().contains(oracle.firstKey()));
        assertFalse(m.keySet().contains(9999L));

        // entrySet().remove(entry): wrong value no-op, matching value removes
        assertFalse(m.entrySet().remove(new AbstractMap.SimpleImmutableEntry<>(5L, -1L)));
        assertTrue(m.containsKey(5L));
        assertTrue(m.entrySet().remove(new AbstractMap.SimpleImmutableEntry<>(5L, 10L)));
        oracle.remove(5L);
        assertFalse(m.containsKey(5L));

        Set<Long> drop = new HashSet<>(Arrays.asList(10L, 11L, 12L, 13L));
        assertTrue(m.keySet().removeAll(drop));
        for (Long k : drop) oracle.remove(k);
        assertAscendingViews(m, oracle);

        Set<Long> keep = new HashSet<>();
        for (long k = 20; k < 30; k++) keep.add(k);
        m.keySet().retainAll(keep);
        oracle.keySet().retainAll(keep);
        assertAscendingViews(m, oracle);

        m.values().retainAll(new HashSet<>(Arrays.asList(-77L)));
        oracle.values().retainAll(new HashSet<>(Arrays.asList(-77L)));
        assertTrue(m.isEmpty());
        assertTrue(oracle.isEmpty());
    }

    @Test
    public void mapEqualsAndHashCode() {
        BufferTreeMap<Long, Long> m = longMap(8, 64);
        TreeMap<Long, Long> oracle = new TreeMap<>();
        for (long k = 0; k < 300; k++) { m.put(k, k + 1); oracle.put(k, k + 1); }
        assertTrue(m.equals(oracle));
        assertTrue(oracle.equals(m));
        assertEquals(oracle.hashCode(), m.hashCode());
        assertTrue(m.equals(m));      // reflexive
        m.put(0L, 999L);              // one differing value breaks equality
        assertFalse(m.equals(oracle));
        assertFalse(oracle.equals(m));
        m.put(0L, 1L);
        assertTrue(m.equals(oracle));
        m.put(-1L, -1L);              // an extra key breaks equality
        assertFalse(m.equals(oracle));
        BufferTreeMap<Long, Long> empty = longMap(8, 64);
        assertTrue(empty.equals(new TreeMap<Long, Long>()));
    }

    @Test
    public void putAllAndClearThroughMapInterface() {
        TreeMap<Long, Long> src = new TreeMap<>();
        for (long k = 0; k < 500; k++) src.put(k, k * 3 + 1);
        Map<Long, Long> m = longMap(8, 64);
        m.putAll(src);
        assertAscendingViews(m, src);
        m.clear();
        assertTrue(m.isEmpty());
        assertEquals(0, m.size());
        assertTrue(m.entrySet().isEmpty());
        assertFalse(m.entrySet().iterator().hasNext());
    }

    /** comparator()==null; firstKey/lastKey correct incl. after removing min/max, empty-map
     *  → NoSuchElementException, and the empty-leaf-skip (blind removeOnly + flushAll leaves
     *  empty leaves linked; firstKey/lastKey must still find the true min/max). */
    @Test
    public void comparatorFirstLastAndEmptyLeafSkip() {
        BufferTreeMap<Long, Long> m = longMap(8, 64);
        assertNull(m.comparator());
        try { m.firstKey(); fail("expected NoSuchElementException"); }
        catch (NoSuchElementException expected) { /* ok */ }
        try { m.lastKey(); fail("expected NoSuchElementException"); }
        catch (NoSuchElementException expected) { /* ok */ }

        final int N = 3_000;
        for (long k = 0; k < N; k++) m.putOnly(k, k);
        m.flushAll();
        assertEquals(Long.valueOf(0L), m.firstKey());
        assertEquals(Long.valueOf(N - 1), m.lastKey());

        // remove the current min and max, then a whole leftmost and rightmost region
        m.removeOnly(0L);
        m.removeOnly((long) (N - 1));
        for (long k = 1; k < 500; k++) m.removeOnly(k);
        for (long k = N - 2; k >= 2_500; k--) m.removeOnly(k);
        m.flushAll(); // consolidate the tombstones so leftmost/rightmost leaves go empty
        assertEquals("empty-leaf skip on firstKey", Long.valueOf(500L), m.firstKey());
        assertEquals("empty-leaf skip on lastKey", Long.valueOf(2_499L), m.lastKey());
    }

    /** headMap/tailMap/subMap: bound inclusivity, out-of-range put IAE, from>to IAE,
     *  nested submaps, bounded clear, submap first/last/size/isEmpty and ascending
     *  iteration matching the oracle submap. */
    @Test
    public void headTailSubMapSemantics() {
        BufferTreeMap<Long, Long> m = longMap(8, 64);
        TreeMap<Long, Long> oracle = new TreeMap<>();
        for (long k = 0; k < 200; k++) { m.put(k, k * 10); oracle.put(k, k * 10); }

        assertAscendingViews(m.headMap(50L), oracle.headMap(50L));
        assertAscendingViews(m.tailMap(150L), oracle.tailMap(150L));
        assertAscendingViews(m.subMap(50L, 150L), oracle.subMap(50L, 150L));

        SortedMap<Long, Long> sub = m.subMap(50L, 150L);
        assertNull(sub.comparator());
        assertTrue(sub.containsKey(50L));    // low inclusive
        assertFalse(sub.containsKey(150L));  // high exclusive
        assertNull(sub.get(150L));
        assertNull(sub.get(49L));
        assertEquals(Long.valueOf(50L), sub.firstKey());
        assertEquals(Long.valueOf(149L), sub.lastKey());
        assertEquals(100, sub.size());
        assertFalse(sub.isEmpty());

        try { sub.put(200L, 1L); fail("expected IllegalArgumentException"); }
        catch (IllegalArgumentException expected) { /* ok */ }
        try { sub.put(49L, 1L); fail("expected IllegalArgumentException"); }
        catch (IllegalArgumentException expected) { /* ok */ }
        assertEquals(Long.valueOf(75L * 10), sub.put(75L, 999L));
        oracle.put(75L, 999L);
        assertEquals(Long.valueOf(999L), m.get(75L));

        try { m.subMap(100L, 50L); fail("expected IllegalArgumentException"); }
        catch (IllegalArgumentException expected) { /* ok */ }

        SortedMap<Long, Long> nested = sub.subMap(80L, 120L);
        assertAscendingViews(nested, oracle.subMap(80L, 120L));
        try { sub.subMap(10L, 120L); fail("expected IllegalArgumentException"); }
        catch (IllegalArgumentException expected) { /* ok */ }
        try { sub.headMap(300L); fail("expected IllegalArgumentException"); }
        catch (IllegalArgumentException expected) { /* ok */ }

        m.subMap(100L, 150L).clear();
        for (long k = 100; k < 150; k++) oracle.remove(k);
        assertEquals(Long.valueOf(0L), m.get(0L));
        assertEquals(Long.valueOf(999L), m.get(75L));
        assertEquals(Long.valueOf(1500L), m.get(150L)); // exactly the exclusive high survives
        assertAscendingViews(m, oracle);

        SortedMap<Long, Long> emptySub = m.subMap(100L, 120L);
        assertTrue(emptySub.isEmpty());
        assertEquals(0, emptySub.size());
        try { emptySub.firstKey(); fail("expected NoSuchElementException"); }
        catch (NoSuchElementException expected) { /* ok */ }
        try { emptySub.lastKey(); fail("expected NoSuchElementException"); }
        catch (NoSuchElementException expected) { /* ok */ }
    }

    /** Value-returning put/remove in three states — BUFFERED (no flush), FLUSHED, and
     *  TOMBSTONE-HEAVY — with returned old values and final reads compared to the oracle.
     *  remove of an ABSENT key returns null (and appends no tombstone). */
    @Test
    public void valueReturningPutRemoveThreeStates() {
        BufferTreeMap<Long, Long> m = longMap(8, 64);
        TreeMap<Long, Long> oracle = new TreeMap<>();

        // ---- BUFFERED ----
        for (long k = 0; k < 2_000; k++) assertEquals(oracle.put(k, k * 3), m.put(k, k * 3)); // fresh: null
        for (long k = 0; k < 2_000; k += 50) assertEquals(oracle.put(k, -k), m.put(k, -k));    // overwrite: old
        for (long k = 0; k < 2_000; k += 7) assertEquals(oracle.remove(k), m.remove(k));       // remove: old
        assertNull("remove absent returns null", m.remove(1_000_000L));
        assertMatches(m, oracle);

        // ---- FLUSHED ----
        m.flushAll();
        for (long k = 1; k < 2_000; k += 13) assertEquals(oracle.put(k, k + 7), m.put(k, k + 7)); // over flushed base
        for (long k = 3; k < 2_000; k += 11) assertEquals(oracle.remove(k), m.remove(k));
        m.flushAll();
        assertMatches(m, oracle);
    }

    /** TOMBSTONE-HEAVY: mass removes (checking each returned old value), redundant removes
     *  return null, then reads match the oracle across flush. */
    @Test
    public void tombstoneHeavyPutRemoveReturns() {
        BufferTreeMap<Long, Long> m = longMap(8, 64);
        TreeMap<Long, Long> oracle = new TreeMap<>();
        for (long k = 0; k < 3_000; k++) { m.put(k, k); oracle.put(k, k); }
        for (long k = 0; k < 3_000; k += 2) assertEquals(oracle.remove(k), m.remove(k)); // even keys
        for (long k = 0; k < 200; k += 2) assertNull("already removed", m.remove(k));    // redundant
        assertMatches(m, oracle);
        m.flushAll();
        assertMatches(m, oracle);
    }

    /** The overridden default CAS methods (putIfAbsent/remove(k,v)/replace/replace(k,ov,nv))
     *  driven through a java.util.Map ref, oracle-checked across buffered and flushed states
     *  under the single writer (with periodic flushAll to interleave consolidation). */
    @Test
    public void casThroughMapInterfaceBufferedAndFlushed() {
        BufferTreeMap<Long, Long> bm = longMap(8, 64);
        Map<Long, Long> m = bm;
        TreeMap<Long, Long> oracle = new TreeMap<>();
        Random rnd = new Random(4242);
        final int KEYSPACE = 1_500, OPS = 15_000;

        for (int i = 0; i < OPS; i++) {
            long k = rnd.nextInt(KEYSPACE);
            int roll = rnd.nextInt(100);
            if (roll < 20) {
                long v = rnd.nextLong();
                assertEquals("put[" + i + "]", oracle.put(k, v), m.put(k, v));
            } else if (roll < 35) {
                long v = rnd.nextLong();
                assertEquals("putIfAbsent[" + i + "]", oracle.putIfAbsent(k, v), m.putIfAbsent(k, v));
            } else if (roll < 50) {
                long v = rnd.nextLong();
                assertEquals("replace[" + i + "]", oracle.replace(k, v), m.replace(k, v));
            } else if (roll < 64) {
                Long og = oracle.get(k);
                if (og == null || !rnd.nextBoolean()) og = rnd.nextLong(); // ~half match, ~half mismatch
                long nv = rnd.nextLong();
                assertEquals("replaceCAS[" + i + "]",
                        oracle.replace(k, og, nv), m.replace(k, og, nv));
            } else if (roll < 78) {
                Long vg = oracle.get(k);
                if (vg == null || !rnd.nextBoolean()) vg = rnd.nextLong();
                assertEquals("remove(k,v)[" + i + "]",
                        oracle.remove(k, vg), m.remove(k, vg));
            } else if (roll < 86) {
                assertEquals("remove[" + i + "]", oracle.remove(k), m.remove(k));
            } else {
                assertEquals("get[" + i + "]", oracle.get(k), m.get(k));
            }
            if ((i & 4095) == 4095) {
                bm.flushAll();
                assertEquals("size after mid flush", (long) oracle.size(), bm.sizeLong());
            }
        }
        assertMatchesAcrossFlush(bm, oracle);
    }

    /**
     * CAS atomicity when MANY threads call the CAS methods concurrently. BufferTreeMap is a
     * single-writer map — every CAS serializes on the map-global write lock — so these threads
     * CONTEND on the lock; the point is that each CAS remains atomic (never a lost update or a
     * double insert), not that writes scale. Two races:
     * (1) many threads {@code putIfAbsent} the same keys — exactly ONE insert wins per key;
     * (2) N threads each do M {@code replace(k, old, old+1)} CAS-retry increments on one shared
     * counter key — the final value must equal the total number of increments. Reads run
     * lock-free throughout. flushAll afterward must leave the outcome unchanged.
     */
    @Test
    public void casAtomicityUnderConcurrentCallers() throws Exception {
        assertTrue("dialect store must be thread-safe for this test", store.isThreadSafe());
        final BufferTreeMap<Long, Long> m = longMap(8, 64);

        // ---- race 1: single-winner putIfAbsent ----
        final int KEYS = 300, THREADS = 8;
        final AtomicInteger inserts = new AtomicInteger();
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final CyclicBarrier gate = new CyclicBarrier(THREADS);
        Thread[] ts = new Thread[THREADS];
        for (int t = 0; t < THREADS; t++) {
            final long tid = t;
            ts[t] = new Thread(() -> {
                try {
                    gate.await();
                    for (long k = 0; k < KEYS; k++) {
                        long val = tid * 1_000_000L + k; // val % 1_000_000 == k, tid recoverable
                        if (m.putIfAbsent(k, val) == null) inserts.incrementAndGet();
                    }
                } catch (Throwable e) { failure.compareAndSet(null, e); }
            });
        }
        for (Thread th : ts) th.start();
        for (Thread th : ts) th.join();
        if (failure.get() != null) throw new AssertionError("worker failed", failure.get());
        assertEquals("exactly one insert per key", KEYS, inserts.get());
        assertEquals(KEYS, (int) m.sizeLong());
        for (long k = 0; k < KEYS; k++) {
            Long v = m.get(k);
            assertNotNull("key " + k + " must be present", v);
            assertEquals("winner value must belong to some caller", k, v % 1_000_000L);
        }
        // outcome survives consolidation identically
        m.flushAll();
        assertEquals(KEYS, (int) m.sizeLong());
        for (long k = 0; k < KEYS; k++) assertEquals(k, m.get(k) % 1_000_000L);

        // ---- race 2: CAS-retry increments on one counter, no lost updates ----
        final long COUNTER = -1L;
        final int INC_THREADS = 6, INC_PER = 2_000;
        m.put(COUNTER, 0L);
        final AtomicReference<Throwable> failure2 = new AtomicReference<>();
        final CyclicBarrier gate2 = new CyclicBarrier(INC_THREADS);
        Thread[] its = new Thread[INC_THREADS];
        for (int t = 0; t < INC_THREADS; t++) {
            its[t] = new Thread(() -> {
                try {
                    gate2.await();
                    for (int i = 0; i < INC_PER; i++) {
                        for (;;) {
                            Long cur = m.get(COUNTER);
                            if (m.replace(COUNTER, cur, cur + 1)) break;
                        }
                    }
                } catch (Throwable e) { failure2.compareAndSet(null, e); }
            });
        }
        for (Thread th : its) th.start();
        for (Thread th : its) th.join();
        if (failure2.get() != null) throw new AssertionError("increment worker failed", failure2.get());
        assertEquals("CAS increments must not be lost",
                Long.valueOf((long) INC_THREADS * INC_PER), m.get(COUNTER));
        m.flushAll();
        assertEquals("increments survive flush",
                Long.valueOf((long) INC_THREADS * INC_PER), m.get(COUNTER));
    }

    /** Blind putOnly/removeOnly consistency + leak gate: a mix of blind messages then
     *  flushAll must read like the oracle and pass store.verify() (tearDown re-verifies). */
    @Test
    public void blindPathConsistencyAndLeakGate() {
        BufferTreeMap<Long, Long> m = longMap(8, 64);
        TreeMap<Long, Long> oracle = new TreeMap<>();
        Random rnd = new Random(1234);
        for (int i = 0; i < 20_000; i++) {
            long k = rnd.nextInt(3_000);
            if (rnd.nextInt(100) < 60) { long v = rnd.nextLong(); m.putOnly(k, v); oracle.put(k, v); }
            else { m.removeOnly(k); oracle.remove(k); } // removeOnly of absent is legal
        }
        assertMatches(m, oracle);   // buffered
        m.flushAll();
        store.verify();             // leak/consistency gate
        assertMatches(m, oracle);   // consolidated
    }

    /** Null-key / null-value rejection across the CAS + blind surface (put/remove null
     *  already covered above). */
    @Test
    public void casAndBlindNullRejection() {
        BufferTreeMap<Long, Long> m = longMap(8, 64);
        m.put(1L, 1L);
        Runnable[] calls = {
                () -> m.putOnly(null, 1L),
                () -> m.putOnly(1L, null),
                () -> m.removeOnly(null),
                () -> m.putIfAbsent(null, 1L),
                () -> m.putIfAbsent(1L, null),
                () -> m.replace(null, 1L),
                () -> m.replace(1L, null),
                () -> m.replace(null, 1L, 2L),
                () -> m.replace(1L, null, 2L),
                () -> m.replace(1L, 1L, null),
                () -> m.remove(null, 1L),
                () -> m.remove(1L, null),
        };
        for (Runnable r : calls) {
            try { r.run(); fail("expected NullPointerException"); }
            catch (NullPointerException expected) { /* ok */ }
        }
        assertEquals(Long.valueOf(1L), m.get(1L));
        assertEquals(1L, m.sizeLong());
    }

    // =====================================================================
    // NavigableMap conformance (BufferTreeMap implements NavigableMap, NOT
    // ConcurrentNavigableMap — single writer). Oracle = java.util.TreeMap and its
    // navigable views. Navigation must hold in BOTH the buffered state AND after
    // flushAll() (mirroring assertMatchesAcrossFlush), including tombstones in
    // reverse traversal. Small bufferBytes forces flushes/splits so nav is exercised
    // over buffered ops at multiple levels.
    // =====================================================================

    private BufferTreeMap<byte[], byte[]> byteArrayMap(int maxNodeSize, int bufferBytes) {
        return BufferTreeMap.create(store, ByteArrayFormat.INSTANCE, ByteArrayFormat.INSTANCE,
                maxNodeSize, bufferBytes);
    }

    /** Probe keys spanning below-min, exact-hit, absent-in-between, above-max, extremes.
     *  Map keys in the nav tests are multiples of 10, so odd/5-offset probes are absent. */
    private static final long[] NAV_PROBES = {
            Long.MIN_VALUE, -1L, 0L, 1L, 5L, 9L, 10L, 11L, 15L, 100L, 105L,
            995L, 1000L, 1005L, 1990L, 2000L, 2005L, 5000L, Long.MAX_VALUE
    };

    /** Assert an oracle entry and a map entry are equal (null-safe); the MAP entry must be
     *  an IMMUTABLE snapshot (setValue throws) since it comes from a *Entry/poll method. */
    private static void assertNavEntry(String msg, Map.Entry<Long, Long> exp, Map.Entry<Long, Long> act) {
        if (exp == null) { assertNull(msg + " expected null", act); return; }
        assertNotNull(msg + " expected non-null", act);
        assertEquals(msg + " key", exp.getKey(), act.getKey());
        assertEquals(msg + " value", exp.getValue(), act.getValue());
        try { act.setValue(-999L); fail(msg + " entry must be immutable"); }
        catch (UnsupportedOperationException expected) { /* ok */ }
    }

    /** Full navigable-surface comparison of a Long map/view against a Long NavigableMap oracle
     *  (both in the SAME orientation): first/last, lower/floor/ceiling/higher (Entry + Key),
     *  firstKey/lastKey (throwing when empty), ascending + descending iteration, key-sets. */
    private static void assertNavLong(NavigableMap<Long, Long> m, NavigableMap<Long, Long> o) {
        assertEquals("comparator null-parity", o.comparator() == null, m.comparator() == null);
        assertEquals("size", o.size(), m.size());
        assertEquals("isEmpty", o.isEmpty(), m.isEmpty());

        assertNavEntry("firstEntry", o.firstEntry(), m.firstEntry());
        assertNavEntry("lastEntry", o.lastEntry(), m.lastEntry());

        if (o.isEmpty()) {
            try { m.firstKey(); fail("firstKey on empty must throw"); }
            catch (NoSuchElementException expected) { /* ok */ }
            try { m.lastKey(); fail("lastKey on empty must throw"); }
            catch (NoSuchElementException expected) { /* ok */ }
        } else {
            assertEquals("firstKey", o.firstKey(), m.firstKey());
            assertEquals("lastKey", o.lastKey(), m.lastKey());
        }

        for (long p : NAV_PROBES) {
            assertNavEntry("lowerEntry(" + p + ")", o.lowerEntry(p), m.lowerEntry(p));
            assertNavEntry("floorEntry(" + p + ")", o.floorEntry(p), m.floorEntry(p));
            assertNavEntry("ceilingEntry(" + p + ")", o.ceilingEntry(p), m.ceilingEntry(p));
            assertNavEntry("higherEntry(" + p + ")", o.higherEntry(p), m.higherEntry(p));
            assertEquals("lowerKey(" + p + ")", o.lowerKey(p), m.lowerKey(p));
            assertEquals("floorKey(" + p + ")", o.floorKey(p), m.floorKey(p));
            assertEquals("ceilingKey(" + p + ")", o.ceilingKey(p), m.ceilingKey(p));
            assertEquals("higherKey(" + p + ")", o.higherKey(p), m.higherKey(p));
        }

        assertEquals("ascending entries", new ArrayList<>(o.entrySet()), new ArrayList<>(m.entrySet()));
        assertEquals("ascending navigableKeySet", new ArrayList<>(o.navigableKeySet()),
                new ArrayList<>(m.navigableKeySet()));
        assertEquals("descending entries", new ArrayList<>(o.descendingMap().entrySet()),
                new ArrayList<>(m.descendingMap().entrySet()));
        assertEquals("descendingKeySet", new ArrayList<>(o.descendingKeySet()),
                new ArrayList<>(m.descendingKeySet()));
    }

    /** Nav must read identically buffered, after flushAll, and after a second flushAll. */
    private static void assertNavLongAcrossFlush(BufferTreeMap<Long, Long> m, NavigableMap<Long, Long> o) {
        assertNavLong(m, o);
        m.flushAll();
        assertNavLong(m, o);
        m.flushAll();
        assertNavLong(m, o);
    }

    private static BufferTreeMap<Long, Long> filledLongMap(BufferTreeMapTCK t, TreeMap<Long, Long> oracle,
                                                           int keys, int step) {
        BufferTreeMap<Long, Long> m = t.longMap(4, 48); // tiny -> deep tree, buffers at many levels
        for (int i = 0; i < keys; i++) {
            long k = (long) i * step;
            long v = k * 7 + 1;
            m.put(k, v);
            oracle.put(k, v);
        }
        return m;
    }

    /** floor/lower/ceiling/higher/first/last (+ *Key) vs the TreeMap oracle across
     *  buffered and flushed states; plus the empty-map contract (null / throwing). */
    @Test
    public void navFloorCeilingLowerHigherAcrossFlush() {
        // empty map first
        BufferTreeMap<Long, Long> empty = longMap(4, 48);
        assertNavLong(empty, new TreeMap<>());
        assertNull(empty.firstEntry());
        assertNull(empty.lastEntry());
        assertNull(empty.floorEntry(5L));
        assertNull(empty.ceilingKey(5L));

        TreeMap<Long, Long> oracle = new TreeMap<>();
        BufferTreeMap<Long, Long> m = filledLongMap(this, oracle, 201, 10); // keys 0,10,..,2000
        assertNavLongAcrossFlush(m, oracle);
    }

    /** pollFirstEntry/pollLastEntry: single-threaded ordering + emptying matches the oracle,
     *  entries are immutable snapshots, and draining works whether the victim is a buffered
     *  PUT (extremes put after flushAll) or a consolidated base entry. */
    @Test
    public void navPollDrainsBufferedAndConsolidated() {
        BufferTreeMap<Long, Long> m = longMap(4, 48);
        TreeMap<Long, Long> oracle = new TreeMap<>();
        for (long k = 0; k < 400; k++) { m.put(k * 10, k); oracle.put(k * 10, k); }
        m.flushAll(); // everything is now a consolidated base entry
        // buffered PUTs at the extremes + middle: these become the first poll victims
        for (long k : new long[]{-100L, -50L, 4000L, 4500L, 1234L}) { m.put(k, k); oracle.put(k, k); }

        boolean pollFirst = true;
        while (!oracle.isEmpty()) {
            if (pollFirst) {
                assertNavEntry("pollFirstEntry", oracle.pollFirstEntry(), m.pollFirstEntry());
            } else {
                assertNavEntry("pollLastEntry", oracle.pollLastEntry(), m.pollLastEntry());
            }
            pollFirst = !pollFirst;
            assertEquals("size after poll", (long) oracle.size(), m.sizeLong());
            if ((oracle.size() & 63) == 0) m.flushAll(); // consolidate remaining tombstones
        }
        assertNull("pollFirst on empty", m.pollFirstEntry());
        assertNull("pollLast on empty", m.pollLastEntry());
        assertTrue(m.isEmpty());
        assertEquals(0L, m.sizeLong());
        m.flushAll();
        assertTrue(m.isEmpty());
        assertNull(m.pollFirstEntry());
    }

    /** TOMBSTONES in REVERSE traversal: buffer removes (removeOnly + remove) so tombstones
     *  are UN-FLUSHED, assert descendingMap iteration and floor/lower correct; then flushAll
     *  and assert again. */
    @Test
    public void navTombstonesInReverseTraversal() {
        BufferTreeMap<Long, Long> m = longMap(4, 48);
        TreeMap<Long, Long> oracle = new TreeMap<>();
        for (long k = 0; k < 300; k++) { m.put(k * 10, k); oracle.put(k * 10, k); }
        m.flushAll(); // consolidate the base so the removes below stay as un-flushed tombstones

        // buffered tombstones via BOTH removeOnly (blind) and remove (value-returning)
        for (long k = 0; k < 300; k += 3) { m.removeOnly(k * 10); oracle.remove(k * 10); }
        for (long k = 1; k < 300; k += 7) { m.remove(k * 10); oracle.remove(k * 10); }

        // reverse iteration + floor/lower must respect the un-flushed tombstones
        assertNavLong(m, oracle);           // buffered tombstones
        m.flushAll();
        assertNavLong(m, oracle);           // tombstones consolidated away
        m.flushAll();
        assertNavLong(m, oracle);
    }

    /** descendingMap: orientation method mapping, reversed non-null comparator, nested descending
     *  subMaps, and descendingMap().descendingMap() behavioral identity with the original. */
    @Test
    public void descendingMapMappingAndDoubleReverse() {
        TreeMap<Long, Long> oracle = new TreeMap<>();
        BufferTreeMap<Long, Long> m = filledLongMap(this, oracle, 101, 10); // keys 0..1000
        m.flushAll();

        NavigableMap<Long, Long> dm = m.descendingMap();
        NavigableMap<Long, Long> odm = oracle.descendingMap();

        // comparator: reversed and NON-null even for natural order
        assertNull(m.comparator());
        assertNotNull(dm.comparator());
        assertTrue("descending comparator must reverse", dm.comparator().compare(1L, 2L) > 0);
        assertEquals(Collections.reverseOrder(), dm.comparator());

        // orientation mapping: descending firstEntry = backing greatest, etc. — oracle-checked wholesale
        assertNavLong(dm, odm);

        // spot-check the orientation flips directly against the ascending map
        assertEquals(m.lastEntry(), dm.firstEntry());
        assertEquals(m.firstEntry(), dm.lastEntry());
        assertEquals(m.higherEntry(500L), dm.lowerEntry(500L));   // desc lower = backing higher
        assertEquals(m.ceilingEntry(505L), dm.floorEntry(505L));  // desc floor = backing ceiling
        assertEquals(m.floorEntry(505L), dm.ceilingEntry(505L));  // desc ceiling = backing floor
        assertEquals(m.lowerEntry(500L), dm.higherEntry(500L));   // desc higher = backing lower

        // descendingMap().descendingMap() behaves as the original ascending orientation
        NavigableMap<Long, Long> ddm = dm.descendingMap();
        assertNull("double-reverse comparator back to natural (null)", ddm.comparator());
        assertNavLong(ddm, oracle);

        // nested descending subMap vs the oracle's nested descending subMap
        assertNavLong(dm.subMap(800L, true, 200L, false), odm.subMap(800L, true, 200L, false));
        assertNavLong(dm.headMap(300L, true), odm.headMap(300L, true));
        assertNavLong(dm.tailMap(700L, false), odm.tailMap(700L, false));
    }

    /** navigableKeySet / descendingKeySet: order, subSet/headSet/tailSet (inclusive flags),
     *  pollFirst/pollLast on the SET, contains/remove/iterator.remove reflect the backing map,
     *  add() throws UnsupportedOperationException. */
    @Test
    public void navigableKeySetAndDescendingKeySet() {
        BufferTreeMap<Long, Long> m = longMap(4, 48);
        TreeMap<Long, Long> oracle = new TreeMap<>();
        for (long k = 0; k < 100; k++) { m.put(k, k * 3); oracle.put(k, k * 3); }

        NavigableSet<Long> ks = m.navigableKeySet();
        NavigableSet<Long> oks = oracle.navigableKeySet();

        assertEquals(new ArrayList<>(oks), new ArrayList<>(ks));
        assertEquals(new ArrayList<>(oracle.descendingKeySet()), new ArrayList<>(m.descendingKeySet()));
        assertNull(ks.comparator());
        assertNotNull(m.descendingKeySet().comparator());

        // subSet/headSet/tailSet with inclusive flags
        assertEquals(new ArrayList<>(oks.subSet(30L, true, 70L, false)),
                new ArrayList<>(ks.subSet(30L, true, 70L, false)));
        assertEquals(new ArrayList<>(oks.subSet(30L, false, 70L, true)),
                new ArrayList<>(ks.subSet(30L, false, 70L, true)));
        assertEquals(new ArrayList<>(oks.headSet(50L, true)), new ArrayList<>(ks.headSet(50L, true)));
        assertEquals(new ArrayList<>(oks.headSet(50L, false)), new ArrayList<>(ks.headSet(50L, false)));
        assertEquals(new ArrayList<>(oks.tailSet(50L, true)), new ArrayList<>(ks.tailSet(50L, true)));
        assertEquals(new ArrayList<>(oks.tailSet(50L, false)), new ArrayList<>(ks.tailSet(50L, false)));

        // add() unsupported
        try { ks.add(5L); fail("add on key-set must throw"); }
        catch (UnsupportedOperationException expected) { /* ok */ }

        // contains reflects the map
        assertTrue(ks.contains(42L));
        assertFalse(ks.contains(1000L));

        // remove(key) on the set mutates the map
        assertTrue(ks.remove(42L));
        assertFalse(m.containsKey(42L));
        oracle.remove(42L);
        assertFalse(ks.remove(42L)); // already gone

        // iterator.remove() mutates the map
        Iterator<Long> it = ks.iterator();
        Long firstKey = it.next();
        it.remove();
        assertFalse(m.containsKey(firstKey));
        oracle.remove(firstKey);

        // pollFirst / pollLast on the SET drain the map, in order
        assertEquals(oracle.navigableKeySet().pollFirst(), ks.pollFirst());
        assertEquals(oracle.navigableKeySet().pollLast(), ks.pollLast());
        assertEquals("size after set polls", (long) oracle.size(), m.sizeLong());
        assertEquals(new ArrayList<>(oracle.navigableKeySet()), new ArrayList<>(m.navigableKeySet()));
    }

    /** All fromInc×toInc combinations on subMap/headMap/tailMap; exclusive-empty ranges;
     *  out-of-range navigation on bounded views; nested views not widening exclusive parent
     *  bounds; out-of-parent-range bound → IAE; from>to → IAE. */
    @Test
    public void boundInclusivityCombinationsAndBoundedNavigation() {
        BufferTreeMap<Long, Long> m = longMap(4, 48);
        TreeMap<Long, Long> oracle = new TreeMap<>();
        for (long k = 0; k < 1000; k++) { m.put(k, k * 2); oracle.put(k, k * 2); }
        m.flushAll();

        // all 4 fromInc x toInc combinations
        for (boolean fi : new boolean[]{false, true}) {
            for (boolean ti : new boolean[]{false, true}) {
                assertNavLong(m.subMap(200L, fi, 800L, ti), oracle.subMap(200L, fi, 800L, ti));
            }
            assertNavLong(m.headMap(600L, fi), oracle.headMap(600L, fi));
            assertNavLong(m.tailMap(400L, fi), oracle.tailMap(400L, fi));
        }

        // exclusive-empty ranges
        assertTrue(m.subMap(300L, false, 300L, false).isEmpty());
        assertTrue(m.subMap(300L, true, 300L, false).isEmpty());
        assertTrue(m.subMap(300L, false, 300L, true).isEmpty());
        assertEquals(1, m.subMap(300L, true, 300L, true).size());
        assertTrue(m.subMap(300L, true, 301L, false).size() == 1);
        NavigableMap<Long, Long> emptyEx = m.subMap(300L, false, 300L, false);
        assertFalse(emptyEx.entrySet().iterator().hasNext());
        try { emptyEx.firstKey(); fail("firstKey on empty exclusive range"); }
        catch (NoSuchElementException expected) { /* ok */ }
        try { emptyEx.lastKey(); fail("lastKey on empty exclusive range"); }
        catch (NoSuchElementException expected) { /* ok */ }

        // out-of-range navigation on a bounded view [100,700)
        NavigableMap<Long, Long> sub = m.subMap(100L, true, 700L, false);
        assertNull("ceiling above range", sub.ceilingEntry(800L));
        assertNull("higher of excluded high", sub.higherEntry(700L)); // 700 excluded
        assertNull("floor below range", sub.floorEntry(50L));
        assertNull("lower of low bound", sub.lowerEntry(100L));
        assertEquals(Long.valueOf(100L), sub.firstKey());
        assertEquals(Long.valueOf(699L), sub.lastKey());
        assertNull(sub.get(700L));       // excluded high not visible
        assertNull(sub.get(50L));        // below range

        // an INCLUSIVE nested bound equal to the parent's EXCLUSIVE bound throws, like TreeMap
        try { sub.subMap(200L, true, 700L, true); fail("inclusive 700 vs exclusive parent hi must throw IAE"); }
        catch (IllegalArgumentException expected) { /* ok */ }
        // an EXCLUSIVE nested bound at the same value is fine and must not widen the parent
        NavigableMap<Long, Long> nested = sub.subMap(200L, true, 700L, false);
        assertNull("nested must not widen parent exclusive high", nested.get(700L));
        assertFalse(nested.containsKey(700L));
        assertEquals(Long.valueOf(699L), nested.lastKey());
        assertNull(nested.higherEntry(699L));

        // nested descending must not widen either
        NavigableMap<Long, Long> dsub = sub.descendingMap();          // keys (700,100] descending... [100,700)
        NavigableMap<Long, Long> dnested = dsub.subMap(699L, true, 100L, true);
        assertNull(dnested.get(700L));
        assertEquals(Long.valueOf(699L), dnested.firstKey());        // descending first = greatest

        // out-of-parent-range bound → IAE
        try { sub.subMap(50L, true, 600L, false); fail("below parent lo must throw IAE"); }
        catch (IllegalArgumentException expected) { /* ok */ }
        try { sub.subMap(200L, true, 800L, false); fail("above parent hi must throw IAE"); }
        catch (IllegalArgumentException expected) { /* ok */ }
        try { sub.headMap(900L, false); fail("headMap above parent hi must throw IAE"); }
        catch (IllegalArgumentException expected) { /* ok */ }
        try { sub.tailMap(50L, true); fail("tailMap below parent lo must throw IAE"); }
        catch (IllegalArgumentException expected) { /* ok */ }

        // from > to (strict) → IAE
        try { m.subMap(500L, true, 200L, false); fail("from>to must throw IAE"); }
        catch (IllegalArgumentException expected) { /* ok */ }
    }

    /** Empty PERSISTENT leaves after heavy removes: predecessor (floor/lower/lastEntry) AND
     *  descending iteration remain correct vs the oracle, buffered and flushed. Blind removes
     *  + flushAll leave empty leaves linked in the base tree. */
    @Test
    public void navOverEmptyPersistentLeaves() {
        BufferTreeMap<Long, Long> m = longMap(4, 48);
        TreeMap<Long, Long> oracle = new TreeMap<>();
        final int N = 3000;
        for (long k = 0; k < N; k++) { m.putOnly(k * 10, k); oracle.put(k * 10, k); }
        m.flushAll();

        // carve out large contiguous regions (whole leaves) at both ends and the middle
        for (long k = 0; k < 500; k++) { m.removeOnly(k * 10); oracle.remove(k * 10); }
        for (long k = N - 500; k < N; k++) { m.removeOnly(k * 10); oracle.remove(k * 10); }
        for (long k = 1200; k < 1800; k++) { m.removeOnly(k * 10); oracle.remove(k * 10); }

        // buffered tombstones: predecessor / descending must still be right
        assertNavLong(m, oracle);
        m.flushAll(); // consolidate -> empty persistent leaves remain linked
        assertNavLong(m, oracle);
        m.flushAll();
        assertNavLong(m, oracle);

        // explicit predecessor probes straddling the emptied middle gap
        assertEquals(Long.valueOf(11990L), m.floorKey(12000L)); // 1199*10, last before the gap
        assertEquals(Long.valueOf(11990L), m.lowerKey(12005L));
        assertEquals(Long.valueOf(18000L), m.ceilingKey(12000L)); // 1800*10, first after the gap
        assertEquals(Long.valueOf(5000L), m.firstKey());          // 500*10
        assertEquals(Long.valueOf((N - 501) * 10), m.lastKey());
    }

    /** comparator reversal for NON-natural order (ByteArrayFormat, unsigned byte[] order):
     *  ascending + descending navigation oracle-checked against TreeMap<>(map.comparator());
     *  plus the natural-order (Long) case: comparator()==null, descending reversed non-null. */
    @Test
    public void comparatorReversalNaturalAndByteArray() {
        // ---- natural order (Long): comparator null, descending reversed non-null ----
        BufferTreeMap<Long, Long> lm = longMap(4, 48);
        lm.put(1L, 1L); lm.put(2L, 2L);
        assertNull(lm.comparator());
        Comparator<? super Long> ldesc = lm.descendingMap().comparator();
        assertNotNull(ldesc);
        assertTrue(ldesc.compare(1L, 2L) > 0);

        // ---- non-natural order (ByteArrayFormat, unsigned lexicographic) ----
        BufferTreeMap<byte[], byte[]> m = byteArrayMap(4, 48);
        Comparator<? super byte[]> cmp = m.comparator();
        assertNotNull("ByteArrayFormat map must expose a non-null comparator", cmp);
        // sanity: unsigned order (0x80 > 0x7F unsigned)
        assertTrue(cmp.compare(new byte[]{(byte) 0x80}, new byte[]{0x7F}) > 0);

        TreeMap<byte[], byte[]> oracle = new TreeMap<>(cmp);
        Random rnd = new Random(31);
        List<byte[]> keys = new ArrayList<>();
        for (int i = 0; i < 2000; i++) {
            byte[] k = randomBytes(rnd, 1 + rnd.nextInt(6));
            byte[] v = randomBytes(rnd, 1 + rnd.nextInt(6));
            m.put(k, v);
            oracle.put(k, v);
            keys.add(k);
        }
        // probes: some existing keys, some fresh (absent), plus extremes
        List<byte[]> probes = new ArrayList<>();
        for (int i = 0; i < keys.size(); i += 137) probes.add(keys.get(i));
        for (int i = 0; i < 30; i++) probes.add(randomBytes(rnd, 1 + rnd.nextInt(8)));
        probes.add(new byte[]{0});
        probes.add(new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF});

        assertByteNav(m, oracle, probes);        // buffered
        m.flushAll();
        assertByteNav(m, oracle, probes);        // consolidated
        m.flushAll();
        assertByteNav(m, oracle, probes);

        // descending comparator reverses the (non-natural) backing comparator
        Comparator<? super byte[]> dcmp = m.descendingMap().comparator();
        assertNotNull(dcmp);
        byte[] a = {0x01}, b = {0x02};
        assertTrue("desc reverses cmp", Integer.signum(dcmp.compare(a, b)) == -Integer.signum(cmp.compare(a, b)));
    }

    private static byte[] randomBytes(Random rnd, int len) {
        byte[] b = new byte[len];
        rnd.nextBytes(b);
        return b;
    }

    private static void assertByteKey(String msg, byte[] exp, byte[] act) {
        if (exp == null) { assertNull(msg, act); return; }
        assertArrayEquals(msg, exp, act);
    }

    private static void assertByteKeyList(String msg, List<byte[]> exp, List<byte[]> act) {
        assertEquals(msg + " size", exp.size(), act.size());
        for (int i = 0; i < exp.size(); i++) assertArrayEquals(msg + "[" + i + "]", exp.get(i), act.get(i));
    }

    /** ByteArray navigation vs a TreeMap oracle sharing the map's comparator: ascending +
     *  descending key order, first/last, floor/ceiling/lower/higher keys, and value bytes. */
    private static void assertByteNav(NavigableMap<byte[], byte[]> m, NavigableMap<byte[], byte[]> o,
                                      List<byte[]> probes) {
        assertByteKeyList("ascending keys", new ArrayList<>(o.navigableKeySet()),
                new ArrayList<>(m.navigableKeySet()));
        assertByteKeyList("descending keys", new ArrayList<>(o.descendingKeySet()),
                new ArrayList<>(m.descendingKeySet()));
        assertEquals("size", o.size(), m.size());
        if (o.isEmpty()) {
            try { m.firstKey(); fail("firstKey empty"); } catch (NoSuchElementException ok) { /* ok */ }
        } else {
            assertArrayEquals("firstKey", o.firstKey(), m.firstKey());
            assertArrayEquals("lastKey", o.lastKey(), m.lastKey());
        }
        for (Map.Entry<byte[], byte[]> e : o.entrySet()) {
            assertArrayEquals("get " + Arrays.toString(e.getKey()), e.getValue(), m.get(e.getKey()));
        }
        for (byte[] p : probes) {
            assertByteKey("floorKey", o.floorKey(p), m.floorKey(p));
            assertByteKey("ceilingKey", o.ceilingKey(p), m.ceilingKey(p));
            assertByteKey("lowerKey", o.lowerKey(p), m.lowerKey(p));
            assertByteKey("higherKey", o.higherKey(p), m.higherKey(p));
        }
    }

    /** Every entry returned by a *Entry / poll method is an IMMUTABLE snapshot (setValue
     *  throws) — on the base map, a descending view, and a bounded sub-view. */
    @Test
    public void navEntriesAreImmutableSnapshots() {
        BufferTreeMap<Long, Long> m = longMap(4, 48);
        for (long k = 0; k < 50; k++) m.put(k, k * 2);

        List<Map.Entry<Long, Long>> entries = new ArrayList<>();
        entries.add(m.firstEntry());
        entries.add(m.lastEntry());
        entries.add(m.lowerEntry(25L));
        entries.add(m.floorEntry(25L));
        entries.add(m.ceilingEntry(25L));
        entries.add(m.higherEntry(25L));
        NavigableMap<Long, Long> dm = m.descendingMap();
        entries.add(dm.firstEntry());
        entries.add(dm.floorEntry(25L));
        NavigableMap<Long, Long> sub = m.subMap(10L, true, 40L, false);
        entries.add(sub.firstEntry());
        entries.add(sub.ceilingEntry(20L));
        // poll methods too (drains, but the returned entry must still be immutable)
        entries.add(m.pollFirstEntry());
        entries.add(m.pollLastEntry());
        for (Map.Entry<Long, Long> e : entries) {
            assertNotNull(e);
            try { e.setValue(-1L); fail("returned entry must be immutable"); }
            catch (UnsupportedOperationException expected) { /* ok */ }
        }
    }

    /** Null search key → NPE on the base map AND on sub-views/descending view; get/containsKey
     *  reject null too. (Do NOT assert ClassCastException handling.) */
    @Test
    public void navNullSearchKeyThrowsNPE() {
        BufferTreeMap<Long, Long> m = longMap(4, 48);
        for (long k = 0; k < 20; k++) m.put(k, k);
        List<NavigableMap<Long, Long>> views = List.of(
                m, m.descendingMap(), m.subMap(2L, true, 18L, false), m.headMap(10L, false), m.tailMap(5L, true));
        for (NavigableMap<Long, Long> v : views) {
            Runnable[] calls = {
                    () -> v.lowerEntry(null), () -> v.floorEntry(null),
                    () -> v.ceilingEntry(null), () -> v.higherEntry(null),
                    () -> v.lowerKey(null), () -> v.floorKey(null),
                    () -> v.ceilingKey(null), () -> v.higherKey(null),
                    () -> v.get(null), () -> v.containsKey(null),
            };
            for (Runnable r : calls) {
                try { r.run(); fail("expected NullPointerException for null search key"); }
                catch (NullPointerException expected) { /* ok */ }
            }
        }
    }

    /** Fill a fresh long map with the EVEN keys in [0,200) → value key*10, so every odd key
     *  in that span is a known-absent in-range probe for the CAS truth table. */
    private BufferTreeMap<Long, Long> evenFilledLongMap(TreeMap<Long, Long> oracle) {
        BufferTreeMap<Long, Long> m = longMap(8, 64);
        for (long k = 0; k < 200; k++) {
            if ((k & 1L) == 0L) { m.put(k, k * 10); oracle.put(k, k * 10); }
        }
        return m;
    }

    /** Run the full ConcurrentMap CAS truth table THROUGH a bounded concurrent view, all keys
     *  in-range: {@code a1} absent (gets inserted), {@code p} present (gets swapped), {@code a2}
     *  absent throughout. Results are checked AND cross-checked against the backing map + oracle. */
    private void casTruthTableInRange(ConcurrentNavigableMap<Long, Long> view,
                                      BufferTreeMap<Long, Long> m, TreeMap<Long, Long> oracle,
                                      long a1, long p, long a2) {
        assertNull("precondition a1 absent", view.get(a1));
        assertNull("precondition a2 absent", view.get(a2));
        assertNotNull("precondition p present", oracle.get(p));

        // putIfAbsent: inserts (returns null), then returns the existing value (no overwrite)
        assertNull("putIfAbsent insert", view.putIfAbsent(a1, 111L));
        oracle.put(a1, 111L);
        assertEquals(Long.valueOf(111L), m.get(a1));
        assertEquals(Long.valueOf(111L), view.putIfAbsent(a1, 222L)); // present → existing
        assertEquals("putIfAbsent must not overwrite", Long.valueOf(111L), m.get(a1));

        // replace(k,v): absent → null no-op; present → swap returns old
        assertNull("replace(k,v) absent no-op", view.replace(a2, 999L));
        assertFalse("replace(k,v) absent must not insert", m.containsKey(a2));
        Long oldP = oracle.get(p);
        assertEquals("replace(k,v) present returns old", oldP, view.replace(p, 333L));
        oracle.put(p, 333L);
        assertEquals(Long.valueOf(333L), m.get(p));

        // replace(k,ov,nv): wrong-expected → false; matching → true; absent → false
        assertFalse("replace3 wrong expected", view.replace(p, oldP, 444L)); // oldP now stale
        assertEquals("replace3 mismatch must not mutate", Long.valueOf(333L), m.get(p));
        assertTrue("replace3 matching", view.replace(p, 333L, 444L));
        oracle.put(p, 444L);
        assertEquals(Long.valueOf(444L), m.get(p));
        assertFalse("replace3 absent key", view.replace(a2, 1L, 2L));
        assertFalse("replace3 absent must not insert", m.containsKey(a2));
    }

    /** The full CAS truth table driven THROUGH concurrent bounded sub-views — subMap, headMap,
     *  tailMap AND a doubly-nested subMap — each over a disjoint key region. Results and the
     *  backing map are cross-checked against a TreeMap oracle, then {@code assertMatchesAcrossFlush}
     *  proves the CAS mutations survive consolidation identically. */
    @Test
    public void casThroughConcurrentSubViewsMatchesOracle() {
        TreeMap<Long, Long> oracle = new TreeMap<>();
        BufferTreeMap<Long, Long> m = evenFilledLongMap(oracle);

        ConcurrentNavigableMap<Long, Long> sub  = m.subMap(50L, true, 150L, false);
        ConcurrentNavigableMap<Long, Long> head = m.headMap(80L, false);
        ConcurrentNavigableMap<Long, Long> tail = m.tailMap(120L, true);
        ConcurrentNavigableMap<Long, Long> nested =
                m.subMap(50L, true, 150L, false).subMap(90L, true, 110L, false);

        casTruthTableInRange(sub,    m, oracle, 51L, 60L, 53L);
        casTruthTableInRange(head,   m, oracle, 71L, 70L, 73L);
        casTruthTableInRange(tail,   m, oracle, 121L, 130L, 123L);
        casTruthTableInRange(nested, m, oracle, 91L, 100L, 93L);

        assertMatchesAcrossFlush(m, oracle);
    }

    /** Bound-checking contract on the concurrent sub-view CAS surface (mirrors
     *  {@link ConcurrentOrderedNavigableView}): putIfAbsent on an OUT-OF-RANGE key throws
     *  IllegalArgumentException, while replace(k,v)/replace(k,ov,nv) SHORT-CIRCUIT out-of-range
     *  keys as no-ops (null / false) WITHOUT touching the backing map — even when the key exists
     *  in the backing map just outside the view's bounds. */
    @Test
    public void casOnConcurrentSubViewRejectsOutOfRange() {
        TreeMap<Long, Long> oracle = new TreeMap<>();
        BufferTreeMap<Long, Long> m = evenFilledLongMap(oracle); // evens 0..198 present
        ConcurrentNavigableMap<Long, Long> view = m.subMap(50L, true, 150L, false);

        // an out-of-range key that is ABSENT (200) and one that is PRESENT in the backing map (10)
        for (long oor : new long[]{200L, 10L, 150L /* excluded high */, 49L}) {
            try { view.putIfAbsent(oor, 1L); fail("putIfAbsent out of range must throw IAE @" + oor); }
            catch (IllegalArgumentException expected) { /* ok */ }
            assertNull("replace(k,v) out of range no-op @" + oor, view.replace(oor, 1L));
            assertFalse("replace3 out of range no-op @" + oor, view.replace(oor, 100L, 1L));
        }
        // the backing entry just below the view (10 → 100) is untouched; nothing was inserted
        assertEquals(Long.valueOf(100L), m.get(10L));
        assertFalse(m.containsKey(200L));
        assertEquals(Long.valueOf(1500L), m.get(150L)); // excluded-high entry unchanged
        assertMatchesAcrossFlush(m, oracle);            // no mutation reached the map
    }

    /** Null-arg NPE matrix for putIfAbsent/replace/replace(k,ov,nv) driven THROUGH concurrent
     *  sub-views (bounded, nested and descending). Null key or null value → NPE, mirroring the
     *  top-level surface; the in-range probe key exists in every view. */
    @Test
    public void casNullArgsThrowNPEThroughViews() {
        BufferTreeMap<Long, Long> m = longMap(8, 64);
        for (long k = 0; k < 200; k++) m.put(k, k);
        final long K = 100L; // in range of every view below
        List<ConcurrentNavigableMap<Long, Long>> views = List.of(
                m.subMap(2L, true, 198L, false),
                m.headMap(150L, false),
                m.tailMap(50L, true),
                m.descendingMap(),
                m.subMap(2L, true, 198L, false).subMap(50L, true, 150L, false),
                m.descendingMap().subMap(198L, true, 2L, false));
        for (ConcurrentNavigableMap<Long, Long> v : views) {
            Runnable[] calls = {
                    () -> v.putIfAbsent(null, 1L),
                    () -> v.putIfAbsent(K, null),
                    () -> v.replace(null, 1L),
                    () -> v.replace(K, null),
                    () -> v.replace(null, 1L, 2L),
                    () -> v.replace(K, null, 2L),
                    () -> v.replace(K, 1L, null),
            };
            for (Runnable r : calls) {
                try { r.run(); fail("expected NullPointerException through " + v.getClass()); }
                catch (NullPointerException expected) { /* ok */ }
            }
        }
    }

    /** BufferTreeMap and EVERY sub-view (nested + descending) are ConcurrentNavigableMap —
     *  thread-safe under a single writer lock, not write-scaling. navigableKeySet is a
     *  NavigableSet. */
    @Test
    public void subViewsAreConcurrentNavigable() {
        BufferTreeMap<Long, Long> m = longMap(4, 48);
        for (long k = 0; k < 20; k++) m.put(k, k);

        assertTrue("map must be a ConcurrentNavigableMap", m instanceof ConcurrentNavigableMap);

        ConcurrentNavigableMap<Long, Long>[] views = new ConcurrentNavigableMap[]{
                m.subMap(2L, true, 18L, false),
                m.headMap(10L, false),
                m.tailMap(5L, true),
                m.descendingMap(),
                m.subMap(2L, true, 18L, false).subMap(4L, true, 16L, false),
                m.descendingMap().subMap(18L, true, 2L, false),
        };
        for (ConcurrentNavigableMap<?, ?> v : views) {
            assertTrue("sub-view must be ConcurrentNavigableMap: " + v.getClass(),
                    v instanceof ConcurrentNavigableMap);
        }
        assertTrue(m.navigableKeySet() instanceof NavigableSet);
        assertTrue(m.descendingKeySet() instanceof NavigableSet);
        assertTrue(m.subMap(2L, true, 18L, false).navigableKeySet() instanceof NavigableSet);
        // descendingMap and a doubly-nested subMap also expose NavigableSet key-sets
        assertTrue(m.descendingMap().navigableKeySet() instanceof NavigableSet);
        assertTrue(m.descendingMap().descendingKeySet() instanceof NavigableSet);
        assertTrue(m.subMap(2L, true, 18L, false).subMap(4L, true, 16L, false)
                .navigableKeySet() instanceof NavigableSet);
    }

    /** SortedMap 2-arg overloads keep their back-compat semantics: subMap [from,to),
     *  headMap head-exclusive, tailMap tail-inclusive — oracle-checked and delegating to
     *  the inclusive-flag overloads. */
    @Test
    public void sortedMapTwoArgBackCompat() {
        BufferTreeMap<Long, Long> m = longMap(4, 48);
        TreeMap<Long, Long> oracle = new TreeMap<>();
        for (long k = 0; k < 200; k++) { m.put(k, k * 5); oracle.put(k, k * 5); }
        m.flushAll();

        SortedMap<Long, Long> sub = m.subMap(50L, 150L);
        assertTrue("2-arg subMap: from inclusive", sub.containsKey(50L));
        assertFalse("2-arg subMap: to exclusive", sub.containsKey(150L));
        assertEquals(new ArrayList<>(oracle.subMap(50L, 150L).entrySet()), new ArrayList<>(sub.entrySet()));

        SortedMap<Long, Long> head = m.headMap(100L);
        assertFalse("2-arg headMap: to exclusive", head.containsKey(100L));
        assertTrue(head.containsKey(99L));
        assertEquals(new ArrayList<>(oracle.headMap(100L).entrySet()), new ArrayList<>(head.entrySet()));

        SortedMap<Long, Long> tail = m.tailMap(100L);
        assertTrue("2-arg tailMap: from inclusive", tail.containsKey(100L));
        assertEquals(new ArrayList<>(oracle.tailMap(100L).entrySet()), new ArrayList<>(tail.entrySet()));

        // 2-arg overloads return ConcurrentNavigableMap-capable views (covariant), typed as SortedMap
        assertTrue(sub instanceof NavigableMap);
        assertTrue(sub instanceof ConcurrentNavigableMap);
    }
}
