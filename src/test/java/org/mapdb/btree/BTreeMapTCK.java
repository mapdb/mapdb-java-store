package org.mapdb.btree;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mapdb.ser.GroupFormat;
import org.mapdb.ser.LongFormat;
import org.mapdb.ser.ObjectArrayFormat;
import org.mapdb.ser.Serializers;
import org.mapdb.store.Store;

import org.mapdb.ser.ByteArrayFormat;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Abstract TCK for {@link BTreeMap}. Concrete subclasses bind {@link #openStore()}
 * to a specific Store dialect (heap / byte-array / direct / WAL). Every case runs
 * unchanged across all four.
 */
public abstract class BTreeMapTCK {

    /** A fresh, empty store. Subclasses may allocate resources (temp files) here. */
    protected abstract Store openStore();

    protected Store store;

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

    private BTreeMap<Long, Long> longMap(int maxNodeSize) {
        return BTreeMap.create(store, LongFormat.INSTANCE, LongFormat.INSTANCE, maxNodeSize);
    }

    private BTreeMap<String, String> stringMap(int maxNodeSize) {
        return BTreeMap.create(store,
                new ObjectArrayFormat<>(Serializers.STRING),
                new ObjectArrayFormat<>(Serializers.STRING), maxNodeSize);
    }

    private BTreeMap<String, String> stringMapBinary(int maxNodeSize) {
        return BTreeMap.create(store,
                org.mapdb.ser.StringGroupFormat.INSTANCE,
                org.mapdb.ser.StringGroupFormat.INSTANCE, maxNodeSize);
    }

    // =====================================================================

    @Test
    public void emptyMap() {
        BTreeMap<Long, Long> m = longMap(8);
        assertNull(m.get(1L));
        assertNull(m.remove(1L));
        assertFalse(m.containsKey(1L));
        assertEquals(0L, m.sizeLong());
        assertFalse(m.entryIterator().hasNext());
    }

    @Test
    public void singleEntryLifecycle() {
        BTreeMap<Long, Long> m = longMap(8);

        assertNull(m.put(10L, 100L));
        assertEquals(Long.valueOf(100L), m.get(10L));
        assertTrue(m.containsKey(10L));
        assertEquals(1L, m.sizeLong());

        // overwrite returns old
        assertEquals(Long.valueOf(100L), m.put(10L, 200L));
        assertEquals(Long.valueOf(200L), m.get(10L));
        assertEquals(1L, m.sizeLong());

        // remove returns old, then null
        assertEquals(Long.valueOf(200L), m.remove(10L));
        assertNull(m.get(10L));
        assertFalse(m.containsKey(10L));
        assertNull(m.remove(10L));
        assertEquals(0L, m.sizeLong());
        assertFalse(m.entryIterator().hasNext());
    }

    // ---------- bulk insert (forces deep splits) ----------

    private void bulkInsertOrder(long[] keys) {
        final int N = keys.length;
        BTreeMap<Long, Long> m = longMap(8); // small node -> deep tree

        for (long k : keys) {
            assertNull("duplicate insert " + k, m.put(k, k * 7 + 1));
        }
        assertEquals(N, m.sizeLong());

        // every key retrievable
        for (long k : keys) {
            assertEquals("get " + k, Long.valueOf(k * 7 + 1), m.get(k));
            assertTrue(m.containsKey(k));
        }

        // iteration strictly ascending and complete
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
        assertEquals(N, count);
    }

    @Test
    public void bulkAscending() {
        long[] keys = new long[50_000];
        for (int i = 0; i < keys.length; i++) keys[i] = i;
        bulkInsertOrder(keys);
    }

    @Test
    public void bulkDescending() {
        long[] keys = new long[50_000];
        for (int i = 0; i < keys.length; i++) keys[i] = keys.length - 1 - i;
        bulkInsertOrder(keys);
    }

    @Test
    public void bulkShuffled() {
        long[] keys = new long[50_000];
        for (int i = 0; i < keys.length; i++) keys[i] = i;
        Random rnd = new Random(42);
        for (int i = keys.length - 1; i > 0; i--) {
            int j = rnd.nextInt(i + 1);
            long t = keys[i]; keys[i] = keys[j]; keys[j] = t;
        }
        bulkInsertOrder(keys);
    }

    // ---------- long-running seeded fuzz vs TreeMap oracle ----------

    @Test
    public void fuzzAgainstTreeMap() {
        final long seed = 0x5EEDC0FFEEL;
        System.out.println("[BTreeMapTCK.fuzzAgainstTreeMap] store=" + store.getClass().getSimpleName()
                + " seed=" + seed);
        Random rnd = new Random(seed);
        final int KEYSPACE = 20_000;
        final int OPS = 100_000;

        BTreeMap<Long, Long> m = longMap(8);
        TreeMap<Long, Long> oracle = new TreeMap<>();

        for (int i = 0; i < OPS; i++) {
            long key = rnd.nextInt(KEYSPACE);
            int roll = rnd.nextInt(100);
            if (roll < 60) { // put
                long val = rnd.nextLong();
                Long expected = oracle.put(key, val);
                Long actual = m.put(key, val);
                assertEquals("put[" + i + "] key=" + key, expected, actual);
            } else if (roll < 85) { // remove
                Long expected = oracle.remove(key);
                Long actual = m.remove(key);
                assertEquals("remove[" + i + "] key=" + key, expected, actual);
            } else { // get
                Long expected = oracle.get(key);
                Long actual = m.get(key);
                assertEquals("get[" + i + "] key=" + key, expected, actual);
            }
        }

        assertEquals("size after fuzz (seed=" + seed + ")", (long) oracle.size(), m.sizeLong());

        // full iteration comparison
        Iterator<Map.Entry<Long, Long>> it = m.entryIterator();
        Iterator<Map.Entry<Long, Long>> ot = oracle.entrySet().iterator();
        while (ot.hasNext()) {
            assertTrue("map iterator ended early (seed=" + seed + ")", it.hasNext());
            Map.Entry<Long, Long> me = it.next();
            Map.Entry<Long, Long> oe = ot.next();
            assertEquals(oe.getKey(), me.getKey());
            assertEquals(oe.getValue(), me.getValue());
        }
        assertFalse("map iterator has extra entries (seed=" + seed + ")", it.hasNext());
    }

    // ---------- remove-heavy then reinsert (leaf-coverage regression) ----------

    @Test
    public void removeHeavyThenReinsert() {
        BTreeMap<Long, Long> m = longMap(8);
        final int N = 2000;

        for (long k = 0; k <= N; k++) assertNull(m.put(k, k));

        // remove every key divisible by 3
        for (long k = 0; k <= N; k++) {
            if (k % 3 == 0) assertEquals(Long.valueOf(k), m.remove(k));
        }
        // verify they are gone and the rest survive
        for (long k = 0; k <= N; k++) {
            if (k % 3 == 0) assertNull("should be removed " + k, m.get(k));
            else assertEquals(Long.valueOf(k), m.get(k));
        }

        // reinsert the removed keys with a distinct value
        for (long k = 0; k <= N; k++) {
            if (k % 3 == 0) assertNull("reinsert should be fresh " + k, m.put(k, k + 1_000_000));
        }

        // verify everything present and iteration order intact
        for (long k = 0; k <= N; k++) {
            Long expected = (k % 3 == 0) ? (k + 1_000_000) : k;
            assertEquals("final get " + k, expected, m.get(k));
        }
        assertEquals(N + 1, m.sizeLong());

        Iterator<Map.Entry<Long, Long>> it = m.entryIterator();
        long prev = Long.MIN_VALUE, count = 0;
        while (it.hasNext()) {
            Map.Entry<Long, Long> e = it.next();
            assertTrue("order broke at " + e.getKey(), e.getKey() > prev);
            prev = e.getKey();
            count++;
        }
        assertEquals(N + 1, count);
    }

    // ---------- String keys and values ----------

    @Test
    public void stringKeysAndValues() {
        BTreeMap<String, String> m = stringMap(6);
        TreeMap<String, String> oracle = new TreeMap<>();
        Random rnd = new Random(7);
        final int N = 10_000;

        for (int i = 0; i < N; i++) {
            String k = randomString(rnd, 1 + rnd.nextInt(12));
            String v = randomString(rnd, 1 + rnd.nextInt(20));
            oracle.put(k, v);
            m.put(k, v);
        }

        assertEquals((long) oracle.size(), m.sizeLong());
        for (Map.Entry<String, String> e : oracle.entrySet()) {
            assertEquals("get " + e.getKey(), e.getValue(), m.get(e.getKey()));
            assertTrue(m.containsKey(e.getKey()));
        }

        Iterator<Map.Entry<String, String>> it = m.entryIterator();
        Iterator<Map.Entry<String, String>> ot = oracle.entrySet().iterator();
        while (ot.hasNext()) {
            assertTrue(it.hasNext());
            Map.Entry<String, String> me = it.next();
            Map.Entry<String, String> oe = ot.next();
            assertEquals(oe.getKey(), me.getKey());
            assertEquals(oe.getValue(), me.getValue());
        }
        assertFalse(it.hasNext());
    }

    /** Same coverage as {@link #stringKeysAndValues} but exercises the binary
     *  {@link org.mapdb.ser.StringGroupFormat} read path (binarySearch/binaryGet over
     *  serialized bytes) across every store dialect. */
    @Test
    public void stringKeysAndValuesBinary() {
        BTreeMap<String, String> m = stringMapBinary(6);
        TreeMap<String, String> oracle = new TreeMap<>();
        Random rnd = new Random(7);
        final int N = 10_000;

        for (int i = 0; i < N; i++) {
            String k = randomString(rnd, 1 + rnd.nextInt(12));
            String v = randomString(rnd, 1 + rnd.nextInt(20));
            oracle.put(k, v);
            m.put(k, v);
        }

        assertEquals((long) oracle.size(), m.sizeLong());
        for (Map.Entry<String, String> e : oracle.entrySet()) {
            assertEquals("get " + e.getKey(), e.getValue(), m.get(e.getKey()));
            assertTrue(m.containsKey(e.getKey()));
        }
        // absent keys (never inserted; different alphabet region) resolve to null
        for (int i = 0; i < 1000; i++) {
            assertNull(m.get("ABSENT-" + i));
        }

        Iterator<Map.Entry<String, String>> it = m.entryIterator();
        Iterator<Map.Entry<String, String>> ot = oracle.entrySet().iterator();
        while (ot.hasNext()) {
            assertTrue(it.hasNext());
            Map.Entry<String, String> me = it.next();
            Map.Entry<String, String> oe = ot.next();
            assertEquals(oe.getKey(), me.getKey());
            assertEquals(oe.getValue(), me.getValue());
        }
        assertFalse(it.hasNext());
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
        BTreeMap<Long, String> m = BTreeMap.create(store, kf, vf, 6);

        TreeMap<Long, String> oracle = new TreeMap<>();
        Random rnd = new Random(99);
        final int N = 8_000;
        for (int i = 0; i < N; i++) {
            long k = rnd.nextInt(N * 2);
            String v = "v" + k + "-" + rnd.nextInt(1000);
            oracle.put(k, v);
            m.put(k, v);
        }

        assertEquals((long) oracle.size(), m.sizeLong());
        for (Map.Entry<Long, String> e : oracle.entrySet()) {
            assertEquals(e.getValue(), m.get(e.getKey()));
        }

        List<Long> got = new ArrayList<>();
        Iterator<Map.Entry<Long, String>> it = m.entryIterator();
        while (it.hasNext()) got.add(it.next().getKey());
        assertEquals(new ArrayList<>(oracle.keySet()), got);
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

    private void verifyPumped(BTreeMap<Long, Long> m, long n) {
        for (long k = 0; k < n; k++) {
            assertEquals("get " + k, Long.valueOf(k * 7 + 1), m.get(k));
        }
        assertNull(m.get(-1L));
        assertNull(m.get(n));
        assertNull(m.get(n + 1000));
        Iterator<Map.Entry<Long, Long>> it = m.entryIterator();
        long expect = 0;
        while (it.hasNext()) {
            Map.Entry<Long, Long> e = it.next();
            assertEquals(Long.valueOf(expect), e.getKey());
            assertEquals(Long.valueOf(expect * 7 + 1), e.getValue());
            expect++;
        }
        assertEquals("iterated count", n, expect);
    }

    private void pumpAndVerify(long n, int maxNodeSize) {
        BTreeMap<Long, Long> m = BTreeMap.createFromSorted(store,
                LongFormat.INSTANCE, LongFormat.INSTANCE, maxNodeSize, ascending(n));
        store.verify();
        verifyPumped(m, n);
        // reopen from the persisted root pointer
        verifyPumped(BTreeMap.open(store, m.rootRecidRecid(),
                LongFormat.INSTANCE, LongFormat.INSTANCE, maxNodeSize), n);
    }

    /** Node-boundary sizes around the default fill (maxNodeSize 8 → fill 6):
     *  empty, single leaf, exactly-full leaf ± 1, exactly-full two-level ± 1. */
    @Test
    public void pumpBoundarySizes() {
        for (long n : new long[]{0, 1, 2, 5, 6, 7, 12, 13, 35, 36, 37, 1000}) {
            pumpAndVerify(n, 8);
        }
    }

    @Test
    public void pumpLarge() {
        pumpAndVerify(50_000, 8); // deep tree
        pumpAndVerify(10_000, 32);
    }

    /** Explicit nodeFill extremes: fully packed nodes and minimum fill. */
    @Test
    public void pumpFillExtremes() {
        for (int fill : new int[]{2, 8}) {
            BTreeMap<Long, Long> m = BTreeMap.createFromSorted(store,
                    LongFormat.INSTANCE, LongFormat.INSTANCE, 8, fill, ascending(1000));
            store.verify();
            verifyPumped(m, 1000);
        }
    }

    /** A pumped tree must behave exactly like an incrementally built one under
     *  subsequent put/remove — splits into pump-filled nodes included. */
    @Test
    public void pumpThenMutate() {
        final long N = 5_000;
        TreeMap<Long, Long> oracle = new TreeMap<>();
        for (long k = 0; k < 2 * N; k += 2) oracle.put(k, k * 7 + 1);
        BTreeMap<Long, Long> m = BTreeMap.createFromSorted(store,
                LongFormat.INSTANCE, LongFormat.INSTANCE, 8,
                oracle.entrySet().iterator());

        // fresh odd keys force splits of pump-filled leaves
        for (long k = 1; k < 2 * N; k += 2) {
            assertNull("fresh put " + k, m.put(k, -k));
            oracle.put(k, -k);
        }
        // overwrites return the pumped value
        for (long k = 0; k < 2 * N; k += 100) {
            assertEquals(oracle.put(k, k + 5), m.put(k, k + 5));
        }
        // removals return the live value
        for (long k = 0; k < 2 * N; k += 7) {
            assertEquals(oracle.remove(k), m.remove(k));
        }
        store.verify();

        assertEquals((long) oracle.size(), m.sizeLong());
        for (long k = 0; k < 2 * N; k++) {
            assertEquals("get " + k, oracle.get(k), m.get(k));
        }
        Iterator<Map.Entry<Long, Long>> it = m.entryIterator();
        for (Map.Entry<Long, Long> oe : oracle.entrySet()) {
            assertTrue(it.hasNext());
            Map.Entry<Long, Long> me = it.next();
            assertEquals(oe.getKey(), me.getKey());
            assertEquals(oe.getValue(), me.getValue());
        }
        assertFalse(it.hasNext());
    }

    @Test
    public void pumpEmptyThenUsable() {
        BTreeMap<Long, Long> m = BTreeMap.createFromSorted(store,
                LongFormat.INSTANCE, LongFormat.INSTANCE, 8,
                java.util.Collections.emptyIterator());
        assertEquals(0L, m.sizeLong());
        assertNull(m.put(1L, 10L));
        assertEquals(Long.valueOf(10L), m.get(1L));
        assertEquals(Long.valueOf(10L), m.remove(1L));
        assertEquals(0L, m.sizeLong());
    }

    @Test
    public void pumpRejectsUnsortedAndDuplicates() {
        List<Map.Entry<Long, Long>> dup = List.of(
                Map.entry(1L, 1L), Map.entry(2L, 2L), Map.entry(2L, 3L));
        try {
            BTreeMap.createFromSorted(store, LongFormat.INSTANCE, LongFormat.INSTANCE, 8, dup.iterator());
            fail("expected NotSorted for duplicate key");
        } catch (org.mapdb.DBException.NotSorted expected) { /* ok */ }

        List<Map.Entry<Long, Long>> desc = List.of(Map.entry(5L, 1L), Map.entry(4L, 2L));
        try {
            BTreeMap.createFromSorted(store, LongFormat.INSTANCE, LongFormat.INSTANCE, 8, desc.iterator());
            fail("expected NotSorted for descending keys");
        } catch (org.mapdb.DBException.NotSorted expected) { /* ok */ }
    }

    /** String pump over both the object-array and the binary group format. */
    @Test
    public void pumpStrings() {
        TreeMap<String, String> oracle = new TreeMap<>();
        Random rnd = new Random(11);
        for (int i = 0; i < 10_000; i++) {
            String k = randomString(rnd, 1 + rnd.nextInt(12));
            oracle.put(k, "v-" + k);
        }
        List<GroupFormat<String>> formats = List.of(
                new ObjectArrayFormat<>(Serializers.STRING),
                org.mapdb.ser.StringGroupFormat.INSTANCE);
        for (GroupFormat<String> f : formats) {
            BTreeMap<String, String> m = BTreeMap.createFromSorted(store, f, f, 6,
                    oracle.entrySet().iterator());
            store.verify();
            assertEquals((long) oracle.size(), m.sizeLong());
            for (Map.Entry<String, String> e : oracle.entrySet()) {
                assertEquals(e.getValue(), m.get(e.getKey()));
            }
            for (int i = 0; i < 500; i++) {
                assertNull(m.get("ABSENT-" + i));
            }
            Iterator<Map.Entry<String, String>> it = m.entryIterator();
            Iterator<Map.Entry<String, String>> ot = oracle.entrySet().iterator();
            while (ot.hasNext()) {
                assertTrue(it.hasNext());
                assertEquals(ot.next(), it.next());
            }
            assertFalse(it.hasNext());
        }
    }

    // ---------- null rejection ----------

    @Test
    public void putNullKeyRejected() {
        BTreeMap<Long, Long> m = longMap(8);
        try {
            m.put(null, 1L);
            fail("expected NullPointerException");
        } catch (NullPointerException expected) { /* ok */ }
    }

    @Test
    public void putNullValueRejected() {
        BTreeMap<Long, Long> m = longMap(8);
        try {
            m.put(1L, null);
            fail("expected NullPointerException");
        } catch (NullPointerException expected) { /* ok */ }
    }

    // =====================================================================
    // java.util.Map / SortedMap / ConcurrentMap interface conformance (the
    // newly-added surface). Oracle = java.util.TreeMap; views are ASCENDING so
    // sequences are compared in order. Runs across every store dialect.
    // =====================================================================

    /** Ascending-order comparison of every collection view against a SortedMap oracle,
     *  plus size/isEmpty/equals/hashCode/containsValue. */
    private static void assertAscendingViews(Map<Long, Long> m, SortedMap<Long, Long> oracle) {
        assertEquals("size", oracle.size(), m.size());
        assertEquals("isEmpty", oracle.isEmpty(), m.isEmpty());
        // keySet / values / entrySet iterate in ASCENDING key order
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
        BTreeMap<Long, Long> m = longMap(8);
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
        BTreeMap<Long, Long> m = longMap(8);
        TreeMap<Long, Long> oracle = new TreeMap<>();
        for (long k = 0; k < 1_500; k++) { m.put(k, k * 7 + 1); oracle.put(k, k * 7 + 1); }
        assertAscendingViews(m, oracle);
    }

    /** iterator.remove, keySet.removeAll, values/keySet.retainAll, entrySet.remove(entry),
     *  keySet.contains — every mutation reflects in the live map and stays oracle-consistent. */
    @Test
    public void viewMutationReflectsInMap() {
        BTreeMap<Long, Long> m = longMap(8);
        TreeMap<Long, Long> oracle = new TreeMap<>();
        for (long k = 0; k < 40; k++) { m.put(k, k * 2); oracle.put(k, k * 2); }

        // entrySet().iterator().remove() deletes the last-returned entry from the live map
        Iterator<Map.Entry<Long, Long>> eit = m.entrySet().iterator();
        Long removedKey = eit.next().getKey();
        eit.remove();
        oracle.remove(removedKey);
        assertFalse(m.containsKey(removedKey));
        try { eit.remove(); fail("double remove must throw IllegalStateException"); }
        catch (IllegalStateException expected) { /* ok */ }

        // keySet().contains
        assertTrue(m.keySet().contains(oracle.firstKey()));
        assertFalse(m.keySet().contains(9999L));

        // entrySet().remove(entry): wrong value no-op, matching value removes
        assertFalse(m.entrySet().remove(new AbstractMap.SimpleImmutableEntry<>(5L, -1L)));
        assertTrue(m.containsKey(5L));
        assertTrue(m.entrySet().remove(new AbstractMap.SimpleImmutableEntry<>(5L, 10L)));
        oracle.remove(5L);
        assertFalse(m.containsKey(5L));

        // keySet().removeAll(...) removes the given keys
        Set<Long> drop = new HashSet<>(Arrays.asList(10L, 11L, 12L, 13L));
        assertTrue(m.keySet().removeAll(drop));
        for (Long k : drop) oracle.remove(k);
        assertAscendingViews(m, oracle);

        // keySet().retainAll(...) keeps only the given keys
        Set<Long> keep = new HashSet<>();
        for (long k = 20; k < 30; k++) keep.add(k);
        m.keySet().retainAll(keep);
        oracle.keySet().retainAll(keep);
        assertAscendingViews(m, oracle);

        // values().retainAll(...) empties when nothing matches
        m.values().retainAll(new HashSet<>(Arrays.asList(-77L)));
        oracle.values().retainAll(new HashSet<>(Arrays.asList(-77L)));
        assertTrue(m.isEmpty());
        assertTrue(oracle.isEmpty());
    }

    @Test
    public void mapEqualsAndHashCode() {
        BTreeMap<Long, Long> m = longMap(8);
        TreeMap<Long, Long> oracle = new TreeMap<>();
        for (long k = 0; k < 300; k++) { m.put(k, k + 1); oracle.put(k, k + 1); }
        assertTrue(m.equals(oracle));
        assertTrue(oracle.equals(m));
        assertEquals(oracle.hashCode(), m.hashCode());
        assertTrue(m.equals(m)); // reflexive
        m.put(0L, 999L);              // one differing value breaks equality
        assertFalse(m.equals(oracle));
        assertFalse(oracle.equals(m));
        m.put(0L, 1L);
        assertTrue(m.equals(oracle));
        m.put(-1L, -1L);              // an extra key breaks equality
        assertFalse(m.equals(oracle));
        BTreeMap<Long, Long> empty = longMap(8);
        assertTrue(empty.equals(new TreeMap<Long, Long>()));
    }

    @Test
    public void putAllAndClearThroughMapInterface() {
        TreeMap<Long, Long> src = new TreeMap<>();
        for (long k = 0; k < 500; k++) src.put(k, k * 3 + 1);
        Map<Long, Long> m = longMap(8);
        m.putAll(src);
        assertAscendingViews(m, src);
        m.clear();
        assertTrue(m.isEmpty());
        assertEquals(0, m.size());
        assertTrue(m.entrySet().isEmpty());
        assertFalse(m.entrySet().iterator().hasNext());
    }

    /** comparator()==null; firstKey/lastKey correct incl. after removing the current
     *  min/max, empty-map → NoSuchElementException, and the empty-leaf-skip: removing a
     *  whole leftmost/rightmost region leaves empty leaves linked, and firstKey/lastKey
     *  must still find the true min/max. */
    @Test
    public void comparatorFirstLastAndEmptyLeafSkip() {
        BTreeMap<Long, Long> m = longMap(8);
        assertNull(m.comparator());
        try { m.firstKey(); fail("expected NoSuchElementException"); }
        catch (NoSuchElementException expected) { /* ok */ }
        try { m.lastKey(); fail("expected NoSuchElementException"); }
        catch (NoSuchElementException expected) { /* ok */ }

        final int N = 3_000;
        for (long k = 0; k < N; k++) m.put(k, k);
        assertEquals(Long.valueOf(0L), m.firstKey());
        assertEquals(Long.valueOf(N - 1), m.lastKey());

        // remove the current min and max
        assertEquals(Long.valueOf(0L), m.remove(0L));
        assertEquals(Long.valueOf(N - 1), m.remove((long) (N - 1)));
        assertEquals(Long.valueOf(1L), m.firstKey());
        assertEquals(Long.valueOf(N - 2), m.lastKey());

        // empty out a whole leftmost region: many leaves go empty but stay linked
        for (long k = 1; k < 500; k++) m.remove(k);
        assertEquals("empty-leaf skip on firstKey", Long.valueOf(500L), m.firstKey());
        // and a whole rightmost region
        for (long k = N - 2; k >= 2_500; k--) m.remove(k);
        assertEquals("empty-leaf skip on lastKey", Long.valueOf(2_499L), m.lastKey());
    }

    /** headMap/tailMap/subMap: bound inclusivity, out-of-range put IAE, from>to IAE,
     *  nested submaps (intersect), bounded clear, submap first/last/size/isEmpty and
     *  ascending iteration matching the oracle's submap. */
    @Test
    public void headTailSubMapSemantics() {
        BTreeMap<Long, Long> m = longMap(8);
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

        // out-of-range put → IllegalArgumentException; in-range put reflects
        try { sub.put(200L, 1L); fail("expected IllegalArgumentException"); }
        catch (IllegalArgumentException expected) { /* ok */ }
        try { sub.put(49L, 1L); fail("expected IllegalArgumentException"); }
        catch (IllegalArgumentException expected) { /* ok */ }
        assertEquals(Long.valueOf(75L * 10), sub.put(75L, 999L));
        oracle.put(75L, 999L);
        assertEquals(Long.valueOf(999L), m.get(75L));

        // from>to → IllegalArgumentException
        try { m.subMap(100L, 50L); fail("expected IllegalArgumentException"); }
        catch (IllegalArgumentException expected) { /* ok */ }

        // nested submap intersects ranges
        SortedMap<Long, Long> nested = sub.subMap(80L, 120L);
        assertAscendingViews(nested, oracle.subMap(80L, 120L));
        // a bound outside the parent range → IllegalArgumentException
        try { sub.subMap(10L, 120L); fail("expected IllegalArgumentException"); }
        catch (IllegalArgumentException expected) { /* ok */ }
        try { sub.headMap(300L); fail("expected IllegalArgumentException"); }
        catch (IllegalArgumentException expected) { /* ok */ }

        // bounded clear removes only in-range entries
        m.subMap(100L, 150L).clear();
        for (long k = 100; k < 150; k++) oracle.remove(k);
        assertEquals(Long.valueOf(0L), m.get(0L));       // out-of-range survives
        assertEquals(Long.valueOf(999L), m.get(75L));    // in-range-but-not-cleared survives
        assertEquals(Long.valueOf(1500L), m.get(150L));  // exactly the exclusive high survives
        assertAscendingViews(m, oracle);

        // empty submap: firstKey/lastKey throw
        SortedMap<Long, Long> emptySub = m.subMap(100L, 120L); // just cleared that range
        assertTrue(emptySub.isEmpty());
        assertEquals(0, emptySub.size());
        try { emptySub.firstKey(); fail("expected NoSuchElementException"); }
        catch (NoSuchElementException expected) { /* ok */ }
        try { emptySub.lastKey(); fail("expected NoSuchElementException"); }
        catch (NoSuchElementException expected) { /* ok */ }
    }

    /** Blind putOnly/removeOnly round-trip: values readable via get/iteration; removeOnly
     *  reports present-ness. */
    @Test
    public void blindPutOnlyRemoveOnlyRoundTrip() {
        BTreeMap<Long, Long> m = longMap(8);
        TreeMap<Long, Long> oracle = new TreeMap<>();
        for (long k = 0; k < 1_000; k++) { m.putOnly(k, k * 2); oracle.put(k, k * 2); }
        for (long k = 0; k < 1_000; k += 10) { m.putOnly(k, -k); oracle.put(k, -k); } // blind overwrite
        for (long k = 0; k < 1_000; k += 3) { assertTrue(m.removeOnly(k)); oracle.remove(k); }
        assertFalse("already removed", m.removeOnly(0L));       // 0 is divisible by 3
        assertFalse("never present", m.removeOnly(5_000L));
        assertEquals((long) oracle.size(), m.sizeLong());
        for (Map.Entry<Long, Long> e : oracle.entrySet()) assertEquals(e.getValue(), m.get(e.getKey()));
        List<Long> keys = new ArrayList<>();
        for (Iterator<Map.Entry<Long, Long>> it = m.entryIterator(); it.hasNext(); ) keys.add(it.next().getKey());
        assertEquals(new ArrayList<>(oracle.keySet()), keys);
    }

    /** Null-key / null-value rejection across the CAS + blind surface. */
    @Test
    public void casAndBlindNullRejection() {
        BTreeMap<Long, Long> m = longMap(8);
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

    /** ConcurrentMap CAS + default methods reached purely through a ConcurrentMap ref. */
    @Test
    public void concurrentMapCasThroughInterface() {
        ConcurrentMap<Long, Long> cm = longMap(8);
        // putIfAbsent
        assertNull(cm.putIfAbsent(1L, 10L));                              // inserted
        assertEquals(Long.valueOf(10L), cm.putIfAbsent(1L, 20L));         // present: existing
        assertEquals(Long.valueOf(10L), cm.get(1L));                      // unchanged
        assertEquals(Long.valueOf(10L), cm.getOrDefault(1L, -1L));
        assertEquals(Long.valueOf(-1L), cm.getOrDefault(2L, -1L));        // absent: default
        // remove(k,v)
        assertFalse(cm.remove(1L, 999L));                                // value mismatch: kept
        assertTrue(cm.containsKey(1L));
        // replace(k,v): present vs absent
        assertNull(cm.replace(2L, 99L));                                 // absent: no-op
        assertNull(cm.get(2L));
        assertEquals(Long.valueOf(10L), cm.replace(1L, 11L));            // present: swap
        assertEquals(Long.valueOf(11L), cm.get(1L));
        // replace(k,ov,nv): match vs mismatch vs absent
        assertFalse(cm.replace(1L, 999L, 12L));                          // wrong expected
        assertEquals(Long.valueOf(11L), cm.get(1L));
        assertTrue(cm.replace(1L, 11L, 12L));                           // matching expected
        assertEquals(Long.valueOf(12L), cm.get(1L));
        assertFalse(cm.replace(2L, 1L, 2L));                           // absent key
        // remove(k,v): matching value removes
        assertTrue(cm.remove(1L, 12L));
        assertFalse(cm.containsKey(1L));
        // Map default methods routed through the CAS primitives
        assertEquals(Long.valueOf(5L), cm.computeIfAbsent(3L, k -> 5L));
        assertEquals(Long.valueOf(5L), cm.computeIfAbsent(3L, k -> 999L)); // present: unchanged
        assertEquals(Long.valueOf(8L), cm.merge(3L, 3L, Long::sum));       // 5 + 3
        assertEquals(Long.valueOf(100L), cm.compute(4L, (k, v) -> 100L));
        assertNull(cm.compute(4L, (k, v) -> null));                        // null result removes
        assertFalse(cm.containsKey(4L));
        assertEquals(Long.valueOf(8L), cm.get(3L));
    }

    /**
     * CAS atomicity under concurrent writers on a shared thread-safe store. Two races:
     * (1) many threads {@code putIfAbsent} the same keys — exactly ONE insert wins per key;
     * (2) N threads each do M {@code replace(k, old, old+1)} CAS-retry increments on one
     * shared counter key — the final value must equal the total number of increments (no
     * lost updates).
     */
    @Test
    public void casAtomicityUnderConcurrentWriters() throws Exception {
        // require a thread-safe store; every TCK dialect provides one by default
        assertTrue("dialect store must be thread-safe for this test", store.isThreadSafe());
        final BTreeMap<Long, Long> m = longMap(8);

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
            assertEquals("winner value must belong to some writer", k, v % 1_000_000L);
        }

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
    }

    // =====================================================================
    // NavigableMap / ConcurrentNavigableMap conformance (spec-btree-map A/D).
    // Oracle = java.util.TreeMap and its navigable views. Runs across all
    // store dialects. Assertions respect weak consistency (§ cost caveats).
    // =====================================================================

    private BTreeMap<byte[], byte[]> byteArrayMap(int maxNodeSize) {
        return BTreeMap.create(store, ByteArrayFormat.INSTANCE, ByteArrayFormat.INSTANCE, maxNodeSize);
    }

    /** Long map with keys = {@code step*i} for i in [0,count), value = key*7+1. */
    private BTreeMap<Long, Long> sparseLongMap(int maxNodeSize, long step, int count, TreeMap<Long, Long> oracle) {
        BTreeMap<Long, Long> m = longMap(maxNodeSize);
        for (int i = 0; i < count; i++) {
            long k = step * i;
            m.put(k, k * 7 + 1);
            if (oracle != null) oracle.put(k, k * 7 + 1);
        }
        return m;
    }

    private static List<Long> longNavProbes() {
        return Arrays.asList(Long.MIN_VALUE, -5L, -1L, 0L, 1L, 5L, 10L, 15L, 100L, 105L,
                495L, 500L, 505L, 985L, 990L, 995L, 999L, 1000L, 5000L, Long.MAX_VALUE);
    }

    /** Oracle-compares every single-key navigation + first/last across a probe set. Works
     *  for any orientation (ascending / descending) — {@code m} and {@code o} must share it. */
    private static void assertNavMatches(NavigableMap<Long, Long> m, NavigableMap<Long, Long> o,
                                         Iterable<Long> probes) {
        assertEquals("firstEntry", o.firstEntry(), m.firstEntry());
        assertEquals("lastEntry", o.lastEntry(), m.lastEntry());
        if (o.isEmpty()) {
            assertNull(m.firstEntry());
            assertNull(m.lastEntry());
            try { m.firstKey(); fail("firstKey on empty must throw"); }
            catch (NoSuchElementException expected) { /* ok */ }
            try { m.lastKey(); fail("lastKey on empty must throw"); }
            catch (NoSuchElementException expected) { /* ok */ }
        } else {
            assertEquals("firstKey", o.firstKey(), m.firstKey());
            assertEquals("lastKey", o.lastKey(), m.lastKey());
        }
        for (Long k : probes) {
            assertEquals("floorEntry " + k, o.floorEntry(k), m.floorEntry(k));
            assertEquals("floorKey " + k, o.floorKey(k), m.floorKey(k));
            assertEquals("lowerEntry " + k, o.lowerEntry(k), m.lowerEntry(k));
            assertEquals("lowerKey " + k, o.lowerKey(k), m.lowerKey(k));
            assertEquals("ceilingEntry " + k, o.ceilingEntry(k), m.ceilingEntry(k));
            assertEquals("ceilingKey " + k, o.ceilingKey(k), m.ceilingKey(k));
            assertEquals("higherEntry " + k, o.higherEntry(k), m.higherEntry(k));
            assertEquals("higherKey " + k, o.higherKey(k), m.higherKey(k));
        }
        // iteration order in this view's orientation
        assertEquals("entrySet order", new ArrayList<>(o.entrySet()), new ArrayList<>(m.entrySet()));
        assertEquals("keySet order", new ArrayList<>(o.keySet()), new ArrayList<>(m.keySet()));
    }

    /** floor/lower/ceiling/higher and first/last against a TreeMap oracle: absent,
     *  below-min, above-max, exact-hit; *Key variants; empty-map null / throw. */
    @Test
    public void navFloorLowerCeilingHigherAndFirstLast() {
        BTreeMap<Long, Long> empty = longMap(8);
        assertNull(empty.firstEntry());
        assertNull(empty.lastEntry());
        assertNull(empty.lowerEntry(0L));
        assertNull(empty.floorKey(0L));
        assertNull(empty.ceilingEntry(0L));
        assertNull(empty.higherKey(0L));
        try { empty.firstKey(); fail("expected NoSuchElementException"); }
        catch (NoSuchElementException expected) { /* ok */ }
        try { empty.lastKey(); fail("expected NoSuchElementException"); }
        catch (NoSuchElementException expected) { /* ok */ }

        TreeMap<Long, Long> oracle = new TreeMap<>();
        BTreeMap<Long, Long> m = sparseLongMap(8, 10L, 100, oracle); // keys 0,10,..,990
        assertNavMatches(m, oracle, longNavProbes());
    }

    /** Every *Entry and poll method returns an immutable snapshot; setValue throws. */
    @Test
    public void navAndPollEntriesAreImmutable() {
        TreeMap<Long, Long> oracle = new TreeMap<>();
        BTreeMap<Long, Long> m = sparseLongMap(8, 10L, 50, oracle);
        List<Map.Entry<Long, Long>> entries = Arrays.asList(
                m.firstEntry(), m.lastEntry(),
                m.floorEntry(105L), m.lowerEntry(105L), m.ceilingEntry(105L), m.higherEntry(105L),
                m.floorEntry(100L), m.pollFirstEntry(), m.pollLastEntry());
        for (Map.Entry<Long, Long> e : entries) {
            assertNotNull(e);
            assertTrue("must be SimpleImmutableEntry", e instanceof AbstractMap.SimpleImmutableEntry);
            try { e.setValue(-1L); fail("setValue must throw UnsupportedOperationException"); }
            catch (UnsupportedOperationException expected) { /* ok */ }
        }
    }

    /** Null search key → NPE on every single-key navigation method. */
    @Test
    public void navNullSearchKeyThrowsNPE() {
        BTreeMap<Long, Long> m = sparseLongMap(8, 10L, 20, null);
        @SuppressWarnings("unchecked")
        java.util.function.Consumer<NavigableMap<Long, Long>>[] calls = new java.util.function.Consumer[]{
                (java.util.function.Consumer<NavigableMap<Long, Long>>) x -> x.lowerEntry(null),
                (java.util.function.Consumer<NavigableMap<Long, Long>>) x -> x.lowerKey(null),
                (java.util.function.Consumer<NavigableMap<Long, Long>>) x -> x.floorEntry(null),
                (java.util.function.Consumer<NavigableMap<Long, Long>>) x -> x.floorKey(null),
                (java.util.function.Consumer<NavigableMap<Long, Long>>) x -> x.ceilingEntry(null),
                (java.util.function.Consumer<NavigableMap<Long, Long>>) x -> x.ceilingKey(null),
                (java.util.function.Consumer<NavigableMap<Long, Long>>) x -> x.higherEntry(null),
                (java.util.function.Consumer<NavigableMap<Long, Long>>) x -> x.higherKey(null),
                (java.util.function.Consumer<NavigableMap<Long, Long>>) x -> x.get(null),
                (java.util.function.Consumer<NavigableMap<Long, Long>>) x -> x.containsKey(null),
        };
        for (java.util.function.Consumer<NavigableMap<Long, Long>> c : calls) {
            try { c.accept(m); fail("expected NullPointerException"); }
            catch (NullPointerException expected) { /* ok */ }
        }
    }

    /** pollFirstEntry/pollLastEntry: single-threaded order + emptying matches the oracle,
     *  return type immutable, and null on empty. */
    @Test
    public void pollFirstLastSingleThreadedMatchesOracle() {
        TreeMap<Long, Long> oracle = new TreeMap<>();
        BTreeMap<Long, Long> m = sparseLongMap(8, 3L, 500, oracle);

        // drain from both ends alternately, oracle-checking each poll
        boolean fromFront = true;
        while (!oracle.isEmpty()) {
            if (fromFront) {
                assertEquals("pollFirstEntry", oracle.pollFirstEntry(), m.pollFirstEntry());
            } else {
                assertEquals("pollLastEntry", oracle.pollLastEntry(), m.pollLastEntry());
            }
            fromFront = !fromFront;
        }
        assertTrue(m.isEmpty());
        assertEquals(0L, m.sizeLong());
        assertNull("pollFirstEntry on empty", m.pollFirstEntry());
        assertNull("pollLastEntry on empty", m.pollLastEntry());
    }

    /**
     * Value-race atomicity of poll under concurrent writers on a thread-safe store. Value for
     * key k is {@code k + generation*KEYS}, so {@code value % KEYS == k} ALWAYS encodes the key
     * and value only ever grows. Writer threads bump generations via {@code replace} CAS while
     * poller threads drain from both ends. We do NOT assert strict least/greatest selection
     * (weakly consistent); we assert the (k,v) ATOMICITY: every key is returned at MOST once
     * (a conditional-remove is the single mutation point) and every returned value legitimately
     * encodes its key (poll never returns/removes a torn or foreign value).
     */
    @Test
    public void pollValueRaceAtomicity() throws Exception {
        assertTrue("dialect store must be thread-safe for this test", store.isThreadSafe());
        final int KEYS = 500;
        final BTreeMap<Long, Long> m = longMap(8);
        for (long k = 0; k < KEYS; k++) m.put(k, k); // generation 0: value == key

        final ConcurrentLinkedQueue<Map.Entry<Long, Long>> polled = new ConcurrentLinkedQueue<>();
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final AtomicBoolean pollersDone = new AtomicBoolean(false);

        final int POLLERS = 4, WRITERS = 3;
        Thread[] pollers = new Thread[POLLERS];
        for (int t = 0; t < POLLERS; t++) {
            final boolean front = (t % 2 == 0);
            pollers[t] = new Thread(() -> {
                try {
                    Map.Entry<Long, Long> e;
                    while ((e = front ? m.pollFirstEntry() : m.pollLastEntry()) != null) {
                        polled.add(e);
                    }
                } catch (Throwable ex) { failure.compareAndSet(null, ex); }
            });
        }
        Thread[] writers = new Thread[WRITERS];
        for (int t = 0; t < WRITERS; t++) {
            final long seed = 100L + t;
            writers[t] = new Thread(() -> {
                try {
                    Random rnd = new Random(seed);
                    while (!pollersDone.get()) {
                        long k = rnd.nextInt(KEYS);
                        Long cur = m.get(k);
                        if (cur != null) m.replace(k, cur, cur + KEYS); // bump generation, keeps value%KEYS==k
                    }
                } catch (Throwable ex) { failure.compareAndSet(null, ex); }
            });
        }
        for (Thread w : writers) w.start();
        for (Thread p : pollers) p.start();
        for (Thread p : pollers) p.join();
        pollersDone.set(true);
        for (Thread w : writers) w.join();
        if (failure.get() != null) throw new AssertionError("worker failed", failure.get());

        assertTrue("map fully drained", m.isEmpty());
        Set<Long> seen = new HashSet<>();
        for (Map.Entry<Long, Long> e : polled) {
            long k = e.getKey(), v = e.getValue();
            assertTrue("key returned more than once by poll: " + k, seen.add(k));
            assertTrue("returned value must encode its key (atomic pair): k=" + k + " v=" + v,
                    ((v % KEYS) + KEYS) % KEYS == k);
            assertTrue("returned value must be >= initial (a value the key actually held)", v >= k);
        }
        assertEquals("every key drained exactly once", KEYS, seen.size());
    }

    /** descendingMap: reversed iteration + the §D single-key mapping (oracle =
     *  TreeMap.descendingMap()); comparator non-null & reversed; double-descending == ascending;
     *  nested descending subMaps. */
    @Test
    public void descendingMapSemantics() {
        TreeMap<Long, Long> oracle = new TreeMap<>();
        BTreeMap<Long, Long> m = sparseLongMap(8, 10L, 100, oracle); // 0,10,..,990

        // §D mapping and reversed iteration, fully oracle-checked
        assertNavMatches(m.descendingMap(), oracle.descendingMap(), longNavProbes());

        // comparator: ascending is natural (null); descending is non-null and reversed
        assertNull(m.comparator());
        Comparator<? super Long> dcmp = m.descendingMap().comparator();
        assertNotNull("descending comparator must be non-null even for natural order", dcmp);
        assertTrue("descending comparator reversed", dcmp.compare(1L, 2L) > 0);
        assertTrue(dcmp.compare(2L, 1L) < 0);
        assertEquals(0, dcmp.compare(5L, 5L));

        // descendingMap().descendingMap() behaves as the original ascending orientation
        NavigableMap<Long, Long> dd = m.descendingMap().descendingMap();
        assertNull("double-descending comparator is natural again", dd.comparator());
        assertNavMatches(dd, oracle, longNavProbes());

        // nested descending subMap oracle-checked (descending-order args)
        NavigableMap<Long, Long> dSub = m.descendingMap().subMap(700L, true, 200L, false);
        NavigableMap<Long, Long> oSub = oracle.descendingMap().subMap(700L, true, 200L, false);
        assertNavMatches(dSub, oSub, longNavProbes());
        assertEquals(new ArrayList<>(oSub.entrySet()), new ArrayList<>(dSub.entrySet()));
    }

    /** navigableKeySet / descendingKeySet: order, subSet/headSet/tailSet inclusivity,
     *  pollFirst/pollLast on the SET, contains/remove/iterator.remove reflect the backing
     *  map, add() → UnsupportedOperationException. */
    @Test
    public void navigableKeySetAndDescendingKeySet() {
        TreeMap<Long, Long> oracle = new TreeMap<>();
        BTreeMap<Long, Long> m = sparseLongMap(8, 5L, 100, oracle); // 0,5,..,495

        NavigableSet<Long> ks = m.navigableKeySet();
        NavigableSet<Long> oks = oracle.navigableKeySet();
        assertEquals(new ArrayList<>(oks), new ArrayList<>(ks));
        assertEquals(new ArrayList<>(oracle.descendingKeySet()), new ArrayList<>(m.descendingKeySet()));
        assertEquals(new ArrayList<>(oks.descendingSet()), new ArrayList<>(ks.descendingSet()));

        // navigation
        assertEquals(oks.first(), ks.first());
        assertEquals(oks.last(), ks.last());
        assertEquals(oks.floor(102L), ks.floor(102L));
        assertEquals(oks.ceiling(102L), ks.ceiling(102L));
        assertEquals(oks.lower(100L), ks.lower(100L));
        assertEquals(oks.higher(100L), ks.higher(100L));
        assertEquals(oks.comparator(), ks.comparator()); // both null (natural)

        // subSet / headSet / tailSet (inclusive flags)
        assertEquals(new ArrayList<>(oks.subSet(100L, true, 300L, false)),
                new ArrayList<>(ks.subSet(100L, true, 300L, false)));
        assertEquals(new ArrayList<>(oks.headSet(100L, true)), new ArrayList<>(ks.headSet(100L, true)));
        assertEquals(new ArrayList<>(oks.tailSet(400L, false)), new ArrayList<>(ks.tailSet(400L, false)));
        assertEquals(new ArrayList<>(oks.subSet(100L, 300L)), new ArrayList<>(ks.subSet(100L, 300L)));

        // contains reflects backing; add unsupported
        assertTrue(ks.contains(100L));
        assertFalse(ks.contains(101L));
        try { ks.add(9999L); fail("add must throw UnsupportedOperationException"); }
        catch (UnsupportedOperationException expected) { /* ok */ }

        // pollFirst / pollLast mutate the backing map
        assertEquals(oks.pollFirst(), ks.pollFirst());
        oracle.remove(0L);
        assertEquals(oks.pollLast(), ks.pollLast());
        assertFalse(m.containsKey(0L));

        // set.remove reflects in the map
        assertTrue(ks.remove(100L));
        oracle.remove(100L);
        assertFalse(m.containsKey(100L));
        assertFalse(ks.remove(100L));

        // iterator.remove mutates the map
        Iterator<Long> it = ks.iterator();
        Long viaIter = it.next();
        it.remove();
        oracle.remove(viaIter);
        assertFalse(m.containsKey(viaIter));

        assertEquals(new ArrayList<>(oracle.keySet()), new ArrayList<>(m.keySet()));
    }

    /** All fromInc×toInc combinations on subMap, plus headMap/tailMap inclusive flags,
     *  each oracle-checked (iteration order, size, first/last, navigation). */
    @Test
    public void inclusivityCombinationsOnSubHeadTailMaps() {
        TreeMap<Long, Long> oracle = new TreeMap<>();
        BTreeMap<Long, Long> m = sparseLongMap(8, 1L, 200, oracle); // 0..199

        for (boolean fromInc : new boolean[]{false, true}) {
            for (boolean toInc : new boolean[]{false, true}) {
                NavigableMap<Long, Long> sm = m.subMap(50L, fromInc, 150L, toInc);
                NavigableMap<Long, Long> so = oracle.subMap(50L, fromInc, 150L, toInc);
                assertEquals("size fromInc=" + fromInc + " toInc=" + toInc, so.size(), sm.size());
                assertEquals(new ArrayList<>(so.entrySet()), new ArrayList<>(sm.entrySet()));
                assertNavMatches(sm, so, longNavProbes());
            }
        }
        for (boolean inc : new boolean[]{false, true}) {
            assertNavMatches(m.headMap(120L, inc), oracle.headMap(120L, inc), longNavProbes());
            assertNavMatches(m.tailMap(80L, inc), oracle.tailMap(80L, inc), longNavProbes());
        }
    }

    /** Exclusive/empty ranges are truly empty: subMap(k,false,k,false); on a bounded parent,
     *  headMap(parentLo,false) and tailMap(parentHi,false). */
    @Test
    public void exclusiveEmptyRanges() {
        BTreeMap<Long, Long> m = sparseLongMap(8, 1L, 200, null); // 0..199

        NavigableMap<Long, Long> emptyExcl = m.subMap(50L, false, 50L, false);
        assertEmptyNavMap(emptyExcl);

        NavigableMap<Long, Long> parent = m.subMap(50L, true, 150L, true); // parentLo=50, parentHi=150
        assertEmptyNavMap(parent.headMap(50L, false));  // keys < 50 in range: none
        assertEmptyNavMap(parent.tailMap(150L, false)); // keys > 150 in range: none
    }

    private static void assertEmptyNavMap(NavigableMap<Long, Long> v) {
        assertTrue(v.isEmpty());
        assertEquals(0, v.size());
        assertNull(v.firstEntry());
        assertNull(v.lastEntry());
        assertFalse(v.entrySet().iterator().hasNext());
        assertFalse(v.keySet().iterator().hasNext());
        assertFalse(v.descendingMap().entrySet().iterator().hasNext());
        try { v.firstKey(); fail("firstKey on empty range must throw"); }
        catch (NoSuchElementException expected) { /* ok */ }
        try { v.lastKey(); fail("lastKey on empty range must throw"); }
        catch (NoSuchElementException expected) { /* ok */ }
    }

    /** Out-of-range navigation on a bounded view returns null at/over the exclusive/inclusive
     *  endpoints (never leaks a backing neighbour outside the view). */
    @Test
    public void outOfRangeNavigationOnBoundedViews() {
        TreeMap<Long, Long> oracle = new TreeMap<>();
        BTreeMap<Long, Long> m = sparseLongMap(8, 1L, 1000, oracle); // 0..999

        NavigableMap<Long, Long> sub = m.subMap(100L, true, 700L, false);
        assertNull("ceiling above high", sub.ceilingEntry(800L));
        assertNull("higher at excluded high", sub.higherEntry(700L)); // 700 excluded
        assertNull("floor below low", sub.floorEntry(50L));
        assertNull("lower at inclusive low", sub.lowerEntry(100L));
        assertEquals(Long.valueOf(699L), sub.lastKey());   // 700 excluded
        assertEquals(Long.valueOf(100L), sub.firstKey());  // 100 included
        assertNavMatches(sub, oracle.subMap(100L, true, 700L, false), longNavProbes());

        NavigableMap<Long, Long> subHiInc = m.subMap(100L, true, 700L, true);
        assertNull("higher at inclusive high", subHiInc.higherEntry(700L));
        assertEquals(Long.valueOf(700L), subHiInc.ceilingKey(700L)); // 700 included
        assertEquals(Long.valueOf(700L), subHiInc.lastKey());
    }

    /** Nested sub-views never widen an exclusive parent bound (ascending and descending),
     *  and reject out-of-parent-range bounds / from>to with IllegalArgumentException. */
    @Test
    public void nestedViewsDoNotWidenAndRejectOutOfRange() {
        BTreeMap<Long, Long> m = sparseLongMap(8, 1L, 1000, null); // 0..999

        // ascending: parent excludes 700; a child asking INCLUSIVE 700 throws, like TreeMap
        NavigableMap<Long, Long> parent = m.subMap(100L, true, 700L, false);
        try { parent.subMap(200L, true, 700L, true); fail("inclusive 700 vs exclusive parent high must throw"); }
        catch (IllegalArgumentException expected) { /* ok */ }
        NavigableMap<Long, Long> child = parent.subMap(200L, true, 700L, false);
        assertFalse("child must not widen exclusive parent high", child.containsKey(700L));
        assertNull(child.get(700L));
        assertNull("no ceiling past the (still-exclusive) high", child.ceilingEntry(700L));
        assertEquals(Long.valueOf(699L), child.lastKey());

        // parent excludes 100 (exclusive low); a child asking INCLUSIVE 100 throws, like TreeMap
        NavigableMap<Long, Long> parentLoExcl = m.subMap(100L, false, 700L, true);
        try { parentLoExcl.tailMap(100L, true); fail("inclusive 100 vs exclusive parent low must throw"); }
        catch (IllegalArgumentException expected) { /* ok */ }
        NavigableMap<Long, Long> childLo = parentLoExcl.tailMap(100L, false);
        assertFalse("child must not widen exclusive parent low", childLo.containsKey(100L));
        assertEquals(Long.valueOf(101L), childLo.firstKey());

        // descending nesting: same strictness through the orientation flip
        NavigableMap<Long, Long> dParent = m.subMap(100L, true, 700L, false).descendingMap();
        try { dParent.headMap(700L, true); fail("inclusive 700 vs exclusive parent high must throw (descending)"); }
        catch (IllegalArgumentException expected) { /* ok */ }
        assertFalse(dParent.headMap(700L, false).containsKey(700L));

        // out-of-parent-range bound → IllegalArgumentException
        try { parent.subMap(10L, true, 500L, false); fail("expected IllegalArgumentException"); }
        catch (IllegalArgumentException expected) { /* ok */ }
        try { parent.headMap(900L); fail("expected IllegalArgumentException"); }
        catch (IllegalArgumentException expected) { /* ok */ }
        try { parent.tailMap(10L); fail("expected IllegalArgumentException"); }
        catch (IllegalArgumentException expected) { /* ok */ }

        // from > to → IllegalArgumentException (2-arg and 4-arg)
        try { m.subMap(500L, 100L); fail("expected IllegalArgumentException"); }
        catch (IllegalArgumentException expected) { /* ok */ }
        try { m.subMap(500L, true, 100L, true); fail("expected IllegalArgumentException"); }
        catch (IllegalArgumentException expected) { /* ok */ }
    }

    /** After removing a large contiguous region (leaving empty PERSISTENT leaves linked),
     *  predecessor queries (floor/lower/lastEntry) AND descending iteration stay correct. */
    @Test
    public void emptyPersistentLeavesPredecessorAndDescending() {
        TreeMap<Long, Long> oracle = new TreeMap<>();
        BTreeMap<Long, Long> m = sparseLongMap(8, 1L, 4000, oracle); // 0..3999 dense → deep tree

        // remove a big contiguous middle region: many whole leaves go empty but stay linked
        for (long k = 1000; k < 3000; k++) { m.remove(k); oracle.remove(k); }
        // also empty the leftmost region
        for (long k = 0; k < 500; k++) { m.remove(k); oracle.remove(k); }

        // predecessor across the hole
        assertEquals(oracle.floorEntry(2500L), m.floorEntry(2500L));   // → 999
        assertEquals(oracle.lowerEntry(3000L), m.lowerEntry(3000L));   // → 3000 (present) predecessor logic
        assertEquals(oracle.floorEntry(2999L), m.floorEntry(2999L));   // → 999
        assertEquals(oracle.ceilingEntry(1500L), m.ceilingEntry(1500L)); // → 3000
        assertEquals(oracle.higherEntry(999L), m.higherEntry(999L));   // → 3000
        assertEquals(oracle.firstKey(), m.firstKey());                 // → 500
        assertEquals(oracle.lastEntry(), m.lastEntry());

        // descending iteration over the whole map is correct across empty leaves
        assertEquals(new ArrayList<>(oracle.descendingMap().entrySet()),
                new ArrayList<>(m.descendingMap().entrySet()));
        // and a descending sub-range straddling the hole
        assertNavMatches(m.descendingMap().subMap(3500L, true, 700L, false),
                oracle.descendingMap().subMap(3500L, true, 700L, false), longNavProbes());
    }

    // ---- non-natural order: ByteArrayFormat (unsigned byte[]) ----

    private static byte[] bkey(int v) { return new byte[]{(byte) v}; }
    private static byte[] bval(int v) { return new byte[]{(byte) v, (byte) (v >>> 8), 0x7A}; }

    private static void assertBytesEq(byte[] exp, byte[] act) { assertArrayEquals(exp, act); }

    private static void assertByteEntry(Map.Entry<byte[], byte[]> exp, Map.Entry<byte[], byte[]> act) {
        if (exp == null) { assertNull(act); return; }
        assertNotNull(act);
        assertBytesEq(exp.getKey(), act.getKey());
        assertBytesEq(exp.getValue(), act.getValue());
    }

    private static List<byte[]> byteKeyList(NavigableMap<byte[], byte[]> m) {
        List<byte[]> out = new ArrayList<>();
        for (Map.Entry<byte[], byte[]> e : m.entrySet()) out.add(e.getKey());
        return out;
    }

    private static void assertByteKeysEqual(List<byte[]> exp, List<byte[]> act) {
        assertEquals("key count", exp.size(), act.size());
        for (int i = 0; i < exp.size(); i++) assertBytesEq(exp.get(i), act.get(i));
    }

    /** Non-natural (unsigned byte[]) order: comparator() non-null; ascending + descending
     *  navigation oracle-checked against TreeMap using the map's own comparator. */
    @Test
    public void byteArrayNavigationAscendingAndDescending() {
        BTreeMap<byte[], byte[]> m = byteArrayMap(6);
        Comparator<? super byte[]> cmp = m.comparator();
        assertNotNull("ByteArrayFormat comparator must be non-null (non-natural order)", cmp);
        // unsigned order: 0x80 sorts ABOVE 0x7F (would be reversed under signed byte order)
        assertTrue(cmp.compare(bkey(0x80), bkey(0x7F)) > 0);

        TreeMap<byte[], byte[]> oracle = new TreeMap<>(cmp);
        int[] present = {0, 1, 5, 10, 50, 0x7F, 0x80, 0x81, 0xC0, 0xFE, 0xFF};
        for (int v : present) { m.put(bkey(v), bval(v)); oracle.put(bkey(v), bval(v)); }

        // ascending iteration order matches the unsigned oracle
        assertByteKeysEqual(new ArrayList<>(oracle.keySet()), byteKeyList(m));

        List<byte[]> probes = new ArrayList<>();
        for (int v : new int[]{0, 2, 5, 6, 49, 50, 51, 0x7E, 0x7F, 0x80, 0x81, 0xBF, 0xC0, 0xFD, 0xFE, 0xFF}) {
            probes.add(bkey(v));
        }
        probes.add(new byte[0]);                       // below every key (empty is smallest)
        probes.add(new byte[]{(byte) 0xFF, 0x00});     // above single 0xFF (longer, same prefix)

        for (byte[] p : probes) {
            assertBytesEq(oracle.floorKey(p), m.floorKey(p));
            assertBytesEq(oracle.lowerKey(p), m.lowerKey(p));
            assertBytesEq(oracle.ceilingKey(p), m.ceilingKey(p));
            assertBytesEq(oracle.higherKey(p), m.higherKey(p));
            assertByteEntry(oracle.floorEntry(p), m.floorEntry(p));
            assertByteEntry(oracle.ceilingEntry(p), m.ceilingEntry(p));
        }
        assertBytesEq(oracle.firstKey(), m.firstKey());
        assertBytesEq(oracle.lastKey(), m.lastKey());

        // descending: order + comparator reversed + navigation
        NavigableMap<byte[], byte[]> md = m.descendingMap();
        NavigableMap<byte[], byte[]> od = oracle.descendingMap();
        assertNotNull(md.comparator());
        assertTrue("descending byte[] comparator reversed", md.comparator().compare(bkey(1), bkey(2)) > 0);
        assertByteKeysEqual(byteKeyList(od), byteKeyList(md));
        for (byte[] p : probes) {
            assertBytesEq(od.floorKey(p), md.floorKey(p));
            assertBytesEq(od.ceilingKey(p), md.ceilingKey(p));
            assertBytesEq(od.lowerKey(p), md.lowerKey(p));
            assertBytesEq(od.higherKey(p), md.higherKey(p));
        }
        assertBytesEq(od.firstKey(), md.firstKey());
        assertBytesEq(od.lastKey(), md.lastKey());
    }

    /** BTreeMap sub-views (and nested chains, and descendingMap) are ConcurrentNavigableMap. */
    @Test
    public void subViewsAreConcurrentNavigableMap() {
        BTreeMap<Long, Long> m = sparseLongMap(8, 1L, 100, null);
        assertTrue(m.subMap(10L, true, 90L, false) instanceof ConcurrentNavigableMap);
        assertTrue(m.headMap(50L, false) instanceof ConcurrentNavigableMap);
        assertTrue(m.tailMap(50L, true) instanceof ConcurrentNavigableMap);
        assertTrue(m.subMap(10L, 90L) instanceof ConcurrentNavigableMap);
        assertTrue(m.headMap(50L) instanceof ConcurrentNavigableMap);
        assertTrue(m.tailMap(50L) instanceof ConcurrentNavigableMap);
        assertTrue(m.descendingMap() instanceof ConcurrentNavigableMap);
        // a nested chain stays concurrent (descending sub-bounds are in descending order)
        ConcurrentNavigableMap<Long, Long> chain = m.subMap(10L, true, 90L, false)
                .descendingMap().subMap(80L, true, 20L, true).tailMap(70L, false);
        assertTrue(chain instanceof ConcurrentNavigableMap);
        assertTrue(chain.descendingMap() instanceof ConcurrentNavigableMap);
    }

    /** SortedMap 2-arg overloads keep their historical bounds: subMap [from incl, to excl],
     *  headMap to-exclusive, tailMap from-inclusive. */
    @Test
    public void sortedMapTwoArgBackCompat() {
        TreeMap<Long, Long> oracle = new TreeMap<>();
        BTreeMap<Long, Long> m = sparseLongMap(8, 1L, 200, oracle); // 0..199

        SortedMap<Long, Long> sub = m.subMap(50L, 150L);
        assertTrue(sub.containsKey(50L));    // from inclusive
        assertFalse(sub.containsKey(150L));  // to exclusive
        assertEquals(Long.valueOf(50L), sub.firstKey());
        assertEquals(Long.valueOf(149L), sub.lastKey());
        assertEquals(new ArrayList<>(oracle.subMap(50L, 150L).entrySet()), new ArrayList<>(sub.entrySet()));

        SortedMap<Long, Long> head = m.headMap(50L);
        assertFalse("headMap(to) excludes to", head.containsKey(50L));
        assertEquals(new ArrayList<>(oracle.headMap(50L).entrySet()), new ArrayList<>(head.entrySet()));

        SortedMap<Long, Long> tail = m.tailMap(150L);
        assertTrue("tailMap(from) includes from", tail.containsKey(150L));
        assertEquals(new ArrayList<>(oracle.tailMap(150L).entrySet()), new ArrayList<>(tail.entrySet()));
    }
}
