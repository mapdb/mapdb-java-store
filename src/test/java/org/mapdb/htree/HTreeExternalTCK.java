package org.mapdb.htree;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mapdb.hash.Hasher;
import org.mapdb.hash.Hashers;
import org.mapdb.ser.Serializers;
import org.mapdb.store.Store;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Abstract TCK for {@link HTreeMapExternal} (external-value HTreeMap variant),
 * bound to each store dialect by concrete subclasses — mirrors {@link HTreeMapTCK}.
 * The extra discipline here is VALUE-RECORD ownership: every entry owns exactly one
 * value record, so record-leak gates count entries too (an empty default-config map
 * owns exactly 17 records: header + 16 segment roots — same as HTreeMap).
 */
public abstract class HTreeExternalTCK {

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

    protected void cleanup() {}

    // ---------- helpers ----------

    private HTreeMapExternal<Long, Long> longMap() {
        return HTreeMapExternal.create(store, Serializers.LONG, Serializers.LONG);
    }

    private HTreeMapExternal<String, String> stringMap() {
        return HTreeMapExternal.create(store, Serializers.STRING, Serializers.STRING);
    }

    private long liveRecidCount() {
        long n = 0;
        for (PrimitiveIterator.OfLong it = store.getAllRecids(); it.hasNext(); it.nextLong()) n++;
        return n;
    }

    private static <K, V> Map<K, V> drain(HTreeMapExternal<K, V> m) {
        Map<K, V> out = new HashMap<>();
        Iterator<Map.Entry<K, V>> it = m.entryIterator();
        while (it.hasNext()) {
            Map.Entry<K, V> e = it.next();
            assertNull("duplicate key in iteration: " + e.getKey(), out.put(e.getKey(), e.getValue()));
        }
        return out;
    }

    private static <K, V> void assertMatches(HTreeMapExternal<K, V> m, Map<K, V> oracle) {
        assertEquals((long) oracle.size(), m.sizeLong());
        for (Map.Entry<K, V> e : oracle.entrySet()) {
            assertEquals("get " + e.getKey(), e.getValue(), m.get(e.getKey()));
        }
        assertEquals("iteration mismatch", oracle, drain(m));
        Map<K, V> viaForEach = new HashMap<>();
        m.forEach(viaForEach::put);
        assertEquals("forEach mismatch", oracle, viaForEach);
    }

    // =====================================================================

    @Test
    public void emptyMap() {
        HTreeMapExternal<Long, Long> m = longMap();
        assertNull(m.get(1L));
        assertFalse(m.containsKey(1L));
        assertEquals(0L, m.sizeLong());
        assertTrue(m.isEmpty());
        assertFalse(m.entryIterator().hasNext());
        m.forEach((k, v) -> fail("forEach on empty map visited " + k));
        assertEquals(17L, liveRecidCount());
    }

    @Test
    public void singleEntryLifecycle() {
        HTreeMapExternal<Long, Long> m = longMap();

        assertNull(m.put(10L, 100L));
        assertEquals(Long.valueOf(100L), m.get(10L));
        assertTrue(m.containsKey(10L));
        assertEquals(1L, m.sizeLong());
        // one leaf + one value record on top of the 17-record skeleton
        assertEquals(19L, liveRecidCount());

        // overwrite returns old, rewrites ONLY the value record: record count stable
        for (long v = 200; v < 300; v++) {
            assertEquals(Long.valueOf(v == 200 ? 100 : v - 1), m.put(10L, v));
            assertEquals(Long.valueOf(v), m.get(10L));
        }
        assertEquals(1L, m.sizeLong());
        assertEquals("overwrites must not allocate", 19L, liveRecidCount());

        // remove frees the value record AND the leaf
        assertEquals(Long.valueOf(299L), m.remove(10L));
        assertNull(m.get(10L));
        assertEquals(0L, m.sizeLong());
        assertEquals("remove leaked records", 17L, liveRecidCount());
    }

    @Test
    public void bulkShuffled() {
        final int N = 20_000;
        long[] keys = new long[N];
        for (int i = 0; i < N; i++) keys[i] = i;
        Random rnd = new Random(42);
        for (int i = N - 1; i > 0; i--) {
            int j = rnd.nextInt(i + 1);
            long t = keys[i]; keys[i] = keys[j]; keys[j] = t;
        }

        HTreeMapExternal<Long, Long> m = longMap();
        for (long k : keys) {
            assertNull("duplicate insert " + k, m.put(k, k * 7 + 1));
        }
        assertEquals(N, m.sizeLong());
        for (long k : keys) {
            assertEquals("get " + k, Long.valueOf(k * 7 + 1), m.get(k));
        }
        assertNull(m.get((long) N));

        Map<Long, Long> got = drain(m);
        assertEquals(N, got.size());
        for (long k : keys) {
            assertEquals(Long.valueOf(k * 7 + 1), got.get(k));
        }
    }

    @Test
    public void createFromSortedByHashBasic() {
        final int seed = 987654;
        Hasher<Long> hasher = Hashers.mixing(k -> Long.hashCode(k));
        ArrayList<Map.Entry<Long, Long>> entries = new ArrayList<>();
        HashMap<Long, Long> oracle = new HashMap<>();
        for (long k = 0; k < 5_000; k++) {
            long value = k * 17 + 3;
            entries.add(new AbstractMap.SimpleImmutableEntry<>(k, value));
            oracle.put(k, value);
        }
        entries.sort((a, b) -> Integer.compareUnsigned(
                hasher.hash(a.getKey(), seed), hasher.hash(b.getKey(), seed)));

        HTreeMapExternal<Long, Long> m = HTreeMapExternal.createFromSortedByHash(store,
                Serializers.LONG, Serializers.LONG, 4, 7, 4, seed, hasher, entries.iterator());

        assertMatches(m, oracle);
        assertTrue("value records should be externally owned",
                liveRecidCount() >= 17L + oracle.size() + oracle.size());
        HTreeMapExternal<Long, Long> reopened = HTreeMapExternal.open(store, m.headerRecid(),
                Serializers.LONG, Serializers.LONG, hasher);
        assertMatches(reopened, oracle);
    }

    /** THE workhorse: seeded random op mix vs HashMap oracle, then a full drain —
     *  every leaf, dir AND VALUE record must be freed (back to the 17-record skeleton). */
    @Test
    public void fuzzWithRemoves() {
        final long seed = 0xE87E54A1L;
        System.out.println("[HTreeExternalTCK.fuzzWithRemoves] store=" + store.getClass().getSimpleName()
                + " seed=" + seed);
        Random rnd = new Random(seed);
        final int KEYSPACE = 10_000;
        final int OPS = 60_000;

        HTreeMapExternal<Long, Long> m = longMap();
        HashMap<Long, Long> oracle = new HashMap<>();

        for (int i = 0; i < OPS; i++) {
            long key = rnd.nextInt(KEYSPACE);
            int roll = rnd.nextInt(100);
            if (roll < 40) {
                long val = rnd.nextLong();
                assertEquals("put[" + i + "] key=" + key, oracle.put(key, val), m.put(key, val));
            } else if (roll < 65) {
                assertEquals("remove[" + i + "] key=" + key, oracle.remove(key), m.remove(key));
            } else if (roll < 80) {
                assertEquals("get[" + i + "] key=" + key, oracle.get(key), m.get(key));
            } else if (roll < 90) {
                assertEquals("containsKey[" + i + "] key=" + key,
                        oracle.containsKey(key), m.containsKey(key));
            } else if (roll < 95) {
                long val = rnd.nextLong();
                assertEquals("putIfAbsent[" + i + "] key=" + key,
                        oracle.putIfAbsent(key, val), m.putIfAbsent(key, val));
            } else {
                long val = rnd.nextLong();
                assertEquals("replace[" + i + "] key=" + key,
                        oracle.replace(key, val), m.replace(key, val));
            }
        }
        assertEquals(oracle.isEmpty(), m.isEmpty());
        assertMatches(m, oracle);

        for (Long key : new ArrayList<>(oracle.keySet())) {
            assertEquals(oracle.remove(key), m.remove(key));
        }
        assertTrue(m.isEmpty());
        assertEquals(0L, m.sizeLong());
        assertEquals("drain leaked records", 17L, liveRecidCount());
    }

    @Test
    public void stringKeysAndValues() {
        HTreeMapExternal<String, String> m = stringMap();
        HashMap<String, String> oracle = new HashMap<>();
        Random rnd = new Random(7);
        for (int i = 0; i < 5_000; i++) {
            String k = randomString(rnd, 1 + rnd.nextInt(12));
            String v = randomString(rnd, 1 + rnd.nextInt(20));
            assertEquals(oracle.put(k, v), m.put(k, v));
        }
        for (int i = 0; i < 500; i++) {
            assertNull(m.get("ABSENT-" + i));
        }
        assertMatches(m, oracle);
    }

    private static String randomString(Random rnd, int len) {
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) sb.append((char) ('a' + rnd.nextInt(26)));
        return sb.toString();
    }

    /** The class's reason to exist: values far larger than anything a bucket scan
     *  should touch. Insert/overwrite/remove large byte[] values; get/containsKey
     *  stay correct and the drain leak-gates value-record ownership. */
    @Test
    public void largeValues() {
        HTreeMapExternal<Long, byte[]> m =
                HTreeMapExternal.create(store, Serializers.LONG, Serializers.BYTE_ARRAY);
        HashMap<Long, byte[]> oracle = new HashMap<>();
        Random rnd = new Random(202);
        for (long k = 0; k < 100; k++) {
            byte[] v = new byte[10_000 + rnd.nextInt(50_000)];
            rnd.nextBytes(v);
            assertNull(m.put(k, v));
            oracle.put(k, v);
        }
        assertEquals(oracle.size(), m.sizeLong());
        for (Map.Entry<Long, byte[]> e : oracle.entrySet()) {
            assertTrue("value for " + e.getKey(), Arrays.equals(e.getValue(), m.get(e.getKey())));
            assertTrue(m.containsKey(e.getKey()));
        }
        // overwrite half with differently-sized values (value record resizes in place)
        for (long k = 0; k < 100; k += 2) {
            byte[] v = new byte[100 + rnd.nextInt(200_000)];
            rnd.nextBytes(v);
            assertTrue(Arrays.equals(oracle.put(k, v), m.put(k, v)));
        }
        for (Map.Entry<Long, byte[]> e : oracle.entrySet()) {
            assertTrue("post-overwrite value for " + e.getKey(),
                    Arrays.equals(e.getValue(), m.get(e.getKey())));
        }
        for (long k = 0; k < 100; k++) {
            assertTrue(Arrays.equals(oracle.get(k), m.remove(k)));
        }
        assertEquals("large-value drain leaked records", 17L, liveRecidCount());
    }

    /** Constant keyHash piles every key into ONE bucket: exercises key-only scans,
     *  per-entry value records within a shared leaf, and mid-bucket removes. */
    @Test
    public void collisionHeavy() {
        Hasher<Long> constantHash = (k, seed) -> 0;
        HTreeMapExternal<Long, Long> m = HTreeMapExternal.create(store,
                Serializers.LONG, Serializers.LONG, 4, 7, 4, 0, constantHash);
        HashMap<Long, Long> oracle = new HashMap<>();
        final int N = 200;

        for (long k = 0; k < N; k++) {
            assertNull(m.put(k, k * 3));
            oracle.put(k, k * 3);
        }
        for (long k = 0; k < N; k += 2) { // overwrite every other key
            assertEquals(Long.valueOf(k * 3), m.put(k, -k));
            oracle.put(k, -k);
        }
        assertNull(m.get((long) N)); // absent key shares the bucket, scan must miss
        assertMatches(m, oracle);

        // remove middle/first/last, then drain the bucket: full collapse + no leaks
        for (long k : new long[]{100, 0, N - 1}) {
            assertEquals(oracle.remove(k), m.remove(k));
        }
        assertMatches(m, oracle);
        for (Long k : new ArrayList<>(oracle.keySet())) {
            assertEquals(oracle.remove(k), m.remove(k));
        }
        assertTrue(m.isEmpty());
        assertEquals("bucket drain leaked records", 17L, liveRecidCount());
    }

    /** CAS return-value + effect semantics (all conditional ops read the value record). */
    @Test
    public void casOperations() {
        HTreeMapExternal<Long, Long> m = longMap();

        assertNull(m.putIfAbsent(1L, 10L));
        assertEquals(Long.valueOf(10L), m.putIfAbsent(1L, 20L));
        assertEquals(Long.valueOf(10L), m.get(1L));

        assertNull(m.replace(2L, 20L));
        assertNull(m.get(2L));
        assertEquals(Long.valueOf(10L), m.replace(1L, 11L));
        assertEquals(Long.valueOf(11L), m.get(1L));

        assertFalse(m.replace(1L, 999L, 12L));
        assertEquals(Long.valueOf(11L), m.get(1L));
        assertTrue(m.replace(1L, 11L, 12L));
        assertEquals(Long.valueOf(12L), m.get(1L));
        assertFalse(m.replace(2L, 1L, 2L));

        assertFalse(m.remove(1L, 999L));
        assertTrue(m.containsKey(1L));
        assertTrue(m.remove(1L, 12L));
        assertNull(m.get(1L));
        assertFalse(m.remove(1L, 12L));
        assertEquals(0L, m.sizeLong());
        assertEquals("cas ops leaked records", 17L, liveRecidCount());
    }

    /** Fill, clear (must free leaf + dir + VALUE records), reuse, reopen. */
    @Test
    public void clearThenReuse() {
        HTreeMapExternal<Long, Long> m = longMap();
        m.clear(); // no-op on empty
        assertTrue(m.isEmpty());

        final int N = 5_000;
        for (long k = 0; k < N; k++) m.put(k, k * 7);
        assertEquals((long) N, m.sizeLong());

        m.clear();
        assertTrue(m.isEmpty());
        assertEquals(0L, m.sizeLong());
        assertEquals("clear leaked records", 17L, liveRecidCount());
        for (long k = 0; k < N; k += 97) {
            assertNull(m.get(k));
            assertFalse(m.containsKey(k));
        }

        HashMap<Long, Long> oracle = new HashMap<>();
        for (long k = 0; k < 1_000; k++) {
            assertNull("fresh after clear " + k, m.put(k, k + 5));
            oracle.put(k, k + 5);
        }
        assertMatches(m, oracle);

        HTreeMapExternal<Long, Long> m2 = HTreeMapExternal.open(store, m.headerRecid(),
                Serializers.LONG, Serializers.LONG);
        assertMatches(m2, oracle);
    }

    @Test
    public void boundaryLongKeys() {
        HTreeMapExternal<Long, Long> m = longMap();
        HashMap<Long, Long> oracle = new HashMap<>();
        for (long k : new long[]{0L, 1L, -1L, Long.MIN_VALUE, Long.MAX_VALUE,
                Integer.MIN_VALUE, Integer.MAX_VALUE, -1_000_000L, 1_000_000L}) {
            assertNull("fresh " + k, m.put(k, k));
            oracle.put(k, k);
        }
        assertMatches(m, oracle);
    }

    @Test
    public void nullRejected() {
        HTreeMapExternal<Long, Long> m = longMap();
        m.put(1L, 1L);
        Runnable[] calls = {
                () -> m.get(null),
                () -> m.put(null, 1L),
                () -> m.put(1L, null),
                () -> m.remove(null),
                () -> m.remove(null, 1L),
                () -> m.remove(1L, null),
                () -> m.containsKey(null),
                () -> m.putIfAbsent(null, 1L),
                () -> m.putIfAbsent(1L, null),
                () -> m.replace(null, 1L),
                () -> m.replace(1L, null),
                () -> m.replace(null, 1L, 2L),
                () -> m.replace(1L, null, 2L),
                () -> m.replace(1L, 1L, null),
        };
        for (Runnable r : calls) {
            try {
                r.run();
                fail("expected NullPointerException");
            } catch (NullPointerException expected) { /* ok */ }
        }
        assertEquals(Long.valueOf(1L), m.get(1L));
        assertEquals(1L, m.sizeLong());
    }

    @Test
    public void invalidConfigRejected() {
        try {
            HTreeMapExternal.create(store, Serializers.LONG, Serializers.LONG, 4, 7, 3);
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) { /* ok */ }
        try {
            HTreeMapExternal.create(store, Serializers.LONG, Serializers.LONG, 4, 8, 4);
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) { /* ok */ }
    }

    /** forEach snapshot allows callback mutation (§0.5), same as HTreeMap. */
    @Test
    public void forEachCallbackMayMutate() {
        HTreeMapExternal<Long, Long> m = longMap();
        for (long k = 0; k < 100; k++) m.put(k, k);
        java.util.Set<Long> seen = new java.util.HashSet<>();
        m.forEach((k, v) -> {
            seen.add(k);
            if (k < 1_000_000) m.put(k + 1_000_000, v);
        });
        for (long k = 0; k < 100; k++) {
            assertTrue("original key not visited: " + k, seen.contains(k));
        }
        assertEquals(Long.valueOf(5L), m.get(1_000_005L));
        assertEquals(200L, m.sizeLong());
    }

    // =====================================================================
    // Parity cases ported from HTreeMapTCK — the external variant shares the
    // dir-tree machinery, so lazy-split / collapse / bucket-scan coverage must
    // hold here too, PLUS value-record ownership (extra leak-gate discipline).
    // =====================================================================

    /** Lazy-split ladder driven THROUGH the external map: an identity Hasher makes
     *  each Long key's hash equal itself, controlling segment + dir index exactly.
     *  Every insert step must keep all prior keys (and their value records) resolvable. */
    @Test
    public void deepSplitTargetedIndices() {
        Hasher<Long> identityHash = (k, seed) -> (int) k.longValue();
        HTreeMapExternal<Long, Long> m = HTreeMapExternal.create(store,
                Serializers.LONG, Serializers.LONG, 4, 7, 4, 0, identityHash);

        long[] indices = {
                0, 1, 1L << 7, 1L << 14, 1L << 21,
                (5L << 21) | (3L << 14), (5L << 21) | (9L << 7),
                (5L << 21) | (9L << 7) | 1, 0x0FFFFFFFL,
        };
        HashMap<Long, Long> oracle = new HashMap<>();
        for (long idx : indices) {
            assertNull("fresh insert " + idx, m.put(idx, idx + 1000));
            oracle.put(idx, idx + 1000);
            for (Map.Entry<Long, Long> e : oracle.entrySet()) {
                assertEquals("get " + e.getKey() + " after inserting " + idx,
                        e.getValue(), m.get(e.getKey()));
            }
        }
        assertNull(m.get(2L));
        assertNull(m.get(2L << 7));
        assertNull(m.get((5L << 21) | (9L << 7) | 2));
        assertMatches(m, oracle);

        for (long idx : indices) { // overwrite through the split tree (value records only)
            assertEquals(Long.valueOf(idx + 1000), m.put(idx, idx + 2000));
            oracle.put(idx, idx + 2000);
        }
        assertMatches(m, oracle);
    }

    /** Targeted dir-collapse THROUGH the external map: insert the deep-split ladder,
     *  then remove one at a time — every step frees a value record and keeps survivors
     *  resolvable; a full drain returns to the 17-record skeleton (no value leaks). */
    @Test
    public void deepSplitTargetedRemoves() {
        Hasher<Long> identityHash = (k, seed) -> (int) k.longValue();
        HTreeMapExternal<Long, Long> m = HTreeMapExternal.create(store,
                Serializers.LONG, Serializers.LONG, 4, 7, 4, 0, identityHash);

        long[] indices = {
                0, 1, 1L << 7, 1L << 14, 1L << 21,
                (5L << 21) | (3L << 14), (5L << 21) | (9L << 7),
                (5L << 21) | (9L << 7) | 1, 0x0FFFFFFFL,
        };
        HashMap<Long, Long> oracle = new HashMap<>();
        for (long idx : indices) {
            assertNull(m.put(idx, idx + 1000));
            oracle.put(idx, idx + 1000);
        }
        for (long idx : indices) {
            assertEquals("remove " + idx, Long.valueOf(idx + 1000), m.remove(idx));
            oracle.remove(idx);
            assertNull("removed " + idx + " must miss", m.get(idx));
            for (Map.Entry<Long, Long> e : oracle.entrySet()) {
                assertEquals("survivor " + e.getKey() + " after removing " + idx,
                        e.getValue(), m.get(e.getKey()));
            }
        }
        assertTrue(m.isEmpty());
        assertEquals(0L, m.sizeLong());
        assertEquals("targeted removes leaked records", 17L, liveRecidCount());

        for (long idx : indices) assertNull("reinsert " + idx, m.put(idx, idx + 2000));
        for (long idx : indices) assertEquals(Long.valueOf(idx + 2000), m.get(idx));
    }

    /** Distinct Strings with IDENTICAL Java hashCode share one bucket even under the
     *  default hasher; the key-only bucket scan must disambiguate them, keep each one's
     *  own value record, and miss an un-inserted equal-hashCode sibling. */
    @Test
    public void equalHashCodeStringsShareBucket() {
        String[] blocks = {"Aa", "BB"};
        java.util.List<String> family = new java.util.ArrayList<>();
        for (int a = 0; a < 2; a++)
            for (int b = 0; b < 2; b++)
                for (int c = 0; c < 2; c++)
                    for (int d = 0; d < 2; d++)
                        family.add(blocks[a] + blocks[b] + blocks[c] + blocks[d]);
        int h = family.get(0).hashCode();
        for (String s : family) assertEquals("hashCode collision precondition", h, s.hashCode());

        HTreeMapExternal<String, String> m = stringMap();
        HashMap<String, String> oracle = new HashMap<>();
        String omitted = family.remove(family.size() - 1);
        for (String s : family) {
            assertNull("fresh " + s, m.put(s, "v-" + s));
            oracle.put(s, "v-" + s);
        }
        assertNull("equal-hash sibling not inserted must miss", m.get(omitted));
        assertEquals(family.size(), m.sizeLong());
        for (String s : family) assertEquals("v-" + s, m.get(s));

        String mid = family.get(family.size() / 2);
        assertEquals("v-" + mid, m.put(mid, "NEW")); // overwrite mid-bucket member
        oracle.put(mid, "NEW");
        assertMatches(m, oracle);

        assertNull(m.put(omitted, "v-" + omitted)); // bucket grows, all coexist
        oracle.put(omitted, "v-" + omitted);
        assertMatches(m, oracle);
    }

    /** byte[] keys with a content keyHash (the intended array-key use case) and byte[]
     *  VALUES living in their own records: fresh equal-content probes must hit, and an
     *  equal-content overwrite returns the old value without growing the bucket. */
    @Test
    public void byteArrayContentHashKeys() {
        HTreeMapExternal<byte[], byte[]> m = HTreeMapExternal.create(store,
                Serializers.BYTE_ARRAY, Serializers.BYTE_ARRAY, 4, 7, 4, Hashers.mixing(Arrays::hashCode));
        for (int i = 0; i < 3_000; i++) {
            assertNull("fresh " + i, m.put(("key-" + i).getBytes(), ("val-" + i).getBytes()));
        }
        assertEquals(3_000L, m.sizeLong());
        for (int i = 0; i < 3_000; i++) {
            assertTrue(Arrays.equals(("val-" + i).getBytes(), m.get(("key-" + i).getBytes())));
        }
        byte[] old = m.put("key-42".getBytes(), "REPLACED".getBytes());
        assertTrue(Arrays.equals("val-42".getBytes(), old));
        assertTrue(Arrays.equals("REPLACED".getBytes(), m.get("key-42".getBytes())));
        assertEquals(3_000L, m.sizeLong());
        assertNull(m.get("absent".getBytes()));
    }

    /** Keys steered into a few DISTINCT top-level segments; the iterator must visit
     *  every entry across those sparse segments, resolving each value record, none lost. */
    @Test
    public void sparseSegmentsIterationComplete() {
        Hasher<Long> identityHash = (k, seed) -> (int) k.longValue();
        HTreeMapExternal<Long, Long> m = HTreeMapExternal.create(store,
                Serializers.LONG, Serializers.LONG, 4, 7, 4, 0, identityHash);
        HashMap<Long, Long> oracle = new HashMap<>();
        for (int seg : new int[]{1, 7, 10, 15}) {
            for (int j = 0; j < 200; j++) {
                long key = ((long) seg << 28) | j;
                assertNull("fresh seg=" + seg + " j=" + j, m.put(key, key * 3));
                oracle.put(key, key * 3);
            }
        }
        assertEquals(4 * 200L, m.sizeLong());
        assertMatches(m, oracle);
    }

    /** Insert N, remove two thirds (mass value-record + bucket + dir collapse), verify
     *  survivors/absences, reinsert removed with new values — catches collapse-regrow bugs. */
    @Test
    public void removeHeavyThenReinsert() {
        HTreeMapExternal<Long, Long> m = longMap();
        final int N = 20_000;
        for (long k = 0; k < N; k++) assertNull(m.put(k, k));

        long survivors = 0;
        for (long k = 0; k < N; k++) {
            if (k % 3 != 0) assertEquals(Long.valueOf(k), m.remove(k));
            else survivors++;
        }
        assertEquals(survivors, m.sizeLong());
        for (long k = 0; k < N; k++) {
            if (k % 3 != 0) {
                assertNull("should be removed " + k, m.get(k));
                assertFalse(m.containsKey(k));
            } else {
                assertEquals("survivor " + k, Long.valueOf(k), m.get(k));
            }
        }
        for (long k = 0; k < N; k++) {
            if (k % 3 != 0) assertNull("reinsert should be fresh " + k, m.put(k, k + 1_000_000));
        }
        for (long k = 0; k < N; k++) {
            assertEquals("final get " + k, (k % 3 != 0) ? k + 1_000_000 : k, (long) m.get(k));
        }
        assertEquals((long) N, m.sizeLong());
    }

    /** Empty String is a valid (non-null) key and value; overwriting the empty-key
     *  entry returns the old value from its value record. */
    @Test
    public void emptyStringKeyAndValue() {
        HTreeMapExternal<String, String> m = stringMap();
        assertNull(m.put("", ""));
        assertEquals("", m.get(""));
        assertNull(m.put("k", ""));
        assertEquals("", m.get("k"));
        assertEquals("", m.put("", "v"));
        assertEquals("v", m.get(""));
        assertEquals(2L, m.sizeLong());
    }

    /** String keys/values exercise the LOCKED (non-optimistic) leaf path plus
     *  valueSer.equals-based CAS on fresh equal-content instances. */
    @Test
    public void stringRemoveAndCas() {
        HTreeMapExternal<String, String> m = stringMap();
        HashMap<String, String> oracle = new HashMap<>();
        Random rnd = new Random(31);
        for (int i = 0; i < 5_000; i++) {
            String k = randomString(rnd, 1 + rnd.nextInt(12));
            String v = randomString(rnd, 1 + rnd.nextInt(20));
            assertEquals(oracle.put(k, v), m.put(k, v));
        }
        java.util.List<String> keys = new java.util.ArrayList<>(oracle.keySet());
        for (int i = 0; i < keys.size(); i += 2) {
            String k = new String(keys.get(i));
            assertEquals(oracle.remove(k), m.remove(k));
        }
        String someKey = keys.get(1);
        String someVal = oracle.get(someKey);
        assertFalse(m.remove(someKey, someVal + "-x"));
        assertTrue(m.replace(someKey, new String(someVal), "swapped"));
        oracle.put(someKey, "swapped");
        assertTrue(m.remove(someKey, new String("swapped")));
        oracle.remove(someKey);
        assertMatches(m, oracle);
    }

    /** A single key overwritten many times: each put returns the exact prior value, the
     *  size stays 1, and (external) overwrites NEVER allocate a new record. */
    @Test
    public void overwriteReturnsOldValueRepeatedly() {
        HTreeMapExternal<Long, Long> m = longMap();
        assertNull(m.put(7L, 0L));
        long baseline = liveRecidCount();
        for (long v = 1; v <= 500; v++) {
            assertEquals("overwrite " + v, Long.valueOf(v - 1), m.put(7L, v));
            assertEquals(1L, m.sizeLong());
        }
        assertEquals(Long.valueOf(500L), m.get(7L));
        assertEquals("repeated overwrite must not allocate", baseline, liveRecidCount());
    }

    /** Single segment (concShift=0), deep narrow tree: 8 levels x 4 bits. */
    @Test
    public void singleSegmentDeepTree() {
        HTreeMapExternal<Long, Long> m = HTreeMapExternal.create(store,
                Serializers.LONG, Serializers.LONG, 0, 4, 8);
        HashMap<Long, Long> oracle = new HashMap<>();
        Random rnd = new Random(11);
        for (int i = 0; i < 20_000; i++) {
            long k = rnd.nextInt(8_000);
            long v = rnd.nextLong();
            assertEquals(oracle.put(k, v), m.put(k, v));
        }
        assertMatches(m, oracle);
    }

    /** Many segments (256), shallow trees: 4 levels x 6 bits. */
    @Test
    public void manySegmentsShallowTree() {
        HTreeMapExternal<Long, Long> m = HTreeMapExternal.create(store,
                Serializers.LONG, Serializers.LONG, 8, 6, 4);
        HashMap<Long, Long> oracle = new HashMap<>();
        Random rnd = new Random(13);
        for (int i = 0; i < 20_000; i++) {
            long k = rnd.nextInt(8_000);
            long v = rnd.nextLong();
            assertEquals(oracle.put(k, v), m.put(k, v));
        }
        assertMatches(m, oracle);
    }

    /** Conditional ops inside a shared collision bucket: replace/CAS must target the
     *  right entry among equal-index neighbors, updating only its value record. */
    @Test
    public void casOperationsInCollisionBucket() {
        Hasher<Long> constantHash = (k, seed) -> 0;
        HTreeMapExternal<Long, Long> m = HTreeMapExternal.create(store,
                Serializers.LONG, Serializers.LONG, 4, 7, 4, 0, constantHash);
        HashMap<Long, Long> oracle = new HashMap<>();
        for (long k = 0; k < 50; k++) {
            m.put(k, k * 3);
            oracle.put(k, k * 3);
        }
        // replace present mid-bucket member rewrites only its value record (no alloc)
        long baseline = liveRecidCount();
        assertEquals(oracle.replace(20L, 999L), m.replace(20L, 999L));
        assertEquals("replace must not allocate", baseline, liveRecidCount());
        assertEquals(oracle.replace(100L, 1L), m.replace(100L, 1L)); // absent shares bucket
        assertEquals(oracle.putIfAbsent(25L, -1L), m.putIfAbsent(25L, -1L));
        assertEquals(oracle.putIfAbsent(100L, -2L), m.putIfAbsent(100L, -2L));
        assertEquals(oracle.replace(30L, 999L, 1000L), m.replace(30L, 999L, 1000L)); // mismatch
        assertEquals(oracle.replace(31L, 31L * 3, 1000L), m.replace(31L, 31L * 3, 1000L));
        assertEquals(oracle.remove(40L, 40L * 3), m.remove(40L, 40L * 3));
        assertEquals(oracle.remove(41L, 555L), m.remove(41L, 555L)); // mismatch
        assertMatches(m, oracle);

        for (Long k : new ArrayList<>(oracle.keySet())) {
            assertEquals(oracle.remove(k), m.remove(k));
        }
        assertEquals("collision cas drain leaked records", 17L, liveRecidCount());
    }

    // =====================================================================
    // java.util.Map / ConcurrentMap interface conformance (size/entrySet/keySet/
    // values/containsValue/putAll, Map.equals/hashCode, the snapshot-view
    // iterators, and the ConcurrentMap CAS + default methods reached purely
    // through the interface). Runs across every dialect; a drain afterwards must
    // still free every value record (the external variant's extra leak gate).
    // =====================================================================

    /** Assert the java.util.Map collection views agree with a HashMap oracle
     *  (values must be distinct so the values-Collection compares as a Set). */
    private static void assertViewsMatchOracle(Map<Long, Long> m, Map<Long, Long> oracle) {
        assertEquals("size", oracle.size(), m.size());
        assertEquals("isEmpty", oracle.isEmpty(), m.isEmpty());
        assertEquals("entrySet", new HashSet<>(oracle.entrySet()), new HashSet<>(m.entrySet()));
        assertEquals("keySet", new HashSet<>(oracle.keySet()), new HashSet<>(m.keySet()));
        assertEquals("values", new HashSet<>(oracle.values()), new HashSet<>(m.values()));
        assertEquals("entrySet.size", oracle.size(), m.entrySet().size());
        assertEquals("keySet.size", oracle.size(), m.keySet().size());
        assertEquals("values.size", oracle.size(), m.values().size());
        for (Map.Entry<Long, Long> e : oracle.entrySet()) {
            assertTrue("keySet.contains " + e.getKey(), m.keySet().contains(e.getKey()));
            assertTrue("values.contains " + e.getValue(), m.values().contains(e.getValue()));
            assertTrue("containsValue " + e.getValue(), m.containsValue(e.getValue()));
        }
        assertFalse(m.containsValue(-999_999L));
        assertTrue(m.equals(oracle));
        assertTrue(oracle.equals(m));
        assertEquals(oracle.hashCode(), m.hashCode());
    }

    @Test
    public void sizeAgreesWithSizeLong() {
        HTreeMapExternal<Long, Long> m = longMap();
        assertEquals(0, m.size());
        assertTrue(m.isEmpty());
        for (long k = 0; k < 1_000; k++) m.put(k, k);
        assertEquals(1_000, m.size());
        assertEquals(m.sizeLong(), (long) m.size());
        assertFalse(m.isEmpty());
        for (long k = 0; k < 400; k++) m.remove(k);
        assertEquals(600, m.size());
        assertEquals(m.sizeLong(), (long) m.size());
    }

    @Test
    public void collectionViewsMatchOracle() {
        HTreeMapExternal<Long, Long> m = longMap();
        HashMap<Long, Long> oracle = new HashMap<>();
        for (long k = 0; k < 1_500; k++) { m.put(k, k * 7 + 1); oracle.put(k, k * 7 + 1); }
        assertViewsMatchOracle(m, oracle);
    }

    @Test
    public void entrySetContainsAndRemove() {
        HTreeMapExternal<Long, Long> m = longMap();
        for (long k = 0; k < 20; k++) m.put(k, k * 2);
        Set<Map.Entry<Long, Long>> es = m.entrySet();
        assertEquals(20, es.size());
        assertTrue(es.contains(new AbstractMap.SimpleImmutableEntry<>(3L, 6L)));
        assertFalse(es.contains(new AbstractMap.SimpleImmutableEntry<>(3L, 7L)));
        assertFalse(es.contains(new AbstractMap.SimpleImmutableEntry<>(99L, 0L)));
        assertFalse(es.contains("not an entry"));
        assertFalse(es.remove(new AbstractMap.SimpleImmutableEntry<>(3L, 7L)));
        assertTrue(m.containsKey(3L));
        assertTrue(es.remove(new AbstractMap.SimpleImmutableEntry<>(3L, 6L)));
        assertFalse(m.containsKey(3L));
        assertEquals(19L, m.sizeLong());
        es.clear();
        assertTrue(m.isEmpty());
        assertEquals("view clear must free value records", 17L, liveRecidCount());
    }

    /** The entrySet/keySet/values views are mutable: iterator.remove() and the derived
     *  Collection.remove/removeAll all delete from the live map, freeing value records. */
    @Test
    public void viewIteratorsSupportRemoval() {
        HTreeMapExternal<Long, Long> m = longMap();
        for (long k = 0; k < 20; k++) m.put(k, k);

        Iterator<Map.Entry<Long, Long>> eit = m.entrySet().iterator();
        Long removedKey = eit.next().getKey();
        eit.remove();
        assertFalse(m.containsKey(removedKey));
        assertEquals(19L, m.sizeLong());
        try { eit.remove(); fail("double remove must throw IllegalStateException"); }
        catch (IllegalStateException expected) { /* ok */ }

        Long k2 = m.keySet().iterator().next();
        assertTrue(m.keySet().remove(k2));
        assertFalse(m.containsKey(k2));
        Long v3 = m.values().iterator().next();
        assertTrue(m.values().remove(v3));
        assertEquals(17L, m.sizeLong());

        m.entrySet().removeAll(new HashSet<>(m.entrySet()));
        assertTrue(m.isEmpty());
        assertEquals("view removal must free value records", 17L, liveRecidCount());
    }

    /** Blind putOnly/removeOnly: correct values AND no value-record leak (the blind paths
     *  must still free the external value record even though they never read it). */
    @Test
    public void blindPutAndRemove() {
        HTreeMapExternal<Long, Long> m = longMap();
        for (long k = 0; k < 200; k++) m.putOnly(k, k * 7 + 1);      // blind inserts
        assertEquals(200L, m.sizeLong());
        for (long k = 0; k < 200; k++) assertEquals(Long.valueOf(k * 7 + 1), m.get(k));
        for (long k = 0; k < 200; k++) m.putOnly(k, k * 7 + 2);      // blind overwrites (no old read)
        for (long k = 0; k < 200; k++) assertEquals(Long.valueOf(k * 7 + 2), m.get(k));
        assertEquals(200L, m.sizeLong());

        assertTrue(m.removeOnly(0L));                                 // existed
        assertFalse(m.removeOnly(0L));                               // already gone
        assertNull(m.get(0L));
        for (long k = 1; k < 200; k++) assertTrue(m.removeOnly(k));   // blind removes
        assertTrue(m.isEmpty());
        assertEquals("blind put/remove must not leak value records", 17L, liveRecidCount());

        try { m.putOnly(null, 1L); fail("null key"); } catch (NullPointerException ok) { }
        try { m.putOnly(1L, null); fail("null value"); } catch (NullPointerException ok) { }
        try { m.removeOnly(null); fail("null key"); } catch (NullPointerException ok) { }
    }

    @Test
    public void putAllAndClearThroughMapInterface() {
        HashMap<Long, Long> src = new HashMap<>();
        for (long k = 0; k < 500; k++) src.put(k, k * 3 + 1);
        Map<Long, Long> m = longMap();
        m.putAll(src);
        assertViewsMatchOracle(m, src);
        m.clear();
        assertTrue(m.isEmpty());
        assertEquals(0, m.size());
        assertTrue(m.entrySet().isEmpty());
        assertEquals("putAll/clear must free value records", 17L, liveRecidCount());
    }

    @Test
    public void mapEqualsAndHashCode() {
        HTreeMapExternal<Long, Long> m = longMap();
        HashMap<Long, Long> oracle = new HashMap<>();
        for (long k = 0; k < 300; k++) { m.put(k, k + 1); oracle.put(k, k + 1); }
        assertTrue(m.equals(oracle));
        assertEquals(oracle.hashCode(), m.hashCode());
        m.put(0L, 999L);
        assertFalse(m.equals(oracle));
        assertFalse(oracle.equals(m));
        m.put(0L, 1L);
        assertTrue(m.equals(oracle));
        m.put(-1L, -1L);
        assertFalse(m.equals(oracle));
        assertTrue(m.equals(m));
        HTreeMapExternal<Long, Long> empty =
                HTreeMapExternal.create(store, Serializers.LONG, Serializers.LONG);
        assertTrue(empty.equals(new HashMap<Long, Long>()));
    }

    @Test
    public void concurrentMapCasAndDefaultsThroughInterface() {
        ConcurrentMap<Long, Long> cm = longMap();
        assertNull(cm.putIfAbsent(1L, 10L));
        assertEquals(Long.valueOf(10L), cm.putIfAbsent(1L, 20L));
        assertEquals(Long.valueOf(10L), cm.getOrDefault(1L, -1L));
        assertEquals(Long.valueOf(-1L), cm.getOrDefault(2L, -1L));
        assertNull(cm.replace(2L, 99L));
        assertFalse(cm.replace(2L, 1L, 2L));
        assertEquals(Long.valueOf(10L), cm.replace(1L, 11L));
        assertFalse(cm.replace(1L, 999L, 12L));
        assertTrue(cm.replace(1L, 11L, 12L));
        assertFalse(cm.remove(1L, 999L));
        assertTrue(cm.remove(1L, 12L));
        assertFalse(cm.containsKey(1L));
        assertEquals(Long.valueOf(5L), cm.computeIfAbsent(3L, k -> 5L));
        assertEquals(Long.valueOf(5L), cm.computeIfAbsent(3L, k -> 999L));
        assertEquals(Long.valueOf(8L), cm.merge(3L, 3L, Long::sum));
        assertEquals(Long.valueOf(100L), cm.compute(4L, (k, v) -> 100L));
        assertNull(cm.compute(4L, (k, v) -> null));
        assertFalse(cm.containsKey(4L));
        assertEquals(Long.valueOf(8L), cm.get(3L));
    }

    @Test
    public void drivenPurelyThroughMapInterface() {
        Map<Long, Long> map = longMap();
        HashMap<Long, Long> oracle = new HashMap<>();
        Random rnd = new Random(2024);
        for (int i = 0; i < 15_000; i++) {
            long k = rnd.nextInt(2_000);
            int roll = rnd.nextInt(100);
            if (roll < 55) {
                long v = rnd.nextLong();
                assertEquals(oracle.put(k, v), map.put(k, v));
            } else if (roll < 80) {
                assertEquals(oracle.remove(k), map.remove(k));
            } else {
                assertEquals(oracle.get(k), map.get(k));
            }
            if ((i & 2047) == 2047) assertEquals(oracle.size(), map.size());
        }
        assertEquals(oracle.size(), map.size());
        assertEquals(oracle, map);
        assertEquals(map, oracle);
        HashMap<Long, Long> viaEntrySet = new HashMap<>();
        for (Map.Entry<Long, Long> e : map.entrySet()) viaEntrySet.put(e.getKey(), e.getValue());
        assertEquals(oracle, viaEntrySet);
        map.clear();
        assertTrue(map.isEmpty());
        assertEquals("interface-driven drain must free value records", 17L, liveRecidCount());
    }
}
