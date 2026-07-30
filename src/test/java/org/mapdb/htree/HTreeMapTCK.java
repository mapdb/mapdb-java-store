package org.mapdb.htree;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;
import org.mapdb.ser.Serializer;
import org.mapdb.hash.Hasher;
import org.mapdb.hash.Hashers;
import org.mapdb.ser.Serializers;
import org.mapdb.store.Store;

import java.util.Arrays;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
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
 * Abstract TCK for {@link HTreeMap}. Concrete subclasses bind {@link #openStore()}
 * to a specific Store dialect (heap / byte-array / direct / WAL). Every case runs
 * unchanged across all four. Unlike the BTree TCK there are NO ordering assertions:
 * iteration is compared as a set.
 */
public abstract class HTreeMapTCK {

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

    // ---------- helpers ----------

    private HTreeMap<Long, Long> longMap() {
        return HTreeMap.create(store, Serializers.LONG, Serializers.LONG);
    }

    private HTreeMap<String, String> stringMap() {
        return HTreeMap.create(store, Serializers.STRING, Serializers.STRING);
    }

    /** Live records in the store: the record-leak gate. A default-config map with no
     *  entries owns EXACTLY 17 records (header + 16 segment roots), so any drained-empty
     *  map whose store holds more has leaked leaf/dir records. */
    private long liveRecidCount() {
        long n = 0;
        for (PrimitiveIterator.OfLong it = store.getAllRecids(); it.hasNext(); it.nextLong()) n++;
        return n;
    }

    /** Drain the map's iterator into a HashMap, asserting no duplicate keys. */
    private static <K, V> Map<K, V> drain(HTreeMap<K, V> m) {
        Map<K, V> out = new HashMap<>();
        Iterator<Map.Entry<K, V>> it = m.entryIterator();
        while (it.hasNext()) {
            Map.Entry<K, V> e = it.next();
            assertNull("duplicate key in iteration: " + e.getKey(), out.put(e.getKey(), e.getValue()));
        }
        return out;
    }

    private static <K, V> void assertMatches(HTreeMap<K, V> m, Map<K, V> oracle) {
        assertEquals((long) oracle.size(), m.sizeLong());
        for (Map.Entry<K, V> e : oracle.entrySet()) {
            assertEquals("get " + e.getKey(), e.getValue(), m.get(e.getKey()));
        }
        assertEquals("iteration mismatch", oracle, drain(m));
        // forEach agrees with the iterator
        Map<K, V> viaForEach = new HashMap<>();
        m.forEach(viaForEach::put);
        assertEquals("forEach mismatch", oracle, viaForEach);
    }

    // =====================================================================

    @Test
    public void emptyMap() {
        HTreeMap<Long, Long> m = longMap();
        assertNull(m.get(1L));
        assertEquals(0L, m.sizeLong());
        assertFalse(m.entryIterator().hasNext());
        m.forEach((k, v) -> fail("forEach on empty map visited " + k));
    }

    @Test
    public void singleEntryLifecycle() {
        HTreeMap<Long, Long> m = longMap();

        assertNull(m.put(10L, 100L));
        assertEquals(Long.valueOf(100L), m.get(10L));
        assertEquals(1L, m.sizeLong());

        // overwrite returns old, size stays 1
        assertEquals(Long.valueOf(100L), m.put(10L, 200L));
        assertEquals(Long.valueOf(200L), m.get(10L));
        assertEquals(1L, m.sizeLong());

        Iterator<Map.Entry<Long, Long>> it = m.entryIterator();
        assertTrue(it.hasNext());
        Map.Entry<Long, Long> e = it.next();
        assertEquals(Long.valueOf(10L), e.getKey());
        assertEquals(Long.valueOf(200L), e.getValue());
        assertFalse(it.hasNext());
    }

    @Test
    public void bulkShuffled() {
        final int N = 50_000;
        long[] keys = new long[N];
        for (int i = 0; i < N; i++) keys[i] = i;
        Random rnd = new Random(42);
        for (int i = N - 1; i > 0; i--) {
            int j = rnd.nextInt(i + 1);
            long t = keys[i]; keys[i] = keys[j]; keys[j] = t;
        }

        HTreeMap<Long, Long> m = longMap();
        for (long k : keys) {
            assertNull("duplicate insert " + k, m.put(k, k * 7 + 1));
        }
        assertEquals(N, m.sizeLong());
        for (long k : keys) {
            assertEquals("get " + k, Long.valueOf(k * 7 + 1), m.get(k));
        }
        assertNull(m.get((long) N)); // never inserted

        // iteration yields exactly the inserted set (no order)
        Map<Long, Long> got = drain(m);
        assertEquals(N, got.size());
        for (long k : keys) {
            assertEquals(Long.valueOf(k * 7 + 1), got.get(k));
        }
    }

    @Test
    public void createFromSortedByHashBasic() {
        final int seed = 1234567;
        Hasher<Long> hasher = Hashers.mixing(k -> Long.hashCode(k));
        List<Map.Entry<Long, Long>> entries = new ArrayList<>();
        HashMap<Long, Long> oracle = new HashMap<>();
        for (long k = 0; k < 20_000; k++) {
            long value = k * 13 + 9;
            entries.add(new AbstractMap.SimpleImmutableEntry<>(k, value));
            oracle.put(k, value);
        }
        entries.sort((a, b) -> Integer.compareUnsigned(
                hasher.hash(a.getKey(), seed), hasher.hash(b.getKey(), seed)));

        HTreeMap<Long, Long> m = HTreeMap.createFromSortedByHash(store, Serializers.LONG, Serializers.LONG,
                4, 7, 4, seed, hasher, entries.iterator());

        assertMatches(m, oracle);
        long recid = m.headerRecid();
        HTreeMap<Long, Long> reopened = HTreeMap.open(store, recid, Serializers.LONG, Serializers.LONG, hasher);
        assertMatches(reopened, oracle);
    }

    @Test
    public void createFromSortedByHashRejectsSignedOrder() {
        Hasher<Long> identity = (key, seed) -> key.intValue();
        List<Map.Entry<Long, Long>> entries = Arrays.asList(
                new AbstractMap.SimpleImmutableEntry<>(-1L, 1L),
                new AbstractMap.SimpleImmutableEntry<>(0L, 2L));
        try {
            HTreeMap.createFromSortedByHash(store, Serializers.LONG, Serializers.LONG,
                    4, 7, 4, 0, identity, entries.iterator());
            fail("expected NotSorted");
        } catch (org.mapdb.DBException.NotSorted expected) {
            // expected
        }
    }

    @Test
    public void createFromSortedByHashRejectsDuplicateKeysInBucket() {
        Hasher<Long> constant = (key, seed) -> 0;
        List<Map.Entry<Long, Long>> entries = Arrays.asList(
                new AbstractMap.SimpleImmutableEntry<>(1L, 10L),
                new AbstractMap.SimpleImmutableEntry<>(1L, 20L));
        try {
            HTreeMap.createFromSortedByHash(store, Serializers.LONG, Serializers.LONG,
                    4, 7, 4, 0, constant, entries.iterator());
            fail("expected NotSorted");
        } catch (org.mapdb.DBException.NotSorted expected) {
            // expected
        }
    }

    // ---------- seeded fuzz vs HashMap oracle (put/get; remove is out of scope) ----------

    @Test
    public void fuzzAgainstHashMap() {
        final long seed = 0x5EEDC0FFEEL;
        System.out.println("[HTreeMapTCK.fuzzAgainstHashMap] store=" + store.getClass().getSimpleName()
                + " seed=" + seed);
        Random rnd = new Random(seed);
        final int KEYSPACE = 20_000;
        final int OPS = 100_000;

        HTreeMap<Long, Long> m = longMap();
        HashMap<Long, Long> oracle = new HashMap<>();

        for (int i = 0; i < OPS; i++) {
            long key = rnd.nextInt(KEYSPACE);
            int roll = rnd.nextInt(100);
            if (roll < 70) { // put (fresh or overwrite)
                long val = rnd.nextLong();
                Long expected = oracle.put(key, val);
                Long actual = m.put(key, val);
                assertEquals("put[" + i + "] key=" + key, expected, actual);
            } else { // get
                Long expected = oracle.get(key);
                Long actual = m.get(key);
                assertEquals("get[" + i + "] key=" + key, expected, actual);
            }
        }
        assertMatches(m, oracle);
    }

    @Test
    public void stringKeysAndValues() {
        HTreeMap<String, String> m = stringMap();
        HashMap<String, String> oracle = new HashMap<>();
        Random rnd = new Random(7);
        final int N = 10_000;

        for (int i = 0; i < N; i++) {
            String k = randomString(rnd, 1 + rnd.nextInt(12));
            String v = randomString(rnd, 1 + rnd.nextInt(20));
            assertEquals(oracle.put(k, v), m.put(k, v));
        }
        for (int i = 0; i < 1000; i++) {
            assertNull(m.get("ABSENT-" + i));
        }
        assertMatches(m, oracle);
    }

    private static String randomString(Random rnd, int len) {
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) sb.append((char) ('a' + rnd.nextInt(26)));
        return sb.toString();
    }

    // ---------- alternate geometries ----------

    /** Single segment (concShift=0), deep narrow tree: 8 levels x 4 bits. */
    @Test
    public void singleSegmentDeepTree() {
        HTreeMap<Long, Long> m = HTreeMap.create(store, Serializers.LONG, Serializers.LONG, 0, 4, 8);
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
        HTreeMap<Long, Long> m = HTreeMap.create(store, Serializers.LONG, Serializers.LONG, 8, 6, 4);
        HashMap<Long, Long> oracle = new HashMap<>();
        Random rnd = new Random(13);
        for (int i = 0; i < 20_000; i++) {
            long k = rnd.nextInt(8_000);
            long v = rnd.nextLong();
            assertEquals(oracle.put(k, v), m.put(k, v));
        }
        assertMatches(m, oracle);
    }

    @Test
    public void invalidConfigRejected() {
        try {
            HTreeMap.create(store, Serializers.LONG, Serializers.LONG, 4, 7, 3); // 4+21 != 32
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) { /* ok */ }
        try {
            HTreeMap.create(store, Serializers.LONG, Serializers.LONG, 4, 8, 4); // dirShift > 7
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) { /* ok */ }
    }

    // ---------- collision-heavy bucket growth (§0.10) ----------

    /**
     * Constant keyHash: every key lands in ONE segment and ONE dir index, so all
     * entries pile into a single bucket record. Exercises bucket growth and the
     * linear equals-scan on both get and overwrite.
     */
    @Test
    public void collisionHeavy() {
        Hasher<Long> constantHash = (k, seed) -> 0;
        HTreeMap<Long, Long> m = HTreeMap.create(store, Serializers.LONG, Serializers.LONG,
                4, 7, 4, 0, constantHash);
        HashMap<Long, Long> oracle = new HashMap<>();
        final int N = 300;

        for (long k = 0; k < N; k++) {
            assertNull(m.put(k, k * 3));
            oracle.put(k, k * 3);
        }
        // overwrite every other key
        for (long k = 0; k < N; k += 2) {
            assertEquals(Long.valueOf(k * 3), m.put(k, -k));
            oracle.put(k, -k);
        }
        assertNull(m.get((long) N)); // absent key shares the bucket, scan must miss
        assertMatches(m, oracle);
    }

    // ---------- targeted lazy-split coverage (§0.10) ----------

    /**
     * Drives the dir-tree's lazy-split path deterministically THROUGH the map, on
     * every store dialect: an identity Hasher makes each Long key's hash equal the
     * key itself, giving full control over segment and dir index.
     * Indices are chosen to collide in the top-level slot and diverge at each lower
     * level (dirShift=7, levels=4: level L consumes bits [7L, 7L+7)).
     */
    @Test
    public void deepSplitTargetedIndices() {
        Hasher<Long> identityHash = (k, seed) -> (int) k.longValue();
        HTreeMap<Long, Long> m = HTreeMap.create(store, Serializers.LONG, Serializers.LONG,
                4, 7, 4, 0, identityHash);

        // all < 2^28 => all segment 0; index == key
        long[] indices = {
                0,                          // root slot 0, terminal at top
                1,                          // collides with 0 at levels 3..1, diverges at level 0
                1L << 7,                    // diverges at level 1
                1L << 14,                   // diverges at level 2
                1L << 21,                   // diverges at level 3 (top)
                (5L << 21) | (3L << 14),    // shares nothing with the above
                (5L << 21) | (9L << 7),     // shares top slot with previous, diverges at level 2
                (5L << 21) | (9L << 7) | 1, // shares levels 3..1 with previous, diverges at level 0
                0x0FFFFFFFL,                // max index
        };

        HashMap<Long, Long> oracle = new HashMap<>();
        for (long idx : indices) {
            assertNull("fresh insert " + idx, m.put(idx, idx + 1000));
            oracle.put(idx, idx + 1000);
            // after every insert (i.e. after every split step) ALL previous keys survive
            for (Map.Entry<Long, Long> e : oracle.entrySet()) {
                assertEquals("get " + e.getKey() + " after inserting " + idx,
                        e.getValue(), m.get(e.getKey()));
            }
        }
        // absent indices that share slots with present ones must miss
        assertNull(m.get(2L));
        assertNull(m.get(2L << 7));
        assertNull(m.get((5L << 21) | (9L << 7) | 2));
        assertMatches(m, oracle);

        // overwrite through the split tree
        for (long idx : indices) {
            assertEquals(Long.valueOf(idx + 1000), m.put(idx, idx + 2000));
            oracle.put(idx, idx + 2000);
        }
        assertMatches(m, oracle);
    }

    // ---------- second handle via open() on the same store ----------

    @Test
    public void openSecondHandleSameStore() {
        HTreeMap<Long, Long> m = longMap();
        HashMap<Long, Long> oracle = new HashMap<>();
        for (long k = 0; k < 1_000; k++) {
            m.put(k, k * 11);
            oracle.put(k, k * 11);
        }
        HTreeMap<Long, Long> m2 = HTreeMap.open(store, m.headerRecid(),
                Serializers.LONG, Serializers.LONG);
        assertMatches(m2, oracle);
    }

    // ---------- null rejection ----------

    @Test
    public void putNullKeyRejected() {
        HTreeMap<Long, Long> m = longMap();
        try {
            m.put(null, 1L);
            fail("expected NullPointerException");
        } catch (NullPointerException expected) { /* ok */ }
    }

    @Test
    public void putNullValueRejected() {
        HTreeMap<Long, Long> m = longMap();
        try {
            m.put(1L, null);
            fail("expected NullPointerException");
        } catch (NullPointerException expected) { /* ok */ }
    }

    @Test
    public void getNullKeyRejected() {
        HTreeMap<Long, Long> m = longMap();
        try {
            m.get(null);
            fail("expected NullPointerException");
        } catch (NullPointerException expected) { /* ok */ }
    }

    // ---------- iterator snapshot allows mutation from callbacks (§0.5) ----------

    @Test
    public void forEachCallbackMayMutate() {
        HTreeMap<Long, Long> m = longMap();
        for (long k = 0; k < 100; k++) m.put(k, k);
        Set<Long> seen = new HashSet<>();
        m.forEach((k, v) -> {
            seen.add(k);
            // mutating inside the callback must not deadlock; inserts into
            // not-yet-drained segments MAY be visited (weakly consistent)
            if (k < 1_000_000) m.put(k + 1_000_000, v);
        });
        for (long k = 0; k < 100; k++) {
            assertTrue("original key not visited: " + k, seen.contains(k));
        }
        assertEquals(Long.valueOf(5L), m.get(1_000_005L));
        assertEquals(200L, m.sizeLong()); // 100 originals + 100 inserts
    }

    // =====================================================================
    // Ported / adapted coverage from mapdb1/2/3 HTreeMap tests.
    // Every case runs across all four store bindings via the TCK subclasses.
    // =====================================================================

    // ---------- new key/value types ----------

    /** Integer key & value roundtrip incl. sign boundaries (new boxed type vs Long/String). */
    @Test
    public void integerKeysAndValues() {
        HTreeMap<Integer, Integer> m =
                HTreeMap.create(store, Serializers.INTEGER, Serializers.INTEGER);
        HashMap<Integer, Integer> oracle = new HashMap<>();

        // boundary keys exercise the unsigned-widening in index()/segment()
        for (int k : new int[]{0, 1, -1, Integer.MIN_VALUE, Integer.MAX_VALUE, -12345, 12345}) {
            assertNull(m.put(k, ~k));
            oracle.put(k, ~k);
        }
        Random rnd = new Random(101);
        for (int i = 0; i < 10_000; i++) {
            int k = rnd.nextInt() >> (rnd.nextInt(20)); // spread magnitude, both signs
            int v = rnd.nextInt();
            assertEquals(oracle.put(k, v), m.put(k, v));
        }
        assertMatches(m, oracle);
    }

    /**
     * Arbitrary boxed key/value type (Character) with a caller-supplied inline
     * serializer — adapted from mapdb2 testUnicodeCharacterKeyInsertion. Proves the
     * map is agnostic to the boxed type as long as a Serializer is provided.
     */
    @Test
    public void characterKeysAndValues() {
        Serializer<Character> charSer = new Serializer<>() {
            @Override public void serialize(DataOutput2 out, Character v) { out.writeInt(v); }
            @Override public Character deserialize(DataInput2 in, int size) {
                return (char) in.readInt();
            }
        };
        HTreeMap<Character, Character> m = HTreeMap.create(store, charSer, charSer);
        HashMap<Character, Character> oracle = new HashMap<>();
        for (char c = 'À'; c < 'Ā'; c++) { // Latin-1 supplement block
            assertNull(m.put(c, c));
            oracle.put(c, c);
        }
        assertEquals(Character.valueOf('À'), m.get('À'));
        assertNull(m.get('A')); // ASCII 'A' never inserted
        assertMatches(m, oracle);
    }

    /**
     * byte[] keys need a caller-supplied content hash (byte[].hashCode is identity),
     * exercising the keyHash hook + BYTE_ARRAY.equals in the bucket scan — the intended
     * "array key" use case (mapdb1/2/3 `hasher`). Equal-content keys overwrite; distinct
     * content coexists; a fresh equal-content probe still hits.
     */
    @Test
    public void byteArrayContentHashKeys() {
        HTreeMap<byte[], byte[]> m = HTreeMap.create(store,
                Serializers.BYTE_ARRAY, Serializers.BYTE_ARRAY,
                4, 7, 4, Hashers.mixing(Arrays::hashCode));

        for (int i = 0; i < 5_000; i++) {
            byte[] k = ("key-" + i).getBytes();
            byte[] v = ("val-" + i).getBytes();
            assertNull("fresh " + i, m.put(k, v));
        }
        assertEquals(5_000L, m.sizeLong());
        // read back with FRESH byte[] instances of equal content: content-hash + equals must hit
        for (int i = 0; i < 5_000; i++) {
            assertTrue(Arrays.equals(("val-" + i).getBytes(), m.get(("key-" + i).getBytes())));
        }
        // overwrite via an independent equal-content key instance; old value returned
        byte[] old = m.put("key-42".getBytes(), "REPLACED".getBytes());
        assertTrue(Arrays.equals("val-42".getBytes(), old));
        assertTrue(Arrays.equals("REPLACED".getBytes(), m.get("key-42".getBytes())));
        assertEquals(5_000L, m.sizeLong()); // overwrite, not insert
        assertNull(m.get("absent".getBytes()));
    }

    /** Large byte[] values stored and read back intact (large-record path). */
    @Test
    public void largeByteArrayValues() {
        HTreeMap<Long, byte[]> m =
                HTreeMap.create(store, Serializers.LONG, Serializers.BYTE_ARRAY);
        HashMap<Long, byte[]> oracle = new HashMap<>();
        Random rnd = new Random(202);
        for (long k = 0; k < 200; k++) {
            byte[] v = new byte[1_000 + rnd.nextInt(30_000)];
            rnd.nextBytes(v);
            assertNull(m.put(k, v));
            oracle.put(k, v);
        }
        assertEquals(oracle.size(), m.sizeLong());
        for (Map.Entry<Long, byte[]> e : oracle.entrySet()) {
            assertTrue("value for " + e.getKey(), Arrays.equals(e.getValue(), m.get(e.getKey())));
        }
    }

    // ---------- edge-value cases ----------

    /** Empty String is a valid (non-null) key and value. */
    @Test
    public void emptyStringKeyAndValue() {
        HTreeMap<String, String> m = stringMap();
        assertNull(m.put("", ""));
        assertEquals("", m.get(""));
        assertNull(m.put("k", ""));
        assertEquals("", m.get("k"));
        assertEquals("", m.put("", "v")); // overwrite empty-key value, returns old ("")
        assertEquals("v", m.get(""));
        assertEquals(2L, m.sizeLong());
    }

    /** Long key sign/boundary handling through segment/index derivation. */
    @Test
    public void boundaryLongKeys() {
        HTreeMap<Long, Long> m = longMap();
        HashMap<Long, Long> oracle = new HashMap<>();
        for (long k : new long[]{0L, 1L, -1L, Long.MIN_VALUE, Long.MAX_VALUE,
                Integer.MIN_VALUE, Integer.MAX_VALUE, -1_000_000L, 1_000_000L}) {
            assertNull("fresh " + k, m.put(k, k));
            oracle.put(k, k);
        }
        assertMatches(m, oracle);
    }

    /** A single key overwritten many times: each put returns the exact prior value, size stays 1. */
    @Test
    public void overwriteReturnsOldValueRepeatedly() {
        HTreeMap<Long, Long> m = longMap();
        assertNull(m.put(7L, 0L));
        for (long v = 1; v <= 500; v++) {
            assertEquals("overwrite " + v, Long.valueOf(v - 1), m.put(7L, v));
            assertEquals(1L, m.sizeLong());
        }
        assertEquals(Long.valueOf(500L), m.get(7L));
    }

    // ---------- real hash collisions through the DEFAULT hasher ----------

    /**
     * Distinct String keys with IDENTICAL Java hashCode share one segment+dir-index
     * bucket even under the random hashSeed (hash() is a pure function of hashCode).
     * Verifies bucket linear-scan disambiguation with NO custom keyHash: each colliding
     * key keeps its own value, an un-inserted equal-hashCode sibling misses the populated
     * bucket, and overwrite of one bucket member returns its old value.
     */
    @Test
    public void equalHashCodeStringsShareBucket() {
        // every concatenation of blocks in {"Aa","BB"} has the same String hashCode,
        // so all 16 four-block combinations collide to one bucket.
        String[] blocks = {"Aa", "BB"};
        java.util.List<String> family = new java.util.ArrayList<>();
        for (int a = 0; a < 2; a++)
            for (int b = 0; b < 2; b++)
                for (int c = 0; c < 2; c++)
                    for (int d = 0; d < 2; d++)
                        family.add(blocks[a] + blocks[b] + blocks[c] + blocks[d]);
        // sanity: they really do all collide
        int h = family.get(0).hashCode();
        for (String s : family) assertEquals("hashCode collision precondition", h, s.hashCode());

        HTreeMap<String, String> m = stringMap();
        HashMap<String, String> oracle = new HashMap<>();
        // insert all but the last: the omitted sibling must miss the populated bucket
        String omitted = family.remove(family.size() - 1);
        for (String s : family) {
            assertNull("fresh " + s, m.put(s, "v-" + s));
            oracle.put(s, "v-" + s);
        }
        assertNull("equal-hash sibling not inserted must miss", m.get(omitted));
        assertEquals(family.size(), m.sizeLong()); // all in ONE bucket, none lost
        for (String s : family) assertEquals("v-" + s, m.get(s));

        // overwrite a mid-bucket member: returns old, size unchanged
        String mid = family.get(family.size() / 2);
        assertEquals("v-" + mid, m.put(mid, "NEW"));
        oracle.put(mid, "NEW");
        assertEquals(family.size(), m.sizeLong());
        assertMatches(m, oracle);

        // finally insert the omitted sibling: bucket grows, all coexist
        assertNull(m.put(omitted, "v-" + omitted));
        oracle.put(omitted, "v-" + omitted);
        assertMatches(m, oracle);
    }

    // ---------- iteration completeness across sparsely-populated distinct segments ----------

    /**
     * Adapted from mapdb1/2 testIteration (minus ordering, which this map does not
     * promise): keys are steered by an identity Hasher into a few DISTINCT
     * top-level segments; entryIterator must visit every entry across those segments
     * with none dropped or duplicated.
     */
    @Test
    public void sparseSegmentsIterationComplete() {
        // identity hasher: hash(key) == (int) key, so the top concShift(=4) bits are
        // the segment: key = ((long) seg << 28) | j  =>  lands exactly in segment `seg`.
        Hasher<Long> identityHash = (k, seed) -> (int) k.longValue();
        HTreeMap<Long, Long> m = HTreeMap.create(store, Serializers.LONG, Serializers.LONG,
                4, 7, 4, 0, identityHash);
        HashMap<Long, Long> oracle = new HashMap<>();

        int[] segments = {1, 7, 10, 15};
        for (int seg : segments) {
            for (int j = 0; j < 200; j++) {
                long key = ((long) seg << 28) | j;
                assertNull("fresh seg=" + seg + " j=" + j, m.put(key, key * 3));
                oracle.put(key, key * 3);
            }
        }
        assertEquals(segments.length * 200L, m.sizeLong());
        assertMatches(m, oracle); // drains every populated segment, iteration == oracle set
    }

    // =====================================================================
    // Tier-1 core-completeness ops: remove (+ dir collapse), CAS ops,
    // containsKey, clear, isEmpty. tearDown's store.verify() is the
    // leak/double-free gate for every case below.
    // =====================================================================

    @Test
    public void removeLifecycle() {
        HTreeMap<Long, Long> m = longMap();
        assertNull(m.remove(1L)); // remove on empty map
        assertNull(m.put(1L, 100L));
        assertEquals(Long.valueOf(100L), m.remove(1L));
        assertNull(m.get(1L));
        assertFalse(m.containsKey(1L));
        assertNull(m.remove(1L)); // second remove is a no-op
        assertEquals(0L, m.sizeLong());
        assertTrue(m.isEmpty());
        assertFalse(m.entryIterator().hasNext());
    }

    /**
     * THE workhorse: seeded random put/remove/get/containsKey/putIfAbsent/replace mix
     * vs a HashMap oracle. 100k ops over a 20k keyspace force bucket growth+shrink and
     * dir split+collapse many times over (this is the coverage the earlier test port
     * had to skip because remove did not exist).
     */
    @Test
    public void fuzzWithRemoves() {
        final long seed = 0xC0FFEE5EEDL;
        System.out.println("[HTreeMapTCK.fuzzWithRemoves] store=" + store.getClass().getSimpleName()
                + " seed=" + seed);
        Random rnd = new Random(seed);
        final int KEYSPACE = 20_000;
        final int OPS = 100_000;

        HTreeMap<Long, Long> m = longMap();
        HashMap<Long, Long> oracle = new HashMap<>();

        for (int i = 0; i < OPS; i++) {
            long key = rnd.nextInt(KEYSPACE);
            int roll = rnd.nextInt(100);
            if (roll < 40) { // put (fresh or overwrite)
                long val = rnd.nextLong();
                assertEquals("put[" + i + "] key=" + key, oracle.put(key, val), m.put(key, val));
            } else if (roll < 65) { // remove
                assertEquals("remove[" + i + "] key=" + key, oracle.remove(key), m.remove(key));
            } else if (roll < 80) { // get
                assertEquals("get[" + i + "] key=" + key, oracle.get(key), m.get(key));
            } else if (roll < 90) { // containsKey
                assertEquals("containsKey[" + i + "] key=" + key,
                        oracle.containsKey(key), m.containsKey(key));
            } else if (roll < 95) { // putIfAbsent
                long val = rnd.nextLong();
                assertEquals("putIfAbsent[" + i + "] key=" + key,
                        oracle.putIfAbsent(key, val), m.putIfAbsent(key, val));
            } else { // replace only-if-present
                long val = rnd.nextLong();
                assertEquals("replace[" + i + "] key=" + key,
                        oracle.replace(key, val), m.replace(key, val));
            }
        }
        assertEquals(oracle.isEmpty(), m.isEmpty());
        assertMatches(m, oracle);

        // drain the survivors one by one: every leaf and every collapsed dir record
        // must be freed — afterwards the store holds ONLY the header + segment roots
        for (Long key : new java.util.ArrayList<>(oracle.keySet())) {
            assertEquals(oracle.remove(key), m.remove(key));
        }
        assertTrue(m.isEmpty());
        assertEquals(0L, m.sizeLong());
        assertEquals("remove leaked records", 17L, liveRecidCount());
    }

    /** Insert N, remove two thirds (mass bucket collapse + dir collapse), verify
     *  survivors and absences, reinsert the removed with new values, verify all —
     *  catches collapse-then-regrow bugs (mirrors BTreeMapTCK.removeHeavyThenReinsert). */
    @Test
    public void removeHeavyThenReinsert() {
        HTreeMap<Long, Long> m = longMap();
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
                assertTrue(m.containsKey(k));
            }
        }

        // reinsert the removed keys with distinct values
        for (long k = 0; k < N; k++) {
            if (k % 3 != 0) assertNull("reinsert should be fresh " + k, m.put(k, k + 1_000_000));
        }
        for (long k = 0; k < N; k++) {
            Long expected = (k % 3 != 0) ? k + 1_000_000 : k;
            assertEquals("final get " + k, expected, m.get(k));
        }
        assertEquals((long) N, m.sizeLong());
    }

    /** Constant keyHash piles every key into ONE bucket: removing middle/first/last
     *  entries must leave the rest intact; draining the bucket entirely must collapse
     *  the leaf + dir path so a fresh insert starts clean. */
    @Test
    public void collisionBucketRemove() {
        Hasher<Long> constantHash = (k, seed) -> 0;
        HTreeMap<Long, Long> m = HTreeMap.create(store, Serializers.LONG, Serializers.LONG,
                4, 7, 4, 0, constantHash);
        final int N = 100;
        for (long k = 0; k < N; k++) assertNull(m.put(k, k * 3));

        // remove a middle entry: the rest of the bucket survives
        assertEquals(Long.valueOf(50L * 3), m.remove(50L));
        assertNull(m.get(50L));
        assertFalse(m.containsKey(50L));
        for (long k = 0; k < N; k++) {
            if (k == 50) continue;
            assertEquals("survivor " + k, Long.valueOf(k * 3), m.get(k));
        }
        assertEquals(N - 1L, m.sizeLong());

        // remove first- and last-inserted entries (head/tail array shrink paths)
        assertEquals(Long.valueOf(0L), m.remove(0L));
        assertEquals(Long.valueOf((N - 1) * 3L), m.remove((long) (N - 1)));
        assertEquals(N - 3L, m.sizeLong());

        // absent key hashing into the same bucket must miss without disturbing it
        assertNull(m.remove((long) N));
        assertEquals(N - 3L, m.sizeLong());

        // drain the bucket completely: the last removal collapses leaf + dir entry
        for (long k = 1; k < N - 1; k++) {
            if (k == 50) continue;
            assertEquals(Long.valueOf(k * 3), m.remove(k));
        }
        assertTrue(m.isEmpty());
        assertEquals(0L, m.sizeLong());
        assertEquals("bucket drain leaked records", 17L, liveRecidCount());

        // fully collapsed: reinsert starts a fresh bucket
        assertNull(m.put(50L, 1L));
        assertEquals(Long.valueOf(1L), m.get(50L));
        assertEquals(1L, m.sizeLong());
    }

    /** CAS return-value + effect semantics, mirrored against ConcurrentMap contracts. */
    @Test
    public void casOperations() {
        HTreeMap<Long, Long> m = longMap();

        // putIfAbsent
        assertNull(m.putIfAbsent(1L, 10L)); // inserted
        assertEquals(Long.valueOf(10L), m.putIfAbsent(1L, 20L)); // present: returns existing
        assertEquals(Long.valueOf(10L), m.get(1L)); // unchanged

        // replace(k, v): only if present
        assertNull(m.replace(2L, 20L)); // absent: no-op
        assertNull(m.get(2L));
        assertEquals(Long.valueOf(10L), m.replace(1L, 11L)); // present: swap
        assertEquals(Long.valueOf(11L), m.get(1L));

        // replace(k, old, new): only if current equals old
        assertFalse(m.replace(1L, 999L, 12L)); // wrong expected: no-op
        assertEquals(Long.valueOf(11L), m.get(1L));
        assertTrue(m.replace(1L, 11L, 12L)); // matching expected: swap
        assertEquals(Long.valueOf(12L), m.get(1L));
        assertFalse(m.replace(2L, 1L, 2L)); // absent key: false

        // remove(k, v): only if current equals v
        assertFalse(m.remove(1L, 999L)); // wrong value: kept
        assertTrue(m.containsKey(1L));
        assertTrue(m.remove(1L, 12L)); // matching value: removed
        assertNull(m.get(1L));
        assertFalse(m.remove(1L, 12L)); // already gone
        assertEquals(0L, m.sizeLong());
        assertTrue(m.isEmpty());
    }

    /** CAS ops inside a shared collision bucket: conditional match/mismatch must
     *  target the right entry among many equal-index neighbors. */
    @Test
    public void casOperationsInCollisionBucket() {
        Hasher<Long> constantHash = (k, seed) -> 0;
        HTreeMap<Long, Long> m = HTreeMap.create(store, Serializers.LONG, Serializers.LONG,
                4, 7, 4, 0, constantHash);
        HashMap<Long, Long> oracle = new HashMap<>();
        for (long k = 0; k < 50; k++) {
            m.put(k, k * 3);
            oracle.put(k, k * 3);
        }
        assertEquals(oracle.putIfAbsent(25L, -1L), m.putIfAbsent(25L, -1L));
        assertEquals(oracle.putIfAbsent(100L, -2L), m.putIfAbsent(100L, -2L));
        assertEquals(oracle.replace(30L, 999L), m.replace(30L, 999L));
        assertEquals(oracle.replace(30L, 999L, 1000L), m.replace(30L, 999L, 1000L));
        assertEquals(oracle.replace(31L, 555L, 666L), m.replace(31L, 555L, 666L)); // mismatch
        assertEquals(oracle.remove(40L, 40L * 3), m.remove(40L, 40L * 3));
        assertEquals(oracle.remove(41L, 555L), m.remove(41L, 555L)); // mismatch
        assertMatches(m, oracle);
    }

    /** Null rejection for every Tier-1 op, map undisturbed afterwards. */
    @Test
    public void tier1NullRejected() {
        HTreeMap<Long, Long> m = longMap();
        m.put(1L, 1L);
        Runnable[] calls = {
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

    /** Fill, clear, assert emptiness + all-absent, then reuse the same map: proves
     *  clear freed the old tree and reset the roots in place. */
    @Test
    public void clearThenReuse() {
        HTreeMap<Long, Long> m = longMap();
        assertTrue(m.isEmpty());
        m.clear(); // clear on an empty map is a no-op
        assertTrue(m.isEmpty());

        final int N = 10_000;
        for (long k = 0; k < N; k++) m.put(k, k * 7);
        assertFalse(m.isEmpty());
        assertEquals((long) N, m.sizeLong());

        m.clear();
        assertTrue(m.isEmpty());
        assertEquals(0L, m.sizeLong());
        assertEquals("clear leaked records", 17L, liveRecidCount());
        assertFalse(m.entryIterator().hasNext());
        for (long k = 0; k < N; k += 97) {
            assertNull(m.get(k));
            assertFalse(m.containsKey(k));
        }

        // the same handle is fully reusable after clear
        HashMap<Long, Long> oracle = new HashMap<>();
        for (long k = 0; k < 2_000; k++) {
            assertNull("fresh after clear " + k, m.put(k, k + 5));
            oracle.put(k, k + 5);
        }
        assertMatches(m, oracle);

        // a second handle opened on the same header sees the post-clear content
        HTreeMap<Long, Long> m2 = HTreeMap.open(store, m.headerRecid(),
                Serializers.LONG, Serializers.LONG);
        assertMatches(m2, oracle);
    }

    /** String keys with variable-length values: removes exercise the LOCKED leaf
     *  path (leafReadOptimistic is off) plus valueSer.equals-based CAS on strings. */
    @Test
    public void stringRemoveAndCas() {
        HTreeMap<String, String> m = stringMap();
        HashMap<String, String> oracle = new HashMap<>();
        Random rnd = new Random(31);
        for (int i = 0; i < 5_000; i++) {
            String k = randomString(rnd, 1 + rnd.nextInt(12));
            String v = randomString(rnd, 1 + rnd.nextInt(20));
            assertEquals(oracle.put(k, v), m.put(k, v));
        }
        // remove half the oracle's keys (fresh String instances: content equality)
        java.util.List<String> keys = new java.util.ArrayList<>(oracle.keySet());
        for (int i = 0; i < keys.size(); i += 2) {
            String k = new String(keys.get(i));
            assertEquals(oracle.remove(k), m.remove(k));
        }
        // conditional ops with fresh equal-content value instances
        String someKey = keys.get(1);
        String someVal = oracle.get(someKey);
        assertFalse(m.remove(someKey, someVal + "-x"));
        assertTrue(m.replace(someKey, new String(someVal), "swapped"));
        oracle.put(someKey, "swapped");
        assertTrue(m.remove(someKey, new String("swapped")));
        oracle.remove(someKey);
        assertMatches(m, oracle);
    }

    /** Targeted dir-collapse THROUGH the map: the deep-split ladder indices are
     *  inserted (forcing subdir chains), then removed one at a time — every removal
     *  step must keep all survivors resolvable (collapse pushes lone occupants up). */
    @Test
    public void deepSplitTargetedRemoves() {
        Hasher<Long> identityHash = (k, seed) -> (int) k.longValue();
        HTreeMap<Long, Long> m = HTreeMap.create(store, Serializers.LONG, Serializers.LONG,
                4, 7, 4, 0, identityHash);

        long[] indices = {
                0,                          // root slot 0, terminal at top
                1,                          // collides with 0 at levels 3..1, diverges at level 0
                1L << 7,                    // diverges at level 1
                1L << 14,                   // diverges at level 2
                1L << 21,                   // diverges at level 3 (top)
                (5L << 21) | (3L << 14),    // shares nothing with the above
                (5L << 21) | (9L << 7),     // shares top slot with previous, diverges at level 2
                (5L << 21) | (9L << 7) | 1, // shares levels 3..1 with previous, diverges at level 0
                0x0FFFFFFFL,                // max index
        };

        HashMap<Long, Long> oracle = new HashMap<>();
        for (long idx : indices) {
            assertNull(m.put(idx, idx + 1000));
            oracle.put(idx, idx + 1000);
        }
        // remove one at a time; after each removal all survivors must still resolve
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

        // collapse-then-regrow: the same ladder re-splits cleanly after full removal
        for (long idx : indices) assertNull("reinsert " + idx, m.put(idx, idx + 2000));
        for (long idx : indices) assertEquals(Long.valueOf(idx + 2000), m.get(idx));
    }

    // =====================================================================
    // java.util.Map / ConcurrentMap interface conformance (the newly-added
    // surface: size/entrySet/keySet/values/containsValue/putAll, Map.equals/
    // hashCode, the snapshot-view iterators, and the ConcurrentMap CAS + default
    // methods reached purely through the interface). Runs across every dialect.
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
        // Map.equals / hashCode against the oracle, in both directions
        assertTrue(m.equals(oracle));
        assertTrue(oracle.equals(m));
        assertEquals(oracle.hashCode(), m.hashCode());
    }

    /** size() equals sizeLong() for a small map; both track put/remove and isEmpty. */
    @Test
    public void sizeAgreesWithSizeLong() {
        HTreeMap<Long, Long> m = longMap();
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
        HTreeMap<Long, Long> m = longMap();
        HashMap<Long, Long> oracle = new HashMap<>();
        for (long k = 0; k < 1_500; k++) { m.put(k, k * 7 + 1); oracle.put(k, k * 7 + 1); }
        assertViewsMatchOracle(m, oracle);
    }

    /** entrySet().contains and the CAS-based entrySet().remove(entry), plus view clear(). */
    @Test
    public void entrySetContainsAndRemove() {
        HTreeMap<Long, Long> m = longMap();
        for (long k = 0; k < 20; k++) m.put(k, k * 2);
        Set<Map.Entry<Long, Long>> es = m.entrySet();
        assertEquals(20, es.size());
        assertTrue(es.contains(new AbstractMap.SimpleImmutableEntry<>(3L, 6L)));
        assertFalse(es.contains(new AbstractMap.SimpleImmutableEntry<>(3L, 7L)));  // wrong value
        assertFalse(es.contains(new AbstractMap.SimpleImmutableEntry<>(99L, 0L))); // absent key
        assertFalse(es.contains("not an entry"));
        // CAS-remove: wrong value leaves the entry, matching value removes it
        assertFalse(es.remove(new AbstractMap.SimpleImmutableEntry<>(3L, 7L)));
        assertTrue(m.containsKey(3L));
        assertTrue(es.remove(new AbstractMap.SimpleImmutableEntry<>(3L, 6L)));
        assertFalse(m.containsKey(3L));
        assertEquals(19L, m.sizeLong());
        es.clear(); // view clear() delegates to the map
        assertTrue(m.isEmpty());
    }

    /** The entrySet/keySet/values views are mutable: iterator.remove() and the derived
     *  Collection.remove/removeAll all delete from the live map (weakly-consistent
     *  snapshot iteration, but remove() acts on the live map by key). */
    @Test
    public void viewIteratorsSupportRemoval() {
        HTreeMap<Long, Long> m = longMap();
        for (long k = 0; k < 20; k++) m.put(k, k);

        // entrySet iterator.remove() deletes the last-returned entry from the live map
        Iterator<Map.Entry<Long, Long>> eit = m.entrySet().iterator();
        Long removedKey = eit.next().getKey();
        eit.remove();
        assertFalse(m.containsKey(removedKey));
        assertEquals(19L, m.sizeLong());
        try { eit.remove(); fail("double remove must throw IllegalStateException"); }
        catch (IllegalStateException expected) { /* ok */ }

        // keySet().remove(k) and values().remove(v) route through iterator.remove()
        Long k2 = m.keySet().iterator().next();
        assertTrue(m.keySet().remove(k2));
        assertFalse(m.containsKey(k2));
        Long v3 = m.values().iterator().next();
        assertTrue(m.values().remove(v3));
        assertEquals(17L, m.sizeLong());

        // bulk removeAll through the view empties the map
        m.entrySet().removeAll(new HashSet<>(m.entrySet()));
        assertTrue(m.isEmpty());
    }

    /** Blind putOnly/removeOnly: insert/overwrite/delete without returning the old value. */
    @Test
    public void blindPutAndRemove() {
        HTreeMap<Long, Long> m = longMap();
        m.putOnly(1L, 10L);                          // insert
        assertEquals(Long.valueOf(10L), m.get(1L));
        m.putOnly(1L, 20L);                          // overwrite, nothing returned
        assertEquals(Long.valueOf(20L), m.get(1L));
        assertEquals(1L, m.sizeLong());
        assertTrue(m.removeOnly(1L));                // existed
        assertNull(m.get(1L));
        assertFalse(m.removeOnly(1L));               // already gone
        assertEquals(0L, m.sizeLong());
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
    }

    @Test
    public void mapEqualsAndHashCode() {
        HTreeMap<Long, Long> m = longMap();
        HashMap<Long, Long> oracle = new HashMap<>();
        for (long k = 0; k < 300; k++) { m.put(k, k + 1); oracle.put(k, k + 1); }
        assertTrue(m.equals(oracle));
        assertEquals(oracle.hashCode(), m.hashCode());
        m.put(0L, 999L);                    // one differing value breaks equality
        assertFalse(m.equals(oracle));
        assertFalse(oracle.equals(m));
        m.put(0L, 1L);
        assertTrue(m.equals(oracle));
        m.put(-1L, -1L);                    // an extra key breaks equality
        assertFalse(m.equals(oracle));
        assertTrue(m.equals(m));            // reflexive
        HTreeMap<Long, Long> empty = HTreeMap.create(store, Serializers.LONG, Serializers.LONG);
        assertTrue(empty.equals(new HashMap<Long, Long>()));
    }

    /** ConcurrentMap CAS + default methods driven through a ConcurrentMap-typed ref. */
    @Test
    public void concurrentMapCasAndDefaultsThroughInterface() {
        ConcurrentMap<Long, Long> cm = longMap();
        assertNull(cm.putIfAbsent(1L, 10L));
        assertEquals(Long.valueOf(10L), cm.putIfAbsent(1L, 20L));         // present: existing
        assertEquals(Long.valueOf(10L), cm.getOrDefault(1L, -1L));
        assertEquals(Long.valueOf(-1L), cm.getOrDefault(2L, -1L));        // absent: default
        assertNull(cm.replace(2L, 99L));                                  // absent: no-op
        assertFalse(cm.replace(2L, 1L, 2L));                             // absent: no-op
        assertEquals(Long.valueOf(10L), cm.replace(1L, 11L));
        assertFalse(cm.replace(1L, 999L, 12L));                          // wrong expected
        assertTrue(cm.replace(1L, 11L, 12L));
        assertFalse(cm.remove(1L, 999L));                               // wrong value
        assertTrue(cm.remove(1L, 12L));                                 // matching value
        assertFalse(cm.containsKey(1L));
        // default methods routed through the CAS primitives
        assertEquals(Long.valueOf(5L), cm.computeIfAbsent(3L, k -> 5L));
        assertEquals(Long.valueOf(5L), cm.computeIfAbsent(3L, k -> 999L)); // present: unchanged
        assertEquals(Long.valueOf(8L), cm.merge(3L, 3L, Long::sum));       // 5 + 3
        assertEquals(Long.valueOf(100L), cm.compute(4L, (k, v) -> 100L));
        assertNull(cm.compute(4L, (k, v) -> null));                        // null result removes
        assertFalse(cm.containsKey(4L));
        assertEquals(Long.valueOf(8L), cm.get(3L));
    }

    /** The whole point of the change: usable as a plain Map with no htree types in sight. */
    @Test
    public void drivenPurelyThroughMapInterface() {
        Map<Long, Long> map = longMap();
        HashMap<Long, Long> oracle = new HashMap<>();
        Random rnd = new Random(2024);
        for (int i = 0; i < 20_000; i++) {
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
        assertEquals(oracle, map);   // HashMap.equals(Map)
        assertEquals(map, oracle);   // AbstractMap.equals(Map)
        HashMap<Long, Long> viaEntrySet = new HashMap<>();
        for (Map.Entry<Long, Long> e : map.entrySet()) viaEntrySet.put(e.getKey(), e.getValue());
        assertEquals(oracle, viaEntrySet);
        map.clear();
        assertTrue(map.isEmpty());
    }
}
