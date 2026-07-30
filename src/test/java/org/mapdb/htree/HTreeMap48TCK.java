package org.mapdb.htree;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mapdb.hash.Hasher64;
import org.mapdb.hash.Hashers;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;
import org.mapdb.ser.Serializer;
import org.mapdb.ser.Serializers;
import org.mapdb.store.Store;

import java.util.Arrays;
import java.util.AbstractMap;
import java.util.ArrayList;
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
 * Abstract TCK for {@link HTreeMap48} (48-bit hash budget). Concrete subclasses bind
 * {@link #openStore()} to a Store dialect (heap / byte-array / direct / append-only /
 * WAL); every case runs unchanged across all five. Mirrors {@link HTreeMapTCK} with
 * the 48-bit specifics: default geometry 6/6/7, a {@link Hasher64}, and deep dir
 * indices that a 32-bit map could never reach.
 */
public abstract class HTreeMap48TCK {

    /** A fresh, empty store. Subclasses may allocate resources (temp files) here. */
    protected abstract Store openStore();

    protected Store store;

    /** Empty default-config (6/6/7) map: header + 64 segment roots. */
    private static final long DEFAULT_LIVE = 65L;
    /** Empty single-segment (0/6/8) map: header + 1 segment root. */
    private static final long SINGLE_SEG_LIVE = 2L;

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

    private HTreeMap48<Long, Long> longMap() {
        return HTreeMap48.create(store, Serializers.LONG, Serializers.LONG);
    }

    private HTreeMap48<String, String> stringMap() {
        return HTreeMap48.create(store, Serializers.STRING, Serializers.STRING);
    }

    /** Live records in the store: the record-leak gate. */
    private long liveRecidCount() {
        long n = 0;
        for (PrimitiveIterator.OfLong it = store.getAllRecids(); it.hasNext(); it.nextLong()) n++;
        return n;
    }

    /** Drain the map's iterator into a HashMap, asserting no duplicate keys. */
    private static <K, V> Map<K, V> drain(HTreeMap48<K, V> m) {
        Map<K, V> out = new HashMap<>();
        Iterator<Map.Entry<K, V>> it = m.entryIterator();
        while (it.hasNext()) {
            Map.Entry<K, V> e = it.next();
            assertNull("duplicate key in iteration: " + e.getKey(), out.put(e.getKey(), e.getValue()));
        }
        return out;
    }

    private static <K, V> void assertMatches(HTreeMap48<K, V> m, Map<K, V> oracle) {
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
        HTreeMap48<Long, Long> m = longMap();
        assertNull(m.get(1L));
        assertEquals(0L, m.sizeLong());
        assertFalse(m.entryIterator().hasNext());
        m.forEach((k, v) -> fail("forEach on empty map visited " + k));
    }

    /** Leak gates read straight off create(): 65 records default, 2 single-segment. */
    @Test
    public void emptyMapLeakGate() {
        HTreeMap48<Long, Long> m = longMap();
        assertEquals(DEFAULT_LIVE, liveRecidCount());
        assertTrue(m.isEmpty());
    }

    @Test
    public void singleEntryLifecycle() {
        HTreeMap48<Long, Long> m = longMap();

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

        HTreeMap48<Long, Long> m = longMap();
        for (long k : keys) {
            assertNull("duplicate insert " + k, m.put(k, k * 7 + 1));
        }
        assertEquals(N, m.sizeLong());
        for (long k : keys) {
            assertEquals("get " + k, Long.valueOf(k * 7 + 1), m.get(k));
        }
        assertNull(m.get((long) N)); // never inserted

        Map<Long, Long> got = drain(m);
        assertEquals(N, got.size());
        for (long k : keys) {
            assertEquals(Long.valueOf(k * 7 + 1), got.get(k));
        }
    }

    @Test
    public void createFromSortedByHashBasic() {
        final long seed = 0x1234ABCD5678EF90L;
        Hasher64<Long> hasher = Hashers.LONG64;
        ArrayList<Map.Entry<Long, Long>> entries = new ArrayList<>();
        HashMap<Long, Long> oracle = new HashMap<>();
        for (long k = 0; k < 20_000; k++) {
            long value = k * 19 + 11;
            entries.add(new AbstractMap.SimpleImmutableEntry<>(k, value));
            oracle.put(k, value);
        }
        entries.sort((a, b) -> Long.compareUnsigned(
                hasher.hash(a.getKey(), seed), hasher.hash(b.getKey(), seed)));

        HTreeMap48<Long, Long> m = HTreeMap48.createFromSortedByHash(store,
                Serializers.LONG, Serializers.LONG, 6, 6, 7, seed, hasher, entries.iterator());

        assertMatches(m, oracle);
        HTreeMap48<Long, Long> reopened = HTreeMap48.open(store, m.headerRecid(),
                Serializers.LONG, Serializers.LONG, hasher);
        assertMatches(reopened, oracle);
    }

    @Test
    public void createFromSortedByHashRejectsDuplicateKeysInTop48Bucket() {
        Hasher64<Long> constant = (key, seed) -> 0;
        java.util.List<Map.Entry<Long, Long>> entries = Arrays.asList(
                new AbstractMap.SimpleImmutableEntry<>(1L, 10L),
                new AbstractMap.SimpleImmutableEntry<>(1L, 20L));
        try {
            HTreeMap48.createFromSortedByHash(store, Serializers.LONG, Serializers.LONG,
                    6, 6, 7, 0L, constant, entries.iterator());
            fail("expected NotSorted");
        } catch (org.mapdb.DBException.NotSorted expected) {
            // expected
        }
    }

    // ---------- seeded fuzz vs HashMap oracle (default objectHasher64) ----------

    @Test
    public void fuzzAgainstHashMap() {
        final long seed = 0x5EEDC0FFEEL;
        System.out.println("[HTreeMap48TCK.fuzzAgainstHashMap] store=" + store.getClass().getSimpleName()
                + " seed=" + seed);
        Random rnd = new Random(seed);
        final int KEYSPACE = 20_000;
        final int OPS = 100_000;

        HTreeMap48<Long, Long> m = longMap();
        HashMap<Long, Long> oracle = new HashMap<>();

        for (int i = 0; i < OPS; i++) {
            long key = rnd.nextInt(KEYSPACE);
            int roll = rnd.nextInt(100);
            if (roll < 70) {
                long val = rnd.nextLong();
                assertEquals("put[" + i + "] key=" + key, oracle.put(key, val), m.put(key, val));
            } else {
                assertEquals("get[" + i + "] key=" + key, oracle.get(key), m.get(key));
            }
        }
        assertMatches(m, oracle);
    }

    @Test
    public void stringKeysAndValues() {
        HTreeMap48<String, String> m = stringMap();
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

    // ---------- alternate geometries (both sum to 48) ----------

    /** Single segment (concShift=0), deep tree: 8 levels x 6 bits. Leak gate = 2. */
    @Test
    public void singleSegmentDeepTree() {
        HTreeMap48<Long, Long> m = HTreeMap48.create(store, Serializers.LONG, Serializers.LONG,
                0, 6, 8, null);
        HashMap<Long, Long> oracle = new HashMap<>();
        Random rnd = new Random(11);
        for (int i = 0; i < 20_000; i++) {
            long k = rnd.nextInt(8_000);
            long v = rnd.nextLong();
            assertEquals(oracle.put(k, v), m.put(k, v));
        }
        assertMatches(m, oracle);
        for (Long key : new java.util.ArrayList<>(oracle.keySet())) m.remove(key);
        assertTrue(m.isEmpty());
        assertEquals("single-segment drain leaked records", SINGLE_SEG_LIVE, liveRecidCount());
    }

    /** Many segments (4096), shallow trees: 6 levels x 6 bits. */
    @Test
    public void manySegmentsShallowTree() {
        HTreeMap48<Long, Long> m = HTreeMap48.create(store, Serializers.LONG, Serializers.LONG,
                12, 6, 6, null);
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
        int[][] bad = {
                {4, 7, 4},   // 4 + 28 = 32 (was legal for the 32-bit map, now must FAIL)
                {6, 6, 8},   // 6 + 48 = 54
                {0, 8, 6},   // dirShift 8 > MAX_DIR_SHIFT
                {-1, 7, 7},  // concShift < 0
        };
        for (int[] cfg : bad) {
            try {
                HTreeMap48.create(store, Serializers.LONG, Serializers.LONG,
                        cfg[0], cfg[1], cfg[2], null);
                fail("expected IllegalArgumentException for " + Arrays.toString(cfg));
            } catch (IllegalArgumentException expected) { /* ok */ }
        }
    }

    // ---------- collision-heavy bucket growth (constant Hasher64) ----------

    /** Constant hash: every key piles into ONE segment + ONE dir index → single bucket. */
    @Test
    public void collisionHeavy() {
        Hasher64<Long> constantHash = (k, seed) -> 0L;
        HTreeMap48<Long, Long> m = HTreeMap48.create(store, Serializers.LONG, Serializers.LONG,
                6, 6, 7, 0L, constantHash);
        HashMap<Long, Long> oracle = new HashMap<>();
        final int N = 300;

        for (long k = 0; k < N; k++) {
            assertNull(m.put(k, k * 3));
            oracle.put(k, k * 3);
        }
        for (long k = 0; k < N; k += 2) {
            assertEquals(Long.valueOf(k * 3), m.put(k, -k));
            oracle.put(k, -k);
        }
        assertNull(m.get((long) N)); // absent key shares the bucket, scan must miss
        assertMatches(m, oracle);
    }

    // ---------- targeted lazy-split coverage (identity hasher, 6/6/7) ----------

    /** Identity Hasher64 {@code k << 16} makes the top-48 slice equal the key: bits 42..47
     *  select the segment, bits 0..41 the dir index. dirShift=6, levels=7 → level L consumes
     *  index bits [6L, 6L+6). The ladder collides at the top and diverges at each of the 7
     *  levels, then adds cross-segment keys via bits 42..47. */
    private static long[] splitLadder() {
        return new long[]{
                0,                              // seg 0, root slot 0, terminal at top
                1,                              // diverges at level 0
                1L << 6,                        // level 1
                1L << 12,                       // level 2
                1L << 18,                       // level 3
                1L << 24,                       // level 4
                1L << 30,                       // level 5  (> 2^29)
                1L << 36,                       // level 6 (top of index) — deep, > 2^32
                (5L << 36) | (3L << 30),        // seg 0, top slot 5, own sub-branch
                (5L << 36) | (9L << 6),         // shares top slot 5, diverges deeper
                (5L << 36) | (9L << 6) | 1,     // shares more, diverges at level 0
                (1L << 42) - 1,                 // seg 0, max index (all 42 bits) — deepest
                (3L << 42) | 5,                 // seg 3, shallow index
                (3L << 42) | (1L << 41),        // seg 3, deep index bit 41 (> 2^32)
                (63L << 42),                    // seg 63, index 0
                (63L << 42) | ((1L << 42) - 1), // seg 63, max index
        };
    }

    private static Hasher64<Long> identityHasher() {
        return (k, seed) -> k << 16; // top-48 slice == k
    }

    @Test
    public void deepSplitTargetedIndices() {
        HTreeMap48<Long, Long> m = HTreeMap48.create(store, Serializers.LONG, Serializers.LONG,
                6, 6, 7, 0L, identityHasher());
        long[] indices = splitLadder();

        HashMap<Long, Long> oracle = new HashMap<>();
        for (long idx : indices) {
            assertNull("fresh insert " + idx, m.put(idx, idx + 1000));
            oracle.put(idx, idx + 1000);
            // after every split step ALL previously inserted keys survive
            for (Map.Entry<Long, Long> e : oracle.entrySet()) {
                assertEquals("get " + e.getKey() + " after inserting " + idx,
                        e.getValue(), m.get(e.getKey()));
            }
        }
        // absent indices that share slot prefixes with present ones must miss
        assertNull(m.get(2L));
        assertNull(m.get(2L << 6));
        assertNull(m.get((5L << 36) | (9L << 6) | 2));
        assertMatches(m, oracle);

        // overwrite through the split tree
        for (long idx : indices) {
            assertEquals(Long.valueOf(idx + 1000), m.put(idx, idx + 2000));
            oracle.put(idx, idx + 2000);
        }
        assertMatches(m, oracle);
    }

    /** The same ladder removed one at a time; collapse must keep every survivor
     *  resolvable, then the tree re-splits cleanly after full drain. */
    @Test
    public void deepSplitTargetedRemoves() {
        HTreeMap48<Long, Long> m = HTreeMap48.create(store, Serializers.LONG, Serializers.LONG,
                6, 6, 7, 0L, identityHasher());
        long[] indices = splitLadder();

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
        assertEquals("targeted removes leaked records", DEFAULT_LIVE, liveRecidCount());

        // collapse-then-regrow: the ladder re-splits after full removal
        for (long idx : indices) assertNull("reinsert " + idx, m.put(idx, idx + 2000));
        for (long idx : indices) assertEquals(Long.valueOf(idx + 2000), m.get(idx));
    }

    /** Deep-index coverage: indices that only exist in the 48-bit budget (bits 33..41,
     *  above the 32-bit ceiling) — the whole point of the class. Each is placed exactly
     *  via the identity hasher and must resolve and survive removal. */
    @Test
    public void deepIndexAbove32Bits() {
        HTreeMap48<Long, Long> m = HTreeMap48.create(store, Serializers.LONG, Serializers.LONG,
                6, 6, 7, 0L, identityHasher());
        HashMap<Long, Long> oracle = new HashMap<>();
        // one key per index bit 33..41, all in segment 0 (bits 42..47 = 0)
        for (int bit = 33; bit <= 41; bit++) {
            long idx = 1L << bit;
            assertNull("fresh bit " + bit, m.put(idx, idx));
            oracle.put(idx, idx);
        }
        // combinations of high bits with low structure, still segment 0
        long[] extra = {(1L << 41) | (1L << 33) | 7, (1L << 40) | (1L << 34), (1L << 41) - 1};
        for (long idx : extra) {
            assertNull("fresh extra " + idx, m.put(idx, idx));
            oracle.put(idx, idx);
        }
        assertMatches(m, oracle);
        // a deep index NOT inserted (sharing high bits) must miss
        assertNull(m.get((1L << 41) | (1L << 35)));
        // drain: all deep entries free cleanly
        for (Long key : new java.util.ArrayList<>(oracle.keySet())) {
            assertEquals(oracle.get(key), m.remove(key));
        }
        assertTrue(m.isEmpty());
        assertEquals("deep-index drain leaked records", DEFAULT_LIVE, liveRecidCount());
    }

    // ---------- full-entropy 64-bit hashers ----------

    /** {@link Hashers#LONG64} mixes all 64 value bits: bulk fuzz vs HashMap oracle. */
    @Test
    public void longFullEntropyFuzz() {
        HTreeMap48<Long, Long> m = HTreeMap48.create(store, Serializers.LONG, Serializers.LONG,
                Hashers.LONG64);
        HashMap<Long, Long> oracle = new HashMap<>();
        Random rnd = new Random(4864);
        for (int i = 0; i < 40_000; i++) {
            long k = rnd.nextInt(10_000);
            int roll = rnd.nextInt(100);
            if (roll < 50) {
                long v = rnd.nextLong();
                assertEquals(oracle.put(k, v), m.put(k, v));
            } else if (roll < 75) {
                assertEquals(oracle.remove(k), m.remove(k));
            } else {
                assertEquals(oracle.get(k), m.get(k));
            }
        }
        assertMatches(m, oracle);
    }

    /** {@link Hashers#STRING64} over string keys: bulk fuzz vs HashMap oracle. */
    @Test
    public void stringFullEntropyFuzz() {
        HTreeMap48<String, String> m = HTreeMap48.create(store, Serializers.STRING, Serializers.STRING,
                Hashers.STRING64);
        HashMap<String, String> oracle = new HashMap<>();
        Random rnd = new Random(97);
        for (int i = 0; i < 10_000; i++) {
            String k = randomString(rnd, 1 + rnd.nextInt(12));
            String v = randomString(rnd, 1 + rnd.nextInt(20));
            assertEquals(oracle.put(k, v), m.put(k, v));
        }
        for (int i = 0; i < 1000; i++) assertNull(m.get("ABSENT-" + i));
        assertMatches(m, oracle);
    }

    /** {@link Hashers#BYTE_ARRAY64} depends on byte[] CONTENT, not identity: fresh
     *  equal-content probes hit; distinct content coexists. */
    @Test
    public void byteArrayFullEntropyKeys() {
        HTreeMap48<byte[], byte[]> m = HTreeMap48.create(store,
                Serializers.BYTE_ARRAY, Serializers.BYTE_ARRAY, Hashers.BYTE_ARRAY64);

        for (int i = 0; i < 5_000; i++) {
            assertNull("fresh " + i, m.put(("key-" + i).getBytes(), ("val-" + i).getBytes()));
        }
        assertEquals(5_000L, m.sizeLong());
        // read back with FRESH equal-content byte[] instances: content hash + equals must hit
        for (int i = 0; i < 5_000; i++) {
            assertTrue(Arrays.equals(("val-" + i).getBytes(), m.get(("key-" + i).getBytes())));
        }
        // overwrite via an independent equal-content key instance; old value returned
        byte[] old = m.put("key-42".getBytes(), "REPLACED".getBytes());
        assertTrue(Arrays.equals("val-42".getBytes(), old));
        assertTrue(Arrays.equals("REPLACED".getBytes(), m.get("key-42".getBytes())));
        assertEquals(5_000L, m.sizeLong());
        assertNull(m.get("absent".getBytes()));
    }

    // ---------- second handle via open() on the same store ----------

    @Test
    public void openSecondHandleSameStore() {
        HTreeMap48<Long, Long> m = longMap();
        HashMap<Long, Long> oracle = new HashMap<>();
        for (long k = 0; k < 1_000; k++) {
            m.put(k, k * 11);
            oracle.put(k, k * 11);
        }
        HTreeMap48<Long, Long> m2 = HTreeMap48.open(store, m.headerRecid(),
                Serializers.LONG, Serializers.LONG);
        assertMatches(m2, oracle);
    }

    // ---------- null rejection ----------

    @Test
    public void putNullKeyRejected() {
        HTreeMap48<Long, Long> m = longMap();
        try {
            m.put(null, 1L);
            fail("expected NullPointerException");
        } catch (NullPointerException expected) { /* ok */ }
    }

    @Test
    public void putNullValueRejected() {
        HTreeMap48<Long, Long> m = longMap();
        try {
            m.put(1L, null);
            fail("expected NullPointerException");
        } catch (NullPointerException expected) { /* ok */ }
    }

    @Test
    public void getNullKeyRejected() {
        HTreeMap48<Long, Long> m = longMap();
        try {
            m.get(null);
            fail("expected NullPointerException");
        } catch (NullPointerException expected) { /* ok */ }
    }

    // ---------- iterator snapshot allows mutation from callbacks ----------

    @Test
    public void forEachCallbackMayMutate() {
        HTreeMap48<Long, Long> m = longMap();
        for (long k = 0; k < 100; k++) m.put(k, k);
        Set<Long> seen = new HashSet<>();
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

    // ---------- new key/value types ----------

    @Test
    public void integerKeysAndValues() {
        HTreeMap48<Integer, Integer> m =
                HTreeMap48.create(store, Serializers.INTEGER, Serializers.INTEGER);
        HashMap<Integer, Integer> oracle = new HashMap<>();

        for (int k : new int[]{0, 1, -1, Integer.MIN_VALUE, Integer.MAX_VALUE, -12345, 12345}) {
            assertNull(m.put(k, ~k));
            oracle.put(k, ~k);
        }
        Random rnd = new Random(101);
        for (int i = 0; i < 10_000; i++) {
            int k = rnd.nextInt() >> (rnd.nextInt(20));
            int v = rnd.nextInt();
            assertEquals(oracle.put(k, v), m.put(k, v));
        }
        assertMatches(m, oracle);
    }

    /** Arbitrary boxed key/value type (Character) with a caller-supplied serializer. */
    @Test
    public void characterKeysAndValues() {
        Serializer<Character> charSer = new Serializer<>() {
            @Override public void serialize(DataOutput2 out, Character v) { out.writeInt(v); }
            @Override public Character deserialize(DataInput2 in, int size) {
                return (char) in.readInt();
            }
        };
        HTreeMap48<Character, Character> m = HTreeMap48.create(store, charSer, charSer);
        HashMap<Character, Character> oracle = new HashMap<>();
        for (char c = 'À'; c < 'Ā'; c++) {
            assertNull(m.put(c, c));
            oracle.put(c, c);
        }
        assertEquals(Character.valueOf('À'), m.get('À'));
        assertNull(m.get('A'));
        assertMatches(m, oracle);
    }

    /** Large byte[] values stored and read back intact (large-record path). */
    @Test
    public void largeByteArrayValues() {
        HTreeMap48<Long, byte[]> m =
                HTreeMap48.create(store, Serializers.LONG, Serializers.BYTE_ARRAY);
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

    @Test
    public void emptyStringKeyAndValue() {
        HTreeMap48<String, String> m = stringMap();
        assertNull(m.put("", ""));
        assertEquals("", m.get(""));
        assertNull(m.put("k", ""));
        assertEquals("", m.get("k"));
        assertEquals("", m.put("", "v"));
        assertEquals("v", m.get(""));
        assertEquals(2L, m.sizeLong());
    }

    @Test
    public void boundaryLongKeys() {
        HTreeMap48<Long, Long> m = longMap();
        HashMap<Long, Long> oracle = new HashMap<>();
        for (long k : new long[]{0L, 1L, -1L, Long.MIN_VALUE, Long.MAX_VALUE,
                Integer.MIN_VALUE, Integer.MAX_VALUE, -1_000_000L, 1_000_000L}) {
            assertNull("fresh " + k, m.put(k, k));
            oracle.put(k, k);
        }
        assertMatches(m, oracle);
    }

    @Test
    public void overwriteReturnsOldValueRepeatedly() {
        HTreeMap48<Long, Long> m = longMap();
        assertNull(m.put(7L, 0L));
        for (long v = 1; v <= 500; v++) {
            assertEquals("overwrite " + v, Long.valueOf(v - 1), m.put(7L, v));
            assertEquals(1L, m.sizeLong());
        }
        assertEquals(Long.valueOf(500L), m.get(7L));
    }

    // ---------- real hash collisions through the DEFAULT hasher ----------

    /** Equal-hashCode strings collide under the default objectHasher64 (a pure function
     *  of hashCode) even at the random seed. Bucket linear-scan must disambiguate. */
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

        HTreeMap48<String, String> m = stringMap();
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
        assertEquals("v-" + mid, m.put(mid, "NEW"));
        oracle.put(mid, "NEW");
        assertEquals(family.size(), m.sizeLong());
        assertMatches(m, oracle);

        assertNull(m.put(omitted, "v-" + omitted));
        oracle.put(omitted, "v-" + omitted);
        assertMatches(m, oracle);
    }

    // ---------- iteration completeness across distinct segments ----------

    /** Identity hasher steers keys into a few DISTINCT segments via bits 42..47;
     *  entryIterator must visit every entry with none dropped or duplicated. */
    @Test
    public void sparseSegmentsIterationComplete() {
        HTreeMap48<Long, Long> m = HTreeMap48.create(store, Serializers.LONG, Serializers.LONG,
                6, 6, 7, 0L, identityHasher());
        HashMap<Long, Long> oracle = new HashMap<>();

        int[] segments = {1, 7, 10, 42, 63};
        for (int seg : segments) {
            for (int j = 0; j < 200; j++) {
                long key = ((long) seg << 42) | j;
                assertNull("fresh seg=" + seg + " j=" + j, m.put(key, key * 3));
                oracle.put(key, key * 3);
            }
        }
        assertEquals(segments.length * 200L, m.sizeLong());
        assertMatches(m, oracle);
    }

    // ---------- Tier-1 remove / CAS / clear ----------

    @Test
    public void removeLifecycle() {
        HTreeMap48<Long, Long> m = longMap();
        assertNull(m.remove(1L));
        assertNull(m.put(1L, 100L));
        assertEquals(Long.valueOf(100L), m.remove(1L));
        assertNull(m.get(1L));
        assertFalse(m.containsKey(1L));
        assertNull(m.remove(1L));
        assertEquals(0L, m.sizeLong());
        assertTrue(m.isEmpty());
        assertFalse(m.entryIterator().hasNext());
    }

    /** Workhorse: seeded random put/remove/get/containsKey/putIfAbsent/replace vs oracle,
     *  then a full drain — the store must hold ONLY header + segment roots afterwards. */
    @Test
    public void fuzzWithRemoves() {
        final long seed = 0xC0FFEE5EEDL;
        System.out.println("[HTreeMap48TCK.fuzzWithRemoves] store=" + store.getClass().getSimpleName()
                + " seed=" + seed);
        Random rnd = new Random(seed);
        final int KEYSPACE = 20_000;
        final int OPS = 100_000;

        HTreeMap48<Long, Long> m = longMap();
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

        for (Long key : new java.util.ArrayList<>(oracle.keySet())) {
            assertEquals(oracle.remove(key), m.remove(key));
        }
        assertTrue(m.isEmpty());
        assertEquals(0L, m.sizeLong());
        assertEquals("remove leaked records", DEFAULT_LIVE, liveRecidCount());
    }

    @Test
    public void removeHeavyThenReinsert() {
        HTreeMap48<Long, Long> m = longMap();
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
        for (long k = 0; k < N; k++) {
            if (k % 3 != 0) assertNull("reinsert should be fresh " + k, m.put(k, k + 1_000_000));
        }
        for (long k = 0; k < N; k++) {
            Long expected = (k % 3 != 0) ? k + 1_000_000 : k;
            assertEquals("final get " + k, expected, m.get(k));
        }
        assertEquals((long) N, m.sizeLong());
    }

    /** Constant hash piles every key into ONE bucket: middle/first/last removes leave the
     *  rest intact; a full drain collapses leaf + dir path back to the empty gate. */
    @Test
    public void collisionBucketRemove() {
        Hasher64<Long> constantHash = (k, seed) -> 0L;
        HTreeMap48<Long, Long> m = HTreeMap48.create(store, Serializers.LONG, Serializers.LONG,
                6, 6, 7, 0L, constantHash);
        final int N = 100;
        for (long k = 0; k < N; k++) assertNull(m.put(k, k * 3));

        assertEquals(Long.valueOf(50L * 3), m.remove(50L));
        assertNull(m.get(50L));
        assertFalse(m.containsKey(50L));
        for (long k = 0; k < N; k++) {
            if (k == 50) continue;
            assertEquals("survivor " + k, Long.valueOf(k * 3), m.get(k));
        }
        assertEquals(N - 1L, m.sizeLong());

        assertEquals(Long.valueOf(0L), m.remove(0L));
        assertEquals(Long.valueOf((N - 1) * 3L), m.remove((long) (N - 1)));
        assertEquals(N - 3L, m.sizeLong());

        assertNull(m.remove((long) N));
        assertEquals(N - 3L, m.sizeLong());

        for (long k = 1; k < N - 1; k++) {
            if (k == 50) continue;
            assertEquals(Long.valueOf(k * 3), m.remove(k));
        }
        assertTrue(m.isEmpty());
        assertEquals(0L, m.sizeLong());
        assertEquals("bucket drain leaked records", DEFAULT_LIVE, liveRecidCount());

        assertNull(m.put(50L, 1L));
        assertEquals(Long.valueOf(1L), m.get(50L));
        assertEquals(1L, m.sizeLong());
    }

    @Test
    public void casOperations() {
        HTreeMap48<Long, Long> m = longMap();

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
        assertTrue(m.isEmpty());
    }

    @Test
    public void casOperationsInCollisionBucket() {
        Hasher64<Long> constantHash = (k, seed) -> 0L;
        HTreeMap48<Long, Long> m = HTreeMap48.create(store, Serializers.LONG, Serializers.LONG,
                6, 6, 7, 0L, constantHash);
        HashMap<Long, Long> oracle = new HashMap<>();
        for (long k = 0; k < 50; k++) {
            m.put(k, k * 3);
            oracle.put(k, k * 3);
        }
        assertEquals(oracle.putIfAbsent(25L, -1L), m.putIfAbsent(25L, -1L));
        assertEquals(oracle.putIfAbsent(100L, -2L), m.putIfAbsent(100L, -2L));
        assertEquals(oracle.replace(30L, 999L), m.replace(30L, 999L));
        assertEquals(oracle.replace(30L, 999L, 1000L), m.replace(30L, 999L, 1000L));
        assertEquals(oracle.replace(31L, 555L, 666L), m.replace(31L, 555L, 666L));
        assertEquals(oracle.remove(40L, 40L * 3), m.remove(40L, 40L * 3));
        assertEquals(oracle.remove(41L, 555L), m.remove(41L, 555L));
        assertMatches(m, oracle);
    }

    @Test
    public void tier1NullRejected() {
        HTreeMap48<Long, Long> m = longMap();
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

    @Test
    public void clearThenReuse() {
        HTreeMap48<Long, Long> m = longMap();
        assertTrue(m.isEmpty());
        m.clear();
        assertTrue(m.isEmpty());

        final int N = 10_000;
        for (long k = 0; k < N; k++) m.put(k, k * 7);
        assertFalse(m.isEmpty());
        assertEquals((long) N, m.sizeLong());

        m.clear();
        assertTrue(m.isEmpty());
        assertEquals(0L, m.sizeLong());
        assertEquals("clear leaked records", DEFAULT_LIVE, liveRecidCount());
        assertFalse(m.entryIterator().hasNext());
        for (long k = 0; k < N; k += 97) {
            assertNull(m.get(k));
            assertFalse(m.containsKey(k));
        }

        HashMap<Long, Long> oracle = new HashMap<>();
        for (long k = 0; k < 2_000; k++) {
            assertNull("fresh after clear " + k, m.put(k, k + 5));
            oracle.put(k, k + 5);
        }
        assertMatches(m, oracle);

        HTreeMap48<Long, Long> m2 = HTreeMap48.open(store, m.headerRecid(),
                Serializers.LONG, Serializers.LONG);
        assertMatches(m2, oracle);
    }

    /** String keys with variable-length values: removes exercise the LOCKED leaf path
     *  (leafReadOptimistic off) plus valueSer.equals-based CAS on strings. */
    @Test
    public void stringRemoveAndCas() {
        HTreeMap48<String, String> m = stringMap();
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

    // =====================================================================
    // java.util.Map / ConcurrentMap interface conformance (size/entrySet/keySet/
    // values/containsValue/putAll, Map.equals/hashCode, the snapshot-view
    // iterators, and the ConcurrentMap CAS + default methods reached purely
    // through the interface). Runs across every dialect.
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
        HTreeMap48<Long, Long> m = longMap();
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
        HTreeMap48<Long, Long> m = longMap();
        HashMap<Long, Long> oracle = new HashMap<>();
        for (long k = 0; k < 1_500; k++) { m.put(k, k * 7 + 1); oracle.put(k, k * 7 + 1); }
        assertViewsMatchOracle(m, oracle);
    }

    @Test
    public void entrySetContainsAndRemove() {
        HTreeMap48<Long, Long> m = longMap();
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
    }

    /** The entrySet/keySet/values views are mutable: iterator.remove() and the derived
     *  Collection.remove/removeAll all delete from the live map. */
    @Test
    public void viewIteratorsSupportRemoval() {
        HTreeMap48<Long, Long> m = longMap();
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
    }

    /** Blind putOnly/removeOnly: insert/overwrite/delete without returning the old value. */
    @Test
    public void blindPutAndRemove() {
        HTreeMap48<Long, Long> m = longMap();
        m.putOnly(1L, 10L);
        assertEquals(Long.valueOf(10L), m.get(1L));
        m.putOnly(1L, 20L);                          // overwrite, nothing returned
        assertEquals(Long.valueOf(20L), m.get(1L));
        assertEquals(1L, m.sizeLong());
        assertTrue(m.removeOnly(1L));
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
        HTreeMap48<Long, Long> m = longMap();
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
        HTreeMap48<Long, Long> empty = HTreeMap48.create(store, Serializers.LONG, Serializers.LONG);
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
        assertEquals(oracle, map);
        assertEquals(map, oracle);
        HashMap<Long, Long> viaEntrySet = new HashMap<>();
        for (Map.Entry<Long, Long> e : map.entrySet()) viaEntrySet.put(e.getKey(), e.getValue());
        assertEquals(oracle, viaEntrySet);
        map.clear();
        assertTrue(map.isEmpty());
    }
}
