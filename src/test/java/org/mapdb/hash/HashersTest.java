package org.mapdb.hash;

import org.junit.Test;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for the {@code org.mapdb.hash} package: the fmix finalizers, the default
 * object hashers, the full-entropy 64-bit hashers, and the mixing wrappers. Statistical
 * checks are seeded and kept deliberately loose (these are placement hashes, not digests —
 * no collision-rate claims beyond "no collisions in this fixed sample").
 */
public class HashersTest {

    // ---------- default object hashers reproduce the documented formula ----------

    @Test
    public void objectHasherIsFmix32OfHashCodeXorSeed() {
        Hasher<String> h = Hashers.objectHasher();
        int[] seeds = {0, 1, -1, 42, -42, Integer.MIN_VALUE, Integer.MAX_VALUE, 0x5EED};
        String[] keys = {"", "a", "hello", "Aa", "BB", "ÀĀ", "the quick brown fox"};
        for (String k : keys) {
            for (int seed : seeds) {
                assertEquals("objectHasher(" + k + "," + seed + ")",
                        Hashers.fmix32(k.hashCode() ^ seed), h.hash(k, seed));
            }
        }
    }

    @Test
    public void objectHasher64IsFmix64OfHashCodeXorSeed() {
        Hasher64<String> h = Hashers.objectHasher64();
        long[] seeds = {0, 1, -1, 42, -42, Long.MIN_VALUE, Long.MAX_VALUE, 0x5EEDC0FFEEL};
        String[] keys = {"", "a", "hello", "Aa", "BB", "ÀĀ", "the quick brown fox"};
        for (String k : keys) {
            for (long seed : seeds) {
                assertEquals("objectHasher64(" + k + "," + seed + ")",
                        Hashers.fmix64(k.hashCode() ^ seed), h.hash(k, seed));
            }
        }
    }

    // ---------- fmix finalizers: identity at 0, injective sample, avalanche ----------

    @Test
    public void fmixZeroMapsToZero() {
        assertEquals(0, Hashers.fmix32(0));
        assertEquals(0L, Hashers.fmix64(0L));
    }

    /** fmix is a bijection; over a large sample distinct inputs must give distinct outputs. */
    @Test
    public void fmixInjectiveOverSample() {
        Random rnd = new Random(1);
        Set<Integer> out32 = new HashSet<>();
        Set<Long> out64 = new HashSet<>();
        final int N = 200_000;
        for (int i = 0; i < N; i++) {
            assertTrue("fmix32 collision", out32.add(Hashers.fmix32(i)));       // sequential inputs
            long x = rnd.nextLong();
            out64.add(Hashers.fmix64(x));
        }
        assertEquals(N, out32.size());
        // random longs: overwhelmingly likely all distinct; allow the birthday-paradox slack of 0
        assertEquals("fmix64 collision in sample", N, out64.size());
    }

    /** Avalanche smoke: flipping ONE input bit flips ~half the output bits on average. */
    @Test
    public void fmixAvalancheSmoke() {
        Random rnd = new Random(2);
        final int TRIALS = 5_000;

        long sum32 = 0;
        for (int t = 0; t < TRIALS; t++) {
            int x = rnd.nextInt();
            int bit = rnd.nextInt(32);
            int y = x ^ (1 << bit);
            sum32 += Integer.bitCount(Hashers.fmix32(x) ^ Hashers.fmix32(y));
        }
        double mean32 = sum32 / (double) TRIALS; // ideal 16
        assertTrue("fmix32 avalanche mean " + mean32, mean32 >= 12 && mean32 <= 20);

        long sum64 = 0;
        for (int t = 0; t < TRIALS; t++) {
            long x = rnd.nextLong();
            int bit = rnd.nextInt(64);
            long y = x ^ (1L << bit);
            sum64 += Long.bitCount(Hashers.fmix64(x) ^ Hashers.fmix64(y));
        }
        double mean64 = sum64 / (double) TRIALS; // ideal 32
        assertTrue("fmix64 avalanche mean " + mean64, mean64 >= 24 && mean64 <= 40);
    }

    // ---------- full-entropy 64-bit hashers ----------

    @Test
    public void long64DeterministicAndSeedSensitive() {
        Random rnd = new Random(3);
        for (int i = 0; i < 10_000; i++) {
            long k = rnd.nextLong();
            long s = rnd.nextLong();
            assertEquals("deterministic", Hashers.LONG64.hash(k, s), Hashers.LONG64.hash(k, s));
            long s2 = s ^ 1; // a different seed reroutes the same key
            assertNotEquals("seed-sensitive key=" + k,
                    Hashers.LONG64.hash(k, s), Hashers.LONG64.hash(k, s2));
        }
    }

    /** LONG64 uses all 64 value bits, so keys that share a 32-bit hashCode still differ. */
    @Test
    public void long64UsesFullWidthNotHashCode() {
        long a = 0x0000_0001_0000_0000L; // hashCode() folds high^low => same as...
        long b = 0x0000_0000_0000_0001L; // ...this: (int)(v ^ v>>>32) == 1 for both
        assertEquals("precondition: equal 32-bit hashCode",
                Long.hashCode(a), Long.hashCode(b));
        assertNotEquals("LONG64 must separate them", Hashers.LONG64.hash(a, 0), Hashers.LONG64.hash(b, 0));
    }

    @Test
    public void string64ContentEqualityAndSeedSensitivity() {
        // deterministic + agrees on equal content (fresh instances)
        assertEquals(Hashers.STRING64.hash("hello", 7), Hashers.STRING64.hash(new String("hello"), 7));
        // seed-sensitive
        assertNotEquals(Hashers.STRING64.hash("hello", 7), Hashers.STRING64.hash("hello", 8));
        // different content differs; no collisions across a 10k fixed-seed sample
        Set<Long> seen = new HashSet<>();
        for (int i = 0; i < 10_000; i++) {
            long h = Hashers.STRING64.hash("key-" + i, 0xABCDEFL);
            assertTrue("STRING64 collision at i=" + i, seen.add(h));
        }
    }

    @Test
    public void byteArray64DependsOnContentNotIdentity() {
        byte[] a = {1, 2, 3, 4, 5};
        byte[] b = {1, 2, 3, 4, 5}; // equal content, distinct instance
        byte[] c = {1, 2, 3, 4, 6}; // one byte differs
        assertEquals("equal content => equal hash",
                Hashers.BYTE_ARRAY64.hash(a, 9), Hashers.BYTE_ARRAY64.hash(b, 9));
        assertNotEquals("different content => different hash",
                Hashers.BYTE_ARRAY64.hash(a, 9), Hashers.BYTE_ARRAY64.hash(c, 9));
        assertNotEquals("seed-sensitive",
                Hashers.BYTE_ARRAY64.hash(a, 9), Hashers.BYTE_ARRAY64.hash(a, 10));
        // no collisions across a 10k fixed-seed sample of distinct byte content
        Set<Long> seen = new HashSet<>();
        for (int i = 0; i < 10_000; i++) {
            long h = Hashers.BYTE_ARRAY64.hash(("bk-" + i).getBytes(), 0x1234L);
            assertTrue("BYTE_ARRAY64 collision at i=" + i, seen.add(h));
        }
    }

    // ---------- mixing wrappers ----------

    @Test
    public void mixing32WrapsContentHashWithSeedXorFmix32() {
        ToIntFunction<int[]> content = arr -> arr[0] * 31 + arr[1];
        Hasher<int[]> h = Hashers.mixing(content);
        Random rnd = new Random(4);
        for (int i = 0; i < 5_000; i++) {
            int[] k = {rnd.nextInt(), rnd.nextInt()};
            int seed = rnd.nextInt();
            assertEquals(Hashers.fmix32(content.applyAsInt(k) ^ seed), h.hash(k, seed));
        }
    }

    @Test
    public void mixing64WrapsContentHashWithSeedXorFmix64() {
        ToLongFunction<int[]> content = arr -> ((long) arr[0] << 32) | (arr[1] & 0xffffffffL);
        Hasher64<int[]> h = Hashers.mixing64(content);
        Random rnd = new Random(5);
        for (int i = 0; i < 5_000; i++) {
            int[] k = {rnd.nextInt(), rnd.nextInt()};
            long seed = rnd.nextLong();
            assertEquals(Hashers.fmix64(content.applyAsLong(k) ^ seed), h.hash(k, seed));
        }
    }
}
