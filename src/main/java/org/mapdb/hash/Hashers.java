package org.mapdb.hash;

import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

/**
 * Built-in {@link Hasher} / {@link Hasher64} implementations and the mixing
 * finalizers they share. All hashers here incorporate the seed BEFORE the final
 * mix, so equal content with different seeds lands in unrelated slots (seed = the
 * per-map salt persisted in each collection's header).
 *
 * None of these are standardized digests — they are INTERNAL placement hashes. A
 * persisted map must be opened with the same hasher it was created with, forever;
 * changing an implementation here is a wire-format break for existing files.
 */
public final class Hashers {

    private Hashers() {}

    // ================= finalizers =================

    /** murmur3 fmix32: full avalanche over 32 bits (spec-htreemap §0.3). */
    public static int fmix32(int h) {
        h ^= h >>> 16;
        h *= 0x85ebca6b;
        h ^= h >>> 13;
        h *= 0xc2b2ae35;
        h ^= h >>> 16;
        return h;
    }

    /** murmur3 fmix64: full avalanche over 64 bits. */
    public static long fmix64(long h) {
        h ^= h >>> 33;
        h *= 0xff51afd7ed558ccdL;
        h ^= h >>> 33;
        h *= 0xc4ceb9fe1a85ec53L;
        h ^= h >>> 33;
        return h;
    }

    // ================= 32-bit hashers =================

    private static final Hasher<Object> OBJECT = (key, seed) -> fmix32(key.hashCode() ^ seed);

    /** The default: {@code fmix32(key.hashCode() ^ seed)}. Requires keys whose
     *  hashCode/equals agree with the key serializer's equals (Long, String, boxed
     *  primitives — NOT byte[], §0.2). */
    @SuppressWarnings("unchecked")
    public static <K> Hasher<K> objectHasher() {
        return (Hasher<K>) OBJECT;
    }

    /** Wrap a content hash (e.g. {@code Arrays::hashCode} for byte[] keys) with the
     *  seed-xor + fmix32 the collections need — supply the content function, get the
     *  mixing for free. */
    public static <K> Hasher<K> mixing(ToIntFunction<? super K> contentHash) {
        return (key, seed) -> fmix32(contentHash.applyAsInt(key) ^ seed);
    }

    // ================= 64-bit hashers =================

    private static final Hasher64<Object> OBJECT64 = (key, seed) -> fmix64(key.hashCode() ^ seed);

    /** 64-bit default over {@code Object.hashCode()} — at most 32 bits of ENTROPY
     *  (see {@link Hasher64} caveat); prefer a genuinely 64-bit source at scale. */
    @SuppressWarnings("unchecked")
    public static <K> Hasher64<K> objectHasher64() {
        return (Hasher64<K>) OBJECT64;
    }

    /** Wrap a 64-bit content hash with seed-xor + fmix64. */
    public static <K> Hasher64<K> mixing64(ToLongFunction<? super K> contentHash) {
        return (key, seed) -> fmix64(contentHash.applyAsLong(key) ^ seed);
    }

    /** Full-entropy Long keys: mixes all 64 value bits (not the 32-bit hashCode). */
    public static final Hasher64<Long> LONG64 = (key, seed) -> fmix64(key ^ seed);

    /** 64-bit String hash: FNV-1a-64 over UTF-16 code units, then seeded fmix64.
     *  Well-formed vs ill-formed strings are NOT conflated (unlike the UTF-8
     *  serializer's domain note — hashing sees raw code units). */
    public static final Hasher64<String> STRING64 = (key, seed) -> {
        long h = 0xcbf29ce484222325L;
        for (int i = 0; i < key.length(); i++) {
            h = (h ^ key.charAt(i)) * 0x100000001b3L;
        }
        return fmix64(h ^ seed);
    };

    /** 64-bit content hash for byte[] keys: FNV-1a-64 over the bytes, seeded fmix64. */
    public static final Hasher64<byte[]> BYTE_ARRAY64 = (key, seed) -> {
        long h = 0xcbf29ce484222325L;
        for (byte b : key) {
            h = (h ^ (b & 0xff)) * 0x100000001b3L;
        }
        return fmix64(h ^ seed);
    };
}
