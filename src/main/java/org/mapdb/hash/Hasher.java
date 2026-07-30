package org.mapdb.hash;

/**
 * 32-bit key hasher for the hash-collection family ({@code org.mapdb.htree}).
 * The single point where a key becomes hash bits — external to the collections so
 * callers can supply content hashes (byte[]-like keys) or test steering without the
 * collection embedding any mixing policy.
 *
 * Contract (the collections rely on all three):
 *  - DETERMINISTIC and stable across JVM runs for a given (key, seed): the seed is
 *    persisted and the same hasher must be supplied on every open of a map.
 *  - CONSISTENT with the key serializer's equals: keys equal per
 *    {@code keySer.equals} must hash identically (this is why {@code byte[]} keys
 *    need a content hasher — identity hashCode breaks it).
 *  - FULLY MIXED: every result bit uniformly distributed — the HIGH bits select the
 *    segment, the low bits the dir index. Incorporate the seed. Unless you know what
 *    you are doing, build on {@link Hashers#mixing} / {@link Hashers#objectHasher}
 *    rather than hand-rolling.
 */
@FunctionalInterface
public interface Hasher<K> {

    int hash(K key, int seed);
}
