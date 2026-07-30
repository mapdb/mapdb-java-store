package org.mapdb.hash;

/**
 * 64-bit key hasher — for wide-budget hash collections ({@code HTreeMap48}) whose
 * index space outgrows a 32-bit hash. Same contract as {@link Hasher}
 * (deterministic per (key, seed), consistent with serializer equals, fully mixed —
 * the TOP bits are consumed first).
 *
 * ENTROPY CAVEAT: {@link Hashers#objectHasher64()} derives from
 * {@code Object.hashCode()} and therefore carries at most 32 bits of entropy —
 * fine functionally, but collision behavior is then no better than a 32-bit map.
 * Maps sized beyond ~10^9 keys should use a genuinely 64-bit source:
 * {@link Hashers#LONG64}, {@link Hashers#STRING64}, {@link Hashers#BYTE_ARRAY64}
 * or a custom {@link Hashers#mixing64}.
 */
@FunctionalInterface
public interface Hasher64<K> {

    long hash(K key, long seed);
}
