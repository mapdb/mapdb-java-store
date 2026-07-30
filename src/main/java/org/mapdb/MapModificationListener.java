package org.mapdb;

/**
 * Runtime map mutation callback, compatible with MapDB 3. {@code triggered} is
 * true for automatic expiry/eviction and false for user-requested mutations.
 */
@FunctionalInterface
public interface MapModificationListener<K, V> {
    void modify(K key, V oldValue, V newValue, boolean triggered);
}
