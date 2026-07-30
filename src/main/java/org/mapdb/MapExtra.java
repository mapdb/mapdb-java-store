package org.mapdb;

import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

import org.mapdb.ser.Serializer;

/** MapDB 3-compatible extensions shared by persistent concurrent maps. */
public interface MapExtra<K, V> extends ConcurrentMap<K, V>, ModificationAwareMap<K, V> {
    long sizeLong();
    boolean isClosed();
    Serializer<K> keySerializer();
    Serializer<V> valueSerializer();

    default boolean putIfAbsentBoolean(K key, V value) { return putIfAbsent(key, value) == null; }
    default void forEachKey(Consumer<? super K> consumer) { keySet().forEach(consumer); }
    default void forEachValue(Consumer<? super V> consumer) { values().forEach(consumer); }
}
