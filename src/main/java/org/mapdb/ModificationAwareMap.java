package org.mapdb;

import java.util.Map;

/** A map that supports runtime-only synchronous modification listeners. */
public interface ModificationAwareMap<K, V> extends Map<K, V> {
    void modificationListenerAdd(MapModificationListener<K, V> listener);
    void modificationListenerRemove(MapModificationListener<K, V> listener);
}
