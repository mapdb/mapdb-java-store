package org.mapdb.db;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/** ConcurrentMap view whose size is maintained in a persistent Atomic.Long. */
final class CounterConcurrentMap<K, V> extends AbstractMap<K, V>
        implements org.mapdb.MapExtra<K, V> {
    private final ConcurrentMap<K, V> delegate;
    private final Atomic.Long counter;

    CounterConcurrentMap(ConcurrentMap<K, V> delegate, Atomic.Long counter) {
        this.delegate = delegate;
        this.counter = counter;
    }

    ConcurrentMap<K, V> delegate() { return delegate; }

    @SuppressWarnings("unchecked")
    @Override public void modificationListenerAdd(org.mapdb.MapModificationListener<K, V> listener) {
        if (!(delegate instanceof org.mapdb.ModificationAwareMap))
            throw new UnsupportedOperationException("delegate has no modification listeners");
        ((org.mapdb.ModificationAwareMap<K, V>) delegate).modificationListenerAdd(listener);
    }

    @SuppressWarnings("unchecked")
    @Override public void modificationListenerRemove(org.mapdb.MapModificationListener<K, V> listener) {
        if (!(delegate instanceof org.mapdb.ModificationAwareMap)) return;
        ((org.mapdb.ModificationAwareMap<K, V>) delegate).modificationListenerRemove(listener);
    }
    Atomic.Long counter() { return counter; }

    @Override public int size() {
        long size = counter.get();
        return size > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) size;
    }
    public long sizeLong() { return counter.get(); }
    @SuppressWarnings("unchecked")
    @Override public boolean isClosed() {
        return delegate instanceof org.mapdb.MapExtra
                && ((org.mapdb.MapExtra<K, V>) delegate).isClosed();
    }
    @SuppressWarnings("unchecked")
    @Override public org.mapdb.ser.Serializer<K> keySerializer() {
        return ((org.mapdb.MapExtra<K, V>) delegate).keySerializer();
    }
    @SuppressWarnings("unchecked")
    @Override public org.mapdb.ser.Serializer<V> valueSerializer() {
        return ((org.mapdb.MapExtra<K, V>) delegate).valueSerializer();
    }
    @Override public boolean isEmpty() { return counter.get() == 0; }
    @Override public boolean containsKey(Object key) { return delegate.containsKey(key); }
    @Override public boolean containsValue(Object value) { return delegate.containsValue(value); }
    @Override public V get(Object key) { return delegate.get(key); }
    @Override public V put(K key, V value) { return delegate.put(key, value); }
    @Override public V remove(Object key) { return delegate.remove(key); }
    @Override public void putAll(Map<? extends K, ? extends V> map) { delegate.putAll(map); }
    @Override public void clear() { delegate.clear(); }
    @Override public Set<K> keySet() { return delegate.keySet(); }
    @Override public Collection<V> values() { return delegate.values(); }
    @Override public Set<Entry<K, V>> entrySet() { return delegate.entrySet(); }
    @Override public V putIfAbsent(K key, V value) { return delegate.putIfAbsent(key, value); }
    @Override public boolean remove(Object key, Object value) { return delegate.remove(key, value); }
    @Override public boolean replace(K key, V oldValue, V newValue) {
        return delegate.replace(key, oldValue, newValue);
    }
    @Override public V replace(K key, V value) { return delegate.replace(key, value); }
}
