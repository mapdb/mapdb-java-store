package org.mapdb.btree;

/**
 * {@link OrderedMapAdapter} refinement that additionally exposes the atomic
 * ConcurrentMap CAS primitives. Implemented by the genuine
 * {@link java.util.concurrent.ConcurrentMap}s here — {@link BTreeMap} (per-leaf-lock CAS)
 * and {@link BufferTreeMap} (single-writer-lock CAS) — so the
 * {@link ConcurrentOrderedNavigableView} and its concurrent sub-views can offer atomic
 * {@code putIfAbsent}/{@code replace}. Kept separate from the base
 * {@link OrderedMapAdapter} so a future non-concurrent ordered map could reuse the view
 * layer without being forced to expose these primitives.
 */
interface ConcurrentOrderedMapAdapter<K, V> extends OrderedMapAdapter<K, V> {

    V putIfAbsent(K key, V value);

    V replace(K key, V value);

    boolean replace(K key, V oldValue, V newValue);
}
