package org.mapdb.btree;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;

/**
 * Narrow bridge between an ordered map ({@link BTreeMap}, {@link BufferTreeMap}) and
 * the shared range/navigation view layer ({@link OrderedNavigableView} and the
 * {@link OrderedKeySet} projection). Deliberately small (spec-btree-map item A): it
 * exposes only the ordering, the point mutators, BOUNDED ascending/descending iterators,
 * atomic bounded poll and a bounded count — the two maps keep their own traversal
 * internals (BTree scans linked leaves; BufferTree does a DFS with LWW/tombstone
 * op-merge), which must NOT be coupled behind this interface.
 *
 * <p>Bounds are passed as nullable keys: a {@code null} lower/upper bound means
 * unbounded on that side (null keys are rejected by the maps, so {@code null} is an
 * unambiguous "no bound" sentinel). {@code loInc}/{@code hiInc} select inclusivity.
 * All iterators/poll take bounds in the map's NATURAL (ascending) key order regardless
 * of the calling view's orientation — the view maps descending semantics onto these
 * ascending-order primitives (spec-btree-map §D).
 *
 * <p>Only the CONCURRENT sub-surface (putIfAbsent/replace) lives in the
 * {@link ConcurrentOrderedMapAdapter} refinement, so a non-concurrent map
 * ({@link BufferTreeMap}) never has to expose ConcurrentMap-style primitives.
 */
public interface OrderedMapAdapter<K, V> {

    /** {@link java.util.SortedMap#comparator()} value: null for natural order. */
    Comparator<? super K> comparator();

    /** The map's total key order (== keyFormat.compare); used for bound checks. */
    int compare(K a, K b);

    V get(Object key);

    boolean containsKey(Object key);

    V put(K key, V value);

    V remove(Object key);

    /** Atomic conditional remove (maps to ConcurrentMap.remove(k,v) / the buffer map's
     *  writeLock'd equivalent) so the entry-set view's {@code remove(entry)} cannot delete
     *  a concurrently-updated value. */
    boolean remove(Object key, Object value);

    /** Logical value equality (format's element equals, not Object.equals). */
    boolean valueEquals(V a, V b);

    /** Ascending entries within [lo,hi] honoring inclusivity; null bound = open. */
    Iterator<Map.Entry<K, V>> entryIterator(K lo, boolean loInc, K hi, boolean hiInc);

    /** Descending entries within [lo,hi] honoring inclusivity; null bound = open.
     *  Weakly consistent, same as the ascending iterator. */
    Iterator<Map.Entry<K, V>> descendingEntryIterator(K lo, boolean loInc, K hi, boolean hiInc);

    /**
     * Atomically remove and return the LEAST in-range entry, or null when the range is
     * empty. Backing (ascending) orientation: a descending view maps its
     * {@code pollFirstEntry} onto {@link #pollLastEntry} (spec-btree-map §D). The
     * returned entry is an immutable snapshot; the removed value equals the returned
     * value (never removes a value it did not return).
     */
    Map.Entry<K, V> pollFirstEntry(K lo, boolean loInc, K hi, boolean hiInc);

    /** Atomically remove and return the GREATEST in-range entry, or null when empty. */
    Map.Entry<K, V> pollLastEntry(K lo, boolean loInc, K hi, boolean hiInc);

    /** Count of entries in the same bounded range (may be O(range)). */
    long sizeLong(K lo, boolean loInc, K hi, boolean hiInc);
}
