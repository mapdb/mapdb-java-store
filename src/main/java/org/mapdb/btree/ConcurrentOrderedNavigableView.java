package org.mapdb.btree;

import java.util.concurrent.ConcurrentNavigableMap;

/**
 * {@link ConcurrentNavigableMap} refinement of {@link OrderedNavigableView} for
 * {@link BTreeMap} and its sub-views, which must ALSO be {@code ConcurrentNavigableMap}, not
 * plain {@code NavigableMap}. Java cannot make a single class conditionally implement
 * {@code ConcurrentNavigableMap}, so this thin subclass adds the marker + the atomic CAS
 * surface while reusing every navigation, bound-algebra and orientation method from the base.
 *
 * <p>{@link #newView} is overridden to produce concurrent sub-views, so nested
 * {@code subMap}/{@code headMap}/{@code tailMap}/{@code descendingMap} — reached even
 * through the base-class implementations — stay concurrent; the covariant overrides below
 * just re-type the base result (safe by that construction). The CAS methods delegate to
 * the map's atomic primitives ({@link ConcurrentOrderedMapAdapter}) after enforcing the
 * view bounds.
 */
final class ConcurrentOrderedNavigableView<K, V> extends OrderedNavigableView<K, V>
        implements ConcurrentNavigableMap<K, V> {

    private final ConcurrentOrderedMapAdapter<K, V> ca;

    ConcurrentOrderedNavigableView(ConcurrentOrderedMapAdapter<K, V> a,
                                   K lo, boolean loInc, K hi, boolean hiInc, boolean descending) {
        super(a, lo, loInc, hi, hiInc, descending);
        this.ca = a;
    }

    @Override OrderedNavigableView<K, V> newView(K lo, boolean loInc, K hi, boolean hiInc, boolean descending) {
        return new ConcurrentOrderedNavigableView<>(ca, lo, loInc, hi, hiInc, descending);
    }

    // ---- covariant returns: ConcurrentNavigableMap (base already builds concurrent views) ----

    @Override public ConcurrentNavigableMap<K, V> descendingMap() {
        return (ConcurrentNavigableMap<K, V>) super.descendingMap();
    }
    @Override public ConcurrentNavigableMap<K, V> subMap(K from, boolean fromInc, K to, boolean toInc) {
        return (ConcurrentNavigableMap<K, V>) super.subMap(from, fromInc, to, toInc);
    }
    @Override public ConcurrentNavigableMap<K, V> headMap(K to, boolean inc) {
        return (ConcurrentNavigableMap<K, V>) super.headMap(to, inc);
    }
    @Override public ConcurrentNavigableMap<K, V> tailMap(K from, boolean inc) {
        return (ConcurrentNavigableMap<K, V>) super.tailMap(from, inc);
    }
    @Override public ConcurrentNavigableMap<K, V> subMap(K from, K to) {
        return (ConcurrentNavigableMap<K, V>) super.subMap(from, to);
    }
    @Override public ConcurrentNavigableMap<K, V> headMap(K to) {
        return (ConcurrentNavigableMap<K, V>) super.headMap(to);
    }
    @Override public ConcurrentNavigableMap<K, V> tailMap(K from) {
        return (ConcurrentNavigableMap<K, V>) super.tailMap(from);
    }

    // ---- atomic ConcurrentMap CAS (bound-checked, delegated to the map) ----

    @Override public V putIfAbsent(K key, V value) {
        if (key == null || value == null) throw new NullPointerException();
        if (!inRange(key)) throw new IllegalArgumentException("key out of submap range");
        return ca.putIfAbsent(key, value);
    }

    @Override public V replace(K key, V value) {
        if (key == null || value == null) throw new NullPointerException();
        if (!inRange(key)) return null;
        return ca.replace(key, value);
    }

    @Override public boolean replace(K key, V oldValue, V newValue) {
        if (key == null || oldValue == null || newValue == null) throw new NullPointerException();
        if (!inRange(key)) return false;
        return ca.replace(key, oldValue, newValue);
    }
    // remove(k,v) is inherited atomic from the base (routes through the adapter).
}
