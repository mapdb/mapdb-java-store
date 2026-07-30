package org.mapdb.btree;

import java.util.AbstractSet;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.SortedSet;

/**
 * {@link NavigableSet} key-view PROJECTION over an {@link OrderedNavigableView}.
 * Every navigation/mutation delegates to the backing map view and unwraps keys — no
 * traversal, ordering, or bound logic is duplicated. Iteration follows the view's
 * orientation (a descending view's key-set iterates descending); {@code iterator.remove()}
 * routes through the view's mutable entry-set. {@code add} is unsupported (a key alone
 * carries no value). Used for {@code keySet}/{@code navigableKeySet}/{@code descendingKeySet}
 * on both maps — a plain {@code NavigableSet} in both the NavigableMap and
 * ConcurrentNavigableMap cases (there is no concurrent set variant to distinguish).
 */
final class OrderedKeySet<K, V> extends AbstractSet<K> implements NavigableSet<K> {

    private final OrderedNavigableView<K, V> v;

    OrderedKeySet(OrderedNavigableView<K, V> v) { this.v = v; }

    private static <K, V> Iterator<K> keyIter(Iterator<Map.Entry<K, V>> it) {
        return new Iterator<>() {
            @Override public boolean hasNext() { return it.hasNext(); }
            @Override public K next() { return it.next().getKey(); }
            @Override public void remove() { it.remove(); }
        };
    }

    @Override public Iterator<K> iterator() { return keyIter(v.entrySet().iterator()); }
    @Override public Iterator<K> descendingIterator() { return keyIter(v.descendingView().entrySet().iterator()); }

    @Override public Comparator<? super K> comparator() { return v.comparator(); }
    @Override public int size() { return v.size(); }
    @Override public boolean isEmpty() { return v.isEmpty(); }
    @Override public boolean contains(Object o) { return v.containsKey(o); }
    @Override public boolean remove(Object o) { return v.remove(o) != null; }
    @Override public void clear() { v.clear(); }
    @Override public boolean add(K k) { throw new UnsupportedOperationException(); }

    @Override public K first() { return v.firstKey(); }
    @Override public K last() { return v.lastKey(); }
    @Override public K lower(K k) { return v.lowerKey(k); }
    @Override public K floor(K k) { return v.floorKey(k); }
    @Override public K ceiling(K k) { return v.ceilingKey(k); }
    @Override public K higher(K k) { return v.higherKey(k); }

    @Override public K pollFirst() { Map.Entry<K, V> e = v.pollFirstEntry(); return e == null ? null : e.getKey(); }
    @Override public K pollLast() { Map.Entry<K, V> e = v.pollLastEntry(); return e == null ? null : e.getKey(); }

    @Override public NavigableSet<K> descendingSet() { return v.descendingView().navigableKeySet(); }

    private NavigableSet<K> keysOf(java.util.NavigableMap<K, V> m) {
        return ((OrderedNavigableView<K, V>) m).navigableKeySet();
    }

    @Override public NavigableSet<K> subSet(K from, boolean fromInc, K to, boolean toInc) {
        return keysOf(v.subMap(from, fromInc, to, toInc));
    }
    @Override public NavigableSet<K> headSet(K to, boolean inc) { return keysOf(v.headMap(to, inc)); }
    @Override public NavigableSet<K> tailSet(K from, boolean inc) { return keysOf(v.tailMap(from, inc)); }

    @Override public SortedSet<K> subSet(K from, K to) { return subSet(from, true, to, false); }
    @Override public SortedSet<K> headSet(K to) { return headSet(to, false); }
    @Override public SortedSet<K> tailSet(K from) { return tailSet(from, true); }
}
