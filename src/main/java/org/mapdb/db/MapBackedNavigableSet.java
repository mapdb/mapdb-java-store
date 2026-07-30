package org.mapdb.db;

import java.util.Comparator;
import java.util.Iterator;
import java.util.AbstractSet;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentNavigableMap;

/**
 * A {@link NavigableSet} view backed by a {@link ConcurrentNavigableMap} whose
 * values are all the sentinel {@link NoValueSerializer#NONE}. Every set element
 * is stored as a map key mapping to that single shared value.
 *
 * <p>Mutating operations that add or remove single elements ({@link #add},
 * {@link #remove}, {@link #clear}) go through the backing map. All read and
 * navigation operations delegate to the map's {@code navigableKeySet()}.
 *
 * <p><b>Note:</b> the sub-range views returned by {@link #headSet},
 * {@link #tailSet}, {@link #subSet} and {@link #descendingSet} are
 * read/remove-only in v1: they come straight from the backing key set and do
 * not support {@code add}.
 *
 * @param <E> the element type
 */
public final class MapBackedNavigableSet<E> extends AbstractSet<E> implements NavigableSet<E> {

    private final ConcurrentNavigableMap<E, Object> map;
    private final NavigableSet<E> keys;

    /**
     * Creates a navigable set view backed by the given map.
     *
     * @param map the backing map; its keys are the set elements
     */
    public MapBackedNavigableSet(ConcurrentNavigableMap<E, Object> map) {
        this.map = map;
        this.keys = map.navigableKeySet();
    }

    @Override
    public boolean add(E e) {
        return map.put(e, NoValueSerializer.NONE) == null;
    }

    @Override
    public boolean remove(Object o) {
        return map.remove(o) != null;
    }

    @Override
    public void clear() {
        map.clear();
    }

    @Override
    public Iterator<E> iterator() {
        return keys.iterator();
    }

    @Override
    public Iterator<E> descendingIterator() {
        return keys.descendingIterator();
    }

    @Override
    public int size() {
        return keys.size();
    }

    @Override
    public boolean isEmpty() {
        return keys.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return keys.contains(o);
    }

    @Override
    public Comparator<? super E> comparator() {
        return keys.comparator();
    }

    @Override
    public E first() {
        return keys.first();
    }

    @Override
    public E last() {
        return keys.last();
    }

    @Override
    public E lower(E e) {
        return keys.lower(e);
    }

    @Override
    public E floor(E e) {
        return keys.floor(e);
    }

    @Override
    public E ceiling(E e) {
        return keys.ceiling(e);
    }

    @Override
    public E higher(E e) {
        return keys.higher(e);
    }

    @Override
    public E pollFirst() {
        return keys.pollFirst();
    }

    @Override
    public E pollLast() {
        return keys.pollLast();
    }

    @Override
    public NavigableSet<E> descendingSet() {
        return keys.descendingSet();
    }

    @Override
    public NavigableSet<E> headSet(E toElement, boolean inclusive) {
        return keys.headSet(toElement, inclusive);
    }

    @Override
    public SortedSet<E> headSet(E toElement) {
        return keys.headSet(toElement);
    }

    @Override
    public NavigableSet<E> tailSet(E fromElement, boolean inclusive) {
        return keys.tailSet(fromElement, inclusive);
    }

    @Override
    public SortedSet<E> tailSet(E fromElement) {
        return keys.tailSet(fromElement);
    }

    @Override
    public NavigableSet<E> subSet(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive) {
        return keys.subSet(fromElement, fromInclusive, toElement, toInclusive);
    }

    @Override
    public SortedSet<E> subSet(E fromElement, E toElement) {
        return keys.subSet(fromElement, toElement);
    }

    /**
     * Returns the backing map (used by the DB facade for teardown).
     *
     * @return the backing map
     */
    ConcurrentNavigableMap<E, Object> backingMap() {
        return map;
    }
}
