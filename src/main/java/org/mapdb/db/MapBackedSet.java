package org.mapdb.db;

import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;

/**
 * A {@link java.util.Set} view backed by a {@link Map} whose values are all the
 * sentinel {@link NoValueSerializer#NONE}. Every set element is stored as a map
 * key mapping to that single shared value.
 *
 * @param <E> the element type
 */
public final class MapBackedSet<E> extends AbstractSet<E> {

    private final Map<E, Object> map;

    /**
     * Creates a set view backed by the given map.
     *
     * @param map the backing map; its keys are the set elements
     */
    public MapBackedSet(Map<E, Object> map) {
        this.map = map;
    }

    @Override
    public boolean add(E e) {
        return map.put(e, NoValueSerializer.NONE) == null;
    }

    @Override
    public boolean contains(Object o) {
        return map.containsKey(o);
    }

    @Override
    public boolean remove(Object o) {
        return map.remove(o) != null;
    }

    @Override
    public Iterator<E> iterator() {
        return map.keySet().iterator();
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public void clear() {
        map.clear();
    }

    /**
     * Returns the backing map (used by the DB facade for teardown).
     *
     * @return the backing map
     */
    Map<E, Object> backingMap() {
        return map;
    }
}
