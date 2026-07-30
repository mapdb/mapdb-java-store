package org.mapdb.btree;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;

/**
 * Shared, fully-bounded, live {@link NavigableMap} view over an {@link OrderedMapAdapter}
 * (spec-btree-map items A/D). Used both for the {@code headMap}/{@code tailMap}/
 * {@code subMap}/{@code descendingMap} sub-views of {@link BTreeMap}/{@link BufferTreeMap}
 * AND — with open bounds — as the backing entry/key views of the maps themselves, so all
 * range + navigation logic lives in exactly one place (verdict #7).
 *
 * <h2>Orientation</h2>
 * A {@code descending} flag reverses the view WITHOUT touching the backing key interval:
 * it flips {@link #comparator()}, the iteration direction, and the sense of every
 * navigational method per the spec §D mapping table (e.g. a descending {@code firstEntry}
 * is the backing GREATEST in range; {@code pollFirstEntry} maps to the backing
 * {@code pollLastEntry}; descending {@code headMap(to)} selects backing keys {@code > to}).
 * {@link #descendingMap()} merely flips the flag, so
 * {@code descendingMap().descendingMap()} behaves as the original orientation.
 *
 * <h2>Bounds</h2>
 * Every method enforces the bounds, never just iteration: {@code get}/{@code containsKey}/
 * {@code remove} short-circuit out-of-range keys WITHOUT touching the backing map;
 * {@code put} of an out-of-range key throws {@link IllegalArgumentException}; {@code clear}
 * removes only in-range entries. A {@code null} bound key means "unbounded on that side"
 * (null keys are rejected by the maps, so it is an unambiguous open-end sentinel). Bound
 * INTERSECTION for sub-views takes the tighter of parent/child on each side (a child can
 * never widen an exclusive parent bound), and single-key navigation queries that compute
 * an inverted or exclusive-equal range short-circuit through {@link #rangeEmpty} rather
 * than trusting a backing iterator to interpret {@code lo > hi}.
 *
 * <p>Weakly consistent: reflects the live map through the adapter, so concurrent writes may
 * or may not be seen. Entries returned by the {@code *Entry}/poll methods are IMMUTABLE
 * snapshots ({@link AbstractMap.SimpleImmutableEntry}), enforced here.
 *
 * <p>This class is {@code NavigableMap} only; {@link BTreeMap}'s concurrent sub-views use
 * the {@link ConcurrentOrderedNavigableView} subclass. All view-producing methods route
 * through {@link #newView} so the subclass's covariant/concurrent views propagate to
 * nested sub-views.
 */
public class OrderedNavigableView<K, V> extends AbstractMap<K, V> implements NavigableMap<K, V> {

    final OrderedMapAdapter<K, V> a;
    final K lo;
    final boolean loInc;
    final K hi;
    final boolean hiInc;
    final boolean descending;

    public OrderedNavigableView(OrderedMapAdapter<K, V> a, K lo, boolean loInc, K hi, boolean hiInc, boolean descending) {
        this.a = a;
        this.lo = lo;
        this.loInc = loInc;
        this.hi = hi;
        this.hiInc = hiInc;
        this.descending = descending;
    }

    /** Factory hook: the concurrent subclass overrides this to build concurrent sub-views,
     *  so every {@code subMap}/{@code headMap}/{@code tailMap}/{@code descendingMap} — and
     *  the key-set's sub-sets — inherit the right runtime type. */
    OrderedNavigableView<K, V> newView(K lo, boolean loInc, K hi, boolean hiInc, boolean descending) {
        return new OrderedNavigableView<>(a, lo, loInc, hi, hiInc, descending);
    }

    // ---- bound predicates (backing/value order, orientation-independent) ----

    private boolean tooLow(K k) {
        if (lo == null) return false;
        int c = a.compare(k, lo);
        return c < 0 || (c == 0 && !loInc);
    }

    private boolean tooHigh(K k) {
        if (hi == null) return false;
        int c = a.compare(k, hi);
        return c > 0 || (c == 0 && !hiInc);
    }

    boolean inRange(K k) { return !tooLow(k) && !tooHigh(k); }

    /** True when [lo2,hi2] (with the given inclusivity) covers NO key: inverted
     *  (lo2 > hi2) or equal endpoints with either side exclusive. Guards every navigation
     *  query whose computed bounds may invert (e.g. {@code ceilingEntry} above the high
     *  bound), so we never scan a backing iterator over a nonsensical range. */
    private boolean rangeEmpty(K lo2, boolean loInc2, K hi2, boolean hiInc2) {
        if (lo2 != null && hi2 != null) {
            int c = a.compare(lo2, hi2);
            if (c > 0) return true;
            if (c == 0) return !(loInc2 && hiInc2);
        }
        return false;
    }

    /** JDK-conform range check for a new sub-view bound: an INCLUSIVE new bound must respect
     *  the parent's exclusivity at equality (TreeMap's inRange), an exclusive one only needs
     *  closed-range containment (inClosedRange). */
    private void checkBoundKey(K k, boolean kInc) {
        if (k == null) throw new NullPointerException();
        if (lo != null) {
            int c = a.compare(k, lo);
            if (c < 0 || (c == 0 && kInc && !loInc)) throw new IllegalArgumentException("key out of range");
        }
        if (hi != null) {
            int c = a.compare(k, hi);
            if (c > 0 || (c == 0 && kInc && !hiInc)) throw new IllegalArgumentException("key out of range");
        }
    }

    // ---- effective-bound intersection (never widen the parent) ----

    /** Effective LOWER bound = MAX of parent (lo,loInc) and probe (k,kInc). */
    private Object[] effLower(K k, boolean kInc) {
        if (lo == null) return new Object[]{k, kInc};
        int c = a.compare(k, lo);
        if (c > 0) return new Object[]{k, kInc};
        if (c < 0) return new Object[]{lo, loInc};
        return new Object[]{lo, loInc && kInc};
    }

    /** Effective UPPER bound = MIN of parent (hi,hiInc) and probe (k,kInc). */
    private Object[] effUpper(K k, boolean kInc) {
        if (hi == null) return new Object[]{k, kInc};
        int c = a.compare(k, hi);
        if (c < 0) return new Object[]{k, kInc};
        if (c > 0) return new Object[]{hi, hiInc};
        return new Object[]{hi, hiInc && kInc};
    }

    // ---- backing (ascending-order) navigation primitives over this view's [lo,hi] ----

    @SuppressWarnings("unchecked")
    private Map.Entry<K, V> firstOf(K lo2, boolean loInc2, K hi2, boolean hiInc2) {
        if (rangeEmpty(lo2, loInc2, hi2, hiInc2)) return null;
        Iterator<Map.Entry<K, V>> it = a.entryIterator(lo2, loInc2, hi2, hiInc2);
        return it.hasNext() ? it.next() : null;
    }

    /** Greatest in-range entry via an ASCENDING scan keeping the last (O(range) time,
     *  O(1) memory — never materializes the range for a single {@code last}/{@code floor}
     *  query; the descending iterator is reserved for actual reverse iteration). */
    private Map.Entry<K, V> lastOf(K lo2, boolean loInc2, K hi2, boolean hiInc2) {
        if (rangeEmpty(lo2, loInc2, hi2, hiInc2)) return null;
        Iterator<Map.Entry<K, V>> it = a.entryIterator(lo2, loInc2, hi2, hiInc2);
        Map.Entry<K, V> last = null;
        while (it.hasNext()) last = it.next();
        return last;
    }

    @SuppressWarnings("unchecked")
    private Map.Entry<K, V> backingCeiling(K k) { Object[] b = effLower(k, true);  return firstOf((K) b[0], (boolean) b[1], hi, hiInc); }
    @SuppressWarnings("unchecked")
    private Map.Entry<K, V> backingHigher(K k)  { Object[] b = effLower(k, false); return firstOf((K) b[0], (boolean) b[1], hi, hiInc); }
    @SuppressWarnings("unchecked")
    private Map.Entry<K, V> backingFloor(K k)   { Object[] b = effUpper(k, true);  return lastOf(lo, loInc, (K) b[0], (boolean) b[1]); }
    @SuppressWarnings("unchecked")
    private Map.Entry<K, V> backingLower(K k)   { Object[] b = effUpper(k, false); return lastOf(lo, loInc, (K) b[0], (boolean) b[1]); }
    private Map.Entry<K, V> backingFirst()      { return firstOf(lo, loInc, hi, hiInc); }
    private Map.Entry<K, V> backingLast()       { return lastOf(lo, loInc, hi, hiInc); }
    private Map.Entry<K, V> backingPollFirst()  { return rangeEmpty(lo, loInc, hi, hiInc) ? null : a.pollFirstEntry(lo, loInc, hi, hiInc); }
    private Map.Entry<K, V> backingPollLast()   { return rangeEmpty(lo, loInc, hi, hiInc) ? null : a.pollLastEntry(lo, loInc, hi, hiInc); }

    private static <K, V> Map.Entry<K, V> snap(Map.Entry<K, V> e) {
        return e == null ? null : new AbstractMap.SimpleImmutableEntry<>(e.getKey(), e.getValue());
    }

    private static void requireKey(Object k) { if (k == null) throw new NullPointerException(); }

    // ---- NavigableMap: entry navigation (orientation-mapped, spec §D) ----

    @Override public Map.Entry<K, V> firstEntry() { return snap(descending ? backingLast() : backingFirst()); }
    @Override public Map.Entry<K, V> lastEntry()  { return snap(descending ? backingFirst() : backingLast()); }

    @Override public Map.Entry<K, V> lowerEntry(K k)   { requireKey(k); return snap(descending ? backingHigher(k)  : backingLower(k)); }
    @Override public Map.Entry<K, V> floorEntry(K k)   { requireKey(k); return snap(descending ? backingCeiling(k) : backingFloor(k)); }
    @Override public Map.Entry<K, V> ceilingEntry(K k) { requireKey(k); return snap(descending ? backingFloor(k)   : backingCeiling(k)); }
    @Override public Map.Entry<K, V> higherEntry(K k)  { requireKey(k); return snap(descending ? backingLower(k)   : backingHigher(k)); }

    @Override public Map.Entry<K, V> pollFirstEntry() { return snap(descending ? backingPollLast()  : backingPollFirst()); }
    @Override public Map.Entry<K, V> pollLastEntry()  { return snap(descending ? backingPollFirst() : backingPollLast()); }

    @Override public K lowerKey(K k)   { Map.Entry<K, V> e = lowerEntry(k);   return e == null ? null : e.getKey(); }
    @Override public K floorKey(K k)   { Map.Entry<K, V> e = floorEntry(k);   return e == null ? null : e.getKey(); }
    @Override public K ceilingKey(K k) { Map.Entry<K, V> e = ceilingEntry(k); return e == null ? null : e.getKey(); }
    @Override public K higherKey(K k)  { Map.Entry<K, V> e = higherEntry(k);  return e == null ? null : e.getKey(); }

    @Override public K firstKey() { Map.Entry<K, V> e = firstEntry(); if (e == null) throw new NoSuchElementException(); return e.getKey(); }
    @Override public K lastKey()  { Map.Entry<K, V> e = lastEntry();  if (e == null) throw new NoSuchElementException(); return e.getKey(); }

    // ---- SortedMap ----

    @Override public Comparator<? super K> comparator() {
        Comparator<? super K> base = a.comparator();
        if (!descending) return base;
        return base == null ? Collections.reverseOrder() : Collections.reverseOrder(base);
    }

    // ---- point ops (bounded, orientation-independent) ----

    @SuppressWarnings("unchecked")
    @Override public V get(Object key) {
        requireKey(key);
        return inRange((K) key) ? a.get(key) : null;
    }

    @SuppressWarnings("unchecked")
    @Override public boolean containsKey(Object key) {
        requireKey(key);
        return inRange((K) key) && a.containsKey(key);
    }

    @Override public V put(K key, V value) {
        if (key == null || value == null) throw new NullPointerException();
        if (!inRange(key)) throw new IllegalArgumentException("key out of submap range");
        return a.put(key, value);
    }

    @SuppressWarnings("unchecked")
    @Override public V remove(Object key) {
        requireKey(key);
        return inRange((K) key) ? a.remove(key) : null;
    }

    /** Atomic conditional remove routed through the adapter (never deletes a
     *  concurrently-updated value); available on the base view too since both maps' adapters
     *  provide an atomic {@code remove(k,v)}. Null key/value → NPE, matching the top-level
     *  maps and the ConcurrentMap contract for a map that permits no nulls. */
    @SuppressWarnings("unchecked")
    @Override public boolean remove(Object key, Object value) {
        if (key == null || value == null) throw new NullPointerException();
        return inRange((K) key) && a.remove(key, value);
    }

    // ---- bulk / size ----

    @Override public int size() {
        if (rangeEmpty(lo, loInc, hi, hiInc)) return 0;
        long n = a.sizeLong(lo, loInc, hi, hiInc);
        return n > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) n;
    }

    @Override public boolean isEmpty() { return !ascendingRange().hasNext(); }

    /** Bounded clear: removes ONLY in-range entries. Snapshots keys first — the raw range
     *  iterator does not support {@code remove()} and mutating a live BufferTree DFS could
     *  disturb it. */
    @Override public void clear() {
        ArrayList<K> keys = new ArrayList<>();
        for (Iterator<Map.Entry<K, V>> it = ascendingRange(); it.hasNext(); ) keys.add(it.next().getKey());
        for (K k : keys) a.remove(k);
    }

    private Iterator<Map.Entry<K, V>> ascendingRange() {
        return rangeEmpty(lo, loInc, hi, hiInc) ? Collections.emptyIterator() : a.entryIterator(lo, loInc, hi, hiInc);
    }

    private Iterator<Map.Entry<K, V>> descendingRange() {
        return rangeEmpty(lo, loInc, hi, hiInc) ? Collections.emptyIterator() : a.descendingEntryIterator(lo, loInc, hi, hiInc);
    }

    /** Iterator in THIS view's orientation (ascending or descending). */
    private Iterator<Map.Entry<K, V>> orientedRange() {
        return descending ? descendingRange() : ascendingRange();
    }

    // ---- entry-set (weakly consistent, orientation-aware, mutable) ----

    @Override public Set<Map.Entry<K, V>> entrySet() {
        return new AbstractSet<>() {
            @Override public Iterator<Map.Entry<K, V>> iterator() {
                Iterator<Map.Entry<K, V>> base = orientedRange();
                return new Iterator<>() {
                    K lastKey;
                    boolean removable;

                    @Override public boolean hasNext() { return base.hasNext(); }

                    @Override public Map.Entry<K, V> next() {
                        Map.Entry<K, V> e = base.next();
                        lastKey = e.getKey();
                        removable = true;
                        return e;
                    }

                    @Override public void remove() {
                        if (!removable) throw new IllegalStateException();
                        removable = false;
                        a.remove(lastKey);
                    }
                };
            }

            @Override public int size() { return OrderedNavigableView.this.size(); }
            @Override public boolean isEmpty() { return OrderedNavigableView.this.isEmpty(); }
            @Override public void clear() { OrderedNavigableView.this.clear(); }

            @SuppressWarnings("unchecked")
            @Override public boolean contains(Object o) {
                if (!(o instanceof Map.Entry)) return false;
                Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
                Object k = e.getKey(), v = e.getValue();
                if (k == null || v == null || !inRange((K) k)) return false;
                V cur = a.get(k);
                return cur != null && a.valueEquals(cur, (V) v);
            }

            @SuppressWarnings("unchecked")
            @Override public boolean remove(Object o) {
                if (!(o instanceof Map.Entry)) return false;
                Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
                Object k = e.getKey(), v = e.getValue();
                if (k == null || v == null || !inRange((K) k)) return false;
                return a.remove(k, v); // atomic conditional remove
            }
        };
    }

    // ---- key-set views (NavigableSet projections) ----

    @Override public NavigableSet<K> navigableKeySet() { return new OrderedKeySet<>(this); }
    @Override public NavigableSet<K> keySet() { return navigableKeySet(); }
    @Override public NavigableSet<K> descendingKeySet() { return descendingView().navigableKeySet(); }

    // ---- descending / sub-map views (all route through newView) ----

    @Override public NavigableMap<K, V> descendingMap() { return newView(lo, loInc, hi, hiInc, !descending); }

    /** {@link #descendingMap()} narrowed to the concrete type, for key-set projections. */
    OrderedNavigableView<K, V> descendingView() { return newView(lo, loInc, hi, hiInc, !descending); }

    /** Build a sub-view: for each side either inherit the parent bound or check + intersect
     *  an argument bound. Callers pick which arg maps to lo/hi per orientation (spec §D). */
    private OrderedNavigableView<K, V> makeSub(boolean loFromArg, K loArg, boolean loArgInc,
                                               boolean hiFromArg, K hiArg, boolean hiArgInc) {
        K nLo = lo; boolean nLoInc = loInc;
        if (loFromArg) {
            checkBoundKey(loArg, loArgInc);
            Object[] b = effLower(loArg, loArgInc);
            nLo = cast(b[0]); nLoInc = (boolean) b[1];
        }
        K nHi = hi; boolean nHiInc = hiInc;
        if (hiFromArg) {
            checkBoundKey(hiArg, hiArgInc);
            Object[] b = effUpper(hiArg, hiArgInc);
            nHi = cast(b[0]); nHiInc = (boolean) b[1];
        }
        return newView(nLo, nLoInc, nHi, nHiInc, descending);
    }

    @SuppressWarnings("unchecked")
    private K cast(Object o) { return (K) o; }

    @Override public NavigableMap<K, V> subMap(K from, boolean fromInc, K to, boolean toInc) {
        if (from == null || to == null) throw new NullPointerException();
        if (!descending) {
            if (a.compare(from, to) > 0) throw new IllegalArgumentException("fromKey > toKey");
            return makeSub(true, from, fromInc, true, to, toInc);
        }
        // descending: args are in descending order (backing from >= to)
        if (a.compare(to, from) > 0) throw new IllegalArgumentException("fromKey > toKey");
        return makeSub(true, to, toInc, true, from, fromInc);
    }

    @Override public NavigableMap<K, V> headMap(K to, boolean inc) {
        if (to == null) throw new NullPointerException();
        // ascending headMap = backing keys < to; descending headMap = backing keys > to
        return descending ? makeSub(true, to, inc, false, null, false)
                          : makeSub(false, null, false, true, to, inc);
    }

    @Override public NavigableMap<K, V> tailMap(K from, boolean inc) {
        if (from == null) throw new NullPointerException();
        // ascending tailMap = backing keys >= from; descending tailMap = backing keys < from
        return descending ? makeSub(false, null, false, true, from, inc)
                          : makeSub(true, from, inc, false, null, false);
    }

    @Override public SortedMap<K, V> subMap(K from, K to) { return subMap(from, true, to, false); }
    @Override public SortedMap<K, V> headMap(K to) { return headMap(to, false); }
    @Override public SortedMap<K, V> tailMap(K from) { return tailMap(from, true); }
}
