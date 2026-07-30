package org.mapdb;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;

import org.mapdb.btree.BTreeMap;
import org.mapdb.db.Atomic;
import org.mapdb.htree.HTreeCache;
import org.mapdb.htree.HTreeMap;

/**
 * Runtime bindings that keep secondary collections synchronized with a primary map.
 * Bindings are not persisted and must be installed again after reopen.
 *
 * <p><b>Install before concurrent modification.</b> A binding initially populates an empty
 * secondary by iterating the primary and then registers a listener. If another thread mutates
 * the primary during that window, updates can be missed (the initial scan and the listener are
 * not atomic with respect to concurrent writers). Install every binding while the primary is
 * quiescent — before it is exposed to concurrent modification.
 *
 * <p><b>Ordering.</b> The order-sensitive bindings ({@code secondaryValue}, {@code secondaryValues},
 * {@code secondaryKey}, {@code secondaryKeys}, {@code mapInverse}) install SYNCHRONOUS listeners
 * (see {@link SynchronousMapModificationListener}) that fire while the primary still holds the
 * leaf/segment lock that serialized the mutation. This guarantees that two writers racing on the
 * SAME key deliver their listener events in leaf-mutation order, so the secondary index ends up
 * pointing at the winning (last) value rather than being left on a losing one. The commutative
 * bindings ({@code size}, {@code histogram}) stay deferred.
 *
 * <p><b>Lock-order hazard.</b> Because those listeners run while the primary holds its lock, the
 * secondary is mutated INSIDE the primary's critical section. Binding a map to itself is rejected
 * ({@code IllegalArgumentException}), but distinctness excludes only that direct self-cycle: any
 * topology in which the secondary's own operations can hold ITS lock while (transitively)
 * mutating the primary inverts the lock order and can deadlock — reciprocal bindings (A indexed
 * into B and B indexed into A) are the canonical example, and BTreeMap's spin node locks turn
 * such a deadlock into an unbounded spin. Bind order-sensitive indexes into plain concurrent
 * collections (e.g. {@code ConcurrentHashMap}), or into a MapDB map that is never bound (directly
 * or transitively) back into this primary.
 *
 * <p><b>Exception hazard.</b> A binding listener that throws (for example, {@code secondaryKey}/
 * {@code mapInverse} raising {@code IllegalArgumentException} on a duplicate derived key) does so
 * AFTER the primary has already been mutated, and it aborts before any later-registered listeners
 * run. The primary and any already-updated secondaries are left mutated; the throw only signals
 * the constraint violation. Callers must not assume a throwing listener rolls back the primary.
 */
public final class Bind {
    private Bind() {}

    /** Immutable pair used by one-to-many secondary indexes. */
    public static final class Tuple2<A, B> {
        public final A a;
        public final B b;
        public Tuple2(A a, B b) { this.a = a; this.b = b; }
        @Override public boolean equals(Object other) {
            if (!(other instanceof Tuple2)) return false;
            Tuple2<?, ?> that = (Tuple2<?, ?>) other;
            return Objects.equals(a, that.a) && Objects.equals(b, that.b);
        }
        @Override public int hashCode() { return Objects.hash(a, b); }
        @Override public String toString() { return "(" + a + ", " + b + ")"; }
    }

    public static <K, V> void size(ModificationAwareMap<K, V> primary, Atomic.Long counter) {
        Objects.requireNonNull(counter, "counter");
        if (counter.get() == 0) counter.set(sizeLong(primary));
        primary.modificationListenerAdd((key, oldValue, newValue, triggered) -> {
            if (oldValue == null && newValue != null) counter.incrementAndGet();
            else if (oldValue != null && newValue == null) counter.decrementAndGet();
        });
    }

    /** Reject the direct self-cycle: a sync listener mutating its own primary would re-enter the
     *  lock the primary is holding (spins forever on BTreeMap, throws on HTree deferral). This
     *  cannot detect wrappers or longer cycles — see the class-doc lock-order hazard. */
    private static void requireNotPrimary(Object primary, Object secondary) {
        if (primary == secondary)
            throw new IllegalArgumentException("secondary must not be the primary map itself");
    }

    private static long sizeLong(Map<?, ?> map) {
        if (map instanceof BTreeMap) return ((BTreeMap<?, ?>) map).sizeLong();
        if (map instanceof HTreeMap) return ((HTreeMap<?, ?>) map).sizeLong();
        if (map instanceof HTreeCache) return ((HTreeCache<?, ?>) map).sizeLong();
        return map.size();
    }

    public static <K, V, V2> void secondaryValue(ModificationAwareMap<K, V> primary,
            Map<K, V2> secondary, BiFunction<? super K, ? super V, ? extends V2> function) {
        Objects.requireNonNull(secondary, "secondary");
        Objects.requireNonNull(function, "function");
        requireNotPrimary(primary, secondary);
        if (secondary.isEmpty()) for (Map.Entry<K, V> e : primary.entrySet())
            secondary.put(e.getKey(), function.apply(e.getKey(), e.getValue()));
        primary.modificationListenerAdd((SynchronousMapModificationListener<K, V>)
                (key, oldValue, newValue, triggered) -> {
            if (newValue == null) secondary.remove(key);
            else secondary.put(key, function.apply(key, newValue));
        });
    }

    public static <K, V, V2> void secondaryValues(ModificationAwareMap<K, V> primary,
            Set<Tuple2<K, V2>> secondary,
            BiFunction<? super K, ? super V, ? extends Iterable<? extends V2>> function) {
        Objects.requireNonNull(secondary, "secondary");
        Objects.requireNonNull(function, "function");
        requireNotPrimary(primary, secondary);
        if (secondary.isEmpty()) for (Map.Entry<K, V> e : primary.entrySet())
            addValues(secondary, e.getKey(), function.apply(e.getKey(), e.getValue()));
        primary.modificationListenerAdd((SynchronousMapModificationListener<K, V>)
                (key, oldValue, newValue, triggered) -> {
            if (oldValue != null) removeValues(secondary, key, function.apply(key, oldValue));
            if (newValue != null) addValues(secondary, key, function.apply(key, newValue));
        });
    }

    private static <K, V2> void addValues(Set<Tuple2<K, V2>> secondary, K key,
            Iterable<? extends V2> values) {
        if (values == null) return;
        for (V2 value : values) secondary.add(new Tuple2<>(key, value));
    }

    private static <K, V2> void removeValues(Set<Tuple2<K, V2>> secondary, K key,
            Iterable<? extends V2> values) {
        if (values == null) return;
        for (V2 value : values) secondary.remove(new Tuple2<>(key, value));
    }

    /** Unique secondary key index: derived key -> primary key. */
    public static <K, V, K2> void secondaryKey(ModificationAwareMap<K, V> primary,
            Map<K2, K> secondary, BiFunction<? super K, ? super V, ? extends K2> function) {
        Objects.requireNonNull(secondary, "secondary");
        Objects.requireNonNull(function, "function");
        requireNotPrimary(primary, secondary);
        if (secondary.isEmpty()) for (Map.Entry<K, V> e : primary.entrySet())
            putUnique(secondary, function.apply(e.getKey(), e.getValue()), e.getKey());
        primary.modificationListenerAdd((SynchronousMapModificationListener<K, V>)
                (key, oldValue, newValue, triggered) -> {
            if (oldValue != null) secondary.remove(function.apply(key, oldValue), key);
            if (newValue != null) putUnique(secondary, function.apply(key, newValue), key);
        });
    }

    private static <K, K2> void putUnique(Map<K2, K> map, K2 derived, K primary) {
        K existing;
        if (map instanceof ConcurrentMap)
            existing = ((ConcurrentMap<K2, K>) map).putIfAbsent(derived, primary);
        else {
            existing = map.get(derived);
            if (existing == null) map.put(derived, primary);
        }
        if (existing != null && !Objects.equals(existing, primary))
            throw new IllegalArgumentException("duplicate secondary key " + derived);
    }

    /** Non-unique secondary key index: pairs of (derived key, primary key). */
    public static <K, V, K2> void secondaryKeys(ModificationAwareMap<K, V> primary,
            Set<Tuple2<K2, K>> secondary,
            BiFunction<? super K, ? super V, ? extends Iterable<? extends K2>> function) {
        Objects.requireNonNull(secondary, "secondary");
        Objects.requireNonNull(function, "function");
        requireNotPrimary(primary, secondary);
        if (secondary.isEmpty()) for (Map.Entry<K, V> e : primary.entrySet())
            addKeys(secondary, e.getKey(), function.apply(e.getKey(), e.getValue()));
        primary.modificationListenerAdd((SynchronousMapModificationListener<K, V>)
                (key, oldValue, newValue, triggered) -> {
            if (oldValue != null) removeKeys(secondary, key, function.apply(key, oldValue));
            if (newValue != null) addKeys(secondary, key, function.apply(key, newValue));
        });
    }

    private static <K, K2> void addKeys(Set<Tuple2<K2, K>> secondary, K key,
            Iterable<? extends K2> values) {
        if (values == null) return;
        for (K2 value : values) secondary.add(new Tuple2<>(value, key));
    }

    private static <K, K2> void removeKeys(Set<Tuple2<K2, K>> secondary, K key,
            Iterable<? extends K2> values) {
        if (values == null) return;
        for (K2 value : values) secondary.remove(new Tuple2<>(value, key));
    }

    /** Inverse index: primary value -> primary key. Values must be unique. */
    public static <K, V> void mapInverse(ModificationAwareMap<K, V> primary, Map<V, K> inverse) {
        secondaryKey(primary, inverse, (key, value) -> value);
    }

    /** Counts primary entries grouped by a derived category. */
    public static <K, V, C> void histogram(ModificationAwareMap<K, V> primary,
            ConcurrentMap<C, Long> histogram,
            BiFunction<? super K, ? super V, ? extends C> category) {
        Objects.requireNonNull(histogram, "histogram");
        Objects.requireNonNull(category, "category");
        if (histogram.isEmpty()) for (Map.Entry<K, V> e : primary.entrySet())
            addCount(histogram, category.apply(e.getKey(), e.getValue()), 1L);
        primary.modificationListenerAdd((key, oldValue, newValue, triggered) -> {
            if (oldValue != null) addCount(histogram, category.apply(key, oldValue), -1L);
            if (newValue != null) addCount(histogram, category.apply(key, newValue), 1L);
        });
    }

    private static <C> void addCount(ConcurrentMap<C, Long> histogram, C category, long delta) {
        histogram.compute(category, (key, old) -> {
            long value = (old == null ? 0L : old) + delta;
            return value == 0 ? null : value;
        });
    }

    /** On deletion from primary, copy the removed value into the target map. */
    public static <K, V> void mapPutAfterDelete(ModificationAwareMap<K, V> primary,
            Map<K, V> target) {
        Objects.requireNonNull(target, "target");
        primary.modificationListenerAdd((key, oldValue, newValue, triggered) -> {
            if (oldValue != null && newValue == null) target.put(key, oldValue);
        });
    }
}
