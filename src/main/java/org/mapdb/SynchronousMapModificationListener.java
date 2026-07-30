package org.mapdb;

/**
 * Marker for a {@link MapModificationListener} that must fire SYNCHRONOUSLY — while the map still
 * holds the covering leaf (BTreeMap) or segment (HTreeMap/HTreeCache) write lock that serialized
 * the mutation. This preserves per-key event order for order-sensitive listeners such as
 * last-writer-wins secondary indexes (see {@link Bind}).
 *
 * <p>Ordinary {@link MapModificationListener}s are fired AFTER the covering lock is released
 * (deferred), which avoids re-entrancy/lock-ordering deadlocks from arbitrary user code but lets
 * two same-key mutations deliver their events in the opposite order under contention — leaving an
 * order-sensitive index pointing at the losing value permanently.
 *
 * <p>Because a synchronous listener runs UNDER the map's leaf/segment lock, its body MUST NOT
 * re-enter the primary map, and MUST NOT mutate another lock-holding MapDB map in any topology
 * that can invert lock order — if that other map's own operations can hold ITS lock while
 * (transitively) mutating this primary (reciprocal {@link Bind} bindings are the canonical
 * example), the two lock orders deadlock, and BTreeMap's spin node locks turn that into an
 * unbounded spin. Mutating a plain concurrent collection (e.g. {@code ConcurrentHashMap}), or a
 * MapDB map that never feeds back into this primary, is safe. The listener fires AFTER the
 * primary mutation (and its size-counter update) has committed, so a throwing listener leaves
 * the primary consistent — later listeners still receive the event, with the first throwable
 * rethrown afterwards and the rest attached as suppressed.
 */
public interface SynchronousMapModificationListener<K, V> extends MapModificationListener<K, V> {}
