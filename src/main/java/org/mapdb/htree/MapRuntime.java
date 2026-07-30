package org.mapdb.htree;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import org.mapdb.MapModificationListener;

/** Shared runtime-only listener/value-loader state for HTree variants. */
final class MapRuntime<K, V> {
    private static final class Event<K, V> {
        final K key; final V oldValue, newValue; final boolean triggered;
        Event(K key, V oldValue, V newValue, boolean triggered) {
            this.key = key; this.oldValue = oldValue; this.newValue = newValue;
            this.triggered = triggered;
        }
    }
    private final CopyOnWriteArrayList<MapModificationListener<K, V>> listeners =
            new CopyOnWriteArrayList<>();
    // Order-sensitive listeners fired SYNCHRONOUSLY at the fire() call site, which runs under the
    // segment write lock — preserving per-key event order for last-writer-wins secondary indexes.
    // Ordinary listeners stay deferred (fired after the lock is released) to avoid re-entrancy
    // deadlocks. See org.mapdb.SynchronousMapModificationListener.
    private final CopyOnWriteArrayList<MapModificationListener<K, V>> syncListeners =
            new CopyOnWriteArrayList<>();
    volatile Function<? super K, ? extends V> valueLoader;
    private final ThreadLocal<java.util.ArrayList<Event<K, V>>> deferred = new ThreadLocal<>();

    void add(MapModificationListener<K, V> listener) {
        if (listener == null) throw new NullPointerException("listener");
        if (listener instanceof org.mapdb.SynchronousMapModificationListener)
            syncListeners.addIfAbsent(listener);
        else
            listeners.addIfAbsent(listener);
    }

    void remove(MapModificationListener<K, V> listener) {
        listeners.remove(listener);
        syncListeners.remove(listener);
    }
    boolean hasListeners() { return !listeners.isEmpty() || !syncListeners.isEmpty(); }

    void deferBegin() {
        if (deferred.get() != null) throw new IllegalStateException("nested map mutation deferral");
        deferred.set(new java.util.ArrayList<>());
    }

    /** Called only after the collection write lock has been released. Delivers EVERY queued
     *  event to every listener even when one throws — the ThreadLocal is cleared first, the
     *  first throwable rethrown after the loop with later ones suppressed. */
    void deferEnd() {
        java.util.ArrayList<Event<K, V>> events = deferred.get();
        deferred.remove();
        if (events == null) return;
        Throwable first = null;
        for (Event<K, V> event : events) {
            try {
                fireNow(event.key, event.oldValue, event.newValue, event.triggered);
            } catch (RuntimeException | Error t) {
                first = suppress(first, t);
            }
        }
        rethrow(first);
    }

    /** Full fire: sync listeners immediately (caller MUST hold the segment write lock),
     *  ordinary listeners deferred until the lock is released. The deferred event is
     *  queued/delivered even when a sync listener threw — otherwise ordinary listeners
     *  would silently miss the mutation — with the sync throwable rethrown after. */
    void fire(K key, V oldValue, V newValue, boolean triggered) {
        Throwable first = null;
        try {
            fireSync(key, oldValue, newValue, triggered);
        } catch (RuntimeException | Error t) {
            first = t;
        }
        try {
            fireDeferred(key, oldValue, newValue, triggered);
        } catch (RuntimeException | Error t) {
            first = suppress(first, t);
        }
        rethrow(first);
    }

    /** Synchronous listeners ONLY. Caller MUST hold the segment write lock — that lock is
     *  what gives sync listeners their per-key ordering guarantee. Used directly for
     *  eviction victims, which fire their sync event at evict time (under the lock) and
     *  their deferred event in the post-unlock flush. */
    void fireSync(K key, V oldValue, V newValue, boolean triggered) {
        deliver(syncListeners, key, oldValue, newValue, triggered);
    }

    /** Ordinary (deferred) listeners ONLY: enqueued while a deferral window is open on this
     *  thread, fired immediately otherwise (e.g. the post-unlock eviction flush). */
    void fireDeferred(K key, V oldValue, V newValue, boolean triggered) {
        java.util.ArrayList<Event<K, V>> events = deferred.get();
        if (events != null) {
            events.add(new Event<>(key, oldValue, newValue, triggered));
            return;
        }
        fireNow(key, oldValue, newValue, triggered);
    }

    private void fireNow(K key, V oldValue, V newValue, boolean triggered) {
        deliver(listeners, key, oldValue, newValue, triggered);
    }

    /** Per-listener continuation: every listener sees the event even when an earlier one
     *  throws; first throwable rethrown after, later ones suppressed. */
    private static <K, V> void deliver(Iterable<MapModificationListener<K, V>> listeners,
            K key, V oldValue, V newValue, boolean triggered) {
        Throwable first = null;
        for (MapModificationListener<K, V> listener : listeners) {
            try {
                listener.modify(key, oldValue, newValue, triggered);
            } catch (RuntimeException | Error t) {
                first = suppress(first, t);
            }
        }
        rethrow(first);
    }

    static Throwable suppress(Throwable first, Throwable t) {
        if (first == null) return t;
        if (first != t) first.addSuppressed(t); // a shared exception instance must not self-suppress
        return first;
    }

    static void rethrow(Throwable first) {
        if (first == null) return;
        if (first instanceof Error) throw (Error) first;
        throw (RuntimeException) first;
    }

    /**
     * Batch-deliver removal events (entries hold key at [0], value at [1]; extra slots are
     * ignored), CONTINUING past a throwing listener so every event reaches every listener —
     * otherwise listener kinds would see different event sets (e.g. an order-sensitive
     * secondary retaining entries for later victims). The FIRST throwable is rethrown after
     * the batch, later ones attached as suppressed. {@code syncOnly} restricts delivery to
     * the sync listeners (eviction victims, whose deferred events flush post-unlock);
     * callers must hold the segment write lock either way.
     */
    @SuppressWarnings("unchecked")
    void fireRemovalBatch(java.util.List<Object[]> entries, boolean triggered, boolean syncOnly) {
        Throwable first = null;
        for (Object[] e : entries) {
            try {
                if (syncOnly) fireSync((K) e[0], (V) e[1], null, triggered);
                else fire((K) e[0], (V) e[1], null, triggered);
            } catch (RuntimeException | Error t) {
                first = suppress(first, t);
            }
        }
        rethrow(first);
    }

    V load(K key) {
        Function<? super K, ? extends V> loader = valueLoader;
        return loader == null ? null : loader.apply(key);
    }
}
