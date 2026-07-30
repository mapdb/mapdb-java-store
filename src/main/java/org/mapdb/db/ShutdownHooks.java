package org.mapdb.db;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A single JVM shutdown hook that closes every {@link DB} registered with
 * {@link DBMaker#closeOnJvmShutdown()}. Registration is idempotent; a normal
 * {@link DB#close()} deregisters early. One inert hook is kept even when the set
 * empties (removing it would introduce a shutdown race).
 *
 * <p>There are two registries: a <b>strong</b> one ({@link #register}) that keeps the DB
 * reachable for the JVM's lifetime, and a <b>weak</b> one ({@link #registerWeak}) that
 * holds only {@link WeakReference}s so a forgotten DB can be garbage-collected without the
 * hook pinning it alive. The single shutdown pass closes everything still reachable in
 * either registry.
 */
final class ShutdownHooks {

    private ShutdownHooks() {}

    private static final Set<DB> DBS = java.util.Collections.newSetFromMap(new IdentityHashMap<>());
    private static final List<WeakReference<DB>> WEAK_DBS = new ArrayList<>();
    private static final AtomicBoolean HOOK_INSTALLED = new AtomicBoolean(false);

    static void register(DB db) {
        installHookOnce();
        synchronized (DBS) { DBS.add(db); }
    }

    static void deregister(DB db) {
        synchronized (DBS) { DBS.remove(db); }
    }

    /**
     * Register weakly: the DB is closed on JVM shutdown only if it is still reachable
     * (not yet GC'd). The registry never keeps the DB alive.
     */
    static void registerWeak(DB db) {
        installHookOnce();
        synchronized (WEAK_DBS) {
            // opportunistically drop cleared references so the list can't grow unbounded
            WEAK_DBS.removeIf(ref -> ref.get() == null);
            WEAK_DBS.add(new WeakReference<>(db));
        }
    }

    /** Remove a weakly-registered DB early (e.g. on normal close). */
    static void deregisterWeak(DB db) {
        synchronized (WEAK_DBS) {
            for (Iterator<WeakReference<DB>> it = WEAK_DBS.iterator(); it.hasNext(); ) {
                DB referent = it.next().get();
                if (referent == null || referent == db) it.remove();
            }
        }
    }

    private static void installHookOnce() {
        if (HOOK_INSTALLED.compareAndSet(false, true)) {
            Runtime.getRuntime().addShutdownHook(new Thread(ShutdownHooks::closeAll, "mapdb5-db-shutdown"));
        }
    }

    private static void closeAll() {
        DB[] snapshot;
        synchronized (DBS) { snapshot = DBS.toArray(new DB[0]); }
        for (DB db : snapshot) {
            try { db.close(); } catch (Throwable ignore) { /* isolate each close */ }
        }
        List<WeakReference<DB>> weakSnapshot;
        synchronized (WEAK_DBS) { weakSnapshot = new ArrayList<>(WEAK_DBS); }
        for (WeakReference<DB> ref : weakSnapshot) {
            DB db = ref.get();
            if (db == null) continue; // already GC'd: nothing to close
            try { db.close(); } catch (Throwable ignore) { /* isolate each close */ }
        }
    }
}
