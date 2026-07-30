package org.mapdb.db;

import org.mapdb.ser.Serializer;
import org.mapdb.ser.Serializers;
import org.mapdb.store.Store;

/**
 * Atomic scalar variables persisted as a single store record (one recid each).
 *
 * <p>Mutations go through the store and become durable only when the owning
 * {@link DB#commit()} runs. Compare-and-set maps to {@link Store#compareAndSwap};
 * the read-modify-write helpers ({@code incrementAndGet}, {@code getAndAdd}, …)
 * are CAS loops, so they are correct under concurrent DB access as long as the
 * store is thread-safe.
 */
public final class Atomic {

    private Atomic() {}

    /** Persistent {@code long}. */
    public static final class Long extends Number {
        private static final long serialVersionUID = 1L;
        private final Store store;
        private final long recid;

        public Long(Store store, long recid) { this.store = store; this.recid = recid; }

        public long getRecid() { return recid; }

        public long get() { return store.get(recid, Serializers.LONG); }

        public void set(long newValue) { store.update(recid, newValue, Serializers.LONG); }

        public boolean compareAndSet(long expect, long update) {
            return store.compareAndSwap(recid, expect, update, Serializers.LONG);
        }

        public long getAndSet(long newValue) {
            for (;;) {
                long cur = get();
                if (compareAndSet(cur, newValue)) return cur;
            }
        }

        public long getAndAdd(long delta) {
            for (;;) {
                long cur = get();
                if (compareAndSet(cur, cur + delta)) return cur;
            }
        }

        public long addAndGet(long delta) { return getAndAdd(delta) + delta; }
        public long incrementAndGet() { return addAndGet(1L); }
        public long decrementAndGet() { return addAndGet(-1L); }
        public long getAndIncrement() { return getAndAdd(1L); }
        public long getAndDecrement() { return getAndAdd(-1L); }

        @Override public int intValue() { return (int) get(); }
        @Override public long longValue() { return get(); }
        @Override public float floatValue() { return get(); }
        @Override public double doubleValue() { return get(); }

        @Override public java.lang.String toString() { return java.lang.Long.toString(get()); }
    }

    /** Persistent {@code int}. */
    public static final class Integer extends Number {
        private static final long serialVersionUID = 1L;
        private final Store store;
        private final long recid;

        public Integer(Store store, long recid) { this.store = store; this.recid = recid; }

        public long getRecid() { return recid; }

        public int get() { return store.get(recid, Serializers.INTEGER); }

        public void set(int newValue) { store.update(recid, newValue, Serializers.INTEGER); }

        public boolean compareAndSet(int expect, int update) {
            return store.compareAndSwap(recid, expect, update, Serializers.INTEGER);
        }

        public int getAndSet(int newValue) {
            for (;;) {
                int cur = get();
                if (compareAndSet(cur, newValue)) return cur;
            }
        }

        public int getAndAdd(int delta) {
            for (;;) {
                int cur = get();
                if (compareAndSet(cur, cur + delta)) return cur;
            }
        }

        public int addAndGet(int delta) { return getAndAdd(delta) + delta; }
        public int incrementAndGet() { return addAndGet(1); }
        public int decrementAndGet() { return addAndGet(-1); }
        public int getAndIncrement() { return getAndAdd(1); }
        public int getAndDecrement() { return getAndAdd(-1); }

        @Override public int intValue() { return get(); }
        @Override public long longValue() { return get(); }
        @Override public float floatValue() { return get(); }
        @Override public double doubleValue() { return get(); }

        @Override public java.lang.String toString() { return java.lang.Integer.toString(get()); }
    }

    /** Persistent {@code boolean}. */
    public static final class Boolean {
        private final Store store;
        private final long recid;

        public Boolean(Store store, long recid) { this.store = store; this.recid = recid; }

        public long getRecid() { return recid; }

        public boolean get() { return store.get(recid, DbSerializers.BOOLEAN); }

        public void set(boolean newValue) { store.update(recid, newValue, DbSerializers.BOOLEAN); }

        public boolean compareAndSet(boolean expect, boolean update) {
            return store.compareAndSwap(recid, expect, update, DbSerializers.BOOLEAN);
        }

        public boolean getAndSet(boolean newValue) {
            for (;;) {
                boolean cur = get();
                if (compareAndSet(cur, newValue)) return cur;
            }
        }

        @Override public java.lang.String toString() { return java.lang.Boolean.toString(get()); }
    }

    /** Persistent nullable {@link String}. */
    public static final class String {
        private final Store store;
        private final long recid;

        public String(Store store, long recid) { this.store = store; this.recid = recid; }

        public long getRecid() { return recid; }

        public java.lang.String get() { return store.get(recid, DbSerializers.STRING_NULLABLE); }

        public void set(java.lang.String newValue) { store.update(recid, newValue, DbSerializers.STRING_NULLABLE); }

        public boolean compareAndSet(java.lang.String expect, java.lang.String update) {
            return store.compareAndSwap(recid, expect, update, DbSerializers.STRING_NULLABLE);
        }

        public java.lang.String getAndSet(java.lang.String newValue) {
            for (;;) {
                java.lang.String cur = get();
                if (compareAndSet(cur, newValue)) return cur;
            }
        }

        @Override public java.lang.String toString() { return java.lang.String.valueOf(get()); }
    }

    /** Persistent arbitrary value with a caller-supplied serializer. */
    public static final class Var<E> {
        private final Store store;
        private final long recid;
        private final Serializer<E> serializer;

        public Var(Store store, long recid, Serializer<E> serializer) {
            this.store = store; this.recid = recid; this.serializer = serializer;
        }

        public long getRecid() { return recid; }
        public Serializer<E> serializer() { return serializer; }

        public E get() { return store.get(recid, serializer); }

        public void set(E newValue) { store.update(recid, newValue, serializer); }

        public boolean compareAndSet(E expect, E update) {
            return store.compareAndSwap(recid, expect, update, serializer);
        }

        public E getAndSet(E newValue) {
            for (;;) {
                E cur = get();
                if (compareAndSet(cur, newValue)) return cur;
            }
        }

        @Override public java.lang.String toString() { return java.lang.String.valueOf(get()); }
    }
}
