package org.mapdb.store;

import org.mapdb.ser.Serializer;

import java.util.PrimitiveIterator;

/**
 * A {@link Store} decorator that rejects every mutating operation, exposing a delegate
 * store as logically read-only. Read/inspection calls ({@link #get}, {@link #read},
 * {@link #getAllRecids}, {@link #verify}, {@link #isClosed}, {@link #isThreadSafe},
 * {@link #getCurrentSize}) pass straight through; mutators ({@link #put},
 * {@link #update}, {@link #delete}, {@link #compareAndSwap}, {@link #preallocate},
 * {@link #compact}) throw {@link UnsupportedOperationException}. {@link #commit()} is a
 * harmless no-op (a read-only DB may still call it) and {@link #close()} closes the
 * delegate.
 *
 * <h2>Logical guard, not an OS-level mode</h2>
 * This is a <b>logical</b> read-only guard: it rejects mutations at the {@link Store} API.
 * It does <b>not</b> change how the underlying store maps its backing file. mapdb5 file
 * stores still {@code mmap} the file {@code READ_WRITE} at the OS level; this wrapper does
 * not (and cannot from here) downgrade that mapping to {@code READ_ONLY}. That is
 * acceptable and matches the intended use: open an existing DB while forbidding writes
 * through the API. Do not rely on this wrapper to protect the file from every possible
 * write path — it guards the logical {@link Store} contract only.
 *
 * <p>This wrapper is not a {@link StoreTx}: a read-only view has nothing to roll back.
 */
public final class StoreReadOnlyWrapper implements StoreDelta {

    private static final String MSG = "store is read-only";

    private final Store delegate;

    public StoreReadOnlyWrapper(Store delegate) {
        if (delegate == null) throw new NullPointerException("delegate");
        this.delegate = delegate;
    }

    // ---- mutators: rejected -----------------------------------------------

    @Override public long preallocate() {
        throw new UnsupportedOperationException(MSG);
    }

    @Override public void preallocate(int count, long[] into) {
        throw new UnsupportedOperationException(MSG);
    }

    @Override public <R> long put(R record, Serializer<R> serializer) {
        throw new UnsupportedOperationException(MSG);
    }

    @Override public <R> void update(long recid, R record, Serializer<R> serializer) {
        throw new UnsupportedOperationException(MSG);
    }

    @Override public <R> boolean compareAndSwap(long recid, R expectedOldRecord, R newRecord, Serializer<R> serializer) {
        throw new UnsupportedOperationException(MSG);
    }

    @Override public <R> void delete(long recid, Serializer<R> serializer) {
        throw new UnsupportedOperationException(MSG);
    }

    @Override public void compact() {
        throw new UnsupportedOperationException(MSG);
    }

    @Override public long append(long recid, byte[] data, int offset, int len) {
        throw new UnsupportedOperationException(MSG);
    }

    @Override public <R> void updateWithHeadroom(long recid, R record,
                                                 Serializer<R> serializer, int headroom) {
        throw new UnsupportedOperationException(MSG);
    }

    // ---- reads / inspection: delegated ------------------------------------

    @Override public <R> R get(long recid, Serializer<R> serializer) {
        return delegate.get(recid, serializer);
    }

    @Override public long read(long recid, RecordRead action) {
        return delegate.read(recid, action);
    }

    @Override public PrimitiveIterator.OfLong getAllRecids() {
        return delegate.getAllRecids();
    }

    @Override public void verify() {
        delegate.verify();
    }

    @Override public boolean isClosed() {
        return delegate.isClosed();
    }

    @Override public boolean isThreadSafe() {
        return delegate.isThreadSafe();
    }

    @Override public boolean isReadOnly() {
        return true;
    }

    @Override public long getCurrentSize() {
        return delegate.getCurrentSize();
    }

    @Override public long capacityRemaining(long recid) {
        return delegate instanceof StoreDelta
                ? ((StoreDelta) delegate).capacityRemaining(recid) : 0L;
    }

    // ---- lifecycle --------------------------------------------------------

    /** No-op: a read-only view has nothing to make durable. Tolerated so callers may commit(). */
    @Override public void commit() {
        // intentionally empty
    }

    /** Closes the underlying store so its resources (and any afterClose cleanup) are released. */
    @Override public void close() {
        delegate.close();
    }
}
