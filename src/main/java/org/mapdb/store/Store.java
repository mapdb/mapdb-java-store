package org.mapdb.store;

import org.mapdb.ser.Serializer;

import java.util.PrimitiveIterator;

/**
 * Store4 core interface. Maps long recids to records. Structure-blind:
 * knows nothing about BTrees, keys or sort order.
 *
 * Edge-case semantics are defined by {@code StoreTCK}, which every concrete store runs;
 * that suite, not any prose document, is the contract.
 * Recid 0 is never allocated (universal "no link" sentinel).
 *
 * {@link AutoCloseable} so a store can be opened in a try-with-resources block; {@link #close()}
 * narrows away the interface's {@code throws Exception} (as {@code Closeable} does), because a
 * store reports failures as unchecked {@code DBException}.
 */
public interface Store extends AutoCloseable {

    /** Reserve a recid with null content (P state). get() returns null; update() fills it. */
    long preallocate();

    /** Batch variant; bulk-build fast path. */
    default void preallocate(int count, long[] into) {
        for (int i = 0; i < count; i++) into[i] = preallocate();
    }

    <R> long put(R record, Serializer<R> serializer);

    <R> R get(long recid, Serializer<R> serializer);

    /** Push-down read. Returns the action's return value unchanged. */
    long read(long recid, RecordRead action);

    <R> void update(long recid, R record, Serializer<R> serializer);

    /** Logical (serializer.equals) comparison, atomic per recid. */
    <R> boolean compareAndSwap(long recid, R expectedOldRecord, R newRecord, Serializer<R> serializer);

    <R> void delete(long recid, Serializer<R> serializer);

    /** Make preceding mutations durable (no-op for non-durable stores). */
    void commit();

    /** Reclaim obsolete storage where supported. */
    default void compact() {
        // no-op for stores without physical fragmentation
    }

    /** Narrows {@link AutoCloseable#close()}: failures surface as unchecked {@code DBException}. */
    @Override
    void close();

    boolean isClosed();

    /** Check store invariants; throws DBException.VerifyFailed. Called by TCK after every mutation. */
    void verify();

    /** Live recids; excludes preallocated records. */
    PrimitiveIterator.OfLong getAllRecids();

    /**
     * False when the store was opened in single-threaded mode: all locks are no-ops
     * and concurrent access is caller-error.
     */
    default boolean isThreadSafe() { return true; }

    /**
     * True when this store is a logically read-only view. Collections may use this to
     * suppress incidental write-on-read bookkeeping (for example access-order cache
     * queue bumps). Mutating Store methods remain responsible for rejecting writes.
     */
    default boolean isReadOnly() { return false; }

    /**
     * APPROXIMATE current byte footprint of the store (allocated bytes minus reclaimed
     * free space), for byte-budget cache eviction ({@code HTreeCache} storeSize). It
     * MUST decrease when records are deleted so a foreground eviction sweep converges,
     * but need not be exact. The default {@code 0} means "unsupported": a store that
     * cannot cheaply report its size disables store-size eviction (0 is never over any
     * positive budget). {@link StoreDirect} reports {@code fileTail - freeDataBytes}
     * (page-granular for fresh growth, record-granular for reused extents).
     */
    default long getCurrentSize() { return 0; }

    /**
     * Monotonic counter bumped whenever a structural revert (a {@code rollback})
     * may have invalidated a collection's cached structure — e.g. a {@code BTreeMap}'s
     * append-only left-edge spine, which is otherwise always current but can be left
     * TOO LONG by a rollback that shrinks the tree height. Non-tx stores never change
     * it. A collection caches the last-seen value and rebuilds its derived structure
     * only when this advances, so the common (no-rollback) path pays nothing. NOT a
     * commit counter: only reverts need to invalidate caches. The default {@code 0}
     * suits every non-transactional store; {@link StoreTx} implementations override it.
     */
    default long structuralGeneration() { return 0; }
}
