package org.mapdb.store;

import org.mapdb.io.DataInput2;

/**
 * Push-down read action. The store resolves the recid to its current
 * representation under its own locks and dispatches exactly one of these methods.
 *
 * Rules: the handle is valid only during the call; do not mutate it;
 * do not call back into the store; return values are opaque longs passed through
 * bit-exactly by the store.
 *
 * Optimistic reads: a store may run the action WITHOUT a lock and
 * validate afterwards, so an action must additionally be
 * <ul>
 *   <li><b>re-invocable</b> — the store may call it more than once per read();
 *       every invocation must fully overwrite any result state it keeps;</li>
 *   <li><b>torn-tolerant</b> — on a lost race the bytes may be inconsistent
 *       garbage. The action may then throw any exception (the store retries under
 *       a real lock) but must not corrupt external state, loop unboundedly, or
 *       trust decoded lengths without bounds.</li>
 * </ul>
 * Results from a non-validated invocation are discarded by the store.
 */
public interface RecordRead {

    /** Record is byte-resident. Input positioned at content start; size = content length. */
    long onBytes(DataInput2 in, int size);

    /** Record is object-resident (heap store / materialized cache entry). */
    default long onObject(Object record) {
        throw new AssertionError("action does not support object handles: " + getClass().getName());
    }

    /** Record exists but is null (preallocated, or explicit null). */
    default long onNull() { return 0L; }
}
