package org.mapdb.store;

/** Transactional capability. */
public interface StoreTx extends Store {

    /** Discard all uncommitted mutations, including appends. */
    void rollback();
}
