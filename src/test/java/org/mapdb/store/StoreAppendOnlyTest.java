package org.mapdb.store;

public class StoreAppendOnlyTest extends StoreTCK {
    @Override protected Store createStore() { return new StoreAppendOnly(); }
}
