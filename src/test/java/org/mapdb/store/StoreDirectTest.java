package org.mapdb.store;

public class StoreDirectTest extends StoreTCK {
    @Override protected Store createStore() { return new StoreDirect(); }
}
