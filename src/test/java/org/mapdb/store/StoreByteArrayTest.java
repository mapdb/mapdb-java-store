package org.mapdb.store;

public class StoreByteArrayTest extends StoreTCK {
    @Override protected Store createStore() { return new StoreByteArray(); }
}
