package org.mapdb.store;

public class StoreOnHeapTest extends StoreTCK {
    @Override protected Store createStore() { return new StoreOnHeap(); }
}
