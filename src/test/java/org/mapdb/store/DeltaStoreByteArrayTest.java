package org.mapdb.store;

public class DeltaStoreByteArrayTest extends DeltaTCK {
    @Override protected StoreDelta createStore() { return new StoreByteArray(); }
}
