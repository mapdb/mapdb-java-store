package org.mapdb.store;

public class DeltaStoreDirectTest extends DeltaTCK {
    @Override protected StoreDelta createStore() { return new StoreDirect(); }
}
