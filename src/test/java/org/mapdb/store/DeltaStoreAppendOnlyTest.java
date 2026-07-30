package org.mapdb.store;

public class DeltaStoreAppendOnlyTest extends DeltaTCK {
    @Override protected StoreDelta createStore() { return new StoreAppendOnly(); }
}
