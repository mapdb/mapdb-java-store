package org.mapdb.btree;

import org.mapdb.store.Store;
import org.mapdb.store.StoreOnHeap;

public class BTreeOnHeapTest extends BTreeMapTCK {
    @Override protected Store openStore() { return new StoreOnHeap(); }
}
