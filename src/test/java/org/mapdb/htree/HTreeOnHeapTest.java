package org.mapdb.htree;

import org.mapdb.store.Store;
import org.mapdb.store.StoreOnHeap;

public class HTreeOnHeapTest extends HTreeMapTCK {
    @Override protected Store openStore() { return new StoreOnHeap(); }
}
