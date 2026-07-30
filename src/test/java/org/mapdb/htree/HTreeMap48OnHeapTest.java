package org.mapdb.htree;

import org.mapdb.store.Store;
import org.mapdb.store.StoreOnHeap;

public class HTreeMap48OnHeapTest extends HTreeMap48TCK {
    @Override protected Store openStore() { return new StoreOnHeap(); }
}
