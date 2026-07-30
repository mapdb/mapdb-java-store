package org.mapdb.htree;

import org.mapdb.store.Store;
import org.mapdb.store.StoreOnHeap;

public class HTreeCacheOnHeapTest extends HTreeCacheTCK {
    @Override protected Store openStore() { return new StoreOnHeap(); }
}
