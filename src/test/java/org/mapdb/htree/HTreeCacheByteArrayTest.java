package org.mapdb.htree;

import org.mapdb.store.Store;
import org.mapdb.store.StoreByteArray;

public class HTreeCacheByteArrayTest extends HTreeCacheTCK {
    @Override protected Store openStore() { return new StoreByteArray(); }
}
