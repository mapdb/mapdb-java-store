package org.mapdb.btree;

import org.mapdb.store.Store;
import org.mapdb.store.StoreByteArray;

public class BTreeByteArrayTest extends BTreeMapTCK {
    @Override protected Store openStore() { return new StoreByteArray(); }
}
