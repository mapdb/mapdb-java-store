package org.mapdb.htree;

import org.mapdb.store.Store;
import org.mapdb.store.StoreByteArray;

public class HTreeExternalByteArrayTest extends HTreeExternalTCK {
    @Override protected Store openStore() { return new StoreByteArray(); }
}
