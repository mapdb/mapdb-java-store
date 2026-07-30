package org.mapdb.htree;

import org.mapdb.store.Store;
import org.mapdb.store.StoreByteArray;

public class HTreeMap48ByteArrayTest extends HTreeMap48TCK {
    @Override protected Store openStore() { return new StoreByteArray(); }
}
