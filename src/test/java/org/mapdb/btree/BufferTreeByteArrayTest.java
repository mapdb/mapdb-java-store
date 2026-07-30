package org.mapdb.btree;

import org.mapdb.store.StoreByteArray;
import org.mapdb.store.StoreDelta;

public class BufferTreeByteArrayTest extends BufferTreeMapTCK {
    @Override protected StoreDelta openStore() { return new StoreByteArray(); }
}
