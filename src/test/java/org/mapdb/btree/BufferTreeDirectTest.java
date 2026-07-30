package org.mapdb.btree;

import org.mapdb.store.StoreDelta;
import org.mapdb.store.StoreDirect;

public class BufferTreeDirectTest extends BufferTreeMapTCK {
    @Override protected StoreDelta openStore() { return new StoreDirect(); }
}
