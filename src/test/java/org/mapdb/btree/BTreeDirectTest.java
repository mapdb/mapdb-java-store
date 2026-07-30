package org.mapdb.btree;

import org.mapdb.store.Store;
import org.mapdb.store.StoreDirect;

public class BTreeDirectTest extends BTreeMapTCK {
    @Override protected Store openStore() { return new StoreDirect(); }
}
