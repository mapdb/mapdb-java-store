package org.mapdb.htree;

import org.mapdb.store.Store;
import org.mapdb.store.StoreDirect;

public class HTreeDirectTest extends HTreeMapTCK {
    @Override protected Store openStore() { return new StoreDirect(); }
}
