package org.mapdb.htree;

import org.mapdb.store.Store;
import org.mapdb.store.StoreDirect;

public class HTreeCacheDirectTest extends HTreeCacheTCK {
    @Override protected Store openStore() { return new StoreDirect(); }
}
