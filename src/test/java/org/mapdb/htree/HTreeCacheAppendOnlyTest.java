package org.mapdb.htree;

import org.mapdb.store.Store;
import org.mapdb.store.StoreAppendOnly;

public class HTreeCacheAppendOnlyTest extends HTreeCacheTCK {
    @Override protected Store openStore() { return new StoreAppendOnly(); }
}
