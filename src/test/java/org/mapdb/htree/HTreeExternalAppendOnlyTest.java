package org.mapdb.htree;

import org.mapdb.store.Store;
import org.mapdb.store.StoreAppendOnly;

public class HTreeExternalAppendOnlyTest extends HTreeExternalTCK {
    @Override protected Store openStore() { return new StoreAppendOnly(); }
}
