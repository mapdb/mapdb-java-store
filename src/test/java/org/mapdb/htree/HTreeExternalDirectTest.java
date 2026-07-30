package org.mapdb.htree;

import org.mapdb.store.Store;
import org.mapdb.store.StoreDirect;

public class HTreeExternalDirectTest extends HTreeExternalTCK {
    @Override protected Store openStore() { return new StoreDirect(); }
}
