package org.mapdb.htree;

import org.mapdb.store.Store;
import org.mapdb.store.StoreDirect;

public class HTreeMap48DirectTest extends HTreeMap48TCK {
    @Override protected Store openStore() { return new StoreDirect(); }
}
