package org.mapdb.htree;

import org.mapdb.store.Store;
import org.mapdb.store.StoreAppendOnly;

/** HTreeMap48 TCK over {@link StoreAppendOnly} (lock-free SWMR + copying compaction). */
public class HTreeMap48AppendOnlyTest extends HTreeMap48TCK {
    @Override protected Store openStore() { return new StoreAppendOnly(); }
}
