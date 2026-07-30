package org.mapdb.htree;

import org.mapdb.store.Store;
import org.mapdb.store.StoreAppendOnly;

/** HTreeMap TCK over {@link StoreAppendOnly} (lock-free SWMR + copying compaction).
 *  Exercises remove/clear record frees against a store that reclaims via GC-based
 *  compaction rather than a free list. */
public class HTreeAppendOnlyTest extends HTreeMapTCK {
    @Override protected Store openStore() { return new StoreAppendOnly(); }
}
