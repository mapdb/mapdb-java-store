package org.mapdb.sortedtable;

import org.mapdb.store.Store;
import org.mapdb.store.StoreOnHeap;

/** Re-runs the whole correctness suite over a heap store (object-resident records → the
 *  {@code GetAction.onObject} push-down path). */
public class SortedTableMapOnHeapTest extends SortedTableMapTest {
    @Override protected Store newStore() { return new StoreOnHeap(); }
}
