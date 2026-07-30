/**
 * Positional and primitive index-tree collections over Store4.
 *
 * <p>{@link org.mapdb.indextree.IndexTreeList} maps list positions through a
 * sparse {@link org.mapdb.htree.DirTree} directory to element records.
 * {@link org.mapdb.indextree.IndexTreeLongLongMap} provides a primitive
 * {@code long -> long} map with a push-down read path.
 *
 * @see org.mapdb.store.Store
 */
package org.mapdb.indextree;
