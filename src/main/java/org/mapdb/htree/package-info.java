/**
 * Concurrent hash-tree maps and caches over Store4 records.
 *
 * <p>{@link org.mapdb.htree.HTreeMap} stores hash-partitioned buckets beneath
 * sparse {@link org.mapdb.htree.DirTree} directories.
 * {@link org.mapdb.htree.HTreeCache} adds expiration and size-based eviction.
 * Variants include {@link org.mapdb.htree.HTreeMap48} and
 * {@link org.mapdb.htree.HTreeMapExternal}.
 *
 * <p>Hashing is supplied by {@link org.mapdb.hash.Hasher}; keys and values are
 * encoded by {@link org.mapdb.ser.Serializer} and stored through
 * {@link org.mapdb.store.Store}.
 *
 * @see org.mapdb.htree.ExpireQueue
 * @see org.mapdb.store.RecordRead
 */
package org.mapdb.htree;
