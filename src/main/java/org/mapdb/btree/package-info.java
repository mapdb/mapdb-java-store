/**
 * Ordered B-link tree collections over a Store4 store.
 *
 * <p>{@link org.mapdb.btree.BTreeMap} is the read-optimized implementation,
 * with lock-free readers and concurrent writers using per-recid node locks.
 * {@link org.mapdb.btree.BufferTreeMap} is its write-optimized buffered
 * companion; it uses {@link org.mapdb.store.StoreDelta} record headroom and
 * append operations to batch work down the tree.
 *
 * <p>Read paths demonstrate the push-down architecture by executing
 * {@link org.mapdb.store.RecordRead} actions against store-resident bytes and
 * delegating group access to {@link org.mapdb.ser.GroupFormat}.
 *
 * @see org.mapdb.btree.TreePump
 * @see org.mapdb.store.Store
 */
package org.mapdb.btree;
