/**
 * Store4 record storage interfaces and implementations.
 *
 * <p>{@link org.mapdb.store.Store} is structure-blind: it maps recids to
 * opaque records and knows nothing about trees, keys, or ordering. Higher-level
 * collections push reads down through {@link org.mapdb.store.RecordRead}
 * actions and format-owned byte operations.
 *
 * <p>{@link org.mapdb.store.StoreDelta} adds capacity-aware append operations,
 * while {@link org.mapdb.store.StoreTx} adds transaction lifecycle methods.
 * Implementations include {@link org.mapdb.store.StoreOnHeap},
 * {@link org.mapdb.store.StoreByteArray},
 * {@link org.mapdb.store.StoreDirect}, and
 * {@link org.mapdb.store.StoreWAL}.
 *
 * @see org.mapdb.ser.GroupFormat
 * @see org.mapdb.io.DataInput2
 */
package org.mapdb.store;
