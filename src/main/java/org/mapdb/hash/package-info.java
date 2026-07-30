/**
 * External key hash functions for the hash-collection family.
 *
 * <p>{@link org.mapdb.hash.Hasher} and
 * {@link org.mapdb.hash.Hasher64} separate hashing policy from collection
 * layout. {@link org.mapdb.hash.Hashers} supplies common implementations,
 * including content hashing for array-like keys.
 *
 * @see org.mapdb.htree.HTreeMap
 * @see org.mapdb.ser.Serializer
 */
package org.mapdb.hash;
