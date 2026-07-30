/**
 * Positioned input and growable output primitives used by Store4 codecs.
 *
 * <p>{@link org.mapdb.io.DataInput2} is the seekable read cursor handed to
 * {@link org.mapdb.store.RecordRead} actions and byte-side format methods.
 * {@link org.mapdb.io.DataOutput2} is its matching serialization buffer.
 * Both support the packed encodings used by records and collection nodes.
 *
 * @see org.mapdb.ser.Serializer
 * @see org.mapdb.ser.GroupFormat
 */
package org.mapdb.io;
