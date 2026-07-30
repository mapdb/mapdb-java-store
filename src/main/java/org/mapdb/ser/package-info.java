/**
 * Element serializers and collection-owned group formats.
 *
 * <p>{@link org.mapdb.ser.Serializer} defines element encoding, ordering, and
 * logical equality. {@link org.mapdb.ser.GroupFormat} additionally owns the
 * representation and access algorithms for groups such as the sorted keys in a
 * BTree node. Its object-side and byte-side operations let collections search
 * store-resident data without materializing entire groups.
 *
 * <p>{@link org.mapdb.ser.Serializers} contains common element codecs;
 * specialized group formats include {@link org.mapdb.ser.LongFormat},
 * {@link org.mapdb.ser.StringGroupFormat}, and
 * {@link org.mapdb.ser.ObjectArrayFormat}.
 *
 * @see org.mapdb.store.RecordRead
 * @see org.mapdb.io.DataInput2
 */
package org.mapdb.ser;
