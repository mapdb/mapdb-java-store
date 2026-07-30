/**
 * Reusable page-format components.
 *
 * <p>{@link org.mapdb.format.BufferedPageFormat} owns the buffered-page delta
 * framing used by {@link org.mapdb.btree.BufferTreeMap}: a base image followed
 * by reverse-readable PUT and DELETE entries. The store remains byte-blind while
 * the format owns framing, scanning, fingerprints, and integrity checks.
 *
 * @see org.mapdb.store.StoreDelta
 * @see org.mapdb.ser.GroupFormat
 */
package org.mapdb.format;
