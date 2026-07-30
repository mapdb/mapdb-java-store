/**
 * Immutable, bulk-loaded sorted tables over Store4 records.
 *
 * <p>{@link org.mapdb.sortedtable.SortedTableMap} builds fixed-size sorted
 * pages from an ascending input stream. Point lookups use an in-memory page
 * directory followed by byte-side {@link org.mapdb.ser.GroupFormat} search
 * within the selected record.
 *
 * @see org.mapdb.sortedtable.SortedTableMap.Sink
 * @see org.mapdb.store.RecordRead
 */
package org.mapdb.sortedtable;
