/**
 * The {@code DB} facade and {@code DBMaker} builder — the high-level entry point
 * ported from MapDB 3.
 *
 * <p>{@link org.mapdb.db.DBMaker} builds a {@link org.mapdb.db.DB} over one of
 * the {@link org.mapdb.store.Store} implementations. {@code DB} owns the store,
 * persists a small on-store <em>name catalog</em> (one record at recid&nbsp;1)
 * describing every named collection, and hands out {@code create()/open()/
 * createOrOpen()} makers for maps, sets, atomic variables and index-tree lists.
 *
 * <p>Unlike MapDB 3 there is no Elsa/POJO default serializer: every collection is
 * configured with explicit typed {@link org.mapdb.ser.Serializer} /
 * {@link org.mapdb.ser.GroupFormat} codecs, and only codecs registered in
 * {@link org.mapdb.db.SerializerRegistry} survive a reopen without being
 * re-supplied.
 */
package org.mapdb.db;
