package org.mapdb.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.PrimitiveIterator;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mapdb.DBException;
import org.mapdb.btree.BTreeMap;
import org.mapdb.ser.LongFormat;
import org.mapdb.ser.StringGroupFormat;
import org.mapdb.sortedtable.SortedTableMap;

/**
 * Tests for the DB-facade {@code sortedTableMap} maker (bulk-built, immutable sorted map).
 */
public class DBSortedTableMapTest {

    @Rule public TemporaryFolder tmp = new TemporaryFolder();

    private static String v(long i) { return "v" + i; }

    private static int recidCount(DB db) {
        int count = 0;
        PrimitiveIterator.OfLong recids = db.getStore().getAllRecids();
        while (recids.hasNext()) { recids.nextLong(); count++; }
        return count;
    }

    /** A sorted map of [0..n) -> "v<i>". */
    private static TreeMap<Long, String> sourceMap(int n) {
        TreeMap<Long, String> src = new TreeMap<>();
        for (long i = 0; i < n; i++) src.put(i, v(i));
        return src;
    }

    private static void assertAllReadable(NavigableMap<Long, String> m, int n) {
        assertEquals(n, m.size());
        for (long i = 0; i < n; i++) {
            assertEquals("value for key " + i, v(i), m.get(i));
            assertTrue("containsKey " + i, m.containsKey(i));
        }
        assertNull("absent key", m.get((long) n));
        assertFalse(m.containsKey((long) n));
        assertFalse(m.containsKey(-1L));
    }

    private static void assertNavigation(NavigableMap<Long, String> m, int n) {
        assertEquals((Long) 0L, m.firstKey());
        assertEquals((Long) (long) (n - 1), m.lastKey());
        // ceiling / floor around an interior gap-free range
        assertEquals((Long) 5L, m.ceilingKey(5L));
        assertEquals((Long) 5L, m.floorKey(5L));
        assertEquals((Long) 0L, m.ceilingKey(-1L));
        assertNull(m.ceilingKey((long) n));
        assertEquals((Long) (long) (n - 1), m.floorKey((long) n));
        assertNull(m.floorKey(-1L));
        assertEquals((Long) 6L, m.higherKey(5L));
        assertEquals((Long) 4L, m.lowerKey(5L));

        // headMap [0,10)
        NavigableMap<Long, String> head = m.headMap(10L, false);
        assertEquals(10, head.size());
        assertEquals((Long) 0L, head.firstKey());
        assertEquals((Long) 9L, head.lastKey());

        // tailMap [n-10, n)
        NavigableMap<Long, String> tail = m.tailMap((long) (n - 10), true);
        assertEquals(10, tail.size());
        assertEquals((Long) (long) (n - 10), tail.firstKey());
        assertEquals((Long) (long) (n - 1), tail.lastKey());

        // subMap [10,20)
        NavigableMap<Long, String> sub = m.subMap(10L, true, 20L, false);
        assertEquals(10, sub.size());
        assertEquals((Long) 10L, sub.firstKey());
        assertEquals((Long) 19L, sub.lastKey());
        assertEquals(v(15), sub.get(15L));
        assertNull(sub.get(20L));

        // full iteration order check
        long expected = 0;
        for (Map.Entry<Long, String> e : m.entrySet()) {
            assertEquals((Long) expected, e.getKey());
            assertEquals(v(expected), e.getValue());
            expected++;
        }
        assertEquals(n, expected);
    }

    // 1. createFrom(SortedMap)
    @Test public void createFromSortedMap() {
        DB db = DBMaker.memoryDB().make();
        int n = 500;
        SortedTableMap<Long, String> m = db.<Long, String>sortedTableMap("st",
                LongFormat.INSTANCE, StringGroupFormat.INSTANCE).createFrom(sourceMap(n));
        assertAllReadable(m, n);
        assertNavigation(m, n);
        assertEquals("SortedTableMap", db.getType("st"));
        db.close();
    }

    // 2. createFrom(Iterator)
    @Test public void createFromIterator() {
        DB db = DBMaker.memoryDB().make();
        int n = 500;
        Iterator<Map.Entry<Long, String>> it = sourceMap(n).entrySet().iterator();
        SortedTableMap<Long, String> m = db.<Long, String>sortedTableMap("st",
                LongFormat.INSTANCE, StringGroupFormat.INSTANCE).createFrom(it);
        assertAllReadable(m, n);
        assertNavigation(m, n);
        db.close();
    }

    // 3a. createFromSink happy path
    @Test public void createFromSinkAscending() {
        DB db = DBMaker.memoryDB().make();
        int n = 300;
        DB.SortedTableMapMaker<Long, String>.DbSink sink =
                db.<Long, String>sortedTableMap("st", LongFormat.INSTANCE, StringGroupFormat.INSTANCE)
                        .createFromSink();
        for (long i = 0; i < n; i++) sink.put(i, v(i));
        SortedTableMap<Long, String> m = sink.create();
        assertAllReadable(m, n);
        assertNavigation(m, n);
        assertEquals("SortedTableMap", db.getType("st"));
        db.close();
    }

    // 3b. non-ascending key on the sink throws NotSorted
    @Test public void sinkRejectsNonAscending() {
        DB db = DBMaker.memoryDB().make();
        DB.SortedTableMapMaker<Long, String>.DbSink sink =
                db.<Long, String>sortedTableMap("st", LongFormat.INSTANCE, StringGroupFormat.INSTANCE)
                        .createFromSink();
        sink.put(1L, "a");
        sink.put(2L, "b");
        // equal key (not strictly ascending) rejected
        assertThrows(DBException.NotSorted.class, () -> sink.put(2L, "dup"));
        // smaller key rejected
        assertThrows(DBException.NotSorted.class, () -> sink.put(0L, "back"));
        db.close();
    }

    // 3c. put after create() throws
    @Test public void sinkPutAfterCreateThrows() {
        DB db = DBMaker.memoryDB().make();
        DB.SortedTableMapMaker<Long, String>.DbSink sink =
                db.<Long, String>sortedTableMap("st", LongFormat.INSTANCE, StringGroupFormat.INSTANCE)
                        .createFromSink();
        sink.put(1L, "a");
        sink.create();
        assertThrows(IllegalStateException.class, () -> sink.put(2L, "b"));
        db.close();
    }

    // 3d. createFrom(Iterator) with mis-sorted input throws NotSorted
    @Test public void createFromUnsortedIteratorThrows() {
        DB db = DBMaker.memoryDB().make();
        List<Map.Entry<Long, String>> bad = new ArrayList<>();
        bad.add(new AbstractMap.SimpleImmutableEntry<>(5L, "a"));
        bad.add(new AbstractMap.SimpleImmutableEntry<>(3L, "b")); // out of order
        assertThrows(DBException.NotSorted.class,
                () -> db.<Long, String>sortedTableMap("st", LongFormat.INSTANCE, StringGroupFormat.INSTANCE)
                        .createFrom(bad.iterator()));
        db.close();
    }

    // 4. immutability
    @Test public void builtMapIsImmutable() {
        DB db = DBMaker.memoryDB().make();
        SortedTableMap<Long, String> m = db.<Long, String>sortedTableMap("st",
                LongFormat.INSTANCE, StringGroupFormat.INSTANCE).createFrom(sourceMap(10));
        assertThrows(UnsupportedOperationException.class, () -> m.put(1L, "x"));
        assertThrows(UnsupportedOperationException.class, () -> m.remove(1L));
        assertThrows(UnsupportedOperationException.class, () -> m.clear());
        assertThrows(UnsupportedOperationException.class, () -> m.putAll(sourceMap(3)));
        assertThrows(UnsupportedOperationException.class, m::pollFirstEntry);
        assertThrows(UnsupportedOperationException.class, m::pollLastEntry);
        db.close();
    }

    // 5. pageSize small and large read back identically
    @Test public void pageSizeVariationsReadIdentically() {
        DB db = DBMaker.memoryDB().make();
        int n = 130; // spans many pages at pageSize=4, few at pageSize=64
        SortedTableMap<Long, String> small = db.<Long, String>sortedTableMap("small",
                LongFormat.INSTANCE, StringGroupFormat.INSTANCE).pageSize(4).createFrom(sourceMap(n));
        SortedTableMap<Long, String> large = db.<Long, String>sortedTableMap("large",
                LongFormat.INSTANCE, StringGroupFormat.INSTANCE).pageSize(64).createFrom(sourceMap(n));

        assertAllReadable(small, n);
        assertAllReadable(large, n);
        assertNavigation(small, n);
        assertNavigation(large, n);

        // explicit cross-page-boundary probes for the pageSize=4 map (boundaries at 4,8,...)
        assertEquals(v(3), small.get(3L));
        assertEquals(v(4), small.get(4L)); // first key of page 1
        assertEquals(v(7), small.get(7L));
        assertEquals(v(8), small.get(8L)); // first key of page 2
        assertEquals((Long) 4L, small.ceilingKey(4L));
        assertEquals((Long) 3L, small.lowerKey(4L));

        assertEquals("4", db.getNameCatalog().get("small#entriesPerPage"));
        assertEquals("64", db.getNameCatalog().get("large#entriesPerPage"));
        db.close();
    }

    // 6. persistence/reopen + get() on a file-backed DB
    @Test public void persistAcrossReopen() throws Exception {
        File f = tmp.newFile("stm.db");
        f.delete();
        int n = 400;
        DB db = DBMaker.fileDB(f).transactionEnable().make();
        SortedTableMap<Long, String> m = db.<Long, String>sortedTableMap("st",
                LongFormat.INSTANCE, StringGroupFormat.INSTANCE).pageSize(16).createFrom(sourceMap(n));
        assertAllReadable(m, n);
        // catalog records type + entriesPerPage
        SortedMap<String, String> cat = db.getNameCatalog();
        assertEquals("SortedTableMap", cat.get("st#type"));
        assertEquals("16", cat.get("st#entriesPerPage"));
        db.commit();
        db.close();

        DB db2 = DBMaker.fileDB(f).transactionEnable().make();
        assertTrue(db2.exists("st"));
        // reopen via typed maker
        SortedTableMap<Long, String> m2 = db2.<Long, String>sortedTableMap("st",
                LongFormat.INSTANCE, StringGroupFormat.INSTANCE).open();
        assertAllReadable(m2, n);
        assertNavigation(m2, n);
        // reopen via untyped get() dispatch (built-in formats resolve without re-supply)
        @SuppressWarnings("unchecked")
        SortedTableMap<Long, String> m3 = (SortedTableMap<Long, String>) db2.get("st");
        assertAllReadable(m3, n);
        db2.close();
    }

    // 7. empty table
    @Test public void emptyTable() throws Exception {
        File f = tmp.newFile("empty.db");
        f.delete();
        DB db = DBMaker.fileDB(f).transactionEnable().make();
        SortedTableMap<Long, String> m = db.<Long, String>sortedTableMap("st",
                LongFormat.INSTANCE, StringGroupFormat.INSTANCE).create();
        assertEquals(0, m.size());
        assertTrue(m.isEmpty());
        assertNull(m.get(0L));
        assertNull(m.firstEntry());
        assertNull(m.lastEntry());
        assertFalse(m.entrySet().iterator().hasNext());
        db.commit();
        db.close();

        DB db2 = DBMaker.fileDB(f).transactionEnable().make();
        SortedTableMap<Long, String> m2 = db2.<Long, String>sortedTableMap("st",
                LongFormat.INSTANCE, StringGroupFormat.INSTANCE).open();
        assertEquals(0, m2.size());
        assertTrue(m2.isEmpty());
        db2.close();
    }

    // 8a. createFrom on an existing name throws WrongConfiguration
    @Test public void createFromExistingNameFails() {
        DB db = DBMaker.memoryDB().make();
        db.<Long, String>sortedTableMap("st", LongFormat.INSTANCE, StringGroupFormat.INSTANCE).create();
        int before = recidCount(db);
        assertThrows(DBException.WrongConfiguration.class,
                () -> db.<Long, String>sortedTableMap("st", LongFormat.INSTANCE, StringGroupFormat.INSTANCE)
                        .pageSize(4).createFrom(sourceMap(500)));
        assertEquals("duplicate createFrom must not allocate an orphan table", before, recidCount(db));
        // also createFromSink().create() on an existing name
        DB.SortedTableMapMaker<Long, String>.DbSink sink =
                db.<Long, String>sortedTableMap("st", LongFormat.INSTANCE, StringGroupFormat.INSTANCE)
                        .createFromSink();
        sink.put(1L, "a");
        assertThrows(DBException.WrongConfiguration.class, sink::create);
        db.close();
    }

    @Test public void sinkSnapshotsMakerConfiguration() {
        DB db = DBMaker.memoryDB().make();
        DB.SortedTableMapMaker<Long, String> maker = db.sortedTableMap(
                "st", LongFormat.INSTANCE, StringGroupFormat.INSTANCE).pageSize(4);
        DB.SortedTableMapMaker<Long, String>.DbSink sink = maker.createFromSink();
        maker.pageSize(64);
        for (long i = 0; i < 10; i++) sink.put(i, v(i));
        SortedTableMap<Long, String> map = sink.create();
        assertAllReadable(map, 10);
        assertEquals("4", db.getNameCatalog().get("st#entriesPerPage"));
        db.close();
    }

    @Test public void deleteFreesSortedTablePagesAndHeader() {
        DB db = DBMaker.memoryByteArrayDB().make();
        int baseline = recidCount(db);
        db.<Long, String>sortedTableMap("st", LongFormat.INSTANCE, StringGroupFormat.INSTANCE)
                .pageSize(4).createFrom(sourceMap(10));
        assertEquals("three pages plus one header", baseline + 4, recidCount(db));
        assertTrue(db.delete("st"));
        assertEquals(baseline, recidCount(db));
        assertFalse(db.exists("st"));
        db.close();
    }

    // 8b. opening a non-existent name throws
    @Test public void openMissingFails() {
        DB db = DBMaker.memoryDB().make();
        assertThrows(DBException.WrongConfiguration.class,
                () -> db.<Long, String>sortedTableMap("nope", LongFormat.INSTANCE, StringGroupFormat.INSTANCE).open());
        db.close();
    }

    // 8c. wrong-type reopen (treeMap opened as sortedTableMap) fails
    @Test public void wrongTypeReopenFails() {
        DB db = DBMaker.memoryDB().make();
        BTreeMap<Long, String> t = db.treeMap("x", LongFormat.INSTANCE, StringGroupFormat.INSTANCE).create();
        t.put(1L, "a");
        assertThrows(DBException.WrongConfiguration.class,
                () -> db.<Long, String>sortedTableMap("x", LongFormat.INSTANCE, StringGroupFormat.INSTANCE).open());
        db.close();
    }

    // 9. createOrOpen returns the cached singleton; built-in formats resolve on reopen via get()
    @Test public void createOrOpenAndBuiltinFormatResolution() throws Exception {
        File f = tmp.newFile("coo.db");
        f.delete();
        DB db = DBMaker.fileDB(f).transactionEnable().make();
        SortedTableMap<Long, String> a = db.<Long, String>sortedTableMap("st",
                LongFormat.INSTANCE, StringGroupFormat.INSTANCE).createOrOpen();
        Object b = db.<Long, String>sortedTableMap("st",
                LongFormat.INSTANCE, StringGroupFormat.INSTANCE).createOrOpen();
        assertTrue("createOrOpen must return the cached singleton", a == b);
        db.commit();
        db.close();

        // get() dispatch works with no re-supplied formats because both are registered built-ins
        DB db2 = DBMaker.fileDB(f).transactionEnable().make();
        Object m = db2.get("st");
        assertTrue(m instanceof SortedTableMap);
        db2.close();
    }
}
