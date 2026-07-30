package org.mapdb.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mapdb.DBException;
import org.mapdb.btree.BTreeMap;
import org.mapdb.ser.LongFormat;
import org.mapdb.ser.StringGroupFormat;

/**
 * Tests for the DB-facade tree-map/tree-set features layered on top of {@link BTreeMap}:
 * {@code counterEnable()} (O(1) size), {@code modificationListener()}, bulk
 * {@code createFrom(...)}, and reopen/persistence of the counter through the name catalog.
 *
 * <p>Deterministic and single-threaded: listener assertions inspect the exact event set after
 * operations complete on one thread. File-backed DBs use a {@link TemporaryFolder}; every DB is
 * closed. The counter's persistence is proven two ways — the size is correct after reopen AND the
 * catalog records a non-zero {@code #counterRecid} that reopen reuses.
 */
public class DBTreeCounterListenerTest {

    @Rule public final TemporaryFolder tmp = new TemporaryFolder();

    /** A file path inside the temp folder that does not yet exist (StoreDirect wants a fresh file). */
    private File freshFile(String name) {
        return new File(tmp.getRoot(), name);
    }

    private static long iterCount(BTreeMap<?, ?> m) {
        long c = 0;
        for (var it = m.entryIterator(); it.hasNext(); ) { it.next(); c++; }
        return c;
    }

    private static final class Event {
        final long key; final String oldV; final String newV;
        Event(long key, String oldV, String newV) { this.key = key; this.oldV = oldV; this.newV = newV; }
    }

    private static void assertEvent(Event e, long key, String oldV, String newV) {
        assertEquals(key, e.key);
        assertEquals(oldV, e.oldV);
        assertEquals(newV, e.newV);
    }

    // ---------------- 1. counterEnable: size semantics ----------------

    @Test public void counterTracksInsertRemoveReplaceAndClear() {
        DB db = DBMaker.memoryDB().make();
        try {
            BTreeMap<Long, String> m = db.<Long, String>treeMap("c", LongFormat.INSTANCE, StringGroupFormat.INSTANCE)
                    .counterEnable().create();
            assertTrue("counter must be allocated", m.counterRecid() > 0);

            for (long i = 0; i < 100; i++) {
                assertNull(m.put(i, "v" + i));
                assertEquals(i + 1, m.sizeLong());
            }
            assertEquals(100, m.size());
            assertEquals(100L, m.sizeLong());
            assertEquals(iterCount(m), m.sizeLong());

            // update (existing key) does not change the counter
            assertEquals("v0", m.put(0L, "changed"));
            assertEquals(100L, m.sizeLong());
            // replace of existing key: no change
            assertEquals("changed", m.replace(0L, "again"));
            assertEquals(100L, m.sizeLong());
            // replace of absent key: no change
            assertNull(m.replace(1000L, "x"));
            assertEquals(100L, m.sizeLong());

            // removes decrement
            for (long i = 0; i < 40; i++) {
                assertTrue(m.remove(i) != null);
                assertEquals(100 - (i + 1), m.sizeLong());
            }
            assertEquals(60L, m.sizeLong());
            assertEquals(iterCount(m), m.sizeLong());
            // remove of absent key: no change
            assertNull(m.remove(0L));
            assertEquals(60L, m.sizeLong());

            // clear resets to 0
            m.clear();
            assertEquals(0L, m.sizeLong());
            assertEquals(0, m.size());
            assertEquals(0L, iterCount(m));
        } finally {
            db.close();
        }
    }

    @Test public void counterMapAndPlainMapReportIdenticalSize() {
        DB db = DBMaker.memoryDB().make();
        try {
            BTreeMap<Long, String> counted = db.<Long, String>treeMap("counted", LongFormat.INSTANCE, StringGroupFormat.INSTANCE)
                    .counterEnable().create();
            BTreeMap<Long, String> plain = db.<Long, String>treeMap("plain", LongFormat.INSTANCE, StringGroupFormat.INSTANCE)
                    .create();
            for (long i = 0; i < 250; i++) {
                counted.put(i, "v" + i);
                plain.put(i, "v" + i);
            }
            assertTrue("counted map uses the O(1) counter", counted.counterRecid() > 0);
            assertEquals(0L, plain.counterRecid());
            // O(1) counter parity vs traversal-fallback map: identical size
            assertEquals(plain.sizeLong(), counted.sizeLong());
            assertEquals(250L, counted.sizeLong());
            assertEquals(plain.size(), counted.size());
        } finally {
            db.close();
        }
    }

    // ---------------- 2. counter persistence across reopen ----------------

    @Test public void counterPersistsAcrossReopen() {
        File f = freshFile("counterPersist.db");
        long storedRecid;
        DB db = DBMaker.fileDB(f).transactionEnable().make();
        try {
            BTreeMap<Long, String> m = db.<Long, String>treeMap("t", LongFormat.INSTANCE, StringGroupFormat.INSTANCE)
                    .counterEnable().create();
            for (long i = 0; i < 500; i++) m.put(i, "v" + i);
            assertEquals(500L, m.sizeLong());
            // the counter recid is persisted in the catalog (non-zero) so reopen can reuse it
            String s = db.getNameCatalog().get("t#counterRecid");
            assertTrue("counterRecid persisted", s != null && Long.parseLong(s) > 0);
            storedRecid = Long.parseLong(s);
            db.commit();
        } finally {
            db.close();
        }

        DB db2 = DBMaker.fileDB(f).transactionEnable().make();
        try {
            // reopen reuses the persisted counterRecid unchanged — no re-scan surprise, O(1) size
            assertEquals("counterRecid must survive reopen unchanged",
                    Long.toString(storedRecid), db2.getNameCatalog().get("t#counterRecid"));
            BTreeMap<Long, String> re = db2.<Long, String>treeMap("t", LongFormat.INSTANCE, StringGroupFormat.INSTANCE)
                    .open();
            assertEquals(storedRecid, re.counterRecid());
            assertEquals(500L, re.sizeLong());           // counter reused, not recomputed
            // add more and confirm it keeps counting on top of the persisted count
            for (long i = 500; i < 600; i++) re.put(i, "v" + i);
            assertEquals(600L, re.sizeLong());
            db2.commit();
        } finally {
            db2.close();
        }
    }

    // ---------------- 3. counter NOT enabled: plain reopen ----------------

    @Test public void plainTreeMapReopensWithoutCounter() {
        File f = freshFile("plain.db");
        DB db = DBMaker.fileDB(f).transactionEnable().make();
        try {
            BTreeMap<Long, String> m = db.<Long, String>treeMap("p", LongFormat.INSTANCE, StringGroupFormat.INSTANCE)
                    .create();
            for (long i = 0; i < 300; i++) m.put(i, "v" + i);
            assertEquals(0L, m.counterRecid());
            assertEquals("0", db.getNameCatalog().get("p#counterRecid"));
            db.commit();
        } finally {
            db.close();
        }

        DB db2 = DBMaker.fileDB(f).transactionEnable().make();
        try {
            BTreeMap<Long, String> m2 = db2.<Long, String>treeMap("p", LongFormat.INSTANCE, StringGroupFormat.INSTANCE)
                    .open();
            assertEquals(0L, m2.counterRecid());          // counter absent -> 0 -> traversal size
            assertEquals(300L, m2.sizeLong());            // traversal fallback still correct
            assertEquals(300, m2.size());
            assertEquals("v42", m2.get(42L));
        } finally {
            db2.close();
        }
    }

    // ---------------- 4. modificationListener ----------------

    @Test public void modificationListenerCapturesInsertUpdateRemove() {
        DB db = DBMaker.memoryDB().make();
        try {
            List<Event> events = new ArrayList<>();
            BTreeMap<Long, String> m = db.<Long, String>treeMap("m", LongFormat.INSTANCE, StringGroupFormat.INSTANCE)
                    .modificationListener((k, o, n) -> events.add(new Event(k, o, n)))
                    .create();

            // via the Map view (BTreeMap IS the map view)
            Map<Long, String> view = m;
            view.put(5L, "fifty");                       // insert: old=null
            view.put(5L, "sixty");                       // update: old present
            assertEquals("sixty", view.remove(5L));      // remove: new=null

            assertEquals(3, events.size());
            assertEvent(events.get(0), 5L, null, "fifty");
            assertEvent(events.get(1), 5L, "fifty", "sixty");
            assertEvent(events.get(2), 5L, "sixty", null);
        } finally {
            db.close();
        }
    }

    @Test public void multipleModificationListenersAllFire() {
        DB db = DBMaker.memoryDB().make();
        try {
            List<Event> a = new ArrayList<>();
            List<Event> b = new ArrayList<>();
            BTreeMap<Long, String> m = db.<Long, String>treeMap("m", LongFormat.INSTANCE, StringGroupFormat.INSTANCE)
                    .modificationListener((k, o, n) -> a.add(new Event(k, o, n)))
                    .modificationListener((k, o, n) -> b.add(new Event(k, o, n)))
                    .create();

            m.put(1L, "one");
            m.put(1L, "uno");
            m.remove(1L);

            assertEquals(3, a.size());
            assertEquals(3, b.size());
            // both listeners observed the identical event sequence
            for (int i = 0; i < 3; i++) {
                assertEquals(a.get(i).key, b.get(i).key);
                assertEquals(a.get(i).oldV, b.get(i).oldV);
                assertEquals(a.get(i).newV, b.get(i).newV);
            }
            assertEvent(a.get(0), 1L, null, "one");
            assertEvent(a.get(1), 1L, "one", "uno");
            assertEvent(a.get(2), 1L, "uno", null);
        } finally {
            db.close();
        }
    }

    // ---------------- 5. TreeMap.createFrom (bulk) ----------------

    @Test public void treeMapCreateFromIteratorWithCounter() {
        DB db = DBMaker.memoryDB().make();
        try {
            int n = 2000;
            List<Map.Entry<Long, String>> entries = new ArrayList<>();
            for (long i = 0; i < n; i++) {
                entries.add(new AbstractMap.SimpleImmutableEntry<>(i, "v" + i));
            }
            BTreeMap<Long, String> m = db.<Long, String>treeMap("bulk", LongFormat.INSTANCE, StringGroupFormat.INSTANCE)
                    .counterEnable()
                    .createFrom(entries.iterator());

            assertTrue(m.counterRecid() > 0);
            assertEquals(n, m.sizeLong());               // counter equals bulk size
            assertEquals(iterCount(m), m.sizeLong());
            assertEquals((Long) 0L, m.firstKey());
            assertEquals((Long) (long) (n - 1), m.lastKey());
            assertEquals("v0", m.get(0L));
            assertEquals("v1999", m.get(1999L));
            // ordering: ascending scan
            long prev = -1;
            for (Long k : m.keySet()) {
                assertTrue("keys ascending", k > prev);
                prev = k;
            }
            // registered and dispatchable
            assertTrue(db.exists("bulk"));
            assertEquals("TreeMap", db.getType("bulk"));
            // counter keeps tracking after the bulk build
            assertNull(m.put((long) n, "new"));
            assertEquals(n + 1, m.sizeLong());
        } finally {
            db.close();
        }
    }

    @Test public void treeMapCreateFromSortedMap() {
        DB db = DBMaker.memoryDB().make();
        try {
            TreeMap<Long, String> src = new TreeMap<>();
            for (long i = 0; i < 500; i++) src.put(i, "v" + i);
            BTreeMap<Long, String> m = db.<Long, String>treeMap("fromSorted", LongFormat.INSTANCE, StringGroupFormat.INSTANCE)
                    .counterEnable()
                    .createFrom(src);
            assertEquals(500L, m.sizeLong());
            assertEquals(src.size(), m.size());
            for (Map.Entry<Long, String> e : src.entrySet()) {
                assertEquals(e.getValue(), m.get(e.getKey()));
            }
            assertEquals(src.firstKey(), m.firstKey());
            assertEquals(src.lastKey(), m.lastKey());
        } finally {
            db.close();
        }
    }

    @Test public void bulkCreatedCounterMapReopensFromCatalog() {
        File f = freshFile("bulkReopen.db");
        long counterRecid;
        DB db = DBMaker.fileDB(f).transactionEnable().make();
        try {
            List<Map.Entry<Long, String>> entries = new ArrayList<>();
            for (long i = 0; i < 750; i++) {
                entries.add(new AbstractMap.SimpleImmutableEntry<>(i, "v" + i));
            }
            BTreeMap<Long, String> m = db.<Long, String>treeMap(
                            "bulk", LongFormat.INSTANCE, StringGroupFormat.INSTANCE)
                    .counterEnable()
                    .createFrom(entries.iterator());
            counterRecid = m.counterRecid();
            assertTrue(counterRecid > 0);
            assertEquals(Long.toString(counterRecid),
                    db.getNameCatalog().get("bulk#counterRecid"));
            db.commit();
        } finally {
            db.close();
        }

        DB db2 = DBMaker.fileDB(f).transactionEnable().make();
        try {
            @SuppressWarnings("unchecked")
            BTreeMap<Long, String> m = (BTreeMap<Long, String>) db2.get("bulk");
            assertEquals(counterRecid, m.counterRecid());
            assertEquals(750L, m.sizeLong());
            assertEquals(750L, iterCount(m));
            assertEquals("v749", m.get(749L));
            m.remove(0L);
            assertEquals(749L, m.sizeLong());
        } finally {
            db2.close();
        }
    }

    @Test public void createFromExistingNameThrows() {
        DB db = DBMaker.memoryDB().make();
        try {
            db.<Long, String>treeMap("dup", LongFormat.INSTANCE, StringGroupFormat.INSTANCE).create();
            List<Map.Entry<Long, String>> entries = new ArrayList<>();
            entries.add(new AbstractMap.SimpleImmutableEntry<>(1L, "a"));
            assertThrows(DBException.WrongConfiguration.class,
                    () -> db.<Long, String>treeMap("dup", LongFormat.INSTANCE, StringGroupFormat.INSTANCE)
                            .createFrom(entries.iterator()));
        } finally {
            db.close();
        }
    }

    @Test public void createFromUnsortedThrowsNotSorted() {
        DB db = DBMaker.memoryDB().make();
        try {
            List<Map.Entry<Long, String>> bad = new ArrayList<>();
            bad.add(new AbstractMap.SimpleImmutableEntry<>(3L, "c"));
            bad.add(new AbstractMap.SimpleImmutableEntry<>(1L, "a")); // 1 < 3: not ascending
            bad.add(new AbstractMap.SimpleImmutableEntry<>(2L, "b"));
            assertThrows(DBException.NotSorted.class,
                    () -> db.<Long, String>treeMap("us", LongFormat.INSTANCE, StringGroupFormat.INSTANCE)
                            .createFrom(bad.iterator()));
            // the failed build did not register the collection
            assertFalse(db.exists("us"));
        } finally {
            db.close();
        }
    }

    // ---------------- 6. TreeSet: counter + createFrom ----------------

    @Test public void treeSetCounterSizeAndReopen() {
        File f = freshFile("treeset.db");
        DB db = DBMaker.fileDB(f).transactionEnable().make();
        try {
            NavigableSet<Long> s = db.<Long>treeSet("s", LongFormat.INSTANCE).counterEnable().create();
            for (long i = 0; i < 400; i++) s.add(i);
            assertEquals(400, s.size());
            assertEquals((Long) 0L, s.first());
            assertEquals((Long) 399L, s.last());
            // counter persisted in the catalog
            String recid = db.getNameCatalog().get("s#counterRecid");
            assertTrue("treeSet counterRecid persisted", recid != null && Long.parseLong(recid) > 0);
            db.commit();
        } finally {
            db.close();
        }

        DB db2 = DBMaker.fileDB(f).transactionEnable().make();
        try {
            NavigableSet<Long> s2 = db2.<Long>treeSet("s", LongFormat.INSTANCE).open();
            assertEquals(400, s2.size());                // survives reopen
            assertTrue(s2.contains(200L));
            s2.add(400L);
            assertEquals(401, s2.size());                // keeps counting
        } finally {
            db2.close();
        }
    }

    @Test public void treeSetCreateFromIterator() {
        DB db = DBMaker.memoryDB().make();
        try {
            TreeSet<Long> src = new TreeSet<>();
            for (long i = 0; i < 1000; i++) src.add(i * 2); // sorted, distinct
            NavigableSet<Long> s = db.<Long>treeSet("bulkset", LongFormat.INSTANCE)
                    .counterEnable()
                    .createFrom(src.iterator());

            assertEquals(1000, s.size());
            assertEquals((Long) 0L, s.first());
            assertEquals((Long) 1998L, s.last());
            assertTrue(s.contains(500L));
            assertFalse(s.contains(501L));
            // ordering ascending
            long prev = Long.MIN_VALUE;
            for (Long k : s) {
                assertTrue("elements ascending", k > prev);
                prev = k;
            }
            assertTrue(db.exists("bulkset"));
            assertEquals("TreeSet", db.getType("bulkset"));
        } finally {
            db.close();
        }
    }

    @Test public void treeSetCreateFromUnsortedThrows() {
        DB db = DBMaker.memoryDB().make();
        try {
            List<Long> bad = new ArrayList<>();
            bad.add(5L); bad.add(2L); // descending: not ascending
            assertThrows(DBException.NotSorted.class,
                    () -> db.<Long>treeSet("badset", LongFormat.INSTANCE).createFrom(bad.iterator()));
            assertFalse(db.exists("badset"));
        } finally {
            db.close();
        }
    }

    // ---------------- 7. get() / reopen dispatch ----------------

    @Test public void getDispatchReturnsWorkingCounterMap() {
        File f = freshFile("dispatch.db");
        DB db = DBMaker.fileDB(f).transactionEnable().make();
        try {
            BTreeMap<Long, String> m = db.<Long, String>treeMap("t", LongFormat.INSTANCE, StringGroupFormat.INSTANCE)
                    .counterEnable().create();
            for (long i = 0; i < 150; i++) m.put(i, "v" + i);
            db.commit();
        } finally {
            db.close();
        }

        DB db2 = DBMaker.fileDB(f).transactionEnable().make();
        try {
            @SuppressWarnings("unchecked")
            BTreeMap<Long, String> got = (BTreeMap<Long, String>) db2.get("t");
            assertTrue("reopened via get() must carry the counter", got.counterRecid() > 0);
            assertEquals(150L, got.sizeLong());
            assertEquals("v99", got.get(99L));
            got.put(150L, "v150");
            assertEquals(151L, got.sizeLong());
        } finally {
            db2.close();
        }
    }

    @Test public void listenerIsAppliedOnceToCachedHandle() {
        DB db = DBMaker.memoryDB().make();
        try {
            BTreeMap<Long, String> created = db
                    .treeMap("t", LongFormat.INSTANCE, StringGroupFormat.INSTANCE)
                    .create();
            List<String> events = new ArrayList<>();
            DB.TreeMapMaker<Long, String> maker = db
                    .treeMap("t", LongFormat.INSTANCE, StringGroupFormat.INSTANCE)
                    .modificationListener((key, oldValue, newValue) ->
                            events.add(key + ":" + oldValue + ":" + newValue));

            assertTrue(created == maker.open());
            assertTrue(created == maker.open()); // must not register the same listener twice
            created.put(1L, "one");
            assertEquals(java.util.Collections.singletonList("1:null:one"), events);
        } finally {
            db.close();
        }
    }
}
