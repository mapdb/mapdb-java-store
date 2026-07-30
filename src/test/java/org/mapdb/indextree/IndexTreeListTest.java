package org.mapdb.indextree;

import org.junit.Test;
import org.mapdb.TmpFiles;
import org.mapdb.ser.Serializers;
import org.mapdb.store.Store;
import org.mapdb.store.StoreByteArray;
import org.mapdb.store.StoreDirect;
import org.mapdb.store.StoreOnHeap;
import org.mapdb.store.StoreWAL;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * {@link IndexTreeList}: basic positional ops, insert/remove shifting, null elements,
 * iterator, a randomized model-based fuzz vs {@code ArrayList} (both stores), a
 * record-leak check (element records freed on remove/clear), and a reopen test.
 */
public class IndexTreeListTest {

    private interface Case { void run(Store store); }

    private static void onBothStores(Case c) {
        for (Store store : new Store[]{new StoreOnHeap(), new StoreByteArray(), new StoreDirect()}) {
            c.run(store);
            store.verify();
            store.close();
        }
    }

    private static long liveRecids(Store store) {
        long n = 0;
        for (PrimitiveIterator.OfLong it = store.getAllRecids(); it.hasNext(); it.nextLong()) n++;
        return n;
    }

    @Test public void basicOps() {
        onBothStores(store -> {
            IndexTreeList<String> list = IndexTreeList.create(store, Serializers.STRING);
            assertTrue(list.isEmpty());
            assertEquals(0, list.size());

            list.add("a");
            list.add("b");
            list.add("c");
            assertEquals(3, list.size());
            assertEquals("a", list.get(0));
            assertEquals("c", list.get(2));

            assertEquals("b", list.set(1, "B"));
            assertEquals("B", list.get(1));

            try { list.get(3); org.junit.Assert.fail(); }
            catch (IndexOutOfBoundsException expected) { /* ok */ }

            assertEquals("B", list.remove(1));
            assertEquals(2, list.size());
            assertEquals("a", list.get(0));
            assertEquals("c", list.get(1));

            list.clear();
            assertTrue(list.isEmpty());
        });
    }

    @Test public void positionalInsertShifts() {
        onBothStores(store -> {
            IndexTreeList<Integer> list = IndexTreeList.create(store, Serializers.INTEGER);
            List<Integer> oracle = new ArrayList<>();
            for (int i = 0; i < 10; i++) { list.add(i); oracle.add(i); }

            list.add(0, 100);  oracle.add(0, 100);   // insert at head
            list.add(5, 200);  oracle.add(5, 200);   // insert in middle
            list.add(list.size(), 300); oracle.add(oracle.size(), 300); // insert at end
            assertEquals(oracle, new ArrayList<>(list));

            // insert-at-size == append
            list.add(list.size(), 400); oracle.add(400);
            assertEquals(oracle, new ArrayList<>(list));

            try { list.add(list.size() + 1, 9); org.junit.Assert.fail(); }
            catch (IndexOutOfBoundsException expected) { /* ok */ }
        });
    }

    @Test public void positionalRemoveShifts() {
        onBothStores(store -> {
            IndexTreeList<Integer> list = IndexTreeList.create(store, Serializers.INTEGER);
            List<Integer> oracle = new ArrayList<>();
            for (int i = 0; i < 20; i++) { list.add(i); oracle.add(i); }

            assertEquals(oracle.remove(0), list.remove(0));    // head
            assertEquals(oracle.remove(9), list.remove(9));    // middle
            assertEquals(oracle.remove(oracle.size() - 1), list.remove(list.size() - 1)); // tail
            assertEquals(oracle, new ArrayList<>(list));
            for (int i = 0; i < oracle.size(); i++) assertEquals(oracle.get(i), list.get(i));
        });
    }

    @Test public void nullElements() {
        onBothStores(store -> {
            IndexTreeList<String> list = IndexTreeList.create(store, Serializers.STRING);
            list.add(null);
            list.add("x");
            list.add(null);
            assertEquals(3, list.size());
            assertNull(list.get(0));
            assertEquals("x", list.get(1));
            assertNull(list.get(2));
            list.add(1, null);
            assertNull(list.get(1));
            assertEquals("x", list.get(2));
        });
    }

    @Test public void iteratorAndRemove() {
        onBothStores(store -> {
            IndexTreeList<Integer> list = IndexTreeList.create(store, Serializers.INTEGER);
            for (int i = 0; i < 30; i++) list.add(i);
            int sum = 0;
            for (int v : list) sum += v;
            assertEquals(29 * 30 / 2, sum);

            // remove evens via iterator
            Iterator<Integer> it = list.iterator();
            while (it.hasNext()) if (it.next() % 2 == 0) it.remove();
            for (int v : list) assertTrue(v % 2 != 0);
            assertEquals(15, list.size());
        });
    }

    @Test public void removeFreesElementRecords() {
        onBothStores(store -> {
            IndexTreeList<Integer> list = IndexTreeList.create(store, Serializers.INTEGER);
            long base = liveRecids(store); // header + root + counter
            for (int i = 0; i < 500; i++) list.add(i);
            // element records + any DirTree subdir records are all freed on full drain
            for (int i = 0; i < 500; i++) list.remove(0);
            assertEquals("removeAt leaked element records", base, liveRecids(store));

            for (int i = 0; i < 300; i++) list.add(i);
            list.clear();
            assertEquals("clear leaked element records", base, liveRecids(store));
        });
    }

    @Test public void fuzzVsArrayList() {
        onBothStores(store -> {
            IndexTreeList<Integer> list = IndexTreeList.create(store, Serializers.INTEGER);
            List<Integer> oracle = new ArrayList<>();
            Random rnd = new Random(0x1157L);

            for (int i = 0; i < 4000; i++) {
                int roll = rnd.nextInt(100);
                int size = oracle.size();
                boolean forceInsert = size == 0 || (roll < 40 && size < 1200);
                if (forceInsert) {                         // insert at random position
                    int idx = rnd.nextInt(size + 1);
                    int val = rnd.nextInt(1_000_000);
                    list.add(idx, val);
                    oracle.add(idx, val);
                } else if (roll < 65) {                    // remove at random position
                    int idx = rnd.nextInt(size);
                    assertEquals("remove[" + i + "]", oracle.remove(idx), list.remove(idx));
                } else if (roll < 85) {                    // set
                    int idx = rnd.nextInt(size);
                    int val = rnd.nextInt(1_000_000);
                    assertEquals("set[" + i + "]", oracle.set(idx, val), list.set(idx, val));
                } else {                                   // get
                    int idx = rnd.nextInt(size);
                    assertEquals("get[" + i + "]", oracle.get(idx), list.get(idx));
                }
                if (i % 2000 == 0) assertEquals(oracle.size(), list.size());
            }
            assertEquals(oracle.size(), list.size());
            assertEquals(oracle, new ArrayList<>(list));
            store.verify();
        });
    }

    @Test public void committedContentSurvivesReopen() throws IOException {
        File file = TmpFiles.tempFile("indextree-list", ".db");
        file.delete();
        try {
            List<String> oracle = new ArrayList<>();
            long headerRecid;
            {
                StoreWAL store = new StoreWAL(file);
                IndexTreeList<String> list = IndexTreeList.create(store, Serializers.STRING);
                headerRecid = list.headerRecid();
                Random rnd = new Random(2024);
                for (int i = 0; i < 2000; i++) {
                    String v = "e" + rnd.nextInt();
                    int idx = rnd.nextInt(oracle.size() + 1);
                    list.add(idx, v);
                    oracle.add(idx, v);
                }
                for (int i = 0; i < 500; i++) {
                    int idx = rnd.nextInt(oracle.size());
                    assertEquals(oracle.remove(idx), list.remove(idx));
                }
                store.commit();
                store.verify();
                store.close();
            }
            {
                StoreWAL store = new StoreWAL(file);
                store.verify();
                IndexTreeList<String> list = IndexTreeList.open(store, headerRecid, Serializers.STRING);
                assertEquals(oracle.size(), list.size());
                assertEquals(oracle, new ArrayList<>(list));
                // still mutable after reopen
                list.add("tail");
                oracle.add("tail");
                assertEquals(oracle, new ArrayList<>(list));
                store.commit();
                store.verify();
                store.close();
            }
        } finally {
            file.delete();
        }
    }
}
