package org.mapdb;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.mapdb.store.Store;
import org.mapdb.store.StoreOnHeap;
import org.mapdb.store.StoreByteArray;
import org.mapdb.store.StoreDirect;

public class QueueLongTest {

    @Test public void fifoRemoveBumpAndReopen() {
        for (Store store : Arrays.asList(new StoreOnHeap(true), new StoreByteArray(true),
                new StoreDirect(false, true))) {
            QueueLong queue = QueueLong.make(store);
            long a = queue.put(10, 1);
            long b = queue.put(20, 2);
            long c = queue.put(30, 3);
            assertArrayEquals(new long[]{1, 2, 3}, queue.valuesArray());

            assertEquals(2, queue.remove(b, true).value);
            assertArrayEquals(new long[]{1, 3}, queue.valuesArray());
            queue.bump(a, 40);
            assertArrayEquals(new long[]{3, 1}, queue.valuesArray());
            queue.verify();

            QueueLong reopened = new QueueLong(store, queue.tailRecid(), queue.headRecid(),
                    queue.headPrevRecid());
            assertEquals(c, reopened.tail());
            assertEquals(3, reopened.take().value);
            assertEquals(1, reopened.take().value);
            assertNull(reopened.take());
            reopened.verify();
            store.close();
        }
    }

    @Test public void takeUntilAndForEach() {
        Store store = new StoreOnHeap(true);
        QueueLong queue = QueueLong.make(store);
        queue.put(10, 1);
        queue.put(20, 2);
        queue.put(30, 3);
        queue.takeUntil((recid, node) -> node.timestamp <= 20);
        assertArrayEquals(new long[]{3}, queue.valuesArray());

        List<Long> seen = new ArrayList<>();
        queue.forEach((recid, value, timestamp) -> seen.addAll(Arrays.asList(value, timestamp)));
        assertEquals(Arrays.asList(3L, 30L), seen);
        queue.clear();
        assertEquals(0, queue.size());
        queue.verify();
        store.close();
    }

    @Test public void insertPreallocatedNode() {
        Store store = new StoreOnHeap(true);
        QueueLong queue = QueueLong.make(store);
        long recid = store.preallocate();
        queue.put(7, 9, recid);
        assertEquals(recid, queue.tail());
        assertArrayEquals(new long[]{9}, queue.valuesArray());
        queue.verify();
        store.close();
    }
}
