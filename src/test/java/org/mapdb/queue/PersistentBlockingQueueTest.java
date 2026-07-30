package org.mapdb.queue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.mapdb.ser.Serializers;
import org.mapdb.store.Store;
import org.mapdb.store.StoreByteArray;

public class PersistentBlockingQueueTest {
    @Test public void fifoStackCircularAndReopen() throws Exception {
        Store store = new StoreByteArray(true);
        PersistentBlockingQueue<String> fifo = PersistentBlockingQueue.create(
                store, Serializers.STRING, PersistentBlockingQueue.Mode.FIFO, Long.MAX_VALUE);
        fifo.addAll(Arrays.asList("a", "b", "c"));
        assertEquals("a", fifo.poll());
        assertTrue(fifo.remove("b"));
        assertEquals("c", fifo.peek());
        fifo.verify();
        PersistentBlockingQueue<String> reopened = PersistentBlockingQueue.open(
                store, fifo.headerRecid(), Serializers.STRING);
        assertEquals("c", reopened.take());
        assertNull(reopened.poll());

        PersistentBlockingQueue<String> stack = PersistentBlockingQueue.create(
                store, Serializers.STRING, PersistentBlockingQueue.Mode.LIFO, Long.MAX_VALUE);
        stack.addAll(Arrays.asList("a", "b", "c"));
        assertEquals("c", stack.poll());
        assertEquals("b", stack.poll());
        stack.verify();

        PersistentBlockingQueue<String> circular = PersistentBlockingQueue.create(
                store, Serializers.STRING, PersistentBlockingQueue.Mode.CIRCULAR, 3);
        circular.addAll(Arrays.asList("a", "b", "c", "d"));
        assertEquals(3, circular.size());
        assertEquals("b", circular.poll());
        assertEquals("c", circular.poll());
        assertEquals("d", circular.poll());
        circular.verify();
        store.close();
    }

    @Test public void blockingTakeWakesOnPut() throws Exception {
        Store store = new StoreByteArray(true);
        PersistentBlockingQueue<String> queue = PersistentBlockingQueue.create(
                store, Serializers.STRING, PersistentBlockingQueue.Mode.FIFO, Long.MAX_VALUE);
        FutureTask<String> take = new FutureTask<>(queue::take);
        Thread thread = new Thread(take);
        thread.start();
        assertFalse(take.isDone());
        queue.put("ready");
        assertEquals("ready", take.get(5, TimeUnit.SECONDS));
        thread.join();
        store.close();
    }
}
