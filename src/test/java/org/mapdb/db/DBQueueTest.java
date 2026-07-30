package org.mapdb.db;

import org.mapdb.TmpFiles;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.mapdb.queue.PersistentBlockingQueue;
import org.mapdb.ser.Serializers;

public class DBQueueTest {
    @Test public void queueFamiliesPersistAndDispatch() throws Exception {
        File file = TmpFiles.tempFile("mapdb-queues", ".db");
        file.delete();
        try {
            DB db = DBMaker.fileDB(file).transactionEnable().make();
            db.queue("fifo", Serializers.STRING).create().add("first");
            db.stack("stack", Serializers.STRING).create().add("top");
            PersistentBlockingQueue<String> circular = db
                    .circularQueue("circular", Serializers.STRING, 2).create();
            circular.add("a"); circular.add("b"); circular.add("c");
            db.commit(); db.close();

            DB reopened = DBMaker.fileDB(file).transactionEnable().make();
            @SuppressWarnings("unchecked")
            PersistentBlockingQueue<String> fifo =
                    (PersistentBlockingQueue<String>) reopened.get("fifo");
            assertEquals("first", fifo.poll());
            @SuppressWarnings("unchecked")
            PersistentBlockingQueue<String> stack =
                    (PersistentBlockingQueue<String>) reopened.get("stack");
            assertEquals("top", stack.poll());
            @SuppressWarnings("unchecked")
            PersistentBlockingQueue<String> circular2 =
                    (PersistentBlockingQueue<String>) reopened.get("circular");
            assertEquals("b", circular2.poll());
            assertEquals("c", circular2.poll());
            reopened.close();
        } finally {
            file.delete();
            org.mapdb.store.WalTestKit.deleteStore(file);
        }
    }

    @Test public void dbCloseWakesBlockedConsumer() throws Exception {
        DB db = DBMaker.memoryDB().make();
        PersistentBlockingQueue<String> queue = db.queue("queue", Serializers.STRING).create();
        FutureTask<String> take = new FutureTask<>(queue::take);
        Thread thread = new Thread(take);
        thread.start();
        db.close();
        try {
            take.get(5, TimeUnit.SECONDS);
            throw new AssertionError("blocked take returned normally");
        } catch (ExecutionException expected) {
            org.junit.Assert.assertTrue(expected.getCause() instanceof org.mapdb.DBException.StoreClosed);
        }
        thread.join();
    }
}
