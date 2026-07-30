package org.mapdb.htree;

import org.junit.Test;
import org.mapdb.DBException;
import org.mapdb.TmpFiles;
import org.mapdb.store.Store;
import org.mapdb.store.StoreByteArray;
import org.mapdb.store.StoreDirect;
import org.mapdb.store.StoreOnHeap;
import org.mapdb.store.StoreWAL;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * {@link ExpireQueue} unit tests, run over both an object store (StoreOnHeap) and a
 * byte store (StoreByteArray) — the node serializer only runs on the latter. The
 * queue's live-record baseline is 3 (tail/head/headPrev pointer records; the
 * preallocated head node is excluded from getAllRecids), which every case leak-gates.
 */
public class ExpireQueueTest {

    private static void onBothStores(Consumer<Store> test) {
        for (Store store : new Store[]{new StoreOnHeap(), new StoreByteArray()}) {
            try {
                test.accept(store);
                store.verify();
            } finally {
                store.close();
            }
        }
    }

    private static long liveRecidCount(Store store) {
        long n = 0;
        for (PrimitiveIterator.OfLong it = store.getAllRecids(); it.hasNext(); it.nextLong()) n++;
        return n;
    }

    /** Values oldest-first, via forEach. */
    private static List<Long> values(ExpireQueue q) {
        List<Long> out = new ArrayList<>();
        q.forEach((recid, node) -> out.add(node.value));
        return out;
    }

    @Test
    public void fifoPutTake() {
        onBothStores(store -> {
            ExpireQueue q = ExpireQueue.create(store);
            assertEquals(3, liveRecidCount(store));
            assertNull(q.take()); // empty
            for (long i = 1; i <= 5; i++) q.put(100 + i, i);
            q.verify();
            assertEquals(5, q.size());
            for (long i = 1; i <= 5; i++) {
                ExpireQueue.Node n = q.take();
                assertEquals(i, n.value);
                assertEquals(100 + i, n.timestamp);
            }
            assertNull(q.take());
            assertEquals(0, q.size());
            q.verify();
            assertEquals("take leaked", 3, liveRecidCount(store));
        });
    }

    @Test
    public void takeUntilStopsAtRefusal() {
        onBothStores(store -> {
            ExpireQueue q = ExpireQueue.create(store);
            for (long i = 1; i <= 6; i++) q.put(i * 10, i);
            List<Long> taken = new ArrayList<>();
            q.takeUntil((recid, node) -> {
                if (node.timestamp > 30) return false; // refuse 4th node
                taken.add(node.value);
                return true;
            });
            assertEquals(List.of(1L, 2L, 3L), taken);
            assertEquals(List.of(4L, 5L, 6L), values(q)); // refused node stays queued
            q.verify();
        });
    }

    @Test
    public void removeMiddleOldestNewest() {
        onBothStores(store -> {
            ExpireQueue q = ExpireQueue.create(store);
            long[] recids = new long[5];
            for (int i = 0; i < 5; i++) recids[i] = q.put(0, i + 1);

            ExpireQueue.Node mid = q.remove(recids[2], true); // middle
            assertEquals(3L, mid.value);
            assertEquals(List.of(1L, 2L, 4L, 5L), values(q));
            q.verify();

            assertEquals(1L, q.remove(recids[0], true).value); // oldest (tail)
            assertEquals(List.of(2L, 4L, 5L), values(q));
            q.verify();

            assertEquals(5L, q.remove(recids[4], true).value); // newest (headPrev)
            assertEquals(List.of(2L, 4L), values(q));
            q.verify();

            // puts still work after removals at every position
            q.put(0, 6);
            assertEquals(List.of(2L, 4L, 6L), values(q));
            q.verify();

            q.clear();
            assertEquals("remove/clear leaked", 3, liveRecidCount(store));
        });
    }

    @Test
    public void bumpMovesToHeadAndRestamps() {
        onBothStores(store -> {
            ExpireQueue q = ExpireQueue.create(store);
            long r1 = q.put(10, 1);
            q.put(20, 2);
            long r3 = q.put(30, 3);

            q.bump(r1, 40); // oldest -> newest
            assertEquals(List.of(2L, 3L, 1L), values(q));
            q.verify();

            q.bump(r1, 50); // already newest: restamp in place
            assertEquals(List.of(2L, 3L, 1L), values(q));
            List<Long> stamps = new ArrayList<>();
            q.forEach((recid, node) -> stamps.add(node.timestamp));
            assertEquals(List.of(20L, 30L, 50L), stamps);
            q.verify();

            q.bump(r3, 60); // middle -> newest
            assertEquals(List.of(2L, 1L, 3L), values(q));
            q.verify();

            // FIFO take order now reflects the bumps
            assertEquals(2L, q.take().value);
            assertEquals(1L, q.take().value);
            assertEquals(3L, q.take().value);
            assertNull(q.take());
            assertEquals(3, liveRecidCount(store));
        });
    }

    @Test
    public void singleNodeEdgeCases() {
        onBothStores(store -> {
            ExpireQueue q = ExpireQueue.create(store);
            long r = q.put(5, 42);
            q.bump(r, 7); // single node: newest AND oldest
            assertEquals(List.of(42L), values(q));
            q.verify();
            assertEquals(42L, q.remove(r, true).value); // remove the only node
            assertEquals(0, q.size());
            q.verify();
            assertEquals(3, liveRecidCount(store));

            // reuse after emptying via remove
            q.put(1, 7);
            assertEquals(7L, q.take().value);
            q.verify();
        });
    }

    @Test
    public void clearOnLargeQueue() {
        onBothStores(store -> {
            ExpireQueue q = ExpireQueue.create(store);
            for (long i = 0; i < 1_000; i++) q.put(i, i);
            assertEquals(1_000, q.size());
            q.clear();
            assertEquals(0, q.size());
            q.verify();
            assertEquals("clear leaked", 3, liveRecidCount(store));
        });
    }

    @Test
    public void directQueueSurvivesReopenAndKeepsNodeRecidsStable() throws IOException {
        File f = TmpFiles.tempFile("mapdb-expireq-direct", ".db");
        f.delete();
        long tail, head, headPrev, r1, r2, r3;
        try {
            StoreDirect s = new StoreDirect(f);
            try {
                ExpireQueue q = ExpireQueue.create(s);
                tail = q.tailRecid;
                head = q.headRecid;
                headPrev = q.headPrevRecid;
                r1 = q.put(10, 1);
                r2 = q.put(20, 2);
                r3 = q.put(30, 3);
                q.put(40, 4);
                q.bump(r2, 50); // stable recid moves to newest
                q.verify();
            } finally {
                s.close();
            }

            StoreDirect reopened = new StoreDirect(f);
            try {
                ExpireQueue q = new ExpireQueue(reopened, tail, head, headPrev);
                q.verify();
                assertEquals(List.of(1L, 3L, 4L, 2L), values(q));
                ExpireQueue.Node bumped = reopened.get(r2, ExpireQueue.Node.SER);
                assertEquals("bumped node kept its recid", 2L, bumped.value);
                assertEquals("bumped node kept its restamp", 50L, bumped.timestamp);
                q.remove(r3, true);
                q.bump(r1, 60);
                ExpireQueue.Node restamped = reopened.get(r1, ExpireQueue.Node.SER);
                assertEquals(1L, restamped.value);
                assertEquals(60L, restamped.timestamp);
                assertEquals(List.of(4L, 2L, 1L), values(q));
                assertEquals(4L, q.take().value);
                assertEquals(2L, q.take().value);
                assertEquals(1L, q.take().value);
                assertNull(q.take());
                q.verify();
                assertEquals("reopened direct queue leaked", 3, liveRecidCount(reopened));
                // removed/dequeued node recids can be reused by the store, but the
                // still-live node recid stayed addressable across reopen until removal.
                assertEquals(0, q.size());
            } finally {
                reopened.close();
            }

            StoreDirect empty = new StoreDirect(f);
            try {
                ExpireQueue q = new ExpireQueue(empty, tail, head, headPrev);
                q.verify();
                assertEquals(0, q.size());
                assertNull(q.take());
            } finally {
                empty.close();
            }
        } finally {
            f.delete();
        }
    }

    @Test
    public void walQueueSurvivesCommitCheckpointReopenAndClears() throws IOException {
        File f = TmpFiles.tempFile("mapdb-expireq-wal", ".wal");
        f.delete();
        long tail, head, headPrev, r2, r4;
        try {
            StoreWAL s = new StoreWAL(f);
            try {
                ExpireQueue q = ExpireQueue.create(s);
                tail = q.tailRecid;
                head = q.headRecid;
                headPrev = q.headPrevRecid;
                q.put(10, 1);
                r2 = q.put(20, 2);
                q.put(30, 3);
                r4 = q.put(40, 4);
                q.remove(r2, true);
                q.bump(r4, 50);
                assertEquals(List.of(1L, 3L, 4L), values(q));
                s.commit();
            } finally {
                s.close();
            }

            StoreWAL reopened = new StoreWAL(f);
            try {
                ExpireQueue q = new ExpireQueue(reopened, tail, head, headPrev);
                q.verify();
                assertEquals(List.of(1L, 3L, 4L), values(q));
                ExpireQueue.Node bumped = reopened.get(r4, ExpireQueue.Node.SER);
                assertEquals("live uncheckpointed replay kept bumped node recid", 4L, bumped.value);
                assertEquals(50L, bumped.timestamp);
                reopened.checkpoint();
            } finally {
                reopened.close();
            }

            StoreWAL checkpointed = new StoreWAL(f);
            try {
                ExpireQueue q = new ExpireQueue(checkpointed, tail, head, headPrev);
                q.verify();
                assertEquals(List.of(1L, 3L, 4L), values(q));
                ExpireQueue.Node bumped = checkpointed.get(r4, ExpireQueue.Node.SER);
                assertEquals("checkpoint replay kept bumped node recid", 4L, bumped.value);
                assertEquals(50L, bumped.timestamp);
                List<Long> taken = new ArrayList<>();
                q.takeUntil((recid, node) -> {
                    if (node.timestamp > 30) return false;
                    taken.add(node.value);
                    return true;
                });
                assertEquals(List.of(1L, 3L), taken);
                assertEquals(List.of(4L), values(q));
                q.clear();
                q.verify();
                assertEquals(3, liveRecidCount(checkpointed));
                checkpointed.commit();
            } finally {
                checkpointed.close();
            }

            StoreWAL empty = new StoreWAL(f);
            try {
                ExpireQueue q = new ExpireQueue(empty, tail, head, headPrev);
                q.verify();
                assertEquals(0, q.size());
                assertNull(q.take());
            } finally {
                empty.close();
            }
        } finally {
            f.delete();
            org.mapdb.store.WalTestKit.deleteStore(f);
        }
    }

    @Test
    public void removedButUndeletedNodeCannotBeBumpedBackIntoQueue() {
        onBothStores(store -> {
            ExpireQueue q = ExpireQueue.create(store);
            long r1 = q.put(10, 1);
            long orphan = q.put(20, 2);
            long r3 = q.put(30, 3);
            q.remove(orphan, false);
            assertEquals(List.of(1L, 3L), values(q));
            try {
                q.bump(orphan, 40);
                org.junit.Assert.fail("orphaned queue node was accepted by bump");
            } catch (DBException.DataCorruption expected) { /* ok */ }
            assertEquals(List.of(1L, 3L), values(q));
            q.verify();
            try {
                q.remove(orphan, true);
                org.junit.Assert.fail("orphaned queue node was accepted by remove");
            } catch (DBException.DataCorruption expected) { /* ok */ }
            assertEquals(List.of(1L, 3L), values(q));
            q.verify();
            q.remove(r1, true);
            q.remove(r3, true);
            store.delete(orphan, ExpireQueue.Node.SER);
            assertEquals(3, liveRecidCount(store));
        });
    }

    /**
     * Seeded random op-mix (put / take / takeUntil / remove / bump / clear) driven
     * against an in-JVM oracle: an ArrayList of [recid,timestamp,value] held in the
     * SAME oldest-first order the queue keeps. Timestamps are drawn monotonically
     * increasing (matching HTreeCache's single-fixed-TTL usage), so takeUntil with a
     * threshold predicate takes a genuine prefix and the oracle can mirror it exactly.
     * After every 256 ops the model is cross-checked via size()/verify()/forEach;
     * remove(deleteNode=false) intentionally orphans the record (tracked + deleted at
     * the end) so the final drain still leak-gates to the 3-record baseline.
     */
    @Test
    public void randomizedModelFuzz() {
        onBothStores(store -> {
            final long seed = 0x5EEDF1F0L;
            Random rnd = new Random(seed);
            ExpireQueue q = ExpireQueue.create(store);
            // oracle in queue order (oldest first); each entry is {recid, timestamp, value}
            List<long[]> oracle = new ArrayList<>();
            List<Long> orphans = new ArrayList<>(); // remove(deleteNode=false) leftovers
            long time = 1;
            final int OPS = 20_000;

            for (int i = 0; i < OPS; i++) {
                int roll = rnd.nextInt(100);
                if (roll < 35) { // put at the head with a fresh (monotone) timestamp
                    time += 1 + rnd.nextInt(5);
                    long value = rnd.nextLong();
                    long recid = q.put(time, value);
                    oracle.add(new long[]{recid, time, value});
                } else if (roll < 50) { // take the oldest
                    ExpireQueue.Node n = q.take();
                    if (oracle.isEmpty()) {
                        assertNull("take on empty[" + i + "]", n);
                    } else {
                        long[] e = oracle.remove(0);
                        assertEquals("take value[" + i + "]", e[2], n.value);
                        assertEquals("take stamp[" + i + "]", e[1], n.timestamp);
                    }
                } else if (roll < 65) { // takeUntil a threshold (prefix by monotone stamp)
                    long threshold = time - rnd.nextInt(12);
                    List<Long> taken = new ArrayList<>();
                    q.takeUntil((recid, node) -> {
                        if (node.timestamp <= threshold) { taken.add(recid); return true; }
                        return false;
                    });
                    List<Long> expTaken = new ArrayList<>();
                    while (!oracle.isEmpty() && oracle.get(0)[1] <= threshold) {
                        expTaken.add(oracle.remove(0)[0]);
                    }
                    assertEquals("takeUntil prefix[" + i + "]", expTaken, taken);
                } else if (roll < 80) { // remove a random live node (both deleteNode modes)
                    if (!oracle.isEmpty()) {
                        int idx = rnd.nextInt(oracle.size());
                        long[] e = oracle.remove(idx);
                        boolean deleteNode = rnd.nextBoolean();
                        ExpireQueue.Node n = q.remove(e[0], deleteNode);
                        assertEquals("remove value[" + i + "]", e[2], n.value);
                        if (!deleteNode) orphans.add(e[0]); // record stays behind on purpose
                    }
                } else if (roll < 95) { // bump a random live node to the head + restamp
                    if (!oracle.isEmpty()) {
                        int idx = rnd.nextInt(oracle.size());
                        long[] e = oracle.remove(idx);
                        time += 1 + rnd.nextInt(5);
                        q.bump(e[0], time);
                        e[1] = time;
                        oracle.add(e); // moved to newest, keeping the oracle monotone
                    }
                } else { // occasional full clear
                    q.clear();
                    oracle.clear();
                }
                if ((i & 255) == 255) assertModel(q, oracle);
            }

            assertModel(q, oracle);
            q.clear();
            oracle.clear();
            assertEquals(0, q.size());
            q.verify();
            // linked nodes are gone; only the 3 pointers + intentionally-orphaned records remain
            assertEquals("fuzz linked-node leak", 3 + orphans.size(), liveRecidCount(store));
            for (long r : orphans) store.delete(r, ExpireQueue.Node.SER);
            assertEquals("fuzz orphan leak", 3, liveRecidCount(store));
        });
    }

    /** Cross-check the queue against the oracle: structure, count and oldest-first order. */
    private static void assertModel(ExpireQueue q, List<long[]> oracle) {
        q.verify();
        assertEquals("size", oracle.size(), q.size());
        List<long[]> got = new ArrayList<>();
        q.forEach((recid, node) -> got.add(new long[]{recid, node.timestamp, node.value}));
        assertEquals("order length", oracle.size(), got.size());
        for (int i = 0; i < oracle.size(); i++) {
            assertEquals("recid@" + i, oracle.get(i)[0], got.get(i)[0]);
            assertEquals("stamp@" + i, oracle.get(i)[1], got.get(i)[1]);
            assertEquals("value@" + i, oracle.get(i)[2], got.get(i)[2]);
        }
    }
}
