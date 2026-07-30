package org.mapdb.btree;

import org.junit.Test;
import org.mapdb.ser.LongFormat;
import org.mapdb.store.StoreDelta;
import org.mapdb.store.StoreWAL;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class BufferTreeWALTest extends BufferTreeMapTCK {

    private File file;

    @Override
    protected StoreDelta openStore() {
        try {
            file = File.createTempFile("buffertree-wal-tck", ".wal");
            file.delete(); // StoreWAL will (re)create it
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new StoreWAL(file);
    }

    @Override
    protected void cleanup() {
        if (file != null) file.delete();
    }

    // ---------- durability / reopen (WAL-specific) ----------

    private static void assertMatches(BufferTreeMap<Long, Long> m, TreeMap<Long, Long> oracle) {
        assertEquals((long) oracle.size(), m.sizeLong());
        for (Map.Entry<Long, Long> e : oracle.entrySet()) {
            assertEquals("get " + e.getKey(), e.getValue(), m.get(e.getKey()));
        }
        Iterator<Map.Entry<Long, Long>> it = m.entryIterator();
        Iterator<Map.Entry<Long, Long>> ot = oracle.entrySet().iterator();
        while (ot.hasNext()) {
            assertTrue(it.hasNext());
            Map.Entry<Long, Long> me = it.next();
            Map.Entry<Long, Long> oe = ot.next();
            assertEquals(oe.getKey(), me.getKey());
            assertEquals(oe.getValue(), me.getValue());
        }
        assertFalse(it.hasNext());
    }

    /** Put data (buffers live), commit, close, reopen via {@link BufferTreeMap#open},
     *  verify committed content survives — with buffers unflushed at commit time. */
    @Test
    public void committedContentSurvivesReopen() throws IOException {
        File f = File.createTempFile("buffertree-wal-reopen", ".wal");
        f.delete();
        try {
            final TreeMap<Long, Long> oracle = new TreeMap<>();
            final long rrr;

            // ---- session 1: build (leave buffers UNflushed), commit, close ----
            {
                StoreWAL s = new StoreWAL(f);
                BufferTreeMap<Long, Long> m = BufferTreeMap.create(
                        s, LongFormat.INSTANCE, LongFormat.INSTANCE, 8, 64);
                rrr = m.rootRecidRecid();

                Random rnd = new Random(123);
                for (int i = 0; i < 20_000; i++) {
                    long k = rnd.nextInt(30_000);
                    long v = rnd.nextLong();
                    oracle.put(k, v);
                    m.put(k, v);
                }
                for (int i = 0; i < 4_000; i++) {
                    long k = rnd.nextInt(30_000);
                    oracle.remove(k);
                    m.remove(k);
                }
                assertMatches(m, oracle); // visible pre-commit
                s.commit();
                s.verify();
                s.close();
            }

            // ---- session 2: reopen, verify committed content (buffers replayed) ----
            {
                StoreWAL s = new StoreWAL(f);
                s.verify();
                BufferTreeMap<Long, Long> m = BufferTreeMap.open(
                        s, rrr, LongFormat.INSTANCE, LongFormat.INSTANCE, 8, 64);
                assertMatches(m, oracle);
                // still mutable & consistent after reopen
                m.put(999_999L, 7L);
                oracle.put(999_999L, 7L);
                m.flushAll();
                assertMatches(m, oracle);
                s.close();
            }
        } finally {
            f.delete();
        }
    }

    /** A commit issued AFTER flushAll (consolidated on disk) reopens identically. */
    @Test
    public void flushedThenCommittedSurvivesReopen() throws IOException {
        File f = File.createTempFile("buffertree-wal-reopen-flushed", ".wal");
        f.delete();
        try {
            final TreeMap<Long, Long> oracle = new TreeMap<>();
            final long rrr;
            {
                StoreWAL s = new StoreWAL(f);
                BufferTreeMap<Long, Long> m = BufferTreeMap.create(
                        s, LongFormat.INSTANCE, LongFormat.INSTANCE, 8, 64);
                rrr = m.rootRecidRecid();
                for (long k = 0; k < 10_000; k++) { m.put(k, k * 3); oracle.put(k, k * 3); }
                m.flushAll();
                assertMatches(m, oracle);
                s.commit();
                s.close();
            }
            {
                StoreWAL s = new StoreWAL(f);
                s.verify();
                BufferTreeMap<Long, Long> m = BufferTreeMap.open(
                        s, rrr, LongFormat.INSTANCE, LongFormat.INSTANCE, 8, 64);
                assertMatches(m, oracle);
                s.close();
            }
        } finally {
            f.delete();
        }
    }

    /** Mutations made after commit but NOT committed are lost on reopen. */
    @Test
    public void uncommittedMutationsAreLostAfterReopen() throws IOException {
        File f = File.createTempFile("buffertree-wal-uncommitted", ".wal");
        f.delete();
        try {
            final TreeMap<Long, Long> committed = new TreeMap<>();
            final long rrr;

            {
                StoreWAL s = new StoreWAL(f);
                BufferTreeMap<Long, Long> m = BufferTreeMap.create(
                        s, LongFormat.INSTANCE, LongFormat.INSTANCE, 8, 64);
                rrr = m.rootRecidRecid();
                for (long k = 0; k < 5_000; k++) { committed.put(k, k * 3); m.put(k, k * 3); }
                s.commit();
                s.close();
            }

            {
                StoreWAL s = new StoreWAL(f);
                BufferTreeMap<Long, Long> m = BufferTreeMap.open(
                        s, rrr, LongFormat.INSTANCE, LongFormat.INSTANCE, 8, 64);
                assertMatches(m, committed);
                // mutations that will NOT be committed
                for (long k = 5_000; k < 8_000; k++) m.put(k, -k);
                for (long k = 0; k < 2_000; k++) m.remove(k);
                for (long k = 2_000; k < 3_000; k++) m.put(k, 999_999L);
                // visible live
                assertEquals(Long.valueOf(-5_000L), m.get(5_000L));
                assertEquals(Long.valueOf(999_999L), m.get(2_000L));
                assertNull(m.get(0L));
                s.close(); // no commit
            }

            {
                StoreWAL s = new StoreWAL(f);
                s.verify();
                BufferTreeMap<Long, Long> m = BufferTreeMap.open(
                        s, rrr, LongFormat.INSTANCE, LongFormat.INSTANCE, 8, 64);
                assertMatches(m, committed);
                s.close();
            }
        } finally {
            f.delete();
        }
    }
}
