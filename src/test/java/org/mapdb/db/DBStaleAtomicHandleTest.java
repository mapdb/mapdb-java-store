package org.mapdb.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;
import org.mapdb.DBException;
import org.mapdb.btree.BTreeMap;
import org.mapdb.ser.LongFormat;
import org.mapdb.ser.Serializers;
import org.mapdb.ser.StringGroupFormat;
import org.mapdb.store.Store;
import org.mapdb.store.StoreDirect;
import org.mapdb.TmpFiles;

/**
 * astra25 23 F3: a stale {@link Atomic} handle kept after {@link DB#delete} or
 * {@link DB#rollback} held a recid the store reuses, so it silently read and wrote
 * another collection's record. Handles obtained from the DB are now invalidated and
 * throw {@link DBException.StoreClosed}; the collection that inherited the recid is
 * untouched.
 */
public class DBStaleAtomicHandleTest {

    @Test public void deleteInvalidatesEveryAtomicKind() {
        DB db = DBMaker.memoryDB().make();
        try {
            Atomic.Long l = db.atomicLong("l", 7L).create();
            Atomic.Integer i = db.atomicInteger("i").create();
            Atomic.Boolean b = db.atomicBoolean("b").create();
            Atomic.String s = db.atomicString("s").create();
            Atomic.Var<Long> v = db.atomicVar("v", Serializers.LONG).create();
            for (String n : new String[] {"l", "i", "b", "s", "v"}) assertTrue(db.delete(n));

            assertThrows(DBException.StoreClosed.class, l::get);
            assertThrows(DBException.StoreClosed.class, () -> l.set(-1L));
            assertThrows(DBException.StoreClosed.class, () -> l.compareAndSet(7L, 1L));
            assertThrows(DBException.StoreClosed.class, l::incrementAndGet);
            assertThrows(DBException.StoreClosed.class, i::get);
            assertThrows(DBException.StoreClosed.class, () -> i.set(1));
            assertThrows(DBException.StoreClosed.class, i::getAndIncrement);
            assertThrows(DBException.StoreClosed.class, b::get);
            assertThrows(DBException.StoreClosed.class, () -> b.set(true));
            assertThrows(DBException.StoreClosed.class, () -> b.compareAndSet(false, true));
            assertThrows(DBException.StoreClosed.class, s::get);
            assertThrows(DBException.StoreClosed.class, () -> s.set("x"));
            assertThrows(DBException.StoreClosed.class, () -> s.getAndSet("y"));
            assertThrows(DBException.StoreClosed.class, v::get);
            assertThrows(DBException.StoreClosed.class, () -> v.set(1L));
            assertThrows(DBException.StoreClosed.class, () -> v.compareAndSet(null, 1L));
        } finally {
            db.close();
        }
    }

    @Test public void staleHandleCannotReachTheAtomicThatReusedItsRecid() {
        DB db = DBMaker.memoryDB().make();
        try {
            Atomic.Long a = db.atomicLong("a", 7L).create();
            assertTrue(db.delete("a"));
            Atomic.Long b = db.atomicLong("b", 100L).create();
            // The probe in the review saw the recid reused here; whether or not the
            // store reuses it, the stale handle must not observe or write anything.
            assertThrows(DBException.StoreClosed.class, a::get);
            assertThrows(DBException.StoreClosed.class, () -> a.set(-1L));
            assertEquals(100L, b.get());
            // A fresh handle under the deleted name is a new, live atomic.
            Atomic.Long a2 = db.atomicLong("a", 9L).create();
            assertEquals(9L, a2.get());
            assertThrows(DBException.StoreClosed.class, a::get);
        } finally {
            db.close();
        }
    }

    @Test public void staleHandleCannotCorruptTreeCreatedAfterDelete() {
        DB db = DBMaker.memoryDB().make();
        try {
            Atomic.Long a = db.atomicLong("a", 7L).create();
            assertTrue(db.delete("a"));
            BTreeMap<Long, String> t = db.treeMap("t", LongFormat.INSTANCE, StringGroupFormat.INSTANCE).create();
            t.put(1L, "one");
            t.put(2L, "two");
            assertThrows(DBException.StoreClosed.class, () -> a.set(-1L));
            assertThrows(DBException.StoreClosed.class, () -> a.compareAndSet(0L, 5L));
            assertEquals("one", t.get(1L));
            assertEquals("two", t.get(2L));
            assertEquals(2, t.size());
        } finally {
            db.close();
        }
    }

    @Test public void rollbackInvalidatesAtomicHandles() throws Exception {
        File f = TmpFiles.tempFile("mapdb-stale-atomic", ".db");
        f.delete();
        try {
            DB db = DBMaker.fileDB(f).transactionEnable().make();
            try {
                Atomic.Long a = db.atomicLong("a", 7L).create();
                db.commit();
                a.set(8L);
                db.rollback();
                assertThrows(DBException.StoreClosed.class, a::get);
                assertThrows(DBException.StoreClosed.class, () -> a.set(-1L));
                Atomic.Long reopened = db.atomicLong("a").open();
                assertEquals(7L, reopened.get());
                // The reopened handle is live and independent of the stale one.
                reopened.set(11L);
                assertEquals(11L, db.atomicLong("a").open().get());
                assertThrows(DBException.StoreClosed.class, a::get);
            } finally {
                db.close();
            }
        } finally {
            f.delete();
        }
    }

    @Test public void closeInvalidatesAtomicHandles() {
        DB db = DBMaker.memoryDB().make();
        Atomic.Long a = db.atomicLong("a", 7L).create();
        db.close();
        assertThrows(DBException.StoreClosed.class, a::get);
        assertThrows(DBException.StoreClosed.class, () -> a.set(1L));
    }

    @Test public void liveHandlesAreUnaffected() {
        DB db = DBMaker.memoryDB().make();
        try {
            Atomic.Long a = db.atomicLong("a", 7L).create();
            Atomic.String s = db.atomicString("s").create();
            assertNull(s.get());
            assertTrue(db.delete("s"));
            assertEquals(7L, a.get());
            assertEquals(8L, a.incrementAndGet());
            assertTrue(a.compareAndSet(8L, 9L));
            assertFalse(a.compareAndSet(8L, 10L));
            assertEquals(9L, a.get());
        } finally {
            db.close();
        }
    }

    /**
     * Codex r1 P1: an operation that passed the handle's open check but had not yet reached
     * the store could land after {@code delete} freed the recid and another maker reused it.
     * The handle lock makes {@code closeHandle} wait for in-flight operations, and
     * {@code delete} closes the handle before freeing the record.
     */
    @Test public void deleteDrainsInFlightOperationBeforeFreeingTheRecid() throws Exception {
        StoreDirect real = new StoreDirect();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        AtomicReference<Thread> writer = new AtomicReference<>();
        Store store = (Store) Proxy.newProxyInstance(
                Store.class.getClassLoader(), new Class<?>[] {Store.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("update") && Thread.currentThread() == writer.get()) {
                        entered.countDown();
                        resume.await();
                    }
                    try {
                        return method.invoke(real, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
        DB db = new DB(store, true);
        Thread w = null;
        Thread d = null;
        try {
            Atomic.Long old = db.atomicLong("old", 7L).create();
            w = new Thread(() -> {
                try { old.set(-1L); } catch (RuntimeException ignore) { /* either outcome is fine */ }
            });
            writer.set(w);
            w.start();
            assertTrue(entered.await(10, TimeUnit.SECONDS));
            // The write is paused inside the store, past the handle's open check.
            d = new Thread(() -> db.delete("old"));
            d.start();
            d.join(500);
            assertTrue("delete must wait for the in-flight write before freeing the recid", d.isAlive());
            resume.countDown();
            w.join(10_000);
            d.join(10_000);
            assertFalse(d.isAlive());
            Atomic.Long fresh = db.atomicLong("fresh", 100L).create();
            assertEquals(100L, fresh.get());
            assertThrows(DBException.StoreClosed.class, old::get);
            assertThrows(DBException.StoreClosed.class, () -> old.set(5L));
        } finally {
            // Never leave the writer parked: a failed assertion above would otherwise
            // make db.close() wait forever for the handle's read lock.
            resume.countDown();
            if (w != null) w.join(10_000);
            if (d != null) d.join(10_000);
            db.close();
        }
    }
}
