package org.mapdb.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.mapdb.ser.Serializers;

public class DBExpirationExecutorTest {

    @Test public void mapBackgroundExpiryAndCallerExecutorOwnership() throws Exception {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        try {
            CountDownLatch expired = new CountDownLatch(1);
            DB db = DBMaker.memoryDB().make();
            java.util.concurrent.ConcurrentMap<String, Long> map = db
                    .hashMap("cache", Serializers.STRING, Serializers.LONG)
                    .layout(0, 4, 8)
                    .expireAfterCreate(15)
                    .expireExecutor(executor)
                    .expireExecutorPeriod(5)
                    .modificationListener((key, oldValue, newValue, triggered) -> {
                        if (triggered && newValue == null) expired.countDown();
                    })
                    .create();
            map.put("a", 1L);
            assertTrue("background expiry did not run", expired.await(5, TimeUnit.SECONDS));
            assertEquals(0, map.size());
            db.close();
            assertFalse("DB must not close a caller-owned executor", executor.isShutdown());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test public void setBackgroundExpiryAttachesOnCreate() throws Exception {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        try {
            DB db = DBMaker.memoryDB().make();
            Set<String> set = db.hashSet("cache", Serializers.STRING)
                    .layout(0, 4, 8)
                    .expireAfterCreate(15)
                    .expireExecutor(executor)
                    .expireExecutorPeriod(5)
                    .create();
            set.add("a");
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (!set.isEmpty() && System.nanoTime() < deadline) Thread.yield();
            assertTrue("background set expiry did not run", set.isEmpty());
            db.close();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectZeroPeriod() {
        DB db = DBMaker.memoryDB().make();
        try {
            db.hashMap("x", Serializers.STRING, Serializers.LONG).expireExecutorPeriod(0);
        } finally {
            db.close();
        }
    }

    @Test public void listenerFailureDoesNotCancelFutureSweeps() throws Exception {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        try {
            AtomicInteger callbacks = new AtomicInteger();
            CountDownLatch second = new CountDownLatch(1);
            DB db = DBMaker.memoryDB().make();
            java.util.concurrent.ConcurrentMap<String, Long> map = db
                    .hashMap("cache", Serializers.STRING, Serializers.LONG)
                    .layout(0, 4, 8).expireAfterCreate(10)
                    .expireExecutor(executor).expireExecutorPeriod(5)
                    .modificationListener((key, oldValue, newValue, triggered) -> {
                        if (!triggered) return;
                        if (callbacks.incrementAndGet() == 1) throw new IllegalStateException("first");
                        second.countDown();
                    }).create();
            map.put("first", 1L);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (callbacks.get() == 0 && System.nanoTime() < deadline) Thread.yield();
            assertTrue(callbacks.get() >= 1);
            map.put("second", 2L);
            assertTrue("scheduled task was cancelled after listener failure",
                    second.await(5, TimeUnit.SECONDS));
            db.close();
        } finally { executor.shutdownNow(); }
    }
}
