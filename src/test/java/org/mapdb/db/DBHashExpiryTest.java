package org.mapdb.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mapdb.DBException;
import org.mapdb.hash.Hashers;
import org.mapdb.htree.HTreeCache;
import org.mapdb.htree.HTreeMap;
import org.mapdb.htree.HTreeMapExternal;
import org.mapdb.ser.Serializers;

/**
 * Tests for the DB-facade hash-map / hash-set EXPIRY surface (routing to
 * {@link HTreeCache}, TTL / max-size / store-size eviction, overflow spillover,
 * eviction listener, persistence of the expiry config, and the mutual-exclusion
 * constraints). External-value + hashSeed routing lives in {@code DBHashExternalTest}.
 *
 * <p>TTL cases use short TTLs with generous sleep margins and, where a sweep is
 * needed to reclaim physically-present-but-expired entries, an explicit
 * {@code ((HTreeCache) map).expireEvict()} after sleeping. Capacity (max-size /
 * store-size) cases are clock-free and deterministic.
 */
public class DBHashExpiryTest {

    @Rule public final TemporaryFolder tmp = new TemporaryFolder();

    // ---- 1. routing / kind ----------------------------------------------------

    @Test public void plainHashMapIsHTreeMapNotCache() {
        DB db = DBMaker.memoryDB().make();
        ConcurrentMap<String, Long> m = db.hashMap("plain", Serializers.STRING, Serializers.LONG).create();
        assertTrue("plain hashMap must be an HTreeMap", m instanceof HTreeMap);
        assertFalse("plain hashMap must NOT be a cache", m instanceof HTreeCache);
        db.close();
    }

    @Test public void anyExpiryOptionRoutesToCache() {
        DB db = DBMaker.memoryDB().make();
        // an expiry option on create()
        ConcurrentMap<String, Long> m = db.hashMap("c", Serializers.STRING, Serializers.LONG)
                .expireMaxSize(1000).create();
        assertTrue("expiry option must route to HTreeCache", m instanceof HTreeCache);
        // and the untyped db.get(name) reopen path must also yield a cache
        Object reopened = db.get("c");
        assertTrue("db.get() of an expiring map must be an HTreeCache", reopened instanceof HTreeCache);
        assertTrue("db.get() must return the same live handle", reopened == m);
        db.close();
    }

    // ---- 2. max-size eviction (deterministic, no clock) -----------------------

    @Test public void maxSizeEvictionBoundsSizeAndFiresListener() {
        DB db = DBMaker.memoryDB().make();
        final long maxSize = 50;
        final AtomicInteger maxSizeEvictions = new AtomicInteger();
        final AtomicInteger otherEvictions = new AtomicInteger();
        // single segment (concShift 0) so the per-segment share == maxSize and the bound is tight.
        ConcurrentMap<String, Long> m = db.hashMap("cap", Serializers.STRING, Serializers.LONG)
                .layout(0, 4, 8)
                .expireMaxSize(maxSize)
                .expireEvictionListener((k, v, reason) -> {
                    if (reason == HTreeCache.EvictionReason.MAX_SIZE) maxSizeEvictions.incrementAndGet();
                    else otherEvictions.incrementAndGet();
                })
                .create();
        final int inserted = 2000;
        for (int i = 0; i < inserted; i++) m.put("k" + i, (long) i);

        HTreeCache<String, Long> cache = (HTreeCache<String, Long>) m;
        long size = cache.sizeLong();
        // per-segment approximate bound: a single segment may sit at share+1.
        assertTrue("size must stay bounded near maxSize, was " + size, size <= maxSize + 1);
        assertTrue("size must be non-trivial", size > 0);
        assertTrue("must not have grown unbounded", size < inserted);
        // the earliest keys are the oldest and must have been evicted.
        assertNull("oldest key must have been evicted", m.get("k0"));
        // the most recent keys survive.
        assertEquals((Long) (long) (inserted - 1), m.get("k" + (inserted - 1)));
        // listener fired with MAX_SIZE for (roughly) every over-capacity insert.
        assertTrue("MAX_SIZE listener must fire many times, got " + maxSizeEvictions.get(),
                maxSizeEvictions.get() >= inserted - (maxSize + 1) - 5);
        assertEquals("no non-capacity evictions expected", 0, otherEvictions.get());
        db.close();
    }

    // ---- 3. TTL expiry --------------------------------------------------------

    @Test public void expireAfterCreateRemovesAfterTtl() throws Exception {
        DB db = DBMaker.memoryDB().make();
        ConcurrentMap<String, Long> m = db.hashMap("ttl", Serializers.STRING, Serializers.LONG)
                .expireAfterCreate(300, TimeUnit.MILLISECONDS)
                .create();
        for (int i = 0; i < 5; i++) m.put("k" + i, (long) i);
        assertEquals("present immediately", (Long) 0L, m.get("k0"));
        assertEquals(5, ((HTreeCache<String, Long>) m).sizeLong());

        Thread.sleep(700); // > ttl, generous margin
        // strict reads treat expired entries as absent even before a sweep.
        assertNull("expired entry must read as absent", m.get("k0"));
        // now sweep to physically reclaim.
        ((HTreeCache<String, Long>) m).expireEvict();
        assertEquals("expireEvict must reclaim all expired entries", 0, ((HTreeCache<String, Long>) m).sizeLong());
        assertTrue(m.isEmpty());
        db.close();
    }

    @Test public void expireAfterGetKeepsAccessedEntryAlive() throws Exception {
        DB db = DBMaker.memoryDB().make();
        ConcurrentMap<String, Long> m = db.hashMap("acc", Serializers.STRING, Serializers.LONG)
                .expireAfterGet(400, TimeUnit.MILLISECONDS)
                .create();
        m.put("hot", 1L);

        // Keep touching within the TTL window; total elapsed (5*150=750ms) exceeds the
        // 400ms TTL, so survival proves access-order re-stamping.
        for (int i = 0; i < 5; i++) {
            Thread.sleep(150);
            assertEquals("access must keep the entry alive", (Long) 1L, m.get("hot"));
        }

        // stop touching, let it lapse, sweep.
        Thread.sleep(700);
        assertNull("untouched entry must expire", m.get("hot"));
        ((HTreeCache<String, Long>) m).expireEvict();
        assertTrue(m.isEmpty());
        db.close();
    }

    // ---- 4. store-size eviction (StoreDirect / memoryDirectDB) ----------------

    @Test public void storeSizeEvictionOnDirectStore() {
        DB db = DBMaker.memoryDirectDB().make();
        final AtomicInteger storeSizeEvictions = new AtomicInteger();
        // A tiny byte budget keeps the store perpetually over-budget, so every mutating
        // op sweeps oldest store-size victims from its segment.
        ConcurrentMap<String, Long> m = db.hashMap("ss", Serializers.STRING, Serializers.LONG)
                .expireStoreSize(1)
                .expireEvictionListener((k, v, reason) -> {
                    if (reason == HTreeCache.EvictionReason.STORE_SIZE) storeSizeEvictions.incrementAndGet();
                })
                .create();
        final int inserted = 500;
        for (int i = 0; i < inserted; i++) m.put("k" + i, (long) i);

        long size = ((HTreeCache<String, Long>) m).sizeLong();
        assertTrue("store-size eviction must fire", storeSizeEvictions.get() > 0);
        assertTrue("size must stay well below the inserted count, was " + size, size < inserted);
        db.close();
    }

    // ---- 5. persistence / reopen of an expiring cache -------------------------

    @Test public void cacheConfigSurvivesReopen() throws Exception {
        File f = tmp.newFile("cache.db");
        f.delete();
        final long maxSize = 1000;
        DB db = DBMaker.fileDB(f).transactionEnable().make();
        ConcurrentMap<String, Long> m = db.hashMap("c", Serializers.STRING, Serializers.LONG)
                .expireMaxSize(maxSize)
                .create();
        for (int i = 0; i < 10; i++) m.put("k" + i, (long) i);
        db.commit();
        db.close();

        DB db2 = DBMaker.fileDB(f).transactionEnable().make();
        ConcurrentMap<String, Long> m2 = db2.hashMap("c", Serializers.STRING, Serializers.LONG).open();
        assertTrue("reopened expiring map must still be an HTreeCache", m2 instanceof HTreeCache);
        assertEquals("committed entries must survive reopen", (Long) 5L, m2.get("k5"));

        // The maxSize config lives in the persisted header: keep inserting and confirm the
        // bound is still enforced after reopen (16 default segments => ~maxSize + segments).
        for (int i = 0; i < 8000; i++) m2.put("x" + i, (long) i);
        long size = ((HTreeCache<String, Long>) m2).sizeLong();
        assertTrue("persisted maxSize must still bound the map, was " + size, size <= maxSize + 16);
        db2.close();
    }

    // ---- 6. overflow spillover ------------------------------------------------

    @Test public void overflowReceivesCapacityVictimsAndBacksMisses() {
        DB db = DBMaker.memoryDB().make();
        final Map<String, Long> overflow = new HashMap<>();
        final long maxSize = 50;
        ConcurrentMap<String, Long> m = db.hashMap("ov", Serializers.STRING, Serializers.LONG)
                .layout(0, 4, 8)
                .expireMaxSize(maxSize)
                .expireOverflow(overflow)
                .create();
        final int inserted = 200;
        for (int i = 0; i < inserted; i++) m.put("k" + i, (long) i);

        // capacity (max-size) victims are still logically live and are handed to overflow.
        assertFalse("overflow must have received capacity victims", overflow.isEmpty());
        assertTrue("the oldest key must have spilled to overflow", overflow.containsKey("k0"));
        assertEquals("spilled value must be intact", (Long) 0L, overflow.get("k0"));

        // a get() MISS in the cache tier consults the overflow map and promotes the value.
        assertEquals("overflow must back a cache miss", (Long) 0L, m.get("k0"));
        db.close();
    }

    // ---- 7. constraint violations (DBException.WrongConfiguration) ------------

    @Test public void rejectWriteAndAccessTtlTogether() {
        DB db = DBMaker.memoryDB().make();
        assertThrows(DBException.WrongConfiguration.class, () ->
                db.hashMap("x", Serializers.STRING, Serializers.LONG)
                        .expireAfterCreate(100).expireAfterGet(100).create());
        db.close();
    }

    @Test public void rejectUnequalCreateAndUpdateTtl() {
        DB db = DBMaker.memoryDB().make();
        assertThrows(DBException.WrongConfiguration.class, () ->
                db.hashMap("x", Serializers.STRING, Serializers.LONG)
                        .expireAfterCreate(100).expireAfterUpdate(200).create());
        db.close();
    }

    @Test public void acceptEqualCreateAndUpdateTtl() {
        DB db = DBMaker.memoryDB().make();
        ConcurrentMap<String, Long> m = db.hashMap("x", Serializers.STRING, Serializers.LONG)
                .expireAfterCreate(500).expireAfterUpdate(500).create();
        assertTrue(m instanceof HTreeCache);
        db.close();
    }

    @Test public void rejectExternalWithExpiry() {
        DB db = DBMaker.memoryDB().make();
        assertThrows(DBException.WrongConfiguration.class, () ->
                db.hashMap("x", Serializers.STRING, Serializers.LONG)
                        .valueInline(false).expireMaxSize(100).create());
        db.close();
    }

    @Test public void rejectOverflowWithoutExpiry() {
        DB db = DBMaker.memoryDB().make();
        assertThrows(DBException.WrongConfiguration.class, () ->
                db.hashMap("x", Serializers.STRING, Serializers.LONG)
                        .expireOverflow(new HashMap<>()).create());
        db.close();
    }

    @Test public void rejectEvictionListenerWithoutExpiry() {
        DB db = DBMaker.memoryDB().make();
        assertThrows(DBException.WrongConfiguration.class, () ->
                db.hashMap("x", Serializers.STRING, Serializers.LONG)
                        .expireEvictionListener((k, v, r) -> { }).create());
        db.close();
    }

    @Test public void runtimeOverflowIsAppliedToCachedHandle() {
        DB db = DBMaker.memoryDB().make();
        ConcurrentMap<String, Long> created = db.hashMap("x", Serializers.STRING, Serializers.LONG)
                .expireMaxSize(4)
                .create();

        Map<String, Long> overflow = new HashMap<>();
        ConcurrentMap<String, Long> reopened = db.hashMap("x", Serializers.STRING, Serializers.LONG)
                .expireOverflow(overflow)
                .open();
        assertTrue("DB must retain one live handle", created == reopened);
        for (long i = 0; i < 100; i++) reopened.put("k" + i, i);
        assertFalse("cache-hit maker must attach overflow to the live cache", overflow.isEmpty());
        db.close();
    }

    @Test public void rejectInvalidExpiryLimits() {
        DB db = DBMaker.memoryDB().make();
        assertThrows(DBException.WrongConfiguration.class, () ->
                db.hashMap("a", Serializers.STRING, Serializers.LONG).expireMaxSize(-1));
        assertThrows(DBException.WrongConfiguration.class, () ->
                db.hashMap("b", Serializers.STRING, Serializers.LONG).expireStoreSize(-1));
        assertThrows(DBException.WrongConfiguration.class, () ->
                db.hashMap("c", Serializers.STRING, Serializers.LONG).expireMaxEvictPerOp(-1));
        assertThrows(DBException.WrongConfiguration.class, () ->
                db.hashMap("d", Serializers.STRING, Serializers.LONG)
                        .expireMaxEvictPerOp(1).create());
        db.close();
    }

    // ---- 8. hashSet expiry ----------------------------------------------------

    @Test public void hashSetExpiryRoutesToCacheAndReopens() throws Exception {
        File f = tmp.newFile("set.db");
        f.delete();
        DB db = DBMaker.fileDB(f).transactionEnable().make();
        // long TTL so the elements survive the test; max-size caps the set.
        Set<String> s = db.hashSet("hs", Serializers.STRING)
                .expireAfterCreate(60, TimeUnit.SECONDS)
                .expireMaxSize(1000)
                .create();
        assertTrue(s.add("a"));
        assertTrue(s.add("b"));
        assertTrue(s.contains("a"));
        assertFalse(s.add("a"));
        assertEquals("HashSet", db.getType("hs"));
        assertEquals("expiring set must be catalog-kind CACHE", "CACHE", db.getNameCatalog().get("hs#kind"));
        db.commit();
        db.close();

        DB db2 = DBMaker.fileDB(f).transactionEnable().make();
        @SuppressWarnings("unchecked")
        Set<String> s2 = (Set<String>) db2.get("hs");
        assertTrue("elements must survive reopen", s2.contains("a"));
        assertTrue(s2.contains("b"));
        assertTrue("reopened set must still be usable", s2.add("c"));
        db2.close();
    }

    // ---- 9. byte[] keys with a content hasher + expiry ------------------------

    @Test public void byteArrayKeysWithHasherAndExpiry() throws Exception {
        File f = tmp.newFile("ba.db");
        f.delete();
        DB db = DBMaker.fileDB(f).transactionEnable().make();
        ConcurrentMap<byte[], Long> m = db.hashMap("b", Serializers.BYTE_ARRAY, Serializers.LONG)
                .hasher(Hashers.mixing(java.util.Arrays::hashCode))
                .expireMaxSize(1000)
                .create();
        assertTrue("hasher + expiry must route to HTreeCache", m instanceof HTreeCache);
        m.put(new byte[]{1, 2, 3}, 42L);
        assertEquals("content-equal byte[] key must hit", (Long) 42L, m.get(new byte[]{1, 2, 3}));
        db.commit();
        db.close();

        DB db2 = DBMaker.fileDB(f).transactionEnable().make();
        ConcurrentMap<byte[], Long> m2 = db2.hashMap("b", Serializers.BYTE_ARRAY, Serializers.LONG)
                .hasher(Hashers.mixing(java.util.Arrays::hashCode))
                .open();
        assertTrue(m2 instanceof HTreeCache);
        assertEquals((Long) 42L, m2.get(new byte[]{1, 2, 3}));
        db2.close();
    }
}
