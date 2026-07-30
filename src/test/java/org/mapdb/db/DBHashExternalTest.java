package org.mapdb.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mapdb.DBException;
import org.mapdb.hash.Hasher;
import org.mapdb.hash.Hashers;
import org.mapdb.htree.HTreeCache;
import org.mapdb.htree.HTreeMap;
import org.mapdb.htree.HTreeMapExternal;
import org.mapdb.ser.Serializers;

/**
 * Tests for the DB-facade EXTERNAL-value hash map ({@code valueInline(false)} →
 * {@link HTreeMapExternal}) and the explicit {@code hashSeed(int)} option (persisted
 * in the map header and reconstructed on open).
 */
public class DBHashExternalTest {

    @Rule public final TemporaryFolder tmp = new TemporaryFolder();

    private static final class RecordingStringHasher implements Hasher<String> {
        final AtomicInteger lastSeed = new AtomicInteger();

        @Override public int hash(String key, int seed) {
            lastSeed.set(seed);
            return Hashers.fmix32(key.hashCode() ^ seed);
        }
    }

    // ---- routing --------------------------------------------------------------

    @Test public void valueInlineFalseRoutesToExternal() {
        DB db = DBMaker.memoryDB().make();
        ConcurrentMap<String, Long> m = db.hashMap("e", Serializers.STRING, Serializers.LONG)
                .valueInline(false).create();
        assertTrue("valueInline(false) must be an HTreeMapExternal", m instanceof HTreeMapExternal);
        assertFalse("external map is not a plain inline HTreeMap", m instanceof HTreeMap);
        assertFalse("external map is not a cache", m instanceof HTreeCache);
        assertEquals("EXTERNAL", db.getNameCatalog().get("e#kind"));
        db.close();
    }

    @Test public void explicitHashSeedIsPassedToEveryConcreteMapKind() {
        DB db = DBMaker.memoryDB().make();
        final int seed = 0x1234ABCD;

        RecordingStringHasher plainHasher = new RecordingStringHasher();
        ConcurrentMap<String, Long> plain = db.hashMap("plain", Serializers.STRING, Serializers.LONG)
                .hasher(plainHasher).hashSeed(seed).create();
        plain.put("plain-key", 1L);
        assertTrue(plain instanceof HTreeMap);
        assertEquals(seed, plainHasher.lastSeed.get());
        assertEquals("PLAIN", db.getNameCatalog().get("plain#kind"));

        RecordingStringHasher cacheHasher = new RecordingStringHasher();
        ConcurrentMap<String, Long> cache = db.hashMap("cache", Serializers.STRING, Serializers.LONG)
                .hasher(cacheHasher).hashSeed(seed).expireMaxSize(100).create();
        cache.put("cache-key", 2L);
        assertTrue(cache instanceof HTreeCache);
        assertEquals(seed, cacheHasher.lastSeed.get());
        assertEquals("CACHE", db.getNameCatalog().get("cache#kind"));

        RecordingStringHasher externalHasher = new RecordingStringHasher();
        ConcurrentMap<String, Long> external = db.hashMap("external", Serializers.STRING, Serializers.LONG)
                .hasher(externalHasher).hashSeed(seed).valueInline(false).create();
        external.put("external-key", 3L);
        assertTrue(external instanceof HTreeMapExternal);
        assertEquals(seed, externalHasher.lastSeed.get());
        assertEquals("EXTERNAL", db.getNameCatalog().get("external#kind"));
        db.close();
    }

    // ---- external-value round trip + reopen -----------------------------------

    @Test public void externalKindSurvivesReopenWithValuesIntact() throws Exception {
        File f = tmp.newFile("ext.db");
        f.delete();
        DB db = DBMaker.fileDB(f).transactionEnable().make();
        ConcurrentMap<String, Long> m = db.hashMap("e", Serializers.STRING, Serializers.LONG)
                .valueInline(false).create();
        for (int i = 0; i < 50; i++) m.put("k" + i, (long) (i * 7));
        assertEquals("EXTERNAL", db.getNameCatalog().get("e#kind"));
        db.commit();
        db.close();

        DB db2 = DBMaker.fileDB(f).transactionEnable().make();
        ConcurrentMap<String, Long> m2 = db2.hashMap("e", Serializers.STRING, Serializers.LONG).open();
        assertTrue("EXTERNAL kind must survive reopen", m2 instanceof HTreeMapExternal);
        for (int i = 0; i < 50; i++) {
            assertEquals("external value must survive reopen", (Long) (long) (i * 7), m2.get("k" + i));
        }
        // untyped dispatch reopens the same external map
        assertTrue(db2.get("e") instanceof HTreeMapExternal);
        db2.close();
    }

    // ---- hashSeed persists in the header --------------------------------------

    @Test public void explicitHashSeedSurvivesReopen() throws Exception {
        File f = tmp.newFile("seed.db");
        f.delete();
        final int seed = 0x1234ABCD;
        RecordingStringHasher createHasher = new RecordingStringHasher();
        DB db = DBMaker.fileDB(f).transactionEnable().make();
        ConcurrentMap<String, Long> m = db.hashMap("s", Serializers.STRING, Serializers.LONG)
                .hasher(createHasher)
                .hashSeed(seed)
                .create();
        for (int i = 0; i < 200; i++) m.put("key" + i, (long) i);
        assertEquals(seed, createHasher.lastSeed.get());
        db.commit();
        db.close();

        // The custom-hasher marker is part of the catalog contract: it must be supplied
        // again, even though the serializers can be reconstructed from the catalog.
        DB db2 = DBMaker.fileDB(f).transactionEnable().make();
        org.junit.Assert.assertThrows(DBException.WrongConfiguration.class,
                () -> db2.hashMap("s", Serializers.STRING, Serializers.LONG).open());

        // On reopen the seed comes from the persisted collection header, not from the
        // maker. Supplying a deliberately different maker seed must therefore have no effect.
        RecordingStringHasher reopenHasher = new RecordingStringHasher();
        ConcurrentMap<String, Long> m2 = db2.hashMap("s", Serializers.STRING, Serializers.LONG)
                .hasher(reopenHasher)
                .hashSeed(0x76543210)
                .open();
        for (int i = 0; i < 200; i++) {
            assertEquals("persisted hashSeed must let every key read back",
                    (Long) (long) i, m2.get("key" + i));
        }
        assertEquals("open must use the seed persisted at create time", seed, reopenHasher.lastSeed.get());
        db2.close();
    }

    @Test public void sameHashSeedGivesIdenticalLayout() {
        DB db = DBMaker.memoryDB().make();
        // Two maps with the SAME explicit seed and the SAME keys inserted in the SAME order
        // have an identical (segment, index) placement, hence identical iteration order.
        ConcurrentMap<String, Long> a = db.hashMap("a", Serializers.STRING, Serializers.LONG)
                .hashSeed(777).create();
        ConcurrentMap<String, Long> b = db.hashMap("b", Serializers.STRING, Serializers.LONG)
                .hashSeed(777).create();
        for (int i = 0; i < 300; i++) {
            a.put("k" + i, (long) i);
            b.put("k" + i, (long) i);
        }
        List<String> orderA = new ArrayList<>(a.keySet());
        List<String> orderB = new ArrayList<>(b.keySet());
        assertEquals("same seed + same keys => identical iteration order", orderA, orderB);
        assertEquals(300, orderA.size());
        db.close();
    }
}
