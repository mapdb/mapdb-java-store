package org.mapdb.db;

import org.mapdb.TmpFiles;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.mapdb.DBException;
import org.mapdb.btree.BTreeMap;
import org.mapdb.ser.LongFormat;
import org.mapdb.ser.Serializers;
import org.mapdb.ser.StringGroupFormat;

/**
 * Coverage for the MapDB-3-parity DBMaker options: {@link DBMaker.Maker#readOnly()},
 * {@link DBMaker.Maker#fileMmapEnable()} / {@link DBMaker.Maker#fileMmapEnableIfSupported()},
 * {@link DBMaker.Maker#fileDeleteAfterOpen()}, and
 * {@link DBMaker.Maker#closeOnJvmShutdownWeakReference()}.
 */
public class DBReadOnlyMakerTest {

    private static File freshFile() throws Exception {
        File f = TmpFiles.tempFile("mapdb-ro", ".db");
        f.delete();
        return f;
    }

    private static void cleanup(File f) {
        f.delete();
        org.mapdb.store.WalTestKit.deleteStore(f);
    }

    // ---- readOnly round-trip ---------------------------------------------

    @Test public void readOnlyReopenReadsButRejectsWrites() throws Exception {
        File f = freshFile();
        try {
            // 1. Write a file DB and close.
            DB db = DBMaker.fileDB(f).make();
            BTreeMap<Long, String> m = db.treeMap("m", LongFormat.INSTANCE, StringGroupFormat.INSTANCE).create();
            m.put(1L, "one");
            m.put(2L, "two");
            db.commit();
            db.close();

            // 2. Reopen read-only and read values back.
            DB ro = DBMaker.fileDB(f).readOnly().make();
            BTreeMap<Long, String> rm = ro.treeMap("m", LongFormat.INSTANCE, StringGroupFormat.INSTANCE).open();
            assertEquals("one", rm.get(1L));
            assertEquals("two", rm.get(2L));

            // 3. A mutation must be rejected by the read-only store wrapper.
            assertThrows(UnsupportedOperationException.class, () -> rm.put(3L, "three"));

            // commit() on a read-only DB is a harmless no-op.
            ro.commit();
            ro.close();
        } finally {
            cleanup(f);
        }
    }

    @Test public void readOnlyReopenCanReadAccessOrderCacheWithoutQueueWrites() throws Exception {
        File f = freshFile();
        try {
            DB db = DBMaker.fileDB(f).make();
            ConcurrentMap<String, String> cache = db
                    .hashMap("cache", Serializers.STRING, Serializers.STRING)
                    .expireAfterGet(1, TimeUnit.DAYS)
                    .create();
            cache.put("key", "value");
            db.commit();
            db.close();

            DB ro = DBMaker.fileDB(f).readOnly().make();
            ConcurrentMap<String, String> reopened = ro
                    .hashMap("cache", Serializers.STRING, Serializers.STRING)
                    .open();
            assertEquals("value", reopened.get("key"));
            assertThrows(UnsupportedOperationException.class,
                    () -> reopened.put("other", "blocked"));
            ro.close();
        } finally {
            cleanup(f);
        }
    }

    @Test public void readOnlyPlusTransactionEnableRejected() throws Exception {
        File f = freshFile();
        try {
            assertThrows(DBException.WrongConfiguration.class,
                    () -> DBMaker.fileDB(f).readOnly().transactionEnable().make());
            assertThrows(DBException.WrongConfiguration.class,
                    () -> DBMaker.fileDB(f).transactionEnable().readOnly().make());
        } finally {
            cleanup(f);
        }
    }

    @Test public void readOnlyOnEmptyStoreFailsCleanly() throws Exception {
        File f = freshFile();
        try {
            // Opening a fresh (empty) store read-only cannot write the catalog: clear message.
            DBException.WrongConfiguration ex = assertThrows(DBException.WrongConfiguration.class,
                    () -> DBMaker.fileDB(f).readOnly().make());
            assertTrue(ex.getMessage().toLowerCase().contains("read"));
        } finally {
            cleanup(f);
        }
    }

    @Test public void readOnlyOnEmptyInMemoryStoreFailsCleanly() {
        // A fresh in-memory store is empty, so opening it read-only cannot write the catalog.
        DBException.WrongConfiguration ex = assertThrows(DBException.WrongConfiguration.class,
                () -> DBMaker.memoryDB().readOnly().make());
        assertTrue(ex.getMessage().toLowerCase().contains("read"));
    }

    // ---- mmap no-ops ------------------------------------------------------

    @Test public void fileMmapEnableIsHarmlessNoOp() throws Exception {
        File f = freshFile();
        try {
            DB db = DBMaker.fileDB(f).fileMmapEnable().fileMmapEnableIfSupported().make();
            db.treeMap("m", LongFormat.INSTANCE, StringGroupFormat.INSTANCE).create().put(1L, "x");
            db.commit();
            db.close();

            DB db2 = DBMaker.fileDB(f).fileMmapEnable().make();
            assertEquals("x",
                    db2.treeMap("m", LongFormat.INSTANCE, StringGroupFormat.INSTANCE).open().get(1L));
            db2.close();
        } finally {
            cleanup(f);
        }
    }

    // ---- fileDeleteAfterOpen ---------------------------------------------

    @Test public void fileDeleteAfterOpenRemovesFileButDbStillWorks() throws Exception {
        File f = freshFile();
        try {
            DB db = DBMaker.fileDB(f).fileDeleteAfterOpen().make();
            // The backing file is already gone from disk.
            assertFalse("backing file should be deleted right after open", f.exists());

            // Yet the DB keeps working from its open mapping / page cache until close.
            BTreeMap<Long, String> m = db.treeMap("m", LongFormat.INSTANCE, StringGroupFormat.INSTANCE).create();
            m.put(1L, "alive");
            db.commit();
            assertEquals("alive", m.get(1L));
            db.close();

            // After close the data is gone; nothing left on disk.
            assertFalse(f.exists());
        } finally {
            cleanup(f);
        }
    }

    @Test public void fileDeleteAfterOpenRequiresFileDb() {
        assertThrows(DBException.WrongConfiguration.class,
                () -> DBMaker.memoryDB().fileDeleteAfterOpen().make());
    }

    @Test public void fileDeleteAfterOpenRejectsTransactionalWal() throws Exception {
        File f = freshFile();
        try {
            assertThrows(DBException.WrongConfiguration.class,
                    () -> DBMaker.fileDB(f).transactionEnable().fileDeleteAfterOpen().make());
        } finally {
            cleanup(f);
        }
    }

    // ---- weak shutdown hook ----------------------------------------------

    @Test public void weakShutdownHookRegistrationDoesNotCrash() {
        DB db = DBMaker.memoryDB().closeOnJvmShutdownWeakReference().make();
        // Registration is enough to exercise the weak registry; explicit close must still work.
        db.treeMap("m", LongFormat.INSTANCE, StringGroupFormat.INSTANCE).create().put(1L, "y");
        db.close();
    }

    @Test public void weakShutdownHookComposesWithStrong() {
        DB strong = DBMaker.memoryDB().closeOnJvmShutdown().make();
        DB weak = DBMaker.memoryDB().closeOnJvmShutdownWeakReference().make();
        strong.close();
        weak.close();
    }
}
