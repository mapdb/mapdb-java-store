package org.mapdb.store;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Runs the full TCK + delta suite in isThreadSafe=false mode:
 * all locks are no-ops, semantics must be identical under single-threaded use.
 */
@RunWith(Enclosed.class)
public class StoreNoLockTests {

    public static class StoreOnHeapNoLockTest extends StoreTCK {
        @Override protected Store createStore() { return new StoreOnHeap(false); }

        @Test public void reportsNotThreadSafe() {
            Store s = createStore();
            assertFalse(s.isThreadSafe());
            assertTrue(new StoreOnHeap().isThreadSafe());
        }
    }

    public static class StoreByteArrayNoLockTest extends StoreTCK {
        @Override protected Store createStore() { return new StoreByteArray(false); }
    }

    public static class StoreDirectNoLockTest extends StoreTCK {
        @Override protected Store createStore() { return new StoreDirect(false, false); }
    }

    public static class StoreWALNoLockTest extends StoreTCK {
        private final List<File> files = new ArrayList<>();

        @Override protected Store createStore() {
            try {
                File f = Files.createTempFile("mapdb5-wal-nolock", ".wal").toFile();
                f.delete();
                files.add(f);
                return new StoreWAL(f, false, false);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override protected boolean reusesRecidsImmediately() { return false; }

        @After public void deleteFiles() {
            for (File f : files) f.delete();
            files.clear();
        }
    }

    public static class DeltaStoreByteArrayNoLockTest extends DeltaTCK {
        @Override protected StoreDelta createStore() { return new StoreByteArray(false); }
    }

    public static class DeltaStoreDirectNoLockTest extends DeltaTCK {
        @Override protected StoreDelta createStore() { return new StoreDirect(false, false); }
    }

    public static class DeltaStoreWALNoLockTest extends DeltaTCK {
        private final List<File> files = new ArrayList<>();

        @Override protected StoreDelta createStore() {
            try {
                File f = Files.createTempFile("mapdb5-wal-delta-nolock", ".wal").toFile();
                f.delete();
                files.add(f);
                return new StoreWAL(f, false, false);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @After public void deleteFiles() {
            for (File f : files) f.delete();
            files.clear();
        }
    }
}
