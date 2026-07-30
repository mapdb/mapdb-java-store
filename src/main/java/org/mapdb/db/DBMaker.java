package org.mapdb.db;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;

import org.mapdb.DBException;
import org.mapdb.store.Store;
import org.mapdb.store.StoreAppendOnly;
import org.mapdb.store.StoreByteArray;
import org.mapdb.store.StoreDirect;
import org.mapdb.store.StoreOnHeap;
import org.mapdb.store.StoreReadOnlyWrapper;
import org.mapdb.store.StoreWAL;

/**
 * Builder for {@link DB}, ported from MapDB 3. Pick a backing store with a static
 * factory, tune it with the chainable options, then call {@link Maker#make()}.
 *
 * <pre>{@code
 * DB db = DBMaker.fileDB("data.db").transactionEnable().make();
 * var map = db.treeMap("m", LongFormat.INSTANCE, StringGroupFormat.INSTANCE).createOrOpen();
 * }</pre>
 *
 * <h2>Factory → store</h2>
 * <ul>
 *   <li>{@link #heapDB()} — {@code StoreOnHeap} (live objects, no serialization, no durability)</li>
 *   <li>{@link #memoryDB()} — {@code StoreDirect} on heap ByteBuffers (serialized, paged; the
 *       standard in-memory analogue of the durable engine)</li>
 *   <li>{@link #memoryDirectDB()} — {@code StoreDirect} on off-heap direct ByteBuffers</li>
 *   <li>{@link #memoryByteArrayDB()} — {@code StoreByteArray} reference oracle (testing)</li>
 *   <li>{@link #fileDB(File)} — {@code StoreDirect(File)} (mmap, crash-<b>unsafe</b>), or
 *       {@code StoreWAL(File)} when {@link Maker#transactionEnable()} is set (crash-safe)</li>
 *   <li>{@link #tempFileDB()} — a {@code fileDB} on a fresh temp file, deleted on close</li>
 * </ul>
 *
 * <p>Some MapDB 3 options are accepted for source compatibility but are no-ops here:
 * {@link Maker#fileMmapEnable()} / {@link Maker#fileMmapEnableIfSupported()} (mapdb5 file
 * stores are always memory-mapped). Others that mapdb5's stores cannot honor
 * (allocateStartSize, checksum bypass) remain intentionally omitted rather than silently
 * ignored.
 */
public final class DBMaker {

    private DBMaker() {}

    private enum StoreType { onHeap, memory, memoryDirect, byteArray, appendOnly, appendOnlyDirect, file }

    public static Maker heapDB() { return new Maker(StoreType.onHeap, null); }
    public static Maker memoryDB() { return new Maker(StoreType.memory, null); }
    public static Maker memoryDirectDB() { return new Maker(StoreType.memoryDirect, null); }
    public static Maker memoryByteArrayDB() { return new Maker(StoreType.byteArray, null); }
    public static Maker memoryAppendOnlyDB() { return new Maker(StoreType.appendOnly, null); }
    public static Maker memoryAppendOnlyDirectDB() { return new Maker(StoreType.appendOnlyDirect, null); }

    public static Maker fileDB(File file) { return new Maker(StoreType.file, file); }
    public static Maker fileDB(String path) { return new Maker(StoreType.file, new File(path)); }

    /** A fileDB on a fresh temp file that is deleted when the DB closes. */
    public static Maker tempFileDB() {
        try {
            File f = File.createTempFile("mapdb5", ".db");
            f.delete(); // we want the store to create it fresh
            return fileDB(f).fileDeleteAfterClose();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Fluent builder returned by the factories above. */
    public static final class Maker {
        private final StoreType storeType;
        private final File file;
        private boolean transactionEnable = false;
        private boolean threadSafe = true;
        private boolean fileDeleteAfterClose = false;
        private boolean fileDeleteAfterOpen = false;
        private boolean closeOnJvmShutdown = false;
        private boolean closeOnJvmShutdownWeak = false;
        private boolean readOnly = false;

        Maker(StoreType storeType, File file) { this.storeType = storeType; this.file = file; }

        /** Use a crash-safe WAL store (file DBs only). */
        public Maker transactionEnable() { this.transactionEnable = true; return this; }

        /** Build a single-threaded store; the whole DB must then be externally synchronized. */
        public Maker concurrencyDisable() { this.threadSafe = false; return this; }

        /** Delete the backing file(s) when the DB closes. */
        public Maker fileDeleteAfterClose() { this.fileDeleteAfterClose = true; return this; }

        /** @deprecated MapDB 3 alias for {@link #fileDeleteAfterClose()}. */
        @Deprecated public Maker deleteFilesAfterClose() { return fileDeleteAfterClose(); }

        /** MapDB 3 compatibility hook; background work is configured per collection in mapdb5. */
        public Maker executorEnable() { return this; }

        /**
         * Delete the backing file(s) immediately after the store is opened (file DBs only).
         * The store keeps working from its open mapping / the OS page cache until close;
         * once closed the data is gone. Useful for scratch DBs that must not survive a crash
         * and must not leave a file visible on disk while running. Cannot be combined with
         * {@link #transactionEnable()} because a later WAL checkpoint recreates the file path.
         */
        public Maker fileDeleteAfterOpen() { this.fileDeleteAfterOpen = true; return this; }

        /** Register this DB to be closed automatically on JVM shutdown. */
        public Maker closeOnJvmShutdown() { this.closeOnJvmShutdown = true; return this; }

        /**
         * Like {@link #closeOnJvmShutdown()} but registers via a {@link java.lang.ref.WeakReference}:
         * the shutdown hook does not keep the DB alive, so a forgotten DB can be garbage-collected
         * normally and is only closed on shutdown if it is still reachable.
         */
        public Maker closeOnJvmShutdownWeakReference() { this.closeOnJvmShutdownWeak = true; return this; }

        /**
         * Open the store as logically read-only: the DB rejects all mutations at the API by
         * wrapping the store in {@link StoreReadOnlyWrapper}. Intended for opening an existing,
         * non-empty DB. This is a logical guard only — mapdb5 file stores still {@code mmap} the
         * file {@code READ_WRITE} at the OS level (see {@link StoreReadOnlyWrapper}). Cannot be
         * combined with {@link #transactionEnable()} (a read-only DB has nothing to commit).
         */
        public Maker readOnly() { this.readOnly = true; return this; }

        /** No-op; mapdb5 file stores are always memory-mapped. Present for MapDB 3 source compatibility. */
        public Maker fileMmapEnable() { return this; }

        /** No-op; mapdb5 file stores are always memory-mapped. Present for MapDB 3 source compatibility. */
        public Maker fileMmapEnableIfSupported() { return this; }

        /** Mapdb5 file stores already unmap/clean their buffers on close. */
        public Maker cleanerHackEnable() { return this; }

        /** Mapdb5 always uses its mmap implementation for file DBs; retained for source compatibility. */
        public Maker fileChannelEnable() { return this; }

        /** StoreDirect/WAL checksums are inherent rather than optional. */
        public Maker checksumStoreEnable() { return this; }

        public DB make() {
            if (transactionEnable && storeType != StoreType.file) {
                throw new DBException.WrongConfiguration(
                        "transactionEnable() requires a file DB (no in-memory WAL store exists)");
            }
            if ((fileDeleteAfterClose) && storeType != StoreType.file) {
                throw new DBException.WrongConfiguration("fileDeleteAfterClose() requires a file DB");
            }
            if (fileDeleteAfterOpen && storeType != StoreType.file) {
                throw new DBException.WrongConfiguration("fileDeleteAfterOpen() requires a file DB");
            }
            if (fileDeleteAfterOpen && transactionEnable) {
                throw new DBException.WrongConfiguration(
                        "fileDeleteAfterOpen() cannot be combined with transactionEnable(); WAL checkpoints recreate the file");
            }
            if (readOnly && transactionEnable) {
                throw new DBException.WrongConfiguration(
                        "readOnly() cannot be combined with transactionEnable() (a read-only DB has nothing to commit)");
            }

            Store store;
            Runnable afterClose = null;
            switch (storeType) {
                case onHeap:
                    store = new StoreOnHeap(threadSafe);
                    break;
                case memory:
                    store = new StoreDirect(false, threadSafe);
                    break;
                case memoryDirect:
                    store = new StoreDirect(true, threadSafe);
                    break;
                case byteArray:
                    store = new StoreByteArray(threadSafe);
                    break;
                case appendOnly:
                    store = new StoreAppendOnly(false);
                    break;
                case appendOnlyDirect:
                    store = new StoreAppendOnly(true);
                    break;
                case file:
                    if (transactionEnable) {
                        store = new StoreWAL(file, false, threadSafe);
                    } else {
                        store = new StoreDirect(file, threadSafe);
                    }
                    if (fileDeleteAfterOpen) {
                        // Data lives in the open mapping / OS page cache; drop the on-disk file now.
                        deleteBackingFiles(file, "fileDeleteAfterOpen");
                    }
                    if (fileDeleteAfterClose) {
                        final File f = file;
                        // deleteIfExists tolerates an already-gone file, so this composes with
                        // fileDeleteAfterOpen without a double-delete error.
                        afterClose = () -> deleteBackingFiles(f, "fileDeleteAfterClose");
                    }
                    break;
                default:
                    throw new AssertionError(storeType);
            }

            // Logical read-only guard: wrap before handing to DB so all mutations are rejected.
            // The wrapper's close() still closes the real store, so afterClose cleanup runs.
            Store dbStore = readOnly ? new StoreReadOnlyWrapper(store) : store;

            DB db;
            try {
                // Strong shutdown hook is registered by the DB constructor; the weak variant
                // is registered below without pinning the DB alive.
                db = new DB(dbStore, threadSafe, afterClose, closeOnJvmShutdown);
            } catch (RuntimeException | Error e) {
                // DB construction (catalog init / hook registration) failed: don't leak the store.
                // Closing the wrapper closes the underlying store; afterClose is NOT run here
                // (it only runs on a successful DB's close()).
                try { dbStore.close(); } catch (RuntimeException | Error ce) { e.addSuppressed(ce); }
                if (readOnly && e instanceof UnsupportedOperationException) {
                    // Almost certainly an empty store: DB.init() tried to write the catalog.
                    throw new DBException.WrongConfiguration(
                            "readOnly() requires an existing, non-empty DB store (nothing to open)", e);
                }
                throw e;
            }
            if (closeOnJvmShutdownWeak) ShutdownHooks.registerWeak(db);
            return db;
        }

        /**
         * Removes the store file and every sidecar {@code StoreWAL} owns. Under WAL format v2
         * the log is a SEGMENT SET — {@code <db>.wal.<16 hex>} — plus the {@code <db>.lock}
         * exclusive open lock, so deleting only {@code <db>} would leave the whole committed
         * history on disk and the next open would recover it.
         */
        private static void deleteBackingFiles(File f, String what) {
            try {
                Files.deleteIfExists(f.toPath());
                Files.deleteIfExists(new File(f.getPath() + ".lock").toPath());
                // The v1 single-file log too. It is not part of a v2 store, but leaving one behind
                // arms N6: the next open at this path is refused as "v1 WAL present, no migration"
                // on what the caller was told is a deleted store. Regular files only, matching what
                // N6 actually refuses — a DIRECTORY at that name is not a v1 log, and deleting it
                // would throw DirectoryNotEmptyException on something N6 happily ignores.
                Path v1 = new File(f.getPath() + ".wal").toPath();
                if (Files.isRegularFile(v1, LinkOption.NOFOLLOW_LINKS))
                    Files.deleteIfExists(v1);
                File dir = f.getAbsoluteFile().getParentFile();
                String prefix = f.getAbsoluteFile().getName() + ".wal.";
                String[] names = dir == null ? null : dir.list();
                if (names != null) {
                    for (String name : names) {
                        if (name.startsWith(prefix)) Files.deleteIfExists(new File(dir, name).toPath());
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(what + " failed for " + f, e);
            }
        }
    }
}
