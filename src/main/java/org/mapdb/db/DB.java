package org.mapdb.db;

import java.io.Closeable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.PrimitiveIterator;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.mapdb.DBException;
import org.mapdb.MapModificationListener;
import org.mapdb.btree.BTreeMap;
import org.mapdb.btree.BufferTreeMap;
import org.mapdb.hash.Hasher;
import org.mapdb.hash.Hasher64;
import org.mapdb.hash.Hashers;
import org.mapdb.htree.HTreeCache;
import org.mapdb.htree.HTreeMap;
import org.mapdb.htree.HTreeMap48;
import org.mapdb.htree.HTreeMapExternal;
import org.mapdb.indextree.IndexTreeList;
import org.mapdb.indextree.IndexTreeLongLongMap;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;
import org.mapdb.queue.PersistentBlockingQueue;
import org.mapdb.ser.GroupFormat;
import org.mapdb.ser.ObjectArrayFormat;
import org.mapdb.ser.Serializer;
import org.mapdb.ser.Serializers;
import org.mapdb.sortedtable.SortedTableMap;
import org.mapdb.store.Store;
import org.mapdb.store.StoreDelta;
import org.mapdb.store.StoreTx;

/**
 * High-level facade over a {@link Store}, ported from MapDB 3's {@code DB}.
 *
 * <p>A {@code DB} owns its store and persists a <em>name catalog</em> — one record
 * at {@link #RECID_CATALOG recid&nbsp;1} — describing every named collection so it
 * can be reopened later. Obtain collections through the typed makers
 * ({@link #hashMap}, {@link #treeMap}, {@link #atomicLong}, …), each of which
 * offers {@code create()} / {@code open()} / {@code createOrOpen()}.
 *
 * <h2>Store ownership</h2>
 * A store managed by a {@code DB} is <b>exclusively owned</b>: recid&nbsp;1 is
 * reserved for the catalog and is written on first initialization. Callers may
 * inspect the store via {@link #getStore()} but must not allocate/delete
 * DB-owned records directly nor wrap the same store in a second {@code DB}.
 *
 * <h2>Durability</h2>
 * Backend-dependent — {@link #commit()} is a durability barrier, not always a
 * transaction boundary:
 * <ul>
 *   <li><b>{@code fileDB(f).transactionEnable()}</b> (StoreWAL): fully transactional.
 *       {@code commit()} atomically persists records + catalog; {@code rollback()}
 *       discards to the last commit; {@link #close()} discards staged-but-uncommitted
 *       changes. Recovers to the last commit after a crash.</li>
 *   <li><b>{@code fileDB(f)}</b> (StoreDirect, no WAL): writes go straight into the
 *       mmap and are <b>crash-unsafe</b>. {@code commit()} flushes + checksums;
 *       {@code close()} also flushes (so a clean close persists pending writes);
 *       {@code rollback()} is unsupported. A torn write refuses to reopen.</li>
 *   <li><b>in-memory stores</b>: no durability at all.</li>
 * </ul>
 * {@code rollback()} is supported only on a transactional store.
 *
 * <h2>Thread-safety</h2>
 * All catalog/lifecycle operations take a DB write lock; makers return the same
 * live handle per name so lock-bearing collections are shared. When built with
 * {@code concurrencyDisable()} the underlying store is single-threaded and the
 * whole DB must be externally synchronized.
 */
public class DB implements Closeable {
    private static final java.util.logging.Logger LOG =
            java.util.logging.Logger.getLogger(DB.class.getName());

    /** Reserved recid holding the name catalog; written first on a fresh store. */
    public static final long RECID_CATALOG = 1L;

    static final String CUSTOM = "CUSTOM";
    private static final int CATALOG_MAGIC = 0x4D444243; // "MDBC"
    private static final int CATALOG_VERSION = 1;
    private static final byte REPR_INLINE = 0;
    private static final int MAX_CATALOG_ENTRIES = 10_000_000;

    private enum State { OPEN, CLOSING, CLOSED }

    private final Store store;
    private final boolean threadSafe;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    /** Strong per-name handle cache: one live object per name (identity for locking). */
    private final Map<String, Object> instances = new LinkedHashMap<>();
    /** Runtime-only background expiry tasks, keyed by live cache identity. */
    private final Map<Object, ScheduledFuture<?>> expirationTasks = new IdentityHashMap<>();
    private volatile State state = State.OPEN;
    private final Runnable afterClose;

    // ---- construction -----------------------------------------------------

    public DB(Store store, boolean threadSafe) { this(store, threadSafe, null, false); }

    /**
     * @param afterClose optional cleanup (e.g. file deletion) run after the store is closed
     * @param registerShutdownHook close this DB automatically on JVM shutdown
     */
    public DB(Store store, boolean threadSafe, Runnable afterClose, boolean registerShutdownHook) {
        this.store = store;
        this.threadSafe = threadSafe;
        this.afterClose = afterClose;
        init();
        if (registerShutdownHook) ShutdownHooks.register(this);
    }

    private void init() {
        if (isStoreEmpty()) {
            TreeMap<String, String> empty = new TreeMap<>();
            long r = store.put(empty, CATALOG_SER);
            if (r != RECID_CATALOG) {
                // Store looked empty (getAllRecids excludes preallocated) but did not hand
                // out recid 1. Undo our write so we don't leak into the caller's store.
                try { store.delete(r, CATALOG_SER); } catch (RuntimeException ignore) { /* best effort */ }
                throw new DBException.WrongConfiguration(
                        "store did not allocate the catalog at recid " + RECID_CATALOG
                                + " (got " + r + "); a DB requires a genuinely empty store");
            }
            store.commit();
        } else {
            // Validate that recid 1 is a well-formed catalog; fail closed otherwise.
            try {
                catalogLoadInternal();
            } catch (DBException.GetVoid e) {
                throw new DBException.WrongConfiguration(
                        "not a mapdb5 DB store: recid " + RECID_CATALOG + " is not present", e);
            } catch (DBException.DataCorruption e) {
                throw e; // already a clean catalog-format error
            } catch (DBException e) {
                throw e;
            } catch (RuntimeException e) {
                // truncated/garbage record: AIOOBE, NegativeArraySize, etc.
                throw new DBException.DataCorruption(
                        "cannot read catalog at recid " + RECID_CATALOG + ": " + e);
            }
        }
    }

    private boolean isStoreEmpty() {
        PrimitiveIterator.OfLong it = store.getAllRecids();
        return !it.hasNext();
    }

    // ---- catalog codec ----------------------------------------------------

    static final Serializer<TreeMap<String, String>> CATALOG_SER = new Serializer<>() {
        @Override public void serialize(DataOutput2 out, TreeMap<String, String> cat) {
            out.writeInt(CATALOG_MAGIC);
            out.writeInt(CATALOG_VERSION);
            out.writeByte(REPR_INLINE);
            out.packInt(cat.size());
            for (Map.Entry<String, String> e : cat.entrySet()) {
                Serializers.STRING.serialize(out, e.getKey());
                Serializers.STRING.serialize(out, e.getValue());
            }
        }

        @Override public TreeMap<String, String> deserialize(DataInput2 in, int size) {
            // size is the record length; bound every read/allocation by it (hostile-input safe).
            int start = in.pos();
            long end = (size >= 0) ? (long) start + size : Long.MAX_VALUE;
            if (size >= 0 && size < 10) {
                throw new DBException.DataCorruption("catalog record too small: " + size + " bytes");
            }
            int magic = in.readInt();
            if (magic != CATALOG_MAGIC) {
                throw new DBException.DataCorruption(
                        "not a mapdb5 catalog: bad magic 0x" + Integer.toHexString(magic));
            }
            int version = in.readInt();
            if (version != CATALOG_VERSION) {
                throw new DBException.DataCorruption(
                        "unsupported catalog version " + version + " (this build supports " + CATALOG_VERSION + ")");
            }
            byte repr = in.readByte();
            if (repr != REPR_INLINE) {
                throw new DBException.DataCorruption("unsupported catalog representation " + repr);
            }
            int n = (int) readBoundedPackedLong(in, end);
            if (n < 0 || n > MAX_CATALOG_ENTRIES) {
                throw new DBException.DataCorruption("implausible catalog entry count " + n);
            }
            TreeMap<String, String> cat = new TreeMap<>();
            for (int i = 0; i < n; i++) {
                String k = readBoundedString(in, end);
                String v = readBoundedString(in, end);
                if (cat.put(k, v) != null) {
                    throw new DBException.DataCorruption("duplicate catalog key: " + k);
                }
            }
            if (size >= 0 && in.pos() != end) {
                throw new DBException.DataCorruption(
                        "catalog record has " + (end - in.pos()) + " trailing/short bytes");
            }
            return cat;
        }
    };

    /** Length-framed UTF-8 read that never allocates or reads past {@code end}. */
    private static String readBoundedString(DataInput2 in, long end) {
        int len = (int) readBoundedPackedLong(in, end);
        if (len < 0 || (long) in.pos() + len > end) {
            throw new DBException.DataCorruption("catalog string length out of bounds: " + len);
        }
        byte[] b = new byte[len];
        in.readFully(b);
        return new String(b, java.nio.charset.StandardCharsets.UTF_8);
    }

    /**
     * Reads a packed long (mapdb5 varint: 7 bits/byte, high bit marks the terminal byte)
     * without reading past {@code end} — a corrupt/unterminated value fails cleanly instead
     * of scanning into adjacent store bytes.
     */
    private static long readBoundedPackedLong(DataInput2 in, long end) {
        long ret = 0;
        for (;;) {
            if (in.pos() >= end) {
                throw new DBException.DataCorruption("packed value runs past catalog record boundary");
            }
            byte v = in.readByte();
            ret = (ret << 7) | (v & 0x7F);
            if ((v & 0x80) != 0) return ret;
        }
    }

    /** Loads the catalog. Caller must hold the appropriate lock. */
    private TreeMap<String, String> catalogLoadInternal() {
        return store.get(RECID_CATALOG, CATALOG_SER);
    }

    /** Persists the catalog. Caller must hold the write lock. */
    private void catalogSaveInternal(TreeMap<String, String> cat) {
        store.update(RECID_CATALOG, cat, CATALOG_SER);
    }

    // ---- codec id resolution ---------------------------------------------

    /** True for a custom-codec catalog marker ({@code "CUSTOM"} or {@code "CUSTOM:<fqcn>"}). */
    static boolean isCustom(String id) {
        return id != null && (id.equals(CUSTOM) || id.startsWith(CUSTOM + ":"));
    }

    private static String customMarker(Object codec) { return CUSTOM + ":" + codec.getClass().getName(); }

    /** Rejects a re-supplied custom codec whose class differs from the one recorded at create time. */
    private static void checkCustomClass(String id, Object supplied) {
        int i = id.indexOf(':');
        if (i < 0) return; // legacy plain "CUSTOM" marker carries no class to check
        String recorded = id.substring(i + 1);
        String actual = supplied.getClass().getName();
        if (!recorded.equals(actual)) {
            throw new DBException.WrongConfiguration(
                    "custom codec class mismatch for reopen: catalog recorded '" + recorded
                            + "', supplied '" + actual + "'");
        }
    }

    static String serId(Serializer<?> s) {
        String id = SerializerRegistry.serializerId(s);
        return id != null ? id : customMarker(s);
    }

    static String fmtId(GroupFormat<?> f) {
        String id = SerializerRegistry.groupFormatId(f);
        return id != null ? id : customMarker(f);
    }

    @SuppressWarnings("unchecked")
    static <A> Serializer<A> resolveSerializer(String id, Serializer<A> supplied) {
        if (isCustom(id)) {
            if (supplied == null) {
                throw new DBException.WrongConfiguration(
                        "collection uses a custom serializer; re-supply it on the maker before opening");
            }
            checkCustomClass(id, supplied);
            return supplied;
        }
        Serializer<?> s = SerializerRegistry.serializerById(id);
        if (s == null) throw new DBException.DataCorruption("unknown serializer id in catalog: " + id);
        if (supplied != null && !id.equals(serId(supplied))) {
            throw new DBException.WrongConfiguration(
                    "serializer mismatch for reopen: catalog='" + id + "', supplied resolves to '" + serId(supplied) + "'");
        }
        return (Serializer<A>) s;
    }

    @SuppressWarnings("unchecked")
    static <A> GroupFormat<A> resolveFormat(String id, GroupFormat<A> supplied) {
        if (isCustom(id)) {
            if (supplied == null) {
                throw new DBException.WrongConfiguration(
                        "collection uses a custom group format; re-supply it on the maker before opening");
            }
            checkCustomClass(id, supplied);
            return supplied;
        }
        GroupFormat<?> f = SerializerRegistry.groupFormatById(id);
        if (f == null) throw new DBException.DataCorruption("unknown group format id in catalog: " + id);
        if (supplied != null && !id.equals(fmtId(supplied))) {
            throw new DBException.WrongConfiguration(
                    "group format mismatch for reopen: catalog='" + id + "', supplied resolves to '" + fmtId(supplied) + "'");
        }
        return (GroupFormat<A>) f;
    }

    // ---- lifecycle --------------------------------------------------------

    /** The backing store. For inspection only — do not mutate DB-owned records. */
    public Store getStore() {
        checkOpen();
        return store;
    }

    /** Flushes all changes to the store, making them durable (subject to the store's guarantees). */
    public void commit() {
        lock.writeLock().lock();
        try {
            checkOpen();
            store.commit();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Reclaim obsolete physical storage where supported by the backing store. */
    public void compact() {
        lock.writeLock().lock();
        try {
            checkOpen();
            store.compact();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Discards all changes since the last commit. Supported only on a transactional
     * (WAL) store; throws {@link UnsupportedOperationException} otherwise.
     *
     * <p><b>Handle invalidation:</b> rollback clears the per-name instance cache, so
     * after a rollback you must treat <em>all</em> previously obtained collection/atomic
     * handles as invalid and re-open by name. Continuing to use an old handle — including
     * one for a collection that still exists — is unsafe: a second live handle over the
     * same roots has an independent lock domain and can corrupt concurrent writes.
     */
    public void rollback() {
        lock.writeLock().lock();
        try {
            checkOpen();
            if (!(store instanceof StoreTx)) {
                throw new UnsupportedOperationException(
                        "rollback requires a transactional store (DBMaker.fileDB(f).transactionEnable())");
            }
            ((StoreTx) store).rollback();
            for (Object instance : instances.values()) closeRuntimeHandle(instance);
            instances.clear();
            for (ScheduledFuture<?> task : expirationTasks.values()) task.cancel(false);
            expirationTasks.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override public void close() {
        // one-shot transition to CLOSING under the write lock
        lock.writeLock().lock();
        try {
            if (state != State.OPEN) return;
            state = State.CLOSING;
            for (Object instance : instances.values()) closeRuntimeHandle(instance);
            for (ScheduledFuture<?> task : expirationTasks.values()) task.cancel(false);
            expirationTasks.clear();
        } finally {
            lock.writeLock().unlock();
        }
        ShutdownHooks.deregister(this);
        ShutdownHooks.deregisterWeak(this);
        RuntimeException primary = null;
        try {
            store.close();
        } catch (RuntimeException e) {
            primary = e;
        }
        lock.writeLock().lock();
        try {
            instances.clear();
            state = State.CLOSED;
        } finally {
            lock.writeLock().unlock();
        }
        if (afterClose != null) {
            try { afterClose.run(); }
            catch (RuntimeException e) { if (primary != null) primary.addSuppressed(e); else primary = e; }
        }
        if (primary != null) throw primary;
    }

    public boolean isClosed() { return state == State.CLOSED; }

    private void checkOpen() {
        if (state != State.OPEN) throw new DBException.StoreClosed();
    }

    /** Register at most one background sweep for a live cache. Caller executors are never closed. */
    private void scheduleExpiration(HTreeCache<?, ?> cache, ScheduledExecutorService executor,
            long periodMillis, Double compactThreshold) {
        if (executor == null || expirationTasks.containsKey(cache)) return;
        Runnable sweep = () -> {
            try {
                lock.readLock().lock();
                try {
                    if (state != State.OPEN) return;
                    long before = compactThreshold == null ? 0L : store.getCurrentSize();
                    cache.expireEvict();
                    if (compactThreshold != null && before > 0) {
                        long after = store.getCurrentSize();
                        double freedFraction = Math.max(0L, before - after) / (double) before;
                        if (freedFraction >= compactThreshold) store.compact();
                    }
                } finally {
                    lock.readLock().unlock();
                }
            } catch (RuntimeException e) {
                // ScheduledExecutorService suppresses every future execution if a task throws.
                // Runtime listeners are user code, so isolate a bad invocation and keep sweeping.
                LOG.log(java.util.logging.Level.WARNING,
                        "background expiration failed for cache; later sweeps will continue", e);
            }
        };
        ScheduledFuture<?> task = executor.scheduleWithFixedDelay(
                sweep, periodMillis, periodMillis, TimeUnit.MILLISECONDS);
        expirationTasks.put(cache, task);
    }

    // ---- catalog / introspection -----------------------------------------

    /** Immutable snapshot of the raw name catalog ({@code name#param -> value}). */
    public SortedMap<String, String> getNameCatalog() {
        lock.readLock().lock();
        try {
            checkOpen();
            return Collections.unmodifiableSortedMap(new TreeMap<>(catalogLoadInternal()));
        } finally {
            lock.readLock().unlock();
        }
    }

    /** Names of all collections in the catalog. */
    public Iterable<String> getAllNames() {
        lock.readLock().lock();
        try {
            checkOpen();
            List<String> names = new ArrayList<>();
            for (String k : catalogLoadInternal().keySet()) {
                if (k.endsWith("#type")) names.add(k.substring(0, k.length() - "#type".length()));
            }
            return names;
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean exists(String name) {
        lock.readLock().lock();
        try {
            checkOpen();
            return catalogLoadInternal().containsKey(name + "#type");
        } finally {
            lock.readLock().unlock();
        }
    }

    /** The catalog type discriminator for {@code name}, or {@code null} if it does not exist. */
    public String getType(String name) {
        lock.readLock().lock();
        try {
            checkOpen();
            return catalogLoadInternal().get(name + "#type");
        } finally {
            lock.readLock().unlock();
        }
    }

    /** Immutable snapshot of this name's raw catalog parameters (including {@code #type}). */
    public Map<String, String> nameCatalogParamsFor(String name) {
        checkName(name);
        lock.readLock().lock();
        try {
            checkOpen();
            String prefix = name + "#";
            TreeMap<String, String> result = new TreeMap<>();
            for (Map.Entry<String, String> e : catalogLoadInternal().entrySet()) {
                if (e.getKey().startsWith(prefix)) result.put(e.getKey(), e.getValue());
            }
            return Collections.unmodifiableMap(result);
        } finally {
            lock.readLock().unlock();
        }
    }

    /** Name currently associated with this exact live object identity, or {@code null}. */
    public String getNameForObject(Object object) {
        if (object == null) return null;
        lock.readLock().lock();
        try {
            checkOpen();
            for (Map.Entry<String, Object> e : instances.entrySet()) {
                if (e.getValue() == object) return e.getKey();
            }
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    /** Open every named object and return an immutable name-sorted snapshot. */
    public Map<String, Object> getAll() {
        lock.writeLock().lock();
        try {
            checkOpen();
            TreeMap<String, Object> result = new TreeMap<>();
            for (String name : getAllNames()) result.put(name, get(name));
            return Collections.unmodifiableMap(result);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Validate catalog structure without throwing. Store-level physical verification is
     * deliberately separate; use {@link #verify()} to run both.
     */
    public List<String> nameCatalogVerifyGetMessages() {
        lock.readLock().lock();
        try {
            checkOpen();
            TreeMap<String, String> cat = catalogLoadInternal();
            List<String> messages = new ArrayList<>();
            Map<String, List<String>> required = catalogRequiredParams();
            Map<String, java.util.Set<String>> allowed = catalogAllowedParams();
            java.util.Set<String> names = new java.util.TreeSet<>();
            for (String key : cat.keySet()) {
                int hash = key.indexOf('#');
                if (hash <= 0) {
                    messages.add(key + ": malformed catalog key");
                    continue;
                }
                names.add(key.substring(0, hash));
            }
            for (String name : names) {
                String type = cat.get(name + "#type");
                if (type == null) {
                    messages.add(name + "#type: required parameter not found");
                    continue;
                }
                List<String> params = required.get(type);
                if (params == null) {
                    messages.add(name + "#type: unknown type '" + type + "'");
                    continue;
                }
                for (String param : params) {
                    String key = name + "#" + param;
                    String value = cat.get(key);
                    if (value == null) {
                        messages.add(key + ": required parameter not found");
                    } else if (param.endsWith("Recid") || param.equals("recid")) {
                        try {
                            long recid = Long.parseLong(value);
                            boolean optionalCounter = param.equals("counterRecid");
                            if (recid < (optionalCounter ? 0 : 1)) {
                                messages.add(key + ": recid "
                                        + (optionalCounter ? "must be non-negative" : "must be positive"));
                            }
                        } catch (NumberFormatException e) {
                            messages.add(key + ": invalid recid '" + value + "'");
                        }
                    }
                }
                java.util.Set<String> allowedParams = allowed.get(type);
                String prefix = name + "#";
                for (String catalogKey : cat.tailMap(prefix).keySet()) {
                    if (!catalogKey.startsWith(prefix)) break;
                    String param = catalogKey.substring(prefix.length());
                    if (!allowedParams.contains(param)) {
                        messages.add(catalogKey + ": unknown parameter for type " + type);
                    }
                }
            }
            return Collections.unmodifiableList(messages);
        } finally {
            lock.readLock().unlock();
        }
    }

    /** Run physical Store verification followed by name-catalog verification. */
    public void verify() {
        lock.readLock().lock();
        try {
            checkOpen();
            store.verify();
            List<String> messages = nameCatalogVerifyGetMessages();
            if (!messages.isEmpty()) throw new DBException.VerifyFailed(String.join("; ", messages));
        } finally {
            lock.readLock().unlock();
        }
    }

    private static Map<String, List<String>> catalogRequiredParams() {
        Map<String, List<String>> ret = new java.util.HashMap<>();
        ret.put("HashMap", java.util.Arrays.asList("type", "keySerializer", "valueSerializer", "headerRecid", "hasher"));
        ret.put("HashSet", java.util.Arrays.asList("type", "serializer", "headerRecid", "hasher"));
        ret.put("HashMap48", java.util.Arrays.asList("type", "keySerializer", "valueSerializer",
                "headerRecid", "hasher64"));
        ret.put("HashSet48", java.util.Arrays.asList("type", "serializer", "headerRecid", "hasher64"));
        ret.put("TreeMap", java.util.Arrays.asList("type", "keySerializer", "valueSerializer", "rootRecidRecid", "maxNodeSize"));
        ret.put("TreeSet", java.util.Arrays.asList("type", "serializer", "rootRecidRecid", "maxNodeSize"));
        ret.put("BufferTreeMap", java.util.Arrays.asList("type", "keySerializer", "valueSerializer",
                "rootRecidRecid", "maxNodeSize", "bufferBytes", "leafHeadroom"));
        ret.put("SortedTableMap", java.util.Arrays.asList("type", "keySerializer", "valueSerializer", "headerRecid", "entriesPerPage"));
        ret.put("AtomicLong", java.util.Arrays.asList("type", "recid"));
        ret.put("AtomicInteger", java.util.Arrays.asList("type", "recid"));
        ret.put("AtomicBoolean", java.util.Arrays.asList("type", "recid"));
        ret.put("AtomicString", java.util.Arrays.asList("type", "recid"));
        ret.put("AtomicVar", java.util.Arrays.asList("type", "recid", "serializer"));
        ret.put("IndexTreeList", java.util.Arrays.asList("type", "headerRecid", "serializer"));
        ret.put("IndexTreeLongLongMap", java.util.Arrays.asList("type", "headerRecid"));
        ret.put("Queue", java.util.Arrays.asList("type", "headerRecid", "serializer"));
        ret.put("Stack", java.util.Arrays.asList("type", "headerRecid", "serializer"));
        ret.put("CircularQueue", java.util.Arrays.asList("type", "headerRecid", "serializer"));
        return ret;
    }

    private static Map<String, java.util.Set<String>> catalogAllowedParams() {
        Map<String, java.util.Set<String>> ret = new java.util.HashMap<>();
        ret.put("HashMap", setOf("type", "keySerializer", "valueSerializer", "headerRecid",
                "hasher", "kind", "concShift", "dirShift", "levels", "counterRecid"));
        ret.put("HashSet", setOf("type", "serializer", "headerRecid", "hasher", "kind",
                "concShift", "dirShift", "levels", "counterRecid"));
        ret.put("HashMap48", setOf("type", "keySerializer", "valueSerializer", "headerRecid",
                "hasher64", "concShift", "dirShift", "levels"));
        ret.put("HashSet48", setOf("type", "serializer", "headerRecid", "hasher64",
                "concShift", "dirShift", "levels"));
        ret.put("TreeMap", setOf("type", "keySerializer", "valueSerializer", "rootRecidRecid",
                "maxNodeSize", "counterRecid", "valueInline"));
        ret.put("TreeSet", setOf("type", "serializer", "rootRecidRecid", "maxNodeSize",
                "counterRecid"));
        ret.put("BufferTreeMap", setOf("type", "keySerializer", "valueSerializer",
                "rootRecidRecid", "maxNodeSize", "bufferBytes", "leafHeadroom"));
        ret.put("SortedTableMap", setOf("type", "keySerializer", "valueSerializer",
                "headerRecid", "entriesPerPage"));
        ret.put("AtomicLong", setOf("type", "recid"));
        ret.put("AtomicInteger", setOf("type", "recid"));
        ret.put("AtomicBoolean", setOf("type", "recid"));
        ret.put("AtomicString", setOf("type", "recid"));
        ret.put("AtomicVar", setOf("type", "recid", "serializer"));
        ret.put("IndexTreeList", setOf("type", "headerRecid", "serializer"));
        ret.put("IndexTreeLongLongMap", setOf("type", "headerRecid"));
        ret.put("Queue", setOf("type", "headerRecid", "serializer"));
        ret.put("Stack", setOf("type", "headerRecid", "serializer"));
        ret.put("CircularQueue", setOf("type", "headerRecid", "serializer"));
        return ret;
    }

    private static java.util.Set<String> setOf(String... values) {
        return java.util.Collections.unmodifiableSet(
                new java.util.HashSet<>(java.util.Arrays.asList(values)));
    }

    /**
     * Opens an existing collection by name, dispatching on its stored type. Only
     * collections whose codecs are registered built-ins can be opened this way;
     * a collection created with a custom serializer/format must be opened through
     * its typed maker with the codec re-supplied.
     */
    public Object get(String name) {
        String type = getType(name);
        if (type == null) throw new DBException.WrongConfiguration("no such collection: " + name);
        switch (type) {
            case "HashMap": return hashMap(name).open();
            case "HashSet": return hashSet(name).open();
            case "HashMap48": return hashMap48(name).open();
            case "HashSet48": return hashSet48(name).open();
            case "TreeMap": return treeMap(name).open();
            case "TreeSet": return treeSet(name).open();
            case "BufferTreeMap": return bufferTreeMap(name).open();
            case "SortedTableMap": return sortedTableMap(name).open();
            case "AtomicLong": return atomicLong(name).open();
            case "AtomicInteger": return atomicInteger(name).open();
            case "AtomicBoolean": return atomicBoolean(name).open();
            case "AtomicString": return atomicString(name).open();
            case "AtomicVar": return atomicVar(name).open();
            case "IndexTreeList": return indexTreeList(name).open();
            case "IndexTreeLongLongMap": return indexTreeLongLongMap(name).open();
            case "Queue": return queue(name).open();
            case "Stack": return stack(name).open();
            case "CircularQueue": return circularQueue(name).open();
            default: throw new DBException.DataCorruption("unknown collection type '" + type + "' for " + name);
        }
    }

    /**
     * Unlinks {@code name} from the catalog and best-effort frees its records.
     *
     * <p><b>Limitation:</b> mapdb5 collections have no {@code destroy()} primitive.
     * For maps/sets/lists this calls {@code clear()} (freeing entry records) but the
     * structural root/directory records are <em>leaked</em> until the store is
     * compacted or discarded. Atomic records are freed exactly. A collection created
     * with a custom codec that cannot be re-derived is unlinked without clearing.
     *
     * @return {@code true} if the collection existed
     */
    public boolean delete(String name) {
        lock.writeLock().lock();
        try {
            checkOpen();
            TreeMap<String, String> cat = catalogLoadInternal();
            String type = cat.get(name + "#type");
            if (type == null) return false;

            // Capture the teardown target BEFORE unlinking (params disappear on strip;
            // a collection object holds its own recids independent of the catalog).
            boolean atomic = isAtomicType(type);
            long atomicRecid = -1;
            Object obj = null;
            if (atomic) {
                String r = cat.get(name + "#recid");
                if (r != null) atomicRecid = Long.parseLong(r);
            } else {
                obj = instances.get(name);
                if (obj == null) {
                    try { obj = get(name); } catch (RuntimeException ignore) { /* custom codec: cannot clear */ }
                }
            }

            // Unlink FIRST so a subsequent teardown failure leaks records rather than
            // leaving the catalog pointing at destroyed data (crash-order safety).
            String prefix = name + "#";
            cat.keySet().removeIf(k -> k.equals(name + "#type") || k.startsWith(prefix));
            catalogSaveInternal(cat);
            instances.remove(name);
            cancelExpirationFor(obj);

            // THEN best-effort free the records.
            try {
                if (atomic) {
                    if (atomicRecid >= 0) store.delete(atomicRecid, Serializers.LONG); // freed by recid
                } else if (obj instanceof SortedTableMap) {
                    ((SortedTableMap<?, ?>) obj).deleteAllRecords();
                } else if (obj instanceof Map) {
                    ((Map<?, ?>) obj).clear();
                } else if (obj instanceof java.util.Collection) {
                    ((java.util.Collection<?>) obj).clear();
                } else if (obj instanceof IndexTreeLongLongMap) {
                    ((IndexTreeLongLongMap) obj).clear();
                }
            } catch (RuntimeException ignore) {
                // structural records leak; the collection is already unlinked
            } finally {
                closeRuntimeHandle(obj);
            }
            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private static boolean isAtomicType(String type) {
        switch (type) {
            case "AtomicLong":
            case "AtomicInteger":
            case "AtomicBoolean":
            case "AtomicString":
            case "AtomicVar":
                return true;
            default:
                return false;
        }
    }

    @SuppressWarnings("unchecked")
    private void cancelExpirationFor(Object object) {
        Object raw = object;
        if (raw instanceof MapBackedSet) raw = ((MapBackedSet<?>) raw).backingMap();
        if (raw instanceof CounterConcurrentMap) raw = ((CounterConcurrentMap<?, ?>) raw).delegate();
        ScheduledFuture<?> task = expirationTasks.remove(raw);
        if (task != null) task.cancel(false);
    }

    private void closeRuntimeHandle(Object object) {
        if (object instanceof PersistentBlockingQueue)
            ((PersistentBlockingQueue<?>) object).closeHandle();
    }

    /** Renames a collection (catalog-key rewrite only; recids and data are untouched). */
    public void rename(String oldName, String newName) {
        checkName(newName);
        lock.writeLock().lock();
        try {
            checkOpen();
            TreeMap<String, String> cat = catalogLoadInternal();
            if (!cat.containsKey(oldName + "#type")) {
                throw new DBException.WrongConfiguration("no such collection: " + oldName);
            }
            if (cat.containsKey(newName + "#type")) {
                throw new DBException.WrongConfiguration("target name already exists: " + newName);
            }
            String oldPrefix = oldName + "#";
            List<String> keys = new ArrayList<>(cat.keySet());
            for (String k : keys) {
                if (k.startsWith(oldPrefix)) {
                    String v = cat.remove(k);
                    cat.put(newName + "#" + k.substring(oldPrefix.length()), v);
                }
            }
            catalogSaveInternal(cat);
            Object live = instances.remove(oldName);
            if (live != null) instances.put(newName, live);
        } finally {
            lock.writeLock().unlock();
        }
    }

    static void checkName(String name) {
        if (name == null || name.isEmpty()) {
            throw new DBException.WrongConfiguration("collection name must be non-empty");
        }
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            boolean ok = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
                    || (c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-';
            if (!ok) {
                throw new DBException.WrongConfiguration(
                        "illegal collection name '" + name + "': only [A-Za-z0-9._-] allowed");
            }
        }
    }

    // ---- maker base -------------------------------------------------------

    /** Base class for the typed collection makers. */
    public abstract class Maker<E> {
        final String name;

        Maker(String name) { checkName(name); this.name = name; }

        abstract String type();
        abstract E create2(TreeMap<String, String> cat);
        abstract E open2(TreeMap<String, String> cat);

        /**
         * Validates that this maker's configuration is compatible with the stored
         * catalog entry, WITHOUT constructing a handle. Run on a cache hit so custom
         * codecs/hashers are still required and a mismatched codec still fails fast.
         * Default: nothing to check.
         */
        void checkConfig(TreeMap<String, String> cat) { }

        /** Re-apply runtime-only maker configuration to an already cached live handle. */
        void onCacheHit(E cached, TreeMap<String, String> cat) { }

        String key(String param) { return name + "#" + param; }

        /** Create; fails if the name already exists. */
        public E create() { return make2(Boolean.TRUE); }
        /** Open an existing collection; fails if the name does not exist. */
        public E open() { return make2(Boolean.FALSE); }
        /** Open if present, else create. */
        public E createOrOpen() { return make2(null); }

        @SuppressWarnings("unchecked")
        private E make2(Boolean createFlag) {
            lock.writeLock().lock();
            try {
                checkOpen();
                TreeMap<String, String> cat = catalogLoadInternal();
                String existingType = cat.get(name + "#type");
                boolean exists = existingType != null;

                if (createFlag == Boolean.TRUE && exists) {
                    throw new DBException.WrongConfiguration("collection already exists: " + name);
                }
                if (createFlag == Boolean.FALSE && !exists) {
                    throw new DBException.WrongConfiguration("no such collection: " + name);
                }

                if (exists) {
                    if (!existingType.equals(type())) {
                        throw new DBException.WrongConfiguration(
                                "collection '" + name + "' is a " + existingType + ", not a " + type());
                    }
                    Object cached = instances.get(name);
                    if (cached != null) {
                        checkConfig(cat); // enforce codec/hasher contract even on a cache hit
                        E result = (E) cached;
                        onCacheHit(result, cat);
                        return result;
                    }
                    E opened = open2(cat);
                    instances.put(name, opened);
                    return opened;
                }

                // create path: build records first, publish the catalog entry last
                E made = create2(cat);
                cat.put(name + "#type", type());
                catalogSaveInternal(cat);
                instances.put(name, made);
                return made;
            } finally {
                lock.writeLock().unlock();
            }
        }
    }

    // ---- HashMap ----------------------------------------------------------

    public <K, V> HashMapMaker<K, V> hashMap(String name) { return new HashMapMaker<>(name, null, null); }

    public <K, V> HashMapMaker<K, V> hashMap(String name, Serializer<K> keySer, Serializer<V> valueSer) {
        return new HashMapMaker<>(name, keySer, valueSer);
    }

    /**
     * Builder for a hash map. Returns a {@link ConcurrentMap} because the concrete backing
     * class depends on the configured options:
     * <ul>
     *   <li>plain {@link HTreeMap} (default, inline values);</li>
     *   <li>{@link HTreeMapExternal} when {@link #valueInline(boolean) valueInline(false)} — values
     *       stored in their own records;</li>
     *   <li>{@link HTreeCache} when any expiry option is set — a counted map with TTL / max-size /
     *       store-size eviction.</li>
     * </ul>
     *
     * <p>Note: HTreeMap hashes keys by object identity hash by default, which is wrong for
     * content-equal-but-distinct keys such as {@code byte[]}; pass a content hasher via
     * {@link #hasher} (e.g. {@code Hashers.mixing(Arrays::hashCode)}). A custom hasher must be
     * re-supplied identically on every {@code open}.
     *
     * <h3>mapdb5 vs mapdb3 divergences</h3>
     * <ul>
     *   <li><b>Single write-TTL.</b> mapdb5's cache has one TTL plus one access-order flag. A
     *       write-TTL ({@link #expireAfterCreate}/{@link #expireAfterUpdate}) and an access-TTL
     *       ({@link #expireAfterGet}) are mutually exclusive; {@code expireAfterCreate} and
     *       {@code expireAfterUpdate} may both be set only if equal.</li>
     *   <li><b>Expiry is fixed at creation.</b> All expiry parameters live in the cache's own
     *       persisted header. On reopen the maker's TTL/max-size/store-size options are ignored;
     *       only the runtime-only {@link #expireOverflow}/{@link #expireEvictionListener} are
     *       re-applied.</li>
     *   <li><b>Overflow is SPILLOVER.</b> {@link #expireOverflow} receives sweep-evicted entries and
     *       serves get() misses; it is runtime-only (never persisted).</li>
     *   <li>The no-argument mapdb3 TTL setters are omitted: a cache requires a positive TTL, so only
     *       the {@code (long)} / {@code (long, TimeUnit)} forms exist.</li>
     * </ul>
     */
    public final class HashMapMaker<K, V> extends Maker<ConcurrentMap<K, V>> {
        private Serializer<K> keySer;
        private Serializer<V> valueSer;
        private Hasher<? super K> hasher;
        private int concShift = 4, dirShift = 7, levels = 4;
        private long expireCreateTtl = 0;
        private long expireUpdateTtl = 0;
        private long expireGetTtl = 0;
        private long expireMaxSize = 0;
        private long expireStoreSize = 0;
        private long expireMaxEvictPerOp = 0;
        private Map<K, V> expireOverflow;
        private HTreeCache.EvictionListener<K, V> expireEvictionListener;
        private ScheduledExecutorService expireExecutor;
        private long expireExecutorPeriod = 10_000L;
        private Double expireCompactThreshold;
        private final List<MapModificationListener<K, V>> modificationListeners = new ArrayList<>();
        private java.util.function.Function<? super K, ? extends V> valueLoader;
        private boolean valueInline = true;
        private boolean counterEnable;
        private boolean hashSeedSet = false;
        private int hashSeed = 0;

        HashMapMaker(String name, Serializer<K> keySer, Serializer<V> valueSer) {
            super(name); this.keySer = keySer; this.valueSer = valueSer;
        }

        public HashMapMaker<K, V> keySerializer(Serializer<K> s) { this.keySer = s; return this; }
        public HashMapMaker<K, V> valueSerializer(Serializer<V> s) { this.valueSer = s; return this; }
        public HashMapMaker<K, V> hasher(Hasher<? super K> h) { this.hasher = h; return this; }
        public HashMapMaker<K, V> layout(int concShift, int dirShift, int levels) {
            this.concShift = concShift; this.dirShift = dirShift; this.levels = levels; return this;
        }

        /** Expire entries {@code ttlMillis} after they are created (write-TTL). Routes to a
         *  {@link HTreeCache}. Mutually exclusive with {@link #expireAfterGet}. */
        public HashMapMaker<K, V> expireAfterCreate(long ttlMillis) {
            this.expireCreateTtl = requirePositiveTtl(ttlMillis, "expireAfterCreate"); return this;
        }
        /** {@link #expireAfterCreate(long)} with a {@link TimeUnit}. */
        public HashMapMaker<K, V> expireAfterCreate(long ttl, TimeUnit unit) {
            return expireAfterCreate(unit.toMillis(ttl));
        }
        /** Expire entries {@code ttlMillis} after their last update (write-TTL). Must equal
         *  {@link #expireAfterCreate} if both are set. Routes to a {@link HTreeCache}. */
        public HashMapMaker<K, V> expireAfterUpdate(long ttlMillis) {
            this.expireUpdateTtl = requirePositiveTtl(ttlMillis, "expireAfterUpdate"); return this;
        }
        /** {@link #expireAfterUpdate(long)} with a {@link TimeUnit}. */
        public HashMapMaker<K, V> expireAfterUpdate(long ttl, TimeUnit unit) {
            return expireAfterUpdate(unit.toMillis(ttl));
        }
        /** Expire entries {@code ttlMillis} after their last access (access-TTL, LRU order).
         *  Routes to a {@link HTreeCache}. Mutually exclusive with the write-TTL setters. */
        public HashMapMaker<K, V> expireAfterGet(long ttlMillis) {
            this.expireGetTtl = requirePositiveTtl(ttlMillis, "expireAfterGet"); return this;
        }
        /** {@link #expireAfterGet(long)} with a {@link TimeUnit}. */
        public HashMapMaker<K, V> expireAfterGet(long ttl, TimeUnit unit) {
            return expireAfterGet(unit.toMillis(ttl));
        }
        /** Evict entries once the map holds more than {@code maxSize} of them. Routes to a
         *  {@link HTreeCache}. */
        public HashMapMaker<K, V> expireMaxSize(long maxSize) {
            this.expireMaxSize = requireNonNegative(maxSize, "expireMaxSize"); return this;
        }
        /** Evict entries once the backing store exceeds {@code storeSizeBytes} (needs a store that
         *  reports {@code getCurrentSize()}, e.g. StoreDirect). Routes to a {@link HTreeCache}. */
        public HashMapMaker<K, V> expireStoreSize(long storeSizeBytes) {
            this.expireStoreSize = requireNonNegative(storeSizeBytes, "expireStoreSize"); return this;
        }
        /** Cap the entries a single foreground operation will sweep-evict ({@code 0} = unbounded).
         *  Advanced mapdb5 throttle; only meaningful for an expiring cache. */
        public HashMapMaker<K, V> expireMaxEvictPerOp(long n) {
            this.expireMaxEvictPerOp = requireNonNegative(n, "expireMaxEvictPerOp"); return this;
        }
        /** SPILLOVER overflow map for an expiring cache: receives sweep-evicted entries and serves
         *  get() misses. Runtime-only (not persisted); re-supply on every open. Requires expiry. */
        public HashMapMaker<K, V> expireOverflow(Map<K, V> overflowMap) { this.expireOverflow = overflowMap; return this; }
        /** Eviction listener for an expiring cache. Runtime-only (not persisted); re-supply on every
         *  open. Requires expiry. */
        public HashMapMaker<K, V> expireEvictionListener(HTreeCache.EvictionListener<K, V> l) {
            this.expireEvictionListener = l; return this;
        }
        /** Run full expiry sweeps in the caller-owned executor. The task is cancelled on DB close. */
        public HashMapMaker<K, V> expireExecutor(ScheduledExecutorService executor) {
            this.expireExecutor = executor; return this;
        }
        /** Background sweep period in milliseconds (default 10 seconds). */
        public HashMapMaker<K, V> expireExecutorPeriod(long periodMillis) {
            if (periodMillis <= 0) throw new IllegalArgumentException("expireExecutorPeriod must be > 0");
            this.expireExecutorPeriod = periodMillis; return this;
        }
        /** Compact after a sweep frees at least this fraction of the store (0..1). */
        public HashMapMaker<K, V> expireCompactThreshold(double freeFraction) {
            if (!(freeFraction >= 0.0 && freeFraction <= 1.0) || Double.isNaN(freeFraction))
                throw new IllegalArgumentException("expireCompactThreshold must be between 0 and 1");
            this.expireCompactThreshold = freeFraction; return this;
        }
        public HashMapMaker<K, V> modificationListener(MapModificationListener<K, V> listener) {
            if (listener == null) throw new NullPointerException("listener");
            modificationListeners.add(listener);
            return this;
        }
        public HashMapMaker<K, V> valueLoader(
                java.util.function.Function<? super K, ? extends V> loader) {
            valueLoader = loader;
            return this;
        }
        public HashMapMaker<K, V> counterEnable() { counterEnable = true; return this; }
        /** {@code false} stores values in their own records ({@link HTreeMapExternal}, mapdb3's
         *  {@code valueInline(false)}); default {@code true}. External-value maps do not support
         *  expiry. */
        public HashMapMaker<K, V> valueInline(boolean inline) { this.valueInline = inline; return this; }
        /** Explicit hash seed, used only at create time (it is persisted in the map header and
         *  reconstructed on open). */
        public HashMapMaker<K, V> hashSeed(int seed) { this.hashSeed = seed; this.hashSeedSet = true; return this; }

        @Override String type() { return "HashMap"; }

        @Override ConcurrentMap<K, V> create2(TreeMap<String, String> cat) {
            if (keySer == null || valueSer == null) {
                throw new DBException.WrongConfiguration("hashMap " + name + " requires key and value serializers");
            }
            long writeTtl;
            if (expireCreateTtl > 0 && expireUpdateTtl > 0) {
                if (expireCreateTtl != expireUpdateTtl) {
                    throw new DBException.WrongConfiguration("hashMap " + name
                            + ": mapdb5 uses a single write-TTL; expireAfterCreate and expireAfterUpdate must be equal");
                }
                writeTtl = expireCreateTtl;
            } else if (expireCreateTtl > 0) {
                writeTtl = expireCreateTtl;
            } else if (expireUpdateTtl > 0) {
                writeTtl = expireUpdateTtl;
            } else {
                writeTtl = 0;
            }
            long getTtl = expireGetTtl;
            boolean anyExpiry = writeTtl > 0 || getTtl > 0 || expireMaxSize > 0 || expireStoreSize > 0;
            if (writeTtl > 0 && getTtl > 0) {
                throw new DBException.WrongConfiguration("hashMap " + name
                        + ": mapdb5 HTreeCache supports a single TTL; cannot combine a write-TTL"
                        + " (expireAfterCreate/expireAfterUpdate) with an access-TTL (expireAfterGet)");
            }
            if (!valueInline && anyExpiry) {
                throw new DBException.WrongConfiguration("hashMap " + name
                        + ": external-value maps (valueInline(false)) do not support expiry");
            }
            if ((expireOverflow != null || expireEvictionListener != null) && !anyExpiry) {
                throw new DBException.WrongConfiguration("hashMap " + name
                        + ": expireOverflow/expireEvictionListener require an expiring cache"
                        + " (set a TTL, expireMaxSize or expireStoreSize)");
            }
            if (expireMaxEvictPerOp > 0 && !anyExpiry) {
                throw new DBException.WrongConfiguration("hashMap " + name
                        + ": expireMaxEvictPerOp requires a TTL, expireMaxSize or expireStoreSize");
            }

            String kind;
            ConcurrentMap<K, V> m;
            long headerRecid;
            if (anyExpiry) {
                long ttl = getTtl > 0 ? getTtl : writeTtl;
                boolean accessOrder = getTtl > 0;
                HTreeCache<K, V> c = hashSeedSet
                        ? HTreeCache.create(store, keySer, valueSer, concShift, dirShift, levels,
                                ttl, accessOrder, expireMaxSize, expireStoreSize, expireMaxEvictPerOp, hashSeed, hasher)
                        : HTreeCache.create(store, keySer, valueSer, concShift, dirShift, levels,
                                ttl, accessOrder, expireMaxSize, expireStoreSize, expireMaxEvictPerOp, hasher);
                if (expireOverflow != null) c.overflow(expireOverflow);
                if (expireEvictionListener != null) c.evictionListener(expireEvictionListener);
                headerRecid = c.headerRecid();
                m = c;
                kind = "CACHE";
            } else if (!valueInline) {
                HTreeMapExternal<K, V> e = hashSeedSet
                        ? HTreeMapExternal.create(store, keySer, valueSer, concShift, dirShift, levels, hashSeed, hasher)
                        : HTreeMapExternal.create(store, keySer, valueSer, concShift, dirShift, levels, hasher);
                headerRecid = e.headerRecid();
                m = e;
                kind = "EXTERNAL";
            } else {
                HTreeMap<K, V> p = hashSeedSet
                        ? HTreeMap.create(store, keySer, valueSer, concShift, dirShift, levels, hashSeed, hasher)
                        : HTreeMap.create(store, keySer, valueSer, concShift, dirShift, levels, hasher);
                headerRecid = p.headerRecid();
                m = p;
                kind = "PLAIN";
            }
            cat.put(key("keySerializer"), serId(keySer));
            cat.put(key("valueSerializer"), serId(valueSer));
            cat.put(key("headerRecid"), Long.toString(headerRecid));
            cat.put(key("concShift"), Integer.toString(concShift));
            cat.put(key("dirShift"), Integer.toString(dirShift));
            cat.put(key("levels"), Integer.toString(levels));
            cat.put(key("hasher"), hasher == null ? "DEFAULT" : customMarker(hasher));
            cat.put(key("kind"), kind);
            long counterRecid = counterEnable && !anyExpiry
                    ? store.put(0L, Serializers.LONG) : 0L;
            cat.put(key("counterRecid"), Long.toString(counterRecid));
            if (counterRecid != 0) m = withCounter(m, counterRecid);
            configureRuntime(m);
            return m;
        }

        @Override ConcurrentMap<K, V> open2(TreeMap<String, String> cat) {
            long headerRecid = Long.parseLong(cat.get(key("headerRecid")));
            Serializer<K> ks = resolveSerializer(cat.get(key("keySerializer")), keySer);
            Serializer<V> vs = resolveSerializer(cat.get(key("valueSerializer")), valueSer);
            Hasher<? super K> h = resolveHasher(cat.get(key("hasher")), hasher);
            String kind = cat.getOrDefault(key("kind"), "PLAIN");
            long counterRecid = Long.parseLong(cat.getOrDefault(key("counterRecid"), "0"));
            switch (kind) {
                case "EXTERNAL":
                    ConcurrentMap<K, V> external = (h == null)
                            ? HTreeMapExternal.open(store, headerRecid, ks, vs)
                            : HTreeMapExternal.open(store, headerRecid, ks, vs, h);
                    if (counterRecid != 0) external = withCounter(external, counterRecid);
                    configureRuntime(external);
                    return external;
                case "CACHE": {
                    // Expiry params come from the persisted header, not the maker (fixed at create);
                    // only the runtime-only overflow/eviction-listener are re-applied here.
                    HTreeCache<K, V> c = (h == null) ? HTreeCache.open(store, headerRecid, ks, vs)
                                                     : HTreeCache.open(store, headerRecid, ks, vs, h);
                    if (expireOverflow != null) c.overflow(expireOverflow);
                    if (expireEvictionListener != null) c.evictionListener(expireEvictionListener);
                    ConcurrentMap<K, V> cache = c;
                    if (counterRecid != 0) cache = withCounter(cache, counterRecid);
                    configureRuntime(cache);
                    return cache;
                }
                case "PLAIN":
                    ConcurrentMap<K, V> map = (h == null)
                            ? HTreeMap.open(store, headerRecid, ks, vs)
                            : HTreeMap.open(store, headerRecid, ks, vs, h);
                    if (counterRecid != 0) map = withCounter(map, counterRecid);
                    configureRuntime(map);
                    return map;
                default:
                    throw new DBException.DataCorruption(
                            "unknown hashMap kind in catalog for '" + name + "': " + kind);
            }
        }

        @Override void checkConfig(TreeMap<String, String> cat) {
            resolveSerializer(cat.get(key("keySerializer")), keySer);
            resolveSerializer(cat.get(key("valueSerializer")), valueSer);
            resolveHasher(cat.get(key("hasher")), hasher);
            String kind = cat.getOrDefault(key("kind"), "PLAIN");
            if (!kind.equals("PLAIN") && !kind.equals("CACHE") && !kind.equals("EXTERNAL")) {
                throw new DBException.DataCorruption(
                        "unknown hashMap kind in catalog for '" + name + "': " + kind);
            }
            if ((expireOverflow != null || expireEvictionListener != null) && !kind.equals("CACHE")) {
                throw new DBException.WrongConfiguration("hashMap " + name
                        + ": expireOverflow/expireEvictionListener require an expiring cache");
            }
        }

        @Override void onCacheHit(ConcurrentMap<K, V> cached, TreeMap<String, String> cat) {
            configureRuntime(cached);
        }

        @SuppressWarnings("unchecked")
        private void configureRuntime(ConcurrentMap<K, V> map) {
            if (map instanceof CounterConcurrentMap)
                map = ((CounterConcurrentMap<K, V>) map).delegate();
            if (map instanceof HTreeCache) {
                HTreeCache<K, V> cache = (HTreeCache<K, V>) map;
                if (expireOverflow != null) cache.overflow(expireOverflow);
                if (expireEvictionListener != null) cache.evictionListener(expireEvictionListener);
                if (valueLoader != null) cache.valueLoader(valueLoader);
                for (MapModificationListener<K, V> listener : modificationListeners)
                    cache.modificationListenerAdd(listener);
                scheduleExpiration(cache, expireExecutor, expireExecutorPeriod, expireCompactThreshold);
            } else if (map instanceof HTreeMapExternal) {
                HTreeMapExternal<K, V> external = (HTreeMapExternal<K, V>) map;
                if (valueLoader != null) external.valueLoader(valueLoader);
                for (MapModificationListener<K, V> listener : modificationListeners)
                    external.modificationListenerAdd(listener);
            } else if (map instanceof HTreeMap) {
                HTreeMap<K, V> plain = (HTreeMap<K, V>) map;
                if (valueLoader != null) plain.valueLoader(valueLoader);
                for (MapModificationListener<K, V> listener : modificationListeners)
                    plain.modificationListenerAdd(listener);
            }
        }

        private ConcurrentMap<K, V> withCounter(ConcurrentMap<K, V> map, long counterRecid) {
            Atomic.Long counter = new Atomic.Long(store, counterRecid);
            MapModificationListener<K, V> listener = (key, oldValue, newValue, triggered) -> {
                if (oldValue == null && newValue != null) counter.incrementAndGet();
                else if (oldValue != null && newValue == null) counter.decrementAndGet();
            };
            if (map instanceof HTreeMap)
                ((HTreeMap<K, V>) map).modificationListenerAdd(listener);
            else if (map instanceof HTreeMapExternal)
                ((HTreeMapExternal<K, V>) map).modificationListenerAdd(listener);
            else if (map instanceof HTreeCache)
                ((HTreeCache<K, V>) map).modificationListenerAdd(listener);
            else throw new AssertionError("unsupported counted hash map " + map.getClass());
            return new CounterConcurrentMap<>(map, counter);
        }
    }

    private static long requirePositiveTtl(long ttlMillis, String what) {
        if (ttlMillis <= 0) {
            throw new DBException.WrongConfiguration(what + " requires a positive TTL (got " + ttlMillis + ")");
        }
        return ttlMillis;
    }

    private static long requireNonNegative(long value, String what) {
        if (value < 0) {
            throw new DBException.WrongConfiguration(what + " must be non-negative (got " + value + ")");
        }
        return value;
    }

    /** Resolves the stored hasher marker; returns null for DEFAULT, else the re-supplied custom hasher. */
    private static <K> Hasher<? super K> resolveHasher(String marker, Hasher<? super K> supplied) {
        if (marker == null || marker.equals("DEFAULT")) {
            if (supplied != null) {
                throw new DBException.WrongConfiguration(
                        "collection was created with the default hasher; do not supply a custom hasher on reopen");
            }
            return null;
        }
        if (isCustom(marker)) {
            if (supplied == null) {
                throw new DBException.WrongConfiguration(
                        "hash collection uses a custom hasher; re-supply it on the maker before opening");
            }
            checkCustomClass(marker, supplied);
            return supplied;
        }
        throw new DBException.DataCorruption("unknown hasher marker in catalog: " + marker);
    }

    private static String hasher64Id(Hasher64<?> hasher) {
        if (hasher == null) return "DEFAULT64";
        if (hasher == Hashers.LONG64) return "LONG64";
        if (hasher == Hashers.STRING64) return "STRING64";
        if (hasher == Hashers.BYTE_ARRAY64) return "BYTE_ARRAY64";
        return customMarker(hasher);
    }

    @SuppressWarnings("unchecked")
    private static <K> Hasher64<? super K> resolveHasher64(
            String marker, Hasher64<? super K> supplied) {
        Hasher64<?> builtin;
        switch (marker == null ? "DEFAULT64" : marker) {
            case "DEFAULT64": builtin = null; break;
            case "LONG64": builtin = Hashers.LONG64; break;
            case "STRING64": builtin = Hashers.STRING64; break;
            case "BYTE_ARRAY64": builtin = Hashers.BYTE_ARRAY64; break;
            default:
                if (!isCustom(marker)) {
                    throw new DBException.DataCorruption(
                            "unknown 64-bit hasher marker in catalog: " + marker);
                }
                if (supplied == null) {
                    throw new DBException.WrongConfiguration(
                            "48-bit hash collection uses a custom hasher; re-supply it before opening");
                }
                checkCustomClass(marker, supplied);
                return supplied;
        }
        if (supplied != null && hasher64Id(supplied).equals(marker) == false) {
            throw new DBException.WrongConfiguration(
                    "64-bit hasher mismatch for reopen: catalog='" + marker
                            + "', supplied='" + hasher64Id(supplied) + "'");
        }
        return supplied != null ? supplied : (Hasher64<? super K>) builtin;
    }

    // ---- HashMap48 --------------------------------------------------------

    public <K, V> HashMap48Maker<K, V> hashMap48(String name) {
        return new HashMap48Maker<>(name, null, null);
    }

    public <K, V> HashMap48Maker<K, V> hashMap48(
            String name, Serializer<K> keySer, Serializer<V> valueSer) {
        return new HashMap48Maker<>(name, keySer, valueSer);
    }

    /** Maker for the wide-hash {@link HTreeMap48}. */
    public final class HashMap48Maker<K, V> extends Maker<HTreeMap48<K, V>> {
        private Serializer<K> keySer;
        private Serializer<V> valueSer;
        private Hasher64<? super K> hasher;
        private int concShift = 6, dirShift = 6, levels = 7;
        private boolean hashSeedSet;
        private long hashSeed;
        private final List<MapModificationListener<K, V>> modificationListeners = new ArrayList<>();
        private java.util.function.Function<? super K, ? extends V> valueLoader;

        HashMap48Maker(String name, Serializer<K> keySer, Serializer<V> valueSer) {
            super(name);
            this.keySer = keySer;
            this.valueSer = valueSer;
        }

        public HashMap48Maker<K, V> keySerializer(Serializer<K> s) { keySer = s; return this; }
        public HashMap48Maker<K, V> valueSerializer(Serializer<V> s) { valueSer = s; return this; }
        public HashMap48Maker<K, V> hasher(Hasher64<? super K> h) { hasher = h; return this; }
        public HashMap48Maker<K, V> layout(int concurrencyShift, int directoryShift, int levelCount) {
            concShift = concurrencyShift;
            dirShift = directoryShift;
            levels = levelCount;
            return this;
        }
        public HashMap48Maker<K, V> hashSeed(long seed) {
            hashSeed = seed;
            hashSeedSet = true;
            return this;
        }
        public HashMap48Maker<K, V> modificationListener(MapModificationListener<K, V> listener) {
            if (listener == null) throw new NullPointerException("listener");
            modificationListeners.add(listener);
            return this;
        }
        public HashMap48Maker<K, V> valueLoader(
                java.util.function.Function<? super K, ? extends V> loader) {
            valueLoader = loader;
            return this;
        }

        @Override String type() { return "HashMap48"; }

        private void requireSerializers() {
            if (keySer == null || valueSer == null) {
                throw new DBException.WrongConfiguration(
                        "hashMap48 " + name + " requires key and value serializers");
            }
        }

        @Override HTreeMap48<K, V> create2(TreeMap<String, String> cat) {
            requireSerializers();
            HTreeMap48<K, V> map = hashSeedSet
                    ? HTreeMap48.create(store, keySer, valueSer, concShift, dirShift, levels,
                            hashSeed, hasher)
                    : HTreeMap48.create(store, keySer, valueSer, concShift, dirShift, levels, hasher);
            writeCatalog(cat, map);
            configureRuntime(map);
            return map;
        }

        @Override HTreeMap48<K, V> open2(TreeMap<String, String> cat) {
            Serializer<K> ks = resolveSerializer(cat.get(key("keySerializer")), keySer);
            Serializer<V> vs = resolveSerializer(cat.get(key("valueSerializer")), valueSer);
            Hasher64<? super K> h = resolveHasher64(cat.get(key("hasher64")), hasher);
            HTreeMap48<K, V> map = HTreeMap48.open(store,
                    Long.parseLong(cat.get(key("headerRecid"))), ks, vs, h);
            configureRuntime(map);
            return map;
        }

        @Override void checkConfig(TreeMap<String, String> cat) {
            resolveSerializer(cat.get(key("keySerializer")), keySer);
            resolveSerializer(cat.get(key("valueSerializer")), valueSer);
            resolveHasher64(cat.get(key("hasher64")), hasher);
        }

        @Override void onCacheHit(HTreeMap48<K, V> cached, TreeMap<String, String> cat) {
            configureRuntime(cached);
        }

        private void configureRuntime(HTreeMap48<K, V> map) {
            if (valueLoader != null) map.valueLoader(valueLoader);
            for (MapModificationListener<K, V> listener : modificationListeners)
                map.modificationListenerAdd(listener);
        }

        private void writeCatalog(TreeMap<String, String> cat, HTreeMap48<K, V> map) {
            cat.put(key("keySerializer"), serId(keySer));
            cat.put(key("valueSerializer"), serId(valueSer));
            cat.put(key("headerRecid"), Long.toString(map.headerRecid()));
            cat.put(key("hasher64"), hasher64Id(hasher));
            cat.put(key("concShift"), Integer.toString(concShift));
            cat.put(key("dirShift"), Integer.toString(dirShift));
            cat.put(key("levels"), Integer.toString(levels));
        }

        /**
         * Bulk-build from entries sorted by the unsigned 64-bit hash. An explicit
         * {@link #hashSeed(long)} is required so callers can compute the same order.
         */
        public HTreeMap48<K, V> createFromSortedByHash(
                Iterator<? extends Map.Entry<K, V>> sortedEntries) {
            lock.writeLock().lock();
            try {
                checkOpen();
                requireSerializers();
                if (!hashSeedSet) {
                    throw new DBException.WrongConfiguration(
                            "hashMap48 bulk creation requires an explicit hashSeed");
                }
                TreeMap<String, String> cat = catalogLoadInternal();
                if (cat.containsKey(name + "#type")) {
                    throw new DBException.WrongConfiguration("collection already exists: " + name);
                }
                HTreeMap48<K, V> map = HTreeMap48.createFromSortedByHash(store, keySer, valueSer,
                        concShift, dirShift, levels, hashSeed, hasher, sortedEntries);
                writeCatalog(cat, map);
                configureRuntime(map);
                cat.put(name + "#type", type());
                catalogSaveInternal(cat);
                instances.put(name, map);
                return map;
            } finally {
                lock.writeLock().unlock();
            }
        }
    }

    public <E> HashSet48Maker<E> hashSet48(String name) {
        return new HashSet48Maker<>(name, null);
    }

    public <E> HashSet48Maker<E> hashSet48(String name, Serializer<E> serializer) {
        return new HashSet48Maker<>(name, serializer);
    }

    /** Set facade over {@link HTreeMap48}. */
    public final class HashSet48Maker<E> extends Maker<java.util.Set<E>> {
        private Serializer<E> serializer;
        private Hasher64<? super E> hasher;
        private int concShift = 6, dirShift = 6, levels = 7;
        private boolean hashSeedSet;
        private long hashSeed;

        HashSet48Maker(String name, Serializer<E> serializer) {
            super(name);
            this.serializer = serializer;
        }

        public HashSet48Maker<E> serializer(Serializer<E> s) { serializer = s; return this; }
        public HashSet48Maker<E> hasher(Hasher64<? super E> h) { hasher = h; return this; }
        public HashSet48Maker<E> layout(int concurrencyShift, int directoryShift, int levelCount) {
            concShift = concurrencyShift;
            dirShift = directoryShift;
            levels = levelCount;
            return this;
        }
        public HashSet48Maker<E> hashSeed(long seed) {
            hashSeed = seed;
            hashSeedSet = true;
            return this;
        }

        @Override String type() { return "HashSet48"; }

        private void requireSerializer() {
            if (serializer == null) {
                throw new DBException.WrongConfiguration(
                        "hashSet48 " + name + " requires a serializer");
            }
        }

        @Override java.util.Set<E> create2(TreeMap<String, String> cat) {
            requireSerializer();
            HTreeMap48<E, Object> map = hashSeedSet
                    ? HTreeMap48.create(store, serializer, NoValueSerializer.INSTANCE,
                            concShift, dirShift, levels, hashSeed, hasher)
                    : HTreeMap48.create(store, serializer, NoValueSerializer.INSTANCE,
                            concShift, dirShift, levels, hasher);
            writeCatalog(cat, map);
            return new MapBackedSet<>(map);
        }

        @Override java.util.Set<E> open2(TreeMap<String, String> cat) {
            Serializer<E> ser = resolveSerializer(cat.get(key("serializer")), serializer);
            Hasher64<? super E> h = resolveHasher64(cat.get(key("hasher64")), hasher);
            HTreeMap48<E, Object> map = HTreeMap48.open(store,
                    Long.parseLong(cat.get(key("headerRecid"))), ser,
                    NoValueSerializer.INSTANCE, h);
            return new MapBackedSet<>(map);
        }

        @Override void checkConfig(TreeMap<String, String> cat) {
            resolveSerializer(cat.get(key("serializer")), serializer);
            resolveHasher64(cat.get(key("hasher64")), hasher);
        }

        private void writeCatalog(TreeMap<String, String> cat, HTreeMap48<E, Object> map) {
            cat.put(key("serializer"), serId(serializer));
            cat.put(key("headerRecid"), Long.toString(map.headerRecid()));
            cat.put(key("hasher64"), hasher64Id(hasher));
            cat.put(key("concShift"), Integer.toString(concShift));
            cat.put(key("dirShift"), Integer.toString(dirShift));
            cat.put(key("levels"), Integer.toString(levels));
        }

        public java.util.Set<E> createFromSortedByHash(Iterator<E> sortedElements) {
            if (!hashSeedSet) {
                throw new DBException.WrongConfiguration(
                        "hashSet48 bulk creation requires an explicit hashSeed");
            }
            Iterator<Map.Entry<E, Object>> entries = new Iterator<>() {
                @Override public boolean hasNext() { return sortedElements.hasNext(); }
                @Override public Map.Entry<E, Object> next() {
                    return new AbstractMap.SimpleImmutableEntry<>(
                            sortedElements.next(), NoValueSerializer.NONE);
                }
            };
            lock.writeLock().lock();
            try {
                checkOpen();
                requireSerializer();
                TreeMap<String, String> cat = catalogLoadInternal();
                if (cat.containsKey(name + "#type")) {
                    throw new DBException.WrongConfiguration("collection already exists: " + name);
                }
                HTreeMap48<E, Object> map = HTreeMap48.createFromSortedByHash(store,
                        serializer, NoValueSerializer.INSTANCE, concShift, dirShift, levels,
                        hashSeed, hasher, entries);
                writeCatalog(cat, map);
                cat.put(name + "#type", type());
                catalogSaveInternal(cat);
                java.util.Set<E> set = new MapBackedSet<>(map);
                instances.put(name, set);
                return set;
            } finally {
                lock.writeLock().unlock();
            }
        }
    }

    // ---- HashSet ----------------------------------------------------------

    public <E> HashSetMaker<E> hashSet(String name) { return new HashSetMaker<>(name, null); }

    public <E> HashSetMaker<E> hashSet(String name, Serializer<E> ser) { return new HashSetMaker<>(name, ser); }

    /**
     * Builder for a hash set. Backed by a plain {@link HTreeMap} or, when any expiry option is set,
     * a {@link HTreeCache} — both keyed on {@code E} with a zero-byte {@link NoValueSerializer}
     * value. The expiry surface mirrors mapdb3's {@code HashSetMaker} (no update-TTL, no external
     * values, no overflow). The single-TTL / expiry-fixed-at-create divergences from
     * {@link HashMapMaker} apply here too.
     */
    public final class HashSetMaker<E> extends Maker<java.util.Set<E>> {
        private Serializer<E> serializer;
        private Hasher<? super E> hasher;
        private int concShift = 4, dirShift = 7, levels = 4;
        private long expireCreateTtl = 0;
        private long expireGetTtl = 0;
        private long expireMaxSize = 0;
        private long expireStoreSize = 0;
        private long expireMaxEvictPerOp = 0;
        private ScheduledExecutorService expireExecutor;
        private long expireExecutorPeriod = 10_000L;
        private Double expireCompactThreshold;
        private boolean hashSeedSet = false;
        private int hashSeed = 0;
        private boolean counterEnable;

        HashSetMaker(String name, Serializer<E> ser) { super(name); this.serializer = ser; }

        public HashSetMaker<E> serializer(Serializer<E> s) { this.serializer = s; return this; }
        public HashSetMaker<E> hasher(Hasher<? super E> h) { this.hasher = h; return this; }
        public HashSetMaker<E> layout(int concShift, int dirShift, int levels) {
            this.concShift = concShift; this.dirShift = dirShift; this.levels = levels; return this;
        }
        /** Expire elements {@code ttlMillis} after creation (write-TTL). Routes to a
         *  {@link HTreeCache}. Mutually exclusive with {@link #expireAfterGet}. */
        public HashSetMaker<E> expireAfterCreate(long ttlMillis) {
            this.expireCreateTtl = requirePositiveTtl(ttlMillis, "expireAfterCreate"); return this;
        }
        /** {@link #expireAfterCreate(long)} with a {@link TimeUnit}. */
        public HashSetMaker<E> expireAfterCreate(long ttl, TimeUnit unit) {
            return expireAfterCreate(unit.toMillis(ttl));
        }
        /** Expire elements {@code ttlMillis} after their last access (access-TTL). Routes to a
         *  {@link HTreeCache}. Mutually exclusive with {@link #expireAfterCreate}. */
        public HashSetMaker<E> expireAfterGet(long ttlMillis) {
            this.expireGetTtl = requirePositiveTtl(ttlMillis, "expireAfterGet"); return this;
        }
        /** {@link #expireAfterGet(long)} with a {@link TimeUnit}. */
        public HashSetMaker<E> expireAfterGet(long ttl, TimeUnit unit) {
            return expireAfterGet(unit.toMillis(ttl));
        }
        /** Evict once the set holds more than {@code maxSize} elements. Routes to a {@link HTreeCache}. */
        public HashSetMaker<E> expireMaxSize(long maxSize) {
            this.expireMaxSize = requireNonNegative(maxSize, "expireMaxSize"); return this;
        }
        /** Evict once the backing store exceeds {@code storeSizeBytes}. Routes to a {@link HTreeCache}. */
        public HashSetMaker<E> expireStoreSize(long storeSizeBytes) {
            this.expireStoreSize = requireNonNegative(storeSizeBytes, "expireStoreSize"); return this;
        }
        /** Cap the entries a single foreground operation will sweep-evict ({@code 0} = unbounded). */
        public HashSetMaker<E> expireMaxEvictPerOp(long n) {
            this.expireMaxEvictPerOp = requireNonNegative(n, "expireMaxEvictPerOp"); return this;
        }
        public HashSetMaker<E> expireExecutor(ScheduledExecutorService executor) {
            this.expireExecutor = executor; return this;
        }
        public HashSetMaker<E> expireExecutorPeriod(long periodMillis) {
            if (periodMillis <= 0) throw new IllegalArgumentException("expireExecutorPeriod must be > 0");
            this.expireExecutorPeriod = periodMillis; return this;
        }
        public HashSetMaker<E> expireCompactThreshold(double freeFraction) {
            if (!(freeFraction >= 0.0 && freeFraction <= 1.0) || Double.isNaN(freeFraction))
                throw new IllegalArgumentException("expireCompactThreshold must be between 0 and 1");
            this.expireCompactThreshold = freeFraction; return this;
        }
        /** Explicit hash seed, used only at create time (persisted in the header). */
        public HashSetMaker<E> hashSeed(int seed) { this.hashSeed = seed; this.hashSeedSet = true; return this; }
        public HashSetMaker<E> counterEnable() { counterEnable = true; return this; }

        @Override String type() { return "HashSet"; }

        @Override java.util.Set<E> create2(TreeMap<String, String> cat) {
            if (serializer == null) {
                throw new DBException.WrongConfiguration("hashSet " + name + " requires a serializer");
            }
            long writeTtl = expireCreateTtl;
            long getTtl = expireGetTtl;
            boolean anyExpiry = writeTtl > 0 || getTtl > 0 || expireMaxSize > 0 || expireStoreSize > 0;
            if (writeTtl > 0 && getTtl > 0) {
                throw new DBException.WrongConfiguration("hashSet " + name
                        + ": mapdb5 HTreeCache supports a single TTL; cannot combine expireAfterCreate with expireAfterGet");
            }
            if (expireMaxEvictPerOp > 0 && !anyExpiry) {
                throw new DBException.WrongConfiguration("hashSet " + name
                        + ": expireMaxEvictPerOp requires a TTL, expireMaxSize or expireStoreSize");
            }
            ConcurrentMap<E, Object> m;
            String kind;
            long headerRecid;
            if (anyExpiry) {
                long ttl = getTtl > 0 ? getTtl : writeTtl;
                boolean accessOrder = getTtl > 0;
                HTreeCache<E, Object> c = hashSeedSet
                        ? HTreeCache.create(store, serializer, NoValueSerializer.INSTANCE, concShift, dirShift, levels,
                                ttl, accessOrder, expireMaxSize, expireStoreSize, expireMaxEvictPerOp, hashSeed, hasher)
                        : HTreeCache.create(store, serializer, NoValueSerializer.INSTANCE, concShift, dirShift, levels,
                                ttl, accessOrder, expireMaxSize, expireStoreSize, expireMaxEvictPerOp, hasher);
                headerRecid = c.headerRecid();
                m = c;
                kind = "CACHE";
            } else {
                HTreeMap<E, Object> p = hashSeedSet
                        ? HTreeMap.create(store, serializer, NoValueSerializer.INSTANCE, concShift, dirShift, levels, hashSeed, hasher)
                        : (hasher == null
                                ? HTreeMap.create(store, serializer, NoValueSerializer.INSTANCE, concShift, dirShift, levels)
                                : HTreeMap.create(store, serializer, NoValueSerializer.INSTANCE, concShift, dirShift, levels, hasher));
                headerRecid = p.headerRecid();
                m = p;
                kind = "PLAIN";
            }
            cat.put(key("serializer"), serId(serializer));
            cat.put(key("headerRecid"), Long.toString(headerRecid));
            cat.put(key("concShift"), Integer.toString(concShift));
            cat.put(key("dirShift"), Integer.toString(dirShift));
            cat.put(key("levels"), Integer.toString(levels));
            cat.put(key("hasher"), hasher == null ? "DEFAULT" : customMarker(hasher));
            cat.put(key("kind"), kind);
            long counterRecid = counterEnable && !anyExpiry
                    ? store.put(0L, Serializers.LONG) : 0L;
            cat.put(key("counterRecid"), Long.toString(counterRecid));
            if (counterRecid != 0) m = withCounter(m, counterRecid);
            configureRuntime(m);
            return new MapBackedSet<>(m);
        }

        @Override java.util.Set<E> open2(TreeMap<String, String> cat) {
            long headerRecid = Long.parseLong(cat.get(key("headerRecid")));
            Serializer<E> s = resolveSerializer(cat.get(key("serializer")), serializer);
            Hasher<? super E> h = resolveHasher(cat.get(key("hasher")), hasher);
            String kind = cat.getOrDefault(key("kind"), "PLAIN");
            ConcurrentMap<E, Object> m;
            switch (kind) {
                case "CACHE":
                    m = (h == null) ? HTreeCache.open(store, headerRecid, s, NoValueSerializer.INSTANCE)
                                    : HTreeCache.open(store, headerRecid, s, NoValueSerializer.INSTANCE, h);
                    break;
                case "PLAIN":
                    m = (h == null) ? HTreeMap.open(store, headerRecid, s, NoValueSerializer.INSTANCE)
                                    : HTreeMap.open(store, headerRecid, s, NoValueSerializer.INSTANCE, h);
                    break;
                default:
                    throw new DBException.DataCorruption(
                            "unknown hashSet kind in catalog for '" + name + "': " + kind);
            }
            long counterRecid = Long.parseLong(cat.getOrDefault(key("counterRecid"), "0"));
            if (counterRecid != 0) m = withCounter(m, counterRecid);
            configureRuntime(m);
            return new MapBackedSet<>(m);
        }

        @Override void onCacheHit(java.util.Set<E> cached, TreeMap<String, String> cat) {
            @SuppressWarnings("unchecked")
            ConcurrentMap<E, Object> map = (ConcurrentMap<E, Object>)
                    ((MapBackedSet<E>) cached).backingMap();
            configureRuntime(map);
        }

        @SuppressWarnings("unchecked")
        private void configureRuntime(ConcurrentMap<E, Object> map) {
            if (map instanceof CounterConcurrentMap)
                map = ((CounterConcurrentMap<E, Object>) map).delegate();
            if (map instanceof HTreeCache)
                scheduleExpiration((HTreeCache<E, Object>) map, expireExecutor,
                        expireExecutorPeriod, expireCompactThreshold);
        }

        @Override void checkConfig(TreeMap<String, String> cat) {
            resolveSerializer(cat.get(key("serializer")), serializer);
            resolveHasher(cat.get(key("hasher")), hasher);
        }

        @SuppressWarnings("unchecked")
        private ConcurrentMap<E, Object> withCounter(
                ConcurrentMap<E, Object> map, long counterRecid) {
            Atomic.Long counter = new Atomic.Long(store, counterRecid);
            MapModificationListener<E, Object> listener = (key, oldValue, newValue, triggered) -> {
                if (oldValue == null && newValue != null) counter.incrementAndGet();
                else if (oldValue != null && newValue == null) counter.decrementAndGet();
            };
            if (map instanceof HTreeMap)
                ((HTreeMap<E, Object>) map).modificationListenerAdd(listener);
            else throw new AssertionError("unsupported counted hash set " + map.getClass());
            return new CounterConcurrentMap<>(map, counter);
        }
    }

    // ---- TreeMap ----------------------------------------------------------

    static final GroupFormat<Object> NO_VALUE_FORMAT = new ObjectArrayFormat<>(NoValueSerializer.INSTANCE);
    private static final int DEFAULT_MAX_NODE_SIZE = 32;

    public <K, V> TreeMapMaker<K, V> treeMap(String name) { return new TreeMapMaker<>(name, null, null); }

    public <K, V> TreeMapMaker<K, V> treeMap(String name, GroupFormat<K> keyFmt, GroupFormat<V> valueFmt) {
        return new TreeMapMaker<>(name, keyFmt, valueFmt);
    }

    public final class TreeMapMaker<K, V> extends Maker<BTreeMap<K, V>> {
        private GroupFormat<K> keyFmt;
        private GroupFormat<V> valueFmt;
        private int maxNodeSize = DEFAULT_MAX_NODE_SIZE;
        private boolean counterEnable = false;
        private boolean valueInline = true;
        private final List<BTreeMap.ModificationListener<K, V>> modListeners = new ArrayList<>();

        TreeMapMaker(String name, GroupFormat<K> keyFmt, GroupFormat<V> valueFmt) {
            super(name); this.keyFmt = keyFmt; this.valueFmt = valueFmt;
        }

        public TreeMapMaker<K, V> keySerializer(GroupFormat<K> f) { this.keyFmt = f; return this; }
        public TreeMapMaker<K, V> valueSerializer(GroupFormat<V> f) { this.valueFmt = f; return this; }
        public TreeMapMaker<K, V> maxNodeSize(int n) { this.maxNodeSize = n; return this; }
        /** Maintain an O(1) size counter (Feature A). Persisted; {@code size()} becomes O(1). */
        public TreeMapMaker<K, V> counterEnable() { this.counterEnable = true; return this; }
        /** Store values in separate records; BTree leaf nodes contain only value recids. */
        public TreeMapMaker<K, V> valuesOutsideNodesEnable() {
            this.valueInline = false; return this;
        }
        /** Register a modification listener. Runtime-only (not persisted); re-applied on every
         *  create/open of this map through this maker. */
        public TreeMapMaker<K, V> modificationListener(BTreeMap.ModificationListener<K, V> l) {
            if (l == null) throw new NullPointerException("listener");
            this.modListeners.add(l); return this;
        }
        /** MapDB 3-compatible listener signature; BTree user mutations are never triggered expiry. */
        public TreeMapMaker<K, V> modificationListener(MapModificationListener<K, V> listener) {
            if (listener == null) throw new NullPointerException("listener");
            this.modListeners.add((key, oldValue, newValue) ->
                    listener.modify(key, oldValue, newValue, false));
            return this;
        }

        @Override String type() { return "TreeMap"; }

        @Override BTreeMap<K, V> create2(TreeMap<String, String> cat) {
            if (keyFmt == null || valueFmt == null) {
                throw new DBException.WrongConfiguration("treeMap " + name + " requires key and value formats");
            }
            BTreeMap<K, V> m = valueInline
                    ? BTreeMap.create(store, keyFmt, valueFmt, maxNodeSize, counterEnable)
                    : BTreeMap.createExternalValues(store, keyFmt, valueFmt, maxNodeSize, counterEnable);
            writeCatalog(cat, m);
            for (BTreeMap.ModificationListener<K, V> l : modListeners) m.addModificationListener(l);
            return m;
        }

        @Override BTreeMap<K, V> open2(TreeMap<String, String> cat) {
            long rrr = Long.parseLong(cat.get(key("rootRecidRecid")));
            int mns = Integer.parseInt(cat.get(key("maxNodeSize")));
            long counterRecid = Long.parseLong(cat.getOrDefault(key("counterRecid"), "0"));
            GroupFormat<K> kf = resolveFormat(cat.get(key("keySerializer")), keyFmt);
            GroupFormat<V> vf = resolveFormat(cat.get(key("valueSerializer")), valueFmt);
            String inlineText = cat.getOrDefault(key("valueInline"), "true");
            if (!inlineText.equals("true") && !inlineText.equals("false"))
                throw new DBException.DataCorruption("invalid valueInline for treeMap '"
                        + name + "': " + inlineText);
            boolean inline = inlineText.equals("true");
            BTreeMap<K, V> m = inline
                    ? BTreeMap.open(store, rrr, kf, vf, mns, counterRecid)
                    : BTreeMap.openExternalValues(store, rrr, kf, vf, mns, counterRecid);
            for (BTreeMap.ModificationListener<K, V> l : modListeners) m.addModificationListener(l);
            return m;
        }

        @Override void checkConfig(TreeMap<String, String> cat) {
            resolveFormat(cat.get(key("keySerializer")), keyFmt);
            resolveFormat(cat.get(key("valueSerializer")), valueFmt);
        }

        @Override void onCacheHit(BTreeMap<K, V> cached, TreeMap<String, String> cat) {
            for (BTreeMap.ModificationListener<K, V> l : modListeners) {
                cached.addModificationListener(l);
            }
        }

        private void writeCatalog(TreeMap<String, String> cat, BTreeMap<K, V> m) {
            cat.put(key("keySerializer"), fmtId(keyFmt));
            cat.put(key("valueSerializer"), fmtId(valueFmt));
            cat.put(key("rootRecidRecid"), Long.toString(m.rootRecidRecid()));
            cat.put(key("maxNodeSize"), Integer.toString(maxNodeSize));
            cat.put(key("counterRecid"), Long.toString(m.counterRecid()));
            cat.put(key("valueInline"), Boolean.toString(valueInline));
        }

        /**
         * Bulk-build a new tree map from entries already sorted by key (strictly ascending),
         * atomically create-and-register it, and return the live handle. Fails if the name already
         * exists. Uses {@link BTreeMap#createFromSorted} (Pump), honouring {@link #counterEnable()},
         * and re-applies any registered {@link #modificationListener modification listeners}.
         *
         * @throws DBException.NotSorted if the input keys are not strictly ascending
         */
        public BTreeMap<K, V> createFrom(Iterator<? extends Map.Entry<K, V>> presortedByKey) {
            lock.writeLock().lock();
            try {
                checkOpen();
                if (keyFmt == null || valueFmt == null) {
                    throw new DBException.WrongConfiguration("treeMap " + name + " requires key and value formats");
                }
                if (!valueInline) throw new UnsupportedOperationException(
                        "bulk build is not yet supported with valuesOutsideNodesEnable()");
                TreeMap<String, String> cat = catalogLoadInternal();
                if (cat.containsKey(name + "#type")) {
                    throw new DBException.WrongConfiguration("collection already exists: " + name);
                }
                BTreeMap<K, V> m = BTreeMap.createFromSorted(store, keyFmt, valueFmt, maxNodeSize,
                        presortedByKey, counterEnable);
                writeCatalog(cat, m);
                cat.put(name + "#type", type());
                catalogSaveInternal(cat);
                for (BTreeMap.ModificationListener<K, V> l : modListeners) m.addModificationListener(l);
                instances.put(name, m);
                return m;
            } finally {
                lock.writeLock().unlock();
            }
        }

        /** {@link #createFrom(Iterator)} from a {@link SortedMap} (uses its entry-set iterator; the
         *  map must be ordered consistently with {@code keySerializer}). */
        public BTreeMap<K, V> createFrom(SortedMap<K, V> source) {
            return createFrom(source.entrySet().iterator());
        }

        /**
         * Start a streaming sorted bulk build. Feed strictly ascending entries, then
         * call {@link DbSink#create()} exactly once to publish the completed map in the
         * name catalog.
         */
        public DbSink createFromSink() {
            lock.writeLock().lock();
            try {
                checkOpen();
                if (keyFmt == null || valueFmt == null) {
                    throw new DBException.WrongConfiguration("treeMap " + name + " requires key and value formats");
                }
                if (!valueInline) throw new UnsupportedOperationException(
                        "streaming build is not yet supported with valuesOutsideNodesEnable()");
                if (catalogLoadInternal().containsKey(name + "#type")) {
                    throw new DBException.WrongConfiguration("collection already exists: " + name);
                }
                GroupFormat<K> builtKeyFmt = keyFmt;
                GroupFormat<V> builtValueFmt = valueFmt;
                int builtMaxNodeSize = maxNodeSize;
                boolean builtCounterEnable = counterEnable;
                List<BTreeMap.ModificationListener<K, V>> builtListeners =
                        new ArrayList<>(modListeners);
                BTreeMap.Sink<K, V> underlying = BTreeMap.createFromSink(store,
                        builtKeyFmt, builtValueFmt, builtMaxNodeSize, builtCounterEnable);
                return new DbSink(underlying, builtKeyFmt, builtValueFmt,
                        builtMaxNodeSize, builtListeners);
            } finally {
                lock.writeLock().unlock();
            }
        }

        public final class DbSink {
            private final BTreeMap.Sink<K, V> underlying;
            private final GroupFormat<K> builtKeyFmt;
            private final GroupFormat<V> builtValueFmt;
            private final int builtMaxNodeSize;
            private final List<BTreeMap.ModificationListener<K, V>> builtListeners;

            private DbSink(BTreeMap.Sink<K, V> underlying, GroupFormat<K> builtKeyFmt,
                           GroupFormat<V> builtValueFmt, int builtMaxNodeSize,
                           List<BTreeMap.ModificationListener<K, V>> builtListeners) {
                this.underlying = underlying;
                this.builtKeyFmt = builtKeyFmt;
                this.builtValueFmt = builtValueFmt;
                this.builtMaxNodeSize = builtMaxNodeSize;
                this.builtListeners = builtListeners;
            }

            public DbSink put(K key, V value) {
                underlying.put(key, value);
                return this;
            }

            public BTreeMap<K, V> create() {
                lock.writeLock().lock();
                try {
                    checkOpen();
                    TreeMap<String, String> cat = catalogLoadInternal();
                    if (cat.containsKey(name + "#type")) {
                        throw new DBException.WrongConfiguration("collection already exists: " + name);
                    }
                    BTreeMap<K, V> map = underlying.create();
                    cat.put(key("keySerializer"), fmtId(builtKeyFmt));
                    cat.put(key("valueSerializer"), fmtId(builtValueFmt));
                    cat.put(key("rootRecidRecid"), Long.toString(map.rootRecidRecid()));
                    cat.put(key("maxNodeSize"), Integer.toString(builtMaxNodeSize));
                    cat.put(key("counterRecid"), Long.toString(map.counterRecid()));
                    cat.put(name + "#type", type());
                    catalogSaveInternal(cat);
                    for (BTreeMap.ModificationListener<K, V> listener : builtListeners) {
                        map.addModificationListener(listener);
                    }
                    instances.put(name, map);
                    return map;
                } finally {
                    lock.writeLock().unlock();
                }
            }
        }
    }

    // ---- TreeSet ----------------------------------------------------------

    public <E> TreeSetMaker<E> treeSet(String name) { return new TreeSetMaker<>(name, null); }

    public <E> TreeSetMaker<E> treeSet(String name, GroupFormat<E> keyFmt) { return new TreeSetMaker<>(name, keyFmt); }

    public final class TreeSetMaker<E> extends Maker<NavigableSet<E>> {
        private GroupFormat<E> keyFmt;
        private int maxNodeSize = DEFAULT_MAX_NODE_SIZE;
        private boolean counterEnable = false;

        TreeSetMaker(String name, GroupFormat<E> keyFmt) { super(name); this.keyFmt = keyFmt; }

        public TreeSetMaker<E> serializer(GroupFormat<E> f) { this.keyFmt = f; return this; }
        public TreeSetMaker<E> maxNodeSize(int n) { this.maxNodeSize = n; return this; }
        /** Maintain an O(1) size counter (Feature A). Persisted; {@code size()} becomes O(1). */
        public TreeSetMaker<E> counterEnable() { this.counterEnable = true; return this; }

        @Override String type() { return "TreeSet"; }

        @Override NavigableSet<E> create2(TreeMap<String, String> cat) {
            if (keyFmt == null) {
                throw new DBException.WrongConfiguration("treeSet " + name + " requires a format");
            }
            BTreeMap<E, Object> m = BTreeMap.create(store, keyFmt, NO_VALUE_FORMAT, maxNodeSize, counterEnable);
            cat.put(key("serializer"), fmtId(keyFmt));
            cat.put(key("rootRecidRecid"), Long.toString(m.rootRecidRecid()));
            cat.put(key("maxNodeSize"), Integer.toString(maxNodeSize));
            cat.put(key("counterRecid"), Long.toString(m.counterRecid()));
            return new MapBackedNavigableSet<>(m);
        }

        @Override NavigableSet<E> open2(TreeMap<String, String> cat) {
            long rrr = Long.parseLong(cat.get(key("rootRecidRecid")));
            int mns = Integer.parseInt(cat.get(key("maxNodeSize")));
            long counterRecid = Long.parseLong(cat.getOrDefault(key("counterRecid"), "0"));
            GroupFormat<E> kf = resolveFormat(cat.get(key("serializer")), keyFmt);
            ConcurrentNavigableMap<E, Object> m = BTreeMap.open(store, rrr, kf, NO_VALUE_FORMAT, mns, counterRecid);
            return new MapBackedNavigableSet<>(m);
        }

        @Override void checkConfig(TreeMap<String, String> cat) {
            resolveFormat(cat.get(key("serializer")), keyFmt);
        }

        /**
         * Bulk-build a new tree set from elements already sorted (strictly ascending, distinct),
         * atomically create-and-register it, and return the live navigable set. Fails if the name
         * already exists. Honours {@link #counterEnable()}.
         *
         * @throws DBException.NotSorted if the input is not strictly ascending
         */
        public NavigableSet<E> createFrom(Iterator<E> sortedDistinct) {
            lock.writeLock().lock();
            try {
                checkOpen();
                if (keyFmt == null) {
                    throw new DBException.WrongConfiguration("treeSet " + name + " requires a format");
                }
                TreeMap<String, String> cat = catalogLoadInternal();
                if (cat.containsKey(name + "#type")) {
                    throw new DBException.WrongConfiguration("collection already exists: " + name);
                }
                Iterator<Map.Entry<E, Object>> entries = new Iterator<>() {
                    @Override public boolean hasNext() { return sortedDistinct.hasNext(); }
                    @Override public Map.Entry<E, Object> next() {
                        return new AbstractMap.SimpleImmutableEntry<>(sortedDistinct.next(), NoValueSerializer.NONE);
                    }
                };
                BTreeMap<E, Object> m = BTreeMap.createFromSorted(store, keyFmt, NO_VALUE_FORMAT, maxNodeSize,
                        entries, counterEnable);
                cat.put(key("serializer"), fmtId(keyFmt));
                cat.put(key("rootRecidRecid"), Long.toString(m.rootRecidRecid()));
                cat.put(key("maxNodeSize"), Integer.toString(maxNodeSize));
                cat.put(key("counterRecid"), Long.toString(m.counterRecid()));
                cat.put(name + "#type", type());
                catalogSaveInternal(cat);
                NavigableSet<E> set = new MapBackedNavigableSet<>(m);
                instances.put(name, set);
                return set;
            } finally {
                lock.writeLock().unlock();
            }
        }
    }

    // ---- BufferTreeMap ----------------------------------------------------

    public <K, V> BufferTreeMapMaker<K, V> bufferTreeMap(String name) {
        return new BufferTreeMapMaker<>(name, null, null);
    }

    public <K, V> BufferTreeMapMaker<K, V> bufferTreeMap(
            String name, GroupFormat<K> keyFmt, GroupFormat<V> valueFmt) {
        return new BufferTreeMapMaker<>(name, keyFmt, valueFmt);
    }

    /** Maker for MapDB5's write-optimized buffered B-tree. */
    public final class BufferTreeMapMaker<K, V> extends Maker<BufferTreeMap<K, V>> {
        private GroupFormat<K> keyFmt;
        private GroupFormat<V> valueFmt;
        private int maxNodeSize = DEFAULT_MAX_NODE_SIZE;
        private int bufferBytes = 4096;
        private int leafHeadroom = BufferTreeMap.DEFAULT_LEAF_HEADROOM;

        BufferTreeMapMaker(String name, GroupFormat<K> keyFmt, GroupFormat<V> valueFmt) {
            super(name);
            this.keyFmt = keyFmt;
            this.valueFmt = valueFmt;
        }

        public BufferTreeMapMaker<K, V> keySerializer(GroupFormat<K> f) { keyFmt = f; return this; }
        public BufferTreeMapMaker<K, V> valueSerializer(GroupFormat<V> f) { valueFmt = f; return this; }
        public BufferTreeMapMaker<K, V> maxNodeSize(int n) { maxNodeSize = n; return this; }
        public BufferTreeMapMaker<K, V> bufferBytes(int n) { bufferBytes = n; return this; }
        public BufferTreeMapMaker<K, V> leafHeadroom(int n) { leafHeadroom = n; return this; }

        @Override String type() { return "BufferTreeMap"; }

        private StoreDelta deltaStore() {
            if (!(store instanceof StoreDelta)) {
                throw new DBException.WrongConfiguration(
                        "bufferTreeMap requires a delta-capable store; heapDB() is not supported");
            }
            return (StoreDelta) store;
        }

        private void requireFormats() {
            if (keyFmt == null || valueFmt == null) {
                throw new DBException.WrongConfiguration(
                        "bufferTreeMap " + name + " requires key and value formats");
            }
        }

        @Override BufferTreeMap<K, V> create2(TreeMap<String, String> cat) {
            requireFormats();
            BufferTreeMap<K, V> map = BufferTreeMap.create(deltaStore(), keyFmt, valueFmt,
                    maxNodeSize, bufferBytes, leafHeadroom);
            writeCatalog(cat, map, keyFmt, valueFmt, maxNodeSize, bufferBytes, leafHeadroom);
            return map;
        }

        @Override BufferTreeMap<K, V> open2(TreeMap<String, String> cat) {
            GroupFormat<K> kf = resolveFormat(cat.get(key("keySerializer")), keyFmt);
            GroupFormat<V> vf = resolveFormat(cat.get(key("valueSerializer")), valueFmt);
            return BufferTreeMap.open(deltaStore(),
                    Long.parseLong(cat.get(key("rootRecidRecid"))), kf, vf,
                    Integer.parseInt(cat.get(key("maxNodeSize"))),
                    Integer.parseInt(cat.get(key("bufferBytes"))),
                    Integer.parseInt(cat.get(key("leafHeadroom"))));
        }

        @Override void checkConfig(TreeMap<String, String> cat) {
            resolveFormat(cat.get(key("keySerializer")), keyFmt);
            resolveFormat(cat.get(key("valueSerializer")), valueFmt);
            deltaStore();
        }

        private void writeCatalog(TreeMap<String, String> cat, BufferTreeMap<K, V> map,
                                  GroupFormat<K> builtKeyFmt, GroupFormat<V> builtValueFmt,
                                  int builtMaxNodeSize, int builtBufferBytes,
                                  int builtLeafHeadroom) {
            cat.put(key("keySerializer"), fmtId(builtKeyFmt));
            cat.put(key("valueSerializer"), fmtId(builtValueFmt));
            cat.put(key("rootRecidRecid"), Long.toString(map.rootRecidRecid()));
            cat.put(key("maxNodeSize"), Integer.toString(builtMaxNodeSize));
            cat.put(key("bufferBytes"), Integer.toString(builtBufferBytes));
            cat.put(key("leafHeadroom"), Integer.toString(builtLeafHeadroom));
        }

        public BufferTreeMap<K, V> createFrom(
                Iterator<? extends Map.Entry<K, V>> presortedByKey) {
            lock.writeLock().lock();
            try {
                checkOpen();
                requireFormats();
                TreeMap<String, String> cat = catalogLoadInternal();
                if (cat.containsKey(name + "#type")) {
                    throw new DBException.WrongConfiguration("collection already exists: " + name);
                }
                GroupFormat<K> builtKeyFmt = keyFmt;
                GroupFormat<V> builtValueFmt = valueFmt;
                int builtMaxNodeSize = maxNodeSize;
                int builtBufferBytes = bufferBytes;
                int builtLeafHeadroom = leafHeadroom;
                BufferTreeMap<K, V> map = BufferTreeMap.createFromSorted(deltaStore(),
                        builtKeyFmt, builtValueFmt, builtMaxNodeSize, builtBufferBytes,
                        builtLeafHeadroom, Math.max(2, builtMaxNodeSize * 3 / 4), presortedByKey);
                writeCatalog(cat, map, builtKeyFmt, builtValueFmt, builtMaxNodeSize,
                        builtBufferBytes, builtLeafHeadroom);
                cat.put(name + "#type", type());
                catalogSaveInternal(cat);
                instances.put(name, map);
                return map;
            } finally {
                lock.writeLock().unlock();
            }
        }

        public BufferTreeMap<K, V> createFrom(SortedMap<K, V> source) {
            return createFrom(source.entrySet().iterator());
        }
    }

    // ---- SortedTableMap ---------------------------------------------------

    public <K, V> SortedTableMapMaker<K, V> sortedTableMap(String name) {
        return new SortedTableMapMaker<>(name, null, null);
    }

    public <K, V> SortedTableMapMaker<K, V> sortedTableMap(String name, GroupFormat<K> keyFmt, GroupFormat<V> valueFmt) {
        return new SortedTableMapMaker<>(name, keyFmt, valueFmt);
    }

    /**
     * Builder for a {@link SortedTableMap} — a bulk-built, immutable-after-build sorted map packed
     * into fixed-size pages. {@code create()}/{@code createOrOpen()} build an EMPTY table; populate a
     * table at build time with {@link #createFrom(Iterator)}, {@link #createFrom(SortedMap)} or the
     * incremental {@link #createFromSink()}. Keys must be STRICTLY ascending; the map rejects writes
     * after it is built ({@code put}/{@code remove} throw {@link UnsupportedOperationException}).
     */
    public final class SortedTableMapMaker<K, V> extends Maker<SortedTableMap<K, V>> {
        private GroupFormat<K> keyFmt;
        private GroupFormat<V> valueFmt;
        private int entriesPerPage = SortedTableMap.DEFAULT_ENTRIES_PER_PAGE;

        SortedTableMapMaker(String name, GroupFormat<K> keyFmt, GroupFormat<V> valueFmt) {
            super(name); this.keyFmt = keyFmt; this.valueFmt = valueFmt;
        }

        public SortedTableMapMaker<K, V> keySerializer(GroupFormat<K> f) { this.keyFmt = f; return this; }
        public SortedTableMapMaker<K, V> valueSerializer(GroupFormat<V> f) { this.valueFmt = f; return this; }
        /** Entries packed per page (default {@link SortedTableMap#DEFAULT_ENTRIES_PER_PAGE}). */
        public SortedTableMapMaker<K, V> pageSize(int entriesPerPage) { this.entriesPerPage = entriesPerPage; return this; }

        @Override String type() { return "SortedTableMap"; }

        @Override SortedTableMap<K, V> create2(TreeMap<String, String> cat) {
            if (keyFmt == null || valueFmt == null) {
                throw new DBException.WrongConfiguration("sortedTableMap " + name + " requires key and value formats");
            }
            SortedTableMap<K, V> m = SortedTableMap.createFromSink(store, keyFmt, valueFmt, entriesPerPage).create();
            writeCatalog(cat, m, keyFmt, valueFmt, entriesPerPage);
            return m;
        }

        @Override SortedTableMap<K, V> open2(TreeMap<String, String> cat) {
            long headerRecid = Long.parseLong(cat.get(key("headerRecid")));
            GroupFormat<K> kf = resolveFormat(cat.get(key("keySerializer")), keyFmt);
            GroupFormat<V> vf = resolveFormat(cat.get(key("valueSerializer")), valueFmt);
            return SortedTableMap.open(store, headerRecid, kf, vf);
        }

        @Override void checkConfig(TreeMap<String, String> cat) {
            resolveFormat(cat.get(key("keySerializer")), keyFmt);
            resolveFormat(cat.get(key("valueSerializer")), valueFmt);
        }

        private void writeCatalog(TreeMap<String, String> cat, SortedTableMap<K, V> m,
                                  GroupFormat<K> builtKeyFmt, GroupFormat<V> builtValueFmt,
                                  int builtEntriesPerPage) {
            cat.put(key("keySerializer"), fmtId(builtKeyFmt));
            cat.put(key("valueSerializer"), fmtId(builtValueFmt));
            cat.put(key("headerRecid"), Long.toString(m.headerRecid()));
            cat.put(key("entriesPerPage"), Integer.toString(builtEntriesPerPage));
        }

        /** Atomically build-from-data and register under the DB write lock. Caller must hold the
         *  formats non-null and the name free. */
        private SortedTableMap<K, V> register(SortedTableMap<K, V> m,
                                              GroupFormat<K> builtKeyFmt,
                                              GroupFormat<V> builtValueFmt,
                                              int builtEntriesPerPage) {
            TreeMap<String, String> cat = catalogLoadInternal();
            if (cat.containsKey(name + "#type")) {
                throw new DBException.WrongConfiguration("collection already exists: " + name);
            }
            writeCatalog(cat, m, builtKeyFmt, builtValueFmt, builtEntriesPerPage);
            cat.put(name + "#type", type());
            catalogSaveInternal(cat);
            instances.put(name, m);
            return m;
        }

        /**
         * Bulk-build a new table from entries already sorted STRICTLY ASCENDING by key, atomically
         * create-and-register it, and return the immutable map. Fails if the name already exists.
         *
         * @throws DBException.NotSorted if the input keys are not strictly ascending
         */
        public SortedTableMap<K, V> createFrom(Iterator<? extends Map.Entry<K, V>> presortedStrictlyAscending) {
            lock.writeLock().lock();
            try {
                checkOpen();
                if (keyFmt == null || valueFmt == null) {
                    throw new DBException.WrongConfiguration("sortedTableMap " + name + " requires key and value formats");
                }
                TreeMap<String, String> cat = catalogLoadInternal();
                if (cat.containsKey(name + "#type")) {
                    throw new DBException.WrongConfiguration("collection already exists: " + name);
                }
                GroupFormat<K> builtKeyFmt = keyFmt;
                GroupFormat<V> builtValueFmt = valueFmt;
                int builtEntriesPerPage = entriesPerPage;
                SortedTableMap<K, V> m = SortedTableMap.createFromSorted(store, builtKeyFmt, builtValueFmt,
                        builtEntriesPerPage, presortedStrictlyAscending);
                return register(m, builtKeyFmt, builtValueFmt, builtEntriesPerPage);
            } finally {
                lock.writeLock().unlock();
            }
        }

        /** {@link #createFrom(Iterator)} from a {@link SortedMap} (uses its entry-set iterator; the
         *  map must be ordered consistently with {@code keySerializer}). */
        public SortedTableMap<K, V> createFrom(SortedMap<K, V> source) {
            return createFrom(source.entrySet().iterator());
        }

        /**
         * Incremental bulk builder: feed STRICTLY ASCENDING {@code (key,value)} pairs via
         * {@link DbSink#put}, then call {@link DbSink#create} exactly once. {@code create()}
         * finalizes the table AND registers it in the catalog under the DB write lock (failing if
         * the name already exists).
         */
        public DbSink createFromSink() {
            if (keyFmt == null || valueFmt == null) {
                throw new DBException.WrongConfiguration("sortedTableMap " + name + " requires key and value formats");
            }
            GroupFormat<K> builtKeyFmt = keyFmt;
            GroupFormat<V> builtValueFmt = valueFmt;
            int builtEntriesPerPage = entriesPerPage;
            return new DbSink(SortedTableMap.createFromSink(store, builtKeyFmt, builtValueFmt,
                    builtEntriesPerPage), builtKeyFmt, builtValueFmt, builtEntriesPerPage);
        }

        /** A {@link SortedTableMap.Sink} whose {@link #create()} also registers the map in the DB
         *  catalog. See {@link SortedTableMapMaker#createFromSink()}. */
        public final class DbSink {
            private final SortedTableMap.Sink<K, V> underlying;
            private final GroupFormat<K> builtKeyFmt;
            private final GroupFormat<V> builtValueFmt;
            private final int builtEntriesPerPage;

            DbSink(SortedTableMap.Sink<K, V> underlying, GroupFormat<K> builtKeyFmt,
                   GroupFormat<V> builtValueFmt, int builtEntriesPerPage) {
                this.underlying = underlying;
                this.builtKeyFmt = builtKeyFmt;
                this.builtValueFmt = builtValueFmt;
                this.builtEntriesPerPage = builtEntriesPerPage;
            }

            /** Append the next entry (keys must be strictly ascending). */
            public DbSink put(K key, V value) { underlying.put(key, value); return this; }

            /** Finalize the table and register it in the catalog (atomic under the DB write lock). */
            public SortedTableMap<K, V> create() {
                lock.writeLock().lock();
                try {
                    checkOpen();
                    TreeMap<String, String> cat = catalogLoadInternal();
                    if (cat.containsKey(name + "#type")) {
                        throw new DBException.WrongConfiguration("collection already exists: " + name);
                    }
                    SortedTableMap<K, V> m = underlying.create();
                    return register(m, builtKeyFmt, builtValueFmt, builtEntriesPerPage);
                } finally {
                    lock.writeLock().unlock();
                }
            }
        }
    }

    // ---- Atomic scalars ---------------------------------------------------

    public AtomicLongMaker atomicLong(String name) { return new AtomicLongMaker(name, 0L); }
    public AtomicLongMaker atomicLong(String name, long init) { return new AtomicLongMaker(name, init); }

    public final class AtomicLongMaker extends Maker<Atomic.Long> {
        private final long init;
        AtomicLongMaker(String name, long init) { super(name); this.init = init; }
        @Override String type() { return "AtomicLong"; }
        @Override Atomic.Long create2(TreeMap<String, String> cat) {
            long recid = store.put(init, Serializers.LONG);
            cat.put(key("recid"), Long.toString(recid));
            return new Atomic.Long(store, recid);
        }
        @Override Atomic.Long open2(TreeMap<String, String> cat) {
            return new Atomic.Long(store, Long.parseLong(cat.get(key("recid"))));
        }
    }

    public AtomicIntegerMaker atomicInteger(String name) { return new AtomicIntegerMaker(name, 0); }
    public AtomicIntegerMaker atomicInteger(String name, int init) { return new AtomicIntegerMaker(name, init); }

    public final class AtomicIntegerMaker extends Maker<Atomic.Integer> {
        private final int init;
        AtomicIntegerMaker(String name, int init) { super(name); this.init = init; }
        @Override String type() { return "AtomicInteger"; }
        @Override Atomic.Integer create2(TreeMap<String, String> cat) {
            long recid = store.put(init, Serializers.INTEGER);
            cat.put(key("recid"), Long.toString(recid));
            return new Atomic.Integer(store, recid);
        }
        @Override Atomic.Integer open2(TreeMap<String, String> cat) {
            return new Atomic.Integer(store, Long.parseLong(cat.get(key("recid"))));
        }
    }

    public AtomicBooleanMaker atomicBoolean(String name) { return new AtomicBooleanMaker(name, false); }
    public AtomicBooleanMaker atomicBoolean(String name, boolean init) { return new AtomicBooleanMaker(name, init); }

    public final class AtomicBooleanMaker extends Maker<Atomic.Boolean> {
        private final boolean init;
        AtomicBooleanMaker(String name, boolean init) { super(name); this.init = init; }
        @Override String type() { return "AtomicBoolean"; }
        @Override Atomic.Boolean create2(TreeMap<String, String> cat) {
            long recid = store.put(init, DbSerializers.BOOLEAN);
            cat.put(key("recid"), Long.toString(recid));
            return new Atomic.Boolean(store, recid);
        }
        @Override Atomic.Boolean open2(TreeMap<String, String> cat) {
            return new Atomic.Boolean(store, Long.parseLong(cat.get(key("recid"))));
        }
    }

    public AtomicStringMaker atomicString(String name) { return new AtomicStringMaker(name, null); }
    public AtomicStringMaker atomicString(String name, String init) { return new AtomicStringMaker(name, init); }

    public final class AtomicStringMaker extends Maker<Atomic.String> {
        private final String init;
        AtomicStringMaker(String name, String init) { super(name); this.init = init; }
        @Override String type() { return "AtomicString"; }
        @Override Atomic.String create2(TreeMap<String, String> cat) {
            long recid = store.put(init, DbSerializers.STRING_NULLABLE);
            cat.put(key("recid"), Long.toString(recid));
            return new Atomic.String(store, recid);
        }
        @Override Atomic.String open2(TreeMap<String, String> cat) {
            return new Atomic.String(store, Long.parseLong(cat.get(key("recid"))));
        }
    }

    public <E> AtomicVarMaker<E> atomicVar(String name) { return new AtomicVarMaker<>(name, null, null); }
    public <E> AtomicVarMaker<E> atomicVar(String name, Serializer<E> ser) { return new AtomicVarMaker<>(name, ser, null); }
    public <E> AtomicVarMaker<E> atomicVar(String name, Serializer<E> ser, E init) { return new AtomicVarMaker<>(name, ser, init); }

    public final class AtomicVarMaker<E> extends Maker<Atomic.Var<E>> {
        private Serializer<E> serializer;
        private final E init;
        AtomicVarMaker(String name, Serializer<E> ser, E init) { super(name); this.serializer = ser; this.init = init; }
        public AtomicVarMaker<E> serializer(Serializer<E> s) { this.serializer = s; return this; }
        @Override String type() { return "AtomicVar"; }
        @Override Atomic.Var<E> create2(TreeMap<String, String> cat) {
            if (serializer == null) {
                throw new DBException.WrongConfiguration("atomicVar " + name + " requires a serializer");
            }
            long recid = (init == null) ? store.preallocate() : store.put(init, serializer);
            cat.put(key("recid"), Long.toString(recid));
            cat.put(key("serializer"), serId(serializer));
            return new Atomic.Var<>(store, recid, serializer);
        }
        @Override Atomic.Var<E> open2(TreeMap<String, String> cat) {
            long recid = Long.parseLong(cat.get(key("recid")));
            Serializer<E> s = resolveSerializer(cat.get(key("serializer")), serializer);
            return new Atomic.Var<>(store, recid, s);
        }

        @Override void checkConfig(TreeMap<String, String> cat) {
            resolveSerializer(cat.get(key("serializer")), serializer);
        }
    }

    // ---- IndexTreeList ----------------------------------------------------

    public <E> IndexTreeListMaker<E> indexTreeList(String name) { return new IndexTreeListMaker<>(name, null); }
    public <E> IndexTreeListMaker<E> indexTreeList(String name, Serializer<E> ser) { return new IndexTreeListMaker<>(name, ser); }

    public final class IndexTreeListMaker<E> extends Maker<IndexTreeList<E>> {
        private Serializer<E> serializer;
        IndexTreeListMaker(String name, Serializer<E> ser) { super(name); this.serializer = ser; }
        public IndexTreeListMaker<E> serializer(Serializer<E> s) { this.serializer = s; return this; }
        @Override String type() { return "IndexTreeList"; }
        @Override IndexTreeList<E> create2(TreeMap<String, String> cat) {
            if (serializer == null) {
                throw new DBException.WrongConfiguration("indexTreeList " + name + " requires a serializer");
            }
            IndexTreeList<E> list = IndexTreeList.create(store, serializer);
            cat.put(key("serializer"), serId(serializer));
            cat.put(key("headerRecid"), Long.toString(list.headerRecid()));
            return list;
        }
        @Override IndexTreeList<E> open2(TreeMap<String, String> cat) {
            long headerRecid = Long.parseLong(cat.get(key("headerRecid")));
            Serializer<E> s = resolveSerializer(cat.get(key("serializer")), serializer);
            return IndexTreeList.open(store, headerRecid, s);
        }

        @Override void checkConfig(TreeMap<String, String> cat) {
            resolveSerializer(cat.get(key("serializer")), serializer);
        }
    }

    // ---- IndexTreeLongLongMap --------------------------------------------

    public IndexTreeLongLongMapMaker indexTreeLongLongMap(String name) { return new IndexTreeLongLongMapMaker(name, 0L); }
    public IndexTreeLongLongMapMaker indexTreeLongLongMap(String name, long defaultValue) {
        return new IndexTreeLongLongMapMaker(name, defaultValue);
    }

    public final class IndexTreeLongLongMapMaker extends Maker<IndexTreeLongLongMap> {
        private final long defaultValue;
        IndexTreeLongLongMapMaker(String name, long defaultValue) { super(name); this.defaultValue = defaultValue; }
        @Override String type() { return "IndexTreeLongLongMap"; }
        @Override IndexTreeLongLongMap create2(TreeMap<String, String> cat) {
            IndexTreeLongLongMap m = IndexTreeLongLongMap.create(store, defaultValue);
            cat.put(key("headerRecid"), Long.toString(m.headerRecid()));
            return m;
        }
        @Override IndexTreeLongLongMap open2(TreeMap<String, String> cat) {
            return IndexTreeLongLongMap.open(store, Long.parseLong(cat.get(key("headerRecid"))));
        }
    }

    // ---- Persistent blocking queues -------------------------------------

    public <E> QueueMaker<E> queue(String name) {
        return new QueueMaker<>(name, null, PersistentBlockingQueue.Mode.FIFO, Long.MAX_VALUE);
    }
    public <E> QueueMaker<E> queue(String name, Serializer<E> serializer) {
        return new QueueMaker<>(name, serializer, PersistentBlockingQueue.Mode.FIFO, Long.MAX_VALUE);
    }
    public <E> QueueMaker<E> stack(String name) {
        return new QueueMaker<>(name, null, PersistentBlockingQueue.Mode.LIFO, Long.MAX_VALUE);
    }
    public <E> QueueMaker<E> stack(String name, Serializer<E> serializer) {
        return new QueueMaker<>(name, serializer, PersistentBlockingQueue.Mode.LIFO, Long.MAX_VALUE);
    }
    public <E> QueueMaker<E> circularQueue(String name) {
        return new QueueMaker<>(name, null, PersistentBlockingQueue.Mode.CIRCULAR, 1024L);
    }
    public <E> QueueMaker<E> circularQueue(String name, Serializer<E> serializer, long capacity) {
        return new QueueMaker<>(name, serializer, PersistentBlockingQueue.Mode.CIRCULAR, capacity);
    }

    public final class QueueMaker<E> extends Maker<PersistentBlockingQueue<E>> {
        private Serializer<E> serializer;
        private final PersistentBlockingQueue.Mode mode;
        private final long capacity;

        QueueMaker(String name, Serializer<E> serializer, PersistentBlockingQueue.Mode mode,
                   long capacity) {
            super(name); this.serializer = serializer; this.mode = mode; this.capacity = capacity;
        }

        public QueueMaker<E> serializer(Serializer<E> serializer) {
            this.serializer = serializer; return this;
        }

        @Override String type() {
            switch (mode) {
                case FIFO: return "Queue";
                case LIFO: return "Stack";
                case CIRCULAR: return "CircularQueue";
                default: throw new AssertionError(mode);
            }
        }

        @Override PersistentBlockingQueue<E> create2(TreeMap<String, String> cat) {
            if (serializer == null)
                throw new DBException.WrongConfiguration(type() + " " + name + " requires a serializer");
            PersistentBlockingQueue<E> queue = PersistentBlockingQueue.create(
                    store, serializer, mode, capacity);
            cat.put(key("serializer"), serId(serializer));
            cat.put(key("headerRecid"), Long.toString(queue.headerRecid()));
            return queue;
        }

        @Override PersistentBlockingQueue<E> open2(TreeMap<String, String> cat) {
            Serializer<E> resolved = resolveSerializer(cat.get(key("serializer")), serializer);
            PersistentBlockingQueue<E> queue = PersistentBlockingQueue.open(store,
                    Long.parseLong(cat.get(key("headerRecid"))), resolved);
            if (queue.mode() != mode)
                throw new DBException.DataCorruption("queue mode does not match catalog type " + type());
            return queue;
        }

        @Override void checkConfig(TreeMap<String, String> cat) {
            resolveSerializer(cat.get(key("serializer")), serializer);
        }
    }
}
