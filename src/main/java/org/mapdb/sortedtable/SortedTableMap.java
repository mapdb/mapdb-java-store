package org.mapdb.sortedtable;

import org.mapdb.DBException;
import org.mapdb.btree.OrderedMapAdapter;
import org.mapdb.btree.OrderedNavigableView;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;
import org.mapdb.ser.GroupFormat;
import org.mapdb.ser.Serializer;
import org.mapdb.store.RecordRead;
import org.mapdb.store.Store;

import java.util.AbstractMap;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Immutable, read-only, bulk-built sorted map over a Store4 {@link Store} (the Store4
 * evolution of mapdb3's {@code SortedTableMap}). Built ONCE from a STRICTLY
 * ASCENDING stream via a {@link Sink}; afterwards no key may be inserted, removed or
 * replaced — every mutating {@link Map}/{@link NavigableMap} method throws
 * {@link UnsupportedOperationException}.
 *
 * <h2>Layout (Store-record-per-page)</h2>
 * The entries are packed into fixed-count PAGES; each page is ONE store record holding a
 * single {@link GroupFormat} key group + value group:
 * <pre>
 *   page  = packInt(count), keyFormat.serialize(keys), valueFormat.serialize(values)
 * </pre>
 * A single HEADER record is the page directory — the first key of every page plus the page
 * recids and the total entry count:
 * <pre>
 *   header = packLong(size), packInt(pageCount),
 *            keyFormat.serialize(firstKeyOfEachPage),   // group of pageCount keys
 *            pageCount * packLong(pageRecid)
 * </pre>
 * The header recid ({@link #headerRecid()}) is the only thing a caller persists to reopen the
 * map; this reuses StoreDirect/StoreWAL durability and reopen for free (mirrors
 * {@code BTreeMap.rootRecidRecid}). Unlike mapdb3 there is NO page-&gt;node sublevel: the
 * byte-side {@link GroupFormat#binarySearch} already searches a packed page group in
 * O(log&nbsp;count) without materializing it, so a second in-page level would add nothing.
 *
 * <h2>Search</h2>
 * {@code get} binary-searches the in-memory directory (first keys) to the covering page, then
 * pushes a {@link RecordRead} into the store that binary-searches the page's serialized bytes
 * in place ({@code keyFormat.binarySearch} then {@code valueFormat.binaryGet}) — the page is
 * never deserialized when the formats support binary access. Iteration materializes one page
 * at a time (O(maxPageEntries) memory) and, because the page recids are an INDEXABLE array,
 * runs in both directions with no back-pointers.
 *
 * <h2>Navigation</h2>
 * The full {@link NavigableMap} surface (floor/ceiling/higher/lower, first/last,
 * head/tail/sub maps, descending map + key sets) is delegated to the shared
 * {@link OrderedNavigableView} over an immutable {@link OrderedMapAdapter} whose mutators
 * throw. This is a {@code NavigableMap}, not a {@code ConcurrentNavigableMap}: being immutable
 * the ConcurrentMap CAS surface is pointless.
 *
 * <h2>Thread-safety</h2>
 * All state is final and the backing records are read-only, so any number of threads may read
 * concurrently with no locks (the backing {@code Store} must itself be readable concurrently —
 * the default).
 */
public final class SortedTableMap<K, V> extends AbstractMap<K, V> implements NavigableMap<K, V> {

    /** Default entries-per-page for the builder; small enough to keep a page's offset table and
     *  materialization cheap, large enough to amortize the per-page record + directory entry. */
    public static final int DEFAULT_ENTRIES_PER_PAGE = 128;

    private final Store store;
    private final GroupFormat<K> keyFormat;
    private final GroupFormat<V> valueFormat;
    private final long headerRecid;

    private final PageSerializer<K, V> pageSer;

    // ---- directory (immutable, loaded once from the header) ----
    private final long sizeLong;
    private final int pageCount;
    private final Object firstKeys;   // key group of size pageCount (page[i]'s first key)
    private final long[] pageRecids;  // page[i]'s store recid

    private SortedTableMap(Store store, GroupFormat<K> keyFormat, GroupFormat<V> valueFormat, long headerRecid) {
        this.store = store;
        this.keyFormat = keyFormat;
        this.valueFormat = valueFormat;
        this.headerRecid = headerRecid;
        this.pageSer = new PageSerializer<>(keyFormat, valueFormat);
        Header<K> h = store.get(headerRecid, new HeaderSerializer<>(keyFormat));
        this.sizeLong = h.size;
        this.firstKeys = h.firstKeys;
        this.pageRecids = h.pageRecids;
        this.pageCount = h.pageRecids.length;
    }

    /** Recid of the header (directory) record; persist this to {@link #open} the map later. */
    public long headerRecid() { return headerRecid; }

    /**
     * Delete every page record and the header owned by this table. The handle is invalid
     * after this call. Intended for owning facades such as {@code DB.delete}; normal map
     * mutation remains unsupported.
     */
    public void deleteAllRecords() {
        for (long pageRecid : pageRecids) store.delete(pageRecid, pageSer);
        store.delete(headerRecid, new HeaderSerializer<>(keyFormat));
    }

    // =========================================================================
    // Build
    // =========================================================================

    /** Open an existing map previously built into {@code store}, addressed by its header recid. */
    public static <K, V> SortedTableMap<K, V> open(Store store, long headerRecid,
                                                   GroupFormat<K> keyFormat, GroupFormat<V> valueFormat) {
        return new SortedTableMap<>(store, keyFormat, valueFormat, headerRecid);
    }

    /** Sink builder with {@link #DEFAULT_ENTRIES_PER_PAGE}. */
    public static <K, V> Sink<K, V> createFromSink(Store store, GroupFormat<K> keyFormat,
                                                   GroupFormat<V> valueFormat) {
        return createFromSink(store, keyFormat, valueFormat, DEFAULT_ENTRIES_PER_PAGE);
    }

    /** Sink builder packing {@code entriesPerPage} entries per page. */
    public static <K, V> Sink<K, V> createFromSink(Store store, GroupFormat<K> keyFormat,
                                                   GroupFormat<V> valueFormat, int entriesPerPage) {
        return new Sink<>(store, keyFormat, valueFormat, entriesPerPage);
    }

    /** Bulk-build directly from a sorted iterator (default page size). */
    public static <K, V> SortedTableMap<K, V> createFromSorted(Store store, GroupFormat<K> keyFormat,
                                                               GroupFormat<V> valueFormat,
                                                               Iterator<? extends Map.Entry<K, V>> sorted) {
        return createFromSorted(store, keyFormat, valueFormat, DEFAULT_ENTRIES_PER_PAGE, sorted);
    }

    /** Bulk-build directly from a sorted iterator with an explicit page size. */
    public static <K, V> SortedTableMap<K, V> createFromSorted(Store store, GroupFormat<K> keyFormat,
                                                               GroupFormat<V> valueFormat, int entriesPerPage,
                                                               Iterator<? extends Map.Entry<K, V>> sorted) {
        Sink<K, V> sink = createFromSink(store, keyFormat, valueFormat, entriesPerPage);
        while (sorted.hasNext()) {
            Map.Entry<K, V> e = sorted.next();
            sink.put(e.getKey(), e.getValue());
        }
        return sink.create();
    }

    /**
     * Write-once bulk builder. Feed STRICTLY ASCENDING {@code (key,value)} pairs via {@link #put}
     * then call {@link #create} exactly once. Rejects a misordered or duplicate key with
     * {@link DBException.NotSorted} (like {@code TreePump}). Every page record and the header are
     * written exactly once through {@link Store#put}. Single-threaded.
     */
    public static final class Sink<K, V> {
        private final Store store;
        private final GroupFormat<K> keyFormat;
        private final GroupFormat<V> valueFormat;
        private final PageSerializer<K, V> pageSer;
        private final int entriesPerPage;

        private final java.util.ArrayList<Object> pageKeys = new java.util.ArrayList<>();
        private final java.util.ArrayList<Object> pageVals = new java.util.ArrayList<>();
        private final java.util.ArrayList<Object> firstKeys = new java.util.ArrayList<>();
        private final java.util.ArrayList<Long> recids = new java.util.ArrayList<>();

        private K prevKey;
        private long size;
        private boolean done;

        Sink(Store store, GroupFormat<K> keyFormat, GroupFormat<V> valueFormat, int entriesPerPage) {
            if (entriesPerPage < 1) throw new IllegalArgumentException("entriesPerPage must be >= 1");
            this.store = store;
            this.keyFormat = keyFormat;
            this.valueFormat = valueFormat;
            this.pageSer = new PageSerializer<>(keyFormat, valueFormat);
            this.entriesPerPage = entriesPerPage;
        }

        public void put(K key, V value) {
            if (done) throw new IllegalStateException("sink already finished");
            if (key == null || value == null) throw new NullPointerException();
            if (prevKey != null && keyFormat.element().compare(prevKey, key) >= 0)
                throw new DBException.NotSorted("bulk-load keys not strictly ascending at " + key);
            prevKey = key;
            pageKeys.add(key);
            pageVals.add(value);
            size++;
            if (pageKeys.size() == entriesPerPage) flushPage();
        }

        private void flushPage() {
            if (pageKeys.isEmpty()) return;
            Object keys = keyFormat.fromArray(pageKeys.toArray());
            Object vals = valueFormat.fromArray(pageVals.toArray());
            long recid = store.put(new Page(keys, vals), pageSer);
            firstKeys.add(pageKeys.get(0));
            recids.add(recid);
            pageKeys.clear();
            pageVals.clear();
        }

        public SortedTableMap<K, V> create() {
            if (done) throw new IllegalStateException("sink already finished");
            done = true;
            flushPage();
            long[] pageRecids = new long[recids.size()];
            for (int i = 0; i < pageRecids.length; i++) pageRecids[i] = recids.get(i);
            Header<K> h = new Header<>(size, keyFormat.fromArray(firstKeys.toArray()), pageRecids);
            long headerRecid = store.put(h, new HeaderSerializer<>(keyFormat));
            return new SortedTableMap<>(store, keyFormat, valueFormat, headerRecid);
        }
    }

    // =========================================================================
    // Records + serializers
    // =========================================================================

    /** In-memory image of one page: a key group + a value group (OPAQUE, owned by the formats). */
    static final class Page {
        final Object keys;
        final Object values;
        Page(Object keys, Object values) { this.keys = keys; this.values = values; }
    }

    static final class PageSerializer<K, V> implements Serializer<Page> {
        private final GroupFormat<K> keyFormat;
        private final GroupFormat<V> valueFormat;
        PageSerializer(GroupFormat<K> keyFormat, GroupFormat<V> valueFormat) {
            this.keyFormat = keyFormat;
            this.valueFormat = valueFormat;
        }
        @Override public void serialize(DataOutput2 out, Page p) {
            int count = keyFormat.size(p.keys);
            out.packInt(count);
            keyFormat.serialize(out, p.keys);
            valueFormat.serialize(out, p.values);
        }
        @Override public Page deserialize(DataInput2 in, int size) {
            int count = in.unpackInt();
            Object keys = keyFormat.deserialize(in, count);
            Object values = valueFormat.deserialize(in, count);
            return new Page(keys, values);
        }
    }

    static final class Header<K> {
        final long size;
        final Object firstKeys; // key group of size pageCount
        final long[] pageRecids;
        Header(long size, Object firstKeys, long[] pageRecids) {
            this.size = size;
            this.firstKeys = firstKeys;
            this.pageRecids = pageRecids;
        }
    }

    /** Header self-identification: cheap fail-fast on a wrong recid / wrong collection type /
     *  incompatible format version at {@link #open} (the caller-supplied formats still have to
     *  match the data — magic does not verify that, it only catches gross mismatches early). */
    static final int MAGIC = 0x53544D31; // "STM1"
    static final int VERSION = 1;

    static final class HeaderSerializer<K> implements Serializer<Header<K>> {
        private final GroupFormat<K> keyFormat;
        HeaderSerializer(GroupFormat<K> keyFormat) { this.keyFormat = keyFormat; }
        @Override public void serialize(DataOutput2 out, Header<K> h) {
            out.packInt(MAGIC);
            out.packInt(VERSION);
            out.packLong(h.size);
            out.packInt(h.pageRecids.length);
            keyFormat.serialize(out, h.firstKeys);
            for (long recid : h.pageRecids) out.packLong(recid);
        }
        @Override public Header<K> deserialize(DataInput2 in, int size) {
            int magic = in.unpackInt();
            if (magic != MAGIC)
                throw new DBException.DataCorruption("not a SortedTableMap header (magic=0x"
                        + Integer.toHexString(magic) + ")");
            int version = in.unpackInt();
            if (version != VERSION)
                throw new DBException.DataCorruption("unsupported SortedTableMap version: " + version);
            long sz = in.unpackLong();
            int pageCount = in.unpackInt();
            Object firstKeys = keyFormat.deserialize(in, pageCount);
            long[] recids = new long[pageCount];
            for (int i = 0; i < pageCount; i++) recids[i] = in.unpackLong();
            return new Header<>(sz, firstKeys, recids);
        }
    }

    // =========================================================================
    // Directory routing
    // =========================================================================

    /** Index of the page whose first key is the greatest {@code <= key}, or -1 when {@code key}
     *  is below the first key of page 0 (so it cannot be present and no page covers it). */
    private int pageForFloor(K key) {
        int p = keyFormat.search(firstKeys, key);
        return p >= 0 ? p : -p - 2;
    }

    // =========================================================================
    // get / containsKey (push-down byte-side search)
    // =========================================================================

    /** One page lookup executed inside the store against serialized bytes (or a live Page on a
     *  heap store) — never materializes the page when the formats support binary access. */
    private final class GetAction implements RecordRead {
        final K key;
        V value;
        boolean found;
        GetAction(K key) { this.key = key; }

        @Override public long onBytes(DataInput2 in, int size) {
            found = false;
            value = null;
            int count = in.unpackInt();
            // every key occupies >= 1 serialized byte, so count > size is torn/corrupt
            if (count > size) throw new IllegalStateException("page count exceeds record size");
            int pos = keyFormat.supportsBinary()
                    ? keyFormat.binarySearch(key, in, count)
                    : keyFormat.search(keyFormat.deserialize(in, count), key);
            if (pos >= 0) {
                found = true;
                value = valueFormat.supportsBinary()
                        ? valueFormat.binaryGet(in, count, pos)
                        : valueFormat.get(valueFormat.deserialize(in, count), pos);
            }
            return 0;
        }

        @Override public long onObject(Object record) {
            found = false;
            value = null;
            Page p = (Page) record;
            int pos = keyFormat.search(p.keys, key);
            if (pos >= 0) {
                found = true;
                value = valueFormat.get(p.values, pos);
            }
            return 0;
        }
    }

    @SuppressWarnings("unchecked")
    @Override public V get(Object key) {
        if (key == null) throw new NullPointerException();
        if (pageCount == 0) return null;
        int pageIdx = pageForFloor((K) key);
        if (pageIdx < 0) return null;
        GetAction action = new GetAction((K) key);
        store.read(pageRecids[pageIdx], action);
        return action.found ? action.value : null;
    }

    @SuppressWarnings("unchecked")
    @Override public boolean containsKey(Object key) {
        if (key == null) throw new NullPointerException();
        if (pageCount == 0) return false;
        int pageIdx = pageForFloor((K) key);
        if (pageIdx < 0) return false;
        GetAction action = new GetAction((K) key);
        store.read(pageRecids[pageIdx], action);
        return action.found;
    }

    // =========================================================================
    // Iteration (page-at-a-time, both directions, bounded)
    // =========================================================================

    private Page loadPage(int idx) { return store.get(pageRecids[idx], pageSer); }

    /** Ascending entries within {@code [lo,hi]} (null bound = open), honoring inclusivity. */
    Iterator<Map.Entry<K, V>> entryIterator(K lo, boolean loInc, K hi, boolean hiInc) {
        if (pageCount == 0) return emptyIter();
        int startPage = (lo == null) ? 0 : Math.max(0, pageForFloor(lo));
        return new AscendingIter(startPage, lo, loInc, hi, hiInc);
    }

    /** Descending entries within {@code [lo,hi]} (null bound = open), honoring inclusivity. */
    Iterator<Map.Entry<K, V>> descendingEntryIterator(K lo, boolean loInc, K hi, boolean hiInc) {
        if (pageCount == 0) return emptyIter();
        int startPage = (hi == null) ? pageCount - 1 : pageForFloor(hi);
        if (startPage < 0) return emptyIter(); // hi below every key
        return new DescendingIter(startPage, lo, loInc, hi, hiInc);
    }

    private final class AscendingIter implements Iterator<Map.Entry<K, V>> {
        private final K lo, hi;
        private final boolean loInc, hiInc;
        private int pageIdx;
        private Page page;
        private int pos;
        private int count;
        private boolean done, has;
        private K nk; private V nv;

        AscendingIter(int startPage, K lo, boolean loInc, K hi, boolean hiInc) {
            this.lo = lo; this.loInc = loInc; this.hi = hi; this.hiInc = hiInc;
            this.pageIdx = startPage;
            loadCurrent();
            // position within the start page at the first key >= lo (or > lo when exclusive)
            if (lo == null) {
                pos = 0;
            } else {
                int p = keyFormat.search(page.keys, lo);
                pos = p >= 0 ? (loInc ? p : p + 1) : -p - 1;
            }
        }

        private void loadCurrent() {
            page = loadPage(pageIdx);
            count = keyFormat.size(page.keys);
        }

        private void advance() {
            if (has || done) return;
            for (;;) {
                while (pos >= count) {
                    pageIdx++;
                    if (pageIdx >= pageCount) { done = true; return; }
                    loadCurrent();
                    pos = 0;
                }
                K k = keyFormat.get(page.keys, pos);
                if (hi != null) {
                    int c = keyFormat.compare(k, hi);
                    if (c > 0 || (c == 0 && !hiInc)) { done = true; return; }
                }
                nk = k;
                nv = valueFormat.get(page.values, pos);
                pos++;
                has = true;
                return;
            }
        }

        @Override public boolean hasNext() { advance(); return has; }
        @Override public Map.Entry<K, V> next() {
            advance();
            if (!has) throw new NoSuchElementException();
            has = false;
            return new AbstractMap.SimpleImmutableEntry<>(nk, nv);
        }
    }

    private final class DescendingIter implements Iterator<Map.Entry<K, V>> {
        private final K lo, hi;
        private final boolean loInc, hiInc;
        private int pageIdx;
        private Page page;
        private int pos;
        private boolean done, has;
        private K nk; private V nv;

        DescendingIter(int startPage, K lo, boolean loInc, K hi, boolean hiInc) {
            this.lo = lo; this.loInc = loInc; this.hi = hi; this.hiInc = hiInc;
            this.pageIdx = startPage;
            loadCurrent();
            // position within the start page at the last key <= hi (or < hi when exclusive)
            if (hi == null) {
                pos = keyFormat.size(page.keys) - 1;
            } else {
                int p = keyFormat.search(page.keys, hi);
                pos = p >= 0 ? (hiInc ? p : p - 1) : -p - 2;
            }
        }

        private void loadCurrent() { page = loadPage(pageIdx); }

        private void advance() {
            if (has || done) return;
            for (;;) {
                while (pos < 0) {
                    pageIdx--;
                    if (pageIdx < 0) { done = true; return; }
                    loadCurrent();
                    pos = keyFormat.size(page.keys) - 1;
                }
                K k = keyFormat.get(page.keys, pos);
                if (lo != null) {
                    int c = keyFormat.compare(k, lo);
                    if (c < 0 || (c == 0 && !loInc)) { done = true; return; }
                }
                nk = k;
                nv = valueFormat.get(page.values, pos);
                pos--;
                has = true;
                return;
            }
        }

        @Override public boolean hasNext() { advance(); return has; }
        @Override public Map.Entry<K, V> next() {
            advance();
            if (!has) throw new NoSuchElementException();
            has = false;
            return new AbstractMap.SimpleImmutableEntry<>(nk, nv);
        }
    }

    private static <K, V> Iterator<Map.Entry<K, V>> emptyIter() {
        return java.util.Collections.emptyIterator();
    }

    /** Ascending entry iterator over the whole map. */
    public Iterator<Map.Entry<K, V>> entryIterator() { return entryIterator(null, true, null, true); }

    // =========================================================================
    // Map / NavigableMap surface
    // =========================================================================

    /** Exact entry count; {@link #size()} saturates this to int. */
    public long sizeLong() { return sizeLong; }

    @Override public int size() { return sizeLong > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) sizeLong; }
    @Override public boolean isEmpty() { return sizeLong == 0; }

    @Override public Comparator<? super K> comparator() { return keyFormat.comparator(); }

    // ---- immutable: every mutator throws ----
    @Override public V put(K key, V value) { throw uoe(); }
    @Override public V remove(Object key) { throw uoe(); }
    @Override public void putAll(Map<? extends K, ? extends V> m) { throw uoe(); }
    @Override public void clear() { throw uoe(); }
    @Override public Map.Entry<K, V> pollFirstEntry() { throw uoe(); }
    @Override public Map.Entry<K, V> pollLastEntry() { throw uoe(); }
    private static UnsupportedOperationException uoe() { return new UnsupportedOperationException("read-only"); }

    // ---- navigable surface delegated to the shared view over an immutable adapter ----
    private volatile OrderedNavigableView<K, V> fullView;

    private OrderedNavigableView<K, V> fullView() {
        OrderedNavigableView<K, V> v = fullView;
        if (v == null) {
            v = new OrderedNavigableView<>(new Adapter(), null, true, null, true, false);
            fullView = v;
        }
        return v;
    }

    @Override public Set<Map.Entry<K, V>> entrySet() { return fullView().entrySet(); }
    @Override public Set<K> keySet() { return fullView().navigableKeySet(); }
    @Override public java.util.Collection<V> values() { return fullView().values(); }

    @Override public Map.Entry<K, V> firstEntry() { return fullView().firstEntry(); }
    @Override public Map.Entry<K, V> lastEntry() { return fullView().lastEntry(); }
    @Override public Map.Entry<K, V> lowerEntry(K key) { return fullView().lowerEntry(key); }
    @Override public Map.Entry<K, V> floorEntry(K key) { return fullView().floorEntry(key); }
    @Override public Map.Entry<K, V> ceilingEntry(K key) { return fullView().ceilingEntry(key); }
    @Override public Map.Entry<K, V> higherEntry(K key) { return fullView().higherEntry(key); }
    @Override public K lowerKey(K key) { return fullView().lowerKey(key); }
    @Override public K floorKey(K key) { return fullView().floorKey(key); }
    @Override public K ceilingKey(K key) { return fullView().ceilingKey(key); }
    @Override public K higherKey(K key) { return fullView().higherKey(key); }

    @Override public K firstKey() {
        Map.Entry<K, V> e = firstEntry();
        if (e == null) throw new NoSuchElementException();
        return e.getKey();
    }
    @Override public K lastKey() {
        Map.Entry<K, V> e = lastEntry();
        if (e == null) throw new NoSuchElementException();
        return e.getKey();
    }

    @Override public NavigableSet<K> navigableKeySet() { return fullView().navigableKeySet(); }
    @Override public NavigableSet<K> descendingKeySet() { return fullView().descendingKeySet(); }
    @Override public NavigableMap<K, V> descendingMap() { return fullView().descendingMap(); }

    @Override public NavigableMap<K, V> subMap(K from, boolean fromInc, K to, boolean toInc) {
        return fullView().subMap(from, fromInc, to, toInc);
    }
    @Override public NavigableMap<K, V> headMap(K to, boolean inc) { return fullView().headMap(to, inc); }
    @Override public NavigableMap<K, V> tailMap(K from, boolean inc) { return fullView().tailMap(from, inc); }
    @Override public java.util.SortedMap<K, V> subMap(K from, K to) { return fullView().subMap(from, to); }
    @Override public java.util.SortedMap<K, V> headMap(K to) { return fullView().headMap(to); }
    @Override public java.util.SortedMap<K, V> tailMap(K from) { return fullView().tailMap(from); }

    /** Immutable bridge to the shared range/navigation view; mutators throw. */
    private final class Adapter implements OrderedMapAdapter<K, V> {
        @Override public Comparator<? super K> comparator() { return keyFormat.comparator(); }
        @Override public int compare(K a, K b) { return keyFormat.compare(a, b); }
        @Override public V get(Object key) { return SortedTableMap.this.get(key); }
        @Override public boolean containsKey(Object key) { return SortedTableMap.this.containsKey(key); }
        @Override public V put(K key, V value) { throw uoe(); }
        @Override public V remove(Object key) { throw uoe(); }
        @Override public boolean remove(Object key, Object value) { throw uoe(); }
        @Override public boolean valueEquals(V a, V b) { return valueFormat.element().equals(a, b); }
        @Override public Iterator<Map.Entry<K, V>> entryIterator(K lo, boolean loInc, K hi, boolean hiInc) {
            return SortedTableMap.this.entryIterator(lo, loInc, hi, hiInc);
        }
        @Override public Iterator<Map.Entry<K, V>> descendingEntryIterator(K lo, boolean loInc, K hi, boolean hiInc) {
            return SortedTableMap.this.descendingEntryIterator(lo, loInc, hi, hiInc);
        }
        @Override public Map.Entry<K, V> pollFirstEntry(K lo, boolean loInc, K hi, boolean hiInc) { throw uoe(); }
        @Override public Map.Entry<K, V> pollLastEntry(K lo, boolean loInc, K hi, boolean hiInc) { throw uoe(); }
        @Override public long sizeLong(K lo, boolean loInc, K hi, boolean hiInc) {
            long n = 0;
            for (Iterator<Map.Entry<K, V>> it = SortedTableMap.this.entryIterator(lo, loInc, hi, hiInc); it.hasNext(); ) {
                it.next();
                n++;
            }
            return n;
        }
    }
}
