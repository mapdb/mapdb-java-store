package org.mapdb.btree;

import org.mapdb.DBException;
import org.mapdb.ser.Serializer;
import org.mapdb.store.Store;

import java.util.ArrayList;

/**
 * Bottom-up bulk builder for the B-link trees in this package — the mapdb1/2/3
 * "Pump", rebuilt for Store4 and this engine's node shape. Feed it strictly
 * ascending entries via {@link #put}, then {@link #finish()} once; every node is
 * written EXACTLY once through the bulk path:
 *
 * <pre>
 *   link = preallocate()            // reserve the RIGHT sibling's recid
 *   update(recid, node(link))       // first write of a preallocated recid (B3)
 * </pre>
 *
 * Preallocating the next sibling before writing the current node yields forward
 * B-link pointers from an ascending source with no back-patching (mapdb3
 * Pump.kt discipline); on a fresh store recids and data lay out sequentially in
 * key order (B2). At any instant each level holds exactly one
 * preallocated-but-unwritten recid ({@link Level#pending}); {@code finish()}
 * consumes every one of them as that level's final (rightmost) node, so nothing
 * leaks and no link dangles.
 *
 * Node shape produced (this engine's shape, NOT mapdb3's — no duplicated boundary keys):
 * interior nodes are filled to {@code nodeFill} entries; a flushed leaf's last
 * key becomes its inclusive high bound, propagated up as the parent separator
 * (the sink also writes it as the leaf's fence where the tree has one).
 * Non-rightmost dirs get equal key/child counts (last key = the dir's bound);
 * the final dir per level takes the level's last node as an extra keyless
 * child (rightmost shape, childCount == keysLen + 1). The first node of each
 * level carries LEFT, the final one RIGHT; the top level never flushes
 * mid-build, so its single final node is LEFT|RIGHT — a valid root. An empty
 * source degenerates to the same empty LEFT|RIGHT leaf {@code create()} makes.
 *
 * Single-threaded; structure code (this class) never serializes — the sink owns
 * node materialization and the store write.
 */
final class TreePump<K, V> {

    /**
     * Writes one finished node. {@code recid} is preallocated by the pump;
     * the sink must write it exactly once (update / updateWithHeadroom).
     * {@code keys} (and leaf {@code values}) are fresh arrays owned by the sink.
     */
    interface NodeSink<K, V> {
        void writeLeaf(long recid, int flags, long link, Object[] keys, Object[] values);

        void writeDir(long recid, int flags, long link, Object[] keys, long[] children);
    }

    /** One in-progress node per tree level; index 0 = leaf, values only there. */
    private static final class Level {
        final ArrayList<Object> keys = new ArrayList<>();
        final ArrayList<Object> values;  // leaf level only
        final ArrayList<Long> children;  // dir levels only
        long pending;                    // preallocated recid of this level's NEXT node; 0 = none yet
        boolean first = true;            // no node flushed at this level yet (LEFT candidate)

        Level(boolean leaf) {
            this.values = leaf ? new ArrayList<>() : null;
            this.children = leaf ? null : new ArrayList<>();
        }
    }

    private final Store store;
    private final NodeSink<K, V> sink;
    private final Serializer<K> keyElem;
    private final int nodeFill;
    private final ArrayList<Level> levels = new ArrayList<>();
    private K prevKey;
    private boolean finished;

    TreePump(Store store, NodeSink<K, V> sink, Serializer<K> keyElem, int maxNodeSize, int nodeFill) {
        if (nodeFill < 2 || nodeFill > maxNodeSize)
            throw new IllegalArgumentException("nodeFill must be in [2, maxNodeSize]: " + nodeFill);
        this.store = store;
        this.sink = sink;
        this.keyElem = keyElem;
        this.nodeFill = nodeFill;
        levels.add(new Level(true));
    }

    /** Default pump fill: 3/4 of maxNodeSize (mapdb1/2/3 lineage) — leaves room
     *  for post-load inserts before the first wave of splits. */
    static int defaultFill(int maxNodeSize) {
        return Math.max(2, maxNodeSize * 3 / 4);
    }

    void put(K key, V value) {
        if (key == null || value == null) throw new NullPointerException();
        if (finished) throw new IllegalStateException("pump already finished");
        if (prevKey != null && keyElem.compare(prevKey, key) >= 0)
            throw new DBException.NotSorted("bulk-load keys not strictly ascending at " + key);
        Level leaf = levels.get(0);
        // flush BEFORE adding: interior leaves hold exactly nodeFill entries and
        // the final leaf is never empty (except for an entirely empty source)
        if (leaf.keys.size() == nodeFill) flushLeaf();
        leaf.keys.add(key);
        leaf.values.add(value);
        prevKey = key;
    }

    /** Recid this level's next node lands in: reserved by the previous flush
     *  (it is the left sibling's link target), or fresh for a level's first node. */
    private long nodeRecid(Level level) {
        return level.pending != 0 ? level.pending : store.preallocate();
    }

    private void flushLeaf() {
        Level leaf = levels.get(0);
        long recid = nodeRecid(leaf);
        long link = store.preallocate();
        leaf.pending = link;
        int flags = leaf.first ? BTreeMap.LEFT : 0;
        sink.writeLeaf(recid, flags, link, leaf.keys.toArray(), leaf.values.toArray());
        leaf.first = false;
        @SuppressWarnings("unchecked")
        K sep = (K) leaf.keys.get(leaf.keys.size() - 1); // the leaf's inclusive high bound
        leaf.keys.clear();
        leaf.values.clear();
        pushUp(1, sep, recid);
    }

    /** Register a flushed node with its parent level: sep = child's inclusive high bound. */
    private void pushUp(int levelIdx, K sep, long child) {
        if (levels.size() == levelIdx) levels.add(new Level(false));
        Level dir = levels.get(levelIdx);
        if (dir.keys.size() == nodeFill) flushDir(levelIdx);
        dir.keys.add(sep);
        dir.children.add(child);
    }

    private void flushDir(int levelIdx) {
        Level dir = levels.get(levelIdx);
        long recid = nodeRecid(dir);
        long link = store.preallocate();
        dir.pending = link;
        int flags = BTreeMap.DIR | (dir.first ? BTreeMap.LEFT : 0);
        // non-rightmost dir shape: childCount == keysLen, last key = this dir's bound
        sink.writeDir(recid, flags, link, dir.keys.toArray(), toLongArray(dir.children));
        dir.first = false;
        @SuppressWarnings("unchecked")
        K sep = (K) dir.keys.get(dir.keys.size() - 1);
        dir.keys.clear();
        dir.children.clear();
        pushUp(levelIdx + 1, sep, recid);
    }

    /**
     * Flush the final (rightmost) node of every level, bottom-up, and return the
     * root NODE recid. Each level's final dir absorbs the level below's final
     * node as its keyless rightmost child. A level above level i exists iff
     * level i flushed at least once, so every non-top final node is RIGHT-only
     * and the top one is LEFT|RIGHT — the root.
     */
    long finish() {
        if (finished) throw new IllegalStateException("pump already finished");
        finished = true;
        Level leaf = levels.get(0);
        long child = nodeRecid(leaf);
        sink.writeLeaf(child, (leaf.first ? BTreeMap.LEFT : 0) | BTreeMap.RIGHT, 0L,
                leaf.keys.toArray(), leaf.values.toArray());
        for (int i = 1; i < levels.size(); i++) {
            Level dir = levels.get(i);
            dir.children.add(child); // rightmost extra child, no key (RIGHT dir shape)
            long recid = nodeRecid(dir);
            sink.writeDir(recid, BTreeMap.DIR | (dir.first ? BTreeMap.LEFT : 0) | BTreeMap.RIGHT, 0L,
                    dir.keys.toArray(), toLongArray(dir.children));
            child = recid;
        }
        return child;
    }

    private static long[] toLongArray(ArrayList<Long> list) {
        long[] r = new long[list.size()];
        for (int i = 0; i < r.length; i++) r[i] = list.get(i);
        return r;
    }
}
