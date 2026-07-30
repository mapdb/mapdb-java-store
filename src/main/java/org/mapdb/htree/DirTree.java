package org.mapdb.htree;

import org.mapdb.DBException;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;
import org.mapdb.ser.Serializer;
import org.mapdb.store.RecordRead;
import org.mapdb.store.Store;

import java.util.Arrays;

/**
 * Sparse, bitmap-compressed directory tree mapping a bounded long index to a long
 * value (a recid). Port of mapdb3's {@code IndexTreeListJava} onto the Store4 store;
 * used by {@link HTreeMap} to map hash indices to bucket recids.
 *
 * Dir node = {@code long[]}: {@code dir[0], dir[1]} are two 64-bit bitmaps (up to 128
 * slots), followed by one {@code (value, storedIndex+1)} pair per set bit, in slot
 * order. A pair with {@code storedIndex+1 != 0} is a TERMINAL entry for that exact
 * index — even at a non-leaf level (path compression: a lone index is stored where it
 * first becomes unique, not at full depth). A pair with {@code storedIndex+1 == 0}
 * points to a sub-directory one level down. Inserting a second index into a terminal
 * slot lazily splits it into a sub-dir chain until the two indices diverge
 * ({@link #treePutSub}), so the tree stays shallow for few keys yet never rehashes.
 *
 * Level convention (spec-htreemap §0.1): {@code levels} = the number of directory
 * layers; callers pass {@code rootLevel = levels - 1}; an index occupies exactly
 * {@code levels*dirShift} bits. Level L consumes index bits
 * {@code [L*dirShift, (L+1)*dirShift)}.
 *
 * Single-writer per tree is assumed ({@code treePut}/{@code treeRemove}/{@code treeClear}
 * span multiple records); HTreeMap guarantees it with a per-segment write lock.
 */
public final class DirTree {

    private DirTree() {}

    /** Bitmap is 128 bits, so a dir level resolves at most 7 index bits. */
    public static final int MAX_DIR_SHIFT = 7;

    /** length == 2 + one (value, index+1) pair per set bitmap bit — §0.9 invariant. */
    static boolean dirLenInvariant(long[] dir) {
        return dir.length == 2 + 2 * (Long.bitCount(dir[0]) + Long.bitCount(dir[1]));
    }

    public static long[] dirEmpty() {
        return new long[2];
    }

    /**
     * Dir node wire format: two raw bitmap longs, then the pairs as packed longs
     * (values are recids, indices are stored +1 — both non-negative).
     */
    public static final Serializer<long[]> DIR_SER = new Serializer<>() {

        @Override public void serialize(DataOutput2 out, long[] dir) {
            assert dirLenInvariant(dir) : "dir bitmap/length mismatch";
            out.writeLong(dir[0]);
            out.writeLong(dir[1]);
            for (int i = 2; i < dir.length; i++) out.packLong(dir[i]);
        }

        @Override public long[] deserialize(DataInput2 in, int size) {
            long bitmap1 = in.readLong();
            long bitmap2 = in.readLong();
            int len = 2 + 2 * (Long.bitCount(bitmap1) + Long.bitCount(bitmap2));
            long[] dir = new long[len];
            dir[0] = bitmap1;
            dir[1] = bitmap2;
            for (int i = 2; i < len; i++) dir[i] = in.unpackLong();
            return dir;
        }
    };

    /** Index bits consumed by {@code level}: a slot in [0, 1<<dirShift). */
    static int treePos(int dirShift, int level, long index) {
        return (int) ((index >>> (dirShift * level)) & ((1 << dirShift) - 1));
    }

    /**
     * Slot → offset of its pair in the dir array, via bitmap popcount.
     * Returns the NEGATED insertion offset when the slot's bit is unset.
     */
    static int dirOffsetFromSlot(long[] dir, int slot) {
        return dirOffsetFromBitmaps(dir[0], dir[1], slot);
    }

    /** Same as {@link #dirOffsetFromSlot} but over raw bitmaps — usable by the
     * push-down byte path before (and without) materializing the array. */
    static int dirOffsetFromBitmaps(long bitmap1, long bitmap2, int slot) {
        assert slot >= 0 && slot < 128 : "slot out of range: " + slot;
        int offset = 0;
        long v = bitmap1;
        if (slot > 63) {
            offset += Long.bitCount(v) * 2;
            v = bitmap2;
        }
        slot &= 63;
        long mask = (1L << slot) - 1;
        offset += 2 + Long.bitCount(v & mask) * 2;
        int set = (int) ((v >>> slot) & 1); // 1 if occupied, 0 if empty
        return -offset + ((set << 1) * offset);
    }

    /** Copy-on-write insert/replace of the pair at {@code slot}; never mutates input. */
    static long[] dirPut(long[] dir, int slot, long v1, long v2) {
        int offset = dirOffsetFromSlot(dir, slot);
        if (offset < 0) { // empty slot: grow by one pair, set bitmap bit
            offset = -offset;
            dir = Arrays.copyOf(dir, dir.length + 2);
            System.arraycopy(dir, offset, dir, offset + 2, dir.length - 2 - offset);
            dir[slot / 64] |= 1L << (slot % 64);
        } else {
            dir = dir.clone();
        }
        dir[offset] = v1;
        dir[offset + 1] = v2;
        assert dirLenInvariant(dir) : "dirPut broke bitmap/length invariant";
        return dir;
    }

    /** Copy-on-write removal of the pair at {@code slot} (which must be set):
     * shrink the array by one pair and clear the bitmap bit; never mutates input. */
    static long[] dirRemove(long[] dir, int slot) {
        int offset = dirOffsetFromSlot(dir, slot);
        if (offset < 0) throw new DBException.DataCorruption("dirRemove on empty slot");
        long[] dir2 = new long[dir.length - 2];
        System.arraycopy(dir, 0, dir2, 0, offset);
        System.arraycopy(dir, offset + 2, dir2, offset, dir2.length - offset);
        dir2[slot / 64] &= ~(1L << (slot % 64));
        assert dirLenInvariant(dir2) : "dirRemove broke bitmap/length invariant";
        return dir2;
    }

    // ================= push-down slot reads (mirrors BTreeMap.GetAction) =================

    /**
     * One descent hop executed inside the store: decode the two bitmap longs plus
     * EXACTLY the queried slot's {@code (value, storedIndex+1)} pair — skipping
     * earlier pairs undecoded — instead of materializing the whole {@code long[]}
     * (that materialization was ~91% of CPU in the 160M-op JFR profile). Ported
     * from mapdb3 IndexTreeListJava.treeGetBinary onto {@link RecordRead}.
     *
     * Implements BOTH dialects: {@code onBytes} for byte stores, {@code onObject}
     * for {@link org.mapdb.store.StoreOnHeap} where the record is the live array.
     *
     * Optimistic-read discipline: keeps NO result state (the whole
     * answer rides in the return long, so re-invocation is trivially safe), never
     * allocates, and clamps the bitmap-derived offset against the record size so
     * torn bitmaps fail fast instead of scanning far past the record. Skip counts
     * are bounded (≤ 127 pairs) and the input cursor is bounds-checked by the
     * store, so no torn state can loop unboundedly.
     *
     * Subclasses map the decoded slot to a verdict; "dive to child" is uniformly
     * encoded as {@code -childRecid} (< 0), terminal verdicts as >= 0.
     */
    private abstract static class SlotAction implements RecordRead {
        final int dirShift;
        final long index;
        int level; // set by the descent loop before each hop; never mutated in the action

        SlotAction(int dirShift, long index) {
            this.dirShift = dirShift;
            this.index = index;
        }

        /** Verdict for an unset slot. */
        abstract long onEmpty();

        /** Verdict for the slot's decoded pair ({@code storedIndex} == -1 → subdir pointer). */
        abstract long onPair(long value, long storedIndex);

        @Override public final long onBytes(DataInput2 in, int size) {
            long bitmap1 = in.readLong();
            long bitmap2 = in.readLong();
            int dirPos = dirOffsetFromBitmaps(bitmap1, bitmap2, treePos(dirShift, level, index));
            if (dirPos < 0) return onEmpty();
            // pairs are >= 2 serialized bytes each, so a consistent record holds at
            // least 16 + dirPos bytes up to and including our pair — torn/corrupt
            // bitmaps fail here instead of scanning into neighboring records
            if (16 + dirPos > size) throw new IllegalStateException("dir bitmaps exceed record size");
            in.unpackLongSkip(dirPos - 2); // skip earlier pairs without decoding
            long value = in.unpackLong();
            long storedIndex = in.unpackLong() - 1;
            return onPair(value, storedIndex);
        }

        @Override public final long onObject(Object record) {
            long[] dir = (long[]) record; // heap store: live array, no bytes exist
            int dirPos = dirOffsetFromSlot(dir, treePos(dirShift, level, index));
            if (dirPos < 0) return onEmpty();
            return onPair(dir[dirPos], dir[dirPos + 1] - 1);
        }

        @Override public final long onNull() {
            // only dispatched on validated state; a dir recid must hold a dir node
            throw new DBException.DataCorruption("dir node record is null");
        }

        /** Dive verdict; a zero child recid would alias a terminal verdict, so reject it. */
        final long dive(long childRecid) {
            if (childRecid == 0) throw new DBException.DataCorruption("subdir pointer recid 0");
            return -childRecid;
        }
    }

    /** treeGet hop: >= 0 → final answer (value recid, or 0 = absent); < 0 → -childRecid. */
    private static final class GetSlotAction extends SlotAction {
        GetSlotAction(int dirShift, long index) { super(dirShift, index); }

        @Override long onEmpty() { return 0; }

        @Override long onPair(long value, long storedIndex) {
            if (storedIndex != -1) {
                // terminal entry: hit only on the exact index
                return storedIndex == index ? value : 0;
            }
            return dive(value); // sub-directory: dive one level
        }
    }

    /**
     * Look up {@code index}; returns its value (recid) or 0 when absent.
     * Root call passes {@code level = levels - 1}.
     */
    public static long treeGet(int dirShift, long recid, Store store, int level, long index) {
        assert dirShift >= 1 && dirShift <= MAX_DIR_SHIFT;
        assert index >= 0;
        assert index >>> ((level + 1) * dirShift) == 0 : "index exceeds level budget";

        GetSlotAction action = new GetSlotAction(dirShift, index);
        for (; level >= 0; level--) {
            action.level = level;
            long ret = store.read(recid, action);
            if (ret >= 0) return ret;
            recid = -ret;
        }
        throw new DBException.DataCorruption("dir tree deeper than declared levels");
    }

    /** treePut descent hop: < 0 → -childRecid (dive on, nothing to write at this level);
     * {@link #NOOP} → mapping already present verbatim; {@link #MUTATE} → this node
     * must change — materialize it (and only it) and apply the write. */
    private static final class PutProbeAction extends SlotAction {
        static final long MUTATE = 0, NOOP = 1;
        final long value;

        PutProbeAction(int dirShift, long index, long value) {
            super(dirShift, index);
            this.value = value;
        }

        @Override long onEmpty() { return MUTATE; } // empty slot: insert here

        @Override long onPair(long storedValue, long storedIndex) {
            if (storedIndex == -1) return dive(storedValue); // sub-directory
            return storedIndex == index && storedValue == value ? NOOP : MUTATE;
        }
    }

    /**
     * Insert or replace {@code index → value} (value must be a live recid, != 0).
     * Root call passes {@code level = levels - 1}. The root dir record is only ever
     * updated in place, so the root recid is stable for the tree's lifetime (§0.6).
     * Caller must hold the tree's write lock.
     *
     * Descent is push-down: ancestors on the way down are probed slot-only via
     * {@link PutProbeAction}; only the single node that actually mutates is
     * materialized (via {@link #DIR_SER}) and rewritten. The probe→materialize
     * re-read of that node is race-free because the tree is single-writer.
     */
    public static void treePut(int dirShift, long recid, Store store, int level, long index, long value) {
        assert dirShift >= 1 && dirShift <= MAX_DIR_SHIFT;
        assert index >= 0 && value != 0;
        assert index >>> ((level + 1) * dirShift) == 0 : "index exceeds level budget";

        PutProbeAction probe = new PutProbeAction(dirShift, index, value);
        for (; level >= 0; level--) {
            probe.level = level;
            long verdict = store.read(recid, probe);
            if (verdict < 0) { // sub-directory: dive one level
                recid = -verdict;
                continue;
            }
            if (verdict == PutProbeAction.NOOP) return; // mapping already present
            // this node mutates: materialize just it and apply the write
            long[] dir = store.get(recid, DIR_SER);
            int slot = treePos(dirShift, level, index);
            int dirPos = dirOffsetFromSlot(dir, slot);
            if (dirPos < 0) { // empty slot: install terminal entry
                store.update(recid, dirPut(dir, slot, value, index + 1), DIR_SER);
                return;
            }
            long oldValue = dir[dirPos];
            long oldIndex = dir[dirPos + 1] - 1;
            // single writer per tree: the node cannot have changed since the probe,
            // so the slot cannot have become a subdir pointer
            assert oldIndex != -1 : "dir node changed under the tree write lock";
            if (oldIndex == index) { // same index: swap the value
                if (oldValue == value) return;
                dir = dir.clone();
                dir[dirPos] = value;
                store.update(recid, dir, DIR_SER);
                return;
            }
            // slot holds a terminal entry for a DIFFERENT index: lazy split — push
            // both entries into a fresh subdir chain until their slots diverge
            long subRecid = treePutSub(dirShift, store, level - 1, index, value, oldIndex, oldValue);
            dir = dir.clone();
            dir[dirPos] = subRecid;
            dir[dirPos + 1] = 0; // mark as subdir pointer
            store.update(recid, dir, DIR_SER);
            return;
        }
        throw new DBException.DataCorruption("dir tree deeper than declared levels");
    }

    /** Build the subdir chain holding two colliding terminal entries; returns its recid. */
    private static long treePutSub(int dirShift, Store store, int level,
                                   long index1, long value1, long index2, long value2) {
        // distinct indices masked to the same budget must diverge at some level >= 0
        assert level >= 0 : "identical indices reached below level 0";
        assert index1 >>> ((level + 1) * dirShift) == index2 >>> ((level + 1) * dirShift)
                : "split indices disagree above current level";
        int pos1 = treePos(dirShift, level, index1);
        int pos2 = treePos(dirShift, level, index2);
        long[] dir;
        if (pos1 == pos2) { // still colliding: chain one level deeper
            long recid = treePutSub(dirShift, store, level - 1, index1, value1, index2, value2);
            dir = dirPut(dirEmpty(), pos1, recid, 0L);
        } else {
            dir = dirPut(dirEmpty(), pos1, value1, index1 + 1);
            dir = dirPut(dir, pos2, value2, index2 + 1);
        }
        return store.put(dir, DIR_SER);
    }

    // ================= remove (collapsing) =================

    /** Sentinel result: entry removed, no collapse propagates past that level. */
    private static final long[] REMOVED = new long[0];

    /**
     * Remove {@code index} from the tree, collapsing on the way out (port of mapdb3
     * IndexTreeListJava.treeRemoveCollapsing). Returns true iff the index was present.
     * Root call passes {@code level = levels - 1}; caller must hold the tree's write lock.
     *
     * Collapse rules (the path-compression invariant of {@link #treePutSub}, in reverse):
     *  - a non-root dir left with a SINGLE terminal occupant is deleted and that occupant
     *    is pushed UP into the parent slot the dir hung from (legal precisely because the
     *    occupant's index routes through that same parent slot);
     *  - a non-root dir whose only occupant was the just-collapsed child pointer is
     *    deleted too, propagating the pushed-up occupant further;
     *  - the ROOT is exempt from both (its recid must stay stable, §0.6): it may rest
     *    with any occupancy, including a single terminal entry or fully empty.
     *  Note mapdb3 needs no root exemption in the terminal branch only because its level
     *  convention gives the root a single slot; with our root holding up to 128 slots the
     *  {@code topLevel} guard is required or the root record would be deleted.
     */
    public static boolean treeRemove(int dirShift, long recid, Store store, int level, long index) {
        assert dirShift >= 1 && dirShift <= MAX_DIR_SHIFT;
        assert index >= 0;
        assert index >>> ((level + 1) * dirShift) == 0 : "index exceeds level budget";
        return treeRemoveCollapsing(dirShift, recid, store, level, true, index) != null;
    }

    /**
     * @return null = index absent (no mutation); {@link #REMOVED} = removed, parents
     *         unaffected; any other array = THIS node was deleted and its lone surviving
     *         TERMINAL occupant is at {@code [2],[3]} — the caller must install it in
     *         place of its pointer to this node (or keep propagating the collapse).
     */
    private static long[] treeRemoveCollapsing(int dirShift, long recid, Store store,
                                               int level, boolean topLevel, long index) {
        assert level >= 0 : "dir tree deeper than declared levels";
        long[] dir = store.get(recid, DIR_SER);
        int slot = treePos(dirShift, level, index);
        int pos = dirOffsetFromSlot(dir, slot);
        if (pos < 0) return null; // slot empty: index absent

        long value = dir[pos];
        long storedIndex = dir[pos + 1] - 1;

        if (storedIndex == -1) { // subdir pointer: dive one level
            assert value != 0 : "subdir pointer recid 0";
            long[] result = treeRemoveCollapsing(dirShift, value, store, level - 1, false, index);
            if (result == null || result == REMOVED) return result;
            // child collapsed: result[2],[3] is its lone terminal occupant, pushed up
            if (dir.length == 4 && !topLevel) {
                // that pointer was our only occupant: collapse this node too
                store.delete(recid, DIR_SER);
                return result;
            }
            // replace the subdir pointer with the pushed-up terminal entry
            dir = dir.clone();
            dir[pos] = result[2];
            dir[pos + 1] = result[3];
            store.update(recid, dir, DIR_SER);
            return REMOVED;
        }

        if (storedIndex != index) return null; // terminal for a DIFFERENT index: absent

        dir = dirRemove(dir, slot);
        if (dir.length == 4 && dir[3] > 0 && !topLevel) {
            // one occupant left and it is TERMINAL (dir[3] = storedIndex+1 > 0): delete
            // this node and push the occupant up (never push up a subdir pointer — its
            // subtree's indices don't share the parent slot's full path)
            store.delete(recid, DIR_SER);
            return dir;
        }
        store.update(recid, dir, DIR_SER);
        return REMOVED;
    }

    /** Callback for {@link #treeFold}. */
    public interface EntryVisitor {
        void visit(long index, long value);
    }

    /**
     * Visit every {@code (index, value)} entry in the tree, unordered.
     * Caller must hold at least the tree's read lock for the whole traversal.
     *
     * Deliberately NOT push-down: a full traversal decodes every pair anyway, and a
     * {@link RecordRead} may be invoked on torn bytes, so it could not safely call
     * the visitor (external state) mid-invocation — buffering pairs until validation
     * would just reintroduce the per-node allocation that {@code store.get} does.
     */
    public static void treeFold(long recid, Store store, int level, EntryVisitor visitor) {
        assert level >= 0 : "dir tree deeper than declared levels";
        long[] dir = store.get(recid, DIR_SER);
        for (int pos = 2; pos < dir.length; pos += 2) {
            long value = dir[pos];
            long storedIndex = dir[pos + 1] - 1;
            if (storedIndex == -1) {
                assert value != 0 : "subdir pointer recid 0";
                treeFold(value, store, level - 1, visitor);
            } else {
                visitor.visit(storedIndex, value);
            }
        }
    }

    /**
     * Delete the whole tree's contents in one traversal (port of mapdb3
     * IndexTreeListJava.treeClear): every subdir record is freed, the ROOT record is
     * reset to {@link #dirEmpty()} IN PLACE (recid stays stable, §0.6), and every
     * terminal entry is reported to {@code visitor} so the caller can free the records
     * its values point to. Caller must hold the tree's write lock.
     */
    public static void treeClear(long recid, Store store, int level, EntryVisitor visitor) {
        treeClear(recid, store, level, true, visitor);
    }

    private static void treeClear(long recid, Store store, int level, boolean topLevel,
                                  EntryVisitor visitor) {
        assert level >= 0 : "dir tree deeper than declared levels";
        long[] dir = store.get(recid, DIR_SER);
        if (topLevel) {
            store.update(recid, dirEmpty(), DIR_SER);
        } else {
            store.delete(recid, DIR_SER);
        }
        for (int pos = 2; pos < dir.length; pos += 2) {
            long value = dir[pos];
            long storedIndex = dir[pos + 1] - 1;
            if (storedIndex == -1) {
                assert value != 0 : "subdir pointer recid 0";
                treeClear(value, store, level - 1, false, visitor);
            } else {
                visitor.visit(storedIndex, value);
            }
        }
    }

    /**
     * Push-down root emptiness probe: decodes only the two bitmap longs. An empty root
     * implies an empty tree — every occupant (terminal or subdir chain) has at least one
     * terminal entry beneath it: {@link #treePutSub} only builds chains bottoming out in
     * two terminals, and {@link #treeRemove} collapses any dir that drops to a lone
     * terminal, so pointer chains over zero entries never persist.
     *
     * Stateless (the verdict rides in the return long), so safe under the store's
     * optimistic-read retry discipline.
     */
    private static final RecordRead IS_EMPTY = new RecordRead() {
        @Override public long onBytes(DataInput2 in, int size) {
            return (in.readLong() | in.readLong()) == 0 ? 1 : 0;
        }
        @Override public long onObject(Object record) {
            long[] dir = (long[]) record;
            return (dir[0] | dir[1]) == 0 ? 1 : 0;
        }
        @Override public long onNull() {
            throw new DBException.DataCorruption("dir node record is null");
        }
    };

    /** True iff the tree holds no entries; reads only the root's bitmaps. */
    public static boolean treeIsEmpty(long recid, Store store) {
        return store.read(recid, IS_EMPTY) == 1;
    }
}
