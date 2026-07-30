package org.mapdb.htree;

import org.mapdb.DBException;
import org.mapdb.store.Store;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * Bottom-up bulk builder for HTree directory trees. The caller owns hash
 * validation/grouping and leaf serialization; this class only builds the
 * compressed dir shape and writes each dir record once.
 */
final class HTreePump {

    private HTreePump() {}

    interface LeafWriter<K, V> {
        long writeLeaf(Object[] leaf);
    }

    interface HashSource<K> {
        long hash(K key);
    }

    static final class BuildResult {
        final long[] segmentRoots;
        final long headerRecid;

        BuildResult(long[] segmentRoots, long headerRecid) {
            this.segmentRoots = segmentRoots;
            this.headerRecid = headerRecid;
        }
    }

    interface HeaderWriter {
        long writeHeader(long[] segmentRoots);
    }

    static <K, V> BuildResult build(Store store, int concShift, int dirShift, int levels,
                                    Iterator<? extends Map.Entry<K, V>> entries,
                                    HashSource<? super K> hashSource,
                                    org.mapdb.ser.Serializer<K> keySer,
                                    LeafWriter<K, V> leafWriter,
                                    HeaderWriter headerWriter) {
        long[] roots = new long[1 << concShift];
        store.preallocate(roots.length, roots);

        int segmentShift = levels * dirShift;
        long indexMask = (1L << segmentShift) - 1;
        int concMask = (1 << concShift) - 1;

        BuildState state = new BuildState();
        long previousHash = 0;
        boolean havePreviousHash = false;

        Group<K, V> group = null;
        while (entries.hasNext()) {
            Map.Entry<K, V> e = entries.next();
            K key = e.getKey();
            V value = e.getValue();
            if (key == null || value == null) throw new NullPointerException();
            long hash = hashSource.hash(key);
            if (havePreviousHash && Long.compareUnsigned(previousHash, hash) > 0) {
                throw new DBException.NotSorted("bulk-load hashes not sorted unsigned");
            }
            havePreviousHash = true;
            previousHash = hash;

            int segment = (int) (hash >>> segmentShift) & concMask;
            long index = hash & indexMask;
            if (group == null) {
                group = new Group<>(segment, index);
            } else if (group.segment != segment || group.index != index) {
                if (segment < group.segment) {
                    throw new DBException.NotSorted("bulk-load segments not sorted");
                }
                if (group.segment == segment && Long.compareUnsigned(group.index, index) > 0) {
                    throw new DBException.NotSorted("bulk-load indices not sorted");
                }
                flushGroup(store, roots, dirShift, levels, leafWriter, group, state);
                group = new Group<>(segment, index);
            }
            group.add(keySer, key, value);
        }

        if (group != null) {
            flushGroup(store, roots, dirShift, levels, leafWriter, group, state);
        }
        if (state.segmentBuilder != null) {
            store.update(roots[state.currentSegment], state.segmentBuilder.finish(store), DirTree.DIR_SER);
            state.currentSegment++;
        } else {
            state.currentSegment++;
        }
        for (int seg = Math.max(0, state.currentSegment); seg < roots.length; seg++) {
            store.update(roots[seg], DirTree.dirEmpty(), DirTree.DIR_SER);
        }

        long headerRecid = headerWriter.writeHeader(roots);
        return new BuildResult(roots, headerRecid);
    }

    private static final class BuildState {
        int currentSegment = -1;
        SegmentBuilder segmentBuilder;
    }

    private static <K, V> void flushGroup(Store store, long[] roots, int dirShift, int levels,
                                          LeafWriter<K, V> leafWriter, Group<K, V> group,
                                          BuildState state) {
        if (state.currentSegment == -1) {
            for (int seg = 0; seg < group.segment; seg++) {
                store.update(roots[seg], DirTree.dirEmpty(), DirTree.DIR_SER);
            }
            state.currentSegment = group.segment;
            state.segmentBuilder = new SegmentBuilder(dirShift, levels);
        } else if (group.segment != state.currentSegment) {
            store.update(roots[state.currentSegment], state.segmentBuilder.finish(store), DirTree.DIR_SER);
            for (int seg = state.currentSegment + 1; seg < group.segment; seg++) {
                store.update(roots[seg], DirTree.dirEmpty(), DirTree.DIR_SER);
            }
            state.currentSegment = group.segment;
            state.segmentBuilder = new SegmentBuilder(dirShift, levels);
        }
        long leafRecid = leafWriter.writeLeaf(group.toLeaf());
        state.segmentBuilder.put(group.index, leafRecid);
    }

    private static final class Group<K, V> {
        final int segment;
        final long index;
        Object[] leaf = new Object[8];
        int size;

        Group(int segment, long index) {
            this.segment = segment;
            this.index = index;
        }

        @SuppressWarnings("unchecked")
        void add(org.mapdb.ser.Serializer<K> keySer, K key, V value) {
            for (int i = 0; i < size; i += 2) {
                if (keySer.equals((K) leaf[i], key)) {
                    throw new DBException.NotSorted("duplicate key in hash bucket: " + key);
                }
            }
            if (size == leaf.length) {
                Object[] next = new Object[leaf.length * 2];
                System.arraycopy(leaf, 0, next, 0, leaf.length);
                leaf = next;
            }
            leaf[size++] = key;
            leaf[size++] = value;
        }

        Object[] toLeaf() {
            Object[] out = new Object[size];
            System.arraycopy(leaf, 0, out, 0, size);
            return out;
        }
    }

    private static final class SegmentBuilder {
        final int dirShift;
        final int rootLevel;
        final Node root = new Node();

        SegmentBuilder(int dirShift, int levels) {
            this.dirShift = dirShift;
            this.rootLevel = levels - 1;
        }

        void put(long index, long value) {
            put(root, rootLevel, index, value);
        }

        private void put(Node node, int level, long index, long value) {
            int slot = DirTree.treePos(dirShift, level, index);
            int pos = DirTree.dirOffsetFromSlot(node.dir, slot);
            if (pos < 0) {
                node.dir = DirTree.dirPut(node.dir, slot, value, index + 1);
                return;
            }
            long oldIndex = node.dir[pos + 1] - 1;
            if (oldIndex == -1) {
                put(node.child(slot), level - 1, index, value);
                return;
            }
            if (oldIndex == index) {
                throw new DBException.NotSorted("duplicate hash index " + index);
            }
            long oldValue = node.dir[pos];
            Node child = treePutSub(level - 1, index, value, oldIndex, oldValue);
            node.children.put(slot, child);
            node.dir = node.dir.clone();
            node.dir[pos] = 0;
            node.dir[pos + 1] = 0;
        }

        private Node treePutSub(int level, long index1, long value1, long index2, long value2) {
            if (level < 0) throw new DBException.NotSorted("duplicate hash index " + index1);
            int pos1 = DirTree.treePos(dirShift, level, index1);
            int pos2 = DirTree.treePos(dirShift, level, index2);
            Node node = new Node();
            if (pos1 == pos2) {
                Node child = treePutSub(level - 1, index1, value1, index2, value2);
                node.children.put(pos1, child);
                node.dir = DirTree.dirPut(node.dir, pos1, 0, 0);
            } else {
                node.dir = DirTree.dirPut(node.dir, pos1, value1, index1 + 1);
                node.dir = DirTree.dirPut(node.dir, pos2, value2, index2 + 1);
            }
            return node;
        }

        long[] finish(Store store) {
            return materialize(root, store);
        }

        private long[] materialize(Node node, Store store) {
            long[] dir = node.dir.clone();
            for (Map.Entry<Integer, Node> e : node.children.entrySet()) {
                int pos = DirTree.dirOffsetFromSlot(dir, e.getKey());
                long childRecid = store.put(materialize(e.getValue(), store), DirTree.DIR_SER);
                dir[pos] = childRecid;
                dir[pos + 1] = 0;
            }
            assert DirTree.dirLenInvariant(dir);
            return dir;
        }
    }

    private static final class Node {
        long[] dir = DirTree.dirEmpty();
        final TreeMap<Integer, Node> children = new TreeMap<>();

        Node child(int slot) {
            Node child = children.get(slot);
            if (child == null) throw new DBException.DataCorruption("missing in-memory child");
            return child;
        }
    }
}
