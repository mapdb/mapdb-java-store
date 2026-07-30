package org.mapdb.btree;

import org.junit.Test;
import org.mapdb.ser.LongFormat;
import org.mapdb.ser.ObjectArrayFormat;
import org.mapdb.ser.Serializers;
import org.mapdb.store.StoreByteArray;
import org.mapdb.store.StoreDelta;
import org.mapdb.store.StoreDirect;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Targeted white-box mechanics tests for {@link BufferTreeMap}. Each test picks a
 * deliberately tiny {@code maxNodeSize}/{@code bufferBytes} pair to force a single
 * code path (flush cascade, k-way split, tombstone LWW, over-sized op, cross-level
 * merge, reopen, empty-map edges). Complements {@link BufferTreeSmokeTest} — no
 * random-fuzz duplication here; everything is a hand-constructed scenario.
 */
public class BufferTreeMechanicsTest {

    private static BufferTreeMap<Long, Long> longMap(StoreDelta store, int maxNodeSize, int bufferBytes) {
        return BufferTreeMap.create(store, LongFormat.INSTANCE, LongFormat.INSTANCE, maxNodeSize, bufferBytes);
    }

    private static <K extends Comparable<K>, V> void assertOrderedEquals(TreeMap<K, V> oracle,
                                                                         BufferTreeMap<K, V> m) {
        Iterator<Map.Entry<K, V>> a = oracle.entrySet().iterator();
        Iterator<Map.Entry<K, V>> b = m.entryIterator();
        while (a.hasNext()) {
            assertTrue("map iterator ended early", b.hasNext());
            Map.Entry<K, V> ea = a.next(), eb = b.next();
            assertEquals(ea.getKey(), eb.getKey());
            assertEquals(ea.getValue(), eb.getValue());
        }
        assertFalse("map iterator has extra entries", b.hasNext());
    }

    private static <K, V> void assertGetsAgree(TreeMap<K, V> oracle, BufferTreeMap<K, V> m) {
        for (Map.Entry<K, V> e : oracle.entrySet()) {
            assertEquals("get(" + e.getKey() + ")", e.getValue(), m.get(e.getKey()));
        }
    }

    // ============================================================
    // 1. Flush cascade — multi-level flushes, readable at every step
    // ============================================================
    @Test public void flushCascadeReadableThroughout() {
        // maxNodeSize=4 + tiny buffer => the root buffer refuses almost immediately,
        // cascading flushes/splits build a multi-level tree very early.
        BufferTreeMap<Long, Long> m = longMap(new StoreDirect(false), 4, 64);
        int n = 150;
        for (int i = 0; i < n; i++) {
            m.put((long) i, (long) (i * 7 + 1));
            // every previously-inserted key must still be readable at this point
            for (int j = 0; j <= i; j++) {
                assertEquals("after put " + i + " reading " + j,
                        (Long) (long) (j * 7 + 1), m.get((long) j));
            }
        }
        TreeMap<Long, Long> oracle = new TreeMap<>();
        for (int i = 0; i < n; i++) oracle.put((long) i, (long) (i * 7 + 1));
        assertOrderedEquals(oracle, m);
        assertEquals(n, m.sizeLong());
        m.flushAll();
        assertOrderedEquals(oracle, m);
        assertEquals(n, m.sizeLong());
    }

    // ============================================================
    // 2. Multi-way (k-way) split — one flush overfills a leaf far beyond 2x
    // ============================================================
    @Test public void kWaySplitOnOverfilledLeaf() {
        // bufferBytes=2048 with maxNodeSize=4: the initial root-leaf buffer holds
        // ~100 PUT ops (19 bytes each) before REFUSED, so the first flush consolidates
        // a ~100-key batch into a 4-slot leaf and must split it k-way in one shot.
        BufferTreeMap<Long, Long> m = longMap(new StoreDirect(false), 4, 2048);
        int n = 200;
        List<Long> keys = new ArrayList<>();
        for (long k = 0; k < n; k++) keys.add(k);
        Collections.shuffle(keys, new Random(7));
        TreeMap<Long, Long> oracle = new TreeMap<>();
        for (Long k : keys) {
            long v = k * 3 + 5;
            m.put(k, v);
            oracle.put(k, v);
        }
        // merge correctness before any explicit flush (buffers still populated)
        assertOrderedEquals(oracle, m);
        assertGetsAgree(oracle, m);
        m.flushAll();
        assertOrderedEquals(oracle, m);
        assertEquals(n, m.sizeLong());
        assertGetsAgree(oracle, m);
    }

    // ============================================================
    // 3. Tombstone semantics — buffer-only / consolidated / absent + LWW re-order
    // ============================================================
    @Test public void tombstoneSemantics() {
        BufferTreeMap<Long, Long> m = longMap(new StoreDirect(false), 4, 96);
        TreeMap<Long, Long> oracle = new TreeMap<>();

        // build a small consolidated tree so some keys live in leaf bases
        for (long k = 0; k < 40; k++) { m.put(k, k + 1000); oracle.put(k, k + 1000); }
        m.flushAll(); // keys 0..39 now consolidated in leaf bases

        // (a) remove a key that is only in a buffer (never flushed)
        m.put(500L, 5000L); oracle.put(500L, 5000L);
        assertEquals((Long) 5000L, m.get(500L));
        m.remove(500L); oracle.remove(500L);      // tombstone sits in buffer above base-less key
        assertNull(m.get(500L));
        assertFalse(m.containsKey(500L));

        // (b) remove a key that is consolidated in a leaf base
        assertEquals((Long) 1005L, m.get(5L));
        m.remove(5L); oracle.remove(5L);           // tombstone in root buffer, value deep in a leaf
        assertNull("consolidated key hidden by buffered tombstone", m.get(5L));
        assertOrderedEquals(oracle, m);            // iterate must skip it before flush
        m.flushAll();                              // tombstone reaches the leaf and deletes the base entry
        assertNull(m.get(5L));
        assertOrderedEquals(oracle, m);

        // (c) remove a key that is absent entirely — must be a no-op, no crash
        m.remove(9999L);                           // never inserted
        assertNull(m.get(9999L));
        assertOrderedEquals(oracle, m);

        // (d) LWW within a buffer: a tombstone OLDER than a later PUT of the same key loses
        m.remove(7L);                              // tombstone appended first
        m.put(7L, 77777L);                         // later PUT of same key overrides it
        oracle.put(7L, 77777L);
        assertEquals("later PUT beats earlier tombstone (append-order LWW)", (Long) 77777L, m.get(7L));
        assertOrderedEquals(oracle, m);
        m.flushAll();
        assertEquals((Long) 77777L, m.get(7L));
        assertOrderedEquals(oracle, m);
    }

    // ============================================================
    // 4. Op larger than an empty buffer — direct delivery, no infinite loop
    // ============================================================
    @Test public void opLargerThanBufferDeliveredDirectly() {
        // Values are big Strings that dwarf the 64-byte buffer, so append() always
        // REFUSES and writeMessage must push the single op straight down. maxNodeSize=4
        // makes the root become a dir after a few inserts, exercising the recursive
        // direct-delivery path (dir -> leaf) as well as the initial leaf path.
        BufferTreeMap<Long, String> m = BufferTreeMap.create(
                new StoreDirect(false), LongFormat.INSTANCE,
                new ObjectArrayFormat<>(Serializers.STRING), 4, 64);
        TreeMap<Long, String> oracle = new TreeMap<>();
        int n = 12;
        for (long k = 0; k < n; k++) {
            String v = "v" + k + "-" + "x".repeat(2000); // ~2KB, >> 64-byte buffer
            m.put(k, v);
            oracle.put(k, v);
            assertEquals("big value readable immediately after put", v, m.get(k));
        }
        assertGetsAgree(oracle, m);
        assertOrderedEquals(oracle, m);
        assertEquals(n, m.sizeLong());
        m.flushAll();
        assertGetsAgree(oracle, m);
        assertOrderedEquals(oracle, m);
    }

    // ============================================================
    // 5. Iterator merging — ops for one range live at several levels at once
    // ============================================================
    @Test public void iteratorMergesOpsAcrossLevels() {
        // Build a flushed multi-level tree, then layer buffered updates that (as the
        // root buffer overflows) settle at different depths without being flushed to
        // the leaves. The iterator must overlay root-buffer + interior-dir-buffer +
        // leaf-buffer ops onto the leaf bases with correct LWW — WITHOUT flushAll.
        BufferTreeMap<Long, Long> m = longMap(new StoreDirect(false), 4, 96);
        TreeMap<Long, Long> oracle = new TreeMap<>();
        for (long k = 0; k < 120; k++) { m.put(k, k); oracle.put(k, k); }
        m.flushAll(); // every key now in a leaf base; buffers empty

        // Layer many updates + a few new keys + a few removes; these stay buffered
        // at whatever level each batch stalls at. No flush before comparison.
        for (long k = 0; k < 120; k += 3) { m.put(k, k + 900_000); oracle.put(k, k + 900_000); }
        for (long k = 120; k < 140; k++)  { m.put(k, k + 500_000); oracle.put(k, k + 500_000); }
        for (long k = 1; k < 120; k += 17) { m.remove(k); oracle.remove(k); }
        for (long k = 2; k < 40; k += 2)  { m.put(k, k + 111);     oracle.put(k, k + 111); }

        // multi-level merge correctness with live buffers at (potentially) every level
        assertOrderedEquals(oracle, m);
        assertGetsAgree(oracle, m);
        assertEquals(oracle.size(), m.sizeLong());

        // and it stays correct once everything is pushed to the leaves
        m.flushAll();
        assertOrderedEquals(oracle, m);
        assertGetsAgree(oracle, m);
    }

    // ============================================================
    // 6. LWW across levels — newer root-buffer op beats deep consolidated value
    // ============================================================
    @Test public void lwwAcrossLevels() {
        BufferTreeMap<Long, Long> m = longMap(new StoreDirect(false), 4, 64);
        // build depth so the target key lands several levels down after flushing
        for (long k = 0; k < 100; k++) m.put(k, k);
        long key = 42L;
        m.put(key, 1L);
        m.flushAll();                 // v1 now consolidated deep in a leaf base
        assertEquals((Long) 1L, m.get(key));

        m.put(key, 2L);               // v2 enters the root buffer (newest, highest level)
        assertEquals("newer root-buffer PUT wins over deep base value", (Long) 2L, m.get(key));
        boolean sawTwo = false, sawOne = false;
        for (Iterator<Map.Entry<Long, Long>> it = m.entryIterator(); it.hasNext(); ) {
            Map.Entry<Long, Long> e = it.next();
            if (e.getKey() == key) { sawTwo |= e.getValue() == 2L; sawOne |= e.getValue() == 1L; }
        }
        assertTrue("iterator shows v2", sawTwo);
        assertFalse("iterator never shows stale v1", sawOne);

        m.remove(key);                // root-buffer tombstone over the deep v1
        assertNull("buffered tombstone hides deep consolidated value", m.get(key));
        for (Iterator<Map.Entry<Long, Long>> it = m.entryIterator(); it.hasNext(); ) {
            assertFalse("iterator skips removed key", it.next().getKey() == key);
        }
        m.flushAll();                 // tombstone reaches the leaf
        assertNull(m.get(key));
    }

    // ============================================================
    // 7. sizeLong with mixed buffered/consolidated state
    // ============================================================
    @Test public void sizeLongMixedState() {
        BufferTreeMap<Long, Long> m = longMap(new StoreDirect(false), 4, 96);
        TreeMap<Long, Long> oracle = new TreeMap<>();
        for (long k = 0; k < 50; k++) { m.put(k, k); oracle.put(k, k); }
        m.flushAll();                                   // 50 consolidated
        assertEquals(oracle.size(), m.sizeLong());

        for (long k = 50; k < 80; k++) { m.put(k, k); oracle.put(k, k); } // 30 buffered new keys
        m.put(10L, 999L); oracle.put(10L, 999L);        // buffered update of consolidated key (no size change)
        m.remove(20L); oracle.remove(20L);              // buffered tombstone over consolidated key
        m.remove(60L); oracle.remove(60L);              // buffered tombstone over buffered key
        m.remove(4242L);                                // tombstone over absent key (no size change)

        assertEquals("size with live buffers over consolidated base", oracle.size(), m.sizeLong());
        m.flushAll();
        assertEquals("size after flush", oracle.size(), m.sizeLong());
        assertOrderedEquals(oracle, m);
    }

    // ============================================================
    // 8. Reopen — second read-only instance on same store + rootRecidRecid
    // ============================================================
    @Test public void reopenReadOnlyAgrees() {
        StoreDirect store = new StoreDirect(false);
        BufferTreeMap<Long, Long> writer =
                BufferTreeMap.create(store, LongFormat.INSTANCE, LongFormat.INSTANCE, 4, 96);
        TreeMap<Long, Long> oracle = new TreeMap<>();
        List<Long> keys = new ArrayList<>();
        for (long k = 0; k < 300; k++) keys.add(k);
        Collections.shuffle(keys, new Random(11));
        for (Long k : keys) { writer.put(k, k * 2 + 1); oracle.put(k, k * 2 + 1); }
        for (int i = 0; i < keys.size(); i += 5) { Long k = keys.get(i); writer.remove(k); oracle.remove(k); }

        long rrr = writer.rootRecidRecid();
        // single-writer contract: reopen as a pure reader AFTER writes are quiesced
        BufferTreeMap<Long, Long> reader = BufferTreeMap.open(
                store, rrr, LongFormat.INSTANCE, LongFormat.INSTANCE, 4, 96);
        assertGetsAgree(oracle, reader);
        assertNull(reader.get(999_999L));
        assertOrderedEquals(oracle, reader);
        assertEquals(oracle.size(), reader.sizeLong());
    }

    // ============================================================
    // 9. Empty-map edge cases
    // ============================================================
    @Test public void emptyMapEdges() {
        BufferTreeMap<Long, Long> m = longMap(new StoreByteArray(), 4, 64);
        assertFalse("empty iterator has no next", m.entryIterator().hasNext());
        assertEquals(0L, m.sizeLong());
        assertNull(m.get(1L));
        assertFalse(m.containsKey(1L));
        m.remove(1L);            // remove on empty — no crash
        m.flushAll();            // flush on empty — no crash
        assertFalse(m.entryIterator().hasNext());
        assertEquals(0L, m.sizeLong());
        assertNull(m.get(1L));

        // a removed-then-reinserted key still behaves on an otherwise-empty map
        m.put(1L, 100L);
        m.remove(1L);
        assertNull(m.get(1L));
        assertEquals(0L, m.sizeLong());
        assertFalse(m.entryIterator().hasNext());
    }

    // ============================================================
    // 10. Non-canonical element serializer — get takes the decode-and-compare
    //     op-scan path (equalsBySerializedBytes()==false), same answers
    // ============================================================
    @Test public void nonCanonicalKeySerializer_getUsesDecodeFallback() {
        // delegate to LONG but withhold the canonical-bytes capability
        org.mapdb.ser.Serializer<Long> nonCanonical = new org.mapdb.ser.Serializer<>() {
            @Override public void serialize(org.mapdb.io.DataOutput2 out, Long v) {
                Serializers.LONG.serialize(out, v);
            }
            @Override public Long deserialize(org.mapdb.io.DataInput2 in, int size) {
                return Serializers.LONG.deserialize(in, size);
            }
            @Override public int fixedSize() { return 8; }
            @Override public int compare(Long a, Long b) { return Long.compare(a, b); }
        };
        assertFalse(nonCanonical.equalsBySerializedBytes());
        BufferTreeMap<Long, Long> m = BufferTreeMap.create(new StoreDirect(false),
                new ObjectArrayFormat<>(nonCanonical), new ObjectArrayFormat<>(nonCanonical), 4, 96);
        TreeMap<Long, Long> oracle = new TreeMap<>();
        Random rnd = new Random(42);
        for (int i = 0; i < 500; i++) {
            long k = rnd.nextInt(200);
            if (rnd.nextInt(10) < 7) { m.put(k, k * 31); oracle.put(k, k * 31); }
            else { m.remove(k); oracle.remove(k); }
        }
        assertGetsAgree(oracle, m);                       // buffered (op tails hot)
        assertNull(m.get(100_000L));
        m.flushAll();
        assertGetsAgree(oracle, m);                       // consolidated
        assertOrderedEquals(oracle, m);
    }
}
