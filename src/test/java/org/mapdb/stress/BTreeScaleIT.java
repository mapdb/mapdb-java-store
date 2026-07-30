package org.mapdb.stress;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.mapdb.btree.BTreeMap;
import org.mapdb.ser.LongFormat;
import org.mapdb.ser.ObjectArrayFormat;
import org.mapdb.ser.Serializers;
import org.mapdb.store.StoreDirect;

import java.util.Iterator;
import java.util.Map;
import java.util.SplittableRandom;

import static org.junit.Assert.assertEquals;
import static org.mapdb.stress.StressSupport.*;

/**
 * {@link BTreeMap} scale over {@link StoreDirect} (heap buffers). Tags b1/b2/b3/b4.
 *
 * Full-scale (scale=1.0):
 *   b1 longSeq   : Long->Long, maxNodeSize 32, 200,000,000 sequential inserts,
 *                  100,000,000 random gets, full ascending iteration.
 *   b2 longRand  : Long->Long, maxNodeSize 32, 50,000,000 random-order inserts.
 *   b3 string    : String->Long, ObjectArrayFormat(STRING) keys, maxNodeSize 16,
 *                  20,000,000 entries, 1,000,000 random gets, ordered iteration.
 *   b4 longPump  : b1's workload built via createFromSorted (bulk pump) instead of
 *                  per-entry put — the bulk-load baseline.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class BTreeScaleIT {

    static final long LONG_SEQ_N  = 200_000_000L;
    static final long LONG_GET_N  = 100_000_000L;
    static final long LONG_RAND_N = 50_000_000L;
    static final long STRING_N    = 20_000_000L;
    static final long STRING_GET_N = 1_000_000L;

    @Test public void b1_longSequential() {
        requireTag("b1");
        long n = scaledL(LONG_SEQ_N);
        StoreDirect store = new StoreDirect(false);
        try {
            BTreeMap<Long, Long> map =
                BTreeMap.create(store, LongFormat.INSTANCE, LongFormat.INSTANCE, 32);

            long t0 = System.nanoTime();
            for (long k = 0; k < n; k++) map.put(k, k);
            long tPut = System.nanoTime() - t0;

            long getN = scaledL(LONG_GET_N);
            SplittableRandom rnd = new SplittableRandom(2024);
            long t1 = System.nanoTime();
            for (long i = 0; i < getN; i++) {
                long k = rnd.nextLong(n);
                Long v = map.get(k);
                if (v == null || v != k) throw new AssertionError("get(" + k + ")=" + v);
            }
            long tGet = System.nanoTime() - t1;

            long t2 = System.nanoTime();
            long count = iterateAscending(map);
            long tIter = System.nanoTime() - t2;
            assertEquals("iteration count", n, count);

            summary("btree long-seq PUT (maxNode=32)", n, tPut, n * 16L);
            phase("btree long-seq random GET", getN, tGet);
            phase("btree long-seq ITER(" + count + ")", count, tIter);
        } finally {
            store.close();
        }
        System.gc();
    }

    @Test public void b2_longRandom() {
        requireTag("b2");
        int n = scaled(LONG_RAND_N);
        StoreDirect store = new StoreDirect(false);
        try {
            BTreeMap<Long, Long> map =
                BTreeMap.create(store, LongFormat.INSTANCE, LongFormat.INSTANCE, 32);

            long[] keys = new long[n];
            for (int i = 0; i < n; i++) keys[i] = i;
            SplittableRandom rnd = new SplittableRandom(99);
            for (int i = n - 1; i > 0; i--) { // Fisher-Yates
                int j = rnd.nextInt(i + 1);
                long t = keys[i]; keys[i] = keys[j]; keys[j] = t;
            }

            long t0 = System.nanoTime();
            for (int i = 0; i < n; i++) map.put(keys[i], keys[i]);
            long tPut = System.nanoTime() - t0;

            long verifyN = scaledL(1_000_000L);
            long t1 = System.nanoTime();
            for (long i = 0; i < verifyN; i++) {
                long k = rnd.nextInt(n);
                Long v = map.get(k);
                if (v == null || v != k) throw new AssertionError("get(" + k + ")=" + v);
            }
            long tGet = System.nanoTime() - t1;

            long count = iterateAscending(map);
            assertEquals("iteration count", (long) n, count);

            summary("btree long-rand PUT (maxNode=32)", n, tPut, n * 16L);
            phase("btree long-rand random GET", verifyN, tGet);
        } finally {
            store.close();
        }
        System.gc();
    }

    @Test public void b3_string() {
        requireTag("b3");
        int n = scaled(STRING_N);
        StoreDirect store = new StoreDirect(false);
        try {
            BTreeMap<String, Long> map = BTreeMap.create(
                store, new ObjectArrayFormat<>(Serializers.STRING), LongFormat.INSTANCE, 16);

            long t0 = System.nanoTime();
            for (int i = 0; i < n; i++) map.put(key(i), (long) i);
            long tPut = System.nanoTime() - t0;

            long getN = scaledL(STRING_GET_N);
            SplittableRandom rnd = new SplittableRandom(555);
            long t1 = System.nanoTime();
            for (long i = 0; i < getN; i++) {
                int j = rnd.nextInt(n);
                Long v = map.get(key(j));
                if (v == null || v != j) throw new AssertionError("get(" + key(j) + ")=" + v);
            }
            long tGet = System.nanoTime() - t1;

            // ordered iteration: keys are zero-padded so lexical == numeric order
            long t2 = System.nanoTime();
            long count = 0;
            String prev = null;
            Iterator<Map.Entry<String, Long>> it = map.entryIterator();
            while (it.hasNext()) {
                Map.Entry<String, Long> e = it.next();
                if (prev != null && prev.compareTo(e.getKey()) >= 0)
                    throw new AssertionError("iteration not ascending: " + prev + " -> " + e.getKey());
                prev = e.getKey();
                count++;
            }
            long tIter = System.nanoTime() - t2;
            assertEquals("iteration count", (long) n, count);

            summary("btree string PUT (maxNode=16)", n, tPut, n * 22L);
            phase("btree string random GET", getN, tGet);
            phase("btree string ITER(" + count + ")", count, tIter);
        } finally {
            store.close();
        }
        System.gc();
    }

    /** Bulk-load baseline: same workload as b1 but built through the
     *  pump ({@code createFromSorted}) instead of per-entry put — the pump-vs-put
     *  comparison IS the number this benchmark exists for. */
    @Test public void b4_longPump() {
        requireTag("b4");
        long n = scaledL(LONG_SEQ_N);
        StoreDirect store = new StoreDirect(false);
        try {
            long t0 = System.nanoTime();
            BTreeMap<Long, Long> map = BTreeMap.createFromSorted(
                store, LongFormat.INSTANCE, LongFormat.INSTANCE, 32,
                new Iterator<>() {
                    long i = 0;
                    @Override public boolean hasNext() { return i < n; }
                    @Override public Map.Entry<Long, Long> next() {
                        long k = i++;
                        return new java.util.AbstractMap.SimpleImmutableEntry<>(k, k);
                    }
                });
            long tPump = System.nanoTime() - t0;

            long getN = scaledL(LONG_GET_N);
            SplittableRandom rnd = new SplittableRandom(2024);
            long t1 = System.nanoTime();
            for (long i = 0; i < getN; i++) {
                long k = rnd.nextLong(n);
                Long v = map.get(k);
                if (v == null || v != k) throw new AssertionError("get(" + k + ")=" + v);
            }
            long tGet = System.nanoTime() - t1;

            long t2 = System.nanoTime();
            long count = iterateAscending(map);
            long tIter = System.nanoTime() - t2;
            assertEquals("iteration count", n, count);

            summary("btree long-seq PUMP (maxNode=32)", n, tPump, n * 16L);
            phase("btree long-pump random GET", getN, tGet);
            phase("btree long-pump ITER(" + count + ")", count, tIter);
        } finally {
            store.close();
        }
        System.gc();
    }

    private static String key(int i) { return String.format("key%09d", i); }

    private static long iterateAscending(BTreeMap<Long, Long> map) {
        long count = 0, prev = Long.MIN_VALUE;
        Iterator<Map.Entry<Long, Long>> it = map.entryIterator();
        while (it.hasNext()) {
            Map.Entry<Long, Long> e = it.next();
            long k = e.getKey();
            if (count > 0 && k <= prev)
                throw new AssertionError("iteration not strictly ascending: " + prev + " -> " + k);
            if (e.getValue() != k)
                throw new AssertionError("value mismatch key=" + k + " val=" + e.getValue());
            prev = k;
            count++;
        }
        return count;
    }
}
