package org.mapdb.store;

import org.junit.Test;
import org.mapdb.DBException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Oracle fuzz. A seeded op sequence over a bounded recid set is run
 * op-for-op against {@link StoreByteArray} (the reference implementation / oracle) and
 * each store under test ({@link StoreDirect}, {@link StoreWAL}); every outcome (return
 * value, exception type, and full content via a size-driven get) must match.
 *
 * Recids are NOT assumed identical across stores (WAL defers recid reuse to commit), so a
 * per-store recid map is kept per logical slot. Capacity is implementation-defined (REFUSED
 * is a permitted, not mandated, response), so appends are gated by a headroom budget that
 * all stores guarantee — this keeps the append *content* comparable while still exercising
 * the append path heavily. Content bytes encode (slot, version) so any mismatch is
 * self-describing.
 */
public class StoreFuzzTest {

    private static final int N_SLOTS = 200;
    private static final int OPS = 10_000;

    // logical per-slot oracle state
    private static final int VOID = 0, PREALLOC = 1, LIVE = 2;

    // op kinds
    private static final int PUT = 0, PREALLOC_OP = 1, UPDATE = 2, UPDATE_HR = 3,
            APPEND = 4, DELETE = 5, CAS = 6, GET = 7, COMMIT = 8;

    private StoreDelta[] stores;
    private String[] names;
    private long[][] recid;   // [store][slot]
    private int[] state;      // per slot
    private byte[][] content; // per slot (null = null-content record)
    private int[] budget;     // per slot, guaranteed-appendable bytes
    private boolean[] allocated;
    private int version = 1;

    @Test public void fuzz_direct_and_wal_match_reference() throws IOException {
        long seed = Long.getLong("fuzz.seed", 0xDEADBEEF12345L);
        System.out.println("StoreFuzzTest seed=" + Long.toHexString(seed) + " ops=" + OPS + " slots=" + N_SLOTS);
        File walFile = Files.createTempFile("mapdb-fuzz", ".wal").toFile();
        walFile.delete();
        StoreWAL wal = new StoreWAL(walFile);
        stores = new StoreDelta[]{new StoreByteArray(), new StoreDirect(), wal, new StoreAppendOnly(),
                new StoreAppendOnly(false, 1 << 16)}; // eager compaction: sweeps fire mid-fuzz
        names = new String[]{"ByteArray(ref)", "Direct", "WAL", "AppendOnly", "AppendOnly(compacting)"};
        recid = new long[stores.length][N_SLOTS];
        for (long[] row : recid) Arrays.fill(row, -1L);
        state = new int[N_SLOTS];
        content = new byte[N_SLOTS][];
        budget = new int[N_SLOTS];
        allocated = new boolean[N_SLOTS];

        Random rnd = new Random(seed);
        try {
            for (int op = 0; op < OPS; op++) {
                step(rnd, op);
                if (op % 500 == 0) for (StoreDelta s : stores) s.verify();
            }
            // final commit + full state reconciliation
            for (StoreDelta s : stores) { s.commit(); s.verify(); }
            for (int slot = 0; slot < N_SLOTS; slot++) checkContent(slot, -1);
        } finally {
            for (StoreDelta s : stores) s.close();
            walFile.delete();
        }
    }

    private void step(Random rnd, int op) {
        int slot = rnd.nextInt(N_SLOTS);
        int kind = pickKind(rnd);

        // normalize op vs slot state so every call is drivable
        if (kind == COMMIT) { for (StoreDelta s : stores) s.commit(); return; }
        if ((kind == PUT || kind == PREALLOC_OP) && state[slot] != VOID) kind = UPDATE;
        if (kind != PUT && kind != PREALLOC_OP && !allocated[slot]) kind = PUT;

        switch (kind) {
            case PUT -> doPut(rnd, slot, op);
            case PREALLOC_OP -> doPrealloc(slot, op);
            case UPDATE -> doUpdate(rnd, slot, op, false);
            case UPDATE_HR -> doUpdate(rnd, slot, op, true);
            case APPEND -> doAppend(rnd, slot, op);
            case DELETE -> doDelete(slot, op);
            case CAS -> doCas(rnd, slot, op);
            case GET -> checkContent(slot, op);
            default -> throw new AssertionError();
        }
    }

    private int pickKind(Random rnd) {
        int r = rnd.nextInt(100);
        if (r < 14) return PUT;
        if (r < 22) return PREALLOC_OP;
        if (r < 40) return UPDATE;
        if (r < 52) return UPDATE_HR;
        if (r < 70) return APPEND;
        if (r < 80) return DELETE;
        if (r < 90) return CAS;
        if (r < 96) return GET;
        return COMMIT;
    }

    // ---------- op implementations ----------

    private byte[] maybeNull(Random rnd, int slot, int extra) {
        if (rnd.nextInt(10) == 0) return null;
        return Fixtures.payload(slot, version++, extra);
    }

    private void doPut(Random rnd, int slot, int op) {
        byte[] v = maybeNull(rnd, slot, rnd.nextInt(20));
        for (int i = 0; i < stores.length; i++) {
            long r = stores[i].put(v, Fixtures.RAW);
            assertTrue(msg(op, "put returned recid 0", i), r != 0);
            recid[i][slot] = r;
        }
        allocated[slot] = true;
        state[slot] = LIVE;
        content[slot] = v;
        budget[slot] = 0;
        checkContent(slot, op);
    }

    private void doPrealloc(int slot, int op) {
        for (int i = 0; i < stores.length; i++) {
            long r = stores[i].preallocate();
            assertTrue(msg(op, "preallocate returned recid 0", i), r != 0);
            recid[i][slot] = r;
        }
        allocated[slot] = true;
        state[slot] = PREALLOC;
        content[slot] = null;
        budget[slot] = 0;
        checkContent(slot, op);
    }

    private void doUpdate(Random rnd, int slot, int op, boolean headroom) {
        byte[] v = maybeNull(rnd, slot, rnd.nextInt(20));
        int h = headroom ? 8 + rnd.nextInt(56) : 0;
        boolean expectVoid = state[slot] == VOID;
        for (int i = 0; i < stores.length; i++) {
            boolean threw = false;
            try {
                if (headroom) stores[i].updateWithHeadroom(recid[i][slot], v, Fixtures.RAW, h);
                else stores[i].update(recid[i][slot], v, Fixtures.RAW);
            } catch (DBException.GetVoid e) { threw = true; }
            assertEquals(msg(op, "update void mismatch", i), expectVoid, threw);
        }
        if (!expectVoid) {
            state[slot] = LIVE;
            content[slot] = v;
            budget[slot] = (headroom && v != null) ? h : 0;
        }
        checkContent(slot, op);
    }

    private void doAppend(Random rnd, int slot, int op) {
        boolean expectVoid = state[slot] == VOID;
        boolean establishes = state[slot] == PREALLOC || (state[slot] == LIVE && content[slot] == null);
        int len;
        if (expectVoid) len = 1 + rnd.nextInt(4);
        else if (establishes) len = 1 + rnd.nextInt(8);
        else if (budget[slot] > 0) len = 1 + rnd.nextInt(budget[slot]);
        else len = 0; // no guaranteed capacity: legal zero-length no-op
        byte[] data = new byte[len];
        for (int k = 0; k < len; k++) data[k] = (byte) (version + k * 7 + slot);
        version++;

        Long ret = null;
        for (int i = 0; i < stores.length; i++) {
            boolean threw = false;
            long r = 0;
            try {
                r = stores[i].append(recid[i][slot], data, 0, len);
            } catch (DBException.GetVoid e) { threw = true; }
            assertEquals(msg(op, "append void mismatch", i), expectVoid, threw);
            if (!threw) {
                assertTrue(msg(op, "append unexpectedly REFUSED (len=" + len + ")", i), r != StoreDelta.REFUSED);
                if (ret == null) ret = r;
                else assertEquals(msg(op, "append return mismatch", i), (long) ret, r);
            }
        }
        if (!expectVoid) {
            if (establishes) {
                content[slot] = data.clone();
                state[slot] = LIVE;
                budget[slot] = 0;
            } else {
                content[slot] = concat(content[slot], data);
                budget[slot] -= len;
            }
            long expSize = content[slot] == null ? 0 : content[slot].length;
            assertEquals(msg(op, "append size wrong", 0), expSize, (long) ret);
        }
        checkContent(slot, op);
    }

    private void doDelete(int slot, int op) {
        boolean expectVoid = state[slot] == VOID;
        for (int i = 0; i < stores.length; i++) {
            boolean threw = false;
            try {
                stores[i].delete(recid[i][slot], Fixtures.RAW);
            } catch (DBException.GetVoid e) { threw = true; }
            assertEquals(msg(op, "delete void mismatch", i), expectVoid, threw);
        }
        if (!expectVoid) {
            // Free the slot entirely: the recid may now be reused by a put into another
            // slot (LIFO/FIFO reuse), so keeping the stale mapping would alias a live
            // record. Deleted->GetVoid semantics are covered exhaustively by the TCK.
            state[slot] = VOID;
            content[slot] = null;
            budget[slot] = 0;
            allocated[slot] = false;
            for (int i = 0; i < stores.length; i++) recid[i][slot] = -1L;
        }
    }

    private void doCas(Random rnd, int slot, int op) {
        boolean expectVoid = state[slot] == VOID;
        byte[] current = state[slot] == LIVE ? content[slot] : null; // PREALLOC compares vs null
        byte[] expected = rnd.nextBoolean() ? current : maybeNull(rnd, slot, rnd.nextInt(8));
        byte[] newVal = maybeNull(rnd, slot, rnd.nextInt(12));
        boolean eq = Arrays.equals(expected, current);

        Boolean ret = null;
        for (int i = 0; i < stores.length; i++) {
            boolean threw = false;
            boolean r = false;
            try {
                r = stores[i].compareAndSwap(recid[i][slot], expected, newVal, Fixtures.RAW);
            } catch (DBException.GetVoid e) { threw = true; }
            assertEquals(msg(op, "cas void mismatch", i), expectVoid, threw);
            if (!threw) {
                if (ret == null) ret = r;
                else assertEquals(msg(op, "cas return mismatch", i), ret, r);
            }
        }
        if (!expectVoid) {
            assertEquals(msg(op, "cas result vs oracle", 0), eq, ret);
            if (eq) {
                state[slot] = LIVE;
                content[slot] = newVal;
                budget[slot] = 0;
            }
        }
        checkContent(slot, op);
    }

    // ---------- content reconciliation ----------

    /** Asserts get() on the slot matches the oracle across all stores. op=-1 = final sweep. */
    private void checkContent(int slot, int op) {
        if (!allocated[slot]) return;
        for (int i = 0; i < stores.length; i++) {
            byte[] got;
            boolean threw = false;
            try {
                got = stores[i].get(recid[i][slot], Fixtures.RAW);
            } catch (DBException.GetVoid e) { threw = true; got = null; }
            if (state[slot] == VOID) {
                assertTrue(msg(op, "get should GetVoid", i), threw);
            } else if (state[slot] == PREALLOC) {
                assertTrue(msg(op, "prealloc get should not throw", i), !threw);
                assertEquals(msg(op, "prealloc get should be null", i), null, got);
            } else { // LIVE
                assertTrue(msg(op, "live get should not throw", i), !threw);
                assertArrayEquals(msg(op, "content mismatch", i), content[slot], got);
            }
        }
    }

    private static byte[] concat(byte[] a, byte[] b) {
        if (a == null) return b.clone();
        byte[] out = new byte[a.length + b.length];
        System.arraycopy(a, 0, out, 0, a.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }

    private String msg(int op, String what, int store) {
        return "op#" + op + " store=" + (names == null ? store : names[store]) + " :: " + what;
    }
}
