package org.mapdb.store;

import org.junit.After;
import org.junit.Test;
import org.mapdb.DBException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * <b>The replay transition gate.</b> Exhaustive enumeration of every history up to a bounded
 * depth over a bounded number of recids, each checked against an in-memory oracle after
 * EVERY operation.
 *
 * <p>Why this is a gate and not a nice-to-have: the base stamp is a single point of writer
 * truth, and §4.2 establishes that a commit-time cross-check of it would be circular (the
 * only value the apply path could compare against is the very table the stamp was read
 * from). Byte-exact writer fixtures catch a PORT diverging from Java; they cannot catch Java
 * itself stamping a wrong base. Enumeration can. Every counterexample found so far lives at
 * depth ≤ 5 over ≤ 3 recids, and manual inspection never exhausted the transition table —
 * so the evidence has to be mechanical.
 *
 * <p><b>What this covers:</b> {@code put}, {@code update}, {@code update-to-null},
 * {@code append}, a rejected {@code append} (bad arguments — the transaction must be
 * unchanged), {@code delete}, {@code preallocate}, {@code commit}, {@code rollback},
 * {@code checkpoint} (the re-homing event) and {@code reopen}, including consecutive
 * reopens, over every live recid.
 *
 * <p><b>What it CANNOT find, stated so the bound is never mistaken for completeness.</b>
 * These were established by counterexample, not by inspection — two confirmed defects lived
 * in the first two:
 * <ol>
 *   <li><b>Capacity boundaries.</b> Payloads here are small and headroom is fixed, so the
 *       straddle at {@code IndexVal.MAX_CAPACITY} — where a merged record still fits but its
 *       requested headroom does not — is unreachable at any depth. That is exactly where the
 *       a confirmed defect lived. {@code StoreWALWriterReplayAgreementTest} covers it directly.</li>
 *   <li><b>Anything below the API.</b> Hand-crafted log images, torn tails, bit rot and
 *       durable-write faults are not modelled; {@code StoreWALBaseIdentityTest} and
 *       {@code StoreWALFormatV2Test} carry those.</li>
 *   <li><b>Capacity admission itself.</b> The oracle MIRRORS the store's own {@code REFUSED}
 *       answer rather than modelling capacity independently, so a wrongly admitted or wrongly
 *       refused append is invisible to it — only the resulting CONTENT is checked
 *       independently.</li>
 *   <li><b>Preallocated vs null-content.</b> Both read as null through the Store API, so the
 *       oracle cannot tell them apart even though they are distinct WAL entries with distinct
 *       §4.2 rows.</li>
 *   <li><b>Crash-at-writer-obligation boundaries.</b> With a single log file the only mid-write
 *       boundary is the checkpoint swap, which {@code StoreWALCheckpointTest} covers. Segment
 *       rollover, unlink and the {@code 'K'} mark are outside this enumerator — it would have
 *       to grow those events and the crash points between them;
 *       {@code StoreWALSegmentEventEnumeratorTest} enumerates them instead.</li>
 *   <li><b>The encoder append path</b> ({@code append(recid, DeltaEncoder)}) and its LSN
 *       reservation; {@code StoreWALDeltaLsnTest} covers that.</li>
 * </ol>
 *
 * <p><b>The CI gate is depth {@value #DEFAULT_DEPTH}</b>, which is what a plain
 * {@code mvn test} runs. Depth 5 — the bound §4.2 names, ~37k histories and ~60s — is a manual
 * invocation: {@code -Dmapdb.enum.depth=5}. The count of histories actually checked is
 * printed, because a silently truncated sweep reads exactly like a complete one.
 */
public class StoreWALSmallModelEnumeratorTest {

    private static final int DEFAULT_DEPTH = 4;
    /** Records live at once; §4.2's counterexamples all fit in 3. */
    private static final int MAX_RECIDS = 3;

    private final List<File> files = new ArrayList<>();
    private int histories;
    private int operations;

    @After public void cleanup() {
        for (File f : files) {
            f.delete();
            org.mapdb.store.WalTestKit.deleteStore(f);
        }
        files.clear();
    }

    // ---------------------------------------------------------------- the oracle

    /** Committed and currently-visible state; value {@code null} = present with null content. */
    private static final class Model {
        final Map<Long, byte[]> committed;
        final Map<Long, byte[]> visible;

        Model() { this(new LinkedHashMap<>(), new LinkedHashMap<>()); }

        Model(Map<Long, byte[]> committed, Map<Long, byte[]> visible) {
            this.committed = committed;
            this.visible = visible;
        }

        Model copy() { return new Model(copyOf(committed), copyOf(visible)); }

        void commit() { committed.clear(); committed.putAll(copyOf(visible)); }

        /** Rollback and reopen agree: both discard everything not committed. */
        void discardStaged() { visible.clear(); visible.putAll(copyOf(committed)); }

        static Map<Long, byte[]> copyOf(Map<Long, byte[]> m) {
            Map<Long, byte[]> c = new LinkedHashMap<>();
            for (Map.Entry<Long, byte[]> e : m.entrySet())
                c.put(e.getKey(), e.getValue() == null ? null : e.getValue().clone());
            return c;
        }
    }

    /** One step of a history: a label for the failure message and the effect on both sides. */
    private interface Step {
        String label();
        /** @return the store handle to keep using (reopen returns a new one) */
        StoreWAL run(StoreWAL s, Model m, File f);
    }

    // ---------------------------------------------------------------- the sweep

    @Test public void every_short_history_recovers_to_the_oracle_state() throws IOException {
        int depth = Integer.getInteger("mapdb.enum.depth", DEFAULT_DEPTH);
        long t0 = System.nanoTime();
        explore(new ArrayList<>(), depth);
        long ms = (System.nanoTime() - t0) / 1_000_000;
        System.out.printf("small-model enumerator: depth %d, ≤%d recids, %d histories,"
                        + " %d operations, %d ms%n", depth, MAX_RECIDS, histories, operations, ms);
        // a sweep that silently checked nothing would also pass every assertion in it
        if (histories < 1000) fail("enumeration collapsed to " + histories + " histories");
    }

    private void explore(List<String> prefix, int remaining) throws IOException {
        if (remaining == 0) {
            run(prefix);
            return;
        }
        // The applicable alphabet depends on the state a prefix reaches, so it is recomputed
        // by replaying the prefix — the sweep is over histories, not over op-code tuples.
        List<String> alphabet = alphabetAfter(prefix);
        run(prefix); // every prefix is itself a history: check it, do not only check leaves
        for (String op : alphabet) {
            List<String> next = new ArrayList<>(prefix);
            next.add(op);
            explore(next, remaining - 1);
        }
    }

    /**
     * Ops that would throw on the state a prefix reaches (an update of a recid that does not
     * exist yet) are not part of the alphabet: they are pruned rather than counted, so the
     * printed history count is the number of histories genuinely exercised.
     */
    private List<String> alphabetAfter(List<String> prefix) throws IOException {
        Model m = new Model();
        File f = newFile();
        StoreWAL s = new StoreWAL(f);
        try {
            for (String op : prefix) s = apply(s, m, f, op);
            List<String> ops = new ArrayList<>();
            if (m.visible.size() < MAX_RECIDS) {
                ops.add("put");
                ops.add("prealloc");
            }
            for (int i = 0; i < m.visible.size(); i++) {
                ops.add("update" + i);
                ops.add("null" + i);
                ops.add("append" + i);
                ops.add("delete" + i);
            }
            // a rejected call must leave the transaction exactly as it found it; the
            // defect was a bad-argument append that left an empty staged entry behind, which the
            // classifier then turned into an entry replay refuses
            if (!m.visible.isEmpty()) ops.add("badappend");
            ops.add("commit");
            ops.add("rollback");
            ops.add("checkpoint");
            ops.add("reopen");
            return ops;
        } finally {
            close(s);
            f.delete();
            org.mapdb.store.WalTestKit.deleteStore(f);
        }
    }

    private void run(List<String> history) throws IOException {
        try {
            runHistory(history);
        } catch (DBException e) {
            throw new AssertionError("history " + history + " broke the store: " + e.getMessage(), e);
        }
        histories++;
    }

    private void runHistory(List<String> history) throws IOException {
        Model m = new Model();
        File f = newFile();
        StoreWAL s = new StoreWAL(f);
        try {
            for (String op : history) {
                try {
                    s = apply(s, m, f, op);
                } catch (RuntimeException e) {
                    throw new AssertionError("history " + history + " failed at " + op, e);
                }
                operations++;
                check(s, m.visible, history, op);
            }
            // the load-bearing check: a final reopen must rebuild exactly the committed state
            close(s);
            s = new StoreWAL(f);
            check(s, m.committed, history, "final reopen");
            s.verify();
            // and again, because a reopen that is only correct once is not correct (§4.2 asks
            // for double and triple reopen explicitly)
            close(s);
            s = new StoreWAL(f);
            check(s, m.committed, history, "second reopen");
        } finally {
            close(s);
            f.delete();
            org.mapdb.store.WalTestKit.deleteStore(f);
        }
    }

    private StoreWAL apply(StoreWAL s, Model m, File f, String op) {
        switch (op) {
            case "put" -> {
                byte[] v = value(m.visible.size(), "p");
                long r = s.put(v, Fixtures.RAW);
                m.visible.put(r, v);
            }
            case "prealloc" -> {
                long r = s.preallocate();
                m.visible.put(r, null);
            }
            case "commit" -> {
                s.commit();
                m.commit();
            }
            case "rollback" -> {
                s.rollback();
                m.discardStaged();
            }
            case "checkpoint" -> s.checkpoint();
            case "badappend" -> {
                long r = recidAt(m, 0);
                byte[] d = value(0, "a");
                try {
                    s.append(r, d, -1, d.length);
                    fail("expected an argument error");
                } catch (IndexOutOfBoundsException expected) { /* nothing may have changed */ }
            }
            case "reopen" -> {
                close(s);
                m.discardStaged();
                return new StoreWAL(f);
            }
            default -> {
                int idx = op.charAt(op.length() - 1) - '0';
                long r = recidAt(m, idx);
                if (op.startsWith("update")) {
                    byte[] v = value(idx, "u");
                    s.updateWithHeadroom(r, v, Fixtures.RAW, 64); // headroom: appends may follow
                    m.visible.put(r, v);
                } else if (op.startsWith("null")) {
                    s.update(r, null, Fixtures.RAW);
                    m.visible.put(r, null);
                } else if (op.startsWith("append")) {
                    byte[] d = value(idx, "a");
                    long res = s.append(r, d, 0, d.length);
                    // capacity refusal is the store's answer to give; the CONTENT is still
                    // modelled independently, which is what the oracle is for
                    if (res != StoreDelta.REFUSED) {
                        byte[] cur = m.visible.get(r);
                        m.visible.put(r, cur == null ? d.clone() : concat(cur, d));
                    }
                } else if (op.startsWith("delete")) {
                    s.delete(r, Fixtures.RAW);
                    m.visible.remove(r);
                } else {
                    throw new AssertionError("unknown op " + op);
                }
            }
        }
        return s;
    }

    private void check(StoreWAL s, Map<Long, byte[]> expected, List<String> history, String at) {
        for (Map.Entry<Long, byte[]> e : expected.entrySet()) {
            byte[] got;
            try {
                got = s.get(e.getKey(), Fixtures.RAW);
            } catch (DBException.GetVoid v) {
                throw new AssertionError("history " + history + " after " + at
                        + ": recid " + e.getKey() + " vanished", v);
            }
            if (e.getValue() == null) assertNull("history " + history + " after " + at
                    + ": recid " + e.getKey() + " should have null content", got);
            else assertArrayEquals("history " + history + " after " + at
                    + ": recid " + e.getKey(), e.getValue(), got);
        }
        // and nothing that should be gone may still be there
        long max = 0;
        for (long r : expected.keySet()) max = Math.max(max, r);
        for (long r = 1; r <= max + 2; r++) {
            if (expected.containsKey(r)) continue;
            try {
                s.get(r, Fixtures.RAW);
                fail("history " + history + " after " + at + ": recid " + r + " should be void");
            } catch (DBException.GetVoid expectedVoid) { /* ok */ }
        }
    }

    private static long recidAt(Model m, int idx) {
        int i = 0;
        for (long r : m.visible.keySet()) {
            if (i++ == idx) return r;
        }
        throw new AssertionError("no recid at index " + idx);
    }

    private static byte[] value(int seed, String kind) {
        return (kind + seed + "-0123456789").getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] r = new byte[a.length + b.length];
        System.arraycopy(a, 0, r, 0, a.length);
        System.arraycopy(b, 0, r, a.length, b.length);
        return r;
    }

    private static void close(StoreWAL s) {
        try {
            s.close();
        } catch (RuntimeException ignored) { /* already closed / failed closed */ }
    }

    private File newFile() throws IOException {
        File f = Files.createTempFile("mapdb-wal-enum", ".wal").toFile();
        f.delete();
        files.add(f);
        return f;
    }

    @Test public void the_alphabet_is_what_the_sweep_claims_it_is() throws IOException {
        // guards the sweep against silently losing an op: the empty-state alphabet is fixed
        assertEquals(List.of("put", "prealloc", "commit", "rollback", "checkpoint", "reopen"),
                alphabetAfter(new ArrayList<>()));
        // and with one live record, every per-recid op plus the rejected-call probe
        assertEquals(List.of("put", "prealloc", "update0", "null0", "append0", "delete0",
                        "badappend", "commit", "rollback", "checkpoint", "reopen"),
                alphabetAfter(new ArrayList<>(List.of("put"))));
    }
}
