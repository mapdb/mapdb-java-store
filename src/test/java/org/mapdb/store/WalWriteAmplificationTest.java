package org.mapdb.store;

import org.junit.After;
import org.junit.Test;
import org.mapdb.btree.BTreeMap;
import org.mapdb.btree.BufferTreeMap;
import org.mapdb.ser.LongFormat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * R2's headline claim, measured: DEVICE BYTES WRITTEN PER COMMITTED OP.
 *
 * <p>For {@code StoreWAL} the log is the only durable artifact (the inner {@code StoreDirect}
 * is memory-only), so device bytes == bytes added to the WAL file set, INCLUDING whatever a
 * checkpoint/clean rewrites. Byte counts are deterministic — unlike latency they need no
 * forks or warmup — so this is a plain measurement, not a latency benchmark.
 *
 * <p>Segmentation-agnostic on purpose: {@link #logBytes} sums every file whose name starts
 * with the db file's name, so the same harness measures the single-file v1 log and a
 * multi-segment v2 log without change. That is what makes the before/after comparable.
 *
 * <p>This is a MEASUREMENT, not a target assertion: it prints a table and asserts only
 * liveness invariants. Run:
 * {@code mvn -o test -Dtest=WalWriteAmplificationTest -DfailIfNoTests=false}
 */
public class WalWriteAmplificationTest {

    /** Same shape as the R1 feature bench, so the two are comparable. */
    private static final int NODE_SIZE = 256;
    private static final int BUFFER_BYTES = 4096;

    private final List<File> files = new ArrayList<>();
    private final List<Row> rows = new ArrayList<>();

    private File newFile(String tag) {
        try {
            File f = Files.createTempFile("mapdb5-wamp-" + tag, ".wal").toFile();
            f.delete();
            files.add(f);
            return f;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @After public void cleanup() {
        for (File f : files) {
            File dir = f.getParentFile();
            File[] siblings = dir.listFiles((d, n) -> n.startsWith(f.getName()));
            if (siblings != null) for (File s : siblings) s.delete();
            f.delete();
        }
        files.clear();
    }

    /** Sum of every file whose name starts with {@code f}'s name — one file or N segments. */
    private static long logBytes(File f) throws IOException {
        Path dir = f.getParentFile().toPath();
        String base = f.getName();
        try (var s = Files.list(dir)) {
            return s.filter(p -> p.getFileName().toString().startsWith(base))
                    .mapToLong(p -> p.toFile().length())
                    .sum();
        }
    }

    private record Row(String subject, int ops, int opsPerCommit, long bytes) {
        /** The R2 metric. */
        double bytesPerOp() { return bytes / (double) ops; }
    }

    private void report(String title) {
        StringBuilder sb = new StringBuilder("\n=== " + title + " ===\n");
        sb.append(String.format("%-36s %8s %8s %14s %12s%n",
                "subject", "ops", "ops/tx", "log bytes", "bytes/op"));
        for (Row r : rows) {
            sb.append(String.format("%-36s %8d %8d %14d %12.1f%n",
                    r.subject(), r.ops(), r.opsPerCommit(), r.bytes(), r.bytesPerOp()));
        }
        System.out.println(sb);
        rows.clear();
    }

    // ------------------------------------------------------------------ collections

    /**
     * Random point updates into an already-populated map — the shape the ~130x class lives in
     * (a small value change rewriting a whole leaf image). Both a plain {@link BTreeMap} (full
     * node image per put) and an R1 {@link BufferTreeMap} (blind append) are measured, because
     * R2's win is exactly the gap between them.
     */
    @Test
    public void random_update_write_amplification() throws Exception {
        final int entries = 20_000;
        final int ops = 5_000;

        for (int perCommit : new int[]{1, 10, 100}) {
            rows.add(measureBufferTree(entries, ops, perCommit));
            rows.add(measurePlainBTree(entries, ops, perCommit));
        }
        report("device bytes per committed op — random updates (StoreWAL)");
    }

    private Row measureBufferTree(int entries, int ops, int perCommit) throws Exception {
        File f = newFile("bt-" + perCommit);
        StoreWAL s = new StoreWAL(f, false, true);
        try {
            BufferTreeMap<Long, Long> map =
                    BufferTreeMap.create(s, LongFormat.INSTANCE, LongFormat.INSTANCE, NODE_SIZE, BUFFER_BYTES);
            for (long i = 0; i < entries; i++) map.put(i, i);
            s.commit();
            s.checkpoint();               // exclude the load phase from the measured window
            long before = logBytes(f);

            Random rnd = new Random(42);
            for (int i = 0; i < ops; i++) {
                map.put((long) rnd.nextInt(entries), (long) i);
                if ((i + 1) % perCommit == 0) s.commit();
            }
            s.commit();
            return new Row("BufferTreeMap (R1 append)", ops, perCommit, logBytes(f) - before);
        } finally {
            s.close();
        }
    }

    private Row measurePlainBTree(int entries, int ops, int perCommit) throws Exception {
        File f = newFile("plain-" + perCommit);
        StoreWAL s = new StoreWAL(f, false, true);
        try {
            BTreeMap<Long, Long> map =
                    BTreeMap.create(s, LongFormat.INSTANCE, LongFormat.INSTANCE, NODE_SIZE);
            for (long i = 0; i < entries; i++) map.put(i, i);
            s.commit();
            s.checkpoint();
            long before = logBytes(f);

            Random rnd = new Random(42);
            for (int i = 0; i < ops; i++) {
                map.put((long) rnd.nextInt(entries), (long) i);
                if ((i + 1) % perCommit == 0) s.commit();
            }
            s.commit();
            return new Row("BTreeMap (full node image)", ops, perCommit, logBytes(f) - before);
        } finally {
            s.close();
        }
    }

    // ------------------------------------------------------------------ raw store

    /**
     * Store-level shapes, free of any collection: one record grown by small appends, the same
     * record rewritten whole, and the COLLAPSE case (a transaction that stages
     * both a base and appends degrades today to one full merged image). This isolates the
     * WAL's own behaviour from B-tree mechanics.
     */
    @Test
    public void record_level_append_vs_rewrite() throws Exception {
        final int ops = 2_000;
        final int recordBytes = 4_000;   // ~ a leaf page
        final int deltaBytes = 32;       // ~ one keyed op

        rows.add(measureAppendOnly(ops, recordBytes, deltaBytes));
        rows.add(measureWholeRewrite(ops, recordBytes));
        rows.add(measureCollapse(ops, recordBytes, deltaBytes));
        report("device bytes per committed op — raw record shapes (StoreWAL)");
    }

    /** Grow by append: device cost should already be ~deltaBytes/op today (T_APPEND exists). */
    private Row measureAppendOnly(int ops, int recordBytes, int deltaBytes) throws Exception {
        File f = newFile("append");
        StoreWAL s = new StoreWAL(f, false, true);
        try {
            byte[] base = new byte[recordBytes];
            long recid = s.put(base, Fixtures.RAW);
            s.updateWithHeadroom(recid, base, Fixtures.RAW, ops * deltaBytes + 64);
            s.commit();
            s.checkpoint();
            long before = logBytes(f);
            byte[] d = new byte[deltaBytes];
            for (int i = 0; i < ops; i++) {
                d[0] = (byte) i;
                s.append(recid, d, 0, d.length);
                s.commit();
            }
            return new Row("record: append " + deltaBytes + "B", ops, 1, logBytes(f) - before);
        } finally { s.close(); }
    }

    /** Rewrite the whole record each time: device cost ~recordBytes/op — the amplification. */
    private Row measureWholeRewrite(int ops, int recordBytes) throws Exception {
        File f = newFile("rewrite");
        StoreWAL s = new StoreWAL(f, false, true);
        try {
            byte[] base = new byte[recordBytes];
            long recid = s.put(base, Fixtures.RAW);
            s.commit();
            s.checkpoint();
            long before = logBytes(f);
            for (int i = 0; i < ops; i++) {
                base[0] = (byte) i;
                s.update(recid, base, Fixtures.RAW);
                s.commit();
            }
            return new Row("record: rewrite " + recordBytes + "B", ops, 1, logBytes(f) - before);
        } finally { s.close(); }
    }

    /** §2.4: base staged AND appends in one tx — collapses to a full merged image today. */
    private Row measureCollapse(int ops, int recordBytes, int deltaBytes) throws Exception {
        File f = newFile("collapse");
        StoreWAL s = new StoreWAL(f, false, true);
        try {
            byte[] base = new byte[recordBytes];
            long recid = s.put(base, Fixtures.RAW);
            s.commit();
            s.checkpoint();
            long before = logBytes(f);
            byte[] d = new byte[deltaBytes];
            for (int i = 0; i < ops; i++) {
                base[0] = (byte) i;
                s.updateWithHeadroom(recid, base, Fixtures.RAW, deltaBytes + 64);
                d[0] = (byte) i;
                s.append(recid, d, 0, d.length);
                s.commit();
            }
            return new Row("record: rewrite+append (collapse)", ops, 1, logBytes(f) - before);
        } finally { s.close(); }
    }
}
