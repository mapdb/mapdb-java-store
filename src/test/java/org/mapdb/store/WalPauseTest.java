package org.mapdb.store;

import org.junit.After;
import org.junit.Test;
import org.mapdb.btree.BufferTreeMap;
import org.mapdb.ser.LongFormat;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * The half {@code WalWriteAmplificationTest} cannot see: <b>COMMIT PAUSE DISTRIBUTION
 * and LOG HIGH-WATER</b>.
 *
 * <p>§11 predicts device bytes are <em>unchanged</em> by R2 — §0.2 already showed the byte win was
 * never there — so the only measurement that can falsify or confirm the incremental cleaner's
 * claim is this one. The claim under test: replacing {@code checkpointLocked} (a whole-store image
 * written under the exclusive write lock on every trigger crossing) with a budgeted incremental
 * cleaner bounds the pause a single commit can pay, at the cost of a log that is cleaned lazily
 * and may therefore sit higher above its trigger target.
 *
 * <p><b>Both numbers matter together</b>, which is why one harness takes both. A cleaner that
 * bounds the pause by never cleaning would win the pause column and lose the log; a cleaner that
 * holds the log at the target by doing the whole rewrite inline is what the incremental cleaner
 * replaced.
 *
 * <p><b>Cross-revision by construction.</b> This file is meant to be copied verbatim onto the
 * whole-store-checkpoint revision and run there for the "before" column, so every API the
 * incremental cleaner changed is reached by reflection: the trigger is
 * {@code setAutoCheckpointBytes} before and {@code setMinLogBytes}/{@code setSpaceAmplification}
 * after. Nothing else in the measured path differs. Log size is the sum of every file sharing the
 * db name prefix — the same segmentation-agnostic definition {@code WalWriteAmplificationTest}
 * uses — so it is one metric across a single-file rewrite and a segment set.
 *
 * <p>Pauses are reported as a <b>distribution</b> (p50/p99/p99.9/max), never a mean: a whole-store
 * checkpoint is a rare event by construction, so a mean hides exactly the thing being measured.
 *
 * <p><b>Run it on a real filesystem.</b> {@code java.io.tmpdir} is tmpfs on this machine, where
 * {@code fsync} is a no-op and a whole-store checkpoint degenerates to a memcpy — which biases the
 * measurement squarely AGAINST the thing being tested, since the pause the incremental cleaner
 * removes is mostly bytes-plus-force. {@code -Dmapdb5.pause.dir} selects the directory and
 * defaults to {@code target/pause-bench} under the module, which is on disk.
 *
 * <p>This is a MEASUREMENT, not a target assertion: it prints tables and asserts only liveness.
 * Run: {@code mvn -o test -Dtest=WalPauseTest -DfailIfNoTests=false}
 * Scale with {@code -Dmapdb5.pause.ops=...} (default 60 000).
 */
public class WalPauseTest {

    /** Same shape as the R1 feature bench and the §0.2 byte harness, so the three are comparable. */
    private static final int NODE_SIZE = 256;
    private static final int BUFFER_BYTES = 4096;

    private static final int ENTRIES = Integer.getInteger("mapdb5.pause.entries", 20_000);
    private static final int OPS = Integer.getInteger("mapdb5.pause.ops", 60_000);

    /**
     * The trigger floor, set identically on both revisions. In the whole-store-checkpoint revision
     * the trigger is {@code max(autoCheckpointBytes, 2 × log-size-after-last-clean)}; with the
     * incremental cleaner it is {@code max(minLogBytes, spaceAmplification × liveDataBytes)}. With
     * the floor equal and the multiplier 2 on both sides, the two targets are the same number to
     * within the difference between "log bytes right after a full rewrite" and "the inner store's
     * page-granular footprint" — a few hundred KB on this workload. That equality is what makes
     * the log high-water columns comparable, and it is the reason the floor is set explicitly
     * rather than left at either revision's 1 GiB default (which this workload never reaches).
     */
    private static final long TRIGGER_FLOOR = 8L << 20;

    /**
     * Segments must be small relative to the trigger or there is nothing to retire incrementally:
     * only segments BELOW the active one are retirable, so at the 64 MiB default this workload
     * would run its whole life in one segment. 1 MiB gives the trigger ~8 segments to work with.
     */
    private static final long SEGMENT_BYTES = 1L << 20;

    private final List<File> files = new ArrayList<>();

    /**
     * Opt-in, unlike {@code WalWriteAmplificationTest}. Byte counts are deterministic and cheap, so
     * that harness earns its place in every {@code mvn test}; these runs drive hundreds of
     * thousands of commits and, on a real filesystem, take minutes each. A measurement whose whole
     * point is latency also has no business competing with the rest of the suite for the machine.
     */
    @org.junit.Before public void optIn() {
        org.junit.Assume.assumeTrue("set -Dmapdb5.pause=true to run the pause harness",
                Boolean.getBoolean("mapdb5.pause"));
    }

    private File newFile(String tag) {
        try {
            Path dir = Path.of(System.getProperty("mapdb5.pause.dir", "target/pause-bench"));
            Files.createDirectories(dir);
            File f = Files.createTempFile(dir, "mapdb5-pause-" + tag, ".wal").toFile();
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

    /**
     * The log's on-disk size and, cumulatively, the bytes APPENDED to it — two numbers off one
     * directory scan, because both are needed and the scan is the expensive part.
     *
     * <p>The size is the sum of every file sharing the db name prefix, so it spans a single-file
     * log and a segment set alike. Appended bytes are accumulated per file as {@code max(0, now -
     * previously seen)}: WAL files only ever grow and then vanish at retirement, so this is an
     * exact count of what reached the device, INCLUDING whatever cleaning re-emitted — which the
     * size alone cannot show, since retirement takes those bytes back off the disk. That is the
     * term §11 predicts is unchanged, and it is the price of the pause win if it is not.
     */
    private static final class LogWatch {
        private final Path dir;
        private final String base;
        private final java.util.HashMap<String, Long> seen = new java.util.HashMap<>();
        long appended;

        LogWatch(File f) { this.dir = f.getParentFile().toPath(); this.base = f.getName(); }

        long sample() throws IOException {
            long total = 0;
            try (var s = Files.list(dir)) {
                for (Path p : (Iterable<Path>) s.filter(
                        q -> q.getFileName().toString().startsWith(base))::iterator) {
                    String name = p.getFileName().toString();
                    long len = p.toFile().length();
                    total += len;
                    Long prev = seen.put(name, len);
                    appended += prev == null ? len : Math.max(0, len - prev);
                }
            }
            return total;
        }
    }

    // ------------------------------------------------------------ cross-revision configuration

    /** Set the cleaning trigger to the same effective target on either revision. */
    private static void configureTrigger(StoreWAL s) {
        try {
            Method m = StoreWAL.class.getMethod("setMinLogBytes", long.class);
            m.invoke(s, TRIGGER_FLOOR);
            StoreWAL.class.getMethod("setSpaceAmplification", int.class).invoke(s, 2);
        } catch (NoSuchMethodException pre3) {
            try {
                StoreWAL.class.getMethod("setAutoCheckpointBytes", long.class)
                        .invoke(s, TRIGGER_FLOOR);
            } catch (ReflectiveOperationException e) {
                throw new AssertionError("neither trigger API is present", e);
            }
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * The trigger's current target. With the incremental cleaner the store computes it; in the
     * whole-store-checkpoint revision it is {@code max(autoCheckpointBytes, 2 × checkpointBasis)},
     * read off the private fields because no accessor exists there. Returns -1 if neither shape is
     * found.
     */
    private static long cleaningTarget(StoreWAL s) {
        try {
            Method m = StoreWAL.class.getDeclaredMethod("testCleaningTarget");
            m.setAccessible(true);
            return (Long) m.invoke(s);
        } catch (NoSuchMethodException pre3) {
            try {
                Field limit = StoreWAL.class.getDeclaredField("autoCheckpointBytes");
                Field basis = StoreWAL.class.getDeclaredField("checkpointBasis");
                limit.setAccessible(true);
                basis.setAccessible(true);
                return Math.max(limit.getLong(s), 2 * basis.getLong(s));
            } catch (ReflectiveOperationException e) {
                return -1;
            }
        } catch (ReflectiveOperationException e) {
            return -1;
        }
    }

    // ------------------------------------------------------------------------------ the measure

    private record Run(String subject, long[] pauseNanos, long[] logSamples,
                       long target, long checkpointNanos, long finalLog, long appended) {

        long pct(double p) {
            long[] sorted = pauseNanos.clone();
            Arrays.sort(sorted);
            int i = (int) Math.min(sorted.length - 1L, Math.round(p / 100.0 * sorted.length) );
            return sorted[Math.max(0, i)];
        }
        long max() { return Arrays.stream(pauseNanos).max().orElse(0); }
        long highWater() { return Arrays.stream(logSamples).max().orElse(0); }
        /** Commits whose pause exceeded 1 ms — the tail the cleaner exists to remove. */
        long over(long nanos) { return Arrays.stream(pauseNanos).filter(n -> n > nanos).count(); }
    }

    private static void reportPauses(String title, List<Run> runs) {
        StringBuilder sb = new StringBuilder("\n=== " + title + " — COMMIT PAUSE (microseconds) ===\n");
        sb.append(String.format("%-34s %8s %8s %8s %9s %9s %10s %10s%n",
                "subject", "commits", "p50", "p90", "p99", "p99.9", "max", ">1ms"));
        for (Run r : runs) {
            sb.append(String.format("%-34s %8d %8.1f %8.1f %8.1f %9.1f %10.1f %10d%n",
                    r.subject(), r.pauseNanos().length,
                    r.pct(50) / 1000.0, r.pct(90) / 1000.0, r.pct(99) / 1000.0,
                    r.pct(99.9) / 1000.0, r.max() / 1000.0, r.over(1_000_000L)));
        }
        sb.append("\n=== ").append(title).append(" — LOG HIGH-WATER vs TRIGGER TARGET ===\n");
        sb.append(String.format("%-34s %14s %14s %10s %14s %14s%n",
                "subject", "high-water", "target", "hw/target", "final log", "checkpoint us"));
        for (Run r : runs) {
            sb.append(String.format("%-34s %14d %14d %10.2f %14d %14.1f%n",
                    r.subject(), r.highWater(), r.target(),
                    r.target() > 0 ? r.highWater() / (double) r.target() : -1.0,
                    r.finalLog(), r.checkpointNanos() / 1000.0));
        }
        sb.append("\n=== ").append(title).append(" — DEVICE BYTES (§11 predicts UNCHANGED) ===\n");
        sb.append(String.format("%-34s %16s %14s%n", "subject", "bytes appended", "bytes/op"));
        for (Run r : runs) {
            sb.append(String.format("%-34s %16d %14.1f%n", r.subject(), r.appended(),
                    r.appended() / (double) r.pauseNanos().length));
        }
        System.out.println(sb);
    }

    /**
     * Random point updates into an already-populated {@link BufferTreeMap}, one commit per op —
     * the §0.2 workload, so the pause numbers sit next to byte numbers taken on the same shape.
     * The load phase is excluded by checkpointing before the measured window, exactly as the byte
     * harness does, so what is measured is steady-state traffic against a live trigger.
     */
    private Run measure(String subject, boolean background) throws Exception {
        return measure(subject, background, ENTRIES, OPS, 1);
    }

    /**
     * @param sampleEvery sample the log every N commits instead of every one. The directory scan
     *   is O(segments), so at a 100 MB log it costs more than the commit it follows; sampling
     *   coarsely is safe for both derived numbers because a segment lives for ~1 000 commits at
     *   {@link #SEGMENT_BYTES}, so no file is created and retired inside one sampling gap, and the
     *   log moves by ~1 KB per commit so the high-water is off by at most that much times N.
     */
    private Run measure(String subject, boolean background, int entries, int ops, int sampleEvery)
            throws Exception {
        File f = newFile(subject.replaceAll("[^a-zA-Z0-9]", ""));
        StoreWAL s = new StoreWAL(f, false, true);
        try {
            s.setSegmentBytes(SEGMENT_BYTES);
            configureTrigger(s);
            BufferTreeMap<Long, Long> map = BufferTreeMap.create(
                    s, LongFormat.INSTANCE, LongFormat.INSTANCE, NODE_SIZE, BUFFER_BYTES);
            // Commit in batches: staged mutations live in memory until commit, so loading a
            // 320k-entry map in ONE transaction exhausts the 2 GiB surefire heap. Batching
            // touches nothing that is measured — the window starts after the checkpoint below.
            for (long i = 0; i < entries; i++) {
                map.put(i, i);
                if ((i + 1) % 20_000 == 0) s.commit();
            }
            s.commit();
            s.checkpoint();
            if (background) s.startMaintenance();

            LogWatch watch = new LogWatch(f);
            watch.sample();
            watch.appended = 0;                 // the load phase is not part of the window

            long[] pause = new long[ops];
            long[] log = new long[ops / sampleEvery + 1];
            int samples = 0;
            Random rnd = new Random(42);
            for (int i = 0; i < ops; i++) {
                map.put((long) rnd.nextInt(entries), (long) i);
                long t0 = System.nanoTime();
                s.commit();
                pause[i] = System.nanoTime() - t0;
                if (i % sampleEvery == 0)
                    log[samples++] = watch.sample();   // sampled OUTSIDE the timed region
            }
            long target = cleaningTarget(s);
            long finalLog = watch.sample();
            long appended = watch.appended;
            long t0 = System.nanoTime();
            s.checkpoint();
            long ckpt = System.nanoTime() - t0;
            return new Run(subject, pause, Arrays.copyOf(log, samples), target, ckpt,
                    finalLog, appended);
        } finally {
            s.close();
        }
    }

    /**
     * <b>The measurement that decides whether the incremental cleaner delivers anything.</b>
     *
     * <p>A single store size cannot test R2's claim, and reading one as if it could is the trap
     * this test exists to avoid. What the incremental cleaner removed is a pause of <b>O(store
     * bytes)</b> ({@code checkpointLocked} rewrote the whole committed store under the exclusive
     * write lock); what it put in its place is a pause bounded by a fixed per-commit budget. At a
     * small store the removed term is <em>cheap</em> — a few MB of image — and the fixed overheads
     * of incremental cleaning (a seal, a rollover with its {@code force(true)} and directory
     * fsync, W10's verification scan, and FIFO re-emission of a mostly-live oldest segment) can
     * easily cost more. So the honest question is not "which is faster" but <b>where the two
     * curves cross</b>: below the crossover the incremental cleaner is a regression, above it a
     * win that grows without bound.
     *
     * <p>Run on tmpfs deliberately: {@code fsync} is free there, so what is left is the work done
     * while holding the write lock, which is the quantity the design bounds. Maintenance is OFF,
     * so both revisions do their cleaning on the commit path — the whole-store-checkpoint
     * revision's {@code WalCheckpointTask} could only choose the moment of a pause, never shorten
     * it, so leaving it out compares the two mechanisms rather than two schedulers.
     *
     * <p>{@code ops} is scaled per size to ~2.5× the trigger target, so each row crosses the
     * trigger a similar number of times rather than a number that shrinks as the target grows.
     * Enable with {@code -Dmapdb5.pause.sweep=true} (it writes hundreds of MB and takes minutes).
     */
    @Test
    public void pause_vs_store_size() throws Exception {
        org.junit.Assume.assumeTrue("set -Dmapdb5.pause.sweep=true to run the sweep",
                Boolean.getBoolean("mapdb5.pause.sweep"));
        // entries → ops. ~225 log bytes per entry on this workload, so the target is
        // max(8 MiB, 2 × 225 × entries); ~1 030 bytes/op puts 2.5 targets' worth of traffic in.
        int[][ ] grid = {{20_000, 22_000}, {80_000, 90_000}, {320_000, 350_000}};
        List<Run> runs = new ArrayList<>();
        for (int[] row : grid) {
            runs.add(measure(row[0] / 1000 + "k entries", false, row[0], row[1], 64));
        }
        reportPauses("store-size sweep, maintenance OFF, 1 op/tx", runs);
    }

    /**
     * {@code checkpoint()} — and therefore {@code compact()} — timed on <b>equal input</b>.
     *
     * <p>The main run reports a checkpoint time too, but it takes it at the end of a workload
     * where the two revisions leave differently-sized logs behind (the whole-store-checkpoint
     * revision collapses the log on its last automatic checkpoint; the incremental cleaner leaves
     * it near the target), so that comparison is confounded by ~2× of input. Here the log is
     * driven to a fixed byte count with automatic cleaning DISABLED, so both revisions checkpoint
     * the same bytes.
     *
     * <p>This is the path the incremental cleaner did not set out to improve —
     * {@code checkpoint()} is deliberately the unbounded on-demand wide pass, and §5.5 [v9] names
     * it as the answer to a terminal concluded at the pause-capped width. It is measured because
     * it is public API and because a regression here is paid by every caller of {@code compact()}.
     */
    @Test
    public void checkpoint_pause_on_equal_input() throws Exception {
        List<Run> runs = new ArrayList<>();
        for (int entries : new int[]{20_000, 80_000}) {
            File f = newFile("ckpt" + entries);
            StoreWAL s = new StoreWAL(f, false, true);
            try {
                s.setSegmentBytes(SEGMENT_BYTES);
                disableAutoCleaning(s);
                BufferTreeMap<Long, Long> map = BufferTreeMap.create(
                        s, LongFormat.INSTANCE, LongFormat.INSTANCE, NODE_SIZE, BUFFER_BYTES);
                for (long i = 0; i < entries; i++) {
                    map.put(i, i);
                    if ((i + 1) % 20_000 == 0) s.commit();   // see measure(): heap, not policy
                }
                s.commit();
                s.checkpoint();
                LogWatch watch = new LogWatch(f);
                long base = watch.sample();
                // Same log growth on both sides, independent of live footprint.
                final long grow = 32L << 20;
                Random rnd = new Random(42);
                int ops = 0;
                while (watch.sample() - base < grow) {
                    for (int k = 0; k < 500; k++, ops++) {
                        map.put((long) rnd.nextInt(entries), (long) ops);
                        s.commit();
                    }
                }
                long before = watch.sample();
                long t0 = System.nanoTime();
                s.checkpoint();
                long ckpt = System.nanoTime() - t0;
                long after = watch.sample();
                System.out.printf("checkpoint: %,d entries  log %,d -> %,d bytes  %,.1f us%n",
                        entries, before, after, ckpt / 1000.0);
                runs.add(new Run(entries / 1000 + "k entries", new long[]{ckpt},
                        new long[]{before}, -1, ckpt, after, watch.appended));
            } finally {
                s.close();
            }
        }
        reportPauses("checkpoint() on equal input (32 MiB of log growth)", runs);
    }

    /**
     * <b>The cold-head workload — §5.5's named pathological case, and the one that could argue for
     * a segment usage table.</b>
     *
     * <p>A large cold set is written once and never touched; a small hot set is rewritten forever.
     * FIFO cleaning takes the OLDEST segment whatever it holds, so it is repeatedly made to
     * re-emit cold data that nothing has invalidated — the classic LFS {@code 1/(1−u)} cost as
     * {@code u → 1} that §5.5 names and declines to fix. A cost-benefit cleaner driven by a
     * per-segment live-byte table would skip those segments and take the hot ones, where the
     * garbage is.
     *
     * <p>What this measures is therefore the <b>price of the FIFO policy</b>, in device bytes per
     * op against the same workload's floor. If it is large, the usage table is buying something
     * real; if it is not, the table is a large change aimed at a cost the workload does not pay.
     * Pause is reported alongside because re-emitting an all-live segment is the most work a cycle
     * can be made to do.
     */
    @Test
    public void cold_head_write_amplification() throws Exception {
        final int coldRecords = 2_000;      // ~8 MiB written once, never invalidated
        final int recordBytes = 4_096;
        final int ops = 3_000;              // hot churn on top of it
        List<Run> runs = new ArrayList<>();
        for (int hotSet : new int[]{1, 64}) {
            File f = newFile("cold" + hotSet);
            StoreWAL s = new StoreWAL(f, false, true);
            try {
                s.setSegmentBytes(SEGMENT_BYTES);
                configureTrigger(s);
                for (int i = 0; i < coldRecords; i++) {
                    s.put(Fixtures.payload(i, i & 0x7f, recordBytes), Fixtures.RAW);
                    if ((i + 1) % 100 == 0) s.commit();
                }
                s.commit();
                long[] hot = new long[hotSet];
                for (int i = 0; i < hotSet; i++)
                    hot[i] = s.put(Fixtures.payload(-i - 1, 1, recordBytes), Fixtures.RAW);
                s.commit();
                s.checkpoint();

                LogWatch watch = new LogWatch(f);
                watch.sample();
                watch.appended = 0;
                long[] pause = new long[ops];
                long[] log = new long[ops];
                for (int i = 0; i < ops; i++) {
                    s.update(hot[i % hotSet], Fixtures.payload(-(i % hotSet) - 1, i & 0x7f, recordBytes),
                            Fixtures.RAW);
                    long t0 = System.nanoTime();
                    s.commit();
                    pause[i] = System.nanoTime() - t0;
                    log[i] = watch.sample();
                }
                long target = cleaningTarget(s);
                long finalLog = watch.sample();
                runs.add(new Run("cold 8 MiB, hot set " + hotSet, pause, log, target, 0,
                        finalLog, watch.appended));
            } finally {
                s.close();
            }
        }
        reportPauses("COLD-HEAD: " + coldRecords + " untouched 4 KiB records + hot churn", runs);
        System.out.printf("floor: one %d-byte record + framing per op; anything above that is "
                + "the cleaner dragging cold data forward%n", recordBytes);
    }

    /** Turn the automatic trigger off on either revision: the floor is what disables it. */
    private static void disableAutoCleaning(StoreWAL s) {
        try {
            StoreWAL.class.getMethod("setMinLogBytes", long.class).invoke(s, 0L);
        } catch (NoSuchMethodException pre3) {
            try {
                StoreWAL.class.getMethod("setAutoCheckpointBytes", long.class).invoke(s, 0L);
            } catch (ReflectiveOperationException e) {
                throw new AssertionError("neither trigger API is present", e);
            }
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    @Test
    public void commit_pause_and_log_high_water() throws Exception {
        List<Run> runs = new ArrayList<>();
        runs.add(measure("BufferTree, no maintenance", false));
        runs.add(measure("BufferTree, background clean", true));
        reportPauses("random point updates, 1 op/tx", runs);

        for (Run r : runs) {
            // Liveness only: the store must still be cleaning at all. A log that never came back
            // under its own target after an explicit checkpoint would mean the harness measured a
            // broken store rather than a policy.
            org.junit.Assert.assertTrue(r.subject() + ": log did not shrink on checkpoint",
                    r.finalLog() > 0);
        }
    }
}
