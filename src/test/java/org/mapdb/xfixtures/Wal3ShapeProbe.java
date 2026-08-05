package org.mapdb.xfixtures;

import org.mapdb.store.StoreWAL;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;

/**
 * C2 shape probe: run one candidate {@code cleaned} workload and leave its segments on disk.
 *
 * <p>The Stage C plan predicts, from the rollover rule alone, that §5.3's literal workload
 * ("T1&ndash;T3 then {@code checkpoint()}") <em>cannot</em> produce the shape §5.3.1 requires,
 * because the cleaner's image of 1.2 MB of live data pushes the {@code 'K'} mark out of the
 * lowest retained segment and into the next one &mdash; where row 2 forbids it. That prediction
 * was reasoned, not measured. This probe measures it: it runs a named variant against the real
 * engine, closes cleanly, and leaves {@code x.wal.*} for
 * {@code todo/store-wal3/probes/dump-bundle.py}, which decides the witness rows executably
 * ({@code derive.check_witnesses}) rather than by eye.
 *
 * <pre>
 *   mvn -q test-compile
 *   java -ea -cp target/test-classes:target/classes \
 *       org.mapdb.xfixtures.Wal3ShapeProbe --out &lt;dir&gt; --variant &lt;name&gt;
 *   python3 ../todo/store-wal3/probes/dump-bundle.py &lt;dir&gt;
 * </pre>
 *
 * <p>Every variant that could become the generator's workload ends in the final logical state §5.3
 * pins &mdash; A live(n+5,120), B live(n+1,0), C null, D prealloc, E deleted, F live(n+4,1200000)
 * &mdash; and asserts it before closing, so a variant that reaches the shape by changing what the
 * fixture MEANS fails here rather than in review. The one exception is
 * {@code shaped-half-rotate}, which exists precisely to show that half of a state-preserving PAIR
 * is not state-preserving; it asserts the state it DOES reach — A holding the oversized payload,
 * the rest of §5.2 unchanged — rather than skipping the check. This is a probe, not the generator:
 * it publishes nothing and pins nothing.
 */
public final class Wal3ShapeProbe {

    private Wal3ShapeProbe() {}

    /** §5.1: pinned small so the workload rotates without 64 MiB fixtures. */
    static final long SEGMENT_BYTES = 65_536;
    /** §5.1: above the whole workload's byte total, so auto-clean never fires. */
    static final long MIN_LOG_BYTES = 64L * 1024 * 1024;
    /** §5.2's payload-id base for the java cleaned bundle. */
    static final int BASE = 111;

    /** Recids the workload allocated, in allocation order. */
    static final class Recids {
        long a, b, c, d, e, f;
    }

    private static StoreWAL open(File base) {
        StoreWAL s = new StoreWAL(base);
        s.setSegmentBytes(SEGMENT_BYTES);
        s.setMinLogBytes(MIN_LOG_BYTES);
        return s;
    }

    // ---------- the §5.2/§5.3 transactions, one method each ----------

    private static void t1(StoreWAL s, Recids r) {
        r.a = s.put(FixtureWriter.payload(BASE + 0, 100), FixtureWriter.RAW);
        r.b = s.put(FixtureWriter.payload(BASE + 1, 0), FixtureWriter.RAW);
        r.c = s.put(FixtureWriter.payload(BASE + 2, 40), FixtureWriter.RAW);
        s.commit();
    }

    private static void t2(StoreWAL s, Recids r) {
        s.update(r.c, null, FixtureWriter.RAW);
        r.d = s.preallocate();
        s.commit();
    }

    private static void t3(StoreWAL s, Recids r) {
        r.e = s.put(FixtureWriter.payload(BASE + 3, 256), FixtureWriter.RAW);
        r.f = s.put(FixtureWriter.payload(BASE + 4, 1_200_000), FixtureWriter.RAW);
        s.commit();
    }

    private static void t4(StoreWAL s, Recids r) {
        s.delete(r.e, FixtureWriter.RAW);
        s.update(r.a, FixtureWriter.payload(BASE + 5, 120), FixtureWriter.RAW);
        s.commit();
    }

    // ---------- shaping transactions (state-preserving by construction) ----------

    /**
     * Gives C content, then takes it away again: two sections, no new recid, and C is null before
     * and after. The SECOND one is row 5's size-preserving {@code T_APPEND} candidate &mdash; a
     * null {@code T_RECORD} and a payload-free {@code T_APPEND} are the same four bytes &mdash;
     * and row 5 reads section index 1, so this pair must be the first two sections of the middle
     * segment and in this order.
     */
    private static void shapeC(StoreWAL s, Recids r) {
        s.update(r.c, FixtureWriter.payload(BASE + 6, 48), FixtureWriter.RAW);
        s.commit();
        s.update(r.c, null, FixtureWriter.RAW);
        s.commit();
    }

    /**
     * Pushes the active segment past {@code segmentBytes} and then commits once more, so that the
     * LAST commit lands alone in a fresh segment: rollover is tested BEFORE a section is appended
     * ({@code StoreWAL.java:1688}), so an oversized section joins the segment it overflows and
     * only its successor rotates. Both halves rewrite A, ending on A's §5.2 content, so the final
     * logical state is untouched.
     */
    private static void shapeRotate(StoreWAL s, Recids r) {
        s.update(r.a, FixtureWriter.payload(BASE + 7, (int) SEGMENT_BYTES), FixtureWriter.RAW);
        s.commit();
        s.update(r.a, FixtureWriter.payload(BASE + 5, 120), FixtureWriter.RAW);
        s.commit();
    }

    // ---------- variants ----------

    private static void run(String variant, File base, Recids r) {
        StoreWAL s = open(base);
        try {
            switch (variant) {
                case "spec": // §5.3 exactly as written today
                    t1(s, r); t2(s, r); t3(s, r);
                    s.checkpoint();
                    t4(s, r);
                    break;
                case "ckpt-after-t2": // the plan's proposed move, on its own
                    t1(s, r); t2(s, r);
                    s.checkpoint();
                    t3(s, r); t4(s, r);
                    break;
                case "ckpt-after-t2-shaped": // the plan's proposal in full, as written there
                    t1(s, r); t2(s, r);
                    s.checkpoint();
                    t3(s, r); t4(s, r);
                    shapeC(s, r);
                    s.update(r.f, FixtureWriter.payload(BASE + 4, 1_200_000), FixtureWriter.RAW);
                    s.commit();
                    break;
                case "shaped-no-C": // the adopted workload MINUS shapeC, to show shapeC matters
                    t1(s, r); t2(s, r);
                    s.checkpoint();
                    t3(s, r);
                    t4(s, r);
                    shapeRotate(s, r);
                    break;
                case "shaped-no-rotate": // the adopted workload MINUS shapeRotate
                    t1(s, r); t2(s, r);
                    s.checkpoint();
                    t3(s, r);
                    shapeC(s, r);
                    t4(s, r);
                    break;
                case "shaped-half-rotate":
                    // Only the half of shapeRotate that CROSSES segmentBytes, without the commit
                    // that lands alone in the segment it opens. This is the case the pair exists
                    // for. It ends with A holding the OVERSIZED payload, so it does not reach
                    // §5.3's final state — it asserts the state it does reach instead, below,
                    // which is every §5.2 row plus the recid set, with A's expectation moved.
                    t1(s, r); t2(s, r);
                    s.checkpoint();
                    t3(s, r);
                    shapeC(s, r);
                    t4(s, r);
                    s.update(r.a, FixtureWriter.payload(BASE + 7, (int) SEGMENT_BYTES),
                            FixtureWriter.RAW);
                    s.commit();
                    FixtureWriter.check(s.cleanerBytesWritten() > 0,
                            "the checkpoint wrote no image: this variant is not a CLEANED shape");
                    // Not §5.3's final state — that is the point — but not unchecked either:
                    // A holds the oversized payload and every other record is where §5.2 leaves
                    // it. The exception this variant is granted is exactly one record wide.
                    assertStateWithA(s, r, variant, BASE + 7, (int) SEGMENT_BYTES);
                    return;   // `finally` still closes
                case "shaped": // the candidate the measurements above argue for
                    t1(s, r); t2(s, r);
                    s.checkpoint();
                    t3(s, r);       // 1.2 MB: oversizes the LOWEST retained segment
                    shapeC(s, r);   // the middle segment's first two sections
                    t4(s, r);
                    shapeRotate(s, r);
                    break;
                default:
                    throw new IllegalArgumentException("unknown variant: " + variant);
            }
            FixtureWriter.check(s.cleanerBytesWritten() > 0,
                    "the checkpoint wrote no image: this variant is not a CLEANED shape at all");
            assertFinalState(s, r, variant);
        } finally {
            s.close();
        }
    }

    /** §5.3's "same final logical state as tail", asserted before close in every variant. */
    static void assertFinalState(StoreWAL s, Recids r, String ctx) {
        assertStateWithA(s, r, ctx, BASE + 5, 120);
    }

    /**
     * §5.2's final logical state with A's content named by the caller.
     *
     * <p>Every adopted workload ends with A holding {@code p(BASE+5, 120)}; the one variant that
     * deliberately stops mid-pair ends with A holding the oversized payload instead. Naming A's
     * expectation rather than skipping the check keeps that variant's exception scoped to the ONE
     * record it is about — otherwise an unrelated state defect in it would be exempt too.
     */
    static void assertStateWithA(StoreWAL s, Recids r, String ctx, int aPayloadId, int aLen) {
        FixtureWriter.assertReaderContract(s, Arrays.asList(
                new FixtureWriter.RecidExpect("A", r.a, "live", aPayloadId, aLen),
                new FixtureWriter.RecidExpect("B", r.b, "live", BASE + 1, 0),
                new FixtureWriter.RecidExpect("C", r.c, "null", BASE + 2, 40),
                new FixtureWriter.RecidExpect("D", r.d, "prealloc", 0, 0),
                new FixtureWriter.RecidExpect("E", r.e, "deleted", BASE + 3, 256),
                new FixtureWriter.RecidExpect("F", r.f, "live", BASE + 4, 1_200_000)), ctx);
    }

    public static void main(String[] args) throws IOException {
        File out = null;
        String variant = null;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--out": out = new File(args[++i]); break;
                case "--variant": variant = args[++i]; break;
                default: throw new IllegalArgumentException("unknown argument: " + args[i]);
            }
        }
        if (out == null || variant == null)
            throw new IllegalArgumentException("usage: --out <dir> --variant <name>");
        if (out.isDirectory()) for (File f : out.listFiles()) Files.delete(f.toPath());
        Files.createDirectories(out.toPath());

        Recids r = new Recids();
        run(variant, new File(out, "x"), r);
        Files.deleteIfExists(new File(out, "x.lock").toPath());
        System.out.println("variant=" + variant + " recids A=" + r.a + " B=" + r.b + " C=" + r.c
                + " D=" + r.d + " E=" + r.e + " F=" + r.f);
        for (File f : out.listFiles()) System.out.println("  " + f.getName() + " " + f.length());
    }
}
