package org.mapdb.stress;

import org.junit.After;
import org.junit.Test;
import org.mapdb.TmpFiles;
import org.mapdb.store.StoreWAL;
import org.mapdb.store.WalTestKit;

import java.io.File;
import java.util.Arrays;
import java.util.PrimitiveIterator;

import static org.mapdb.stress.StressSupport.*;

/**
 * Replay of ONE WAL section whose body exceeds 2 GiB. The format's {@code bodyLen} is an
 * int64 and the reader streams it ({@code WalIn} long offsets, windowed body CRC), but the
 * Java production writer cannot emit such a section — {@code DataOutput2} is byte[]-backed
 * and checkpoint images are chunked ~1 MiB — so this coverage needs a FOREIGN writer:
 * {@link WalTestKit.SectionAppender} streams the section the way a Rust/Zig/Go port with no
 * array cap would, and this test proves the Java reader honors the headroom. Distinct from
 * {@link WalHugeReopenIT}, which covers over-2-GiB <em>segments</em> made of small sections.
 *
 * <p>The huge commit both overwrites a seeded record and creates ~2100 fresh recids, and the
 * store must remain writable afterwards: a post-replay commit rolls to a new segment (the
 * foreign segment is far past the default rollover threshold) and must survive a third open.
 *
 * Manual run (2 GiB heap pins that replay of one huge section is streaming, not buffered;
 * direct buffers keep the ~2.3 GiB volume off-heap):
 *   java -ea -Xmx2g -XX:MaxDirectMemorySize=8g \
 *     -cp target/test-classes:target/classes org.junit.runner.JUnitCore \
 *     org.mapdb.stress.WalHugeSectionIT
 * (or: mvn -P integration-tests verify -Dit.test=WalHugeSectionIT)
 */
public class WalHugeSectionIT {

    /** Over the plain-record capacity cap, so the WAL encodes capacity 0 (linked layout). */
    static final int RECSIZE = 1 << 20;
    /** 2100 x (1 MiB + entry framing) puts bodyLen past Integer.MAX_VALUE with margin. */
    static final int RECS = 2100;

    private File walFile;

    @After public void cleanup() {
        TmpFiles.delete(walFile);
    }

    @Test public void replay_single_section_body_over_2gib() throws Exception {
        walFile = TmpFiles.tempFile("stress-wal-hugesection", ".wal");
        walFile.delete();

        // Seed a normal store: two small committed records, cleanly closed.
        long replacedRecid, keptRecid;
        StoreWAL store = new StoreWAL(walFile, true);
        try {
            store.setMinLogBytes(0); // cleaning off: the seeded section must survive as-is
            replacedRecid = store.preallocate();
            keptRecid = store.preallocate();
            store.update(replacedRecid, payload(replacedRecid, 1), RAW);
            store.update(keptRecid, payload(keptRecid, 1), RAW);
            store.commit();
        } finally {
            store.close();
        }
        File[] segs = WalTestKit.segments(walFile);
        if (segs.length != 1)
            throw new AssertionError("expected 1 seeded segment, found " + segs.length);

        // Foreign writer: one streamed 'S' section at the next consecutive LSN, whose body
        // overwrites one seeded record and creates RECS fresh recids.
        long firstForeign = keptRecid + 1;
        long bodyLen;
        long t0 = System.nanoTime();
        try (WalTestKit.SectionAppender app =
                     WalTestKit.SectionAppender.begin(segs[0], 'S', WalTestKit.lastLsn(segs[0]) + 1)) {
            app.record(replacedRecid, 0, payload(replacedRecid, 2, RECSIZE));
            for (int i = 0; i < RECS; i++) {
                long recid = firstForeign + i;
                app.record(recid, 0, payload(recid, 1, RECSIZE));
            }
            bodyLen = app.finish();
        }
        if (bodyLen <= Integer.MAX_VALUE)
            throw new AssertionError("section body not past 2 GiB: " + bodyLen);
        System.out.printf("[STRESS] huge section appended: bodyLen=%.2f GiB, %,d records in %.1fs%n",
                bodyLen / 1024.0 / 1024 / 1024, RECS + 1, (System.nanoTime() - t0) / 1e9);

        // Replay it and verify every record — the seeded survivor, the overwrite, all foreign.
        long t1 = System.nanoTime();
        StoreWAL reopened = new StoreWAL(walFile, true);
        double replaySec = (System.nanoTime() - t1) / 1e9;
        long extraRecid;
        try {
            if (!Arrays.equals(payload(keptRecid, 1), reopened.get(keptRecid, RAW)))
                throw new AssertionError("seeded record lost, recid=" + keptRecid);
            if (!Arrays.equals(payload(replacedRecid, 2, RECSIZE), reopened.get(replacedRecid, RAW)))
                throw new AssertionError("overwrite from the huge section not applied, recid="
                        + replacedRecid);
            for (int i = 0; i < RECS; i++) {
                long recid = firstForeign + i;
                if (!Arrays.equals(payload(recid, 1, RECSIZE), reopened.get(recid, RAW)))
                    throw new AssertionError("bad payload after replay, recid=" + recid);
            }
            PrimitiveIterator.OfLong it = reopened.getAllRecids();
            int count = 0;
            while (it.hasNext()) { it.nextLong(); count++; }
            if (count != 2 + RECS)
                throw new AssertionError("expected " + (2 + RECS) + " recids, found " + count);
            reopened.verify();
            System.out.printf("[STRESS] huge section replayed: %.2fs (%.1f MB/s), %,d records verified%n",
                    replaySec, bodyLen / 1024.0 / 1024 / replaySec, 2 + RECS);

            // The store must keep working past a foreign section: the next commit rolls over.
            long expectedExtraRecid = firstForeign + RECS;
            extraRecid = reopened.put(payload(expectedExtraRecid, 7), RAW);
            if (extraRecid != expectedExtraRecid)
                throw new AssertionError("post-replay allocation collided with foreign recids: expected "
                        + expectedExtraRecid + ", got " + extraRecid);
            reopened.commit();
            File[] rolled = WalTestKit.segments(walFile);
            if (rolled.length != 2)
                throw new AssertionError("huge active segment did not roll over: expected 2 segments, found "
                        + rolled.length);
        } finally {
            reopened.close();
        }

        StoreWAL again = new StoreWAL(walFile, true);
        try {
            if (!Arrays.equals(payload(firstForeign + RECS, 7), again.get(extraRecid, RAW)))
                throw new AssertionError("post-replay commit lost, recid=" + extraRecid);
            if (!Arrays.equals(payload(replacedRecid, 2, RECSIZE), again.get(replacedRecid, RAW)))
                throw new AssertionError("huge-section record lost after rollover, recid="
                        + replacedRecid);
            PrimitiveIterator.OfLong it = again.getAllRecids();
            int count = 0;
            while (it.hasNext()) { it.nextLong(); count++; }
            if (count != 3 + RECS)
                throw new AssertionError("expected " + (3 + RECS)
                        + " recids after post-replay commit, found " + count);
            again.verify();
        } finally {
            again.close();
        }
    }
}
