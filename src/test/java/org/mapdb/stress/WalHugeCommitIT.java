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
 * The Java WRITER emits one commit section whose body exceeds 2 GiB. Companion to
 * {@link WalHugeSectionIT}, which replays a foreign-crafted huge section: this test proves the
 * production commit path itself — {@code StoreWAL.SectionBody}'s two-pass streaming writer —
 * can stage over 2 GiB in ONE transaction and land it as a single section, where the old
 * accumulating writer died in {@code DataOutput2}'s doubling {@code byte[]}.
 *
 * <p>Heap bound is the pin: the staged payload arrays and their merged copies are ~4.1 GiB of
 * unavoidable transaction state, so a 6 GiB heap (4.1 + GC headroom) proves commit adds no
 * whole-body third copy — and the old writer could not even represent one: a 2 GiB body's
 * doubling spike needs an array past {@code Integer.MAX_VALUE} elements.
 *
 * Manual run (direct buffers keep the store volume off-heap):
 *   java -ea -Xmx6g -XX:MaxDirectMemorySize=8g \
 *     -cp target/test-classes:target/classes org.junit.runner.JUnitCore \
 *     org.mapdb.stress.WalHugeCommitIT
 * (or: mvn -P integration-tests verify -Dit.test=WalHugeCommitIT)
 */
public class WalHugeCommitIT {

    /** Over the plain-record capacity cap, so the WAL encodes capacity 0 (linked layout). */
    static final int RECSIZE = 1 << 20;
    /** 2100 x (1 MiB + entry framing) puts bodyLen past Integer.MAX_VALUE with margin. */
    static final int RECS = 2100;

    private File walFile;

    @After public void cleanup() {
        TmpFiles.delete(walFile);
    }

    @Test public void commit_writes_single_section_body_over_2gib() throws Exception {
        walFile = TmpFiles.tempFile("stress-wal-hugecommit", ".wal");
        walFile.delete();

        // ONE transaction: 2100 x 1 MiB puts, committed together.
        long firstRecid;
        long t0 = System.nanoTime();
        StoreWAL store = new StoreWAL(walFile, true);
        try {
            store.setMinLogBytes(0); // cleaning off: the huge section's segment must stay put
            firstRecid = store.preallocate();
            store.update(firstRecid, payload(firstRecid, 1, RECSIZE), RAW);
            for (int i = 1; i < RECS; i++) {
                long recid = store.preallocate();
                if (recid != firstRecid + i)
                    throw new AssertionError("non-contiguous allocation: " + recid);
                store.update(recid, payload(recid, 1, RECSIZE), RAW);
            }
            store.commit();
        } finally {
            store.close();
        }

        // The commit must be ONE section with an over-2-GiB body, on disk, headers walkable
        // without materializing the segment.
        long hugeBody = 0;
        int commitSections = 0;
        for (File seg : WalTestKit.segments(walFile))
            for (long[] h : WalTestKit.sectionHeaders(seg))
                if (h[0] == 'S') {
                    commitSections++;
                    if (h[2] > hugeBody) hugeBody = h[2];
                }
        if (commitSections != 1)
            throw new AssertionError("one transaction emitted " + commitSections
                    + " commit sections instead of exactly one");
        if (hugeBody <= Integer.MAX_VALUE)
            throw new AssertionError("no commit section past 2 GiB, largest body: " + hugeBody);
        System.out.printf("[STRESS] huge commit written: bodyLen=%.2f GiB, %,d records in %.1fs%n",
                hugeBody / 1024.0 / 1024 / 1024, RECS, (System.nanoTime() - t0) / 1e9);

        // Replay it and byte-verify every record.
        long t1 = System.nanoTime();
        StoreWAL reopened = new StoreWAL(walFile, true);
        double replaySec = (System.nanoTime() - t1) / 1e9;
        long extraRecid;
        try {
            for (int i = 0; i < RECS; i++) {
                long recid = firstRecid + i;
                if (!Arrays.equals(payload(recid, 1, RECSIZE), reopened.get(recid, RAW)))
                    throw new AssertionError("bad payload after replay, recid=" + recid);
            }
            PrimitiveIterator.OfLong it = reopened.getAllRecids();
            int count = 0;
            while (it.hasNext()) { it.nextLong(); count++; }
            if (count != RECS)
                throw new AssertionError("expected " + RECS + " recids, found " + count);
            reopened.verify();
            System.out.printf("[STRESS] huge commit replayed: %.2fs (%.1f MB/s), %,d records verified%n",
                    replaySec, hugeBody / 1024.0 / 1024 / replaySec, RECS);

            // The store must keep working: the next commit rolls past the huge segment.
            int segsBefore = WalTestKit.segments(walFile).length;
            extraRecid = reopened.put(payload(firstRecid + RECS, 7), RAW);
            reopened.commit();
            int segsAfter = WalTestKit.segments(walFile).length;
            if (segsAfter != segsBefore + 1)
                throw new AssertionError("huge segment did not roll over: " + segsBefore
                        + " -> " + segsAfter + " segments");
        } finally {
            reopened.close();
        }

        StoreWAL again = new StoreWAL(walFile, true);
        try {
            if (!Arrays.equals(payload(firstRecid + RECS, 7), again.get(extraRecid, RAW)))
                throw new AssertionError("post-replay commit lost, recid=" + extraRecid);
            if (!Arrays.equals(payload(firstRecid, 1, RECSIZE), again.get(firstRecid, RAW)))
                throw new AssertionError("huge-commit record lost after rollover, recid=" + firstRecid);
            PrimitiveIterator.OfLong it = again.getAllRecids();
            int count = 0;
            while (it.hasNext()) { it.nextLong(); count++; }
            if (count != RECS + 1)
                throw new AssertionError("expected " + (RECS + 1)
                        + " recids after post-replay commit, found " + count);
            again.verify();
        } finally {
            again.close();
        }
    }
}
