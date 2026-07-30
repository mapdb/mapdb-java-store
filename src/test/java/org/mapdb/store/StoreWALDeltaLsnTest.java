package org.mapdb.store;

import org.junit.After;
import org.junit.Test;
import org.mapdb.DBException;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Transaction-scoped LSNs delivered through {@link StoreDelta.DeltaEncoder}.
 *
 * <p>The contract under test: the STORE supplies the LSN value and the FORMAT writes it
 * while encoding — no opaque byte is ever patched by the store (P1). One LSN per
 * TRANSACTION, not per append, because per-append LSNs are impossible here: maintenance can
 * publish a section between a reservation and its commit, and one collection-level append
 * carries a whole batch of frames rather than one.
 */
public class StoreWALDeltaLsnTest {

    private static final int SEG_HDR = WalTestKit.SEG_HDR;
    private static final int SEC_HDR = WalTestKit.SEC_HDR;

    private final List<File> files = new ArrayList<>();

    private File newFile(String tag) {
        try {
            File f = Files.createTempFile("mapdb5-wal-lsn-" + tag, ".wal").toFile();
            f.delete();
            files.add(f);
            return f;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @After public void cleanup() {
        for (File f : files) WalTestKit.deleteStore(f);
        files.clear();
    }

    /** LSNs of every section of the log, in (segment, offset) order — which is LSN order. */
    private static List<Long> sectionLsns(File f) throws IOException {
        List<Long> out = new ArrayList<>();
        for (File seg : WalTestKit.segments(f)) {
            byte[] all = WalTestKit.read(seg);
            ByteBuffer bb = ByteBuffer.wrap(all).order(ByteOrder.BIG_ENDIAN);
            int pos = SEG_HDR;
            while (pos + SEC_HDR <= all.length) {
                long lsn = bb.getLong(pos + 1);
                long bodyLen = bb.getLong(pos + 9);
                if (bodyLen < 0 || pos + SEC_HDR + bodyLen > all.length) break;
                out.add(lsn);
                pos = (int) (pos + SEC_HDR + bodyLen);
            }
        }
        return out;
    }

    /** An encoder that records the LSN it was handed and writes it verbatim. */
    private static StoreDelta.DeltaEncoder recording(List<Long> sink, int payloadBytes) {
        return (out, lsn) -> {
            sink.add(lsn);
            out.writeLong(lsn);
            for (int i = 8; i < payloadBytes; i++) out.writeByte(0);
        };
    }

    private long newAppendable(StoreWAL s, int headroom) {
        long r = s.put(new byte[0], Fixtures.RAW);
        s.updateWithHeadroom(r, new byte[0], Fixtures.RAW, headroom);
        s.commit();
        return r;
    }

    @Test public void store_supplies_a_real_lsn_and_it_matches_the_commit_section() throws Exception {
        File f = newFile("match");
        StoreWAL s = new StoreWAL(f, false, true);
        List<Long> handed = new ArrayList<>();
        try {
            long r = newAppendable(s, 256);
            List<Long> before = sectionLsns(f);

            assertNotEquals(StoreDelta.REFUSED, s.append(r, recording(handed, 16)));
            assertEquals("exactly one encode", 1, handed.size());
            assertTrue("a real LSN, not the R1 placeholder 0", handed.get(0) > 0);

            s.commit();
            List<Long> after = sectionLsns(f);
            assertEquals("one new section", before.size() + 1, after.size());
            assertEquals("frame LSN == the LSN of the section that made it durable",
                    handed.get(0), after.get(after.size() - 1));
        } finally {
            s.close();
        }
    }

    @Test public void all_appends_in_one_transaction_share_one_lsn_and_it_advances_per_commit()
            throws Exception {
        File f = newFile("share");
        StoreWAL s = new StoreWAL(f, false, true);
        List<Long> handed = new ArrayList<>();
        try {
            long r = newAppendable(s, 512);

            s.append(r, recording(handed, 16));
            s.append(r, recording(handed, 16));
            s.append(r, recording(handed, 16));
            assertEquals(3, handed.size());
            assertEquals("one LSN per TRANSACTION, not per append",
                    1, handed.stream().distinct().count());
            long first = handed.get(0);
            s.commit();

            handed.clear();
            s.append(r, recording(handed, 16));
            s.commit();
            assertEquals("the next transaction gets a strictly higher LSN",
                    first + 1, (long) handed.get(0));
        } finally {
            s.close();
        }
    }

    /**
     * The reservation window is the whole reason LSNs are transaction-scoped: a checkpoint
     * consumes the next LSN, so letting one run mid-transaction would strand frames already
     * encoded with the reserved value. Pinned by {@code StoreWALCheckpointTest}, which does
     * stage → checkpoint → commit with the RAW append path — that path takes no reservation,
     * so it must keep working.
     */
    @Test public void checkpoint_is_refused_while_a_delta_transaction_holds_a_reservation() {
        File f = newFile("reserved");
        StoreWAL s = new StoreWAL(f, false, true);
        try {
            long r = newAppendable(s, 256);
            s.append(r, recording(new ArrayList<>(), 16));
            try {
                s.checkpoint();
                fail("expected checkpoint to be refused while an LSN reservation is held");
            } catch (DBException e) {
                assertTrue(e.getMessage(), e.getMessage().contains("LSN"));
            }
            s.commit();
            s.checkpoint();          // released by commit -> now fine
            s.verify();
        } finally {
            s.close();
        }
    }

    @Test public void rollback_releases_the_reservation_without_burning_the_lsn() throws Exception {
        File f = newFile("rollback");
        StoreWAL s = new StoreWAL(f, false, true);
        List<Long> handed = new ArrayList<>();
        try {
            long r = newAppendable(s, 256);

            s.append(r, recording(handed, 16));
            long reserved = handed.get(0);
            s.rollback();
            s.checkpoint();          // must be permitted again after rollback

            handed.clear();
            s.append(r, recording(handed, 16));
            assertTrue("the rolled-back LSN is not stranded below the new one",
                    handed.get(0) >= reserved);
            s.commit();
            s.verify();
        } finally {
            s.close();
        }
    }

    /** The raw byte[] path takes no reservation, so it must not disturb checkpointing. */
    @Test public void raw_append_path_takes_no_reservation() {
        File f = newFile("raw");
        StoreWAL s = new StoreWAL(f, false, true);
        try {
            long r = newAppendable(s, 256);
            s.append(r, new byte[]{1, 2, 3}, 0, 3);
            s.checkpoint();          // the StoreWALCheckpointTest shape: must still work
            s.commit();
            s.verify();
        } finally {
            s.close();
        }
    }

    /** An encoder that throws must leave the transaction exactly as it was. */
    @Test public void throwing_encoder_stages_nothing_and_takes_no_reservation() {
        File f = newFile("throw");
        StoreWAL s = new StoreWAL(f, false, true);
        try {
            long r = newAppendable(s, 256);
            try {
                s.append(r, (out, lsn) -> { throw new IllegalStateException("boom"); });
                fail("expected the encoder's exception to propagate");
            } catch (IllegalStateException expected) {
                assertEquals("boom", expected.getMessage());
            }
            s.checkpoint();          // no reservation was taken
            s.commit();
            s.verify();
        } finally {
            s.close();
        }
    }
}
