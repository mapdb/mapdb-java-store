package org.mapdb.xfixtures;

import org.mapdb.DBException;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;
import org.mapdb.ser.Serializer;
import org.mapdb.store.Store;
import org.mapdb.store.StoreDirect;
import org.mapdb.store.StoreWAL;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.TreeSet;

/**
 * Cross-port conformance fixture generator (Stage 1 "D" StoreDirect workload; Stage 2 adds the
 * {@code reject-wal-java-v3.walseg} segment fixture).
 *
 * <p>The fixtures written here pin the CURRENT state of an UNSTABLE format so that silent
 * divergence between the store engines is detected. Cross-engine openability today is an
 * implementation fact, not a supported feature; any format change regenerates the fixtures
 * as part of that change.
 *
 * <p>Runs the deterministic D workload from the Stage-1 implementation contract against a
 * file-backed {@link StoreDirect} through the PUBLIC Store API only, self-checks the result
 * (reopen + full reader contract, plus a raw-bytes decode of the E&rarr;G extent-reuse
 * invariant), and writes {@code direct-v1-java.db}, {@code reject-wal-java-v3.walseg} (see
 * {@link #writeWalSegFixture}) and a {@code fragment.tsv} carrying these fixtures' manifest
 * rows. Compression and manifest assembly are the sync script's job, not this generator's.
 *
 * <p>Run from the repo root (no exec plugin in the POM; junit is deliberately NOT on this
 * classpath, so this class uses no junit assertions):
 * <pre>
 *   mvn -q test-compile
 *   java -ea -cp target/test-classes:target/classes org.mapdb.xfixtures.FixtureWriter --out &lt;dir&gt; [--force]
 * </pre>
 * Refuses a nonempty output directory unless {@code --force} is given.
 */
public final class FixtureWriter {

    private FixtureWriter() {}

    // ---------- fixture identity (shared with XFixtureConformanceTest) ----------

    static final String FIXTURE_ID = "direct-v1-java";
    static final String DB_FILE = "direct-v1-java.db";

    // Stage 2: a real Java v3 WAL segment, published so the PORTS can pin their explicit version
    // rejection (matching magic + version 3 fails their v1 check directly; the framed-MDB guard is
    // not reached). Java itself has no expect row for it in the synced manifest — its reject-wal
    // rows use port v1 files.
    static final String WALSEG_FIXTURE_ID = "reject-wal-java-v3";
    static final String WALSEG_FILE = "reject-wal-java-v3.walseg";
    /** Scratch directory (under {@code --out}) the throwaway StoreWAL namespace lives in. */
    static final String WALSEG_SCRATCH = "walseg-scratch";

    /** Exactly the first payload length that forces the linked-record path (MAX_CAPACITY - 4 + 1). */
    static final int F_LEN = 1_048_525;
    static final int CHURN_COUNT = 200;
    static final int CHURN_PAYLOAD_BASE = 1000;
    /** E and every churn record share this length, hence one 16-byte capacity class. */
    static final int E_LEN = 256;

    // ---------- shared workload primitives ----------

    /**
     * The contract's payload function: {@code payload(payloadId, len)[i] = (i*131 + payloadId) & 0xff}.
     * Recomputed per use; the &gt;1 MiB F payload is never cached.
     */
    static byte[] payload(int payloadId, int len) {
        byte[] b = new byte[len];
        for (int i = 0; i < len; i++) b[i] = (byte) (i * 131 + payloadId);
        return b;
    }

    /**
     * Size-driven raw byte[] serializer: serialized content == value, byte for byte. The engine's
     * {@code Serializers.BYTE_ARRAY} is length-prefixed and would change the on-volume bytes, so
     * the cross-port contract mandates this no-framing shape (same as the store TCK's RAW).
     */
    static final Serializer<byte[]> RAW = new Serializer<>() {
        @Override public void serialize(DataOutput2 out, byte[] value) { out.write(value); }

        @Override public byte[] deserialize(DataInput2 in, int size) {
            byte[] b = new byte[size];
            in.readFully(b);
            return b;
        }

        @Override public boolean equals(byte[] a, byte[] b) { return Arrays.equals(a, b); }

        @Override public int compare(byte[] a, byte[] b) { return Arrays.compare(a, b); }
    };

    /** One manifest {@code recid} row: the expected post-open state of a single recid. */
    static final class RecidExpect {
        final String label;
        final long recid;
        final String state; // live | null | prealloc | deleted
        final int payloadId;
        final int len;

        RecidExpect(String label, long recid, String state, int payloadId, int len) {
            this.label = label;
            this.recid = recid;
            this.state = state;
            this.payloadId = payloadId;
            this.len = len;
        }
    }

    /** Junit-free check so the same helpers run under plain {@code java} and under the test runner. */
    static void check(boolean ok, String msg) {
        if (!ok) throw new AssertionError(msg);
    }

    /**
     * The full accept-cell reader contract over an open store: engine {@code verify()}, then a
     * per-recid assertion for every manifest row, then exact {@code getAllRecids} set equality
     * (live + explicit-null recids listed; prealloc and deleted excluded). Shared between the
     * generator self-check and {@code XFixtureConformanceTest} so both sides pin one contract.
     */
    static void assertReaderContract(Store s, List<RecidExpect> expects, String ctx) {
        s.verify();
        Set<Long> wantListed = new TreeSet<>();
        for (RecidExpect e : expects) {
            String where = ctx + ": " + e.label + " recid=" + e.recid + " (" + e.state + ")";
            switch (e.state) {
                case "live": {
                    byte[] got = s.get(e.recid, RAW);
                    check(got != null, where + ": expected a live record, got null");
                    check(Arrays.equals(got, payload(e.payloadId, e.len)),
                            where + ": content mismatch, expected payload(" + e.payloadId + ", " + e.len
                                    + "), got " + got.length + " bytes");
                    wantListed.add(e.recid);
                    break;
                }
                case "null": // explicit null content IS a record: reads null yet stays listed
                    check(s.get(e.recid, RAW) == null, where + ": expected null content");
                    wantListed.add(e.recid);
                    break;
                case "prealloc": // reads null AND is excluded from getAllRecids
                    check(s.get(e.recid, RAW) == null, where + ": expected null content");
                    break;
                case "deleted": {
                    boolean voided = false;
                    try {
                        s.get(e.recid, RAW);
                    } catch (DBException.GetVoid expected) {
                        voided = true;
                    }
                    check(voided, where + ": expected DBException.GetVoid");
                    break;
                }
                default:
                    throw new AssertionError(where + ": unknown manifest state");
            }
        }
        Set<Long> listed = new TreeSet<>();
        PrimitiveIterator.OfLong it = s.getAllRecids();
        while (it.hasNext()) listed.add(it.nextLong());
        check(listed.equals(wantListed),
                ctx + ": getAllRecids mismatch, expected " + wantListed + ", got " + listed);
    }

    static String sha256Hex(byte[] bytes) {
        try {
            byte[] d = MessageDigest.getInstance("SHA-256").digest(bytes);
            StringBuilder sb = new StringBuilder(64);
            for (byte b : d) sb.append(Character.forDigit((b >>> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("JRE without SHA-256", e);
        }
    }

    // ---------- local index-slot decoder (generator self-check ONLY) ----------
    //
    // Conformance consumers stay on the public API; the generator alone decodes the file bytes to
    // prove the E->G extent reuse the D workload exists to pin. Bit ops copied from the engine:
    //  - slot address: StoreDirect.ZERO_SLOTS_START (= 524352) + (recid-1)*8 for recids on the
    //    zero index page (recid 1..65528) — StoreDirect.recidToOffset;
    //  - parity strip: Parity.p1get — a stored slot has an odd total bit count, payload = val & ~1;
    //  - offset field: IndexVal.MOFFSET = 0x0000FFFFFFFFFFF0 (44-bit 16-aligned offset);
    //  - capacity units: IndexVal.capUnits = val >>> 48 (0xFFFE = deleted tombstone).

    private static final long ZERO_SLOTS_START = 524_352;
    private static final long MOFFSET = 0x0000FFFFFFFFFFF0L;
    private static final int CAP_DELETED = 0xFFFE;

    /** Parity1-stripped index slot for {@code recid}, read big-endian straight from the file. */
    private static long readIndexSlot(File db, long recid) throws IOException {
        check(recid >= 1 && recid <= 65_528, "recid off the zero index page, decoder too small: " + recid);
        try (RandomAccessFile raf = new RandomAccessFile(db, "r")) {
            raf.seek(ZERO_SLOTS_START + (recid - 1) * 8);
            long raw = raf.readLong();
            check((Long.bitCount(raw) & 1) == 1, "parity1 broken in index slot of recid " + recid
                    + ": 0x" + Long.toHexString(raw));
            return raw & ~1L;
        }
    }

    private static long slotOffset(long iv) { return iv & MOFFSET; }

    private static int slotCapUnits(long iv) { return (int) (iv >>> 48); }

    /** Header fileTail at offset 40, parity16 (Parity.p16get: low 16 bits are the checksum). */
    private static long readFileTail(byte[] file) {
        long v = 0;
        for (int i = 0; i < 8; i++) v = (v << 8) | (file[40 + i] & 0xFFL);
        long x = v & ~0xFFFFL;
        check((v & 0xFFFF) == ((Long.bitCount(x) + 1) & 0xFFFF), "parity16 broken in fileTail: 0x" + Long.toHexString(v));
        return x;
    }

    // ---------- the D workload ----------

    /** Recids the workload produced, plus E's pre-delete data offset for the reuse assertion. */
    private static final class Generated {
        long a, b, c, d, e, f, g;
        long churnFrom, churnTo;
        long eOffsetBeforeDelete;

        List<RecidExpect> expects() {
            List<RecidExpect> out = new ArrayList<>();
            out.add(new RecidExpect("A", a, "live", 1, 100));
            out.add(new RecidExpect("B", b, "live", 2, 0));
            out.add(new RecidExpect("C", c, "null", 3, 40));
            out.add(new RecidExpect("D", d, "prealloc", 0, 0));
            out.add(new RecidExpect("F", f, "live", 6, F_LEN));
            out.add(new RecidExpect("G", g, "live", 7, E_LEN));
            out.add(new RecidExpect("E", e, "deleted", 5, E_LEN));
            for (long r = churnFrom; r <= churnTo; r++)
                out.add(new RecidExpect("churn", r, "deleted", (int) (CHURN_PAYLOAD_BASE + r - churnFrom), E_LEN));
            return out;
        }
    }

    /** Contract steps 1..11, in EXACT order (free stacks are LIFO; the order is the fixture). */
    private static Generated writeWorkload(File db) throws IOException {
        Generated r = new Generated();
        StoreDirect s = new StoreDirect(db);
        try {
            r.a = s.put(payload(1, 100), RAW);           // 1. A: small live record
            r.b = s.put(payload(2, 0), RAW);             // 2. B: zero-length live record
            r.c = s.put(payload(3, 40), RAW);            // 3. C: put then update to explicit null
            s.update(r.c, null, RAW);
            r.d = s.preallocate();                       // 4. D: preallocated, never written
            r.f = s.put(payload(6, F_LEN), RAW);         // 5. F: exactly the first-linked boundary
            r.g = s.preallocate();                       // 6. G: filled in step 10
            r.e = s.put(payload(5, E_LEN), RAW);         // 7. E
            long[] churn = new long[CHURN_COUNT];        // 8. churn, same capacity class as E
            for (int j = 0; j < CHURN_COUNT; j++)
                churn[j] = s.put(payload(CHURN_PAYLOAD_BASE + j, E_LEN), RAW);
            for (int j = 1; j < CHURN_COUNT; j++)        // manifest uses recidrange: must be contiguous
                check(churn[j] == churn[0] + j, "churn recids not contiguous at j=" + j
                        + ": " + churn[j] + " != " + (churn[0] + j));
            r.churnFrom = churn[0];
            r.churnTo = churn[CHURN_COUNT - 1];
            // Extra commit between contract steps 8 and 9, explicitly allowed by the contract
            // (commit allocates nothing): it flushes the index slots so E's pre-delete data
            // offset can be captured from the file bytes for the reuse assertion below.
            s.commit();
            r.eOffsetBeforeDelete = slotOffset(readIndexSlot(db, r.e));
            check(r.eOffsetBeforeDelete != 0, "E must have a data extent before deletion");
            for (long c : churn) s.delete(c, RAW);       // 9. delete churn in creation order,
            s.delete(r.e, RAW);                          //    then E LAST (its extent tops the stack)
            s.update(r.g, payload(7, E_LEN), RAW);       // 10. G must reuse E's freed extent
            s.commit();                                  // 11. commit + close, nothing further
        } finally {
            s.close();
        }
        return r;
    }

    // ---------- self-check before publishing ----------

    private static void selfCheck(File db, Generated r) throws IOException {
        byte[] published = Files.readAllBytes(db.toPath());
        // canonical tail: close truncated physical excess, so length must equal header fileTail
        long fileTail = readFileTail(published);
        check(fileTail == published.length,
                "physical length " + published.length + " != fileTail " + fileTail);
        // E -> G extent reuse, decoded from the raw file bytes
        long gOffset = slotOffset(readIndexSlot(db, r.g));
        check(gOffset == r.eOffsetBeforeDelete, "G did not reuse E's extent: G offset " + gOffset
                + " != E pre-delete offset " + r.eOffsetBeforeDelete);
        check(slotCapUnits(readIndexSlot(db, r.e)) == CAP_DELETED,
                "E's index slot is not a deleted tombstone");
        // reopen: engine verify() + the full reader contract every conformance cell will run
        StoreDirect s = new StoreDirect(db);
        try {
            assertReaderContract(s, r.expects(), FIXTURE_ID + " self-check");
        } finally {
            s.close();
        }
        // a read-only reopen/close cycle must leave the published bytes untouched
        check(Arrays.equals(published, Files.readAllBytes(db.toPath())),
                "self-check reopen changed the published file bytes");
    }

    // ---------- Stage 2: the reject-wal-java-v3 segment ----------

    /**
     * Emits {@code reject-wal-java-v3.walseg}: one committed transaction ({@code put(payload(51,
     * 100))}) through a throwaway {@link StoreWAL} at a scratch base under {@code --out}, closed
     * cleanly. The single resulting segment file {@code <base>.wal.<16hex>} is MOVED to the
     * published name and every other scratch artifact is removed, so {@code --out} ends up holding
     * exactly the published files plus {@code fragment.tsv}. Aborts if the close left more than
     * one segment — the fixture must be a single segment.
     *
     * <p>Deterministic by construction: the segment header is magic|version|flags=0|seq|firstLsn|
     * CRC-32 and the section bytes carry only LSNs, recids and the payload function — no
     * timestamps, no randomness (verified by the sync script's two-run byte compare).
     */
    private static void writeWalSegFixture(File out) throws IOException {
        File scratch = new File(out, WALSEG_SCRATCH);
        check(!scratch.exists(), "stale scratch directory in the way: " + scratch);
        Files.createDirectories(scratch.toPath());
        File base = new File(scratch, "x");
        StoreWAL s = new StoreWAL(base);
        try {
            s.put(payload(51, 100), RAW);
            s.commit();
        } finally {
            s.close();
        }
        File[] segs = scratch.listFiles((d, n) -> n.matches("x\\.wal\\.[0-9a-f]{16}"));
        check(segs != null && segs.length == 1, "expected exactly ONE WAL segment in " + scratch
                + ", found " + (segs == null ? "none" : Arrays.toString(segs)));
        File dest = new File(out, WALSEG_FILE);
        Files.deleteIfExists(dest.toPath());
        Files.move(segs[0].toPath(), dest.toPath());
        // everything else in the scratch namespace (the .lock sidecar) is an open-time artifact
        File[] rest = scratch.listFiles();
        check(rest != null, "scratch directory vanished: " + scratch);
        for (File f : rest) {
            check(f.isFile(), "unexpected non-file scratch artifact: " + f);
            Files.delete(f.toPath());
        }
        Files.delete(scratch.toPath());
    }

    // ---------- fragment.tsv ----------

    private static void writeFragment(File fragment, File db, File walseg, Generated r) throws IOException {
        byte[] raw = Files.readAllBytes(db.toPath());
        byte[] walsegRaw = Files.readAllBytes(walseg.toPath());
        StringBuilder sb = new StringBuilder();
        sb.append("# xfixtures fragment written by org.mapdb.xfixtures.FixtureWriter.\n");
        sb.append("# The sync script merges fragments, appends the gzSha256 column to file rows\n");
        sb.append("# and adds expect/edit rows plus the derived reject fixtures.\n");
        sb.append("fixture\t").append(FIXTURE_ID).append("\tdirect\tjava\t").append(gitHeadOrUnknown()).append('\n');
        sb.append("file\t").append(FIXTURE_ID).append('\t').append(DB_FILE).append('\t')
                .append(raw.length).append('\t').append(sha256Hex(raw)).append('\n');
        appendRecid(sb, "A", r.a, "live", 1, 100);
        appendRecid(sb, "B", r.b, "live", 2, 0);
        appendRecid(sb, "C", r.c, "null", 3, 40);
        appendRecid(sb, "D", r.d, "prealloc", 0, 0);
        appendRecid(sb, "F", r.f, "live", 6, F_LEN);
        appendRecid(sb, "G", r.g, "live", 7, E_LEN);
        appendRecid(sb, "E", r.e, "deleted", 5, E_LEN);
        sb.append("recidrange\t").append(FIXTURE_ID).append("\tchurn\t").append(r.churnFrom).append('\t')
                .append(r.churnTo).append("\tdeleted\t").append(CHURN_PAYLOAD_BASE).append('\t').append(E_LEN).append('\n');
        // Stage 2: the java v3 segment gets its file row only — reject fixtures carry no recid rows
        sb.append("file\t").append(WALSEG_FIXTURE_ID).append('\t').append(WALSEG_FILE).append('\t')
                .append(walsegRaw.length).append('\t').append(sha256Hex(walsegRaw)).append('\n');
        Files.write(fragment.toPath(), sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    private static void appendRecid(StringBuilder sb, String label, long recid, String state, int payloadId, int len) {
        sb.append("recid\t").append(FIXTURE_ID).append('\t').append(label).append('\t').append(recid)
                .append('\t').append(state).append('\t').append(payloadId).append('\t').append(len).append('\n');
    }

    /** {@code git rev-parse HEAD} in the cwd (documented run is from the repo root); "unknown" offline. */
    static String gitHeadOrUnknown() {
        try {
            Process p = new ProcessBuilder("git", "rev-parse", "HEAD").redirectErrorStream(true).start();
            String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
            if (p.waitFor() == 0 && out.matches("[0-9a-f]{40}")) return out;
        } catch (IOException | InterruptedException ignore) {
            // fall through: the commit column is informational, not load-bearing
        }
        return "unknown";
    }

    // ---------- CLI ----------

    public static void main(String[] args) throws IOException {
        File out = null;
        boolean force = false;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--out":
                    if (++i == args.length) usage("--out needs a directory argument");
                    out = new File(args[i]);
                    break;
                case "--force":
                    force = true;
                    break;
                default:
                    usage("unknown argument: " + args[i]);
            }
        }
        if (out == null) usage("--out is required");
        if (out.exists() && !out.isDirectory()) usage("--out is not a directory: " + out);
        String[] existing = out.list();
        if (existing != null && existing.length > 0 && !force)
            usage("output directory not empty (use --force to overwrite): " + out);
        Files.createDirectories(out.toPath());

        File db = new File(out, DB_FILE);
        Files.deleteIfExists(db.toPath());
        Files.deleteIfExists(new File(out, DB_FILE + ".lock").toPath());
        Files.deleteIfExists(new File(out, "fragment.tsv").toPath());
        File walseg = new File(out, WALSEG_FILE);
        Files.deleteIfExists(walseg.toPath());
        File staleScratch = new File(out, WALSEG_SCRATCH);
        if (staleScratch.isDirectory()) { // a --force rerun after an aborted emission
            File[] stale = staleScratch.listFiles();
            if (stale != null) for (File f : stale) Files.deleteIfExists(f.toPath());
            Files.deleteIfExists(staleScratch.toPath());
        }

        Generated r = writeWorkload(db);
        selfCheck(db, r);
        writeWalSegFixture(out);
        writeFragment(new File(out, "fragment.tsv"), db, walseg, r);
        // the store lock file is an open-time artifact, not fixture content
        Files.deleteIfExists(new File(out, DB_FILE + ".lock").toPath());
        System.out.println("wrote " + db + " (" + db.length() + " bytes), "
                + walseg + " (" + walseg.length() + " bytes) and fragment.tsv");
    }

    private static void usage(String problem) {
        System.err.println(problem);
        System.err.println("usage: java -ea -cp target/test-classes:target/classes "
                + FixtureWriter.class.getName() + " --out <dir> [--force]");
        System.exit(2);
        throw new IllegalStateException("unreachable");
    }
}
