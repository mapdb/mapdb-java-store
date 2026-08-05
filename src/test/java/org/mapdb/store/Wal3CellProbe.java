package org.mapdb.store;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Opens ONE derived xfixture cell directory against the reference and reports what happened:
 * the verdict, the file set before and after, and — for a cell whose oracle needs it — the
 * bytes of any section the open appended.
 *
 * <p>This is the java half of the reproducer for the <b>measured</b> rows in the cross-engine
 * contract (`todo/store-cross/impl-contract-stage3.md` §7.3 and §8.1). The python half is
 * `todo/store-wal3/probes/emit-cells.py`, which writes the derived bytes. The first review of
 * the Stage C "C2 entry" slice made the point this class exists to answer: a number recorded in
 * a contract from a scratch script that no longer exists is not evidence, it is a claim.
 *
 * <p><b>Not the Stage C harness.</b> C3 wires the engines to the corpus properly, with the
 * manifest as the oracle. This asks one question of one directory and prints the answer; it
 * asserts nothing, precisely so that a changed answer shows up as changed OUTPUT rather than as
 * a failure someone has to interpret.
 *
 * <p>It lives in {@code org.mapdb.store} because the read-only opener is package-private
 * (D7: java's read-only mode is internal-only), which is the same reason the read-only tests in
 * this package are here.
 *
 * <pre>
 *   python3 todo/store-wal3/probes/emit-cells.py /tmp/cells
 *   javac -d target/test-classes -cp target/classes \
 *       src/test/java/org/mapdb/store/Wal3CellProbe.java
 *   java -ea -cp target/test-classes:target/classes \
 *       org.mapdb.store.Wal3CellProbe /tmp/cells/div-wal3-lsn-exhausted-rw rw [--commit]
 * </pre>
 *
 * <p>{@code --commit} runs Q8's executor step from the catalogue: one {@code put} of
 * {@code payload(161, 64)}, then commit and close, then a reopen. It is a flag rather than the
 * default because it MUTATES the directory, and the mutation is what §8.1 asserts.
 */
public final class Wal3CellProbe {

    private Wal3CellProbe() {}

    /** The contract's payload function: {@code payload(id, len)[i] = (i*131 + id) & 0xff}. */
    static byte[] payload(int id, int len) {
        byte[] b = new byte[len];
        for (int i = 0; i < len; i++) b[i] = (byte) (i * 131 + id);
        return b;
    }

    /** Size-driven raw serializer — serialized content == value, byte for byte (§5.1). */
    static final org.mapdb.ser.Serializer<byte[]> RAW = new org.mapdb.ser.Serializer<>() {
        @Override public void serialize(org.mapdb.io.DataOutput2 out, byte[] v) { out.write(v); }

        @Override public byte[] deserialize(org.mapdb.io.DataInput2 in, int size) {
            byte[] b = new byte[size];
            in.readFully(b);
            return b;
        }

        @Override public boolean equals(byte[] a, byte[] b) { return Arrays.equals(a, b); }

        @Override public int compare(byte[] a, byte[] b) { return Arrays.compare(a, b); }
    };

    /** The directory's files with their lengths, sorted, so a diff is reviewable. */
    private static List<String> listing(File dir) {
        String[] names = dir.list();
        if (names == null) return List.of("<vanished>");
        Arrays.sort(names);
        List<String> out = new ArrayList<>(names.length);
        for (String n : names) out.add(n + ":" + new File(dir, n).length());
        return out;
    }

    /** Opens, reports, and returns the store if it opened (already closed) or null if it refused. */
    private static boolean open(File dir, boolean readOnly, boolean commit) {
        System.out.println("  before: " + listing(dir));
        try {
            StoreWAL s = new StoreWAL(new File(dir, "x"), false, true, 4096, readOnly);
            System.out.println("  verdict: ACCEPT");
            if (commit && !readOnly) {
                long recid = s.put(payload(161, 64), RAW);
                s.commit();
                System.out.println("  committed recid=" + recid);
            }
            s.close();
            System.out.println("  after:  " + listing(dir));
            return true;
        } catch (Throwable t) {
            System.out.println("  verdict: REFUSED " + t.getClass().getSimpleName()
                    + ": " + t.getMessage());
            System.out.println("  after:  " + listing(dir));
            return false;
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("usage: Wal3CellProbe <cell-dir> <rw|ro> [--commit]");
            System.exit(2);
        }
        File dir = new File(args[0]);
        boolean readOnly = switch (args[1]) {
            case "rw" -> false;
            case "ro" -> true;
            default -> throw new IllegalArgumentException("mode must be rw or ro: " + args[1]);
        };
        boolean commit = args.length > 2 && args[2].equals("--commit");

        // The active segment's length before the open, so a `modified:` disposition can be read
        // off as a GROWTH rather than merely as a different hash.
        String[] segNames = dir.list((d, n) -> n.matches("x\\.wal\\.[0-9a-f]{16}"));
        Arrays.sort(segNames == null ? new String[0] : segNames);
        File active = segNames == null || segNames.length == 0
                ? null : new File(dir, segNames[segNames.length - 1]);
        long activeLenBefore = active == null ? -1 : active.length();

        System.out.println(dir.getName() + " mode=" + args[1] + (commit ? " +commit" : ""));
        boolean opened = open(dir, readOnly, commit);

        if (commit && opened && active != null && active.length() > activeLenBefore) {
            // §8.1's `bytes` row: the appended section header's lsn field, 8 bytes BE at the
            // section header's offset 1.
            try (RandomAccessFile raf = new RandomAccessFile(active, "r")) {
                raf.seek(activeLenBefore);
                int tag = raf.readByte() & 0xFF;
                long lsn = raf.readLong();
                long bodyLen = raf.readLong();
                System.out.printf("  appended to %s at offset %d: tag='%c' lsn=%016x bodyLen=%d "
                                + "(%d -> %d bytes)%n",
                        active.getName(), activeLenBefore, (char) tag, lsn, bodyLen,
                        activeLenBefore, active.length());
            }
            System.out.println("  reopen:");
            open(dir, false, false);
        }
    }
}
