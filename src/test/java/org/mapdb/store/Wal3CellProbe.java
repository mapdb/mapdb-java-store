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
 *       src/test/java/org/mapdb/store/Wal3Actions.java \
 *       src/test/java/org/mapdb/store/Wal3CellProbe.java
 *   java -ea -cp target/test-classes:target/classes \
 *       org.mapdb.store.Wal3CellProbe /tmp/cells/div-wal3-lsn-exhausted-rw rw \
 *       [--action commit_one_record op=put,recid_label=Q,payload_id=161,payload_len=64,serializer=raw]
 * </pre>
 *
 * <p><b>{@code --action} takes the catalogue's arguments verbatim</b> and hands them to
 * {@link Wal3Actions}, which owns the verb. C4 drives this CLI from {@code catalogue.actions},
 * so an edit to the cell's pinned arguments changes the command line rather than being ignored
 * by a probe that had them baked in — which is what the earlier {@code --commit} flag did, and
 * what the C4 plan review called out: a deterministic CLI committing the wrong record can author
 * a matching {@code modified:} hash and pass. It is a flag rather than the default because it
 * MUTATES the directory, and the mutation is what §8.1 asserts.
 *
 * <p>Lines beginning {@code RESULT } are the machine-readable contract with the sync script; the
 * rest is for a human reading a probe run.
 */
public final class Wal3CellProbe {

    private Wal3CellProbe() {}

    /** The directory's files with their lengths, sorted, so a diff is reviewable. */
    private static List<String> listing(File dir) {
        String[] names = dir.list();
        if (names == null) return List.of("<vanished>");
        Arrays.sort(names);
        List<String> out = new ArrayList<>(names.length);
        for (String n : names) out.add(n + ":" + new File(dir, n).length());
        return out;
    }

    /** Opens, runs the action if there is one, reports. True if the open was accepted. */
    private static boolean open(File dir, boolean readOnly, String verb, String argSpec) {
        System.out.println("  before: " + listing(dir));
        try {
            StoreWAL s = new StoreWAL(new File(dir, "x"), false, true, 4096, readOnly);
            System.out.println("  verdict: ACCEPT");
            System.out.println("RESULT verdict=ACCEPT");
            if (verb != null && !readOnly) {
                // Not caught by the surrounding handler on purpose: a store that OPENED and then
                // failed its action is a different fact from one that refused to open, and
                // collapsing the two would let a broken action print `REFUSED` and be read as
                // the cell's own verdict.
                System.out.println(Wal3Actions.run(s, verb, Wal3Actions.parseArgs(argSpec)));
            }
            s.close();
            System.out.println("  after:  " + listing(dir));
            return true;
        } catch (Throwable t) {
            System.out.println("  verdict: REFUSED " + t.getClass().getSimpleName()
                    + ": " + t.getMessage());
            System.out.println("RESULT verdict=REFUSED family=" + t.getClass().getSimpleName()
                    + " message=" + String.valueOf(t.getMessage()).replace('\n', ' '));
            System.out.println("  after:  " + listing(dir));
            return false;
        }
    }

    /**
     * Copy the cell directory's regular files out. Called BETWEEN the action's close and the
     * reopen: the reopen is itself an open, so a caller that read the directory afterwards would
     * be recording whatever that second open did to it. It happens not to touch the segments
     * today — the S2 refusal comes out of the scan — but "happens not to" is not a property to
     * hash a corpus against.
     */
    private static void emit(File dir, File out) throws java.io.IOException {
        if (!out.mkdirs() && !out.isDirectory()) {
            throw new java.io.IOException("cannot create emit directory " + out);
        }
        String[] names = dir.list();
        Arrays.sort(names == null ? new String[0] : names);
        for (String n : names == null ? new String[0] : names) {
            File src = new File(dir, n);
            if (!src.isFile()) continue;
            java.nio.file.Files.copy(src.toPath(), new File(out, n).toPath(),
                    java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            System.out.println("RESULT emitted=" + n + " len=" + src.length());
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("usage: Wal3CellProbe <cell-dir> <rw|ro> "
                    + "[--action <verb> <k=v,...>] [--emit <dir>]");
            System.exit(2);
        }
        File dir = new File(args[0]);
        boolean readOnly = switch (args[1]) {
            case "rw" -> false;
            case "ro" -> true;
            default -> throw new IllegalArgumentException("mode must be rw or ro: " + args[1]);
        };
        String verb = null, argSpec = "";
        File emitTo = null;
        for (int i = 2; i < args.length; i++) {
            switch (args[i]) {
                case "--action" -> {
                    if (i + 2 >= args.length) {
                        throw new IllegalArgumentException("--action takes <verb> <k=v,...>");
                    }
                    verb = args[++i];
                    argSpec = args[++i];
                }
                case "--emit" -> {
                    if (i + 1 >= args.length) {
                        throw new IllegalArgumentException("--emit takes a directory");
                    }
                    emitTo = new File(args[++i]);
                }
                default -> throw new IllegalArgumentException("unknown argument: " + args[i]);
            }
        }

        // The active segment's length before the open, so a `modified:` disposition can be read
        // off as a GROWTH rather than merely as a different hash.
        String[] segNames = dir.list((d, n) -> n.matches("x\\.wal\\.[0-9a-f]{16}"));
        Arrays.sort(segNames == null ? new String[0] : segNames);
        File active = segNames == null || segNames.length == 0
                ? null : new File(dir, segNames[segNames.length - 1]);
        long activeLenBefore = active == null ? -1 : active.length();

        System.out.println(dir.getName() + " mode=" + args[1]
                + (verb == null ? "" : " +action " + verb));
        boolean opened = open(dir, readOnly, verb, argSpec);
        if (emitTo != null) emit(dir, emitTo);

        if (verb != null && opened && active != null && active.length() > activeLenBefore) {
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
                System.out.printf("RESULT appended rel=%s offset=%d tag=%c lsn=%016x "
                                + "grew=%d:%d%n",
                        active.getName(), activeLenBefore, (char) tag, lsn,
                        activeLenBefore, active.length());
            }
            System.out.println("  reopen:");
            System.out.println("RESULT reopen");
            open(dir, false, null, "");
        }
    }
}
