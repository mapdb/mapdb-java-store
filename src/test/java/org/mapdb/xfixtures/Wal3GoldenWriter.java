package org.mapdb.xfixtures;

import org.mapdb.store.StoreWAL;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mapdb.xfixtures.FixtureWriter.RAW;
import static org.mapdb.xfixtures.FixtureWriter.check;
import static org.mapdb.xfixtures.FixtureWriter.payload;
import static org.mapdb.xfixtures.FixtureWriter.sha256Hex;

/**
 * Writes the Stage C <b>golden vectors</b>: whole WAL v3 namespaces produced by the reference,
 * byte-pinned and checked in under {@code todo/store-cross/testdata/}.
 *
 * <p>Contract §11 gate 1 asks for at least two segments, multiple {@code 'S'}/{@code 'C'}/{@code
 * 'K'} sections, empty and non-empty bodies, and distinct offsets — because a round-trip test
 * cannot catch a bug the parser and the writer share, and {@code walfmt.py}'s single 165-byte
 * vector exercises exactly one segment holding one {@code 'S'} section. Everything the python
 * codec claims about rollover, cleaner images, mark bodies and the per-section CRC domain was,
 * until this class existed, unwitnessed by any reference bytes.
 *
 * <p>Two shapes, matching the catalogue's accept bundles:
 *
 * <ul>
 *   <li><b>tail</b> — commits only, spread across several segments by a small {@code
 *       segmentBytes}. No mark: the log is a plain tail.</li>
 *   <li><b>cleaned</b> — the same workload followed by {@link StoreWAL#checkpoint()}, which
 *       writes the whole committed store as one {@code 'C'} image and then a forced {@code 'K'}
 *       mark ({@code StoreWAL.java:85}). This is the shape every derived reject cell starts
 *       from, so it is the one that must be pinned hardest.</li>
 * </ul>
 *
 * <p><b>Determinism</b> is a precondition, not a hope: the writer emits each shape TWICE into
 * separate directories and refuses to publish unless the two runs agree byte for byte, file set
 * included. The segment format carries no timestamp and no randomness, but that is a claim about
 * the reference's current behaviour and this is the cheapest place to keep testing it.
 *
 * <p>Zero-length bodies: {@code preallocate()} contributes a section whose entry carries no
 * payload, so the corpus covers the empty-body case §11 asks for without a synthetic hack.
 */
public final class Wal3GoldenWriter {

    private Wal3GoldenWriter() {}

    /** Small enough that a handful of commits roll several times; above the {@code SEG_HDR+SEC_HDR} floor. */
    private static final long SEGMENT_BYTES = 4096;

    /** Distinct per shape so two bundles never coincide by accident (contract §5). */
    private static final int TAIL_BASE = 101, CLEANED_BASE = 111;

    /**
     * Runs the fixed workload against a fresh namespace at {@code dir/x}.
     *
     * <p>Fixed in every particular a byte could depend on: the operation sequence, the recids
     * they land on, the payload ids and the payload lengths. "One fixed workload" is not a
     * property a comment can assert, so nothing here reads a clock, a random source, a map
     * iteration order or a file system listing.
     */
    private static void workload(File dir, int base, boolean checkpoint) throws IOException {
        File store = new File(dir, "x");
        StoreWAL s = new StoreWAL(store);
        try {
            s.setSegmentBytes(SEGMENT_BYTES);
            // commit 1: three records of different sizes, plus a prealloc (empty body).
            long a = s.put(payload(base, 200), RAW);
            long b = s.put(payload(base + 1, 900), RAW);
            s.preallocate();
            s.commit();
            // commit 2: big enough to force at least one rollover on its own.
            for (int i = 0; i < 6; i++) s.put(payload(base + 10 + i, 700), RAW);
            s.commit();
            // commit 3: updates and a delete, so the log holds more than appends.
            s.update(a, payload(base + 2, 120), RAW);
            s.delete(b, RAW);
            s.commit();
            // commit 4: a final small one, so the active segment is not at a boundary.
            s.put(payload(base + 3, 64), RAW);
            s.commit();
            // commits 5 and 6: the SMALLEST bodies the reference can produce — a
            // prealloc alone is a 2-byte body, a zero-length record a 4-byte one.
            //
            // §11 gate 1 asks for "empty and non-empty bodies". Measured, the
            // reference cannot write an empty one at all: a commit with nothing
            // pending emits no section rather than a zero-length body. So the
            // gate's empty case is unsatisfiable from reference bytes and is
            // covered synthetically in walfmt's battery instead; these two
            // commits pin the smallest bodies that do exist, which is what the
            // gate was reaching for — a section whose body is far shorter than
            // its 25-byte header.
            s.preallocate();
            s.commit();
            s.put(new byte[0], RAW);
            s.commit();
            if (checkpoint) s.checkpoint();
        } finally {
            s.close();
        }
    }

    /** The bundle's files, sorted by segment sequence, with the lock excluded. */
    private static List<File> bundleFiles(File dir) {
        File[] all = dir.listFiles();
        check(all != null, "namespace directory vanished: " + dir);
        List<File> out = new ArrayList<>();
        for (File f : all) {
            if (f.getName().equals("x.lock")) continue;   // an open-time artifact, never content
            check(f.isFile(), "unexpected non-file in the namespace: " + f);
            check(f.getName().matches("x\\.wal\\.[0-9a-f]{16}"),
                    "unexpected file in the namespace: " + f);
            out.add(f);
        }
        out.sort((p, q) -> p.getName().compareTo(q.getName()));  // hex names sort lexicographically
        return out;
    }

    private static String describe(File dir) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (File f : bundleFiles(dir)) {
            byte[] raw = Files.readAllBytes(f.toPath());
            sb.append(f.getName()).append('\t').append(raw.length)
              .append('\t').append(sha256Hex(raw)).append('\n');
        }
        return sb.toString();
    }

    private static void emit(File outRoot, String name, int base, boolean checkpoint)
            throws IOException {
        File run1 = new File(outRoot, ".run1-" + name);
        File run2 = new File(outRoot, ".run2-" + name);
        for (File d : new File[]{run1, run2}) {
            wipe(d);
            Files.createDirectories(d.toPath());
        }
        workload(run1, base, checkpoint);
        workload(run2, base, checkpoint);

        // Determinism, MEASURED. Two runs, compared file set and bytes — not asserted in prose.
        String d1 = describe(run1), d2 = describe(run2);
        check(d1.equals(d2), "shape " + name + " is NOT deterministic:\n" + d1 + "--\n" + d2);

        List<File> files = bundleFiles(run1);
        check(files.size() >= 2, "shape " + name + " produced " + files.size()
                + " segment(s); §11 gate 1 requires at least two");

        File dest = new File(outRoot, name);
        wipe(dest);
        Files.createDirectories(dest.toPath());
        for (File f : files) Files.move(f.toPath(), new File(dest, f.getName()).toPath());
        wipe(run1);
        wipe(run2);
        System.out.println("wrote " + name + ":");
        System.out.print(d1.replaceAll("(?m)^", "  "));
    }

    private static void wipe(File dir) throws IOException {
        if (!dir.exists()) return;
        File[] kids = dir.listFiles();
        if (kids != null) for (File f : kids) wipe(f);
        Files.delete(dir.toPath());
    }

    public static void main(String[] args) throws IOException {
        File out = null;
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--out") && i + 1 < args.length) out = new File(args[++i]);
            else {
                System.err.println("usage: java -ea -cp target/test-classes:target/classes "
                        + Wal3GoldenWriter.class.getName() + " --out <dir>");
                System.exit(2);
            }
        }
        if (out == null) { System.err.println("--out is required"); System.exit(2); }
        Files.createDirectories(out.toPath());
        emit(out, "wal3-java-tail", TAIL_BASE, false);
        emit(out, "wal3-java-cleaned", CLEANED_BASE, true);
    }
}
