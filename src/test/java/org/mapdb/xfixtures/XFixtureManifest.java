package org.mapdb.xfixtures;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.mapdb.xfixtures.FixtureWriter.check;

/**
 * Reads an xfixtures {@code MANIFEST.tsv} of schema version 1 or 2, dispatching on the
 * {@code version} row and on nothing else.
 *
 * <p><b>Why the dispatch rule is spelled out rather than assumed.</b> The two schemas' {@code
 * expect} rows have <em>the same arity and different columns</em>:
 *
 * <pre>
 *   v1: expect &lt;fid&gt; &lt;engine&gt; &lt;verdict&gt; &lt;opener&gt; &lt;placeAs&gt; &lt;openArg&gt;   (7)
 *   v2: expect &lt;fid&gt; &lt;engine&gt; &lt;mode&gt;    &lt;verdict&gt; &lt;opener&gt; &lt;openArg&gt;   (7)
 * </pre>
 *
 * v2 dropped {@code placeAs} and gained {@code mode}. A reader that keyed on the field count
 * would read v2's {@code mode} as v1's {@code verdict} and v2's {@code verdict} as v1's
 * {@code opener} — and would get away with it today only because {@code rw} happens not to be in
 * the verdict vocabulary. So the version row selects the parser, the two parsers share no row
 * struct, and {@link V1} and {@link V2} are separate types on purpose: the schemas are not a
 * superset relation and modelling them as one is what makes the confusion above possible.
 *
 * <p>Both parsers <b>refuse an unknown row type</b>. That is not defensive decoration: it is the
 * only thing standing between a future row type and a suite that silently ignores it while
 * reporting green, and — because the python validator cannot test a Java parser — it is checked
 * by a negative case in {@code XFixtureConformanceTest}.
 *
 * <p>Junit-free (it throws {@link AssertionError} via {@link FixtureWriter#check}) so the same
 * loader serves the test runner and the {@code main}-driven {@link Wal3BodyDump} emitter.
 */
final class XFixtureManifest {

    private XFixtureManifest() {}

    /** {@code live}/{@code null} are listed by {@code getAllRecids}; {@code prealloc}/{@code deleted} are not. */
    static final List<String> STATES = List.of("live", "null", "prealloc", "deleted");

    /**
     * {@code catalogue.ENGINES}. Validated rather than ignored because the executors select cells
     * with {@code "java".equals(engine)}: an engine name this reader does not know would be
     * silently skipped, which is the shape of every "green because nothing ran" defect this
     * workstream has spent its reviews removing.
     */
    static final List<String> ENGINES = List.of("java", "rust", "zig");

    static final List<String> MODES = List.of("rw", "ro");

    // ---------------------------------------------------------------- schema v1

    static final class V1 {
        static final class FileRow {
            String fixtureId, relName, rawSha, gzSha;
            long rawLen;
        }

        static final class Expect {
            String fixtureId, engine, verdict, opener, placeAs, openArg;
        }

        final Map<String, String> fixtureKinds = new LinkedHashMap<>();
        final List<FileRow> files = new ArrayList<>();
        final List<Expect> expects = new ArrayList<>();
        final Map<String, List<FixtureWriter.RecidExpect>> recids = new HashMap<>();

        /** Schema v1 gives every fixture exactly one file row (asserted while resolving). */
        FileRow fileOf(String fixtureId) {
            FileRow found = null;
            for (FileRow f : files) {
                if (!f.fixtureId.equals(fixtureId)) continue;
                check(found == null,
                        "fixture " + fixtureId + " has more than one file row (schema v1 forbids that)");
                found = f;
            }
            check(found != null, "fixture " + fixtureId + " has no file row");
            return found;
        }
    }

    // ---------------------------------------------------------------- schema v2

    static final class V2 {
        /** A v2 file row. Distinct from {@link V1.FileRow} even though the columns coincide today. */
        static final class FileRow {
            String fixtureId, relName, rawSha, gzSha;
            long rawLen;

            /** The distributed blob name: v2 prefixes the fixture id, so bundles may share relNames. */
            String blobName() { return fixtureId + "." + relName + ".gz"; }
        }

        static final class Expect {
            String fixtureId, engine, mode, verdict, opener, openArg;
        }

        /** An expected post-open disposition of one file: {@code unchanged}, {@code created:<len>:<sha>}, ... */
        static final class Post {
            String fixtureId, engine, mode, relName, verb, sha;
            long length = -1;
        }

        final Map<String, String> fixtureKinds = new LinkedHashMap<>();
        final List<FileRow> files = new ArrayList<>();
        final List<Expect> expects = new ArrayList<>();
        final List<Post> posts = new ArrayList<>();
        final Map<String, List<FixtureWriter.RecidExpect>> recids = new HashMap<>();

        /** A v2 fixture is a whole namespace: one or more file rows, in manifest order. */
        List<FileRow> filesOf(String fixtureId) {
            List<FileRow> out = new ArrayList<>();
            for (FileRow f : files) if (f.fixtureId.equals(fixtureId)) out.add(f);
            check(!out.isEmpty(), "fixture " + fixtureId + " has no file row");
            return out;
        }

        List<Post> postsOf(String fixtureId, String engine, String mode) {
            List<Post> out = new ArrayList<>();
            for (Post p : posts)
                if (p.fixtureId.equals(fixtureId) && p.engine.equals(engine) && p.mode.equals(mode))
                    out.add(p);
            return out;
        }
    }

    // ---------------------------------------------------------------- dispatch

    /** One loaded manifest: exactly one of {@link #v1} and {@link #v2} is non-null. */
    static final class Loaded {
        final int version;
        final V1 v1;
        final V2 v2;

        private Loaded(int version, V1 v1, V2 v2) {
            this.version = version;
            this.v1 = v1;
            this.v2 = v2;
        }
    }

    /**
     * Loads {@code <resourceRoot>MANIFEST.tsv} from the classpath and parses it with the parser
     * its version row names. A missing manifest is a HARD failure: it means the fixture sync step
     * was never run for this checkout, and skipping would report green for an empty suite.
     */
    static Loaded load(String resourceRoot) throws IOException {
        byte[] bytes;
        try (InputStream in = XFixtureManifest.class.getResourceAsStream(resourceRoot + "MANIFEST.tsv")) {
            check(in != null, "classpath resource " + resourceRoot + "MANIFEST.tsv is missing — "
                    + "the fixture sync step was never run for this checkout");
            bytes = in.readAllBytes();
        }
        return parse(new String(bytes, StandardCharsets.UTF_8));
    }

    static Loaded parse(String text) {
        String[] lines = text.split("\n", -1);
        int first = -1;
        for (int i = 0; i < lines.length; i++) {
            if (lines[i].isBlank()) continue;
            first = i;
            break;
        }
        check(first >= 0, "empty manifest");
        String[] head = lines[first].split("\t", -1);
        check(head.length == 2 && head[0].equals("version"),
                "the first manifest data line is not a version row: " + lines[first]);
        switch (head[1]) {
            case "1": return new Loaded(1, parseV1(lines, first + 1), null);
            case "2": return new Loaded(2, null, parseV2(lines, first + 1));
            default: throw new AssertionError("unsupported manifest schema version " + head[1]
                    + " — this reader knows 1 and 2; refusing rather than guessing at the columns");
        }
    }

    // ---------------------------------------------------------------- v1 parser

    private static V1 parseV1(String[] lines, int from) {
        V1 m = new V1();
        for (int i = from; i < lines.length; i++) {
            String line = lines[i];
            if (line.isBlank() || line.startsWith("#")) continue;
            String[] t = line.split("\t", -1);
            switch (t[0]) {
                case "version":
                    throw new AssertionError("a second version row: " + line);
                case "fixture":
                    arity(t, 5, line);
                    check(!m.fixtureKinds.containsKey(t[1]), "duplicate fixture row: " + line);
                    m.fixtureKinds.put(t[1], t[2]);
                    break;
                case "file": {
                    arity(t, 6, line);
                    V1.FileRow f = new V1.FileRow();
                    f.fixtureId = t[1];
                    f.relName = relName(t[2], line);
                    f.rawLen = nat(t[3], line);
                    f.rawSha = sha256(t[4], line);
                    f.gzSha = sha256(t[5], line);
                    m.files.add(f);
                    break;
                }
                case "expect": {
                    arity(t, 7, line);
                    V1.Expect e = new V1.Expect();
                    e.fixtureId = t[1];
                    e.engine = vocab(t[2], ENGINES, "engine", line);
                    e.verdict = vocab(t[3], List.of("accept", "reject"), "verdict", line);
                    e.opener = vocab(t[4], List.of("direct", "wal"), "opener", line);
                    e.placeAs = relName(t[5], line);
                    e.openArg = relName(t[6], line);
                    // A v1 cell is identified by (fixture, engine, opener, placeAs) — NOT by
                    // (fixture, engine). The live tree has both a `direct` and a `wal` cell for the
                    // same engine on `wal-v1-rust-tail`, which is the whole point of the v1
                    // opener column; a narrower key rejects the real manifest, as it did here.
                    for (V1.Expect prior : m.expects)
                        check(!(prior.fixtureId.equals(e.fixtureId) && prior.engine.equals(e.engine)
                                        && prior.opener.equals(e.opener) && prior.placeAs.equals(e.placeAs)),
                                "duplicate expect row for " + e.fixtureId + "/" + e.engine + "/"
                                        + e.opener + ": " + line);
                    m.expects.add(e);
                    break;
                }
                case "recid":
                    arity(t, 7, line);
                    addRecid(m.recids, t[1], recid(t[2], nat(t[3], line), state(t[4], line),
                            nat(t[5], line), nat(t[6], line)), line);
                    break;
                case "recidrange":
                    arity(t, 8, line);
                    expandRange(m.recids, t, line);
                    break;
                case "edit":
                    arity(t, 6, line);
                    // reject-derivation provenance; the referenced files are consumed pre-edited
                    break;
                default:
                    throw new AssertionError("unknown v1 manifest row type: " + line);
            }
        }
        return m;
    }

    // ---------------------------------------------------------------- v2 parser

    private static V2 parseV2(String[] lines, int from) {
        V2 m = new V2();
        for (int i = from; i < lines.length; i++) {
            String line = lines[i];
            // Schema v2 has no comment and no blank-line forms: `manifest_v2.py` refuses both
            // (`blank_line`, and `#` fails `row_type_unknown`). A trailing newline splits to one
            // empty tail element and is the file form, so only that one is tolerated.
            if (line.isEmpty() && i == lines.length - 1) continue;
            String[] t = line.split("\t", -1);
            switch (t[0]) {
                case "version":
                    throw new AssertionError("a second version row: " + line);
                case "fixture":
                    arity(t, 5, line);
                    check(!m.fixtureKinds.containsKey(t[1]), "duplicate fixture row: " + line);
                    m.fixtureKinds.put(t[1], t[2]);
                    break;
                case "derived":
                    arity(t, 5, line);
                    // `derived <fid> <src> <deriverVersion> <recipe>` — PROVENANCE for a fixture
                    // that also has its own `fixture` row, not a second way to declare one. Reading
                    // t[2] as a kind (the shape of the `fixture` row two cases up) would put a
                    // fixture id in the kind column and never be noticed, since nothing here
                    // consumes kinds. The files are distributed already-derived, so like `edit`
                    // this row is a record of how they were made, not an instruction.
                    nat(t[3], line);
                    break;
                case "file": {
                    arity(t, 6, line);
                    V2.FileRow f = new V2.FileRow();
                    f.fixtureId = t[1];
                    f.relName = relName(t[2], line);
                    f.rawLen = nat(t[3], line);
                    f.rawSha = sha256(t[4], line);
                    f.gzSha = sha256(t[5], line);
                    for (V2.FileRow prior : m.files)
                        check(!(prior.fixtureId.equals(f.fixtureId) && prior.relName.equals(f.relName)),
                                "duplicate relName in fixture " + f.fixtureId + ": " + line);
                    m.files.add(f);
                    break;
                }
                case "expect": {
                    arity(t, 7, line);
                    V2.Expect e = new V2.Expect();
                    e.fixtureId = t[1];
                    e.engine = vocab(t[2], ENGINES, "engine", line);
                    e.mode = vocab(t[3], MODES, "mode", line);
                    e.verdict = vocab(t[4], List.of("accept", "reject"), "verdict", line);
                    e.opener = vocab(t[5], List.of("direct", "wal3"), "opener", line);
                    e.openArg = relName(t[6], line);
                    for (V2.Expect prior : m.expects)
                        check(!(prior.fixtureId.equals(e.fixtureId) && prior.engine.equals(e.engine)
                                        && prior.mode.equals(e.mode)),
                                "duplicate expect row for " + e.fixtureId + "/" + e.engine + "/"
                                        + e.mode + ": " + line);
                    m.expects.add(e);
                    break;
                }
                case "post": {
                    arity(t, 6, line);
                    V2.Post p = new V2.Post();
                    p.fixtureId = t[1];
                    p.engine = vocab(t[2], ENGINES, "engine", line);
                    p.mode = vocab(t[3], MODES, "mode", line);
                    p.relName = relName(t[4], line);
                    String[] d = t[5].split(":", -1);
                    p.verb = vocab(d[0],
                            List.of("unchanged", "deleted", "truncated", "created", "modified"),
                            "disposition verb", line);
                    boolean sized = !d[0].equals("unchanged") && !d[0].equals("deleted");
                    check(d.length == (sized ? 3 : 1), "disposition " + t[5] + " has the wrong "
                            + "argument count for verb " + d[0] + ": " + line);
                    if (sized) {
                        p.length = nat(d[1], line);
                        p.sha = sha256(d[2], line);
                    }
                    for (V2.Post prior : m.posts)
                        check(!(prior.fixtureId.equals(p.fixtureId) && prior.engine.equals(p.engine)
                                        && prior.mode.equals(p.mode) && prior.relName.equals(p.relName)),
                                "duplicate post row for " + p.fixtureId + "/" + p.engine + "/"
                                        + p.mode + "/" + p.relName + ": " + line);
                    m.posts.add(p);
                    break;
                }
                case "bytes":
                    arity(t, 7, line);
                    // A byte-level assertion on a derived cell; no derived fixture exists before C4,
                    // so a row here would describe a file this reader was never given. Refuse.
                    throw new AssertionError("v2 `bytes` row, which this reader does not execute yet "
                            + "(C4 introduces the derived fixtures it describes): " + line);
                case "recid":
                    arity(t, 7, line);
                    addRecid(m.recids, t[1], recid(t[2], nat(t[3], line), state(t[4], line),
                            nat(t[5], line), nat(t[6], line)), line);
                    break;
                case "recidrange":
                    arity(t, 8, line);
                    expandRange(m.recids, t, line);
                    break;
                case "edit":
                    arity(t, 6, line);
                    // reject-derivation provenance; the referenced files are consumed pre-edited
                    break;
                default:
                    throw new AssertionError("unknown v2 manifest row type: " + line);
            }
        }
        return m;
    }

    // ---------------------------------------------------------------- shared scalar forms

    /**
     * {@code recid}/{@code recidrange} are the two row types whose columns are IDENTICAL in v1 and
     * v2, so the expansion is shared. Stated here rather than left to be noticed: sharing a helper
     * between the parsers is safe exactly where the columns are the same, and nowhere else.
     */
    private static void expandRange(Map<String, List<FixtureWriter.RecidExpect>> into,
                                    String[] t, String line) {
        long from = nat(t[3], line), to = nat(t[4], line);
        check(from <= to, "empty recidrange row: " + line);
        // A range is EXPANDED into one expectation per recid, so its span is an allocation, not a
        // number. Without this, `to = Long.MAX_VALUE` both exhausts memory and wraps the `r <= to`
        // loop counter, so the parser never returns at all.
        check(to - from < MAX_RANGE_SPAN,
                "recidrange spans " + (to - from + 1) + " recids, above the " + MAX_RANGE_SPAN
                        + " this reader expands: " + line);
        String st = state(t[5], line);
        long base = nat(t[6], line), len = nat(t[7], line);
        for (long r = from; r <= to; r++)
            addRecid(into, t[1], recid(t[2] + "[" + r + "]", r, st, base + (r - from), len), line);
    }

    /** Generous enough for any real fixture, small enough that a bad row fails instead of hanging. */
    private static final long MAX_RANGE_SPAN = 1L << 20;

    /**
     * Adds one expectation, refusing a second row for the same recid within a fixture.
     *
     * <p>{@code manifest_v2.py} refuses a duplicate LABEL; this refuses a duplicate RECID, which is
     * the stronger rule and the one the executor needs: two rows for one recid cannot both be
     * satisfied, and whichever loses is a check that silently never runs.
     */
    private static void addRecid(Map<String, List<FixtureWriter.RecidExpect>> into, String fixtureId,
                                 FixtureWriter.RecidExpect r, String line) {
        List<FixtureWriter.RecidExpect> list = into.computeIfAbsent(fixtureId, k -> new ArrayList<>());
        for (FixtureWriter.RecidExpect prior : list)
            check(prior.recid != r.recid,
                    "duplicate recid " + r.recid + " in fixture " + fixtureId + ": " + line);
        list.add(r);
    }

    /**
     * Builds one expectation, refusing a payload id or length that does not fit the {@code int}
     * the reader contract holds them in. Silent narrowing would turn {@code payloadId = 2^32 + 3}
     * into {@code 3} and compare a record against the wrong payload while reporting a match.
     */
    private static FixtureWriter.RecidExpect recid(String label, long recid, String state,
                                                   long payloadId, long len) {
        check(payloadId <= Integer.MAX_VALUE, "payloadId " + payloadId + " does not fit an int");
        check(len <= Integer.MAX_VALUE, "len " + len + " does not fit an int");
        return new FixtureWriter.RecidExpect(label, recid, state, (int) payloadId, (int) len);
    }

    /**
     * Field count, plus the rule that no field is empty.
     *
     * <p>{@code manifest_v2.py} rejects an empty field outright ({@code field_empty}). An arity
     * check alone lets {@code fixture\t\t\t\t} through with the right shape and nothing in it, and
     * every consumer downstream then works with empty ids that match nothing — which reads as "no
     * cells for that fixture" rather than as an error.
     */
    private static void arity(String[] t, int want, String line) {
        check(t.length == want,
                "bad " + t[0] + " row: expected " + want + " fields, got " + t.length + ": " + line);
        for (int i = 0; i < t.length; i++)
            check(!t[i].isEmpty(), "bad " + t[0] + " row: field " + i + " is empty: " + line);
    }

    /**
     * A canonical decimal natural, matching {@code manifest_v2._nat}: digits only, no sign, and no
     * leading zero. {@code Long.parseLong} alone accepts {@code +1} and {@code 007}, so two
     * manifests that differ textually would compare equal — and these files are compared as text
     * by the sync step.
     */
    private static long nat(String s, String line) {
        check(!s.isEmpty() && s.chars().allMatch(c -> c >= '0' && c <= '9')
                        && (s.equals("0") || s.charAt(0) != '0'),
                "not a canonical decimal non-negative integer: " + s + " in: " + line);
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            throw new AssertionError("integer out of range: " + s + " in: " + line);
        }
    }

    /**
     * A relName safe to resolve against a cell directory, matching {@code manifest_v2._check_relname}.
     *
     * <p>Not schema pedantry: the executor resolves this string with {@code new File(cell, rel)},
     * so {@code ../..}-style names would read and write outside the private cell directory the
     * whole harness is built around.
     */
    private static String relName(String s, String line) {
        check(!s.isEmpty() && s.indexOf('/') < 0 && s.indexOf('\\') < 0 && s.indexOf('\0') < 0
                        && !s.equals(".") && !s.equals("..") && !s.startsWith("-")
                        && !new java.io.File(s).isAbsolute(),
                "unsafe relName " + s + " in: " + line);
        return s;
    }

    private static String sha256(String s, String line) {
        check(s.length() == 64 && s.chars().allMatch(c -> (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')),
                "not 64 lowercase hex digits: " + s + " in: " + line);
        return s;
    }

    private static String state(String s, String line) {
        return vocab(s, STATES, "recid state", line);
    }

    private static String vocab(String s, List<String> allowed, String what, String line) {
        check(allowed.contains(s), "unknown " + what + " " + s + " (expected one of " + allowed
                + ") in: " + line);
        return s;
    }
}
