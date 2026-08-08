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
 * Reads an xfixtures {@code MANIFEST.tsv} of <b>schema version 2</b>.
 *
 * <p>Stage C slice <b>C7j</b> retired the dual v1/v2 dispatch: schema version 1 is refused
 * outright. The arity collision that forced the dual reader still matters as history —
 * v1 and v2 {@code expect} rows were both seven fields with different columns — so the
 * version row remains a hard gate rather than an arity-keyed guess. A version-1 line
 * fails here with a message that names the retirement; an unknown version fails too.
 *
 * <p>The parser <b>refuses an unknown row type</b>. That is not defensive decoration: it is the
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

    // ---------------------------------------------------------------- schema v2

    static final class V2 {
        /** One {@code file} row of a v2 namespace fixture. */
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

        /** {@code applies <fid> <engine> <mode>} — a cell this corpus actually contains. */
        static final class Applies {
            String fixtureId, engine, mode;
        }

        /** {@code action <fid> <engine> <mode> <verb> <args>} — a post-open executor step. */
        static final class Action {
            String fixtureId, engine, mode, verb, argSpec;
        }

        /** {@code bytes <fid> <engine> <mode> <relName> <offset> <hex>} — against the POST bytes. */
        static final class Bytes {
            String fixtureId, engine, mode, relName, hex;
            long offset;
        }

        /** {@code reopen <fid> <engine> <mode> <family>} — the SECOND open must fail with it. */
        static final class Reopen {
            String fixtureId, engine, mode, family;
        }

        /**
         * {@code family <fid> <engine> <mode> <family>} — first-open refusal family (C8f f1).
         *
         * <p>Bijection with reject arms: every reject cell carries exactly one; accept cells
         * carry none. Distinct from {@link Reopen}, which is the stability second open (and is
         * still omitted on mutating R6-audit/rw).
         */
        static final class Family {
            String fixtureId, engine, mode, family;
        }

        final Map<String, String> fixtureKinds = new LinkedHashMap<>();
        final List<FileRow> files = new ArrayList<>();
        final List<Expect> expects = new ArrayList<>();
        final List<Post> posts = new ArrayList<>();
        final List<Applies> applies = new ArrayList<>();
        final List<Action> actions = new ArrayList<>();
        final List<Bytes> bytes = new ArrayList<>();
        final List<Reopen> reopens = new ArrayList<>();
        final List<Family> families = new ArrayList<>();
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

        List<Action> actionsOf(String fixtureId, String engine, String mode) {
            List<Action> out = new ArrayList<>();
            for (Action a : actions)
                if (a.fixtureId.equals(fixtureId) && a.engine.equals(engine) && a.mode.equals(mode))
                    out.add(a);
            return out;
        }

        List<Bytes> bytesOf(String fixtureId, String engine, String mode) {
            List<Bytes> out = new ArrayList<>();
            for (Bytes b : bytes)
                if (b.fixtureId.equals(fixtureId) && b.engine.equals(engine) && b.mode.equals(mode))
                    out.add(b);
            return out;
        }

        List<Reopen> reopensOf(String fixtureId, String engine, String mode) {
            List<Reopen> out = new ArrayList<>();
            for (Reopen r : reopens)
                if (r.fixtureId.equals(fixtureId) && r.engine.equals(engine) && r.mode.equals(mode))
                    out.add(r);
            return out;
        }

        List<Family> familiesOf(String fixtureId, String engine, String mode) {
            List<Family> out = new ArrayList<>();
            for (Family f : families)
                if (f.fixtureId.equals(fixtureId) && f.engine.equals(engine) && f.mode.equals(mode))
                    out.add(f);
            return out;
        }
    }

    // ---------------------------------------------------------------- load / version gate

    /** One loaded schema-v2 manifest. {@link #version} is always 2; kept so call sites can pin it. */
    static final class Loaded {
        final int version;
        final V2 v2;

        private Loaded(int version, V2 v2) {
            this.version = version;
            this.v2 = v2;
        }
    }

    /**
     * Loads {@code <resourceRoot>MANIFEST.tsv} from the classpath and parses it as schema v2.
     * A missing manifest is a HARD failure: it means the fixture sync step was never run for this
     * checkout, and skipping would report green for an empty suite.
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
            case "1":
                throw new AssertionError("manifest schema version 1 is retired (Stage C, C7j) — "
                        + "this reader speaks only schema 2; the dual v1/v2 dispatch is gone");
            case "2":
                return new Loaded(2, parseV2(lines, first + 1));
            default:
                throw new AssertionError("unsupported manifest schema version " + head[1]
                        + " — this reader speaks only schema 2; refusing rather than guessing at the columns");
        }
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
                case "applies": {
                    arity(t, 4, line);
                    V2.Applies ap = new V2.Applies();
                    ap.fixtureId = t[1];
                    ap.engine = vocab(t[2], ENGINES, "engine", line);
                    ap.mode = vocab(t[3], MODES, "mode", line);
                    for (V2.Applies prior : m.applies)
                        check(!cellEq(prior.fixtureId, prior.engine, prior.mode, ap.fixtureId,
                                        ap.engine, ap.mode),
                                "duplicate applies row: " + line);
                    m.applies.add(ap);
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
                case "action": {
                    arity(t, 6, line);
                    V2.Action a = new V2.Action();
                    a.fixtureId = t[1];
                    a.engine = vocab(t[2], ENGINES, "engine", line);
                    a.mode = vocab(t[3], MODES, "mode", line);
                    a.verb = t[4];
                    a.argSpec = actionArgs(t[5], line);
                    // One action row per cell per verb. NOT one per cell: `catalogue.actions` holds
                    // a LIST, so a second verb on the same cell is a legal future shape, and
                    // refusing it here would refuse the corpus rather than a defect.
                    for (V2.Action prior : m.actions)
                        check(!(cellEq(prior.fixtureId, prior.engine, prior.mode, a.fixtureId,
                                        a.engine, a.mode) && prior.verb.equals(a.verb)),
                                "duplicate action row for " + a.fixtureId + "/" + a.engine + "/"
                                        + a.mode + "/" + a.verb + ": " + line);
                    m.actions.add(a);
                    break;
                }
                case "reopen": {
                    arity(t, 5, line);
                    V2.Reopen r = new V2.Reopen();
                    r.fixtureId = t[1];
                    r.engine = vocab(t[2], ENGINES, "engine", line);
                    r.mode = vocab(t[3], MODES, "mode", line);
                    // The family is NOT vocabulary-checked here. `catalogue.FAMILIES` has 18
                    // members and this engine implements a predicate for a handful of them; a
                    // vocabulary check would accept a family the executor then cannot run, and the
                    // executor's own refusal is both stricter and the one that matters. Checking it
                    // twice, in two places, with two lists, is how the second list goes stale.
                    r.family = t[4];
                    for (V2.Reopen prior : m.reopens)
                        check(!cellEq(prior.fixtureId, prior.engine, prior.mode, r.fixtureId,
                                        r.engine, r.mode),
                                "duplicate reopen row for " + r.fixtureId + "/" + r.engine + "/"
                                        + r.mode + ": " + line);
                    m.reopens.add(r);
                    break;
                }
                case "family": {
                    // C8f f1 — first-open family oracle. Same arity/shape as reopen; the executor
                    // grades the cell's OWN refusal against this row and consumes it exactly once.
                    // Vocabulary is not checked here for the same reason as reopen: the executor's
                    // assertFamily is the gate that refuses an unimplemented name.
                    arity(t, 5, line);
                    V2.Family f = new V2.Family();
                    f.fixtureId = t[1];
                    f.engine = vocab(t[2], ENGINES, "engine", line);
                    f.mode = vocab(t[3], MODES, "mode", line);
                    f.family = t[4];
                    for (V2.Family prior : m.families)
                        check(!cellEq(prior.fixtureId, prior.engine, prior.mode, f.fixtureId,
                                        f.engine, f.mode),
                                "duplicate family row for " + f.fixtureId + "/" + f.engine + "/"
                                        + f.mode + ": " + line);
                    m.families.add(f);
                    break;
                }
                case "bytes": {
                    arity(t, 7, line);
                    V2.Bytes b = new V2.Bytes();
                    b.fixtureId = t[1];
                    b.engine = vocab(t[2], ENGINES, "engine", line);
                    b.mode = vocab(t[3], MODES, "mode", line);
                    b.relName = relName(t[4], line);
                    b.offset = nat(t[5], line);
                    b.hex = hexBlob(t[6], line);
                    for (V2.Bytes prior : m.bytes)
                        check(!(cellEq(prior.fixtureId, prior.engine, prior.mode, b.fixtureId,
                                        b.engine, b.mode) && prior.relName.equals(b.relName)
                                        && prior.offset == b.offset),
                                "duplicate bytes row for " + b.fixtureId + "/" + b.engine + "/"
                                        + b.mode + "/" + b.relName + "@" + b.offset + ": " + line);
                    m.bytes.add(b);
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
                    throw new AssertionError("unknown v2 manifest row type: " + line);
            }
        }
        return m;
    }

    // ---------------------------------------------------------------- shared scalar forms

    /** Expands one {@code recidrange} row into one expectation per recid. */
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

    /** The four oracle row types are all keyed by a CELL; written once so three copies cannot drift. */
    private static boolean cellEq(String f1, String e1, String m1, String f2, String e2, String m2) {
        return f1.equals(f2) && e1.equals(e2) && m1.equals(m2);
    }

    /**
     * An {@code action} row's argument spec, matching {@code catalogue.render_action_args}.
     *
     * <p>{@code k=v} pairs joined by {@code ,}, <b>keys in sorted order</b>, each key
     * {@code [a-z][a-z0-9_]*}, each value nonempty and drawn from {@code catalogue.ARG_VALUE_CHARS}
     * — which excludes TAB, {@code ,} and {@code =} precisely so the row can be re-split.
     *
     * <p>The sort order is checked rather than ignored because the spec reaches
     * {@link org.mapdb.store.Wal3Actions} as a STRING and is compared, in todo's gate, against a
     * single rendering authority. A reader that accepted any order would accept a manifest that
     * python refuses, and the two roots would then disagree about what the same cell says.
     */
    private static String actionArgs(String s, String line) {
        String prev = null;
        for (String pair : s.split(",", -1)) {
            int eq = pair.indexOf('=');
            check(eq > 0 && pair.indexOf('=', eq + 1) < 0,
                    "action argument " + pair + " is not one k=v pair in: " + line);
            String k = pair.substring(0, eq), v = pair.substring(eq + 1);
            check(k.charAt(0) >= 'a' && k.charAt(0) <= 'z'
                            && k.chars().allMatch(c -> (c >= 'a' && c <= 'z')
                                    || (c >= '0' && c <= '9') || c == '_'),
                    "action argument key " + k + " is not [a-z][a-z0-9_]* in: " + line);
            check(prev == null || prev.compareTo(k) < 0,
                    "action argument keys must be sorted and distinct: " + k + " follows " + prev
                            + " in: " + line);
            prev = k;
            check(!v.isEmpty() && v.chars().allMatch(c -> ARG_VALUE_CHARS.indexOf(c) >= 0),
                    "action argument " + k + "=" + v + ": value must be nonempty and drawn from "
                            + "the pinned character class in: " + line);
        }
        return s;
    }

    /** {@code catalogue.ARG_VALUE_CHARS}, transcribed. */
    private static final String ARG_VALUE_CHARS =
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._@:/+-";

    /** An even-length run of lowercase hex, nonempty: a {@code bytes} row's asserted value. */
    private static String hexBlob(String s, String line) {
        check(!s.isEmpty() && s.length() % 2 == 0
                        && s.chars().allMatch(c -> (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')),
                "not a nonempty even-length lowercase hex blob: " + s + " in: " + line);
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
