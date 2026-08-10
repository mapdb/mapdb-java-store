package org.mapdb.store;

import org.mapdb.DBException;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/**
 * Cross-process WAL store lock probe for Stage C C8x ({@code wal3-c8-plan.md} §3).
 *
 * <p>Invoked by {@code store-cross/lock_matrix.py}. Speaks the normative env protocol:
 * <pre>
 *   MAPDB_LOCK_PROBE_CMD=hold|open
 *   MAPDB_LOCK_PROBE_BASE=&lt;path&gt;
 *   MAPDB_LOCK_PROBE_MODE=rw|ro
 *   MAPDB_LOCK_PROBE_READY=&lt;path&gt;     # hold only
 *   MAPDB_LOCK_PROBE_RELEASE=&lt;path&gt;   # hold only
 * </pre>
 *
 * <p>CLI flags are accepted as an equivalent form ({@code hold|open --base … --mode …}); where a
 * flag and its environment variable are both present, <b>the flag wins</b>.
 *
 * <p>Exit codes: 0 protocol completed (verdict on stdout); 2 bad invocation; 3 infrastructure.
 *
 * <p>{@code REFUSED} is <b>only</b> the production cross-process string from
 * {@link WalSegmentSet}: {@code "WAL store " + base + " is locked by another process"}.
 * Same-JVM wording and every other {@link DBException} are {@code OTHER:…}.
 */
public final class Wal3LockProbe {

    private Wal3LockProbe() {}

    public static void main(String[] argv) {
        try {
            Args a = Args.parse(argv);
            if ("hold".equals(a.cmd)) {
                hold(a);
            } else if ("open".equals(a.cmd)) {
                open(a);
            } else {
                System.err.println("bad cmd: " + a.cmd);
                System.exit(2);
            }
        } catch (BadInvocation e) {
            System.err.println(e.getMessage());
            System.exit(2);
        } catch (Throwable t) {
            t.printStackTrace(System.err);
            System.exit(3);
        }
    }

    private static void hold(Args a) throws Exception {
        if (a.ready == null || a.release == null) {
            throw new BadInvocation("hold requires READY and RELEASE paths");
        }
        Path ready = Path.of(a.ready);
        Path release = Path.of(a.release);
        if (Files.exists(ready) || Files.exists(release)) {
            throw new BadInvocation("ready/release must be initially absent");
        }
        File base = new File(a.base);
        StoreWAL store = openStore(base, a.mode);
        try {
            // Lock is held; create ready marker then wait for release.
            Files.writeString(ready, "ready\n", StandardCharsets.UTF_8);
            System.out.println("HOLD_READY");
            System.out.flush();
            // `now - deadline > 0`, not `now > deadline`: System.nanoTime()'s origin is arbitrary
            // and the value may sit near Long.MAX_VALUE, where the sum above wraps and a plain
            // comparison fires immediately (or never). The subtraction is correct across the wrap
            // — the same reason the JDK's own timed waits are written this way.
            long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(30);
            while (!Files.exists(release)) {
                if (System.nanoTime() - deadline > 0) {
                    throw new IOException("release file never appeared: " + release);
                }
                Thread.sleep(20);
            }
        } finally {
            store.close();
        }
    }

    static void open(Args a) {
        File base = new File(a.base);
        String refusedMsg = "WAL store " + base + " is locked by another process";
        try {
            StoreWAL s = openStore(base, a.mode);
            try {
                s.close();
            } catch (Throwable ignored) {
                // still OK: the open succeeded
            }
            System.out.println("OK");
        } catch (BadInvocation bad) {
            // A BAD INVOCATION IS NOT A LOCK VERDICT. The catch-all below would print it as
            // `OTHER:…:mode must be rw|ro, got …` on stdout and exit 0, and lock_matrix.py would
            // record a cell of the matrix that was never measured. Rethrown so main's handler
            // exits 2, which is what the protocol reserves for it.
            throw bad;
        } catch (DBException e) {
            String msg = String.valueOf(e.getMessage());
            if (refusedMsg.equals(msg)) {
                System.out.println("REFUSED");
            } else {
                System.out.println("OTHER:" + e.getClass().getName() + ":" + msg);
            }
        } catch (Throwable t) {
            System.out.println("OTHER:" + t.getClass().getName() + ":" + t.getMessage());
        }
    }

    /**
     * The mode is validated by {@link Args#parse} too, so this arm is defence in depth rather than
     * the only gate — but it is a REACHABLE arm for any future caller that builds an {@code Args}
     * another way, and {@code open}'s catch-all would otherwise print it as an {@code OTHER:}
     * verdict on stdout and exit 0. {@code open} rethrows it for that reason; a bad invocation must
     * never masquerade as a measured cell of the lock matrix.
     */
    private static StoreWAL openStore(File base, String mode) {
        if ("ro".equals(mode)) {
            return StoreWAL.openReadOnly(base);
        }
        if ("rw".equals(mode)) {
            return new StoreWAL(base);
        }
        throw new BadInvocation("mode must be rw|ro, got " + mode);
    }

    /**
     * Parsed invocation.
     *
     * <p><b>argv wins.</b> Env supplies the defaults and each recognised flag OVERWRITES what the
     * env put there — {@code --mode ro} beside {@code MAPDB_LOCK_PROBE_MODE=rw} yields {@code ro}.
     * The doc said the reverse until review r1 read the loop; a protocol note that contradicts the
     * code is worse than none, because the harness on the other side is written from it.
     */
    static final class Args {
        final String cmd;
        final String base;
        final String mode;
        final String ready;
        final String release;

        Args(String cmd, String base, String mode, String ready, String release) {
            this.cmd = cmd;
            this.base = base;
            this.mode = mode;
            this.ready = ready;
            this.release = release;
        }

        static Args parse(String[] argv) {
            String cmd = env("MAPDB_LOCK_PROBE_CMD");
            String base = env("MAPDB_LOCK_PROBE_BASE");
            String mode = env("MAPDB_LOCK_PROBE_MODE");
            String ready = env("MAPDB_LOCK_PROBE_READY");
            String release = env("MAPDB_LOCK_PROBE_RELEASE");

            int i = 0;
            if (cmd == null && i < argv.length) {
                cmd = argv[i++];
            }
            while (i < argv.length) {
                String f = argv[i++];
                if ("--base".equals(f) && i < argv.length) base = argv[i++];
                else if ("--mode".equals(f) && i < argv.length) mode = argv[i++];
                else if ("--ready-file".equals(f) && i < argv.length) ready = argv[i++];
                else if ("--release-file".equals(f) && i < argv.length) release = argv[i++];
                else if ("hold".equals(f) || "open".equals(f)) cmd = f;
                else throw new BadInvocation("unknown arg: " + f);
            }
            if (cmd == null || base == null || mode == null) {
                throw new BadInvocation(
                        "usage: hold|open with MAPDB_LOCK_PROBE_* or --base/--mode/[--ready-file/--release-file]");
            }
            if (!"rw".equals(mode) && !"ro".equals(mode)) {
                throw new BadInvocation("mode must be rw|ro");
            }
            if (!"hold".equals(cmd) && !"open".equals(cmd)) {
                throw new BadInvocation("cmd must be hold|open");
            }
            return new Args(cmd, base, mode, ready, release);
        }

        private static String env(String k) {
            String v = System.getenv(k);
            return (v == null || v.isEmpty()) ? null : v;
        }
    }

    static final class BadInvocation extends RuntimeException {
        BadInvocation(String m) { super(m); }
    }
}
