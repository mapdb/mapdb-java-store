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
 * <p>CLI flags are accepted as an equivalent form ({@code hold|open --base … --mode …}).
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
            long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(30);
            while (!Files.exists(release)) {
                if (System.nanoTime() > deadline) {
                    throw new IOException("release file never appeared: " + release);
                }
                Thread.sleep(20);
            }
        } finally {
            store.close();
        }
    }

    private static void open(Args a) {
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

    private static StoreWAL openStore(File base, String mode) {
        if ("ro".equals(mode)) {
            return StoreWAL.openReadOnly(base);
        }
        if ("rw".equals(mode)) {
            return new StoreWAL(base);
        }
        throw new BadInvocation("mode must be rw|ro, got " + mode);
    }

    /** Parsed invocation; env wins, argv fills gaps. */
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
