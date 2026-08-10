package org.mapdb.store;

import org.junit.After;
import org.junit.Test;
import org.mapdb.TmpFiles;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The C8x lock probe's own gate.
 *
 * <p>The probe shipped with no test of any kind (review r1's "defects cluster where the
 * red-discipline wasn't applied"), and the two things it must get right are invisible to the
 * cross-engine matrix that consumes it: the <b>verdict grammar</b> {@code lock_matrix.py} parses,
 * and the rule that a <b>bad invocation is not a verdict</b>. A probe that printed
 * {@code OTHER:…:mode must be rw|ro} and exited 0 would fill in a cell of the matrix that was
 * never measured, and the matrix cannot tell the difference — it only reads the line.
 *
 * <p>What is deliberately NOT here: the cross-process {@code REFUSED} path, which needs a second
 * JVM holding the lock and is the matrix's own job, and {@code hold}'s 30-minute deadline, which
 * has no clock seam.
 */
public class Wal3LockProbeTest {

    private final List<File> files = new ArrayList<>();

    private File newFile() {
        try {
            File f = TmpFiles.tempFile("mapdb-lock-probe", ".wal");
            f.delete();
            files.add(f);
            return f;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @After public void cleanup() {
        for (File f : files) TmpFiles.delete(f);
        files.clear();
    }

    /** Runs {@code r} with stdout captured, and returns exactly what it printed. */
    private static String stdoutOf(Runnable r) {
        PrintStream saved = System.out;
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        try {
            System.setOut(new PrintStream(buf, true, StandardCharsets.UTF_8));
            r.run();
        } finally {
            System.setOut(saved);
        }
        return buf.toString(StandardCharsets.UTF_8);
    }

    /**
     * The grammar {@code lock_matrix.py} parses, pinned by the probe's own output.
     *
     * <p>A free store answers {@code OK} and NOTHING else on stdout: the parser splits on the
     * first colon, so an extra line or a decorated token is a matrix cell that reads as garbage.
     */
    @Test public void a_free_store_answers_exactly_OK() {
        File f = newFile();
        Wal3LockProbe.Args a = new Wal3LockProbe.Args("open", f.getPath(), "rw", null, null);
        assertEquals("OK\n", stdoutOf(() -> Wal3LockProbe.open(a)));
    }

    /**
     * <b>The red for the rethrow.</b> {@code open}'s {@code catch (Throwable)} used to swallow a
     * {@link Wal3LockProbe.BadInvocation} and print it as an {@code OTHER:} verdict on stdout,
     * exiting 0 — a bad invocation masquerading as a measured lock-matrix result. Delete the
     * {@code catch (BadInvocation) { throw bad; }} arm and this case goes green on the exception
     * and red on the stdout assertion, which is the whole point: nothing may be printed.
     */
    @Test public void a_bad_mode_is_a_bad_invocation_not_a_verdict() {
        File f = newFile();
        Wal3LockProbe.Args a = new Wal3LockProbe.Args("open", f.getPath(), "sideways", null, null);
        PrintStream saved = System.out;
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        boolean threw = false;
        try {
            System.setOut(new PrintStream(buf, true, StandardCharsets.UTF_8));
            Wal3LockProbe.open(a);
        } catch (Wal3LockProbe.BadInvocation expected) {
            threw = true;
        } finally {
            System.setOut(saved);
        }
        assertEquals("a bad invocation must print no verdict at all",
                "", buf.toString(StandardCharsets.UTF_8));
        assertTrue("a bad mode produced a verdict instead of a bad invocation", threw);
    }

    /**
     * <b>argv wins</b>, which is what the class doc now says. Env cannot be set from inside this
     * JVM, so the precedence is pinned on the mechanism that implements it: a recognised flag
     * OVERWRITES whatever was already in the field, rather than filling a gap. Make the parser
     * fill gaps only and the second {@code --mode} stops winning here.
     */
    @Test public void a_later_flag_overwrites_an_earlier_value() {
        Wal3LockProbe.Args a = Wal3LockProbe.Args.parse(
                new String[]{"open", "--base", "/tmp/first", "--mode", "ro",
                             "--base", "/tmp/second", "--mode", "rw"});
        assertEquals("rw", a.mode);
        assertEquals("/tmp/second", a.base);
    }

    /** The parser refuses what the protocol does not define, rather than passing it down. */
    @Test public void the_parser_refuses_an_undefined_invocation() {
        refuses("an unknown mode", new String[]{"open", "--base", "/tmp/x", "--mode", "sideways"});
        refuses("an unknown cmd", new String[]{"dance", "--base", "/tmp/x", "--mode", "rw"});
        refuses("an unknown flag", new String[]{"open", "--base", "/tmp/x", "--mode", "rw", "--x"});
        refuses("a missing base", new String[]{"open", "--mode", "rw"});
    }

    private static void refuses(String what, String[] argv) {
        try {
            Wal3LockProbe.Args.parse(argv);
        } catch (Wal3LockProbe.BadInvocation expected) {
            return;
        }
        fail("the parser accepted " + what);
    }
}
