package org.mapdb;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Temporary paths for file-backed stores in the test suite: <b>one private directory per
 * store</b>, all of them under a single per-JVM root that is removed when the JVM exits.
 *
 * <h2>Why not {@code File.createTempFile}</h2>
 *
 * A store is not one file. Opening {@code /tmp/x.wal} also creates {@code /tmp/x.wal.lock} and a
 * segment set {@code /tmp/x.wal.<16 hex>}, and a test that deletes only the base it asked for
 * leaves the rest behind — which is how the suite came to leave tens of thousands of files in
 * {@code java.io.tmpdir}. A whole directory can be removed without knowing what a store put in
 * it, so the sidecars cannot be forgotten one by one.
 *
 * <p>The second reason is cost, and it is the one that made the leak self-amplifying.
 * {@code WalSegmentSet} enumerates its segments by listing <em>the directory the base file sits
 * in</em> — the only way to find {@code <base>.wal.<hex>} without a manifest. With every store in
 * {@code java.io.tmpdir}, each of the suite's thousands of WAL opens listed every file the suite
 * had ever leaked, so the run got slower the longer the leak had been accumulating. One directory
 * per store makes that listing O(this store's own files), which is what it was always meant to be.
 *
 * <h2>Cleanup</h2>
 *
 * {@link #delete} is the eager path, for an {@code @After} hook: it removes the store's private
 * directory once, whatever the store left in it. The shutdown hook is the backstop for the tests
 * that have no hook at all, so the suite leaks nothing even where nobody remembered to clean up.
 * Both are best-effort and never throw: a teardown that throws replaces the failure the test was
 * reporting with its own, and on Windows an mmapped-open file simply cannot be unlinked.
 */
public final class TmpFiles {

    private TmpFiles() { }

    /** Per-store directory, keyed by the absolute path of the base file handed out. */
    private static final Map<String, Path> OWNED = new ConcurrentHashMap<>();

    private static Path root;

    private static synchronized Path root() throws IOException {
        if (root == null) {
            Path r = Files.createTempDirectory("mapdb-test-");
            // Named per JVM rather than fixed, so two concurrent runs never sweep each other's
            // files, and so anything a killed JVM leaves behind is attributable to one run.
            Runtime.getRuntime().addShutdownHook(new Thread(() -> deleteRecursively(r), "mapdb-tmp-sweep"));
            root = r;
        }
        return root;
    }

    /**
     * An empty file named {@code prefix + suffix} alone in a fresh directory — a drop-in for
     * {@code File.createTempFile(prefix, suffix)}, including that the file itself already exists
     * (tests that want a not-yet-existing store {@code delete()} it, as they did before).
     */
    public static File tempFile(String prefix, String suffix) throws IOException {
        Path dir = Files.createTempDirectory(root(), prefix);
        Path file = dir.resolve(prefix + suffix);
        Files.createFile(file);
        File f = file.toFile();
        OWNED.put(f.getAbsolutePath(), dir);
        return f;
    }

    /** A fresh empty directory, for the few tests that place several stores side by side. */
    public static File tempDir(String prefix) throws IOException {
        Path dir = Files.createTempDirectory(root(), prefix);
        File f = dir.toFile();
        OWNED.put(f.getAbsolutePath(), dir);
        return f;
    }

    /**
     * Removes {@code base} and every sidecar its store created, by removing the private directory
     * {@link #tempFile} put it in. <b>Call it only once the store is closed</b>: unlinking a live
     * store's {@code .lock} would let a second opener take a lock on a fresh inode and write to the
     * same store concurrently.
     *
     * <p>A path this class did not hand out is not assumed to own its directory — only the named
     * file is removed then, never the directory around it, because that directory may be somebody
     * else's.
     */
    public static void delete(File base) {
        if (base == null) return;
        Path dir = OWNED.remove(base.getAbsolutePath());
        if (dir != null) deleteRecursively(dir);
        else base.delete();
    }

    /** Best-effort recursive removal: skips what it cannot delete instead of failing the test. */
    private static void deleteRecursively(Path dir) {
        try {
            Files.walkFileTree(dir, new SimpleFileVisitor<>() {
                @Override public FileVisitResult visitFile(Path f, BasicFileAttributes attrs) {
                    tryDelete(f);
                    return FileVisitResult.CONTINUE;
                }

                @Override public FileVisitResult visitFileFailed(Path f, IOException e) {
                    // Vanished or unreadable; either way there is nothing left for us to do to it.
                    return FileVisitResult.CONTINUE;
                }

                @Override public FileVisitResult postVisitDirectory(Path d, IOException e) {
                    tryDelete(d);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException | RuntimeException ignore) {
            // Nothing a test can do about it, and nothing it should fail for.
        }
    }

    private static void tryDelete(Path p) {
        try {
            Files.deleteIfExists(p);
        } catch (IOException ignore) {
            // An open mmapped file on Windows, or a race with the store's own unlink.
        }
    }
}
