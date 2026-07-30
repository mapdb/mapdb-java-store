package org.mapdb.format;

/**
 * Cross-package reach into the P7 fingerprint knob {@code BufferedPageFormat.fpMode}, which is
 * package-private precisely so that a JVM-global fault-injection switch is not shipped as public
 * API. This class lives in {@code org.mapdb.format} under {@code src/test}, so it can see the
 * package-private setter while remaining absent from the published jar. Tests inside the package
 * call {@code BufferedPageFormat.testSetFpMode} directly; tests elsewhere (the BufferTree
 * integration tests) go through here.
 *
 * <p>The knob is global mutable state: a test that sets it MUST restore {@link Mode#NORMAL}.
 */
public final class FpModeTestHook {

    /** Mirrors {@code BufferedPageFormat.FpMode} constant-for-constant; mapped across by name. */
    public enum Mode { NORMAL, FORCE_SEARCH, RANDOM }

    static {
        // a mode added to the format without a mirror here fails loudly rather than going untested
        if (Mode.values().length != BufferedPageFormat.FpMode.values().length)
            throw new AssertionError("FpMode and FpModeTestHook.Mode drifted apart");
        for (Mode m : Mode.values()) BufferedPageFormat.FpMode.valueOf(m.name());
    }

    private FpModeTestHook() {
    }

    /** Installs {@code mode} globally; {@link Mode#NORMAL} (or {@code null}) restores production. */
    public static void set(Mode mode) {
        BufferedPageFormat.testSetFpMode(
                mode == null ? null : BufferedPageFormat.FpMode.valueOf(mode.name()));
    }
}
