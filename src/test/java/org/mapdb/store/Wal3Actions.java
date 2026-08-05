package org.mapdb.store;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The xfixtures v2 post-open ACTION verbs, implemented once (Stage C, slice C4j).
 *
 * <p>A cell whose oracle is "open, then do something, then assert" carries that something as
 * DATA in {@code todo/store-cross/catalogue.py} — an {@code Action(verb, **args)} — precisely so
 * that the validator can read it and two implementers cannot write two different things (C0a
 * round 2, finding 1). This class is the java side of that contract, and it exists as its own
 * class rather than inside a probe for the reason the C4 plan review gave: a separate CLI is
 * fine as transport, a separate implementation of the verb is not. {@link Wal3CellProbe} drives
 * it today; the C6j conformance executor must drive this same code rather than write a second
 * {@code commit_one_record}.
 *
 * <p><b>Every argument the catalogue pins is required and is checked.</b> An unknown verb, a
 * missing argument, an unrecognised argument, or an argument whose value this class does not
 * implement is a hard failure. The point is not defensiveness: it is that the caller passes the
 * catalogue's arguments through verbatim, so a catalogue edit that this class cannot honour must
 * stop the run instead of silently executing the old behaviour and authoring a post-state hash
 * for it.
 */
public final class Wal3Actions {

    private Wal3Actions() {}

    /** The contract's payload function: {@code payload(id, len)[i] = (i*131 + id) & 0xff} (§5.1). */
    public static byte[] payload(int id, int len) {
        byte[] b = new byte[len];
        for (int i = 0; i < len; i++) b[i] = (byte) (i * 131 + id);
        return b;
    }

    /** Size-driven raw serializer — serialized content == value, byte for byte (§5.1). */
    public static final org.mapdb.ser.Serializer<byte[]> RAW = new org.mapdb.ser.Serializer<>() {
        @Override public void serialize(org.mapdb.io.DataOutput2 out, byte[] v) { out.write(v); }

        @Override public byte[] deserialize(org.mapdb.io.DataInput2 in, int size) {
            byte[] b = new byte[size];
            in.readFully(b);
            return b;
        }

        @Override public boolean equals(byte[] a, byte[] b) { return Arrays.equals(a, b); }

        @Override public int compare(byte[] a, byte[] b) { return Arrays.compare(a, b); }
    };

    /** `k=v,k=v,…` as the CLI passes it, order preserved so an error message reads like the input. */
    public static Map<String, String> parseArgs(String spec) {
        Map<String, String> out = new LinkedHashMap<>();
        if (spec == null || spec.isEmpty()) return out;
        for (String part : spec.split(",", -1)) {
            int eq = part.indexOf('=');
            if (eq <= 0) throw new IllegalArgumentException("action argument is not k=v: " + part);
            String k = part.substring(0, eq);
            if (out.put(k, part.substring(eq + 1)) != null) {
                throw new IllegalArgumentException("action argument repeated: " + k);
            }
        }
        return out;
    }

    private static String require(Map<String, String> args, String key, List<String> known) {
        String v = args.get(key);
        if (v == null) {
            throw new IllegalArgumentException(
                    "action argument " + key + " is required; got " + args.keySet()
                            + ", the verb takes " + known);
        }
        return v;
    }

    /**
     * Run one catalogue action against an OPEN store. Returns a one-line machine-readable
     * description of what it did, for the caller to echo and for the sync script to assert on.
     */
    public static String run(StoreWAL s, String verb, Map<String, String> args) {
        if (!verb.equals("commit_one_record")) {
            throw new IllegalArgumentException("unknown action verb: " + verb);
        }
        List<String> known = List.of("op", "recid_label", "payload_id", "payload_len", "serializer");
        for (String k : args.keySet()) {
            if (!known.contains(k)) {
                throw new IllegalArgumentException(
                        "unknown argument " + k + " for " + verb + "; it takes " + known);
            }
        }
        String op = require(args, "op", known);
        String label = require(args, "recid_label", known);
        int payloadId = Integer.parseInt(require(args, "payload_id", known));
        int payloadLen = Integer.parseInt(require(args, "payload_len", known));
        String ser = require(args, "serializer", known);

        if (!op.equals("put")) {
            throw new IllegalArgumentException("commit_one_record: unimplemented op " + op);
        }
        if (!ser.equals("raw")) {
            throw new IllegalArgumentException("commit_one_record: unimplemented serializer " + ser);
        }
        long recid = s.put(payload(payloadId, payloadLen), RAW);
        s.commit();
        return "RESULT action=" + verb + " label=" + label + " recid=" + recid
                + " payloadId=" + payloadId + " payloadLen=" + payloadLen;
    }
}
