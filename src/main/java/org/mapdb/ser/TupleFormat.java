package org.mapdb.ser;

import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Binary-capable {@link GroupFormat} for composite/tuple keys, e.g.
 * {@code (tenant, table, id)}. The element type is a tuple —
 * an {@code Object[]} of typed components — and each tuple is encoded to an
 * ORDER-PRESERVING (memcomparable) {@code byte[]} by its {@link TupleComponent} schema,
 * so the UNSIGNED byte order of the encodings equals the logical tuple order.
 *
 * <h3>Why</h3>
 * Without this, composite keys fall to the generic {@link ObjectArrayFormat}, which is
 * {@code supportsBinary()==false}: a point read must deserialize and box the whole key
 * group on every lookup. Here, once each tuple is a memcomparable byte string, the group
 * is simply "sorted byte strings", so it is stored and searched EXACTLY like
 * {@link ByteArrayFormat} — the byte side binary-searches the serialized bytes in place
 * (a single probe encoding per lookup, then zero-allocation in-place memcmp), never
 * materializing or boxing the group's components.
 *
 * <h3>Layout</h3>
 * The group is represented internally as the encoded {@code byte[][]} and delegated
 * wholesale to {@link ByteArrayFormat}: same wire (blobLen + fixed-width offset table +
 * concatenated bytes) and same unsigned in-place compare with a length tie-break, giving
 * {@code (prefix) < (prefix+more)} — i.e. {@code (a) < (a,b)} for prefix tuples. The only
 * novelty over {@code ByteArrayFormat} is the per-tuple {@link TupleComponent} codec and
 * the typed {@code get}/{@code compare}/{@code comparator} surface.
 *
 * <h3>Order</h3>
 * {@link #compare} is component-wise (each {@link TupleComponent}'s logical order; on an
 * equal shared prefix the SHORTER tuple is smaller). Object-side {@link #search} and
 * byte-side {@link #binarySearch} use the encoded unsigned order. These are ONE order
 * because each component's encoding is order-preserving under unsigned compare and the
 * escaped-terminated variable-length codec is prefix-free — asserted exhaustively by the
 * format test battery (compare-vs-encoded-order fuzz + a TreeMap control). {@code comparator()}
 * is non-null (tuples are not naturally {@link Comparable}).
 *
 * <h3>Arity</h3>
 * A tuple may have FEWER components than the schema (prefix tuples, arity
 * {@code 0..schema.length}); no arity marker is written — fixed-width components
 * self-delimit by width, variable-length ones by their {@code 0x00 0x00} terminator, and
 * decode recovers arity by consuming components until the encoded bytes are exhausted.
 */
public final class TupleFormat implements GroupFormat<Object[]> {

    private static final ByteArrayFormat BYTES = ByteArrayFormat.INSTANCE;

    private final TupleComponent[] schema;
    private final TupleSerializer serializer;

    private TupleFormat(TupleComponent[] schema) {
        this.schema = schema;
        this.serializer = new TupleSerializer();
    }

    /** Build a tuple format over the given ordered component types (arity = length). */
    public static TupleFormat of(TupleComponent... components) {
        if (components == null || components.length == 0)
            throw new IllegalArgumentException("tuple schema must have at least one component");
        for (TupleComponent c : components)
            if (c == null) throw new IllegalArgumentException("null tuple component");
        return new TupleFormat(components.clone());
    }

    /** Defensive copy of this format's persisted component schema. */
    public TupleComponent[] schema() { return schema.clone(); }

    // ---- per-tuple memcomparable codec ----

    /** Shared tuple validator: arity within schema, no null components (nullable columns
     *  are unsupported); used by every element-facing operation so nulls fail fast with the
     *  same IllegalArgumentException everywhere, not a codec-specific NPE. */
    private void checkTuple(Object[] tuple) {
        if (tuple.length > schema.length)
            throw new IllegalArgumentException("tuple arity " + tuple.length + " exceeds schema " + schema.length);
        for (int i = 0; i < tuple.length; i++) {
            if (tuple[i] == null)
                throw new IllegalArgumentException(
                        "null tuple component at " + i + ": nullable columns are not supported");
        }
    }

    byte[] encode(Object[] tuple) {
        checkTuple(tuple);
        DataOutput2 out = new DataOutput2();
        for (int i = 0; i < tuple.length; i++) schema[i].encode(out, tuple[i]);
        return out.copyBytes();
    }

    Object[] decode(byte[] enc) {
        DataInput2 in = new DataInput2.ByteArray(enc, 0);
        List<Object> r = new ArrayList<>(schema.length);
        int i = 0;
        while (in.pos() < enc.length) {
            if (i == schema.length) throw new IllegalStateException("corrupt tuple: more components than schema");
            r.add(schema[i].decode(in, enc.length));
            i++;
        }
        return r.toArray();
    }

    // ---- element ----

    @Override public Serializer<Object[]> element() { return serializer; }

    @Override public int compare(Object[] a, Object[] b) {
        checkTuple(a);
        checkTuple(b);
        int min = Math.min(a.length, b.length);
        for (int i = 0; i < min; i++) {
            int c = schema[i].compare(a[i], b[i]);
            if (c != 0) return c;
        }
        return Integer.compare(a.length, b.length); // shorter prefix is smaller
    }

    @Override public Comparator<Object[]> comparator() { return this::compare; }

    // ---- object side (group == encoded byte[][], delegated to ByteArrayFormat) ----

    @Override public Object empty() { return BYTES.empty(); }

    @Override public int size(Object group) { return BYTES.size(group); }

    @Override public Object[] get(Object group, int pos) { return decode(BYTES.get(group, pos)); }

    @Override public int search(Object group, Object[] key) { return BYTES.search(group, encode(key)); }

    @Override public Object insert(Object group, int pos, Object[] newValue) {
        return BYTES.insert(group, pos, encode(newValue));
    }

    @Override public Object set(Object group, int pos, Object[] newValue) {
        return BYTES.set(group, pos, encode(newValue));
    }

    @Override public Object delete(Object group, int pos) { return BYTES.delete(group, pos); }

    @Override public Object copyRange(Object group, int from, int to) { return BYTES.copyRange(group, from, to); }

    @Override public Object fromArray(Object[] values) {
        Object[] enc = new Object[values.length];
        for (int i = 0; i < values.length; i++) enc[i] = encode((Object[]) values[i]);
        return BYTES.fromArray(enc);
    }

    // ---- wire ----

    @Override public void serialize(DataOutput2 out, Object group) { BYTES.serialize(out, group); }

    @Override public Object deserialize(DataInput2 in, int size) { return BYTES.deserialize(in, size); }

    // ---- byte side ----

    @Override public boolean supportsBinary() { return true; }

    @Override public int binarySearch(Object[] key, DataInput2 in, int size) {
        return BYTES.binarySearch(encode(key), in, size);
    }

    @Override public Object[] binaryGet(DataInput2 in, int size, int pos) {
        return decode(BYTES.binaryGet(in, size, pos));
    }

    /** Self-delimiting standalone codec for a single tuple: {@code packInt(len) + encoded}. */
    private final class TupleSerializer implements Serializer<Object[]> {
        @Override public void serialize(DataOutput2 out, Object[] value) {
            byte[] e = encode(value);
            out.packInt(e.length);
            out.write(e);
        }

        @Override public Object[] deserialize(DataInput2 in, int size) {
            byte[] e = new byte[in.unpackInt()];
            in.readFully(e);
            return decode(e);
        }

        @Override public int compare(Object[] a, Object[] b) { return TupleFormat.this.compare(a, b); }

        @Override public boolean equals(Object[] a, Object[] b) {
            checkTuple(a);
            checkTuple(b);
            if (a.length != b.length) return false;
            for (int i = 0; i < a.length; i++) if (!schema[i].equalTo(a[i], b[i])) return false;
            return true;
        }

        @Override public boolean equalsBySerializedBytes() { return true; } // memcomparable encoding is canonical
        @Override public boolean naturalOrder() { return false; }           // tuples are not natural-Comparable
    }
}
