package org.mapdb.ser;

import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

/** Deterministic DEFLATE wrapper for any element serializer. */
public final class CompressionSerializer<A> implements Serializer<A> {
    private static final int MAX_PLAIN_LENGTH = 256 * 1024 * 1024;
    private static final int MAX_COMPRESSED_LENGTH = MAX_PLAIN_LENGTH + 1024 * 1024;

    private final Serializer<A> delegate;
    private final int level;

    public CompressionSerializer(Serializer<A> delegate) {
        this(delegate, Deflater.DEFAULT_COMPRESSION);
    }

    public CompressionSerializer(Serializer<A> delegate, int level) {
        if (delegate == null) throw new NullPointerException("delegate");
        if (level < Deflater.DEFAULT_COMPRESSION || level > Deflater.BEST_COMPRESSION)
            throw new IllegalArgumentException("invalid compression level " + level);
        this.delegate = delegate;
        this.level = level;
    }

    public Serializer<A> delegate() { return delegate; }
    public int level() { return level; }

    @Override public void serialize(DataOutput2 out, A value) {
        DataOutput2 plainOut = new DataOutput2(Math.max(16, delegate.sizeHint()));
        delegate.serialize(plainOut, value);
        byte[] plain = plainOut.copyBytes();
        if (plain.length > MAX_PLAIN_LENGTH)
            throw new IllegalArgumentException("uncompressed value exceeds " + MAX_PLAIN_LENGTH);
        byte[] compressed;
        Deflater deflater = new Deflater(level);
        try {
            java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
            try (java.util.zip.DeflaterOutputStream compressedOut =
                         new java.util.zip.DeflaterOutputStream(bytes, deflater)) {
                compressedOut.write(plain);
            }
            compressed = bytes.toByteArray();
        } catch (java.io.IOException e) {
            throw new IllegalStateException("compression failed", e);
        } finally {
            deflater.end();
        }
        if (compressed.length > MAX_COMPRESSED_LENGTH)
            throw new IllegalArgumentException("compressed value exceeds " + MAX_COMPRESSED_LENGTH);
        out.packInt(plain.length);
        out.packInt(compressed.length);
        out.write(compressed);
    }

    @Override public A deserialize(DataInput2 in, int size) {
        int start = in.pos();
        int plainLength = in.unpackInt();
        int compressedLength = in.unpackInt();
        if (plainLength < 0 || plainLength > MAX_PLAIN_LENGTH || compressedLength < 0
                || compressedLength > MAX_COMPRESSED_LENGTH)
            throw new IllegalArgumentException("invalid compressed frame length");
        if (size >= 0) {
            long remaining = (long) size - (in.pos() - start);
            if (remaining < 0 || compressedLength > remaining)
                throw new IllegalArgumentException("compressed length exceeds record frame");
        }
        byte[] compressed = new byte[compressedLength];
        in.readFully(compressed);
        byte[] plain = new byte[plainLength];
        Inflater inflater = new Inflater();
        try {
            inflater.setInput(compressed);
            int written = 0;
            if (plain.length == 0) {
                written = inflater.inflate(new byte[1]);
            } else {
                while (!inflater.finished() && written < plain.length) {
                    int count = inflater.inflate(plain, written, plain.length - written);
                    if (count == 0) break;
                    written += count;
                }
            }
            if (written != plainLength || !inflater.finished())
                throw new IllegalArgumentException("invalid compressed frame length");
        } catch (DataFormatException e) {
            throw new IllegalArgumentException("invalid compressed data", e);
        } finally {
            inflater.end();
        }
        return delegate.deserialize(new DataInput2.ByteArray(plain, 0), plainLength);
    }

    @Override public int compare(A a, A b) { return delegate.compare(a, b); }
    @Override public boolean equals(A a, A b) { return delegate.equals(a, b); }
    @Override public boolean naturalOrder() { return delegate.naturalOrder(); }
    // DEFLATE output is not canonical across zlib versions/levels, so equal values can serialize
    // to different bytes. The Serializer contract requires canonical encoding for byte comparison,
    // so we must not delegate here.
    @Override public boolean equalsBySerializedBytes() { return false; }
}
