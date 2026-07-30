package org.mapdb.store;

import org.mapdb.DBException;
import org.mapdb.io.DataInput2;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

/**
 * Paged ByteBuffer volume: 1 MB slices, heap/direct allocation or file-backed
 * mmap. Records never cross a slice boundary (allocator
 * invariant) so reads are always zero-copy views over one slice.
 *
 * File mode maps the file as READ_WRITE 1 MB slices; {@link #ensureAvailable}
 * grows the file (mapping past EOF extends it), {@link #sync()} is the
 * durability point (msync every slice + fsync), and {@link #close(long)} can
 * shrink the file back to the store's logical tail.
 *
 * Thread safety: absolute puts/gets at disjoint offsets are safe; same-offset
 * exclusion is the caller's job (segment locks). Growth is synchronized and the
 * slice array is volatile-published; existing slice references never change.
 */
final class ByteBufferVol {

    static final int SLICE_SHIFT = 20;
    static final long SLICE_SIZE = 1L << SLICE_SHIFT;
    static final long SLICE_MASK = SLICE_SIZE - 1;

    private static final byte[] ZEROES = new byte[1 << 16];

    private final boolean direct;
    private final FileChannel channel; // null = in-memory (anonymous buffers)
    private volatile ByteBuffer[] slices = new ByteBuffer[0];

    ByteBufferVol(boolean direct) {
        this.direct = direct;
        this.channel = null;
    }

    /** File-backed volume: 1 MB mmap slices over {@code file}, with force + truncate. */
    ByteBufferVol(File file) {
        this.direct = false;
        try {
            this.channel = FileChannel.open(file.toPath(),
                    StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        } catch (IOException e) {
            throw new DBException("volume open failed: " + file, e);
        }
    }

    boolean isFileBacked() { return channel != null; }

    /**
     * Physical file length in file mode; addressable mapped length in memory mode.
     * Reopen validation must see a torn/truncated final page before mmap growth can
     * silently round it back up to a whole slice.
     */
    long length() {
        if (channel == null) return ((long) slices.length) << SLICE_SHIFT;
        try {
            return channel.size();
        } catch (IOException e) {
            throw new DBException("volume length failed", e);
        }
    }

    /** Grow so that bytes [0, endOffset) are addressable. */
    synchronized void ensureAvailable(long endOffset) {
        int needed = (int) ((endOffset + SLICE_MASK) >>> SLICE_SHIFT);
        ByteBuffer[] old = slices;
        if (old.length >= needed) return;
        ByteBuffer[] grown = java.util.Arrays.copyOf(old, needed);
        for (int i = old.length; i < needed; i++) {
            if (channel != null) {
                try {
                    grown[i] = channel.map(FileChannel.MapMode.READ_WRITE, ((long) i) << SLICE_SHIFT, SLICE_SIZE);
                } catch (IOException e) {
                    throw new DBException("volume mmap failed at slice " + i, e);
                }
            } else {
                grown[i] = direct ? ByteBuffer.allocateDirect((int) SLICE_SIZE) : ByteBuffer.allocate((int) SLICE_SIZE);
            }
        }
        slices = grown;
    }

    private ByteBuffer slice(long offset) {
        return slices[(int) (offset >>> SLICE_SHIFT)];
    }

    void putData(long offset, byte[] src, int srcOff, int len) {
        assert (offset & SLICE_MASK) + len <= SLICE_SIZE : "record crosses slice boundary";
        ByteBuffer dup = slice(offset).duplicate();
        dup.position((int) (offset & SLICE_MASK));
        dup.put(src, srcOff, len);
    }

    void getData(long offset, byte[] dst, int dstOff, int len) {
        ByteBuffer dup = slice(offset).duplicate();
        dup.position((int) (offset & SLICE_MASK));
        dup.get(dst, dstOff, len);
    }

    void putInt(long offset, int value) {
        slice(offset).putInt((int) (offset & SLICE_MASK), value);
    }

    int getInt(long offset) {
        return slice(offset).getInt((int) (offset & SLICE_MASK));
    }

    void putLong(long offset, long value) {
        slice(offset).putLong((int) (offset & SLICE_MASK), value);
    }

    long getLong(long offset) {
        return slice(offset).getLong((int) (offset & SLICE_MASK));
    }

    void putByte(long offset, int value) {
        slice(offset).put((int) (offset & SLICE_MASK), (byte) value);
    }

    int getUnsignedByte(long offset) {
        return slice(offset).get((int) (offset & SLICE_MASK)) & 0xFF;
    }

    /** Zero out [from, to); may span slices. */
    void clear(long from, long to) {
        while (from < to) {
            ByteBuffer dup = slice(from).duplicate();
            int off = (int) (from & SLICE_MASK);
            int n = (int) Math.min(to - from, SLICE_SIZE - off);
            dup.position(off);
            int done = 0;
            while (done < n) {
                int step = Math.min(ZEROES.length, n - done);
                dup.put(ZEROES, 0, step);
                done += step;
            }
            from += n;
        }
    }

    /** Zero-copy positioned view; valid only during the action call. */
    DataInput2 dataInput(long offset, int size) {
        assert (offset & SLICE_MASK) + size <= SLICE_SIZE : "record crosses slice boundary";
        return new DataInput2.ByteBuf(slice(offset), (int) (offset & SLICE_MASK));
    }

    /**
     * Like {@link #dataInput} but null when the slice was retired (or the volume
     * closed): the caller re-resolves its record location and retries. A non-null
     * result stays valid for the whole call even if the slice retires concurrently —
     * retirement only unlinks the buffer, never touches its bytes, and the returned
     * view's reference keeps it alive for the GC.
     */
    DataInput2 dataInputOrNull(long offset, int size) {
        assert (offset & SLICE_MASK) + size <= SLICE_SIZE : "record crosses slice boundary";
        ByteBuffer[] s = slices;
        int idx = (int) (offset >>> SLICE_SHIFT);
        if (idx >= s.length) return null;
        ByteBuffer b = s[idx]; // single element read: a second one could observe a racing
        if (b == null) return null; // retireSlice, handing the view a null buffer (NPE, not retry)
        return new DataInput2.ByteBuf(b, (int) (offset & SLICE_MASK));
    }

    /**
     * Unlink a slice so the GC can reclaim it once no in-flight reader still holds a
     * view over it. The slot stays null forever (allocation never revisits retired
     * offsets), so its bytes are never overwritten — this is what makes GC-based
     * reclamation safe for wait-free readers. Writer/compactor only.
     */
    void retireSlice(int idx) {
        slices[idx] = null;
    }

    /** Durability point: msync every mapped slice, then fsync. No-op in memory mode. */
    void sync() {
        if (channel == null) return;
        for (ByteBuffer b : slices) {
            if (b instanceof MappedByteBuffer m) m.force();
        }
        try {
            channel.force(true);
        } catch (IOException e) {
            throw new DBException("volume sync failed", e);
        }
    }

    /**
     * Durability point for the header page only (slice 0). Used as the second phase of a
     * commit: data slices must already be durable ({@link #sync()}) before the commit marker
     * is stamped and forced here — flushing the marker in the same pass as the data leaves
     * a window where a crash persists a valid-looking header over unwritten data.
     */
    void syncHeader() {
        if (channel == null) return;
        ByteBuffer[] s = slices;
        if (s.length > 0 && s[0] instanceof MappedByteBuffer m) m.force();
        try {
            channel.force(true);
        } catch (IOException e) {
            throw new DBException("volume sync failed", e);
        }
    }

    void close() { close(-1); }

    /** Shrink the addressable volume to {@code truncateTo}; caller guarantees no one uses removed slices. */
    synchronized void truncate(long truncateTo) {
        if (truncateTo < 0 || (truncateTo & SLICE_MASK) != 0)
            throw new DBException("volume truncate target must be page-aligned: " + truncateTo);
        int needed = (int) (truncateTo >>> SLICE_SHIFT);
        ByteBuffer[] old = slices;
        if (old.length > needed) {
            slices = java.util.Arrays.copyOf(old, needed);
        }
        if (channel != null) {
            try {
                if (truncateTo < channel.size()) channel.truncate(truncateTo);
                channel.force(true);
            } catch (IOException e) {
                throw new DBException("volume truncate failed", e);
            }
        }
    }

    /** Close the volume; in file mode first shrink the file to {@code truncateTo} when >= 0. */
    void close(long truncateTo) {
        slices = new ByteBuffer[0];
        if (channel != null) {
            try {
                if (truncateTo >= 0 && channel.isOpen() && truncateTo < channel.size()) {
                    channel.truncate(truncateTo);
                }
                channel.close();
            } catch (IOException e) {
                throw new DBException("volume close failed", e);
            }
        }
    }
}
