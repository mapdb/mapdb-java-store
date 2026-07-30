package org.mapdb.store;

import org.mapdb.io.DataOutput2;
import org.mapdb.ser.Serializer;

/**
 * Delta capability: in-place record growth with capacity refusal.
 * Record content model: content = base bytes ++ appended bytes, opaque to the store.
 * Implemented by byte-backed stores only.
 */
public interface StoreDelta extends Store {

    /** Returned by append when the record's remaining capacity is insufficient. */
    long REFUSED = -1L;

    /**
     * Extend record content. Returns new total content size, or {@link #REFUSED}.
     * Appending to a preallocated/null record is legal and establishes the record
     * (content = deltas only). Never throws for capacity; throws GetVoid per the record-state contract.
     */
    long append(long recid, byte[] data, int offset, int len);

    /**
     * Encodes one or more framed delta entries, all stamped with the store-supplied LSN
     * (R2). Must be PURE and DETERMINISTIC: the store
     * may encode into a scratch buffer to size the append, and an exception thrown here
     * must leave nothing staged.
     */
    interface DeltaEncoder {
        void encode(DataOutput2 out, long lsn);
    }

    /**
     * Encode-and-append: the STORE supplies the LSN value and the FORMAT writes it while
     * encoding. This is the seam R1 declared and left unwired
     * ({@code BufferedPageFormat.AppendContext}); it exists because the store must never
     * patch an LSN into a fixed offset inside opaque format bytes (that would break P1 —
     * see the R1 design's validation banner).
     *
     * <p>The default is the non-transactional adapter: no durable order to export, so the
     * encoder is stamped with {@code 0} and the bytes go through the ordinary append. Only
     * a transactional store overrides this to mint a real LSN.
     *
     * @return new total content size, or {@link #REFUSED}
     */
    default long append(long recid, DeltaEncoder enc) {
        DataOutput2 out = new DataOutput2(64);
        enc.encode(out, 0L);
        return append(recid, out.buf, 0, out.pos);
    }

    /** Capacity hint; may be stale — append() is authoritative. */
    long capacityRemaining(long recid);

    /** update() that provisions at least {@code headroom} appendable bytes. */
    <R> void updateWithHeadroom(long recid, R record, Serializer<R> serializer, int headroom);
}
