package org.mapdb;

/** Runtime exceptions thrown by mapdb5. Types are part of the Store TCK contract. */
public class DBException extends RuntimeException {

    public DBException(String msg) { super(msg); }

    public DBException(String msg, Throwable cause) { super(msg, cause); }

    /** Recid does not exist (never allocated, or deleted). */
    public static class GetVoid extends DBException {
        public GetVoid(long recid) { super("Record does not exist, recid=" + recid); }
    }

    /** Record exceeds the maximum plain-record capacity. */
    public static class RecordTooLarge extends DBException {
        public RecordTooLarge(long size) { super("Record too large: " + size + " bytes"); }
    }

    /** Store bytes failed a checksum/parity/structure check. */
    public static class DataCorruption extends DBException {
        public DataCorruption(String msg) { super(msg); }
    }

    /** Store's addressable data area is exhausted (e.g. append-only store past its packed-offset limit). */
    public static class StoreFull extends DBException {
        public StoreFull(String msg) { super(msg); }
    }

    /** Operation on a closed store. */
    public static class StoreClosed extends DBException {
        public StoreClosed() { super("Store was closed"); }
    }

    /** Store verify() found an invariant violation. */
    public static class VerifyFailed extends DBException {
        public VerifyFailed(String msg) { super("verify failed: " + msg); }
    }

    /** Bulk-load source violated the strictly-ascending key contract (mapdb3 Pump lineage). */
    public static class NotSorted extends DBException {
        public NotSorted(String msg) { super(msg); }
    }

    /** Invalid or unsupported configuration passed to the DB/DBMaker facade. */
    public static class WrongConfiguration extends DBException {
        public WrongConfiguration(String msg) { super(msg); }
        public WrongConfiguration(String msg, Throwable cause) { super(msg, cause); }
    }
}
