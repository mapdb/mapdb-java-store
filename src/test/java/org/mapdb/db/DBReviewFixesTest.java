package org.mapdb.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.mapdb.DBException;
import org.mapdb.btree.BTreeMap;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;
import org.mapdb.ser.LongFormat;
import org.mapdb.ser.ObjectArrayFormat;
import org.mapdb.ser.Serializer;
import org.mapdb.ser.Serializers;
import org.mapdb.ser.StringGroupFormat;
import org.mapdb.store.StoreDirect;

/** Regression tests for correctness fixes found in review. */
public class DBReviewFixesTest {

    // HIGH: a cache hit must still enforce the custom-codec re-supply contract.
    @Test public void cacheHitRequiresCustomCodecReSupply() {
        DB db = DBMaker.memoryDB().make();
        Serializer<String> customSerializer = new Serializer<>() {
            @Override public void serialize(DataOutput2 out, String value) {
                Serializers.STRING.serialize(out, value);
            }
            @Override public String deserialize(DataInput2 in, int size) {
                return Serializers.STRING.deserialize(in, size);
            }
        };
        ObjectArrayFormat<String> custom = new ObjectArrayFormat<>(customSerializer);
        db.treeMap("t", LongFormat.INSTANCE, custom).create().put(1L, "x");
        // second maker, no custom value format supplied -> must throw even though cached
        assertThrows(DBException.WrongConfiguration.class,
                () -> db.<Long, String>treeMap("t").keySerializer(LongFormat.INSTANCE).createOrOpen());
        db.close();
    }

    // HIGH: a cache hit with a mismatched built-in codec must fail fast, not silently
    // return the first handle.
    @Test public void cacheHitRejectsWrongBuiltinCodec() {
        DB db = DBMaker.memoryDB().make();
        db.treeMap("t", LongFormat.INSTANCE, StringGroupFormat.INSTANCE).create().put(1L, "x");
        // wrong value format on reopen of the cached instance
        assertThrows(DBException.WrongConfiguration.class,
                () -> db.treeMap("t", LongFormat.INSTANCE, LongFormat.INSTANCE).createOrOpen());
        db.close();
    }

    // HIGH: open() (no cache) also rejects a mismatched built-in codec.
    @Test public void openRejectsWrongBuiltinCodec() {
        DB db = DBMaker.memoryDB().make();
        db.hashMap("h", Serializers.STRING, Serializers.LONG).create();
        assertThrows(DBException.WrongConfiguration.class,
                () -> db.hashMap("h", Serializers.STRING, Serializers.STRING).open());
        db.close();
    }

    // HIGH: fail-closed on a store polluted at recid 1 by a non-catalog writer.
    @Test public void nonDbStoreFailsClosed() {
        StoreDirect raw = new StoreDirect(false, true);
        raw.put(12345L, Serializers.LONG); // occupies recid 1 with junk
        assertThrows(DBException.class, () -> new DB(raw, true));
        raw.close();
    }

    // CRITICAL fix: delete unlinks first; recreating the name afterwards works cleanly.
    @Test public void deleteUnlinksThenRecreate() {
        DB db = DBMaker.memoryDB().make();
        BTreeMap<Long, String> m = db.treeMap("t", LongFormat.INSTANCE, StringGroupFormat.INSTANCE).create();
        m.put(1L, "a");
        assertTrue(db.delete("t"));
        assertFalse(db.exists("t"));
        // recreate with fresh data
        BTreeMap<Long, String> m2 = db.treeMap("t", LongFormat.INSTANCE, StringGroupFormat.INSTANCE).create();
        assertEquals(0, m2.size());
        m2.put(2L, "b");
        assertEquals("b", m2.get(2L));
        db.close();
    }

    // Re-review fix: supplying a custom hasher to reopen a DEFAULT-hasher map must fail
    // (both on cache hit and via open), not silently ignore the mismatch.
    @Test public void defaultHasherRejectsSuppliedCustomHasher() {
        DB db = DBMaker.memoryDB().make();
        db.hashMap("h", Serializers.STRING, Serializers.LONG).create(); // default hasher, cached
        assertThrows(DBException.WrongConfiguration.class,
                () -> db.hashMap("h", Serializers.STRING, Serializers.LONG)
                        .hasher(org.mapdb.hash.Hashers.mixing(String::hashCode))
                        .createOrOpen());
        db.close();
    }

    // Re-review fix: a garbage/truncated record at recid 1 must fail cleanly as a DBException,
    // never crash with an unrelated runtime error or scan into adjacent store bytes.
    @Test public void corruptRecid1FailsClosed() {
        StoreDirect raw = new StoreDirect(false, true);
        byte[] junk = new byte[]{0x4D, 0x44, 0x42, 0x43, 0, 0, 0, 1, 0, 0x7F, 0x7F, 0x7F};
        raw.put(junk, Serializers.BYTE_ARRAY);
        assertThrows(DBException.class, () -> new DB(raw, true));
        raw.close();
    }

    // MEDIUM fix: transactionEnable is rejected for in-memory factories, not ignored.
    @Test public void transactionEnableRejectedForHeap() {
        assertThrows(DBException.WrongConfiguration.class,
                () -> DBMaker.heapDB().transactionEnable().make());
    }
}
