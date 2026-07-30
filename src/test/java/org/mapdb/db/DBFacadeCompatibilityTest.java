package org.mapdb.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class DBFacadeCompatibilityTest {
    @Test public void numericAtomicsAreNumbers() {
        DB db = DBMaker.memoryDB().make();
        Atomic.Long value = db.atomicLong("long", 42).create();
        Atomic.Integer integer = db.atomicInteger("int", 7).create();
        assertTrue(value instanceof Number);
        assertTrue(integer instanceof Number);
        assertEquals(42, value.intValue());
        assertEquals(7L, integer.longValue());
        db.compact();
        db.close();
    }

    @SuppressWarnings("deprecation")
    @Test public void legacyMakerAliasesCompose() {
        DB db = DBMaker.memoryDB().executorEnable().cleanerHackEnable()
                .fileChannelEnable().checksumStoreEnable().make();
        db.close();
    }
}
