package org.mapdb.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.File;

import org.junit.Test;
import org.mapdb.btree.BTreeMap;
import org.mapdb.ser.LongFormat;
import org.mapdb.ser.StringGroupFormat;

public class DBTreeExternalValuesTest {
    @Test public void catalogPersistence() throws Exception {
        File file = File.createTempFile("mapdb5-tree-external", ".db");
        file.delete();
        try {
            DB db = DBMaker.fileDB(file).transactionEnable().make();
            BTreeMap<Long, String> map = db.treeMap("tree",
                    LongFormat.INSTANCE, StringGroupFormat.INSTANCE)
                    .valuesOutsideNodesEnable().counterEnable().create();
            map.put(1L, "one");
            map.put(2L, "two");
            assertFalse(map.valueInline());
            db.commit();
            db.close();

            DB reopened = DBMaker.fileDB(file).transactionEnable().make();
            @SuppressWarnings("unchecked")
            BTreeMap<Long, String> map2 = (BTreeMap<Long, String>) reopened.get("tree");
            assertFalse(map2.valueInline());
            assertEquals("one", map2.get(1L));
            assertEquals(2L, map2.sizeLong());
            reopened.close();
        } finally {
            file.delete();
            org.mapdb.store.WalTestKit.deleteStore(file);
        }
    }
}
