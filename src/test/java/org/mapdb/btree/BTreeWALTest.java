package org.mapdb.btree;

import org.mapdb.store.Store;
import org.mapdb.store.StoreWAL;

import java.io.File;
import java.io.IOException;

public class BTreeWALTest extends BTreeMapTCK {

    private File file;

    @Override
    protected Store openStore() {
        try {
            file = File.createTempFile("btree-wal-tck", ".wal");
            file.delete(); // StoreWAL will (re)create it
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new StoreWAL(file);
    }

    @Override
    protected void cleanup() {
        if (file != null) file.delete();
    }
}
