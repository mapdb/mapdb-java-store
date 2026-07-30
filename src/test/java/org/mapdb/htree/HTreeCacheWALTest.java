package org.mapdb.htree;

import org.mapdb.TmpFiles;
import org.mapdb.store.Store;
import org.mapdb.store.StoreWAL;

import java.io.File;
import java.io.IOException;

public class HTreeCacheWALTest extends HTreeCacheTCK {

    private File file;

    @Override
    protected Store openStore() {
        try {
            file = TmpFiles.tempFile("htree-cache-wal-tck", ".wal");
            file.delete(); // StoreWAL will (re)create it
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new StoreWAL(file);
    }

    @Override
    protected void cleanup() {
        TmpFiles.delete(file);
    }
}
