package org.mapdb.store;

import org.mapdb.TmpFiles;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class StoreWALTest extends StoreTCK {

    private final List<File> files = new ArrayList<>();

    @Override protected Store createStore() {
        try {
            File f = TmpFiles.tempFile("mapdb-wal-tck", ".wal");
            f.delete(); // start from a non-existent WAL so each store is empty
            files.add(f);
            return new StoreWAL(f);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** WAL frees deleted recids only at commit; a subsequent put need not reuse immediately. */
    @Override protected boolean reusesRecidsImmediately() { return false; }

    @Override protected void cleanup() {
        for (File f : files) TmpFiles.delete(f);
        files.clear();
    }
}
