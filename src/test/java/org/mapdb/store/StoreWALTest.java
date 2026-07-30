package org.mapdb.store;

import org.junit.After;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class StoreWALTest extends StoreTCK {

    private final List<File> files = new ArrayList<>();

    @Override protected Store createStore() {
        try {
            File f = Files.createTempFile("mapdb-wal-tck", ".wal").toFile();
            f.delete(); // start from a non-existent WAL so each store is empty
            files.add(f);
            return new StoreWAL(f);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** WAL frees deleted recids only at commit; a subsequent put need not reuse immediately. */
    @Override protected boolean reusesRecidsImmediately() { return false; }

    @After public void deleteFiles() {
        for (File f : files) f.delete();
        files.clear();
    }
}
