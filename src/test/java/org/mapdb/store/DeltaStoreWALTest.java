package org.mapdb.store;

import org.junit.After;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class DeltaStoreWALTest extends DeltaTCK {

    private final List<File> files = new ArrayList<>();

    @Override protected StoreDelta createStore() {
        try {
            File f = Files.createTempFile("mapdb-wal-delta", ".wal").toFile();
            f.delete();
            files.add(f);
            return new StoreWAL(f);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @After public void deleteFiles() {
        for (File f : files) f.delete();
        files.clear();
    }
}
