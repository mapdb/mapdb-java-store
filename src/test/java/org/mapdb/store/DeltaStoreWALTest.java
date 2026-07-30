package org.mapdb.store;

import org.mapdb.TmpFiles;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DeltaStoreWALTest extends DeltaTCK {

    private final List<File> files = new ArrayList<>();

    @Override protected StoreDelta createStore() {
        try {
            File f = TmpFiles.tempFile("mapdb-wal-delta", ".wal");
            f.delete();
            files.add(f);
            return new StoreWAL(f);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override protected void cleanup() {
        for (File f : files) TmpFiles.delete(f);
        files.clear();
    }
}
