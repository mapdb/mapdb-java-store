package org.mapdb.store;

import org.mapdb.TmpFiles;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Full Store TCK over the file-backed (durable) StoreDirect volume. */
public class StoreDirectFileTest extends StoreTCK {

    private final List<File> files = new ArrayList<>();

    @Override protected Store createStore() {
        try {
            File f = TmpFiles.tempFile("mapdb-direct", ".db");
            f.delete();
            files.add(f);
            return new StoreDirect(f);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override protected void cleanup() {
        for (File f : files) TmpFiles.delete(f);
        files.clear();
    }
}
