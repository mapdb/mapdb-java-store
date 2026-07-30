package org.mapdb.store;

import org.junit.After;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/** Full Store TCK over the file-backed (durable) StoreDirect volume. */
public class StoreDirectFileTest extends StoreTCK {

    private final List<File> files = new ArrayList<>();

    @Override protected Store createStore() {
        try {
            File f = Files.createTempFile("mapdb-direct", ".db").toFile();
            f.delete();
            files.add(f);
            return new StoreDirect(f);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @After public void deleteFiles() {
        for (File f : files) f.delete();
        files.clear();
    }
}
