package com.troupmar.graphaware.unit.handlers;

import com.esotericsoftware.minlog.Log;
import net.lingala.zip4j.core.ZipFile;
import org.junit.rules.TemporaryFolder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.File;
import java.io.IOException;

/**
 * Created by Martin on 06.04.15.
 */
public class Database {
    private GraphDatabaseService database;
    private TemporaryFolder temporaryFolder;

    public Database(String dbOriginPath) {
        loadDatabaseFromFile(dbOriginPath);
    }

    private void loadDatabaseFromFile(String dbOriginPath) {
        createTemporaryFolder();
        String databasePath = unzipDatabase(temporaryFolder, dbOriginPath);
        String databaseFile = new File(dbOriginPath).getName();
        databaseFile = databaseFile.replace(".zip", "");
        GraphDatabaseBuilder graphDatabaseBuilder = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(databasePath + "/" + databaseFile);
        database = graphDatabaseBuilder.newGraphDatabase();
    }

    private String unzipDatabase(TemporaryFolder tmp, String zipLocation) {
        try {
            ZipFile zipFile = new ZipFile(zipLocation);
            zipFile.extractAll(tmp.getRoot().getAbsolutePath());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return tmp.getRoot().getAbsolutePath();
    }

    /**
     * Create a temporary folder.
     */
    private void createTemporaryFolder() {
        temporaryFolder = new TemporaryFolder();
        try {
            temporaryFolder.create();
            temporaryFolder.getRoot().deleteOnExit();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Close the database and delete its data.
     */
    public void closeDatabase() {
        if (database != null) {
            Log.info("Closing database...");

            database.shutdown();
            temporaryFolder.delete();
            database = null;
        }
    }

    public GraphDatabaseService getDatabase() {
        return this.database;
    }
}
