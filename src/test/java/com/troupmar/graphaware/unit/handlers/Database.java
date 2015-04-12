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

    public Database(String dbPath, String dbLoadProcess) {
        if (dbLoadProcess.equals("zip-still")) {
            loadDatabaseFromZipFile(dbPath);
        } else if (dbLoadProcess.equals("zip-tmp")) {
            loadTemporaryDatabaseFromZipFile(dbPath);
        } else if (dbLoadProcess.equals("still")) {
            loadDatabaseFromFile(dbPath);
        }
    }

    private void loadDatabaseFromFile(String dbPath) {
        String databasePath = new File(dbPath).getAbsolutePath();
        createDatabase(databasePath);
    }

    private void loadDatabaseFromZipFile(String zipDbPath) {
        String databasePath = new File(unzipDatabase("data", zipDbPath)).getAbsolutePath();
        String databaseFile = new File(zipDbPath).getName().replace(".zip", "");
        createDatabase(databasePath + "/" + databaseFile);
    }

    private void loadTemporaryDatabaseFromZipFile(String zipDbPath) {
        createTemporaryFolder();
        String databasePath = unzipDatabase(temporaryFolder, zipDbPath);
        String databaseFile = new File(zipDbPath).getName().replace(".zip", "");
        createDatabase(databasePath + "/" + databaseFile);
    }

    private void createDatabase(String databaseTargetPath) {
        GraphDatabaseBuilder graphDatabaseBuilder = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(databaseTargetPath);
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

    private String unzipDatabase(String targetLocation, String zipLocation) {

        try {
            ZipFile zipFile = new ZipFile(zipLocation);
            zipFile.extractAll(targetLocation);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return targetLocation;
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
            if (temporaryFolder != null) {
                temporaryFolder.delete();
            }
            database = null;
        }
    }

    public GraphDatabaseService getDatabase() {
        return this.database;
    }
}
