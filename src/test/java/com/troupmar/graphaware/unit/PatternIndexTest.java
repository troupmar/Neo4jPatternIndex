package com.troupmar.graphaware.unit;

import com.esotericsoftware.minlog.Log;
import com.graphaware.test.integration.DatabaseIntegrationTest;
import com.troupmar.graphaware.PatternIndexModel;
import net.lingala.zip4j.core.ZipFile;
import org.junit.rules.TemporaryFolder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.File;
import java.io.IOException;

import static com.graphaware.common.util.DatabaseUtils.registerShutdownHook;
import static com.graphaware.runtime.RuntimeRegistry.getRuntime;

/**
 * Created by Martin on 27.04.15.
 */


public class PatternIndexTest extends DatabaseIntegrationTest {

    protected TemporaryFolder temporaryFolder;

    protected String getDatabaseZip() {
        return "testDb/graph1000-5000.db.zip";
    }


    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        closeDatabase();
        PatternIndexModel.destroy();
    }

    /**
     * Instantiate a database by loading it from zip file.
     * @return new database.
     */
    protected GraphDatabaseService createDatabase() {
        createTemporaryFolder();

        //Neo4j database
        String databaseFolderName = new File(getDatabaseZip()).getName();
        databaseFolderName = databaseFolderName.replace(".zip", "");
        GraphDatabaseBuilder graphDatabaseBuilder = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(unzipDatabase(temporaryFolder, getDatabaseZip()) + "/" + databaseFolderName);


        if (propertiesFile() != null) {
            graphDatabaseBuilder = graphDatabaseBuilder.loadPropertiesFromFile(propertiesFile());
        }

        GraphDatabaseService database = graphDatabaseBuilder.newGraphDatabase();
        getRuntime(database).waitUntilStarted();
        registerShutdownHook(database);

        return database;
    }

    @Override
    protected String propertiesFile() {
        if (this.getClass().getClassLoader().getResource("neo4j-pattern-index.properties") != null) {
            return this.getClass().getClassLoader().getResource("neo4j-pattern-index.properties").getPath();
        } else {
            return null;
        }
    }

    protected void createTemporaryFolder() {
        temporaryFolder = new TemporaryFolder();
        try {
            temporaryFolder.create();
            temporaryFolder.getRoot().deleteOnExit();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected String unzipDatabase(TemporaryFolder tmp, String zipLocation) {
        try {
            ZipFile zipFile = new ZipFile(zipLocation);
            zipFile.extractAll(tmp.getRoot().getAbsolutePath());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return tmp.getRoot().getAbsolutePath();
    }

    /**
     * Close the database and delete its data in temporary folder.
     */
    public void closeDatabase() {
        if (getDatabase() != null) {
            Log.info("Closing database...");
            getDatabase().shutdown();
            if (temporaryFolder != null) {
                temporaryFolder.delete();
            }
        }
    }

}