package com.troupmar.graphaware.handlers;

import com.esotericsoftware.minlog.Log;
import com.graphaware.test.integration.DatabaseIntegrationTest;
import com.troupmar.graphaware.PatternIndexModel;
import com.troupmar.graphaware.handlers.Database;
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
        return "testDb/graph100-120.db.zip";
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        PatternIndexModel.destroy();
        super.tearDown();
        Database.removeTemporaryFolder(temporaryFolder);
    }

    /**
     * Instantiate a database by loading it from zip file.
     * @return new database.
     */
    protected GraphDatabaseService createDatabase() {
        temporaryFolder = Database.getNewTemporaryFolder();
        return Database.loadTemporaryDatabaseFromZipFile(getDatabaseZip(), temporaryFolder, propertiesFile());
    }

    @Override
    protected String propertiesFile() {
        if (this.getClass().getClassLoader().getResource("neo4j-pattern-index.properties") != null) {
            return this.getClass().getClassLoader().getResource("neo4j-pattern-index.properties").getPath();
        } else {
            return null;
        }
    }

}