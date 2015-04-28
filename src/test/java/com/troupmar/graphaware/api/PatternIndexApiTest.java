package com.troupmar.graphaware.api;

import com.graphaware.test.integration.GraphAwareApiTest;
import com.troupmar.graphaware.PatternIndexModel;
import com.troupmar.graphaware.handlers.Database;
import org.junit.rules.TemporaryFolder;
import org.neo4j.graphdb.GraphDatabaseService;

import static com.graphaware.runtime.RuntimeRegistry.getRuntime;

/**
 * Created by Martin on 27.04.15.
 */
public class PatternIndexApiTest extends GraphAwareApiTest {
    protected TemporaryFolder temporaryFolder;

    protected String getDatabaseZip() {
        return "testDb/graph100-120.db.zip";
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        getRuntime(getDatabase()).waitUntilStarted();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        Database.removeTemporaryFolder(temporaryFolder);
    }

    @Override
    protected GraphDatabaseService createDatabase() {
        temporaryFolder = Database.getNewTemporaryFolder();
        GraphDatabaseService database = Database.loadTemporaryDatabaseFromZipFile(getDatabaseZip(), temporaryFolder, propertiesFile());

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

    @Override
    public String baseUrl() {
        return super.baseUrl() + "/pattern-index";
    }
}
