package com.troupmar.graphaware.handlers;

import com.esotericsoftware.minlog.Log;
import net.lingala.zip4j.core.ZipFile;
import org.junit.rules.TemporaryFolder;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.File;
import java.io.IOException;
import java.util.Stack;

import static com.graphaware.common.util.DatabaseUtils.registerShutdownHook;

/**
 * Created by Martin on 06.04.15.
 */
public class Database {

    // Example values that can be used.
    // triangle DB
    //public static final String DB_PATH       = "testDb/graph100-500.db";
    // movie DB
    //public static final String DB_PATH       = "testDb/cineasts_12k_movies_50k_actors.db";
    // transaction DB
    public static final String DB_PATH       = "testDb/transactions10k-100k.db";
    public static final String DB_ZIP_PATH   = DB_PATH + ".zip";


    public static GraphDatabaseService loadDatabaseFromFile(String dbPath, String propertiesFile) {
        String databasePath = new File(dbPath).getAbsolutePath();
        return createDatabase(databasePath, propertiesFile);
    }

    public static GraphDatabaseService loadDatabaseFromZipFile(String zipDbPath, String propertiesFile) {
        String targetDir = zipDbPath.substring(0, zipDbPath.lastIndexOf("/"));
        String databasePath = new File(unzipDatabase(targetDir, zipDbPath)).getAbsolutePath();
        String databaseFile = new File(zipDbPath).getName().replace(".zip", "");
        return createDatabase(databasePath + "/" + databaseFile, propertiesFile);
    }

    public static GraphDatabaseService loadTemporaryDatabaseFromZipFile(String zipDbPath, TemporaryFolder temporaryFolder, String propertiesFile) {
        //createTemporaryFolder();
        String databasePath = unzipDatabase(temporaryFolder.getRoot().getAbsolutePath(), zipDbPath);
        String databaseFile = new File(zipDbPath).getName().replace(".zip", "");
        return createDatabase(databasePath + "/" + databaseFile, propertiesFile);
    }

    private static GraphDatabaseService createDatabase(String databaseTargetPath, String propertiesFile) {
        GraphDatabaseBuilder graphDatabaseBuilder = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(databaseTargetPath);

        if (propertiesFile != null) {
            graphDatabaseBuilder = graphDatabaseBuilder.loadPropertiesFromFile(propertiesFile);
        }

        GraphDatabaseService database = graphDatabaseBuilder.newGraphDatabase();
        registerShutdownHook(database);

        return database;
    }

    private static String unzipDatabase(String targetLocation, String zipLocation) {

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
    public static TemporaryFolder getNewTemporaryFolder() {
        TemporaryFolder temporaryFolder = new TemporaryFolder();
        try {
            temporaryFolder.create();
            temporaryFolder.getRoot().deleteOnExit();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return temporaryFolder;
    }

    /**
     * Remove temporary folder.
     */
    public static void removeTemporaryFolder(TemporaryFolder temporaryFolder) {
        if (temporaryFolder != null) {
            temporaryFolder.delete();
        }
    }

    /**
     * Close database.
     */
    public static void closeDatabase(GraphDatabaseService database, TemporaryFolder temporaryFolder) {
        if (database != null) {
            Log.info("Closing database...");
            database.shutdown();
            removeTemporaryFolder(temporaryFolder);
        }
    }

    /**
     * Method to return total number of dbHits for given result of query.
     * @param result based on query
     * @return total number of dbHits
     */
    public static long getTotalDbHits(Result result) {
        long totalDbHits = 0;
        Stack<ExecutionPlanDescription> stack = new Stack<>();
        stack.push(result.getExecutionPlanDescription());
        while (! stack.empty()) {
            ExecutionPlanDescription planFromStack = stack.pop();
            totalDbHits += planFromStack.getProfilerStatistics().getDbHits();
            System.out.println(planFromStack.getName() + " " + planFromStack.getProfilerStatistics().getDbHits());
            for (ExecutionPlanDescription childPlan : planFromStack.getChildren()) {
                stack.push(childPlan);
            }
        }
        return totalDbHits;
    }

}
