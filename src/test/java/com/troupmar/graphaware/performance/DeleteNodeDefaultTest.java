package com.troupmar.graphaware.performance;

import com.esotericsoftware.minlog.Log;
import com.graphaware.test.performance.*;
import com.graphaware.test.util.TestUtils;
import com.troupmar.graphaware.CypherQuery;
import com.troupmar.graphaware.PatternIndex;
import com.troupmar.graphaware.PatternIndexModel;
import com.troupmar.graphaware.PatternQuery;
import com.troupmar.graphaware.exception.InvalidCypherException;
import com.troupmar.graphaware.exception.InvalidCypherMatchException;
import com.troupmar.graphaware.exception.PatternIndexAlreadyExistsException;
import com.troupmar.graphaware.exception.PatternIndexNotFoundException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;

import java.util.*;

public class DeleteNodeDefaultTest implements PerformanceTest {

    private PatternIndexModel model;
    private final String GRAPH_SIZE = "10000-50000";

    /**
     * {@inheritDoc}
     */
    @Override
    public String shortName() {
        // triangle
        //return "DeleteTriangleNodeDefaultTest";
        // movie pattern
        return "DeleteMoviePatternNodeDefaultTest";
        // transaction pattern
        //return "DeleteTransactionPatternNodeDefaultTest";
    }

    @Override
    public String longName() {
        // triangle
        //return "Delete node from triangle pattern by default.";
        // movie pattern
        return "Delete node from movie pattern by default.";
        // transaction pattern
        //return "Delete node from transaction pattern by default.";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Parameter> parameters() {
        List<Parameter> result = new LinkedList<>();
        //result.add(new CacheParameter("cache")); //no cache, low-level cache, high-level cache
        //result.add(new ObjectParameter("cache", new NoCache()));
        //result.add(new ObjectParameter("cache", new LowLevelCache()));
        //result.add(new ObjectParameter("cache", new HighLevelCache()));
        result.add(new ObjectParameter("cache", new HighLevelCache(), new LowLevelCache(), new NoCache())); //low-level cache, high-level cache

        return result;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public int dryRuns(Map<String, Object> params) {
        return ((CacheConfiguration) params.get("cache")).needsWarmup() ? 30 : 3;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int measuredRuns() {
        return 10;
    }


    @Override
    public Map<String, String> databaseParameters(Map<String, Object> params) {
        Map<String, String> config = ((CacheConfiguration) params.get("cache")).addToConfig(Collections.<String, String>emptyMap());
        return config;
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public void prepareDatabase(GraphDatabaseService database, final Map<String, Object> params) {
        Log.info("Loading database...");
    }

    @Override
    public String getExistingDatabasePath() {
        // triangle
        //return "testDb/graph" + GRAPH_SIZE + ".db.zip";
        // movie pattern
        return "testDb/cineasts_12k_movies_50k_actors.db.zip";
        // transaction pattern
        //return "testDb/transactions10k-100k.db.zip";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RebuildDatabase rebuildDatabase() {
        return RebuildDatabase.AFTER_EVERY_RUN;
    }

    @Override
    public long run(final GraphDatabaseService database, Map<String, Object> params) {
        long time = 0;
        Result result = database.execute("MATCH (n) RETURN COUNT(n)");
        System.out.println(result.resultAsString());
        result = database.execute("MATCH (n)-[r]->() RETURN COUNT(r)");
        System.out.println(result.resultAsString());

        time += TestUtils.time(new TestUtils.Timed() {
            @Override
            public void time() {
                // triangle
                //database.execute("MATCH (n {name: 'Alisha Barnes'})-[r]-() DELETE r, n");
                // movie pattern
                database.execute("MATCH (n {login:'adilfulara'})-[r]-() DELETE r,n");
                // transaction pattern
                //database.execute("MATCH (n {id:24})-[r]-() DELETE r,n");
            }
        });

        result = database.execute("MATCH (n) RETURN COUNT(n)");
        System.out.println(result.resultAsString());
        result = database.execute("MATCH (n)-[r]->() RETURN COUNT(r)");
        System.out.println(result.resultAsString());


        return time;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean rebuildDatabase(Map<String, Object> params) {
        throw new UnsupportedOperationException("never needed, database rebuilt after every param change");
    }
}
