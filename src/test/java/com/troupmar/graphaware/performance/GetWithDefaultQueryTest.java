package com.troupmar.graphaware.performance;

import com.graphaware.test.performance.*;
import com.graphaware.test.util.TestUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class GetWithDefaultQueryTest implements PerformanceTest {

    private final String GRAPH_SIZE = "100000-500000";


    /**
     * {@inheritDoc}
     */
    @Override
    public String shortName() {
        // triangle
        return "GetTrianglesByDefaultQuery (" + GRAPH_SIZE + ")";
        // movie pattern
        //return "GetMoviePatternsWithDefaultQuery";
        // transaction pattern
        //return "GetTransactionPatternsWithDefaultQuery";
    }

    @Override
    public String longName() {
        // triangle
        return "Cypher query to get all triangles.";
        // movie pattern
        //return "Cypher query to get all movie patterns.";
        // transaction pattern
        //return "Cypher query to get all transaction patterns.";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Parameter> parameters() {
        List<Parameter> result = new LinkedList<>();
        //result.add(new CacheParameter("cache")); //no cache, low-level cache, high-level cache
        result.add(new ObjectParameter("cache", new HighLevelCache(), new LowLevelCache(), new NoCache())); //low-level cache, high-level cache
        //result.add(new ObjectParameter("cache", new NoCache()));
        //result.add(new ObjectParameter("cache", new LowLevelCache()));
        //result.add(new ObjectParameter("cache", new HighLevelCache()));
        return result;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public int dryRuns(Map<String, Object> params) {
        return ((CacheConfiguration) params.get("cache")).needsWarmup() ? 5 : 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int measuredRuns() {
        return 1;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, String> databaseParameters(Map<String, Object> params) {
        return ((CacheConfiguration) params.get("cache")).addToConfig(Collections.<String, String>emptyMap());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void prepareDatabase(GraphDatabaseService database, final Map<String, Object> params) {

    }

    @Override
    public String getExistingDatabasePath() {
        // triangle
        return "testDb/graph" + GRAPH_SIZE + ".db.zip";
        // movie pattern
        //return "testDb/cineasts_12k_movies_50k_actors.db.zip";
        // transaction pattern
        //return "testDb/transactions.db.zip";
        //return "testDb/transactions10k-100k.db.zip";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RebuildDatabase rebuildDatabase() {
        return RebuildDatabase.AFTER_PARAM_CHANGE;
    }

    /**
     * {@inheritDoc}
     */
    /**
     * {@inheritDoc}
     */
    @Override
    public long run(final GraphDatabaseService database, Map<String, Object> params) {
        long time = 0;

        time += TestUtils.time(new TestUtils.Timed() {
            @Override
            public void time() {
                // triangle
                Result result = database.execute("MATCH (a)--(b)--(c)--(a) RETURN id(a),id(b),id(c)");
                // movie pattern
                //Result result = database.execute("MATCH (a)--(b)--(c)--(a)--(e) RETURN a, b, c, e");
                // transaction pattern
                //Result result = database.execute("MATCH (a)--(b)--(c)--(d)--(e)--(c)--(a)--(d)--(b)--(e)--(a) RETURN a,b,c,d,e");
                //Result result = database.execute("MATCH (a)-[e]-(b)-[f]-(c)-[g]-(a)-[h]-(d)-[i]-(b) RETURN a, b, c, d");

                while (result.hasNext()) {
                    result.next();
                }
            }
        });

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
