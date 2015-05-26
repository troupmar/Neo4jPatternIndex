package com.troupmar.graphaware.performance;

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
import org.neo4j.graphdb.Transaction;

import java.util.*;

public class ChangeNodeDefaultTest implements PerformanceTest {
    private PatternIndexModel model;
    private final String GRAPH_SIZE = "10000-50000";


    /**
     * {@inheritDoc}
     */
    @Override
    public String shortName() {
        // triangle
        return "ChangeTriangleNodeDefaultTest";
        // movie pattern
        //return "ChangeMoviePatternNodeDefaultTest";
        // transaction pattern
        //return "ChangeTransactionPatternNodeDefaultTest";
    }

    @Override
    public String longName() {
        // triangle
        return "Change node's property in triangle pattern by default.";
        // movie pattern
        //return "Change node's property in movie pattern by default.";
        // transaction pattern
        //return "Change node's property in transaction pattern by default.";
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
        return ((CacheConfiguration) params.get("cache")).needsWarmup() ? 20 : 2;
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

        PatternIndexModel.destroy();
        model = PatternIndexModel.getInstance(database);
    }

    @Override
    public String getExistingDatabasePath() {
        // triangle
        return "testDb/graph" + GRAPH_SIZE + ".db.zip";
        // movie pattern
        //return "testDb/cineasts_12k_movies_50k_actors-person-label-indexed.db.zip";
        // transaction pattern
        //return "testDb/transactions10k-100k-cart-label-indexed.db.zip";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RebuildDatabase rebuildDatabase() {
        return RebuildDatabase.AFTER_PARAM_CHANGE;
    }

    @Override
    public long run(final GraphDatabaseService database, Map<String, Object> params) {
        long time = 0;

        // triangle + movie pattern
        Result result = database.execute("MATCH (n:Person) RETURN COUNT (n)");
        // transaction pattern
        //Result result = database.execute("MATCH (n:Cart) RETURN COUNT (n)");
        System.out.println(result.resultAsString());

        time += TestUtils.time(new TestUtils.Timed() {
            @Override
            public void time() {
                // triangle
                database.execute("MATCH (n:Person {name:'Aaliyah Holmes'}) REMOVE n:Person SET n:Animal");
                // movie pattern
                //database.execute("MATCH (n:Person {login:'adilfulara'}) REMOVE n:Person SET n:Animal");
                // transaction pattern
                //database.execute("MATCH (n:Cart {number:'1004 3313'}) REMOVE n:Cart SET n:NoCart");

            }
        });

        // triangle + movie pattern
        result = database.execute("MATCH (n:Person) RETURN COUNT (n)");
        // transaction pattern
        //result = database.execute("MATCH (n:Cart) RETURN COUNT (n)");
        System.out.println(result.resultAsString());
        // triangle
        database.execute("MATCH (n:Animal {name:'Aaliyah Holmes'}) REMOVE n:Animal SET n:Person");
        // movie pattern
        //database.execute("MATCH (n:Animal {login:'adilfulara'}) REMOVE n:Animal SET n:Person");
        // transaction pattern
        //database.execute("MATCH (n:NoCart {number:'1004 3313'}) REMOVE n:NoCart SET n:Cart");

        // triangle + movie pattern
        result = database.execute("MATCH (n:Person) RETURN COUNT (n)");
        // transaction pattern
        //result = database.execute("MATCH (n:Cart) RETURN COUNT (n)");
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
