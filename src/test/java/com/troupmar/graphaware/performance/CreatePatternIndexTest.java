package com.troupmar.graphaware.performance;

import com.graphaware.test.performance.*;
import com.graphaware.test.util.TestUtils;
import com.troupmar.graphaware.CypherQuery;
import com.troupmar.graphaware.PatternIndexModel;
import com.troupmar.graphaware.PatternQuery;
import com.troupmar.graphaware.exception.InvalidCypherException;
import com.troupmar.graphaware.exception.InvalidCypherMatchException;
import com.troupmar.graphaware.exception.PatternIndexAlreadyExistsException;
import com.troupmar.graphaware.exception.PatternIndexNotFoundException;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.*;

public class CreatePatternIndexTest implements PerformanceTest {
    // triangle
    //private final String pattern = "(a)-[r]-(b)-[p]-(c)-[q]-(a)";
    //private final String indexName = "triangle-index";
    //private final String GRAPH_SIZE = "10000-50000";
    // movie pattern
    //private final String pattern = "(a)-[r]-(b)-[s]-(c)-[t]-(a)-[u]-(e)";
    //private final String indexName = "movie-index";
    // transaction pattern
    private final String pattern = "(a)-[e]-(b)-[f]-(c)-[g]-(a)-[h]-(d)-[i]-(b)";
    private final String indexName = "transaction-index";
    private PatternIndexModel model;


    /**
     * {@inheritDoc}
     */
    @Override
    public String shortName() {
        // triangle
        //return "CreateTrianglePatternIndex (" + GRAPH_SIZE + ")";
        // movie pattern
        //return "CreateMoviePatternIndex";
        // transaction pattern
        return "CreateTransactionPatternIndex";
    }

    @Override
    public String longName() {
        // triangle
        //return "Create pattern index for triangle patterns.";
        // movie pattern
        //return "Create pattern index for movie patterns.";
        // transaction pattern
        return "Create pattern index for transaction patterns.";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Parameter> parameters() {
        List<Parameter> result = new LinkedList<>();
        //result.add(new CacheParameter("cache")); //no cache, low-level cache, high-level cache
        result.add(new ObjectParameter("cache", new HighLevelCache()));
        //result.add(new ObjectParameter("cache", new HighLevelCache(), new LowLevelCache(), new NoCache())); //low-level cache, high-level cache
        //result.add(new ObjectParameter("cache", new NoCache()));
        return result;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public int dryRuns(Map<String, Object> params) {
        return ((CacheConfiguration) params.get("cache")).needsWarmup() ? 0 : 0;
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
        PatternIndexModel.destroyInstance();
        model = PatternIndexModel.getInstance(database);
    }

    @Override
    public String getExistingDatabasePath() {
        // triangle
        //return "testDb/graph" + GRAPH_SIZE + ".db.zip";
        // movie pattern
        //return "testDb/cineasts_12k_movies_50k_actors.db.zip";
        // transaction pattern
        return "testDb/transactions10k-100k.db.zip";
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

        time += TestUtils.time(new TestUtils.Timed() {
            @Override
            public void time() {
                try {
                    PatternQuery patternQuery = new PatternQuery(pattern, database);
                    model.buildNewIndex(patternQuery, indexName);
                } catch (InvalidCypherMatchException e) {
                    e.printStackTrace();
                } catch (PatternIndexAlreadyExistsException e) {
                    e.printStackTrace();
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
