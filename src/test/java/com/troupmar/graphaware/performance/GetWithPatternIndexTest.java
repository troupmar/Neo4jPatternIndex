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

public class GetWithPatternIndexTest implements PerformanceTest {

    // triangle
    //private final String query = "MATCH (a)-[r]-(b)-[p]-(c)-[q]-(a) RETURN a, b, c";
    //private final String pattern = "(a)-[r]-(b)-[p]-(c)-[q]-(a)";
    //private final String indexName = "triangle-index";
    //private final String GRAPH_SIZE = "100-500";
    // movie pattern
    //private final String query = "MATCH (a)-[r]-(b)-[p]-(c)-[q]-(a)-[x]-(e) RETURN a, b, c, e";
    //private final String pattern = "(a)-[r]-(b)-[p]-(c)-[q]-(a)-[x]-(e)";
    //private final String indexName = "movie-index";
    // transaction pattern
    private final String query = "MATCH (a)-[e]-(b)-[f]-(c)-[g]-(a)-[h]-(d)-[i]-(b) RETURN a, b, c, d";
    private final String pattern = "(a)-[e]-(b)-[f]-(c)-[g]-(a)-[h]-(d)-[i]-(b)";
    private final String indexName = "transaction-index";

    private PatternIndexModel model;


    /**
     * {@inheritDoc}
     */
    @Override
    public String shortName() {
        // triangle
        //return "GetTrianglesByPatternQuery (" + GRAPH_SIZE + ")";
        // movie pattern
        //return "GetMoviePatternByPatternQuery";
        // transaction pattern
        return "GetTransactionPatternByPatternQuery";
    }

    @Override
    public String longName() {
        // triangle
        //return "Pattern query to get all triangles.";
        // movie pattern
        //return "Pattern query to get all movie patterns.";
        // transaction pattern
        return "Pattern query to get all transaction patterns.";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Parameter> parameters() {
        List<Parameter> result = new LinkedList<>();
        //result.add(new CacheParameter("cache")); //no cache, low-level cache, high-level cache
        //result.add(new ObjectParameter("cache", new HighLevelCache(), new LowLevelCache(), new NoCache())); //low-level cache, high-level cache
        //result.add(new ObjectParameter("cache", new NoCache()));
        result.add(new ObjectParameter("cache", new NoCache()));
        return result;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public int dryRuns(Map<String, Object> params) {
        return ((CacheConfiguration) params.get("cache")).needsWarmup() ? 4 : 0;
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
        PatternIndexModel.destroy();
        model = PatternIndexModel.getInstance(database);
        /*
        try {
            PatternQuery patternQuery = new PatternQuery(pattern, database);
            model.buildNewIndex(patternQuery, indexName);
        } catch (InvalidCypherMatchException e) {
            e.printStackTrace();
        } catch (PatternIndexAlreadyExistsException e) {
            e.printStackTrace();
        }
        */

    }

    @Override
    public String getExistingDatabasePath() {
        // triangle
        //return "testDb/graph" + GRAPH_SIZE + "-indexed.db.zip";
        // movie pattern
        //return "testDb/cineasts_12k_movies_50k_actors-indexed.db.zip";
        // transaction pattern
        return "testDb/transactions10k-100k-indexed.db.zip";
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

        time += TestUtils.time(new TestUtils.Timed() {
            @Override
            public void time() {
                try {
                    CypherQuery cq = new CypherQuery(query, database);
                    model.getResultFromIndex(cq, indexName);
                    // System.out.println(result.size());
                } catch (InvalidCypherException e) {
                    e.printStackTrace();
                } catch (InvalidCypherMatchException e) {
                    e.printStackTrace();
                } catch (PatternIndexNotFoundException e) {
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
