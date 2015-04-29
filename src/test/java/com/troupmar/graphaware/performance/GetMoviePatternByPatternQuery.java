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

public class GetMoviePatternByPatternQuery implements PerformanceTest {

    private final String query = "MATCH (a)-[r]-(b)-[p]-(c)-[q]-(a)-[x]-(e) RETURN a, b, c, e";
    private final String pattern = "(a)-[r]-(b)-[p]-(c)-[q]-(a)-[x]-(e)";
    private final String indexName = "movie-index";
    private PatternIndexModel model;
    private boolean indexBuilt = false;


    /**
     * {@inheritDoc}
     */
    @Override
    public String shortName() {
        return "GetMoviePatternByPatternQuery";
    }

    @Override
    public String longName() {
        return "Pattern query to get all movie patterns.";
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
        return result;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public int dryRuns(Map<String, Object> params) {
        return ((CacheConfiguration) params.get("cache")).needsWarmup() ? 15 : 3;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int measuredRuns() {
        return 10;
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
        try {
            PatternQuery patternQuery = new PatternQuery(pattern, database);
            model.buildNewIndex(patternQuery, indexName);
        } catch (InvalidCypherMatchException e) {
            e.printStackTrace();
        } catch (PatternIndexAlreadyExistsException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String getExistingDatabasePath() {
        return "testDb/cineasts_12k_movies_50k_actors.db.zip";
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
