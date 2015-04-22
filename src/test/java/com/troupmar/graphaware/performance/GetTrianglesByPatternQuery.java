package com.troupmar.graphaware.performance;

import com.graphaware.test.performance.*;
import com.graphaware.test.util.TestUtils;
import com.troupmar.graphaware.CypherQuery;
import com.troupmar.graphaware.PatternIndexModel;
import com.troupmar.graphaware.exception.InvalidCypherException;
import com.troupmar.graphaware.exception.InvalidCypherMatchException;
import com.troupmar.graphaware.exception.PatternIndexNotFoundException;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.*;

public class GetTrianglesByPatternQuery implements PerformanceTest {

    //
    String query = "MATCH (a)-[r]-(b)-[p]-(c)-[q]-(a) RETURN a, b, c";
    String pattern = "(a)-[r]-(b)-[p]-(c)-[q]-(a)";
    String indexName = "triangle-index";
    private final String GRAPH_SIZE = "1000-5000";
    PatternIndexModel pim;


    /**
     * {@inheritDoc}
     */
    @Override
    public String shortName() {
        return "GetTrianglesByPatternQuery (" + GRAPH_SIZE + ")";
    }

    @Override
    public String longName() {
        return "Pattern query to get all triangles.";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Parameter> parameters() {
        List<Parameter> result = new LinkedList<>();
        //result.add(new CacheParameter("cache")); //no cache, low-level cache, high-level cache
        result.add(new ObjectParameter("cache", new NoCache()));
        return result;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public int dryRuns(Map<String, Object> params) {
        return ((CacheConfiguration) params.get("cache")).needsWarmup() ? 1 : 1;
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

    //@Override
    public String getExistingDatabasePath() {
        return "testDb/graph" + GRAPH_SIZE + ".db.zip";
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
        pim = PatternIndexModel.getInstance(database);
        long time = 0;

        time += TestUtils.time(new TestUtils.Timed() {
            @Override
            public void time() {
                try {
                    CypherQuery cq = new CypherQuery(query, database);
                    HashSet<Map<String, Object>> result = pim.getResultFromIndex(cq, indexName);
                    System.out.println(result.size());
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
