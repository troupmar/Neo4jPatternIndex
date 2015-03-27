/**
 * Created by Jaroslav on 3/26/15.
 */

import com.esotericsoftware.minlog.Log;
import com.graphaware.module.algo.generator.api.GeneratorApi;
import com.graphaware.test.performance.*;
import com.graphaware.test.util.TestUtils;
import org.neo4j.cypher.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.unsafe.batchinsert.BatchInsertTest;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class PerformanceTestCypherTriangleReturnNodes implements PerformanceTest {

    /**
     * {@inheritDoc}
     */
    @Override
    public String shortName() {
        return "triangle return nodes";
    }

    @Override
    public String longName() {
        return "Cypher query for get nodes of all triangles";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Parameter> parameters() {
        List<Parameter> result = new LinkedList<>();
        result.add(new CacheParameter("cache")); //no cache, low-level cache, high-level cache
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int dryRuns(Map<String, Object> params) {
        return ((CacheConfiguration) params.get("cache")).needsWarmup() ? 10000 : 100;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int measuredRuns() {
        return 100;
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

        GeneratorApi generator = new GeneratorApi(database);
        generator.erdosRenyiSocialNetwork(1000, 5000);
        Log.info("Database prepared");
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
    @Override
    public long run(GraphDatabaseService database, Map<String, Object> params) {
        long time = 0;
        StringBuffer dumpBuffer =new StringBuffer();
        StringLogger dumpLogger = StringLogger.wrap(dumpBuffer);
        final ExecutionEngine engine = new ExecutionEngine(database, dumpLogger);

        time += TestUtils.time(new TestUtils.Timed() {
            @Override
            public void time() {
               engine.execute("MATCH (a)--(b)--(c)--(a) RETURN a,b,c");
            }
        });

        return  time;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean rebuildDatabase(Map<String, Object> params) {
        throw new UnsupportedOperationException("never needed, database rebuilt after every param change");
    }
}