import com.esotericsoftware.minlog.Log;
import com.graphaware.module.algo.generator.api.GeneratorApi;
import com.graphaware.test.performance.CacheConfiguration;
import com.graphaware.test.performance.ExponentialParameter;
import com.graphaware.test.performance.Parameter;
import com.graphaware.test.performance.PerformanceTest;
import com.graphaware.test.util.TestUtils;
import com.troupmar.graphaware.cache.CacheParameter;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by Jaroslav on 3/25/15.
 */
public class PerformanceTestCypherTriangleCount implements PerformanceTest {


    /*
    * 1) dotazovat se nad 1 id nodu a unionovat to pres počet uzlu
    *   - chytristika nad neopakováním se pro dotazovaní nad jedním uzlem, již dotazované uzly neopakovat a rovnou přeskočit
    * 2) to samé, ale dotazovat se přes všechny nody a udělat faktorial počtu nodu = počet unionu
    * 3) vytvoření externí databaze a po jednom patternu to tam šoupat a dotazovat se nad tím, pak to smazat a takhle dokola krom vytvoreni DB
    * 4) všechno to samé jako předchozí ale na začátku vytvořit DB a všechny patterny tam vložit = subgraf patternu a nad tím se pak dotazovat přes všechny zaindexpvané patterny
    *
    /**
     * {@inheritDoc}
     */
    @Override
    public String shortName() {
        return "triangle count";
    }

    @Override
    public String longName() {
        return "Cypher query for get count of triangles";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Parameter> parameters() {
        List<Parameter> result = new LinkedList<>();
        result.add(new CacheParameter("cache")); //no cache, low-level cache, high-level cache
        result.add(new ExponentialParameter("nodeCount", 10, 1, 2, 1)); //10 nodes, then 100
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int dryRuns(Map<String, Object> params) {
        return ((CacheConfiguration) params.get("cache")).needsWarmup() ? 50 : 5;
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
        /*
        GeneratorApi generator = new GeneratorApi(database);
        int nodeCount = (int) params.get("nodeCount");
        generator.erdosRenyiSocialNetwork(nodeCount, nodeCount * 4); //*5 fails to satisfy Erdos-Renyi precondition that the number of edges must be < (n*(n-1))/2 when there are 10 nodes
        Log.info("Database prepared");
        */
    }

    @Override
    public String getExistingDatabasePath() {
        return "/Users/Martin/Skola/CVUT_FIT/Magister/2.2/DIP/neo4j-community-2.2.0-RC01/data/graph.db.zip";
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
    public long run(final GraphDatabaseService database, Map<String, Object> params) {
        long time = 0;

        time += TestUtils.time(new TestUtils.Timed() {
            @Override
            public void time() {
                database.execute("MATCH (a)--(b)--(c)--(a) RETURN count(a)");
//                database.execute("MATCH (a) RETURN count(a) ");
                //ExecutionResult result = engine.execute("MATCH (a)--(b)--(c)--(a) RETURN count(a)");
                //Log.info(result.dumpToString());
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
