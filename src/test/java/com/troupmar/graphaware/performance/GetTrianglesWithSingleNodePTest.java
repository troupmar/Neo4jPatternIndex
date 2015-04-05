package com.troupmar.graphaware.performance;

import com.graphaware.test.performance.CacheConfiguration;
import com.graphaware.test.performance.Parameter;
import com.graphaware.test.performance.PerformanceTest;
import com.graphaware.test.util.TestUtils;
import com.troupmar.graphaware.cache.CacheParameter;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;

import java.io.FileNotFoundException;
import java.util.*;

/**
 * Created by Jaroslav on 3/25/15.
 */
public class GetTrianglesWithSingleNodePTest implements PerformanceTest {


    /*
    * 1) dotazovat se nad 1 id nodu a unionovat to pres počet uzlu
    *   - vylepšení: nad neopakováním se pro dotazovaní nad jedním uzlem, již dotazované uzly neopakovat a rovnou přeskočit
    * 2) to samé, ale dotazovat se přes všechny nody a udělat faktorial počtu nodu = počet unionu
    * 3) vytvoření externí databaze a po jednom patternu to tam šoupat a dotazovat se nad tím, pak to smazat a takhle dokola krom vytvoreni DB
    * 4) všechno to samé jako předchozí ale na začátku vytvořit DB a všechny patterny tam vložit = subgraf patternu a nad tím se pak dotazovat přes všechny zaindexpvané patterny
    *
    */

    private SortedSet<String> triangleSet;
    private List<Map<String, Object>> optResults;
    private boolean writePermission = true;

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
        //result.add(new ExponentialParameter("nodeCount", 10, 1, 2, 1)); //10 nodes, then 100
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
        /*
        GeneratorApi generator = new GeneratorApi(database);
        int nodeCount = (int) params.get("nodeCount");
        generator.erdosRenyiSocialNetwork(nodeCount, nodeCount * 4); // *5 fails to satisfy Erdos-Renyi precondition that the number of edges must be < (n*(n-1))/2 when there are 10 nodes
        Log.info("Database prepared");
        */

        triangleSet = PerformanceTestHelper.getTriangleSetFromDatabase(database, "only-nodes");
         //triangleSet = PerformanceTestHelper
           //     .getTriangleSetFromFile("/Users/Martin/git/Neo4jPatternIndex/ptt-only-nodes-original.txt", "only-nodes");
    }

    @Override
    public String getExistingDatabasePath() {
        //return null;
        return "/Users/Martin/Skola/CVUT_FIT/Magister/2.2/DIP/neo4j-community-2.2.0-RC01/data/graph-1000.db.zip";
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
        optResults = new LinkedList<Map<String, Object>>();

        time += TestUtils.time(new TestUtils.Timed() {
            @Override
            public void time() {
                // Uncomment to optimize
                //Set<String> usedNodes = new HashSet<String>();
                for (String nodeId : triangleSet) {
                    nodeId = nodeId.split("_")[0];
                    // Uncomment to optimize
                    //if (!usedNodes.contains(nodeId)) {
                        Result result = database.execute(
                                "MATCH (a)--(b)--(c)--(a) " +
                                        "WHERE id(a)=" + nodeId + " " +
                                        "RETURN id(a), id(b), id(c) " +
                                        "UNION " +
                                        "MATCH (a)--(b)--(c)--(a) " +
                                        "WHERE id(b)=" + nodeId + " " +
                                        "RETURN id(a), id(b), id(c) " +
                                        "UNION " +
                                        "MATCH (a)--(b)--(c)--(a) " +
                                        "WHERE id(c)=" + nodeId + " " +
                                        "RETURN id(a), id(b), id(c)");
                        // Uncomment to optimize
                        //usedNodes.add(nodeId);
                        if (writePermission) {
                            PerformanceTestHelper.prepareResults(result, optResults);
                        }
                    // Uncomment to optimize
                    //}
                }
            }
        });
        // ptt = Performance test triangle, opt = optimalized
        if (writePermission) {
            System.out.println("Saving results to file...");
            try {
                PerformanceTestHelper.saveTriangleResultToFile("ptt-single-node-opt.txt", optResults);

                PerformanceTestHelper.saveTriangleSetResultToFile("ptt-single-node-opt-reduced.txt",
                        PerformanceTestHelper.triangleResultToTriangleSet(optResults));

                writePermission = false;
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }


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
