package com.troupmar.graphaware.performance;

import com.graphaware.test.performance.CacheConfiguration;
import com.graphaware.test.performance.CacheParameter;
import com.graphaware.test.performance.Parameter;
import com.graphaware.test.performance.PerformanceTest;
import com.graphaware.test.util.TestUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;

import java.io.*;
import java.util.*;

/**
 * Created by Jaroslav on 3/25/15.
 */
public class GetTrianglesWithAllNodesPTest implements PerformanceTest {

    SortedSet<String> triangleSet = new TreeSet<>();
    private List<Map<String, Object>> optResults;
    private boolean writePermission = true;
    String query;

    /**
     * {@inheritDoc}
     */
    @Override
    public String shortName() {
        return "GetTrianglesWithAllNodesPTest";
    }

    @Override
    public String longName() {
        return "Optimalization to get all triangles with querying on all 3 defined ids" +
                "(permutation on subgraph).";
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
        return ((CacheConfiguration) params.get("cache")).needsWarmup() ? 50 : 2;
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
        triangleSet = PerformanceTestHelper
                .getTriangleSetFromFile("/Users/Martin/git/Neo4jPatternIndex/ptt-only-nodes-original.txt", "only-nodes");
    }

    @Override
    public String getExistingDatabasePath() {
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
        int k=0;

        // TODO remove
        System.out.println("Number of nodes set: " + triangleSet.size());
        SortedSet<String> triangleSetResult = new TreeSet<>();

        optResults = new LinkedList<Map<String, Object>>();

        time += TestUtils.time(new TestUtils.Timed() {
            @Override
            public void time() {
                Set<String> nodesSet = new HashSet<String>();

                Iterator triangleSetIterator = triangleSet.iterator();
                while (triangleSetIterator.hasNext()) {
                    String triangle = triangleSetIterator.next().toString();
                    String[] nodes = triangle.split("_");

                    query = "";
                    permute(nodes, 0);
                    query = query.substring(0, query.length() - 7);
                    Result result = database.execute(query);

                    if (writePermission) {
                        PerformanceTestHelper.prepareResults(result, optResults);
                    }
                }
            }});

        if (writePermission) {
            System.out.println("Saving results to file...");
            try {
                PerformanceTestHelper.saveTriangleResultToFile("ptt-all-nodes-opt.txt", optResults);

                PerformanceTestHelper.saveTriangleSetResultToFile("ptt-all-nodes-opt-reduced.txt",
                        PerformanceTestHelper.triangleResultToTriangleSet(optResults));

                writePermission = false;
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }

        return  time;
    }


    /**
     * Supportive method for permutation.
     */
    public static final <T> void swap (T[] a, int i, int j) {
        T t = a[i];
        a[i] = a[j];
        a[j] = t;
    }

    /**
     * Permutation method.
     */
    private void permute(String[] nodes, int k) {
        for (int i = k; i < nodes.length; i++) {
            swap(nodes, i, k);
            permute(nodes, k + 1);
            swap(nodes, k, i);
        }

        if (k == nodes.length - 1) {
            query += "MATCH (a)--(b)--(c)--(a) " +
                     "WHERE id(a)=" + nodes[0] + " AND id(b)=" + nodes[1] + " AND id(c)=" + nodes[2] + " RETURN id(a), id(b), id(c) UNION ";
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean rebuildDatabase(Map<String, Object> params) {
        throw new UnsupportedOperationException("never needed, database rebuilt after every param change");
    }
}