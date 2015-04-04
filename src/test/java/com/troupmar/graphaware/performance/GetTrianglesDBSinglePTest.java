package com.troupmar.graphaware.performance;

import com.graphaware.test.performance.CacheConfiguration;
import com.graphaware.test.performance.CacheParameter;
import com.graphaware.test.performance.Parameter;
import com.graphaware.test.performance.PerformanceTest;
import com.graphaware.test.util.TestUtils;
import org.junit.rules.TemporaryFolder;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.*;
import java.util.*;

/**
 * Created by Martin on 03.04.15.
 */
public class GetTrianglesDBSinglePTest implements PerformanceTest {

    private TemporaryFolder temporaryFolder;
    private GraphDatabaseService temporaryDatabase;
    Set<String> triangleSet;
    private boolean writePermission = true;
    private List<Map<String, Object>> optResults;

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
        //triangleSet = PerformanceTestHelper.getTriangleSetFromDatabase(database, "");
        triangleSet = PerformanceTestHelper
                .getTriangleSetFromFile("/Users/Martin/git/Neo4jPatternIndex/ppt-all-original.txt", "");
    }



    @Override
    public String getExistingDatabasePath() {
        //loadTringlesFromFile();
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

        System.out.println("Node set " + triangleSet.size());

        /* Create tmp DB - TODO move this to time block */
        createTemporaryFolder();
        GraphDatabaseBuilder graphDatabaseBuilder = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(temporaryFolder.getRoot().getPath());
        Map<String, String> dbConfig = databaseParameters(params);
        if (dbConfig != null) {
            graphDatabaseBuilder = graphDatabaseBuilder.setConfig(dbConfig);
        }
        temporaryDatabase = graphDatabaseBuilder.newGraphDatabase();

        time += TestUtils.time(new TestUtils.Timed() {
            @Override
            public void time() {
                Iterator triangleSetIterator = triangleSet.iterator();
                while (triangleSetIterator.hasNext()) {
                    String triangle = triangleSetIterator.next().toString();
                    String[] rels = triangle.split("_");

                    Relationship[] sourceRelationships = new Relationship[3];
                    Transaction txDatabase = database.beginTx();
                    Transaction txTemporaryDatabase = temporaryDatabase.beginTx();

                    try {
                        Map<Long, Node> copiedNodes = new HashMap<>();
                        // Triangle set format: node1_node2_node3_rel1_rel2_rel3
                        for (int i = 3; i < sourceRelationships.length + 3; i++) {
                            Relationship sourceRelationship = database.getRelationshipById(Long.parseLong(rels[i]));
                            Node sourceStartNode = sourceRelationship.getStartNode();
                            Node sourceEndNode = sourceRelationship.getEndNode();

                            Node targetStartNode;
                            if (!copiedNodes.containsKey(sourceStartNode.getId())) {
                                targetStartNode = temporaryDatabase.createNode();
                                copyProperties(sourceStartNode, targetStartNode);
                                copiedNodes.put(sourceStartNode.getId(), targetStartNode);
                            } else {
                                targetStartNode = copiedNodes.get(sourceStartNode.getId());
                            }

                            Node targetEndNode;
                            if (!copiedNodes.containsKey(sourceEndNode.getId())) {
                                targetEndNode = temporaryDatabase.createNode();
                                copyProperties(sourceEndNode, targetEndNode);
                                copiedNodes.put(sourceEndNode.getId(), targetEndNode);
                            } else {
                                targetEndNode = copiedNodes.get(sourceEndNode.getId());
                            }

                            Relationship targetRelationship = targetStartNode.createRelationshipTo(targetEndNode, sourceRelationship.getType());
                            copyProperties(sourceRelationship, targetRelationship);
                        }

                        txTemporaryDatabase.success();
                        txTemporaryDatabase.close();

                        txDatabase.success();
                        txDatabase.close();
                    } catch (Exception e) {
                        txTemporaryDatabase.failure();
                        txDatabase.failure();
                        e.printStackTrace();
                    }

                    Result result = temporaryDatabase.execute("MATCH (a)--(b)--(c)--(a) RETURN id(a), id(b), id(c)");
                    if (writePermission) {
                        PerformanceTestHelper.prepareResults(result, optResults);
                    }
                    //System.out.println(result.resultAsString());

                    //result = temporaryDatabase.execute("MATCH (n) RETURN count(n)");
                    //System.out.println(result.resultAsString());
                }

                if (writePermission) {
                    System.out.println("Saving results to file...");
                    try {
                        PerformanceTestHelper.saveTriangleResultToFile("ppt-db-single-opt.txt", optResults);

                        PerformanceTestHelper.saveTriangleSetResultToFile("ppt-db-single-opt-reduced.txt",
                                PerformanceTestHelper.triangleResultToTriangleSet(optResults));

                        writePermission = false;
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    }
                }

                temporaryDatabase.execute("START n=node(*) MATCH n-[r]-() DELETE n, r");
                closeDatabase();
            }
        });

        return time;
    }

    private static void copyProperties(PropertyContainer source, PropertyContainer target) {
        for (String key : source.getPropertyKeys())
            target.setProperty(key, source.getProperty(key));
    }

    private void closeDatabase() {
        if (temporaryDatabase != null) {
            temporaryDatabase.shutdown();
            temporaryFolder.delete();
            temporaryDatabase = null;
        }
    }

    /*
    private void loadTringlesFromFile() {
        try {
            try (BufferedReader br = new BufferedReader(new FileReader("/home/Jaroslav/Dropbox/FIT/Magistr/Diplomová práce/Testování/triangles_nodes_with_relationships_data.txt"))) {
                String line;
                try {
                    while ((line = br.readLine()) != null) {
                        //System.out.println(line);
                        String[] parts = line.split("\\|");
                        //System.out.println(parts[1]);
                        Long[] nodes = new Long[3];
                        nodes[0] = Long.parseLong(parts[1]);
                        nodes[1] = Long.parseLong(parts[2]);
                        nodes[2] = Long.parseLong(parts[3]);

                        Long[] relationships = new Long[3];
                        relationships[0] = Long.parseLong(parts[4]);
                        relationships[1] = Long.parseLong(parts[5]);
                        relationships[2] = Long.parseLong(parts[6]);

                        Arrays.sort(nodes);
                        Arrays.sort(relationships);

                        String key = relationships[0] + "_" + relationships[1] + "_" + relationships[2];
                        triangleSet.add(key);
                    }
                } catch (Exception ex) {

                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    */

    private void createTemporaryFolder() {
        temporaryFolder = new TemporaryFolder();
        try {
            temporaryFolder.create();
            temporaryFolder.getRoot().deleteOnExit();
        } catch (IOException e) {
            throw new RuntimeException(e);
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
