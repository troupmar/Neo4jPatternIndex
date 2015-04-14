package com.troupmar.graphaware;

import com.troupmar.graphaware.exception.PatternIndexNotFoundException;
import org.neo4j.graphdb.*;

import java.util.*;

/**
 * Created by Martin on 05.04.15.
 */
public class PatternIndexModel {
    private static PatternIndexModel instance= null;
    private static Object mutex = new Object();

    private GraphDatabaseService database;
    private Map<String, PatternIndex> patternIndexes;

    /* Pattern index model init */
    public PatternIndexModel(GraphDatabaseService database) {
        this.database = database;
        this.patternIndexes = DatabaseHandler.getPatternIndexRoots(database);
    }

    public static PatternIndexModel getInstance(GraphDatabaseService database) {
        if(instance == null) {
            synchronized (mutex) {
                if (instance == null) {
                    instance = new PatternIndexModel(database);
                }
            }
        }
        return instance;
    }

    /* Query on top of the index */
    public HashSet<Map<String, Object>> getResultFromIndex(CypherQuery cypherQuery, String patternName) throws PatternIndexNotFoundException {
        // time
        long totalTime = System.nanoTime();

        if (! patternIndexes.containsKey(patternName)) {
            throw new PatternIndexNotFoundException();
        }
        HashSet<Map<String, Object>> results = new HashSet<Map<String, Object>>();
        HashSet<Long> queriedNodeIDs = new HashSet<Long>();

        Node rootNode = patternIndexes.get(patternName).getRootNode();
        Iterable<Relationship> relsToUnits = DatabaseHandler.getRelationships(database, rootNode, Direction.OUTGOING);

        Node unitNode;
        Long nodeToQueryID;
        Iterable<Relationship> relsToNodes;
        Transaction tx = database.beginTx();

        // time
        long queryTime = 0;
        long time;

        for (Relationship rel : relsToUnits) {
            unitNode = rel.getEndNode();
            relsToNodes = DatabaseHandler.getRelationships(database, unitNode, Direction.OUTGOING);
            nodeToQueryID = relsToNodes.iterator().next().getEndNode().getId();
            if (! queriedNodeIDs.contains(nodeToQueryID)) {
                // time
                time = System.nanoTime();

                addSingleUnitResult(cypherQuery, nodeToQueryID, results);
                // time
                time = System.nanoTime() - time;
                queryTime += time;

                queriedNodeIDs.add(nodeToQueryID);
            }
        }

        // time
        totalTime = System.nanoTime() - totalTime;
        System.out.println("Time of total elapsed: " + totalTime);
        System.out.println("Time of query elapsed: " + queryTime);

        return results;
    }

    public void addSingleUnitResult(CypherQuery cypherQuery, Long nodeToQueryID, HashSet<Map<String, Object>> results) {
        String query = "";
        for (String nodeName : cypherQuery.getNodeNames()) {
            query += cypherQuery.getCypherQuery().substring(0, cypherQuery.getInsertPosition());
            query += buildWhereCondition(cypherQuery, nodeName, nodeToQueryID);
            query += cypherQuery.getCypherQuery().substring(cypherQuery.getInsertPosition(), cypherQuery.getCypherQuery().length());
            query += " UNION ";
        }
        query = query.substring(0, query.length() - 7);
        Result result = database.execute(query);
        while (result.hasNext()) {
            results.add(result.next());
        }

    }

    private String buildWhereCondition(CypherQuery cypherQuery, String nodeToQueryName, Long nodeToQueryID) {
        String condition = " WHERE ";
        condition += "id(" + nodeToQueryName + ")=" + nodeToQueryID + " ";
        for (String nodeName : cypherQuery.getNodeNames()) {
            condition += "AND NOT " + nodeName + ":_META_ ";
        }
        return condition;
    }

    /* Building index */
    public void buildNewIndex(PatternQuery patternQuery, String patternName) {
        if (! patternIndexExists(patternQuery.getPatternQuery(), patternName)) {
            String query = "MATCH " + patternQuery.getPatternQuery() + " WHERE ";

            String returnStatement = "";
            for (String node : patternQuery.getNodeNames()) {
                query += "NOT " + node + ":_META_ AND ";
                returnStatement += "id(" + node + "), ";
            }

            query = query.substring(0, query.length() - 5) + " RETURN " + returnStatement.substring(0, returnStatement.length() - 2);

            Result result = database.execute(query);
            System.out.println("Execution of original query finished!");
            buildIndex(getPatternUnits(result, patternQuery), patternQuery, patternName);
        } else {
            // TODO inform that index already exists
        }
    }

    private void buildIndex(List<Object[]> patternUnits, PatternQuery patternQuery, String patternName) {
        System.out.println(patternUnits.size());
        Node patternRootNode = DatabaseHandler.createNewRootNode(database, patternQuery.getPatternQuery(), patternName, patternUnits.size());
        Node patternUnitNode;

        for (Object[] patternUnit : patternUnits) {
            patternUnitNode = DatabaseHandler.createNewUnitNode(database);
            DatabaseHandler.createRelationship(database, patternRootNode, patternUnitNode, RelationshipTypes.PATTERN_INDEX_RELATION);

            for (Object nodeID : patternUnit) {
                DatabaseHandler.createRelationship(database, patternUnitNode, (Long) nodeID, RelationshipTypes.PATTERN_INDEX_RELATION);
            }
        }
        PatternIndex patternIndex = new PatternIndex(patternQuery.getPatternQuery(), patternName, patternRootNode, patternUnits.size());
        // TODO patternIndexes must be alredy initialized here! - should be done in start method (TransactionHandleModule)
        patternIndexes.put(patternIndex.getPatternName(), patternIndex);
    }

    // TODO - check if pattern index already exists must be robust!
    private boolean patternIndexExists(String patternQuery, String patternName) {
        for (PatternIndex patternIndex : patternIndexes.values()) {
            if (patternIndex.getPatternQuery().equals(patternQuery) || patternIndex.getPatternName().equals(patternName)) {
                return true;
            }
        }
        return false;
    }

    // TODO optimize!
    private List<Object[]> getPatternUnits(Result result, PatternQuery patternQuery) {
        List patternUnits = new ArrayList<Object[]>();
        Set<String> uniquePatternUnitKeys = new HashSet<String>();
        String key;
        while (result.hasNext()) {
            Object[] patternUnit = getPatternUnitIDs(result.next(), patternQuery);
            key = getPatternUnitKey(patternUnit);
            if (! uniquePatternUnitKeys.contains(key)) {
                patternUnits.add(patternUnit);
                uniquePatternUnitKeys.add(key);
            }
        }
        return patternUnits;
    }

    private Object[] getPatternUnitIDs(Map<String, Object> patternUnit, PatternQuery patternQuery) {
        Object[] nodes = new Object[patternQuery.getNodeNames().size()];
        int i = 0;
        for (String nodeName : patternQuery.getNodeNames()) {
            nodes[i++] = patternUnit.get("id(" + nodeName + ")");
        }
        return nodes;
    }

    private String getPatternUnitKey(Object[] nodes) {
        // BE AWARE that I am editing nodes array sent in parameter!
        Arrays.sort(nodes);
        String key = "";
        for (Object node : nodes) {
            key += node + "_";
        }
        return key.substring(0, key.length() - 1);
    }

    public Map<String, PatternIndex> getPatternIndexes() {
        return patternIndexes;
    }

    public void removePatternIndex(String patternIndexName) {
        patternIndexes.remove(patternIndexName);
    }

    public void printResult(HashSet<Map<String, Object>> results) {
        System.out.println("Total of " + results.size() + " results:");
        Iterator itr = results.iterator();
        while (itr.hasNext()) {
            System.out.println(itr.next().toString());
        }
    }

}
