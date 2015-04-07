package com.troupmar.graphaware;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import java.util.*;

/**
 * Created by Martin on 05.04.15.
 */
public class PatternIndexModel {
    private static PatternIndexModel instance= null;
    private static Object mutex = new Object();

    private GraphDatabaseService database;
    //private List<Node> patternIndexRoots;
    private List<PatternIndex> patternIndexes;

    private PatternIndexModel(GraphDatabaseService database) {
        this.database = database;
        //loadIndexRoots();
    }

    public static PatternIndexModel getInstance(GraphDatabaseService database) {
        if(instance==null) {
            synchronized (mutex) {
                if (instance==null) {
                    instance= new PatternIndexModel(database);
                }
            }
        }
        return instance;
    }

    public void buildNewIndex(PatternQuery patternQuery, String patternName) {
        String query = "MATCH " + patternQuery.getPatternQuery() + " RETURN ";
        for (String node : patternQuery.getNodeNames()) {
            query += "id(" + node + "), ";
        }
        for (String rel : patternQuery.getRelNames()) {
            query += "id(" + rel + "), ";
        }
        query = query.substring(0, query.length() - 2);

        buildIndex(database.execute(query), patternQuery, patternName);
    }

    // TODO review - move variable inits from inside of cycle
    private void buildIndex(Result patternUnits, PatternQuery patternQuery, String patternName) {
        if (! patternIndexExists(patternQuery.getPatternQuery(), patternName)) {
            Node patternRootNode = createNewRootNode(patternQuery.getPatternQuery(), patternName);

            int numOfUnits = 0;
            Set<String> indexedPatternUnits = new HashSet<String>();
            while (patternUnits.hasNext()) {
                Map<String, Object> patternUnit = patternUnits.next();
                Object [] nodeIDs = getPatternNodeIDs(patternUnit, patternQuery);
                String key = getPatternUnitKey(nodeIDs);
                if (! indexedPatternUnits.contains(key)) {
                    Node patternUnitNode = createNewUnitNode();
                    patternRootNode.createRelationshipTo(patternUnitNode, RelationshipTypes.PATTERN_INDEX_RELATION);
                    for (Object nodeID : nodeIDs) {
                        patternUnitNode.createRelationshipTo(database.getNodeById((Long) nodeID), RelationshipTypes.PATTERN_INDEX_RELATION);
                    }
                    numOfUnits++;
                    indexedPatternUnits.add(key);
                }
            }
            PatternIndex patternIndex = new PatternIndex(patternQuery.getPatternQuery(), patternName, patternRootNode);
            // TODO patternIndexes must be alredy initialized here! - should be done in start method (TransactionHandleModule)
            patternIndexes.add(patternIndex);
        } else {
            // TODO throw exception - pattern index already exists
        }
    }

    private Node createNewUnitNode() {
        Transaction tx = database.beginTx();
        try {
            Node node = database.createNode();
            node.addLabel(NodeLabels.PATTERN_INDEX_UNIT);
            tx.success();
            return node;
        } catch (RuntimeException e) {
            // TODO Log exception and handle return
            tx.failure();
            return null;
        }
    }

    private Node createNewRootNode(String patternQuery, String patternName) {
        // TODO should this be in transaction?
        Transaction tx = database.beginTx();
        try {
            Node node = database.createNode();
            node.addLabel(NodeLabels._META_);
            node.addLabel(NodeLabels.PATTERN_INDEX_ROOT);
            node.setProperty("patternQuery", patternQuery);
            node.setProperty("patternName", patternName);
            tx.success();
            return node;
        } catch (RuntimeException e) {
            // TODO Log exception and handle return
            tx.failure();
            return null;
        }
    }

    // TODO - check if pattern index already exists must be robust!
    private boolean patternIndexExists(String patternQuery, String patternName) {
        for (PatternIndex patternIndex : patternIndexes) {
            if (patternIndex.getPatternName().equals(patternName) || patternIndex.getPatternQuery().equals(patternQuery)) {
                return true;
            }
        }
        return false;
    }

    private Object[] getPatternNodeIDs(Map<String, Object> patternUnit, PatternQuery patternQuery) {
        Object [] nodes = new Object[patternQuery.getNodeNames().size()];
        int i = 0;
        for (String nodeName : patternQuery.getNodeNames()) {
            nodes[i++] = patternUnit.get("id(" + nodeName + ")");
        }
        return nodes;
    }

    private String getPatternUnitKey(Object [] nodes) {
        // BE AWARE that I am editing nodes array sent in parameter!
        Arrays.sort(nodes);
        String key = "";
        for (Object node : nodes) {
            key += node + "_";
        }
        return key.substring(0, key.length() - 1);
    }

}
