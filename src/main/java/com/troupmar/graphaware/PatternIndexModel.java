package com.troupmar.graphaware;

import org.neo4j.graphdb.*;

import java.util.*;

/**
 * Created by Martin on 05.04.15.
 */
public class PatternIndexModel {
    private static PatternIndexModel instance= null;
    private static Object mutex = new Object();

    private GraphDatabaseService database;
    private List<PatternIndex> patternIndexes;

    public PatternIndexModel(GraphDatabaseService database) {
        this.database = database;
        loadIndexRoots();
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

    private void loadIndexRoots() {
        patternIndexes = new ArrayList<PatternIndex>();

        ResourceIterator<Node> rootNodes = null;
        Transaction tx = database.beginTx();
        // TODO should transaction be big or couple small ones?
        try {
            rootNodes = database.findNodes(NodeLabels.PATTERN_INDEX_ROOT);

            Node rootNode;
            while (rootNodes.hasNext()) {
                rootNode = rootNodes.next();
                PatternIndex patternIndex = new PatternIndex(rootNode.getProperty("patternQuery").toString(),
                        rootNode.getProperty("patternName").toString(), rootNode, (int) rootNode.getProperty("numOfUnits"));
                patternIndexes.add(patternIndex);
            }
            tx.success();
        } catch (RuntimeException e) {
            // TODO Log exception and handle return
            tx.failure();
        } finally {
            tx.close();
        }
    }

    public void buildNewIndex(PatternQuery patternQuery, String patternName) {
        if (! patternIndexExists(patternQuery.getPatternQuery(), patternName)) {
            String query = "MATCH " + patternQuery.getPatternQuery() + " RETURN ";
            for (String node : patternQuery.getNodeNames()) {
                query += "id(" + node + "), ";
            }
            /*
            for (String rel : patternQuery.getRelNames()) {
                query += "id(" + rel + "), ";
            }
            */
            query = query.substring(0, query.length() - 2);

            Result result = database.execute(query);
            buildIndex(getPatternUnits(result, patternQuery), patternQuery, patternName);
        } else {
            // TODO inform that index already exists
        }
    }

    // TODO review - move variable inits from inside of cycle
    private void buildIndex(List<Object[]> patternUnits, PatternQuery patternQuery, String patternName) {
        Node patternRootNode = createNewRootNode(patternQuery.getPatternQuery(), patternName, patternUnits.size());
        Node patternUnitNode;

        for (Object[] patternUnit : patternUnits) {
            patternUnitNode = createNewUnitNode();
            createRelationship(patternRootNode, patternUnitNode, RelationshipTypes.PATTERN_INDEX_RELATION);

            for (Object nodeID : patternUnit) {
                //patternUnitNode.createRelationshipTo(database.getNodeById((Long) nodeID), RelationshipTypes.PATTERN_INDEX_RELATION);
                createRelationship(patternUnitNode, (Long) nodeID, RelationshipTypes.PATTERN_INDEX_RELATION);
            }
        }
        PatternIndex patternIndex = new PatternIndex(patternQuery.getPatternQuery(), patternName, patternRootNode, patternUnits.size());
        // TODO patternIndexes must be alredy initialized here! - should be done in start method (TransactionHandleModule)
        patternIndexes.add(patternIndex);

    }

    private void createRelationship(Node from, Long toID, RelationshipType relType) {
        Transaction tx = database.beginTx();
        try {
            from.createRelationshipTo(database.getNodeById(toID), relType);
            tx.success();
        } catch (RuntimeException e) {
            // TODO Log exception and handle return
            tx.failure();
        } finally {
            tx.close();
        }
    }

    private void createRelationship(Node from, Node to, RelationshipType relType) {
        Transaction tx = database.beginTx();
        try {
            from.createRelationshipTo(to, relType);
            tx.success();
        } catch (RuntimeException e) {
            // TODO Log exception and handle return
            tx.failure();
        } finally {
            tx.close();
        }
    }

    private Node createNewUnitNode() {
        Node node = null;
        Transaction tx = database.beginTx();
        try {
            node = database.createNode();
            node.addLabel(NodeLabels._META_);
            node.addLabel(NodeLabels.PATTERN_INDEX_UNIT);
            tx.success();
        } catch (RuntimeException e) {
            // TODO Log exception and handle return
            tx.failure();
        } finally {
            tx.close();
        }
        return node;
    }

    private Node createNewRootNode(String patternQuery, String patternName, int numOfUnits) {
        Node node = null;
        Transaction tx = database.beginTx();
        try {
            node = database.createNode();
            node.addLabel(NodeLabels._META_);
            node.addLabel(NodeLabels.PATTERN_INDEX_ROOT);
            node.setProperty("patternQuery", patternQuery);
            node.setProperty("patternName", patternName);
            node.setProperty("numOfUnits", numOfUnits);
            tx.success();
        } catch (RuntimeException e) {
            // TODO Log exception and handle return
            tx.failure();
        } finally {
            tx.close();
        }
        return node;
    }

    // TODO - check if pattern index already exists must be robust!
    private boolean patternIndexExists(String patternQuery, String patternName) {
        for (PatternIndex patternIndex : patternIndexes) {
            if (patternIndex.getPatternQuery().equals(patternQuery) || patternIndex.getPatternName().equals(patternName)) {
                return true;
            }
        }
        return false;
    }

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

}
