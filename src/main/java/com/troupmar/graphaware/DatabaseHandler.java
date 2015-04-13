package com.troupmar.graphaware;

import org.neo4j.graphdb.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Martin on 12.04.15.
 */
public class DatabaseHandler {

    public static void createRelationship(GraphDatabaseService database, Node from, Node to, RelationshipType relType) {
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

    public static void createRelationship(GraphDatabaseService database, Node from, Long toID, RelationshipType relType) {
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

    public static Node createNewUnitNode(GraphDatabaseService database) {
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

    public static Node createNewRootNode(GraphDatabaseService database, String patternQuery, String patternName, int numOfUnits) {
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

    public static Map<String, PatternIndex> getPatternIndexRoots(GraphDatabaseService database) {
        Map<String, PatternIndex> patternIndexes = new HashMap<String, PatternIndex>();

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
                patternIndexes.put(patternIndex.getPatternName(), patternIndex);
            }
            tx.success();
        } catch (RuntimeException e) {
            // TODO Log exception and handle return
            tx.failure();
        } finally {
            tx.close();
        }
        return patternIndexes;
    }

    public static Iterable<Relationship> getRelationships(GraphDatabaseService database, Node node, Direction dir) {
        Iterable<Relationship> relationships = null;
        Transaction tx = database.beginTx();
        try {
            relationships = node.getRelationships(dir);
            tx.success();
        } catch (RuntimeException e) {
            // TODO Log exception and handle return
            tx.failure();
        } finally {
            tx.close();
        }
        return relationships;
    }

    public static Iterable<Relationship> getRelationshipsByLabel(GraphDatabaseService database, Node node, Direction dir, RelationshipTypes label) {
        Iterable<Relationship> relationships = null;
        Transaction tx = database.beginTx();
        try {
            relationships = node.getRelationships(dir, label);
            tx.success();
        } catch (RuntimeException e) {
            // TODO Log exception and handle return
            tx.failure();
        } finally {
            tx.close();
        }
        return relationships;
    }

    public static Node getNodeById(GraphDatabaseService database, Long nodeID) {
        Node node = null;
        Transaction tx = database.beginTx();
        try {
            node = database.getNodeById(nodeID);
            tx.success();
        } catch (RuntimeException e) {
            // TODO Log exception and handle return
            tx.failure();
        } finally {
            tx.close();
        }
        return node;
    }

    public static void deleteNode(GraphDatabaseService database, Node node) {
        Transaction tx = database.beginTx();
        try {
            node.delete();
            tx.success();
        } catch (RuntimeException e) {
            // TODO Log exception and handle return
            tx.failure();
        } finally {
            tx.close();
        }
    }

    public static void deleteRelationship(GraphDatabaseService database, Relationship relationship) {
        Transaction tx = database.beginTx();
        try {
            relationship.delete();
            tx.success();
        } catch (RuntimeException e) {
            // TODO Log exception and handle return
            tx.failure();
        } finally {
            tx.close();
        }
    }
}
