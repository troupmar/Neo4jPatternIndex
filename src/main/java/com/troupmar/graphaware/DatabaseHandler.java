package com.troupmar.graphaware;

import org.neo4j.graphdb.*;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * Created by Martin on 12.04.15.
 */
public class DatabaseHandler {

    // Method to create new pattern index root node
    public static Node createNewUnitNode(GraphDatabaseService database, PatternUnit patternUnit) {
        Node node = database.createNode();
        node.addLabel(NodeLabels._META_);
        node.addLabel(NodeLabels.PATTERN_INDEX_UNIT);
        node.setProperty("specificUnits", PatternUnit.specificUnitsToString(patternUnit.getSpecificUnits()));
        return node;
    }

    // Method to create pattern index unit node
    public static Node createNewRootNode(GraphDatabaseService database, PatternQuery patternQuery, String patternName, int numOfUnits) {
        Node node = database.createNode();
        node.addLabel(NodeLabels._META_);
        node.addLabel(NodeLabels.PATTERN_INDEX_ROOT);
        node.setProperty("patternName", patternName);
        node.setProperty("patternQuery", patternQuery.getPatternQuery());
        node.setProperty("nodeNames", PatternQuery.namesToString(patternQuery.getNodeNames()));
        node.setProperty("relNames", PatternQuery.namesToString(patternQuery.getRelNames()));
        node.setProperty("numOfUnits", numOfUnits);
        return node;
    }

    public static Map<String, PatternIndex> getPatternIndexRoots(GraphDatabaseService database) {
        Map<String, PatternIndex> patternIndexes = new HashMap<>();

        ResourceIterator<Node> rootNodes = null;
        Transaction tx = database.beginTx();
        // TODO should transaction be big or couple small ones?
        try {
            rootNodes = database.findNodes(NodeLabels.PATTERN_INDEX_ROOT);

            Node rootNode;
            while (rootNodes.hasNext()) {
                rootNode = rootNodes.next();
                PatternIndex patternIndex = new PatternIndex(rootNode.getProperty("patternName").toString(),
                        rootNode.getProperty("patternQuery").toString(), rootNode, PatternQuery.namesFromString((String) rootNode.getProperty("nodeNames")),
                        PatternQuery.namesFromString((String) rootNode.getProperty("relNames")), (int) rootNode.getProperty("numOfUnits"));
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

    // Method to get all META relationships of given node
    public static HashSet<Relationship> getMetaRelsOfNode(Node node) {
        HashSet<Relationship> metaRelsOfNode = new HashSet<>();
        Iterator<Relationship> itr = node.getRelationships().iterator();
        while (itr.hasNext()) {
            Relationship relationship = itr.next();
            if (relationship.isType(RelationshipTypes.PATTERN_INDEX_RELATION)) {
                metaRelsOfNode.add(relationship);
            }
        }
        return metaRelsOfNode;
    }

    // Method to update unit pattern node's specific units: after relationship is deleted -> some specific units might be deleted
    public static int updatePatternUnitOnDelete(Node unitNode, Long deletedRelID) {
        Set<String> specificUnits = PatternUnit.specificUnitsFromString((String) unitNode.getProperty("specificUnits"));
        Set<String> updatedSpecificUnits = new HashSet<>();
        boolean delete = false;
        for (String specificUnit : specificUnits) {
            for (String relID : specificUnit.split("_")) {
                if (Long.valueOf(relID) == deletedRelID) {
                    delete = true;
                    break;
                }
            }
            if (! delete) {
                updatedSpecificUnits.add(specificUnit);
            } else {
                delete = false;
            }
        }
        if (updatedSpecificUnits.size() != 0) {
            unitNode.setProperty("specificUnits", PatternUnit.specificUnitsToString(updatedSpecificUnits));
        }
        return updatedSpecificUnits.size();
    }

    // TODO review and test
    public static void updatePatternUnitOnCreate(Node unitNode, PatternUnit patternUnit) {
        Set<String> currentSpecificUnits = PatternUnit.specificUnitsFromString((String) unitNode.getProperty("specificUnits"));
        Set<String> newSpecificUnits = patternUnit.getSpecificUnits();
        if (! currentSpecificUnits.equals(newSpecificUnits)) {
            unitNode.setProperty("specificUnits", PatternUnit.specificUnitsToString(newSpecificUnits));
        }
        /*
        boolean updated = false;
        for (String newSpecificUnit : newSpecificUnits) {
            if (! currentSpecificUnits.contains(newSpecificUnit)) {
                currentSpecificUnits.add(newSpecificUnit);
                updated = true;
            }
        }

        if (updated) {
            unitNode.setProperty("specificUnits", PatternUnit.specificUnitsToString(currentSpecificUnits));
        }
        */
    }
}
