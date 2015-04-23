package com.troupmar.graphaware;

import org.neo4j.graphdb.*;

import java.util.*;

/**
 * Created by Martin on 12.04.15.
 */
public class DatabaseHandler {

    /**
     * Method to create new pattern index root node
     * @param database database where to create node.
     * @param patternQuery MATCH clause of Cypher query on which to build new index.
     * @param patternName new pattern index name.
     * @return new pattern index root node.
     */
    public static Node createNewPatternIndexRoot(GraphDatabaseService database, PatternQuery patternQuery, String patternName) {
        Node node = database.createNode();
        node.addLabel(NodeLabels._META_);
        node.addLabel(NodeLabels.PATTERN_INDEX_ROOT);
        node.setProperty("patternName", patternName);
        node.setProperty("patternQuery", patternQuery.getPatternQuery());
        node.setProperty("nodeNames", PatternQuery.namesToString(patternQuery.getNodeNames()));
        node.setProperty("relNames", PatternQuery.namesToString(patternQuery.getRelNames()));
        return node;
    }

    /**
     * Method to create new pattern index unit node.
     * @param database database where to create node.
     * @param patternIndexUnit instance that holds all specific units between its nodes.
     * @return new pattern index unit node
     */
    public static Node createNewPatternIndexUnit(GraphDatabaseService database, PatternIndexUnit patternIndexUnit) {
        Node node = database.createNode();
        node.addLabel(NodeLabels._META_);
        node.addLabel(NodeLabels.PATTERN_INDEX_UNIT);
        node.setProperty("specificUnits", PatternIndexUnit.specificUnitsToString(patternIndexUnit.getSpecificUnits()));
        return node;
    }

    /**
     * Method to get all pattern indexes from the database.
     * @param database database where to search for indexes.
     * @return all pattern indexes from database.
     */
    public static Map<String, PatternIndex> getPatternIndexes(GraphDatabaseService database) {
        Map<String, PatternIndex> patternIndexes = new HashMap<>();

        ResourceIterator<Node> rootNodes = database.findNodes(NodeLabels.PATTERN_INDEX_ROOT);

        Node rootNode;
        while (rootNodes.hasNext()) {
            rootNode = rootNodes.next();
            PatternIndex patternIndex = new PatternIndex(rootNode.getProperty("patternName").toString(),
                    rootNode.getProperty("patternQuery").toString(), rootNode, PatternQuery.namesFromString((String) rootNode.getProperty("nodeNames")),
                    PatternQuery.namesFromString((String) rootNode.getProperty("relNames")));
            patternIndexes.put(patternIndex.getPatternName(), patternIndex);
        }
        return patternIndexes;
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

    /**
     * Method to update pattern index unit node when a single relationship is deleted.
     * @param patternIndexUnitNode pattern index unit node to be updated.
     * @param deletedRelID ID of deleted relationship.
     * @return
     */
    public static int updatePatternIndexUnitOnDelete(Node patternIndexUnitNode, Long deletedRelID) {
        Set<String> specificUnits = PatternIndexUnit.specificUnitsFromString((String) patternIndexUnitNode.getProperty("specificUnits"));
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
            patternIndexUnitNode.setProperty("specificUnits", PatternIndexUnit.specificUnitsToString(updatedSpecificUnits));
        }
        return updatedSpecificUnits.size();
    }

    /**
     * Method to update pattern index unit node. It keeps the record of all specific pattern units between its nodes (each pattern
     * index unit node has relationships to nodes that hold some pattern units). So
     * whenever one of the relationship or node in those specific units gets changed or created, the property of the pattern index unit
     * node (specificUnits) gets updated.
     * @param patternIndexUnitNode pattern index unit node to be updated.
     * @param patternIndexUnit instance that holds all specific units between its nodes.
     */
    public static void updatePatternIndexUnit(Node patternIndexUnitNode, PatternIndexUnit patternIndexUnit) {
        Set<String> currentSpecificUnits = PatternIndexUnit.specificUnitsFromString((String) patternIndexUnitNode.getProperty("specificUnits"));
        Set<String> newSpecificUnits = patternIndexUnit.getSpecificUnits();
        if (! currentSpecificUnits.equals(newSpecificUnits)) {
            patternIndexUnitNode.setProperty("specificUnits", PatternIndexUnit.specificUnitsToString(newSpecificUnits));
        }
    }
}
