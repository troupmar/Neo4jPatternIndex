package com.troupmar.graphaware;

import org.neo4j.graphdb.*;

import java.util.*;

/**
 * Created by Martin on 12.04.15.
 */
public class DatabaseHandler {

    /**
     * Method to build new pattern index unit node. That means it creates new pattern index unit, connects it to the root
     * node of the pattern index and connects it to specified nodes.
     * @param database database to build new pattern index unit in.
     * @param patternIndexUnit instance of PatternIndexUnit represents new pattern index unit - it contains all specific units.
     * @param patternRootNode pattern index root of the index.
     */
    public static void buildNewPatternIndexUnit(GraphDatabaseService database, PatternIndexUnit patternIndexUnit, Node patternRootNode) {
        Node patternUnitNode = DatabaseHandler.createNewPatternIndexUnit(database, patternIndexUnit);
        patternRootNode.createRelationshipTo(patternUnitNode, RelationshipTypes.PATTERN_INDEX_RELATION);
        for (Long nodeID : patternIndexUnit.getNodeIDs()) {
            patternUnitNode.createRelationshipTo(database.getNodeById(nodeID), RelationshipTypes.PATTERN_INDEX_RELATION);
        }
    }

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
        try (Transaction tx = database.beginTx()) {
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
            tx.success();
            return patternIndexes;
        }
    }

    /**
     * Method to delete all empty pattern index roots.
     * @param database database where to delete pattern index roots.
     * @param patternIndexes pattern indexes stored in database.
     */
    public static void deleteEmptyIndexes(GraphDatabaseService database, Map<String, PatternIndex> patternIndexes) {
        // loop over pattern indexes
        try (Transaction tx = database.beginTx()) {
            for (Map.Entry<String, PatternIndex> entry : patternIndexes.entrySet()) {
                // if root node of pattern index does not have any relationships
                if (!entry.getValue().getRootNode().getRelationships(Direction.OUTGOING).iterator().hasNext()) {
                    // remove pattern index
                    patternIndexes.remove(entry.getKey());
                    // delete root node
                    entry.getValue().getRootNode().delete();
                }
            }
            tx.success();
        }
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

    /**
     * Method to return set of start nodes for set of relationships.
     * @param relationships set of relationships to get start nodes for.
     * @return
     */
    public static Set<Node> getStartNodesForRelationships(Iterable<Relationship> relationships) {
        Set<Node> startNodes = new LinkedHashSet<>();
        Iterator itr = relationships.iterator();
        while (itr.hasNext()) {
            Relationship nextRel = (Relationship) itr.next();
            startNodes.add(nextRel.getStartNode());
        }
        return startNodes;
    }

    /**
     * Method to delete set of pattern index units.
     * @param patternIndexUnits set of pattern index units to delete.
     */
    public static void deletePatternIndexUnits(Set<Node> patternIndexUnits) {
        for (Node patternIndexUnit : patternIndexUnits) {
            deletePatternIndexUnit(patternIndexUnit);
        }
    }

    /**
     * Method to delete given pattern index unit. First its relationships must be deleted, then node itself.
     * @param patternIndexUnit pattern index unit to delete.
     */
    public static void deletePatternIndexUnit(Node patternIndexUnit) {
        // get all relationships of pattern index unit
        Iterable<Relationship> patternIndexUnitRels = patternIndexUnit.getRelationships();
        // delete those relationships
        for (Relationship patternIndexUnitRel : patternIndexUnitRels) {
            patternIndexUnitRel.delete();
        }
        // delete unit node itself
        patternIndexUnit.delete();
    }
}
