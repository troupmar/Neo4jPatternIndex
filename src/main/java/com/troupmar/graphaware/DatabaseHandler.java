package com.troupmar.graphaware;

import org.neo4j.graphdb.*;

import java.util.*;

/**
 * This class provides some helper methods to manipulate with data from graph database. Those methods are needed while
 * working with pattern indexes, such as creating meta nodes etc.
 *
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
     * @return the amount of updated specific units. If the pattern index unit remains the same after update - it returns 0.
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
     * @return set of start nodes.
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

    public static long getLowestIdOfEndNodes(Iterable<Relationship> relationships) {
        long lowest = Long.MAX_VALUE;
        Iterator itr = relationships.iterator();
        while (itr.hasNext()) {
            Relationship nextRel = (Relationship) itr.next();
            if (lowest > nextRel.getEndNode().getId()) {
                lowest = nextRel.getEndNode().getId();
            }
        }
        return lowest;
    }

    /**
     * Method accepts set of meta relationships and gets specific unit nodes from them (end nodes) and returns ID of the one
     * that has the most amount of meta relationships.
     * @param metaRels set of meta relationships.
     * @return ID of node with most meta relationships.
     */
    public static long getMostParticipatedNode(Iterable<Relationship> metaRels) {
        int numOfMetaRels = 0;
        long mostParticipatedNodeId = 0;
        for (Relationship metaRel : metaRels) {
            Node endNode = metaRel.getEndNode();
            if (endNode.getDegree(RelationshipTypes.PATTERN_INDEX_RELATION, Direction.INCOMING) > numOfMetaRels) {
                numOfMetaRels = endNode.getDegree(RelationshipTypes.PATTERN_INDEX_RELATION, Direction.INCOMING);
                mostParticipatedNodeId = endNode.getId();
            }
        }
        return mostParticipatedNodeId;
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

    /**
     * Method to find all meta nodes, precisely pattern index units for a specific pattern index that have connection
     * to all given nodes.
     * @param database database where to look for those pattern index units.
     * @param patternIndex represent the pattern index, where those found pattern index units should belong to.
     * @param nodeIDs node IDs to find shared pattern units for.
     * @return pattern index unit node.
     */
    public static Node getPatternIndexUnitForNodes(GraphDatabaseService database, PatternIndex patternIndex, Long[] nodeIDs) {
        // get pattern index units across all existing pattern indexes
        Set<Node> sharedMetaNodes = getPatternIndexesUnitsForNodes(database, nodeIDs);
        // filter only those who belong to given pattern index by checking if they have relationship to root node of given pattern index
        for (Node sharedMetaNode : sharedMetaNodes) {
            Relationship relToRoot = sharedMetaNode.getSingleRelationship(RelationshipTypes.PATTERN_INDEX_RELATION, Direction.INCOMING);
            if (relToRoot.getStartNode().getId() == patternIndex.getRootNode().getId()) {
                return sharedMetaNode;
            }
        }
        return null;
    }

    /**
     * Method to find all meta nodes, precisely pattern index units across all pattern indexes existing in the database
     * that have connection to all given nodes.
     * @param database database where to look for those pattern index units.
     * @param nodeIDs node IDs to find shared pattern units for.
     * @return set of pattern index unit nodes.
     */
    public static Set<Node> getPatternIndexesUnitsForNodes(GraphDatabaseService database, Long[] nodeIDs) {
        // get all meta relationships of first node from given array of node IDs
        Iterable<Relationship> metaRelsOfNode = database.getNodeById(nodeIDs[0]).getRelationships(RelationshipTypes.PATTERN_INDEX_RELATION, Direction.INCOMING);
        // get all pattern index units that first node from given array of node IDs have relationship to
        Set<Node> sharedMetaNodes = DatabaseHandler.getStartNodesForRelationships(metaRelsOfNode);
        Set<Node> toDelete = new HashSet<>();

        if (nodeIDs.length > 1) {
            // loop over the rest of node IDs from given array
            for (int i = 1; i < nodeIDs.length; i++) {
                // get meta relationships for another node from given array
                metaRelsOfNode = database.getNodeById(nodeIDs[i]).getRelationships(RelationshipTypes.PATTERN_INDEX_RELATION, Direction.INCOMING);
                // get pattern index units that have relationships to this node
                Set<Node> metaNodes = DatabaseHandler.getStartNodesForRelationships(metaRelsOfNode);
                /**
                 * find shared pattern units with the node before by deleting those that are remaining from the node before but are not shared with
                 * this new node
                 */
                // this new node
                for (Node sharedMetaNode : sharedMetaNodes) {
                    if (! metaNodes.contains(sharedMetaNode)) {
                        toDelete.add(sharedMetaNode);
                    }
                }
                // remaining pattern index units from previous nodes - those that are not shared with this current node
                // Because Set.removeAll(Set) does not work on server!
                for (Node singleToDelete : toDelete) {
                    sharedMetaNodes.remove(singleToDelete);
                }
                toDelete.clear();
            }
        }
        return sharedMetaNodes;

    }

    /**
     * This method accepts a set of pattern index units across all existing pattern indexes and filters those who belong to
     * pattern index given as parameter.
     * @param patternIndexesUnits set of pattern index units to be filtered.
     * @param patternIndex filter parameter - whose pattern index units should be returned.
     * @return set of pattern index unit nodes.
     */
    public static Set<Node> getPatternIndexUnitsForIndex(Set<Node> patternIndexesUnits, PatternIndex patternIndex) {
        Set<Node> patternIndexUnits = new HashSet<>();
        for (Node patternIndexesUnit : patternIndexesUnits) {
            Relationship relToRoot = patternIndexesUnit.getSingleRelationship(RelationshipTypes.PATTERN_INDEX_RELATION, Direction.INCOMING);
            if (relToRoot.getStartNode().getId() == patternIndex.getRootNode().getId()) {
                patternIndexUnits.add(patternIndexesUnit);
            }
        }
        return patternIndexUnits;
    }

}
