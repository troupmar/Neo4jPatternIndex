package com.troupmar.graphaware;


import com.esotericsoftware.minlog.Log;
import com.graphaware.tx.event.improved.api.Change;
import com.graphaware.tx.event.improved.api.ImprovedTransactionData;
import com.troupmar.graphaware.exception.PatternIndexNotFoundException;
import org.neo4j.graphdb.*;

import java.util.*;

/**
 * Created by Martin on 05.04.15.
 */
public class PatternIndexModel {
    private static PatternIndexModel instance = null;
    private static final Object mutex = new Object();

    private GraphDatabaseService database;
    private Map<String, PatternIndex> patternIndexes;

    /* PATTERN INDEX MODEL INIT */

    private PatternIndexModel(GraphDatabaseService database) {
        this.database = database;
        try (Transaction tx = this.database.beginTx()) {
            this.patternIndexes = DatabaseHandler.getPatternIndexes(database);
            tx.success();
        }
    }

    /**
     * Singleton method. Creates instance only if it was never created before
     * @param database
     * @return instance of this class.
     */
    public static PatternIndexModel getInstance(GraphDatabaseService database) {
        if (instance == null) {
            synchronized (mutex) {
                if (instance == null) {
                    instance = new PatternIndexModel(database);
                }
            }
        }
        return instance;
    }

    /**
     * Method to destroy instance of PatternIndexModel
     */
    public static void destroy() {
        synchronized (mutex) {
            instance = null;
        }
    }

    /* QUERY WITH INDEX */

    /**
     * Method to execute query on specified index.
     * @param cypherQuery instance of CypherQuery, which is user's parsed Cypher query.
     * @param patternName pattern name to be queried on.
     * @return result for Cypher query.
     * @throws PatternIndexNotFoundException
     */
    public HashSet<Map<String, Object>> getResultFromIndex(CypherQuery cypherQuery, String patternName) throws PatternIndexNotFoundException {
        // if pattern index to query on does not exist ...
        if (!patternIndexes.containsKey(patternName)) {
            throw new PatternIndexNotFoundException();
        }
        // results container
        HashSet<Map<String, Object>> results = new HashSet<>();
        // already queried nodes - so they are not queried multiple times
        HashSet<Long> queriedNodeIDs = new HashSet<>();
        // get root node of index to query on
        Node rootNode = patternIndexes.get(patternName).getRootNode();

        try (Transaction tx = database.beginTx()) {
            // get root relationships
            Iterable<Relationship> relsToPatternIndexUnits = rootNode.getRelationships(RelationshipTypes.PATTERN_INDEX_RELATION, Direction.OUTGOING);

            Iterable<Relationship> relsToNodes;
            Node patternIndexUnit;

            // go over all relationships from root to pattern index unit nodes
            for (Relationship rel : relsToPatternIndexUnits) {
                // get pattern unit node
                patternIndexUnit = rel.getEndNode();
                // get relationships from pattern index unit node to nodes of specific unit
                relsToNodes = patternIndexUnit.getRelationships(RelationshipTypes.PATTERN_INDEX_RELATION, Direction.OUTGOING);
                // get single node from specific unit
                Long nodeToQueryID = relsToNodes.iterator().next().getEndNode().getId();
                // if node was not already queried
                if (!queriedNodeIDs.contains(nodeToQueryID)) {
                    // build MATCH clause of the query
                    String matchClause = cypherQuery.getCypherQuery().substring(0, cypherQuery.getInsertPosition());
                    // build RETURN clause of the query
                    String returnClause = cypherQuery.getCypherQuery().substring(cypherQuery.getInsertPosition(), cypherQuery.getCypherQuery().length());
                    // get result from query with single node of specific unit
                    Result result = getPatternIndexUnitsBySingleNode(matchClause, returnClause, cypherQuery.getNodeNames(), nodeToQueryID);
                    // save single result to results container
                    while (result.hasNext()) {
                        results.add(result.next());
                    }
                    // add already queried node to Set, so it is not queried again
                    queriedNodeIDs.add(nodeToQueryID);
                }
            }
            tx.success();
        }
        return results;
    }

    // Method to compose and execute query with single specified node - UNION across all node names and return result
    private Result getPatternIndexUnitsBySingleNode(String matchClause, String returnClause, Set<String> nodes, Long nodeToQueryID) {
        // build query
        String query = "";
        for (String nodeName : nodes) {
            query += matchClause;
            query += " WHERE id(" + nodeName + ")=" + nodeToQueryID + " AND " + composeMetaProtectWhereCondition(nodes);
            query += " " + returnClause + " UNION ";
        }
        query = query.substring(0, query.length() - 6);
        // execute query and return result
        return database.execute(query);
    }

    /* QUERY COMPOSE HELPER METHODS */

    // Method to build WHERE clause as a protection from querying META data (WHERE ALL NOT node:_META_)
    private String composeMetaProtectWhereCondition(Set<String> nodeNames) {
        String condition = "";
        for (String nodeName : nodeNames) {
            condition += "NOT " + nodeName + ":_META_ AND ";
        }
        return condition.substring(0, condition.length() - 5);
    }

    // Method to build RETURN clause with IDs of given nodes and relationships
    private String composeReturnWithIDsOfNamed(Set<String> nodes, Set<String> relationships) {
        String returnClause = "";
        for (String node : nodes) {
            returnClause += "id(" + node + "), ";
        }
        for (String relationship : relationships) {
            returnClause += "id(" + relationship + "), ";
        }
        return returnClause.substring(0, returnClause.length() - 2);
    }

    /* BUILDING INDEX */

    /**
     * Method to build new index based on given query.
     * @param patternQuery instance of PatternQuery, which is parsed MATCH clause of Cypher query for index to be build on.
     * @param patternName name for new index.
     */
    public void buildNewIndex(PatternQuery patternQuery, String patternName) {
        // if pattern does not exists yet
        if (!patternIndexExists(patternQuery.getPatternQuery(), patternName)) {
            // compose query
            String query = "MATCH " + patternQuery.getPatternQuery() + " WHERE ";
            query += composeMetaProtectWhereCondition(patternQuery.getNodeNames()) + " RETURN ";
            query += composeReturnWithIDsOfNamed(patternQuery.getNodeNames(), patternQuery.getRelNames());

            // execute query
            Result result = database.execute(query);
            System.out.println("Execution of original query finished!");
            // build index based on query result
            buildIndex(getUniquePatternIndexUnits(result, patternQuery.getNodeNames(), patternQuery.getRelNames()), patternQuery, patternName);
        } else {
            // TODO inform that index already exists
        }
    }

    // Method to actually create index based on result from query on which index should be built.
    private void buildIndex(Map<String, PatternIndexUnit> patternIndexUnits, PatternQuery patternQuery, String patternName) {
        System.out.println(patternIndexUnits.size());

        try (Transaction tx = database.beginTx()) {
            // create root node of the index
            Node patternRootNode = DatabaseHandler.createNewPatternIndexRoot(database, patternQuery, patternName);
            // create pattern unit node for each of found pattern units
            for (PatternIndexUnit patternIndexUnit : patternIndexUnits.values()) {
                DatabaseHandler.buildNewPatternIndexUnit(database, patternIndexUnit, patternRootNode);
            }
            // create new index of PatternIndex
            PatternIndex patternIndex = new PatternIndex(patternName, patternQuery.getPatternQuery(), patternRootNode,
                    patternQuery.getNodeNames(), patternQuery.getRelNames());
            // save index
            patternIndexes.put(patternIndex.getPatternName(), patternIndex);

            tx.success();
        }
    }

    // TODO - check if pattern index already exists must be robust!
    // Method to check if pattern index given to create already exists - only checks pattern index name and query that is build on.
    private boolean patternIndexExists(String patternQuery, String patternName) {
        for (PatternIndex patternIndex : patternIndexes.values()) {
            if (patternIndex.getPatternQuery().equals(patternQuery) || patternIndex.getPatternName().equals(patternName)) {
                return true;
            }
        }
        return false;
    }

    // Method to get unique pattern index units from result. Unique pattern index unit is identified by nodes in pattern.
    // Instance of PatternIndexUnit keeps all nodes in pattern and all specific units that these nodes carry.
    private Map<String, PatternIndexUnit> getUniquePatternIndexUnits(Result result, Set<String> nodeNames, Set<String> relNames) {
        Map<String, PatternIndexUnit> patternUnits = new HashMap<>();
        // loop over results - row by row
        while (result.hasNext()) {
            Map<String, Object> newSpecificUnit = result.next();
            // get pattern index unit key = sorted nodes by id separated by symbol _
            String patternIndexUnitKey = PatternIndexUnit.getPatternIndexUnitKey(newSpecificUnit, nodeNames);
            // if pattern index unit already exists
            if (patternUnits.containsKey(patternIndexUnitKey)) {
                // update its specific units
                patternUnits.get(patternIndexUnitKey).addSpecificUnit(newSpecificUnit, relNames);
            // if pattern index unit does not exist -> create a new one
            } else {
                PatternIndexUnit patternIndexUnit = new PatternIndexUnit(newSpecificUnit, nodeNames, relNames);
                patternUnits.put(patternIndexUnitKey, patternIndexUnit);
            }
        }
        return patternUnits;
    }


    /* HANDLE DML OPERATIONS */

    /**
     * Method to handle all DML transactions made on the database. Including deleting nodes and relationships, changing
     * properties and labels on nodes and relationships and creating new nodes and relationships.
     * @param itd data that contain all changes on the database.
     */
    public void handleDML(ImprovedTransactionData itd) {

        // if at least one of nodes or relationships was deleted
        if (itd.getAllDeletedNodes().size() != 0 || itd.getAllDeletedRelationships().size() != 0) {
            // handle deleted nodes
            Set<Long> deletedNodeIDs = handleNodeDelete(itd.getAllDeletedNodes(), itd.getAllDeletedRelationships());
            // handle deleted relationships
            handleRelationshipDelete(itd.getAllDeletedRelationships(), deletedNodeIDs);
            // if some of pattern indexes roots were left empty -> delete them
            DatabaseHandler.deleteEmptyIndexes(database, patternIndexes);
        }
        // if at least one of nodes or relationships was created
        if (itd.getAllCreatedNodes().size() != 0 || itd.getAllCreatedRelationships().size() != 0) {
            // handle create
            handleCreate(itd.getAllCreatedRelationships());
        }
        // if at least one of nodes or relationships was changed (including changing and deleting labels or properties)
        if (itd.getAllChangedNodes().size() != 0 || itd.getAllChangedRelationships().size() != 0) {
            // handle change
            handleChange(itd.getAllChangedNodes(), itd.getAllChangedRelationships());
        }
    }

    /* HANDLE CREATE AND CHANGE */

    // Method to handle all created nodes and relationships -> only relationships can affect some pattern index though.
    private void handleCreate(Collection<Relationship> createdRels) {
        Set<Node> affectedNodes = new HashSet<>();
        // get all affected nodes: there are start nodes of created relationships
        for (Relationship createdRel : createdRels) {
            affectedNodes.add(createdRel.getStartNode());
        }
        // update pattern indexes
        updateIndexes(affectedNodes);
    }

    // Method to handle all changed nodes and relationships
    private void handleChange(Collection<Change<Node>> changedNodes, Collection<Change<Relationship>> changedRels) {
        Set<Node> affectedNodes = new HashSet<>();
        // get all affected nodes
        for (Change<Node> changedNode : changedNodes) {
            affectedNodes.add(changedNode.getCurrent());
        }
        // get start nodes of affected relationships
        for (Change<Relationship> changedRel : changedRels) {
            affectedNodes.add(changedRel.getCurrent().getStartNode());
        }
        // update pattern indexes
        updateIndexes(affectedNodes);
    }

    // Main method to update index after create and update changes on the database.
    private void updateIndexes(Set<Node> affectedNodes) {
        // loop over affected nodes (those who where somehow created or changed and those who are incident with changed/created relationships
        for (Node affectedNode : affectedNodes) {
            // get affected node meta relationships
            Iterable<Relationship> affectedNodeMetaRels = affectedNode.getRelationships(RelationshipTypes.PATTERN_INDEX_RELATION, Direction.INCOMING);
            // get pattern index units that affected node is connected to across all pattern indexes
            Set<Node> patternIndexesUnitsOfNode = DatabaseHandler.getStartNodesForRelationships(affectedNodeMetaRels);
            // loop over all pattern indexes
            for (PatternIndex patternIndex : patternIndexes.values()) {
                // get pattern index units for specific pattern index
                Set<Node> patternIndexUnitsOfNode = DatabaseHandler.getPatternIndexUnitsForIndex(patternIndexesUnitsOfNode, patternIndex);
                Set<Node> updatedPatternIndexUnits = new HashSet<>();

                // compose query to get all specific units for specific index around affected node (affected node ID is at all node's positions)
                String matchClause = "MATCH " + patternIndex.getPatternQuery();
                String returnClause = "RETURN " + composeReturnWithIDsOfNamed(patternIndex.getNodeNames(), patternIndex.getRelNames());
                // finish query and execute
                Result result = getPatternIndexUnitsBySingleNode(matchClause, returnClause, patternIndex.getNodeNames(), affectedNode.getId());
                // get unique specific units from result - into PatternIndexUnit instances
                Map<String, PatternIndexUnit> patternUnits = getUniquePatternIndexUnits(result, patternIndex.getNodeNames(), patternIndex.getRelNames());
                // loop over PatternIndexUnit instances (nodes that represent pattern index unit and hold all specific units)
                for (PatternIndexUnit patternUnit : patternUnits.values()) {
                    // get pattern index unit for nodes that represent it (stored in PatternIndexUnit instance)
                    Node indexUnitNode = DatabaseHandler.getPatternIndexUnitForNodes(database,patternIndex, patternUnit.getNodeIDs());
                    // if it does exist (there is a new pattern index unit) -> update pattern index unit
                    if (indexUnitNode != null) {
                        DatabaseHandler.updatePatternIndexUnit(indexUnitNode, patternUnit);
                        updatedPatternIndexUnits.add(indexUnitNode);
                    // if it does not exist -> create new pattern index unit
                    } else {
                        DatabaseHandler.buildNewPatternIndexUnit(database, patternUnit, patternIndex.getRootNode());
                    }
                }

                // Because Set.removeAll(Set) does not work on server!
                // all pattern index units that have connection to affected node minus updated pattern index units
                for (Node updatedPatternIndexUnit : updatedPatternIndexUnits) {
                    patternIndexUnitsOfNode.remove(updatedPatternIndexUnit);
                }
                /**
                 * If some of the pattern index units that have connection to affected node were not updated -> means that
                 * there were no found specific units for those pattern index units (those pattern index units were created but now
                 * do not hold any specific units) -> they need to be deleted.
                 */
                DatabaseHandler.deletePatternIndexUnits(patternIndexUnitsOfNode);
            }
        }
    }


    /* HANDLE DELETE */

    // Method to update indexes if some nodes are deleted in transaction
    private Set<Long> handleNodeDelete(Collection<Node> deletedNodes, Collection<Relationship> deletedRels) {
        // Set that contains all deleted node IDs
        HashSet<Long> deletedNodeIDs = new HashSet<>();

        // if user did not delete META relationships of deleted node -> delete them
        for (Node deletedNode : deletedNodes) {
            Iterator itr = deletedNode.getRelationships(RelationshipTypes.PATTERN_INDEX_RELATION, Direction.INCOMING).iterator();
            while(itr.hasNext()) {
                Relationship metaRel = (Relationship) itr.next();
                metaRel.delete();
            }
        }

        // go over all deleted relationships
        for (Relationship deletedRel : deletedRels) {
            // only if it is META relationship
            if (deletedRel.isType(RelationshipTypes.PATTERN_INDEX_RELATION)) {
                // get deleted node
                Node deletedNode = deletedRel.getEndNode();
                // add deleted node ID to the set
                deletedNodeIDs.add(deletedNode.getId());

                // get pattern index unit node that has relationship to deleted META node
                Node patternIndexUnit = deletedRel.getStartNode();
                // get all unit node relationships
                Iterable<Relationship> unitRels = patternIndexUnit.getRelationships();
                // delete all unit node relationships
                for (Relationship unitRel : unitRels) {
                    unitRel.delete();
                }
                // delete unit node itself
                patternIndexUnit.delete();
            }
        }

        return deletedNodeIDs;
    }

    // Method to update indexes if some relationships are deleted in transaction
    private void handleRelationshipDelete(Collection<Relationship> deletedRels, Set<Long> deletedNodeIDs) {
        // go over all deleted relationships
        for (Relationship deletedRel : deletedRels) {
            // skip META relationships - they should be only deleted when user deletes node with all of its relationships
            // - this situation is already handled in handleNodeDelete method
            if (!deletedRel.isType(RelationshipTypes.PATTERN_INDEX_RELATION)) {
                // get nodes of the relationship to delete
                Long startNodeID = deletedRel.getStartNode().getId();
                Long endNodeID = deletedRel.getEndNode().getId();
                // if relationship is not between nodes, where at least one of them was deleted in this transaction
                // - already handled in handleNodeDelete method
                if (!deletedNodeIDs.contains(startNodeID) && !deletedNodeIDs.contains(endNodeID)) {
                    // get all pattern index units, that have relationships to both nodes of the relationship to delete
                    Long[] incidentNodes = new Long[2];
                    incidentNodes[0] = startNodeID;
                    incidentNodes[1] = endNodeID;
                    Set<Node> patternIndexesUnits = DatabaseHandler.getPatternIndexesUnitsForNodes(database, incidentNodes);

                    // loop over pattern index unit nodes
                    for (Node patternIndexesUnit : patternIndexesUnits) {
                        // if some specific units (across all pattern indexes) were deleted -> remove them from the pattern unit
                        // if the pattern unit has no more specific units -> remove it
                        if (DatabaseHandler.updatePatternIndexUnitOnDelete(patternIndexesUnit, deletedRel.getId()) == 0) {
                            try (Transaction tx = database.beginTx()) {
                                DatabaseHandler.deletePatternIndexUnit(patternIndexesUnit);
                                tx.success();
                            }
                        }
                    }
                }
            }
        }
    }


    /**
     * Method to get all pattern indexes within the database.
     * @return collection of pattern indexes.
     */
    public Map<String, PatternIndex> getPatternIndexes() {
        return patternIndexes;
    }

    // TODO remove - just for testing
    public void printResult(HashSet<Map<String, Object>> results) {
        System.out.println("Total of " + results.size() + " results:");
        Iterator itr = results.iterator();
        while (itr.hasNext()) {
            System.out.println(itr.next().toString());
        }
    }

}
