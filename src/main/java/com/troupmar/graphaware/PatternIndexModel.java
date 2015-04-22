package com.troupmar.graphaware;


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
        this.patternIndexes = DatabaseHandler.getPatternIndexRoots(database);
    }

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

    public static void destroy() {
        synchronized (mutex) {
            instance = null;
        }
    }

    /* QUERY WITH INDEX */

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
        // get root relationships
        Iterable<Relationship> relsToUnits = DatabaseHandler.getRelationships(database, rootNode, Direction.OUTGOING);

        try (Transaction tx = database.beginTx()) {
            Iterable<Relationship> relsToNodes;
            Node unitNode;

            // go over all relationships from root to pattern unit nodes
            for (Relationship rel : relsToUnits) {
                // get pattern unit node
                unitNode = rel.getEndNode();
                // get relationships from pattern unit node to nodes of specific unit
                relsToNodes = unitNode.getRelationships(Direction.OUTGOING);
                // get single node from specific unit
                Long nodeToQueryID = relsToNodes.iterator().next().getEndNode().getId();
                // if node was not already queried
                if (!queriedNodeIDs.contains(nodeToQueryID)) {
                    // build MATCH clause of the query
                    String matchClause = cypherQuery.getCypherQuery().substring(0, cypherQuery.getInsertPosition());
                    // build RETURN clause of the query
                    String returnClause = cypherQuery.getCypherQuery().substring(cypherQuery.getInsertPosition(), cypherQuery.getCypherQuery().length());
                    // get result from query with single node of specific unit
                    Result result = getPatternUnitsBySingleNode(matchClause, returnClause, cypherQuery.getNodeNames(), nodeToQueryID);
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

    // Method to create query with single specified node - UNION across all node names and return result
    public Result getPatternUnitsBySingleNode(String matchClause, String returnClause, Set<String> nodes, Long nodeToQueryID) {
        // build query
        String query = "";
        for (String nodeName : nodes) {
            query += matchClause;
            query += " WHERE id(" + nodeName + ")=" + nodeToQueryID + " AND " + composeMetaWhereCondition(nodes);//buildWhereConditionWithSingleNode(nodes, nodeName, nodeToQueryID) + " ";
            query += " " + returnClause + " UNION ";
        }
        query = query.substring(0, query.length() - 6);
        // execute query and return result
        return database.execute(query);
    }

    /* QUERY COMPOSE HELPER METHODS */
    // Method to build WHERE clause as a protection from querying META data
    private String composeMetaWhereCondition(Set<String> nodeNames/*, String nodeNameToQuery, Long nodeIDsToQuery*/) {
        //String condition = " WHERE ";
        String condition = "";
        //condition += "id(" + nodeNameToQuery + ")=" + nodeIDsToQuery + " ";
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
    // build new index based on given query
    public void buildNewIndex(PatternQuery patternQuery, String patternName) {
        // if pattern does not exists yet
        if (!patternIndexExists(patternQuery.getPatternQuery(), patternName)) {
            // compose query
            String query = "MATCH " + patternQuery.getPatternQuery() + " WHERE ";
            query += composeMetaWhereCondition(patternQuery.getNodeNames()) + " RETURN ";
            query += composeReturnWithIDsOfNamed(patternQuery.getNodeNames(), patternQuery.getRelNames());

            // execute query
            Result result = database.execute(query);
            System.out.println("Execution of original query finished!");
            // build index based on query result
            buildIndex(getUniquePatternUnits(result, patternQuery.getNodeNames(), patternQuery.getRelNames()), patternQuery, patternName);
        } else {
            // TODO inform that index already exists
        }
    }

    // Method to create index based on query
    private void buildIndex(Map<String, PatternUnit> patternUnits, PatternQuery patternQuery, String patternName) {
        System.out.println(patternUnits.size());

        try (Transaction tx = database.beginTx()) {
            // create root node of the index
            Node patternRootNode = DatabaseHandler.createNewRootNode(database, patternQuery, patternName, patternUnits.size());
            // create pattern unit node for each of found pattern units
            for (PatternUnit patternUnit : patternUnits.values()) {
                createPatternUnitNode(patternUnit, patternRootNode);
            }
            // create new index of PatternIndex
            PatternIndex patternIndex = new PatternIndex(patternName, patternQuery.getPatternQuery(), patternRootNode,
                    patternQuery.getNodeNames(), patternQuery.getRelNames(), patternUnits.size());
            // TODO patternIndexes must be alredy initialized here! - should be done in start method (TransactionHandleModule)
            // save index
            patternIndexes.put(patternIndex.getPatternName(), patternIndex);

            tx.success();
        }
    }

    private void createPatternUnitNode(PatternUnit patternUnit, Node patternRootNode) {
        Node patternUnitNode = DatabaseHandler.createNewUnitNode(database, patternUnit);
        patternRootNode.createRelationshipTo(patternUnitNode, RelationshipTypes.PATTERN_INDEX_RELATION);
        for (Long nodeID : patternUnit.getNodeIDs()) {
            patternUnitNode.createRelationshipTo(database.getNodeById(nodeID), RelationshipTypes.PATTERN_INDEX_RELATION);
        }
    }

    // TODO - check if pattern index already exists must be robust!
    // Method to check if pattern index given to create already exists
    private boolean patternIndexExists(String patternQuery, String patternName) {
        for (PatternIndex patternIndex : patternIndexes.values()) {
            if (patternIndex.getPatternQuery().equals(patternQuery) || patternIndex.getPatternName().equals(patternName)) {
                return true;
            }
        }
        return false;
    }

    // TODO optimize!
    // Method to process query result (query to build index on): save node IDs and relationship IDs and reduce automorphism
    private Map<String, PatternUnit> getUniquePatternUnits(Result result, Set<String> nodeNames, Set<String> relNames) {
        Map<String, PatternUnit> patternUnits = new HashMap<>();
        while (result.hasNext()) {
            Map<String, Object> newSpecificUnit = result.next();
            String patternUnitKey = PatternUnit.getPatternUnitKey(newSpecificUnit, nodeNames);
            if (patternUnits.containsKey(patternUnitKey)) {
                patternUnits.get(patternUnitKey).addSpecificUnit(newSpecificUnit, relNames);
            } else {
                PatternUnit patternUnit = new PatternUnit(newSpecificUnit, nodeNames, relNames);
                patternUnits.put(patternUnitKey, patternUnit);
            }
        }
        return patternUnits;
    }


    /* HANDLE DML OPERATIONS */
    // Method to handle delete of relationships or nodes in transaction
    public void handleDML(ImprovedTransactionData itd) {

        if (itd.getAllDeletedNodes().size() != 0 || itd.getAllDeletedRelationships().size() != 0) {
            Set<Long> deletedNodeIDs = handleNodeDelete(itd.getAllDeletedNodes(), itd.getAllDeletedRelationships());
            handleRelationshipDelete(itd.getAllDeletedRelationships(), deletedNodeIDs);
            deleteEmptyIndexes();
        }
        if (itd.getAllCreatedNodes().size() != 0 || itd.getAllCreatedRelationships().size() != 0) {
            handleCreate(itd.getAllCreatedRelationships());
        }
        if (itd.getAllChangedNodes().size() != 0 || itd.getAllChangedRelationships().size() != 0) {
            handleChange(itd.getAllChangedNodes(), itd.getAllChangedRelationships());
        }
    }

    private void handleCreate(Collection<Relationship> createdRels) {
        Set<Node> affectedNodes = new HashSet<>();
        for (Relationship createdRel : createdRels) {
            affectedNodes.add(createdRel.getStartNode());
        }
        updateIndexes(affectedNodes);
    }

    private void handleChange(Collection<Change<Node>> changedNodes, Collection<Change<Relationship>> changedRels) {
        Set<Node> affectedNodes = new HashSet<>();
        for (Change<Node> changedNode : changedNodes) {
            affectedNodes.add(changedNode.getCurrent());
        }
        for (Change<Relationship> changedRel : changedRels) {
            affectedNodes.add(changedRel.getCurrent().getStartNode());
        }
        updateIndexes(affectedNodes);
    }

    // TODO review and debug
    public void updateIndexes(Set<Node> affectedNodes) {
        for (Node affectedNode : affectedNodes) {
            for (PatternIndex patternIndex : patternIndexes.values()) {
                String matchClause = "MATCH " + patternIndex.getPatternQuery();
                String returnClause = "RETURN " + composeReturnWithIDsOfNamed(patternIndex.getNodeNames(), patternIndex.getRelNames());

                Result result = getPatternUnitsBySingleNode(matchClause, returnClause, patternIndex.getNodeNames(), affectedNode.getId());
                Map<String, PatternUnit> patternUnits = getUniquePatternUnits(result, patternIndex.getNodeNames(), patternIndex.getRelNames());
                for (PatternUnit patternUnit : patternUnits.values()) {
                    Node indexUnitNode = getIndexUnitNodeForNodes(patternIndex, patternUnit.getNodeIDs());
                    if (indexUnitNode != null) {
                        DatabaseHandler.updatePatternUnitOnCreate(indexUnitNode, patternUnit);
                    } else {
                        createPatternUnitNode(patternUnit, patternIndex.getRootNode());
                    }
                }
            }
        }
    }

    private Node getIndexUnitNodeForNodes(PatternIndex patternIndex, Long[] nodeIDs) {
        Iterable<Relationship> metaRelsOfNode = database.getNodeById(nodeIDs[0]).getRelationships(RelationshipTypes.PATTERN_INDEX_RELATION, Direction.INCOMING);
        Set<Node> commonMetaNodes = getStartNodesForRelationships(metaRelsOfNode);

        if (nodeIDs.length > 1) {
            for (int i = 1; i < nodeIDs.length; i++) {
                metaRelsOfNode = database.getNodeById(nodeIDs[i]).getRelationships(RelationshipTypes.PATTERN_INDEX_RELATION, Direction.INCOMING);
                Set<Node> metaNodes = getStartNodesForRelationships(metaRelsOfNode);
                for (Node commonMetaNode : commonMetaNodes) {
                    if (!metaNodes.contains(commonMetaNode)) {
                        commonMetaNodes.remove(commonMetaNode);
                    }
                }
            }
        }
        for (Node commonMetaNode : commonMetaNodes) {
            Relationship relToRoot = commonMetaNode.getSingleRelationship(RelationshipTypes.PATTERN_INDEX_RELATION, Direction.INCOMING);
            if (relToRoot.getStartNode().getId() == patternIndex.getRootNode().getId()) {
                return commonMetaNode;
            }
        }
        return null;
    }

    private Set<Node> getStartNodesForRelationships(Iterable<Relationship> relationships) {
        Set<Node> startNodes = new HashSet<>();
        Iterator itr = relationships.iterator();
        while (itr.hasNext()) {
            Relationship nextRel = (Relationship) itr.next();
            startNodes.add(nextRel.getStartNode());
        }
        return startNodes;
    }

    /* HANDLE DELETE */
    // Method to update indexes if some nodes are deleted in transaction
    private HashSet<Long> handleNodeDelete(Collection<Node> deletedNodes, Collection<Relationship> deletedRels) {
        // Set that contains all deleted node IDs
        HashSet<Long> deletedNodeIDs = new HashSet<>();

        // if user did not delete META relationships of deleted node -> delete them
        for (Node deletedNode : deletedNodes) {
            for (Relationship metaRelOfNode : DatabaseHandler.getMetaRelsOfNode(deletedNode)) {
                deletedRels.add(metaRelOfNode);
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

                // get pattern unit node that has relationship to deleted META node
                Node unitNode = deletedRel.getStartNode();
                // get all unit node relationships
                Iterable<Relationship> unitNodeRels = unitNode.getRelationships();
                // delete all unit node relationships
                for (Relationship unitNodeRel : unitNodeRels) {
                    unitNodeRel.delete();
                }
                // delete unit node itself
                unitNode.delete();
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
                    // get all META node IDs, that have relationships to both nodes of the relationship to delete
                    Result result = database.execute("MATCH (a)--(b)--(c) WHERE id(a)=" + startNodeID +
                            " AND id(c)=" + endNodeID + " AND b:_META_ RETURN id(b)");
                    // for each of these META node IDs
                    while (result.hasNext()) {
                        // get the actual META node - pattern unit node
                        Node unitNode = DatabaseHandler.getNodeById(database, (Long) result.next().get("id(b)"));
                        // if some specific units (across all pattern indexes) were deleted -> remove them from the pattern unit
                        // if the pattern unit has no more specific units -> remove it
                        if (DatabaseHandler.updatePatternUnitOnDelete(unitNode, deletedRel.getId()) == 0) {
                            // get all relationships of this unit node
                            Iterable<Relationship> unitNodeRels = DatabaseHandler.getRelationships(database, unitNode, Direction.BOTH);
                            // delete all unit node relationships
                            for (Relationship unitNodeRel : unitNodeRels) {
                                DatabaseHandler.deleteRelationship(database, unitNodeRel);
                            }
                            // delete unit node itself
                            DatabaseHandler.deleteNode(database, unitNode);
                        }
                    }
                }
            }
        }
    }

    // Method to delete empty pattern index root nodes
    public void deleteEmptyIndexes() {
        // loop over pattern indexes
        for (Map.Entry<String, PatternIndex> entry : patternIndexes.entrySet()) {
            // if root node of pattern index does not have any relationships
            if (!entry.getValue().getRootNode().getRelationships(Direction.OUTGOING).iterator().hasNext()) {
                // remove pattern index
                patternIndexes.remove(entry.getKey());
                // delete root node
                DatabaseHandler.deleteNode(database, entry.getValue().getRootNode());
            }
        }
    }


    public Map<String, PatternIndex> getPatternIndexes() {
        return patternIndexes;
    }

    public void printResult(HashSet<Map<String, Object>> results) {
        System.out.println("Total of " + results.size() + " results:");
        Iterator itr = results.iterator();
        while (itr.hasNext()) {
            System.out.println(itr.next().toString());
        }
    }

}
