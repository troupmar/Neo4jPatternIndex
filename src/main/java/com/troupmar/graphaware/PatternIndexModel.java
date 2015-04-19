package com.troupmar.graphaware;


import com.troupmar.graphaware.exception.PatternIndexNotFoundException;
import org.neo4j.graphdb.*;

import java.io.BufferedWriter;
import java.io.IOException;
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
        // if pattern index to query on does not exist ...
        if (! patternIndexes.containsKey(patternName)) {
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
                    // get result from query with single node of specific unit
                    addSingleUnitResult(cypherQuery, nodeToQueryID, results);
                    // add already queried node to Set, so it is not queried again
                    queriedNodeIDs.add(nodeToQueryID);
                }
            }
            tx.success();
        }
        return results;
    }

    // Method to create query with single specified node - UNION across all node names
    public void addSingleUnitResult(CypherQuery cypherQuery, Long nodeToQueryID, HashSet<Map<String, Object>> results) {
        // build query
        String query = "";
        for (String nodeName : cypherQuery.getNodeNames()) {
            query += cypherQuery.getCypherQuery().substring(0, cypherQuery.getInsertPosition());
            query += buildWhereConditionWithSingleNode(cypherQuery.getNodeNames(), nodeName, nodeToQueryID);
            query += cypherQuery.getCypherQuery().substring(cypherQuery.getInsertPosition(), cypherQuery.getCypherQuery().length());
            query += " UNION ";
        }
        query = query.substring(0, query.length() - 7);
        // execute query
        Result result = database.execute(query);
        // save single result to results container
        while (result.hasNext()) {
            results.add(result.next());
        }

    }
    // Method to build WHERE condition containing ID of specified node and protection from querying META data
    private String buildWhereConditionWithSingleNode(Set<String> nodeNames, String nodeNameToQuery, Long nodeIDsToQuery) {
        String condition = " WHERE ";
        condition += "id(" + nodeNameToQuery + ")=" + nodeIDsToQuery + " ";
        for (String nodeName : nodeNames) {
            condition += "AND NOT " + nodeName + ":_META_ ";
        }
        return condition;
    }

    private String buildWhereCondition(Set<String> nodeNames, Map<String, Long> nodesToQuery) {
        String condition = " WHERE ";
        for (Map.Entry<String, Long> nodeToQuery : nodesToQuery.entrySet()) {
            condition += "id(" + nodeToQuery.getKey() + ")=" + nodeToQuery.getValue() + " AND ";
        }
        condition = condition.substring(0, condition.length() - 4);
        for (String nodeName : nodeNames) {
            condition += "AND NOT " + nodeName + ":_META_ ";
        }
        return condition;
    }

    /* Building index */
    // build new index based on given query
    public void buildNewIndex(PatternQuery patternQuery, String patternName) {
        // if pattern does not exists yet
        if (! patternIndexExists(patternQuery.getPatternQuery(), patternName)) {
            // compose query
            String query = "MATCH " + patternQuery.getPatternQuery() + " WHERE ";

            String returnStatement = "";
            for (String node : patternQuery.getNodeNames()) {
                query += "NOT " + node + ":_META_ AND ";
                returnStatement += "id(" + node + "), ";
            }
            for (String rel : patternQuery.getRelsWithNodes().keySet()) {
                returnStatement += "id(" + rel + "), ";
            }

            query = query.substring(0, query.length() - 5) + " RETURN " + returnStatement.substring(0, returnStatement.length() - 2);
            // execute query
            Result result = database.execute(query);
            System.out.println("Execution of original query finished!");
            // build index based on query result
            buildIndex(getPatternUnits(result, patternQuery.getNodeNames(), patternQuery.getRelsWithNodes().keySet()), patternQuery, patternName);
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
            // create pattern unit node for each pattern
            for (PatternUnit patternUnit : patternUnits.values()) {
                Node patternUnitNode = DatabaseHandler.createNewUnitNode(database, patternUnit);
                patternRootNode.createRelationshipTo(patternUnitNode, RelationshipTypes.PATTERN_INDEX_RELATION);
                for (Long nodeID : patternUnit.getNodeIDs()) {
                    patternUnitNode.createRelationshipTo(database.getNodeById(nodeID), RelationshipTypes.PATTERN_INDEX_RELATION);
                }
            }
            // create new index of PatternIndex
            PatternIndex patternIndex = new PatternIndex(patternName, patternQuery.getPatternQuery(), patternRootNode, patternUnits.size(), patternQuery.getRelsWithNodes());
            // TODO patternIndexes must be alredy initialized here! - should be done in start method (TransactionHandleModule)
            // save index
            patternIndexes.put(patternIndex.getPatternName(), patternIndex);

            tx.success();
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
    private Map<String, PatternUnit> getPatternUnits(Result result, Set<String> nodeNames, Set<String> relNames) {
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

    public Map<String, PatternIndex> getPatternIndexes() {
        return patternIndexes;
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

    // Method to handle delete of relationships or nodes in transaction
    public void handleDelete(BufferedWriter writer, Collection<Node> deletedNodes, Collection<Relationship> deletedRels) throws IOException {

        HashSet<Long> deletedNodeIDs = handleNodeDelete(deletedNodes, deletedRels);
        for (Long NodeID : deletedNodeIDs) {
            writer.write(NodeID + " ");
        }
        handleRelationshipDelete(deletedRels, deletedNodeIDs);
    }

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

    private void handleRelationshipDelete(Collection<Relationship> deletedRels, HashSet<Long> deletedNodeIDs) {
        // go over all deleted relationships
        for (Relationship deletedRel : deletedRels) {
            // skip META relationships - they should be only deleted when user deletes node with all of its relationships
            // - this situation is already handled in handleNodeDelete method
            if (! deletedRel.isType(RelationshipTypes.PATTERN_INDEX_RELATION)) {
                // get nodes of the relationship to delete
                Long startNodeID = deletedRel.getStartNode().getId();
                Long endNodeID = deletedRel.getEndNode().getId();
                // if relationship is not between nodes, where at least one of them was deleted in this transaction
                // - already handled in handleNodeDelete method
                if (! deletedNodeIDs.contains(startNodeID) && ! deletedNodeIDs.contains(endNodeID)) {
                    // get all META node IDs, that have relationships to both nodes of the relationship to delete
                    Result result = database.execute("MATCH (a)--(b)--(c) WHERE id(a)=" + startNodeID +
                            " AND id(c)=" + endNodeID + " AND b:_META_ RETURN id(b)");
                    // for each of these META node IDs
                    while (result.hasNext()) {
                        // get the actual META node - pattern unit node
                        Node unitNode = DatabaseHandler.getNodeById(database, (Long) result.next().get("id(b)"));
                        // if some specific units (across all pattern indexes) were deleted -> remove them from the pattern unit
                        // if the pattern unit has no more specific units -> remove it
                        if (DatabaseHandler.updateSpecificUnits(database, unitNode, deletedRel.getId()) == 0) {
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

    /* TODO from here down handleCreate - describe and test */
    /*
    public void handleCreate(Collection<Relationship> createdRels) {
        for (Relationship createdRel : createdRels) {
            Node startNode = createdRel.getStartNode();
            Node endNode = createdRel.getEndNode();
            Result numOfRels = database.execute("MATCH (a)-[r]-(b) WHERE id(a)=" + startNode.getId() + " AND id(b)=" + endNode.getId() + " RETURN count(r)");

            if ((Long) numOfRels.next().get("count(r)") == 1) {
                for (PatternIndex patternIndex : patternIndexes.values()) {
                    newRelUpdateIndex(patternIndex, startNode.getId(), endNode.getId());
                }
            }
        }
    }


    private void newRelUpdateIndex(PatternIndex patternIndex, Long startNode, Long endNode) {

        Set<String> nodeNames = getNodeNamesFromIndex(patternIndex.getRelsWithNodes());
        Result result = database.execute(buildUpdateIndexQuery(patternIndex, nodeNames, startNode, endNode));
        List<Object[]> patternUnits = getPatternUnits(result, nodeNames);

        Node patternUnitNode;
        for (Object[] patternUnit : patternUnits) {
            patternUnitNode = DatabaseHandler.createNewUnitNode(database);
            DatabaseHandler.createRelationship(database, patternIndex.getRootNode(), patternUnitNode, RelationshipTypes.PATTERN_INDEX_RELATION);

            for (Object nodeID : patternUnit) {
                DatabaseHandler.createRelationship(database, patternUnitNode, (Long) nodeID, RelationshipTypes.PATTERN_INDEX_RELATION);
            }
        }
    }

    private String buildUpdateIndexQuery(PatternIndex patternIndex, Set<String> nodeNames, Long startNode, Long endNode) {
        String query = "";
        for (Map.Entry<String, String[]> patternRel :patternIndex.getRelsWithNodes().entrySet()) {
            Map<String, Long> nodesToQuery = new HashMap<String, Long>();
            nodesToQuery.put(patternRel.getValue()[0], startNode);
            nodesToQuery.put(patternRel.getValue()[1], endNode);

            query += "MATCH " + patternIndex.getPatternQuery();
            query += buildWhereCondition(nodeNames, nodesToQuery);
            query += "RETURN ";
            for (String nodeName : nodeNames) {
                query += "id(" + nodeName + "), ";
            }
            query = query.substring(0, query.length() - 2);
            query += " UNION ";
        }
        return query.substring(0, query.length() - 7);

    }
    */

    /**
     * I can get all node names from relsWithNodes, because of the fact, that pattern is meant to be set of
     * connected nodes, which means, that each node must have at least one relationship.
     */

    private Set<String> getNodeNamesFromIndex(Map<String, String[]> relsWithNodes) {
        Set<String> nodeNames = new HashSet<String>();
        for (String[] nodesOfRel : relsWithNodes.values()) {
            nodeNames.add(nodesOfRel[0]);
            nodeNames.add(nodesOfRel[1]);
        }
        return nodeNames;
    }

    public void printResult(HashSet<Map<String, Object>> results) {
        System.out.println("Total of " + results.size() + " results:");
        Iterator itr = results.iterator();
        while (itr.hasNext()) {
            System.out.println(itr.next().toString());
        }
    }

}
