package com.troupmar.graphaware;

import com.graphaware.tx.event.improved.api.ImprovedTransactionData;
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
        // time
        long totalTime = System.nanoTime();

        if (! patternIndexes.containsKey(patternName)) {
            throw new PatternIndexNotFoundException();
        }
        HashSet<Map<String, Object>> results = new HashSet<Map<String, Object>>();
        HashSet<Long> queriedNodeIDs = new HashSet<Long>();

        Node rootNode = patternIndexes.get(patternName).getRootNode();
        Iterable<Relationship> relsToUnits = DatabaseHandler.getRelationships(database, rootNode, Direction.OUTGOING);

        Node unitNode;
        Long nodeToQueryID;
        Iterable<Relationship> relsToNodes;
        Transaction tx = database.beginTx();

        // time
        long queryTime = 0;
        long time;

        for (Relationship rel : relsToUnits) {
            unitNode = rel.getEndNode();
            relsToNodes = DatabaseHandler.getRelationships(database, unitNode, Direction.OUTGOING);
            nodeToQueryID = relsToNodes.iterator().next().getEndNode().getId();
            if (! queriedNodeIDs.contains(nodeToQueryID)) {
                // time
                time = System.nanoTime();

                addSingleUnitResult(cypherQuery, nodeToQueryID, results);
                // time
                time = System.nanoTime() - time;
                queryTime += time;

                queriedNodeIDs.add(nodeToQueryID);
            }
        }

        // time
        totalTime = System.nanoTime() - totalTime;
        System.out.println("Time of total elapsed: " + totalTime);
        System.out.println("Time of query elapsed: " + queryTime);

        return results;
    }

    public void addSingleUnitResult(CypherQuery cypherQuery, Long nodeToQueryID, HashSet<Map<String, Object>> results) {
        String query = "";
        for (String nodeName : cypherQuery.getNodeNames()) {
            query += cypherQuery.getCypherQuery().substring(0, cypherQuery.getInsertPosition());
            query += buildWhereCondition(cypherQuery.getNodeNames(), nodeName, nodeToQueryID);
            query += cypherQuery.getCypherQuery().substring(cypherQuery.getInsertPosition(), cypherQuery.getCypherQuery().length());
            query += " UNION ";
        }
        query = query.substring(0, query.length() - 7);
        Result result = database.execute(query);
        while (result.hasNext()) {
            results.add(result.next());
        }

    }
    private String buildWhereCondition(Set<String> nodeNames, String nodeNameToQuery, Long nodeIDsToQuery) {
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
    public void buildNewIndex(PatternQuery patternQuery, String patternName) {
        if (! patternIndexExists(patternQuery.getPatternQuery(), patternName)) {
            String query = "MATCH " + patternQuery.getPatternQuery() + " WHERE ";

            String returnStatement = "";
            for (String node : patternQuery.getNodeNames()) {
                query += "NOT " + node + ":_META_ AND ";
                returnStatement += "id(" + node + "), ";
            }

            query = query.substring(0, query.length() - 5) + " RETURN " + returnStatement.substring(0, returnStatement.length() - 2);

            Result result = database.execute(query);
            System.out.println("Execution of original query finished!");
            buildIndex(getPatternUnits(result, /*patternQuery*/patternQuery.getNodeNames()), patternQuery, patternName);
        } else {
            // TODO inform that index already exists
        }
    }

    private void buildIndex(List<Object[]> patternUnits, PatternQuery patternQuery, String patternName) {
        System.out.println(patternUnits.size());
        Node patternRootNode = DatabaseHandler.createNewRootNode(database, patternQuery, patternName, patternUnits.size());
        Node patternUnitNode;

        for (Object[] patternUnit : patternUnits) {
            patternUnitNode = DatabaseHandler.createNewUnitNode(database);
            DatabaseHandler.createRelationship(database, patternRootNode, patternUnitNode, RelationshipTypes.PATTERN_INDEX_RELATION);

            for (Object nodeID : patternUnit) {
                DatabaseHandler.createRelationship(database, patternUnitNode, (Long) nodeID, RelationshipTypes.PATTERN_INDEX_RELATION);
            }
        }
        PatternIndex patternIndex = new PatternIndex(patternName, patternQuery.getPatternQuery(), patternRootNode, patternUnits.size(), patternQuery.getRelsWithNodes());
        // TODO patternIndexes must be alredy initialized here! - should be done in start method (TransactionHandleModule)
        patternIndexes.put(patternIndex.getPatternName(), patternIndex);
    }

    // TODO - check if pattern index already exists must be robust!
    private boolean patternIndexExists(String patternQuery, String patternName) {
        for (PatternIndex patternIndex : patternIndexes.values()) {
            if (patternIndex.getPatternQuery().equals(patternQuery) || patternIndex.getPatternName().equals(patternName)) {
                return true;
            }
        }
        return false;
    }

    // TODO optimize!
    private List<Object[]> getPatternUnits(Result result, Set<String> nodeNames) {
        List patternUnits = new ArrayList<Object[]>();
        Set<String> uniquePatternUnitKeys = new HashSet<String>();
        String key;
        while (result.hasNext()) {
            Object[] patternUnit = getPatternUnitIDs(result.next(), nodeNames);
            key = getPatternUnitKey(patternUnit);
            if (! uniquePatternUnitKeys.contains(key)) {
                patternUnits.add(patternUnit);
                uniquePatternUnitKeys.add(key);
            }
        }
        return patternUnits;
    }

    private Object[] getPatternUnitIDs(Map<String, Object> patternUnit, Set<String> nodeNames) {
        Object[] nodes = new Object[nodeNames.size()];
        int i = 0;
        for (String nodeName : nodeNames) {
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

    public Map<String, PatternIndex> getPatternIndexes() {
        return patternIndexes;
    }

    public void removePatternIndex(String patternIndexName) {
        patternIndexes.remove(patternIndexName);
    }

    public void deleteEmptyIndexes() {
        // loop over pattern indexes
        for (Map.Entry<String, PatternIndex> entry : patternIndexes.entrySet()) {
            // if root node of pattern index does not have any relationships
            if (! entry.getValue().getRootNode().getRelationships(Direction.OUTGOING).iterator().hasNext()) {
                // remove pattern index
                patternIndexes.remove(entry.getKey());
                // delete root node
                DatabaseHandler.deleteNode(database, entry.getValue().getRootNode());
            }
        }
    }

    public void handleDelete(Collection<Node> deletedNodes, Collection<Relationship> deletedRels) {
        // store deleted meta relationships within this program block
        HashSet<Long> deletedMetaRels = new HashSet<Long>();
        // store deleted node IDs into set, so it can be queried easier
        HashSet<Long> deletedNodeSet = new HashSet<Long>();
        for (Node deletedNode : deletedNodes) {
            deletedNodeSet.add(deletedNode.getId());
        }

        // go over all deleted relationships
        for (Relationship deletedRel : deletedRels) {
            // if relationship to delete is of META type and was not deleted yet by this program block
            if (deletedRel.isType(RelationshipTypes.PATTERN_INDEX_RELATION) && ! deletedMetaRels.contains(deletedRel.getId())) {
                // get pattern unit node of deleted META relationship
                Node unitNode = deletedRel.getStartNode();
                // get all unit node relationships
                Iterable<Relationship> unitNodeRels = unitNode.getRelationships();
                // delete all unit node relationships and store those to deleted meta relationships within this program block
                for (Relationship unitNodeRel : unitNodeRels) {
                    deletedMetaRels.add(unitNodeRel.getId());
                    DatabaseHandler.deleteRelationship(database, unitNodeRel);
                }
                // delete unit node itself
                DatabaseHandler.deleteNode(database, unitNode);
                // if relationship to delete is not of META type
            } else {
                // get nodes of the relationship to delete
                Long startNodeID = deletedRel.getStartNode().getId();
                Long endNodeID = deletedRel.getEndNode().getId();
                // if relationship is not between nodes, where at least one of them was deleted in this transaction (block above)
                if (! deletedNodeSet.contains(startNodeID) && ! deletedNodeSet.contains(endNodeID)) {
                    // get all META node IDs, that have relationships to both nodes of the relationship to delete
                    Result result = database.execute("MATCH (a)--(b)--(c) WHERE id(a)=" + startNodeID +
                            " AND id(c)=" + endNodeID + " AND b:_META_ RETURN id(b)");
                    // for each of these META node IDs
                    while (result.hasNext()) {
                        // get the actual META node - pattern unit node
                        Node unitNode = DatabaseHandler.getNodeById(database, (Long) result.next().get("id(b)"));
                        // get all relationships of this unit node
                        Iterable<Relationship> unitNodeRels = DatabaseHandler.getRelationships(database, unitNode, Direction.BOTH);
                        // delete tall ynit node relationships
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

    /* TODO from here down handleCreate - describe and test */
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
