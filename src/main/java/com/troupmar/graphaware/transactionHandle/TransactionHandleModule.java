package com.troupmar.graphaware.transactionHandle;

import com.graphaware.runtime.module.BaseTxDrivenModule;
import com.graphaware.runtime.module.DeliberateTransactionRollbackException;
import com.graphaware.tx.event.improved.api.Change;
import com.graphaware.tx.event.improved.api.ImprovedTransactionData;
import com.troupmar.graphaware.*;
import org.neo4j.graphdb.*;

import java.io.*;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by Martin on 11.03.15.
 */
public class TransactionHandleModule extends BaseTxDrivenModule<Void> {

    private GraphDatabaseService database;
    private PatternIndexModel patternIndexModel;

    protected TransactionHandleModule(String moduleId, GraphDatabaseService database) {
        super(moduleId);
        this.database = database;
    }

    // TODO DEBUG!!
    @Override
    public Void beforeCommit(ImprovedTransactionData improvedTransactionData) throws DeliberateTransactionRollbackException {

        // TODO prevent not to execute all of this when building new index (adding new meta nodes...)
        try {
            File file = new File("log-transaction-data.txt");
            //if file doesnt exists, then create it
            if (!file.exists()) {
                file.createNewFile();
            }
            //true = append file
            FileWriter fileWritter = null;
            fileWritter = new FileWriter(file.getName(), true);
            BufferedWriter bufferWritter = new BufferedWriter(fileWritter);

            // get all deleted relationships
            Collection<Relationship> deletedRels = improvedTransactionData.getAllDeletedRelationships();
            // get all deleted nodes
            Collection<Node> deletedNodes = improvedTransactionData.getAllDeletedNodes();
            // store deleted meta relationships within this program block
            HashSet<Long> deletedMetaRels = new HashSet<Long>();
            // store non-meta relationships, that are incident with deleted nodes
            HashSet<Long> deletedNodeSet = new HashSet<Long>();

            // get IDs of all deleted nodes
            for (Node deletedNode : deletedNodes) {
                deletedNodeSet.add(deletedNode.getId());
            }

            // go over all deleted relationships
            for (Relationship deletedRel : deletedRels) {
                // if relationship to delete is of META type and was not deleted yet by this program block
                if (deletedRel.isType(RelationshipTypes.PATTERN_INDEX_RELATION) && ! deletedMetaRels.contains(deletedRel.getId())) {
                    bufferWritter.write("Block of pattern relationship.\n");
                    // get pattern unit node
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
                    bufferWritter.write("Block of classic relationship.\n");
                    // get nodes of the relationship to delete
                    Long startNodeID = deletedRel.getStartNode().getId();
                    Long endNodeID = deletedRel.getEndNode().getId();
                    // if relationship is not between nodes, where at least one of them was deleted in this transaction
                    if (! deletedNodeSet.contains(startNodeID) && ! deletedNodeSet.contains(endNodeID)) {
                        bufferWritter.write("Processing classic relationship.\n");
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

            // get all existing pattern indexes
            Map<String, PatternIndex> patternIndexes = patternIndexModel.getPatternIndexes();
            // loop over pattern indexes
            for (Map.Entry<String, PatternIndex> entry : patternIndexes.entrySet()) {
                // if root node of pattern index does not have any relationships
                if (! entry.getValue().getRootNode().getRelationships(Direction.OUTGOING).iterator().hasNext()) {
                    // remove pattern index
                    patternIndexModel.removePatternIndex(entry.getKey());
                    // delete root node
                    DatabaseHandler.deleteNode(database, entry.getValue().getRootNode());
                }
            }


            bufferWritter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }


    @Override
    public void start(GraphDatabaseService database) {
        // load pattern indexes when database starts
        patternIndexModel = PatternIndexModel.getInstance(database);
    }

    public void writeDownResult(BufferedWriter bufferedWriter, HashSet<Map<String, Object>> results) throws IOException {
        System.out.println("Total of " + results.size() + " results:");
        Iterator itr = results.iterator();
        while (itr.hasNext()) {
            bufferedWriter.write(itr.next().toString());
        }
    }

    private boolean deletedMetaOnly(ImprovedTransactionData improvedTransactionData) {
        Collection<Relationship> deletedRels = improvedTransactionData.getAllDeletedRelationships();
        for (Relationship deletedRel : deletedRels) {
            if (! deletedRel.isType(RelationshipTypes.PATTERN_INDEX_RELATION)) {
                return false;
            }
        }
        Collection<Node> deletedNodes = improvedTransactionData.getAllDeletedNodes();
        for (Node deletedNode : deletedNodes) {
            if (! deletedNode.hasLabel(NodeLabels._META_)) {
                return false;
            }
        }
        return true;
    }
}
