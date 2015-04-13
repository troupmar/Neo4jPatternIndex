package com.troupmar.graphaware.transactionHandle;

import com.graphaware.runtime.module.BaseTxDrivenModule;
import com.graphaware.runtime.module.DeliberateTransactionRollbackException;
import com.graphaware.tx.event.improved.api.Change;
import com.graphaware.tx.event.improved.api.ImprovedTransactionData;
import com.troupmar.graphaware.DatabaseHandler;
import com.troupmar.graphaware.NodeLabels;
import com.troupmar.graphaware.RelationshipTypes;
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

    protected TransactionHandleModule(String moduleId, GraphDatabaseService database) {
        super(moduleId);
        this.database = database;
    }

    // TODO DEBUG!!
    @Override
    public Void beforeCommit(ImprovedTransactionData improvedTransactionData) throws DeliberateTransactionRollbackException {

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

            if (! deletedMetaOnly(improvedTransactionData)) {
                // Removing nodes
                HashSet<Long> incidentRels = new HashSet<Long>();
                if (improvedTransactionData.getAllDeletedNodes().size() != 0) {
                    Collection<Node> deletedNodes = improvedTransactionData.getAllDeletedNodes();
                    for (Node deletedNode : deletedNodes) {
                        Iterable<Relationship> deletedNodeRels = deletedNode.getRelationships(Direction.BOTH);
                        for (Relationship deletedNodeRel : deletedNodeRels) {
                            if (deletedNodeRel.isType(RelationshipTypes.PATTERN_INDEX_RELATION)) {
                                Node unitNode = deletedNodeRel.getStartNode(); // check - can be EndNode!
                                Iterable<Relationship> unitNodeRels = unitNode.getRelationships(Direction.BOTH, RelationshipTypes.PATTERN_INDEX_RELATION);
                                for (Relationship unitNodeRel : unitNodeRels) {
                                    DatabaseHandler.deleteRelationship(database, unitNodeRel);
                                }
                                DatabaseHandler.deleteNode(database, unitNode);
                            } else {
                                incidentRels.add(deletedNodeRel.getId());
                            }
                        }
                    }
                }

                // Removing relationships
                if (improvedTransactionData.getAllDeletedRelationships().size() != 0) {
                    Collection<Relationship> deletedRels = improvedTransactionData.getAllDeletedRelationships();

                    for (Relationship deletedRel : deletedRels) {
                        if (! incidentRels.contains(deletedRel.getId())) {
                            Node startNode = deletedRel.getStartNode();
                            Node endNode = deletedRel.getEndNode();

                            Result result = database.execute("MATCH (a)--(b)--(c) WHERE id(a)=" + startNode.getId() +
                                    " AND id(c)=" + endNode.getId() + " AND b:_META_ RETURN id(b)");

                            while (result.hasNext()) {
                                Node unitNode = DatabaseHandler.getNodeById(database, (Long) result.next().get("id(b)"));
                                Iterable<Relationship> UnitNodeRels = DatabaseHandler.getRelationships(database, unitNode, Direction.BOTH);
                                for (Relationship unitNodeRel : UnitNodeRels) {
                                    DatabaseHandler.deleteRelationship(database, unitNodeRel);
                                }
                                DatabaseHandler.deleteNode(database, unitNode);
                            }
                        }
                    }
                }
            }

            // TODO check if roots have at least one relationship
            // TODO method to check new patterns


            bufferWritter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }


    @Override
    public void start(GraphDatabaseService database) {
        //loadIndexRoots();
        //super.start(database);
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
