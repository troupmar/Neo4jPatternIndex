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

            // handle deleted nodes and relationships
            patternIndexModel.handleDelete(improvedTransactionData.getAllDeletedNodes(), improvedTransactionData.getAllDeletedRelationships());
            // after delete was handled - check and delete index if it has no units
            patternIndexModel.deleteEmptyIndexes();

            // handle created nodes and relationships
            patternIndexModel.handleCreate(improvedTransactionData.getAllCreatedRelationships());


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
