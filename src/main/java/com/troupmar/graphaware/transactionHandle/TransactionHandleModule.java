package com.troupmar.graphaware.transactionHandle;

import com.graphaware.runtime.config.FluentTxDrivenModuleConfiguration;
import com.graphaware.runtime.config.TxDrivenModuleConfiguration;
import com.graphaware.runtime.module.BaseTxDrivenModule;
import com.graphaware.runtime.module.DeliberateTransactionRollbackException;
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
    private TxDrivenModuleConfiguration config;

    protected TransactionHandleModule(String moduleId, GraphDatabaseService database) {
        super(moduleId);
        this.database = database;
        this.config = FluentTxDrivenModuleConfiguration.defaultConfiguration().with(new MetaNodeInclusionPolicy());
    }

    @Override
    public TxDrivenModuleConfiguration getConfiguration() {
        return config;
    }

    // TODO DEBUG!!
    @Override
    public Void beforeCommit(ImprovedTransactionData improvedTransactionData) throws DeliberateTransactionRollbackException {

        if (! onlyMetaRelsCreated(improvedTransactionData)) {
            /*
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
                */

                // handle DML operations
                patternIndexModel.handleDML(improvedTransactionData);

            /*
                bufferWritter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            */
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

    private boolean onlyMetaRelsCreated(ImprovedTransactionData improvedTransactionData) {
        Collection<Relationship> createdRels = improvedTransactionData.getAllCreatedRelationships();
        if (createdRels.size() == 0) {
            return false;
        }
        for (Relationship createdRel : createdRels) {
            if (! createdRel.isType(RelationshipTypes.PATTERN_INDEX_RELATION)) {
                return false;
            }
        }
        return true;
    }
}
