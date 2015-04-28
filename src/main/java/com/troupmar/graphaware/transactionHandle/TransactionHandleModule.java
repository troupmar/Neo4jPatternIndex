package com.troupmar.graphaware.transactionHandle;

import com.esotericsoftware.minlog.Log;
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

    @Override
    public Void beforeCommit(ImprovedTransactionData improvedTransactionData) throws DeliberateTransactionRollbackException {
        if (! onlyMetaRelsCreated(improvedTransactionData) && patternIndexModel.getPatternIndexes().size() != 0) {
            patternIndexModel.handleDML(improvedTransactionData);
        }
        return null;
    }


    @Override
    public void start(GraphDatabaseService database) {
        // load pattern indexes when database starts
        patternIndexModel = PatternIndexModel.getInstance(database);
    }

    /**
     * Method to check if only meta relationships were created in the transaction. It returns true only if all created relationships
     * are of type PATTERN_INDEX_RELATION. That means that during that kind of transaction we are building index and nothing should be
     * done in beforeCommit.
     */
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
