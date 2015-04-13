package com.troupmar.graphaware.transactionHandle;

import com.graphaware.runtime.module.RuntimeModule;
import com.graphaware.runtime.module.RuntimeModuleBootstrapper;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.Map;

/**
 * Created by Martin on 11.03.15.
 */
public class TransactionHandleBootstrapper implements RuntimeModuleBootstrapper {

    @Override
    public RuntimeModule bootstrapModule(String moduleId, Map<String, String> config, GraphDatabaseService database) {
        return new TransactionHandleModule(moduleId, database);
    }
}
