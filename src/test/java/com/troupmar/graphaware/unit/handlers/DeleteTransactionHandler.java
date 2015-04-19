package com.troupmar.graphaware.unit.handlers;

import com.graphaware.tx.event.improved.api.ImprovedTransactionData;
import com.graphaware.tx.event.improved.api.LazyTransactionData;
import com.troupmar.graphaware.PatternIndexModel;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;

/**
 * Created by Martin on 19.04.15.
 */
public class DeleteTransactionHandler extends TransactionEventHandler.Adapter<Void> {
    private Database database;
    private PatternIndexModel model;

    public DeleteTransactionHandler(Database database, PatternIndexModel model) {
        this.database = database;
        this.model = model;
    }

    @Override
    public Void beforeCommit(TransactionData data) throws Exception {
        ImprovedTransactionData improvedTransactionData = new LazyTransactionData(data);

        //model.handleDelete(improvedTransactionData.getAllDeletedNodes(), improvedTransactionData.getAllDeletedRelationships());

        return null;
    }
}
