package com.troupmar.graphaware.transactionHandle;

import com.graphaware.runtime.module.BaseTxDrivenModule;
import com.graphaware.runtime.module.DeliberateTransactionRollbackException;
import com.graphaware.tx.event.improved.api.Change;
import com.graphaware.tx.event.improved.api.ImprovedTransactionData;
import org.neo4j.graphdb.Node;

import java.io.*;
import java.util.Collection;

/**
 * Created by Martin on 11.03.15.
 */
public class TransactionHandleModule extends BaseTxDrivenModule<Void> {

    protected TransactionHandleModule(String moduleId) {
        super(moduleId);
    }

    @Override
    public Void beforeCommit(ImprovedTransactionData improvedTransactionData) throws DeliberateTransactionRollbackException {
        try {
            File file =new File("log-transaction-data.txt");
            //if file doesnt exists, then create it
            if(!file.exists()){
                file.createNewFile();
            }

            //true = append file
            FileWriter fileWritter = null;
            fileWritter = new FileWriter(file.getName(),true);

            BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
            Collection<Node> nodes = improvedTransactionData.getAllCreatedNodes();
            bufferWritter.write("Created: ");
            for (Node node : nodes) {
                bufferWritter.write(node.getId() + " ");
            }
            bufferWritter.write("\n");
            bufferWritter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
