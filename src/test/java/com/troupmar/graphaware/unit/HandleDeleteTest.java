package com.troupmar.graphaware.unit;

import com.troupmar.graphaware.PatternIndexModel;
import com.troupmar.graphaware.exception.InvalidCypherException;
import com.troupmar.graphaware.exception.InvalidCypherMatchException;
import com.troupmar.graphaware.exception.PatternIndexNotFoundException;
import com.troupmar.graphaware.unit.handlers.Database;
import com.troupmar.graphaware.unit.handlers.DeleteTransactionHandler;
import org.junit.Test;
import org.neo4j.graphdb.Result;

/**
 * Created by Martin on 05.04.15.
 */

public class HandleDeleteTest {

    @Test
    public void handleDeleteTest() throws InvalidCypherMatchException, InvalidCypherException, PatternIndexNotFoundException {

        Database database = new Database(Database.DB_PATH, "still");
        PatternIndexModel model = PatternIndexModel.getInstance(database.getDatabase());
        database.getDatabase().registerTransactionEventHandler(new DeleteTransactionHandler(database, model));

        database.getDatabase().execute("MATCH (n {name:'Charles Reid'})-[r]-() DELETE n,r");

        //String cypher = "  (   a)-[r]-(bla {name: 'trik'} )-[ff]- (aa:Person)-[pp:Person]-(a), (kk),   (j),(l:Person)  ";

        database.closeDatabase();
    }
}