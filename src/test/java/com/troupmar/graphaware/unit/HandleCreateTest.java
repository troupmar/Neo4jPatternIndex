package com.troupmar.graphaware.unit;

import com.troupmar.graphaware.PatternIndexModel;
import com.troupmar.graphaware.exception.InvalidCypherException;
import com.troupmar.graphaware.exception.InvalidCypherMatchException;
import com.troupmar.graphaware.exception.PatternIndexNotFoundException;
import com.troupmar.graphaware.handlers.Database;
import com.troupmar.graphaware.handlers.CreateTransactionHandler;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;

/**
 * Created by Martin on 05.04.15.
 */

public class HandleCreateTest {

    @Test
    public void handleDeleteTest() throws InvalidCypherMatchException, InvalidCypherException, PatternIndexNotFoundException {

        //Database database = new Database(Database.DB_PATH, LoadTypes.STILL);
        GraphDatabaseService database = Database.loadDatabaseFromFile(Database.DB_PATH, null);
        PatternIndexModel model = PatternIndexModel.getInstance(database);
        database.registerTransactionEventHandler(new CreateTransactionHandler(model));

        //database.getDatabase().execute("MATCH (n {name:'Charles Reid'}), (m {name:'Kian Hurst'}) CREATE m-[r:NEW2]->n");
        //database.getDatabase().execute("MATCH (n {name:'Harriet Thornton'}), (m {name:'Francesca Russell'}) CREATE m-[r:NEW3]->n");

        //database.getDatabase().execute("CREATE (m:MY {name:'Martin'})");
        //database.getDatabase().execute("MATCH (m:MY {name:'Martin'}), (n:Person {name:'Kian Hurst'}) CREATE m-[r:NEW]->n");
        //database.getDatabase().execute("MATCH (m:MY {name:'Martin'}), (n:Person {name:'Francesca Russell'}) CREATE m-[r:NEW]->n");
        //database.getDatabase().execute("MATCH (n:MY {name:'Martin'}) REMOVE n:MY SET n:Person");

        //database.getDatabase().execute("MATCH (n {name:'Thomas Jennings'})-[r]-(m {name:'Harriet Thornton'}) DELETE r");
        //database.getDatabase().execute("MATCH (n {name:'Harriet Thornton'}), (m {name:'Francesca Russell'}) CREATE m-[r:NEW3]->n");
        model.removePatternIndexByName("triangle-index");

        //String cypher = "  (   a)-[r]-(bla {name: 'trik'} )-[ff]- (aa:Person)-[pp:Person]-(a), (kk),   (j),(l:Person)  ";

        Database.closeDatabase(database, null);
    }
}