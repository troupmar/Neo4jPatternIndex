package com.troupmar.graphaware.unit;

import com.troupmar.graphaware.PatternIndexModel;
import com.troupmar.graphaware.PatternQuery;
import com.troupmar.graphaware.exception.InvalidCypherMatchException;
import com.troupmar.graphaware.exception.PatternIndexAlreadyExistsException;
import com.troupmar.graphaware.handlers.Database;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;

/**
 * Created by Martin on 19.04.15.
 */

public class BuildIndexTest {

    @Test
    public void buildIndexTest() throws InvalidCypherMatchException, PatternIndexAlreadyExistsException {

        GraphDatabaseService database = Database.loadDatabaseFromZipFile(Database.DB_ZIP_PATH, null);

        // building index
        //String cypherMatch = "(a:Person)-[r]-(b:Person)-[p]-(c:Person)-[q]-(a:Person)";

        // triangle
        String cypherMatch = "(a)-[r]-(b)-[p]-(c)-[q]-(a)";
        // movie pattern
        //String cypherMatch = "(a)-[r]-(b)-[p]-(c)-[q]-(a)-[x]-(e)";
        // transaction pattern
        //String cypherMatch = "(a)-[e]-(b)-[f]-(c)-[g]-(a)-[h]-(d)-[i]-(b)";

        PatternQuery patternQuery = new PatternQuery(cypherMatch, database);
        PatternIndexModel model = PatternIndexModel.getInstance(database);
        // triangle
        model.buildNewIndex(patternQuery, "triangle-index");
        // movie pattern
        //model.buildNewIndex(patternQuery, "movie-index");
        // transaction pattern
        //model.buildNewIndex(patternQuery, "transaction-index");

        Database.closeDatabase(database, null);

    }
}

