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
        String cypherMatch = "(a)-[f]-(b)-[g]-(c)-[h]-(d)-[i]-(e)-[j]-(a)";
        PatternQuery patternQuery = new PatternQuery(cypherMatch, database);
        PatternIndexModel model = PatternIndexModel.getInstance(database);
        model.buildNewIndex(patternQuery, "triangle-index");

        Database.closeDatabase(database, null);

    }
}

