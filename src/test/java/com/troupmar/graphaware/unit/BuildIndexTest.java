package com.troupmar.graphaware.unit;

import com.troupmar.graphaware.PatternIndexModel;
import com.troupmar.graphaware.PatternQuery;
import com.troupmar.graphaware.exception.InvalidCypherMatchException;
import com.troupmar.graphaware.unit.handlers.Database;
import org.junit.Test;

/**
 * Created by Martin on 19.04.15.
 */

public class BuildIndexTest {

    @Test
    public void buildIndexTest() throws InvalidCypherMatchException {
        Database database = new Database(Database.DB_ZIP_PATH, "zip-still");

        // building index
        String cypherMatch = "(a)-[r]-(b)-[p]-(c)-[q]-(a)";
        PatternQuery patternQuery = new PatternQuery(cypherMatch, database.getDatabase());
        PatternIndexModel model = PatternIndexModel.getInstance(database.getDatabase());
        model.buildNewIndex(patternQuery, "triangle-index");

        database.closeDatabase();
    }
}

