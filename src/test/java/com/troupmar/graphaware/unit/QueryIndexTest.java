package com.troupmar.graphaware.unit;

import com.troupmar.graphaware.CypherQuery;
import com.troupmar.graphaware.PatternIndexModel;
import com.troupmar.graphaware.PrintTypes;
import com.troupmar.graphaware.exception.InvalidCypherException;
import com.troupmar.graphaware.exception.InvalidCypherMatchException;
import com.troupmar.graphaware.exception.PatternIndexNotFoundException;
import com.troupmar.graphaware.unit.handlers.Database;
import org.junit.Test;

import java.util.HashSet;
import java.util.Map;

/**
 * Created by Martin on 19.04.15.
 */

public class QueryIndexTest {

    @Test
    public void queryIndexTest() throws InvalidCypherException, InvalidCypherMatchException, PatternIndexNotFoundException {
        Database database = new Database(Database.DB_PATH, "still");

        // querying index
        String query = "MATCH (a)-[r]-(b)-[p]-(c)-[q]-(a) RETURN id(a), b, c";
        CypherQuery cypherQuery = new CypherQuery(query, database.getDatabase());

        PatternIndexModel model = PatternIndexModel.getInstance(database.getDatabase());
        HashSet<Map<String, Object>> result = model.getResultFromIndex(cypherQuery, "triangle-index");

        String resultString = model.resultToString(result, PrintTypes.JSON);
        System.out.println(resultString);

        database.closeDatabase();
    }
}

