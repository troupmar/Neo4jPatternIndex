package com.troupmar.graphaware.unit;

import com.troupmar.graphaware.CypherQuery;
import com.troupmar.graphaware.PatternIndexModel;
import com.troupmar.graphaware.PatternQuery;
import com.troupmar.graphaware.exception.InvalidCypherException;
import com.troupmar.graphaware.exception.InvalidCypherMatchException;
import com.troupmar.graphaware.unit.handlers.Database;
import org.junit.Test;
import org.neo4j.graphdb.Result;

/**
 * Created by Martin on 05.04.15.
 */
public class CypherParserTest {

    private static final String DB_PATH     = "data/graph5-8.db";
    private static final String DB_ZIP_PATH = DB_PATH + ".zip";


    @Test
    public void checkNodePattern() throws InvalidCypherMatchException, InvalidCypherException {
        //Database database = new Database(DB_ZIP_PATH, "zip-still");
        Database database = new Database(DB_PATH, "still");
        Result result = database.getDatabase().execute("MATCH (n) RETURN COUNT(*)");
        System.out.println(result.resultAsString());

        //String cypher = "  (   a)-[r]-(bla {name: 'trik'} )-[ff]- (aa:Person)-[pp:Person]-(a), (kk),   (j),(l:Person)  ";

        //String cypherMatch = "(a)-[r]-(b)-[p]-(c)-[q]-(a)";
        String query = "MATCH (a)-[r]-(b)-[p]-(c)-[q]-(a) WHERE id(b)=1 RETURN a, b, c";

        CypherQuery cypherQuery = new CypherQuery(query, database.getDatabase());

        //PatternIndexModel model = PatternIndexModel.getInstance(database.getDatabase());
        //PatternQuery patternQuery = new PatternQuery(cypherMatch, database.getDatabase());
        //model.buildNewIndex(patternQuery, "triangle-index");

        database.closeDatabase();

    }
}
