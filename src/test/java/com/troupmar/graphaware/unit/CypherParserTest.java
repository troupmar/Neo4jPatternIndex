package com.troupmar.graphaware.unit;

import com.troupmar.graphaware.PatternIndexModel;
import com.troupmar.graphaware.PatternQuery;
import com.troupmar.graphaware.exception.InvalidCypherMatchException;
import com.troupmar.graphaware.unit.handlers.Database;
import org.junit.Test;
import org.neo4j.graphdb.Result;

import java.util.regex.Pattern;

/**
 * Created by Martin on 05.04.15.
 */
public class CypherParserTest {

    private static final String DB_PATH = "data/graph1000-5000.db.zip";

    @Test
    public void checkNodePattern() throws InvalidCypherMatchException {
        Database database = new Database(DB_PATH);
        Result result = database.getDatabase().execute("MATCH (n) RETURN COUNT(*)");
        System.out.println(result.resultAsString());

        //String cypher = "  (   a)-[r]-(bla {name: 'trik'} )-[ff]- (aa:Person)-[pp:Person]-(a), (kk),   (j),(l:Person)  ";
        String cypher = "(a)-[r]-(b)-[p]-(c)-[q]-(a)";

        PatternQuery patternQuery = new PatternQuery(cypher, database.getDatabase());
        PatternIndexModel patternIndexModel = PatternIndexModel.getInstance(database.getDatabase());
        patternIndexModel.buildNewIndex(patternQuery);
        //patternQuery.getAllPatternUnits();

        //CypherQuery.validateCypherMatch(cypher, database.getDatabase());
        //System.out.println(CypherQuery.getNodeNamesFromQuery(cypher));

        //database.closeDatabase();
    }
}
