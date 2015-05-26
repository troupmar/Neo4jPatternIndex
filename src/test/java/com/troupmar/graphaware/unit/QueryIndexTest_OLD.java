package com.troupmar.graphaware.unit;

import com.troupmar.graphaware.exception.InvalidCypherException;
import com.troupmar.graphaware.exception.InvalidCypherMatchException;
import com.troupmar.graphaware.exception.PatternIndexNotFoundException;
import com.troupmar.graphaware.handlers.Database;
import org.junit.Test;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;

/**
 * Created by Martin on 19.04.15.
 */

public class QueryIndexTest_OLD {

    @Test
    public void queryIndexTest() throws InvalidCypherException, InvalidCypherMatchException, PatternIndexNotFoundException {
        //GraphDatabaseService database = Database.loadDatabaseFromFile(Database.DB_PATH, null);
        GraphDatabaseService database = Database.loadTemporaryDatabaseFromZipFile(Database.DB_ZIP_PATH, Database.getNewTemporaryFolder(), null);

        // querying index
        String query = "PROFILE MATCH (a)-[r]-(b)-[p]-(c)-[q]-(a) RETURN a, b, c";
        //String query = "MATCH (a)-[f]-(b)-[g]-(c)-[h]-(d)-[i]-(e)-[j]-(a) RETURN a,b,c,d,e";
        //CypherQuery cypherQuery = new CypherQuery(query, database);

        //PatternIndexModel model = PatternIndexModel.getInstance(database);
        //HashSet<Map<String, Object>> result = model.getResultFromIndex(cypherQuery, "triangle-index");

        //String resultString = model.resultToString(result, PrintTypes.SOUT);
        Result result = database.execute(query);
        /*while (result.hasNext()) {
            result.next();
        }*/
        System.out.println(result.resultAsString());

        ExecutionPlanDescription description = result.getExecutionPlanDescription();
        Long dbHits = description.getProfilerStatistics().getDbHits();
        System.out.println(dbHits);

        //System.out.println("Number of results: " + result.size());

        Database.closeDatabase(database, null);
    }
}

