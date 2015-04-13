package com.troupmar.graphaware.unit;

import com.troupmar.graphaware.RelationshipTypes;
import com.troupmar.graphaware.unit.handlers.Database;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import java.util.Map;

/**
 * Created by Martin on 08.04.15.
 */
/*
public class ResultAfterCreateTest {
    private static final String DB_PATH     = "data/graph5-8.db";
    private static final String DB_ZIP_PATH = DB_PATH + ".zip";

    @Test
    public void test() {
        Database database = new Database(DB_ZIP_PATH, "zip-still");

        Result result = database.getDatabase().execute("MATCH (a)--(b)--(c)--(a) RETURN id(a), id(b), id(c)");

        Map<String, Object> row = result.next();
        Transaction tx = database.getDatabase().beginTx();
        try {
            Node metaNode = database.getDatabase().createNode();
            metaNode.createRelationshipTo(database.getDatabase().getNodeById((Long) row.get("id(a)")), RelationshipTypes.PATTERN_INDEX_RELATION);
            metaNode.createRelationshipTo(database.getDatabase().getNodeById((Long) row.get("id(b)")), RelationshipTypes.PATTERN_INDEX_RELATION);
            metaNode.createRelationshipTo(database.getDatabase().getNodeById((Long) row.get("id(c)")), RelationshipTypes.PATTERN_INDEX_RELATION);
            tx.success();
        } catch (RuntimeException e) {
            tx.failure();
        } finally {
            tx.close();
        }


        // TODO Why is new data included in result ?!!
        System.out.println(result.resultAsString());

        database.closeDatabase();
    }
}
*/
