package com.troupmar.graphaware.unit;

import com.troupmar.graphaware.NodeLabels;
import com.troupmar.graphaware.PatternIndexModel;
import com.troupmar.graphaware.PatternQuery;
import com.troupmar.graphaware.exception.InvalidCypherMatchException;
import com.troupmar.graphaware.unit.handlers.Database;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import java.util.regex.Pattern;

/**
 * Created by Martin on 05.04.15.
 */
public class CreateNodeTest {

    private static final String DB_PATH     = "data/graph5-8.db";
    private static final String DB_ZIP_PATH = DB_PATH + ".zip";


    @Test
    public void checkNodePattern() throws InvalidCypherMatchException {
        Database database = new Database(DB_PATH, "still");

        Result result = database.getDatabase().execute("MATCH (n) RETURN COUNT(*)");
        System.out.println(result.resultAsString());

        String cypher = "(a)-[r]-(b)-[p]-(c)-[q]-(a)";
        createNewRootNode("test", "test", database.getDatabase());

        result = database.getDatabase().execute("MATCH (n) RETURN COUNT(*)");
        System.out.println(result.resultAsString());

        database.closeDatabase();
    }

    private Node createNewRootNode(String patternQuery, String patternName, GraphDatabaseService database) {
        Node node = null;
        Transaction tx = database.beginTx();
        try {
            node = database.createNode();
            node.addLabel(NodeLabels._META_);
            node.addLabel(NodeLabels.PATTERN_INDEX_ROOT);
            node.setProperty("patternQuery", patternQuery);
            node.setProperty("patternName", patternName);
            tx.success();
        } catch (RuntimeException e) {
            // TODO Log exception and handle return
            tx.failure();
        } finally {
            tx.close();
        }
        return node;
    }
}
