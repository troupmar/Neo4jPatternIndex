package com.troupmar.graphaware;

import com.troupmar.graphaware.exception.InvalidCypherMatchException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import java.util.LinkedHashSet;

/**
 * Created by Martin on 09.04.15.
 */
public class PatternQuery extends QueryParser {

    private String patternQuery;

    public PatternQuery(String patternQuery, GraphDatabaseService database) throws InvalidCypherMatchException {
        nodeNames = new LinkedHashSet<String>();
        relNames = new LinkedHashSet<String>();

        if (! validateQuery(patternQuery, database)) {
            throw new InvalidCypherMatchException();
        }

        setNamesFromCypherMatch(patternQuery);
        this.patternQuery = patternQuery.trim();
    }

    @Override
    protected boolean validateQuery(String cypherQuery, GraphDatabaseService database) {
        // TODO EXPLAIN needs to be in transaction, why?
        boolean valid;
        Transaction tx = database.beginTx();
        try {
            database.execute("EXPLAIN MATCH " + cypherQuery + " RETURN count(*)");
            valid = true;
            tx.success();
        } catch (RuntimeException e) {
            valid = false;
            tx.failure();
        } finally {
            tx.close();
        }
        return valid;
    }

    public String getPatternQuery() {
        return patternQuery;
    }
}
