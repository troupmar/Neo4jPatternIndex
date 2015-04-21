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
        nodeNames = new LinkedHashSet<>();
        relNames = new LinkedHashSet<>();

        validateQuery(patternQuery, database);
        if (! hasValidRelationships(patternQuery)) {
            throw new InvalidCypherMatchException();
        }

        setNamesFromCypherMatch(patternQuery);
        this.patternQuery = patternQuery.trim();
    }

    @Override
    protected void validateQuery(String cypherQuery, GraphDatabaseService database) {
        // TODO when not in transaction - there is rollback of all operations while building index
        try (Transaction tx = database.beginTx()) {
            database.execute("EXPLAIN MATCH " + cypherQuery + " RETURN count(*)");
            tx.success();
        }
    }

    public String getPatternQuery() {
        return patternQuery;
    }
}
