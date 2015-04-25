package com.troupmar.graphaware;

import com.troupmar.graphaware.exception.InvalidCypherMatchException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import java.util.LinkedHashSet;

/**
 * This class provides methods to parse MATCH clause of Cypher query.
 * It holds names of nodes and relationships that are parsed from MATCH clause of Cypher query given by user.
 *
 * Created by Martin on 09.04.15.
 */
public class PatternQuery extends QueryParser {

    private String patternQuery;

    /**
     * The constructor validates MATCH clause given by user and sets all names of nodes and relationships that are present
     * in the query.
     * @param patternQuery MATCH clause of Cypher query given by user.
     * @param database any database, so the query can be validated.
     * @throws InvalidCypherMatchException
     */
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
