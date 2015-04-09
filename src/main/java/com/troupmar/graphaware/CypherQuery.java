package com.troupmar.graphaware;

import com.troupmar.graphaware.exception.InvalidCypherMatchException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import java.util.LinkedHashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Martin on 09.04.15.
 */
public class CypherQuery extends QueryParser {

    private static final String PATTERN_QUERY_PART  = "MATCH([\\S\\s]*?)(WHERE|RETURN)";

    private String cypherQuery;

    public CypherQuery(String cypherQuery, GraphDatabaseService database) throws InvalidCypherMatchException {
        nodeNames = new LinkedHashSet<String>();
        relNames = new LinkedHashSet<String>();

        if (! validateQuery(cypherQuery, database)) {
            throw new InvalidCypherMatchException();
        }

        setNamesFromCypher(cypherQuery);
        this.cypherQuery = cypherQuery.trim();
    }

    @Override
    protected boolean validateQuery(String cypherQuery, GraphDatabaseService database) {
        // TODO EXPLAIN needs to be in transaction, why?
        boolean valid;
        Transaction tx = database.beginTx();
        try {
            database.execute("EXPLAIN " + cypherQuery);
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

    private void setNamesFromCypher(String cypherQuery) throws InvalidCypherMatchException {
        Pattern pattern = Pattern.compile(PATTERN_QUERY_PART, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(cypherQuery);

        if (matcher.find()) {
            setNamesFromCypherMatch(matcher.group(1));
        }
    }

    public String getCypherQuery() {
        return cypherQuery;
    }
}
