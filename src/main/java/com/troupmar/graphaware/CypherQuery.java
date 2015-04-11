package com.troupmar.graphaware;

import com.troupmar.graphaware.exception.InvalidCypherException;
import com.troupmar.graphaware.exception.InvalidCypherMatchException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Martin on 09.04.15.
 */
public class CypherQuery extends QueryParser {

    private static final String PATTERN_QUERY_PART  = "MATCH([\\S\\s]*?)(WHERE|RETURN)";

    private String cypherQuery;
    private int insertPosition;

    public CypherQuery(String cypherQuery, GraphDatabaseService database) throws InvalidCypherException, InvalidCypherMatchException {
        nodeNames = new LinkedHashSet<String>();
        relNames  = new LinkedHashMap<String, String[]>();

        if (! validateQuery(cypherQuery, database) || ! checkValidRelationships(cypherQuery)) {
            throw new InvalidCypherException();
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

    private void setNamesFromCypher(String cypherQuery) throws InvalidCypherException, InvalidCypherMatchException {
        Pattern pattern = Pattern.compile(PATTERN_QUERY_PART, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(cypherQuery);

        if (matcher.find()) {
            if (matcher.group(2).trim().equalsIgnoreCase("WHERE")) {
                // Removing first WHERE from user's query
                String leftSubstr  = cypherQuery.substring(0, matcher.start(2));
                String rightSubstr = cypherQuery.substring(matcher.end(2), cypherQuery.length());
                this.cypherQuery = leftSubstr + rightSubstr;
            }
            insertPosition = matcher.start(2);
            setNamesFromCypherMatch(matcher.group(1));
        }
    }


    public String getCypherQuery() {
        return cypherQuery;
    }
}
