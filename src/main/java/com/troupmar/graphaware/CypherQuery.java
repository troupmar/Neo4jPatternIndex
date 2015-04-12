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
    private String cypherQuery;
    private int insertPosition;

    public CypherQuery(String cypherQuery, GraphDatabaseService database) throws InvalidCypherException, InvalidCypherMatchException {
        nodeNames = new LinkedHashSet<String>();
        relNames  = new LinkedHashMap<String, String[]>();
        this.cypherQuery = cypherQuery;

        if (! isQueryValid(cypherQuery, database) || ! hasValidRelationships(cypherQuery) || nodeIDsQueried(cypherQuery)) {
            throw new InvalidCypherException();
        }
        setNamesFromCypher(cypherQuery);

        if (nodeIDsQueried(cypherQuery)) {
            throw new InvalidCypherException();
        }
    }

    protected boolean nodeIDsQueried(String patternQuery) {
        Pattern pattern = Pattern.compile("id\\((.*?)\\)");
        Matcher matcher = pattern.matcher(patternQuery);

        while (matcher.find()) {
            for (String nodeName : nodeNames) {
                if (matcher.group(1).equals(nodeName)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    protected boolean isQueryValid(String cypherQuery, GraphDatabaseService database) {
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
        Pattern pattern = Pattern.compile("MATCH([\\S\\s]*?)(WHERE|RETURN)", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(cypherQuery);

        if (matcher.find()) {
            if (matcher.group(2).trim().equalsIgnoreCase("WHERE")) {
                // Removing first WHERE from user's query
                String leftSubstr  = cypherQuery.substring(0, matcher.start(2));
                String rightSubstr = cypherQuery.substring(matcher.end(2), cypherQuery.length());
                this.cypherQuery = leftSubstr + "AND" + rightSubstr;
            }

            insertPosition = matcher.start(2);
            setNamesFromCypherMatch(matcher.group(1));
        }
    }


    public String getCypherQuery() {
        return cypherQuery;
    }

    public int getInsertPosition() {
        return insertPosition;
    }
}
