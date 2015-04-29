package com.troupmar.graphaware;

import com.troupmar.graphaware.exception.InvalidCypherException;
import com.troupmar.graphaware.exception.InvalidCypherMatchException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import java.util.LinkedHashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class provides some methods needed to parse a Cypher query.
 * It holds names of nodes and relationships that are parsed from MATCH clause of Cypher query given by user.
 *
 * Created by Martin on 09.04.15.
 */
public class CypherQuery extends QueryParser {
    private String cypherQuery;
    private int insertPosition;

    /**
     * The constructor validates Cypher query given by user and sets all names of nodes and relationships that are present
     * in the MATCH clause of the query.
     * @param cypherQuery Cypher query given by user.
     * @param database any database, so the query can be validated.
     * @throws InvalidCypherException
     * @throws InvalidCypherMatchException
     */
    public CypherQuery(String cypherQuery, GraphDatabaseService database) throws InvalidCypherException, InvalidCypherMatchException {
        nodeNames        = new LinkedHashSet<>();
        relNames         = new LinkedHashSet<>();
        this.cypherQuery = cypherQuery;

        //validateQuery(cypherQuery, database);
        if (! hasValidRelationships(cypherQuery)) {
            throw new InvalidCypherException();
        }
        setNamesFromCypher(cypherQuery);

        if (nodeIDsQueried(cypherQuery)) {
            throw new InvalidCypherException();
        }
    }

    // Pattern indexes do not allow user to have IDs of nodes ind RETURN clause of the query, so this method
    // checks if the condition was not broken.
    protected boolean nodeIDsQueried(String patternQuery) {
        Pattern pattern = Pattern.compile("WHERE(.*)RETURN", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(patternQuery);

        if (! matcher.find()) {
            return false;
        }
        String whereCondition = matcher.group(1);

        pattern = Pattern.compile("id\\((.*?)\\)");
        matcher = pattern.matcher(whereCondition);

        while (matcher.find()) {
            if (nodeNames.contains(matcher.group(1))) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected void validateQuery(String cypherQuery, GraphDatabaseService database) {
        try (Transaction tx = database.beginTx()) {
            database.execute("EXPLAIN " + cypherQuery);
            tx.success();
        }
    }

    // This method parses MATCH clause from Cypher query.
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
