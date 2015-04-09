package com.troupmar.graphaware;

import com.troupmar.graphaware.exception.InvalidCypherMatchException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Transaction;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Martin on 05.04.15.
 */
public class PatternQuery {

    private static final String NODE_REL_PATTERN = "([^->,][^-<,]+)";
    private static final String NO_NAME_REL_PATTERN = "(<?-->?)";

    private String patternQuery;
    private Set<String> nodeNames;
    private Set<String> relNames;

    public PatternQuery(String patternQuery, GraphDatabaseService database) throws InvalidCypherMatchException {
        nodeNames = new LinkedHashSet<String>();
        relNames = new LinkedHashSet<String>();

        if (! validatePatternQuery(patternQuery, database) || ! checkValidRelationships(patternQuery)) {
            throw new InvalidCypherMatchException();
        }
        getNamesFromCypherMatch(patternQuery);
        this.patternQuery = patternQuery.trim();
    }

    private boolean validatePatternQuery(String cypherMatch, GraphDatabaseService database) {
        // TODO EXPLAIN needs to be in transaction, why?
        boolean valid;
        Transaction tx = database.beginTx();
        try {
            database.execute("EXPLAIN MATCH " + cypherMatch + " RETURN count(*)");
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

    // In case I need relationships - if I dont need them - I can allow -- and <-- and -->
    private boolean checkValidRelationships(String patternQuery) {
        Pattern pattern = Pattern.compile(NO_NAME_REL_PATTERN);
        Matcher matcher = pattern.matcher(patternQuery);

        if (matcher.find()) {
            return false;
        }
        return true;

    }

    private void getNamesFromCypherMatch(String patternQuery) throws InvalidCypherMatchException {
        Pattern pattern = Pattern.compile(NODE_REL_PATTERN);
        Matcher matcher = pattern.matcher(patternQuery);

        String item;
        while (matcher.find()) {
            item = matcher.group();
            item = item.replaceAll("\\s+", " ").trim();
            if (item.charAt(0) == '(') {
                nodeNames.add(getNodeRelName(item));
            } else if (item.charAt(0) == '[') {
                relNames.add(getNodeRelName(item));
            } else {
                nodeNames.add(parseNodeRelName(item));
            }
        }
    }

    private String getNodeRelName(String item) throws InvalidCypherMatchException {
        item = item.substring(1).trim();
        if (item.charAt(0) == '{' || item.charAt(0) == ':' || item.charAt(0) == ')' || item.charAt(0) == ']') {
            throw new InvalidCypherMatchException();
        }
        return parseNodeRelName(item);
    }

    private String parseNodeRelName(String item) {
        int i = 0;
        while (i<item.length()) {
            if (item.charAt(i) == ' ' || item.charAt(i) == ':' || item.charAt(i) == ')' || item.charAt(i) == ']') {
                break;
            }
            i++;
        }
        return item.substring(0, i);
    }

    public String getPatternQuery() {
        return patternQuery;
    }

    public void setPatternQuery(String patternQuery) {
        this.patternQuery = patternQuery;
    }

    public Set<String> getNodeNames() {
        return nodeNames;
    }

    public void setNodeNames(Set<String> nodeNames) {
        this.nodeNames = nodeNames;
    }

    public Set<String> getRelNames() {
        return relNames;
    }

    public void setRelNames(Set<String> relNames) {
        this.relNames = relNames;
    }

    /*
    private void getNodeNamesFromQuery(String cypher, GraphDatabaseService database) throws Exception {
        if (validateCypherMatch(cypher, database)) {
            Pattern pattern = Pattern.compile(NODE_PATTERN);
            Matcher matcher = pattern.matcher(cypher);

            String node;
            int pointer = 0;
            while (matcher.find()) {
                node = matcher.group();
                node = node.substring(1, node.length()).replaceAll("\\s+", " ").trim();
                if (node.charAt(0) == '{' || node.charAt(0) == ')') {
                    String id = "n" + UUID.randomUUID().toString().replace("-", "n");
                    cypher = new StringBuilder(cypher).insert(matcher.start() + 1 + pointer, id + " ").toString();
                    pointer = id.length() + 1;
                    throw new Exception("here");
                } else {
                    for (int i = 0; i < node.length(); i++) {
                        if (node.charAt(i) == ' ' || node.charAt(i) == ':' || node.charAt(i) == ')') {
                            nodeNames.add(node.substring(0, i));
                            break;
                        }
                    }
                }
            }
            cypherQuery = cypher;
        }
    }
    */

}
