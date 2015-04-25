package com.troupmar.graphaware;

import com.troupmar.graphaware.exception.InvalidCypherMatchException;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class provides methods to parse Cypher query. This is a parent class for two classes: PatternQuery - class to process
 * MATCH clause of Cypher query and CypherQuery - class to process complete Cypher query.
 * This class holds and provides class variables (names of nodes and relationships that are parsed from
 * MATCH clause of Cypher query) to classes that extend it.
 *
 *
 * Created by Martin on 09.04.15.
 */
public abstract class QueryParser {
    protected Set<String> nodeNames;
    protected Set<String> relNames;

    protected abstract void validateQuery(String cypherQuery, GraphDatabaseService database);

    // Method to check if all relationships in MATCH clause have name. All names of nodes and relationships in Cypher
    // query must be defined to build index and query on index .
    protected boolean hasValidRelationships(String patternQuery) {
        Pattern pattern = Pattern.compile("(<?-->?)");
        Matcher matcher = pattern.matcher(patternQuery);

        if (matcher.find()) {
            return false;
        }
        return true;
    }

    // Method to retrieve names of nodes and relationships from MATCH clause of Cypher query.
    protected void setNamesFromCypherMatch(String patternQuery) throws InvalidCypherMatchException {
        List<String> parsedVars = getParsedCypherMatch(patternQuery);

        String item;
        for (int i=0; i<parsedVars.size(); i++) {
            item = parsedVars.get(i).replaceAll("\\s+", " ").trim();
            if (item.charAt(0) == '(') {
                nodeNames.add(getVarName(item));
            } else if (item.charAt(0) == '[') {
                relNames.add(getVarName(item));

            } else {
                nodeNames.add(getVarName(item));
            }
        }
    }

    // Method to find all nodes and relationships in MATCH clause of Cypher query.
    private List<String> getParsedCypherMatch(String patternQuery) {
        Pattern pattern = Pattern.compile("([^->,][^-<,]+)");
        Matcher matcher = pattern.matcher(patternQuery);

        List<String> matches = new ArrayList<String>();
        while (matcher.find()) {
            matches.add(matcher.group(1));
        }
        return matches;
    }

    // Method to parse name of node or relationship from found node or relationship in MATCH clause of Cypher query.
    private String getVarName(String item) throws InvalidCypherMatchException {
        item = item.substring(1).trim();
        if (item.charAt(0) == '{' || item.charAt(0) == ':' || item.charAt(0) == ')' || item.charAt(0) == ']') {
            throw new InvalidCypherMatchException();
        }
        return parseVarName(item);
    }

    private String parseVarName(String item) {
        int i = 0;
        while (i<item.length()) {
            if (item.charAt(i) == ' ' || item.charAt(i) == ':' || item.charAt(i) == ')' || item.charAt(i) == ']') {
                break;
            }
            i++;
        }
        return item.substring(0, i);
    }

    public Set<String> getNodeNames() {
        return nodeNames;
    }

    public Set<String> getRelNames() {
        return relNames;
    }

    // Method to transform set of names of nodes or relationships to String -> so it can be stored in pattern index meta nodes in database.
    public static String namesToString(Set<String> names) {
        String namesString = "";
        for (String name : names) {
            namesString += name + ",";
        }
        return namesString.substring(0, namesString.length() - 1);
    }
    // // Method to transform String of names of nodes or relationships to set -> so it can be retrieved from pattern index meta nodes in database.
    public static Set<String> namesFromString(String namesString) {
        Set<String> names = new LinkedHashSet<>();
        for (String name : namesString.split(",")) {
            names.add(name);
        }
        return names;
    }

}
