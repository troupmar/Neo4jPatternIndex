package com.troupmar.graphaware;

import com.troupmar.graphaware.exception.InvalidCypherMatchException;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Martin on 09.04.15.
 */
public abstract class QueryParser {
    protected static final String NODE_REL_PATTERN    = "([^->,][^-<,]+)";
    protected static final String NO_NAME_REL_PATTERN = "(<?-->?)";

    protected Set<String> nodeNames;
    protected Set<String> relNames;

    protected abstract boolean validateQuery(String cypherQuery, GraphDatabaseService database);

    protected void setNamesFromCypherMatch(String patternQuery) throws InvalidCypherMatchException {
        Pattern pattern = Pattern.compile(NODE_REL_PATTERN);
        Matcher matcher = pattern.matcher(patternQuery);

        String item;
        while (matcher.find()) {
            item = matcher.group();
            item = item.replaceAll("\\s+", " ").trim();
            if (item.charAt(0) == '(') {
                nodeNames.add(getVarName(item));
            } else if (item.charAt(0) == '[') {
                relNames.add(getVarName(item));
            } else {
                nodeNames.add(getVarName(item));
            }
        }
    }

    protected String getVarName(String item) throws InvalidCypherMatchException {
        item = item.substring(1).trim();
        if (item.charAt(0) == '{' || item.charAt(0) == ':' || item.charAt(0) == ')' || item.charAt(0) == ']') {
            throw new InvalidCypherMatchException();
        }
        return parseVarName(item);
    }

    protected String parseVarName(String item) {
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
}
