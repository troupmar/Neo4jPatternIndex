package com.troupmar.graphaware;

import com.troupmar.graphaware.exception.InvalidCypherMatchException;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Martin on 09.04.15.
 */
public abstract class QueryParser {
    protected Set<String> nodeNames;
    protected Set<String> relNames;

    protected abstract void validateQuery(String cypherQuery, GraphDatabaseService database);

    protected boolean hasValidRelationships(String patternQuery) {
        Pattern pattern = Pattern.compile("(<?-->?)");
        Matcher matcher = pattern.matcher(patternQuery);

        if (matcher.find()) {
            return false;
        }
        return true;
    }

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

    private List<String> getParsedCypherMatch(String patternQuery) {
        Pattern pattern = Pattern.compile("([^->,][^-<,]+)");
        Matcher matcher = pattern.matcher(patternQuery);

        List<String> matches = new ArrayList<String>();
        while (matcher.find()) {
            matches.add(matcher.group(1));
        }
        return matches;
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

    public static String namesToString(Set<String> names) {
        String namesString = "";
        for (String name : names) {
            namesString += name + ",";
        }
        return namesString.substring(0, namesString.length() - 1);
    }

    public static Set<String> namesFromString(String namesString) {
        Set<String> names = new LinkedHashSet<>();
        for (String name : namesString.split(",")) {
            names.add(name);
        }
        return names;
    }

}
