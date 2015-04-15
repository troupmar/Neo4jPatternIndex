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
    protected Map<String, String []> relsWithNodes;

    protected abstract boolean isQueryValid(String cypherQuery, GraphDatabaseService database);

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
                String[] surroundNodes = new String[2];
                surroundNodes[0] = getVarName(parsedVars.get(i - 1).replaceAll("\\s+", " ").trim());
                surroundNodes[1] = getVarName(parsedVars.get(i + 1).replaceAll("\\s+", " ").trim());
                relsWithNodes.put(getVarName(item), surroundNodes);
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

    public Map<String, String[]> getRelsWithNodes() {
        return relsWithNodes;
    }

    public static String relsWithNodesToString(Map<String, String[]> relsWithNodes) {
        String relsWithNodesString = "";
        for (Map.Entry<String, String[]> relWithNodes : relsWithNodes.entrySet()) {
            relsWithNodesString += relWithNodes.getKey() + ":" + relWithNodes.getValue()[0] + "," + relWithNodes.getValue()[1] + ";";
        }
        return relsWithNodesString.substring(0, relsWithNodesString.length() - 1);
    }

    public static Map<String, String[]> getRelsWithNodesFromString(String relsWithNodesFromString) {
        Map<String, String[]> relsWithNodes = new LinkedHashMap<String, String[]>();
        for (String rel : relsWithNodesFromString.split(";")) {
            String[] relAndNodes = rel.split(":");
            relsWithNodes.put(relAndNodes[0], relAndNodes[1].split(","));
        }
        return relsWithNodes;
    }
}
