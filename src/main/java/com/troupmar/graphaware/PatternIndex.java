package com.troupmar.graphaware;

import org.neo4j.graphdb.Node;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by Martin on 05.04.15.
 */
public class PatternIndex {

    private String patternName;
    private String patternQuery;
    private Set<String> nodeNames;
    private Set<String> relNames;
    private Node rootNode;


    public PatternIndex(String patternName, String patternQuery, Node rootNode, Set<String> nodeNames, Set<String> relNames) {
        this.patternName = patternName;
        this.patternQuery = patternQuery;
        this.rootNode = rootNode;
        this.nodeNames = nodeNames;
        this.relNames = relNames;
    }

    public String getPatternName() {
        return patternName;
    }

    public String getPatternQuery() {
        return patternQuery;
    }

    public Node getRootNode() {
        return rootNode;
    }

    public Set<String> getNodeNames() {
        return nodeNames;
    }

    public Set<String> getRelNames() {
        return relNames;
    }
}