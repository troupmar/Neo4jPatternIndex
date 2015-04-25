package com.troupmar.graphaware;

import org.neo4j.graphdb.Node;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class represents single pattern index. It holds pattern index name, pattern query, that the index was built on and
 * its nodes and relationships names. It also holds the root node of the index.
 *
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