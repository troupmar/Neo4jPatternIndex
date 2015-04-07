package com.troupmar.graphaware;

import org.neo4j.graphdb.Node;

import java.util.List;

/**
 * Created by Martin on 05.04.15.
 */
public class PatternIndex {

    private String patternName;
    private String patternQuery;
    private Node rootNode;
    private int numOfUnits;

    public PatternIndex(String patternQuery, String patternName, Node rootNode) {
        this.rootNode = rootNode;
        this.patternQuery = rootNode.getProperty("patternQuery").toString();
        this.patternName = rootNode.getProperty("patternName").toString();
    }

    public String getPatternName() {
        return patternName;
    }

    public void setPatternName(String patternName) {
        this.patternName = patternName;
    }

    public String getPatternQuery() {
        return patternQuery;
    }

    public void setPatternQuery(String patternQuery) {
        this.patternQuery = patternQuery;
    }

    public Node getRootNode() {
        return rootNode;
    }

    public void setRootNode(Node rootNode) {
        this.rootNode = rootNode;
    }

    public int getNumOfUnits() {
        return numOfUnits;
    }

    public void setNumOfUnits(int numOfUnits) {
        this.numOfUnits = numOfUnits;
    }


}
