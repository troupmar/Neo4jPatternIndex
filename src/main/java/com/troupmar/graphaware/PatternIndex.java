package com.troupmar.graphaware;

import org.neo4j.graphdb.Node;

import java.util.List;
import java.util.Map;

/**
 * Created by Martin on 05.04.15.
 */
public class PatternIndex {

    private String patternName;
    private String patternQuery;
    private Map<String, String[]> relsWithNodes;
    private Node rootNode;
    private int numOfUnits;


    public PatternIndex(String patternName, String patternQuery, Node rootNode, int numOfUnits, Map<String, String[]> relsWithNodes) {
        this.patternName = patternName;
        this.patternQuery = patternQuery;
        this.relsWithNodes = relsWithNodes;
        this.rootNode = rootNode;
        this.numOfUnits = numOfUnits;
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

    public int getNumOfUnits() {
        return numOfUnits;
    }

    public void setNumOfUnits(int numOfUnits) {
        this.numOfUnits = numOfUnits;
    }

    public Map<String, String[]> getRelsWithNodes() {
        return relsWithNodes;
    }
}