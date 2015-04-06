package com.troupmar.graphaware;

import org.neo4j.graphdb.Node;

import java.util.List;

/**
 * Created by Martin on 05.04.15.
 */
public class PatternIndex {

    private String patternName;
    private String patternQuery;
    private int numOfUnits;
    private List<Node> units;

    public PatternIndex(String patternName, String patternQuery) {
        this.patternName = patternName;
        this.patternQuery = patternQuery;
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

    public int getNumOfUnits() {
        return numOfUnits;
    }

    public void setNumOfUnits(int numOfUnits) {
        this.numOfUnits = numOfUnits;
    }

    public List<Node> getUnits() {
        return units;
    }

    public void setUnits(List<Node> units) {
        this.units = units;
    }

}
