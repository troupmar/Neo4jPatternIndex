package com.troupmar.graphaware;

import java.util.*;

/**
 * This class represents a pattern index unit. Each pattern index has exactly one pattern index root and a set of those pattern
 * index units. Each pattern index unit is represented by a set of nodes IDs. Those nodes carry at least
 * one specific unit that satisfy conditions of a pattern for which the pattern index was built. Those nodes IDs that represent
 * a pattern index unit must differ in at least one of them between other pattern index units.
 * Each pattern index unit also carries all specific units that share those nodes that represent it.
 *
 * Created by Martin on 18.04.15.
 */
public class PatternIndexUnit {
    // node IDs that represent single pattern index unit
    private Long[] nodeIDs;
    // set of specific pattern index units
    private Set<String> specificUnits;

    /**
     * Constructor that creates new instance of PatternIndexUnit (it sets node IDs that represent it) and add a new given
     * specific unit right away.
     * @param newSpecificUnit specific unit to be added.
     * @param nodeNames names of nodes that the original pattern query that the index was built on holds.
     * @param relNames names of relationships that the original pattern query that the index was built on holds.
     */
    public PatternIndexUnit(Map<String, Object> newSpecificUnit, Set<String> nodeNames, Set<String> relNames) {
        setNodeIDs(newSpecificUnit, nodeNames);
        specificUnits = new HashSet<>();
        addSpecificUnit(newSpecificUnit, relNames);
    }

    // Method to set node IDs.
    private void setNodeIDs(Map<String, Object> newSpecificUnit, Set<String> nodeNames) {
        nodeIDs = new Long[nodeNames.size()];
        Iterator<String> itr = nodeNames.iterator();
        int i = 0;
        while (itr.hasNext()) {
            nodeIDs[i++] = (Long) newSpecificUnit.get("id(" + itr.next() + ")");
        }
    }

    /**
     * Method to add a new specific unit.
     * @param newSpecificUnit a specific unit to be added.
     * @param relNames names of relationships that the original pattern query that the index was built on holds.
     */
    public void addSpecificUnit(Map<String, Object> newSpecificUnit, Set<String> relNames) {
        Set<Long> sortedRelIDs = new TreeSet<>();
        for (String relName : relNames) {
            sortedRelIDs.add((Long) newSpecificUnit.get("id(" + relName + ")"));
        }
        String specificUnit = "";
        for (Long relID : sortedRelIDs) {
            specificUnit += relID + "_";
        }
        specificUnit = specificUnit.substring(0, specificUnit.length() - 1);
        specificUnits.add(specificUnit);
    }

    /**
     * Method to get a pattern index unit key. This key is built on node IDs that represent it. Those IDs are sorted in
     * ascending order and concatenated with symbol _
     * @param specificUnit get a key for a pattern index unit that contains this specific unit.
     * @param nodeNames names of nodes that the original pattern query that the index was built on holds.
     * @return key as a String.
     */
    public static String getPatternIndexUnitKey(Map<String, Object> specificUnit, Set<String> nodeNames) {
        Set<Long> sortedNodeIDs = new TreeSet<>();
        for (String nodeName : nodeNames) {
            sortedNodeIDs.add((Long) specificUnit.get("id(" + nodeName + ")"));
        }
        String key = "";
        for (Long nodeID : sortedNodeIDs) {
            key += nodeID + "_";
        }
        return key.substring(0, key.length() - 1);
    }

    /**
     * Method to transform specific units array to String -> so it can be stored as a node property.
     * @param specificUnits array of specific units to be transformed to a String.
     * @return String representation of specific units array.
     */
    public static String specificUnitsToString(Set<String> specificUnits) {
        String specificUnitsString = "";
        for (String specificUnit : specificUnits) {
            specificUnitsString += specificUnit + ";";
        }
        return specificUnitsString.substring(0, specificUnitsString.length() - 1);
    }

    /**
     * Method to transform specific units String to array -> so it can be loaded back from a node property.
     * @param specificUnitsString String of specific units array.
     * @return array of specific units.
     */
    public static Set<String> specificUnitsFromString(String specificUnitsString) {
        Set<String> specificUnits = new HashSet<>();
        for (String specificUnit : specificUnitsString.split(";")) {
            specificUnits.add(specificUnit);
        }
        return specificUnits;
    }

    /**
     * Get node IDs that represent the pattern index unit.
     * @return array of node IDs
     */
    public Long[] getNodeIDs() {
        return nodeIDs;
    }

    /**
     * Get specific units that the pattern index unit hodls.
     * @return
     */
    public Set<String> getSpecificUnits() {
        return specificUnits;
    }
}
