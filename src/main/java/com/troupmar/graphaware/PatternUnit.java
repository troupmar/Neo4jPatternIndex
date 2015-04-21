package com.troupmar.graphaware;

import java.util.*;

/**
 * Created by Martin on 18.04.15.
 */
public class PatternUnit {
    private Long[] nodeIDs;
    private Set<String> specificUnits;

    public PatternUnit(Map<String, Object> newSpecificUnit, Set<String> nodeNames, Set<String> relNames) {
        setNodeIDs(newSpecificUnit, nodeNames);
        specificUnits = new HashSet<>();
        addSpecificUnit(newSpecificUnit, relNames);
    }

    private void setNodeIDs(Map<String, Object> newSpecificUnit, Set<String> nodeNames) {
        nodeIDs = new Long[nodeNames.size()];
        Iterator<String> itr = nodeNames.iterator();
        int i = 0;
        while (itr.hasNext()) {
            nodeIDs[i++] = (Long) newSpecificUnit.get("id(" + itr.next() + ")");
        }
    }

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

    public static String getPatternUnitKey(Map<String, Object> newSpecificUnit, Set<String> nodeNames) {
        Set<Long> sortedNodeIDs = new TreeSet<>();
        for (String nodeName : nodeNames) {
            sortedNodeIDs.add((Long) newSpecificUnit.get("id(" + nodeName + ")"));
        }
        String key = "";
        for (Long nodeID : sortedNodeIDs) {
            key += nodeID + "_";
        }
        return key.substring(0, key.length() - 1);
    }

    public static String specificUnitsToString(Set<String> specificUnits) {
        String specificUnitsString = "";
        for (String specificUnit : specificUnits) {
            specificUnitsString += specificUnit + ";";
        }
        return specificUnitsString.substring(0, specificUnitsString.length() - 1);
    }

    public static Set<String> specificUnitsFromString(String specificUnitsString) {
        Set<String> specificUnits = new HashSet<>();
        for (String specificUnit : specificUnitsString.split(";")) {
            specificUnits.add(specificUnit);
        }
        return specificUnits;
    }

    public Long[] getNodeIDs() {
        return nodeIDs;
    }

    public Set<String> getSpecificUnits() {
        return specificUnits;
    }
}
