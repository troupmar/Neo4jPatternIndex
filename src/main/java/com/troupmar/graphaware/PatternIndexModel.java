package com.troupmar.graphaware;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;

import java.util.*;

/**
 * Created by Martin on 05.04.15.
 */
public class PatternIndexModel {
    private static PatternIndexModel instance= null;
    private static Object mutex = new Object();

    private GraphDatabaseService database;
    private List<Node> patternIndexRoots;

    private PatternIndexModel(GraphDatabaseService database) {
        this.database = database;
        //loadIndexRoots();
    }

    public static PatternIndexModel getInstance(GraphDatabaseService database) {
        if(instance==null) {
            synchronized (mutex) {
                if (instance==null) {
                    instance= new PatternIndexModel(database);
                }
            }
        }
        return instance;
    }

    public void buildNewIndex(PatternQuery patternQuery) {
        String query = "MATCH " + patternQuery.getPatternQuery() + " RETURN ";
        for (String node : patternQuery.getNodeNames()) {
            query += "id(" + node + "), ";
        }
        for (String rel : patternQuery.getRelNames()) {
            query += "id(" + rel + "), ";
        }
        query = query.substring(0, query.length() - 2);
        System.out.println(query);

        buildIndex(database.execute(query), patternQuery);
    }

    private void buildIndex(Result result, PatternQuery patternQuery) {
        Set<String> indexedPatternUnits = new HashSet<String>();

        int i=0;
        while (result.hasNext()) {
            Map<String, Object> row = result.next();
            String key = getPatternUnitKey(row, patternQuery);
            if (!indexedPatternUnits.contains(key)) {
                i++;
                indexedPatternUnits.add(key);
            }
        }
        System.out.println(i);
    }

    private String getPatternUnitKey(Map<String, Object> patternUnit, PatternQuery patternQuery) {
        Object [] nodes = new Object[patternQuery.getNodeNames().size()];
        int i = 0;
        for (String nodeName : patternQuery.getNodeNames()) {
            nodes[i++] = patternUnit.get("id(" + nodeName + ")");
        }
        Arrays.sort(nodes);
        String key = "";
        for (Object node : nodes) {
            key += node + "_";
        }
        return key.substring(0, key.length() - 1);
    }
}
