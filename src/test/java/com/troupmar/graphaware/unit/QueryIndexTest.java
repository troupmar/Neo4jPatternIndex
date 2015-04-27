package com.troupmar.graphaware.unit;

import com.google.gson.Gson;
import com.troupmar.graphaware.*;
import com.troupmar.graphaware.exception.InvalidCypherException;
import com.troupmar.graphaware.exception.InvalidCypherMatchException;
import com.troupmar.graphaware.exception.PatternIndexNotFoundException;
import org.junit.Test;
import org.neo4j.graphdb.Result;

import java.util.HashSet;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created by Martin on 27.04.15.
 */
public class QueryIndexTest extends PatternIndexTest {
    @Test
    public void testCirclePatternWithArrow() throws InvalidCypherMatchException, InvalidCypherException, PatternIndexNotFoundException {
        String query = "MATCH (a)-[f]-(b)-[g]-(c)-[h]-(d)-[i]-(e)-[j]-(a) " +
                "WHERE NOT a:_META_ AND NOT b:_META_ AND NOT c:_META_ AND NOT d:_META_ AND NOT e:_META_ RETURN a,b,c";
        String pattern = "(a)-[f]-(b)-[g]-(c)-[h]-(d)-[i]-(e)-[j]-(a)";
        String indexName = "circle";
        int expectedResult = 10149;

        createPatternIndex(indexName, pattern, expectedResult);
        getPatternIndex(indexName, query);
        deletePatternIndex(indexName);
    }

    @Test
    public void testVPatternWithArrow() throws InvalidCypherMatchException, InvalidCypherException, PatternIndexNotFoundException {
        String query = "MATCH (a:Female)-[d]->(b:Person)<-[e]-(c:Male) " +
                "WHERE NOT a:_META_ AND NOT b:_META_ AND NOT c:_META_ RETURN a,b,c";
        String pattern = "(a)-[d]->(b)<-[e]-(c)";
        String indexName = "VWithArrow";
        int expectedResult = 12490;

        createPatternIndex(indexName, pattern, expectedResult);
        getPatternIndex(indexName, query);
        deletePatternIndex(indexName);
    }

    @Test
    public void testVPattern() throws InvalidCypherMatchException, InvalidCypherException, PatternIndexNotFoundException {
        String query = "MATCH (a:Female)-[d]->(b:Person)<-[e]-(c:Male) " +
                "WHERE NOT a:_META_ AND NOT b:_META_ AND NOT c:_META_ RETURN a,b,c";
        String pattern = "(a)-[d]-(b)-[e]-(c)";
        String indexName = "V";
        int expectedResult = 49787;

        createPatternIndex(indexName, pattern, expectedResult);
        getPatternIndex(indexName, query);
        deletePatternIndex(indexName);
    }

    @Test
    public void testTrianglePattern() throws InvalidCypherMatchException, InvalidCypherException, PatternIndexNotFoundException {
        String query = "MATCH (a)-[r]-(b)-[p]-(c)-[q]-(a) " +
                "WHERE NOT a:_META_ AND NOT b:_META_ AND NOT c:_META_ RETURN a,b,c";
        String pattern = "(a)-[r]-(b)-[p]-(c)-[q]-(a)";
        String indexName = "triangle";
        int expectedResult = 199;

        createPatternIndex(indexName, pattern, expectedResult);
        getPatternIndex(indexName, query);
        deletePatternIndex(indexName);
    }

    /* Exceptions */
    /*
    @Test(expected = IllegalArgumentException.class)
    public void blankDefinedRelationshipVariableExceptionPattern() throws InvalidCypherMatchException {
        String pattern = "(a)-[]-(b)";
        String indexName = "triangle";
        int expectedResult = 168;

        createPatternIndex(indexName, pattern, expectedResult);
    }

    @Test(expected = IllegalArgumentException.class)
    public void noDefinedRelationshipVariableExceptionPattern() throws InvalidCypherMatchException {
        String pattern = "(a)--(b)";
        String indexName = "triangle";
        int expectedResult = 168;

        createPatternIndex(indexName, pattern, expectedResult);
    }

    @Test(expected = IllegalArgumentException.class)
    public void blankDefinedNodeVariableExceptionPattern() throws InvalidCypherMatchException {
        String pattern = "()-[c]-(b)";
        String indexName = "triangle";
        int expectedResult = 168;

        createPatternIndex(indexName, pattern, expectedResult);
    }
    */

    private void createPatternIndex(String indexName, String pattern, int expectedResult) throws InvalidCypherMatchException {
        // get pattern index model instance
        PatternIndexModel model = PatternIndexModel.getInstance(getDatabase());
        // get pattern query instance
        PatternQuery patternQuery = new PatternQuery(pattern, getDatabase());
        // build index
        model.buildNewIndex(patternQuery, indexName);
        // get index
        PatternIndex patternIndex = model.getPatternIndexes().get(indexName);
        // check number of pattern index units created
        assertEquals("createPatternIndex " + indexName, expectedResult, model.getNumOfUnitsInPatternIndex(patternIndex));
    }

    private void getPatternIndex(String indexName, String query)
            throws PatternIndexNotFoundException, InvalidCypherException, InvalidCypherMatchException {
        // get pattern index model instance
        PatternIndexModel model = PatternIndexModel.getInstance(getDatabase());
        // get cypher query instance
        CypherQuery cypherQuery = new CypherQuery(query, getDatabase());
        // get results from index
        HashSet<Map<String, Object>> resultFromIndex = model.getResultFromIndex(cypherQuery, indexName);
        // results from index to JSON
        String resultFromIndexInJSON = PatternIndexModel.resultToString(resultFromIndex, PrintTypes.JSON);
        // get original results
        Result queryResult = getDatabase().execute(query);

        Gson gson = new Gson();
        HashSet<Map<String, Object>> result = new HashSet<>();
        while (queryResult.hasNext()) {
            result.add(queryResult.next());
        }

        // compare results from index with original results
        assertEquals("getPatternIndex " + indexName, resultFromIndex.size(), result.size());
    }

    private void deletePatternIndex(String indexName) {
        // get pattern index model instance
        PatternIndexModel model = PatternIndexModel.getInstance(getDatabase());
        // remove index
        model.removePatternIndexByName(indexName);
        // check if index was deleted
        assertEquals("deletePatternIndex " + indexName, 0, model.getPatternIndexes().size());
    }
}
