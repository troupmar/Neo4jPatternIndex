package com.troupmar.graphaware.transactionHandlers;

import com.troupmar.graphaware.PatternIndex;
import com.troupmar.graphaware.PatternIndexModel;
import com.troupmar.graphaware.PatternQuery;
import com.troupmar.graphaware.exception.InvalidCypherMatchException;
import com.troupmar.graphaware.exception.PatternIndexAlreadyExistsException;
import com.troupmar.graphaware.handlers.PatternIndexTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by Martin on 28.04.15.
 */
public class ChangeNodeTest extends PatternIndexTest {
    @Test
    public void testChangeLabelInTriangle() throws InvalidCypherMatchException, PatternIndexAlreadyExistsException {

        String pattern = "(a:Person)-[r]-(b:Person)-[p]-(c:Person)-[q]-(a:Person)";
        String indexName = "triangle";
        int expectedResult = 4;

        createPatternIndex(indexName, pattern, expectedResult);

        PatternIndexModel model = PatternIndexModel.getInstance(getDatabase());
        PatternIndex patternIndex = model.getPatternIndexes().get(indexName);
        int countOfPatternsBefore = model.getNumOfUnitsInPatternIndex(patternIndex);

        //MATCH (a:Person)-[r]-(b:Person)-[p]-(c:Person)-[q]-(a:Person) RETURN a,b,c,id(a) LIMIT 1
        getDatabase().execute("MATCH (n) WHERE id(n)=68 Remove n:Person Set n:MyLabel");
        assertEquals("changeNode " + indexName, countOfPatternsBefore - 1, model.getNumOfUnitsInPatternIndex(patternIndex));

        // MATCH (a:Person)-[r]-(b:Person)-[p]-(c:Person)-[q]-(a:Person) RETURN a,b,c,id(a) LIMIT 1
        getDatabase().execute("MATCH (n) WHERE id(n)=68 Set n:Person");
        assertEquals("changeNode " + indexName, countOfPatternsBefore, model.getNumOfUnitsInPatternIndex(patternIndex));
    }

    @Test
    public void testChangePropertyInTriangle() throws InvalidCypherMatchException, PatternIndexAlreadyExistsException {

        String pattern = "(a:Person {name: 'Holly Harding'})-[r]-(b:Person)-[p]-(c:Person)-[q]-(a:Person)";
        String indexName = "triangle";
        int expectedResult = 2;

        createPatternIndex(indexName, pattern, expectedResult);

        PatternIndexModel model = PatternIndexModel.getInstance(getDatabase());
        PatternIndex patternIndex = model.getPatternIndexes().get(indexName);
        int countOfPatternsBefore = model.getNumOfUnitsInPatternIndex(patternIndex);

        // MATCH (a:Person {name:'Holly Harding'})-[r]-(b:Person)-[p]-(c:Person)-[q]-(a:Person) RETURN a,b,c,id(a) LIMIT 1
        getDatabase().execute("MATCH (n) WHERE id(n)=52 Set n.name = 'Holly Stuart'");
        assertEquals("changeNode " + indexName, countOfPatternsBefore - 2, model.getNumOfUnitsInPatternIndex(patternIndex));

        // MATCH (a:Person)-[r]-(b:Person)-[p]-(c:Person)-[q]-(a:Person) RETURN a,b,c,id(a) LIMIT 1
        getDatabase().execute("MATCH (n) WHERE id(n)=52 Set n.name = 'Holly Harding'");
        assertEquals("changeNode " + indexName, countOfPatternsBefore, model.getNumOfUnitsInPatternIndex(patternIndex));
    }


    private void createPatternIndex(String indexName, String pattern, int expectedResult) throws InvalidCypherMatchException, PatternIndexAlreadyExistsException {
        PatternQuery patternQuery = new PatternQuery(pattern, getDatabase());
        PatternIndexModel model = PatternIndexModel.getInstance(getDatabase());
        model.buildNewIndex(patternQuery, indexName);

        PatternIndex patternIndex = model.getPatternIndexes().get(indexName);
        assertEquals("createPatternIndex " + indexName, expectedResult, model.getNumOfUnitsInPatternIndex(patternIndex));
    }
}
