package com.troupmar.graphaware.transactionHandlers;

import com.troupmar.graphaware.PatternIndex;
import com.troupmar.graphaware.PatternIndexModel;
import com.troupmar.graphaware.PatternQuery;
import com.troupmar.graphaware.exception.InvalidCypherMatchException;
import com.troupmar.graphaware.exception.PatternIndexAlreadyExistsException;
import com.troupmar.graphaware.handlers.PatternIndexTest;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.concurrent.ConcurrentNavigableMap;

import static com.graphaware.runtime.RuntimeRegistry.getRuntime;
import static org.junit.Assert.assertEquals;
/**
 * Created by Martin on 28.04.15.
 */
public class DeleteRelationshipTest extends PatternIndexTest {

    @Test
    public void testDeleteTriangle() throws InvalidCypherMatchException, PatternIndexAlreadyExistsException {

        String pattern = "(a)-[r]-(b)-[p]-(c)-[q]-(a)";
        String indexName = "triangle";
        int expectedResult = 4;

        createPatternIndex(indexName, pattern, expectedResult);

        PatternIndexModel model = PatternIndexModel.getInstance(getDatabase());
        PatternIndex patternIndex = model.getPatternIndexes().get(indexName);
        int countOfPatternsBefore = model.getNumOfUnitsInPatternIndex(patternIndex);

        // MATCH (n {name:'Ellis Mann'})-[r]-(m {name:'Freddie Winter'}) RETURN n, m, id(r)
        getDatabase().execute("MATCH (n)-[r]-() WHERE id(r)=53 DELETE r");
        assertEquals("createRelationship " + indexName, countOfPatternsBefore, model.getNumOfUnitsInPatternIndex(patternIndex));

        // MATCH (a)-[r]-(b)-[p]-(c)-[q]-(a) RETURN a, b, c, id(r) LIMIT 1
        getDatabase().execute("MATCH (n)-[r]-() WHERE id(r)=35 DELETE r");
        assertEquals("createRelationship " + indexName, countOfPatternsBefore - 1, model.getNumOfUnitsInPatternIndex(patternIndex));
    }

    @Test
    public void testDeleteCircle() throws InvalidCypherMatchException, PatternIndexAlreadyExistsException {

        String pattern = "(a)-[f]-(b)-[g]-(c)-[h]-(d)-[i]-(e)-[j]-(a)";
        String indexName = "circle";
        int expectedResult = 5;

        createPatternIndex(indexName, pattern, expectedResult);

        PatternIndexModel model = PatternIndexModel.getInstance(getDatabase());
        PatternIndex patternIndex = model.getPatternIndexes().get(indexName);
        int countOfPatternsBefore = model.getNumOfUnitsInPatternIndex(patternIndex);

        // MATCH (a)-[f]-(b)-[g]-(c)-[h]-(d)-[i]-(e)-[j]-(a) RETURN a,b,c,d,e,id(f) LIMIT 1
        Relationship a, b;
        try (Transaction tx = getDatabase().beginTx()) {
            a = getDatabase().getRelationshipById(41);
            a.delete();
            tx.success();
        }

        assertEquals("createRelationship " + indexName, countOfPatternsBefore - 1, model.getNumOfUnitsInPatternIndex(patternIndex));

    }


    private void createPatternIndex(String indexName, String pattern, int expectedResult) throws InvalidCypherMatchException, PatternIndexAlreadyExistsException {
        PatternQuery patternQuery = new PatternQuery(pattern, getDatabase());
        PatternIndexModel model = PatternIndexModel.getInstance(getDatabase());
        model.buildNewIndex(patternQuery, indexName);

        PatternIndex patternIndex = model.getPatternIndexes().get(indexName);
        assertEquals("createPatternIndex " + indexName, expectedResult, model.getNumOfUnitsInPatternIndex(patternIndex));
    }
}
