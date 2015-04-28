package com.troupmar.graphaware.transactionHandlers;

import com.troupmar.graphaware.PatternIndex;
import com.troupmar.graphaware.PatternIndexModel;
import com.troupmar.graphaware.PatternQuery;
import com.troupmar.graphaware.exception.InvalidCypherMatchException;
import com.troupmar.graphaware.exception.PatternIndexAlreadyExistsException;
import com.troupmar.graphaware.handlers.PatternIndexTest;
import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import static org.junit.Assert.assertEquals;

/**
 * Created by Martin on 28.04.15.
 */
public class CreateRelationshipTest extends PatternIndexTest {
    @Test
    public void testCreateTriangle() throws InvalidCypherMatchException, PatternIndexAlreadyExistsException {

        String pattern = "(a)-[r]-(b)-[p]-(c)-[q]-(a)";
        String indexName = "triangle";
        int expectedResult = 4;

        createPatternIndex(indexName, pattern, expectedResult);

        PatternIndexModel model = PatternIndexModel.getInstance(getDatabase());
        PatternIndex patternIndex = model.getPatternIndexes().get(indexName);
        int countOfPatternsBefore = model.getNumOfUnitsInPatternIndex(patternIndex);

        getDatabase().execute("CREATE (m:Person {name:'Michal'}), (j:Person {name:'Jarda'}), (t:Person {name:'Martin'})");
        assertEquals("createRelationship " + indexName, countOfPatternsBefore, model.getNumOfUnitsInPatternIndex(patternIndex));

        getDatabase().execute("MATCH (m:Person {name:'Michal'}), (j:Person {name:'Jarda'}), (t:Person {name:'Martin'}) MERGE (m)-[:FRIEND_OF]-(j)-[:FRIEND_OF]-(t)");
        assertEquals("createRelationship " + indexName, countOfPatternsBefore, model.getNumOfUnitsInPatternIndex(patternIndex));

        getDatabase().execute("MATCH (m:Person {name:'Michal'}), (t:Person {name:'Martin'}) MERGE (m)-[:FRIEND_OF]-(t)");
        assertEquals("createRelationship " + indexName, countOfPatternsBefore + 1, model.getNumOfUnitsInPatternIndex(patternIndex));
    }

    @Test
    public void testCreateCircle() throws InvalidCypherMatchException, PatternIndexAlreadyExistsException {

        String pattern = "(a)-[f]-(b)-[g]-(c)-[h]-(d)-[i]-(e)-[j]-(a)";
        String indexName = "circle";
        int expectedResult = 5;

        createPatternIndex(indexName, pattern, expectedResult);

        PatternIndexModel model = PatternIndexModel.getInstance(getDatabase());
        PatternIndex patternIndex = model.getPatternIndexes().get(indexName);
        int countOfPatternsBefore = model.getNumOfUnitsInPatternIndex(patternIndex);

        Node a, b, c, d, e;
        try (Transaction tx = getDatabase().beginTx()) {
            a = getDatabase().createNode();
            b = getDatabase().createNode();
            c = getDatabase().createNode();
            d = getDatabase().createNode();
            e = getDatabase().createNode();
            tx.success();
        }

        assertEquals("createRelationship " + indexName, countOfPatternsBefore, model.getNumOfUnitsInPatternIndex(patternIndex));

        try (Transaction tx = getDatabase().beginTx()) {
            a.createRelationshipTo(b, DynamicRelationshipType.withName("TEST"));
            b.createRelationshipTo(c, DynamicRelationshipType.withName("TEST"));
            c.createRelationshipTo(d, DynamicRelationshipType.withName("TEST"));
            d.createRelationshipTo(e, DynamicRelationshipType.withName("TEST"));
            tx.success();
        }

        assertEquals("createRelationship " + indexName, countOfPatternsBefore, model.getNumOfUnitsInPatternIndex(patternIndex));

        try (Transaction tx = getDatabase().beginTx()) {
            e.createRelationshipTo(a, DynamicRelationshipType.withName("TEST"));
            tx.success();
        }

        assertEquals("createRelationship " + indexName, countOfPatternsBefore + 1, model.getNumOfUnitsInPatternIndex(patternIndex));

    }


    private void createPatternIndex(String indexName, String pattern, int expectedResult)
            throws InvalidCypherMatchException, PatternIndexAlreadyExistsException {
        PatternQuery patternQuery = new PatternQuery(pattern, getDatabase());
        PatternIndexModel model = PatternIndexModel.getInstance(getDatabase());
        model.buildNewIndex(patternQuery, indexName);

        PatternIndex patternIndex = model.getPatternIndexes().get(indexName);
        assertEquals("createPatternIndex " + indexName, expectedResult, model.getNumOfUnitsInPatternIndex(patternIndex));
    }
}
