package com.troupmar.graphaware.integration;

import com.graphaware.test.integration.GraphAwareApiTest;
import com.troupmar.graphaware.PatternIndexModel;
import com.troupmar.graphaware.PatternQuery;
import org.junit.Test;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Transaction;

import static com.troupmar.graphaware.RelationshipTypes.PATTERN_INDEX_RELATION;
import static org.junit.Assert.assertTrue;

public class ModuleIntegrationTest extends GraphAwareApiTest {

    @Override
    public void setUp() throws Exception {
        super.setUp();

        String cypherMatch = "(a:Person)-[r]-(b:Person)-[p]-(c:Person)-[q]-(a:Person)";
        PatternQuery patternQuery = new PatternQuery(cypherMatch, getDatabase());
        PatternIndexModel model = PatternIndexModel.getInstance(getDatabase());
        model.buildNewIndex(patternQuery, "triangle-index");
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();

        PatternIndexModel.destroy();
    }

    @Override
    protected String propertiesFile() {
        return ModuleIntegrationTest.class.getClassLoader().getResource("neo4j-with-pattern-index.properties").getFile();
    }

    @Test
    public void patternShouldBeIndexedWhenCreated() {
        httpClient.executeCypher(baseNeoUrl(), "CREATE (m:Person {name:'Martin'})");
        httpClient.executeCypher(baseNeoUrl(), "CREATE (k:Person {name:'Kian Hurst'})");
        httpClient.executeCypher(baseNeoUrl(), "CREATE (f:Person {name:'Francesca Russell'})");
        httpClient.executeCypher(baseNeoUrl(), "MATCH (m:Person {name:'Martin'}), (n:Person {name:'Kian Hurst'}) CREATE m-[r:NEW]->n");
        httpClient.executeCypher(baseNeoUrl(), "MATCH (m:Person {name:'Martin'}), (n:Person {name:'Francesca Russell'}) CREATE m-[r:NEW]->n");
        httpClient.executeCypher(baseNeoUrl(), "MATCH (m:Person {name:'Kian Hurst'}), (n:Person {name:'Francesca Russell'}) CREATE m-[r:NEW]->n");

        try (Transaction tx = getDatabase().beginTx()) {
            assertTrue(getDatabase().findNode(DynamicLabel.label("Person"), "name", "Martin").hasRelationship(PATTERN_INDEX_RELATION));
            tx.success();
        }
    }

    @Test
    public void patternShouldBeIndexedWhenCreatedByLabelChange() {
        httpClient.executeCypher(baseNeoUrl(), "CREATE (m:MY {name:'Martin'})");
        httpClient.executeCypher(baseNeoUrl(), "CREATE (k:Person {name:'Kian Hurst'})");
        httpClient.executeCypher(baseNeoUrl(), "CREATE (f:Person {name:'Francesca Russell'})");
        httpClient.executeCypher(baseNeoUrl(), "MATCH (m:MY {name:'Martin'}), (n:Person {name:'Kian Hurst'}) CREATE m-[r:NEW]->n");
        httpClient.executeCypher(baseNeoUrl(), "MATCH (m:MY {name:'Martin'}), (n:Person {name:'Francesca Russell'}) CREATE m-[r:NEW]->n");
        httpClient.executeCypher(baseNeoUrl(), "MATCH (m:Person {name:'Kian Hurst'}), (n:Person {name:'Francesca Russell'}) CREATE m-[r:NEW]->n");
        httpClient.executeCypher(baseNeoUrl(), "MATCH (n:MY {name:'Martin'}) REMOVE n:MY SET n:Person, n.age=25"); //the age is a workaround of a framework bug fixed in 2.2.1.31

        try (Transaction tx = getDatabase().beginTx()) {
            assertTrue(getDatabase().findNode(DynamicLabel.label("Person"), "name", "Martin").hasRelationship(PATTERN_INDEX_RELATION));
            tx.success();
        }
    }
}
