package com.troupmar.graphaware.api;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.graphaware.test.util.TestHttpClient;
import com.troupmar.graphaware.PatternIndexModel;
import com.troupmar.graphaware.handlers.PatternIndexApiTest;
import org.eclipse.jetty.http.HttpStatus;
import org.junit.Test;
import org.neo4j.graphdb.Result;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashSet;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created by Martin on 28.04.15.
 */
public class GetThroughApiTest extends PatternIndexApiTest {
    @Test
    public void testGetReverseVPatternsFromIndex() throws UnsupportedEncodingException {

        String query = "MATCH (a:Female)-[r]->(b:Person)<-[p]-(c:Male) " +
                "WHERE NOT a:_META_ AND NOT b:_META_ AND NOT c:_META_ RETURN a,b,c";
        String pattern = "(a)-[d]-(b)-[e]-(c)";
        String indexName = "reverseV";

        String patternParameter = URLEncoder.encode(pattern, "UTF-8");
        String queryParameter = URLEncoder.encode(query, "UTF-8");

        TestHttpClient client = createHttpClient();
        client.post(baseUrl() + "/" + indexName + "/" + patternParameter, HttpStatus.CREATED_201);
        String apiResult = client.get(baseUrl() + "/" + indexName + "/" + queryParameter, HttpStatus.OK_200);

        Result queryResult = getDatabase().execute(query);

        PatternIndexModel model = PatternIndexModel.getInstance(getDatabase());
        assertEquals(280, model.getNumOfUnitsInPatternIndex(model.getPatternIndexes().get("reverseV")));
        assertEquals(1, model.getPatternIndexes().size());

        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        HashSet<Map<String, Object>> result = new HashSet<>();
        while (queryResult.hasNext()) {
            result.add(queryResult.next());
        }

        assertEquals("getPatternIndex " + indexName, gson.toJson(result).length(), apiResult.length());
    }
}
