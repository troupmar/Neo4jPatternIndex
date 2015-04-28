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
public class DeleteThroughApiTest extends PatternIndexApiTest {

    @Test
    public void testDeleteCircleFromIndex() throws UnsupportedEncodingException {

        String query = "MATCH (a)-[f]-(b)-[g]-(c)-[h]-(d)-[i]-(e)-[j]-(a) " +
                "WHERE NOT a:_META_ AND NOT b:_META_ AND NOT c:_META_ AND NOT d:_META_ AND NOT e:_META_ RETURN a,b,c";
        String pattern = "(a)-[f]-(b)-[g]-(c)-[h]-(d)-[i]-(e)-[j]-(a)";
        String indexName = "circle";

        String patternParameter = URLEncoder.encode(pattern, "UTF-8");
        String queryParameter = URLEncoder.encode(query, "UTF-8");

        TestHttpClient client = createHttpClient();
        client.post(baseUrl() + "/" + indexName + "/" + patternParameter, HttpStatus.CREATED_201);
        String apiResult = client.get(baseUrl() + "/" + indexName + "/" + queryParameter, HttpStatus.OK_200);



        PatternIndexModel model = PatternIndexModel.getInstance(getDatabase());
        assertEquals(5, model.getNumOfUnitsInPatternIndex(model.getPatternIndexes().get("circle")));
        assertEquals(1, model.getPatternIndexes().size());

        Result queryResult = getDatabase().execute(query);
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        HashSet<Map<String, Object>> result = new HashSet<>();
        while (queryResult.hasNext()) {
            result.add(queryResult.next());
        }

        System.out.println(queryResult);

        assertEquals("getPatternIndex " + indexName, apiResult.length(), gson.toJson(result).length());

        client.delete(baseUrl() + "/" + indexName, HttpStatus.ACCEPTED_202);
        client.get(baseUrl() + "/" + indexName + "/" + queryParameter, HttpStatus.NOT_FOUND_404);

    }
}
