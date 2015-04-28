package com.troupmar.graphaware.api;

import com.graphaware.test.util.TestHttpClient;
import com.troupmar.graphaware.PatternIndex;
import com.troupmar.graphaware.PatternIndexModel;
import org.eclipse.jetty.http.HttpStatus;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import static org.junit.Assert.assertEquals;

/**
 * Created by Martin on 28.04.15.
 */
public class CountThroughApiTest extends PatternIndexApiTest {
    @Test
    public void testCountTrianglePatternIndex() throws UnsupportedEncodingException {
        String pattern = "(a)-[d]-(b)-[e]-(c)-[f]-(a)";
        String patternParameter = URLEncoder.encode(pattern, "UTF-8");
        String indexName = "triangle";

        TestHttpClient client = createHttpClient();
        client.post(baseUrl() + "/" + indexName + "/" + patternParameter, HttpStatus.CREATED_201);

        PatternIndexModel model = PatternIndexModel.getInstance(getDatabase());
        assertEquals(4, model.getNumOfUnitsInPatternIndex(model.getPatternIndexes().get("triangle")));
        assertEquals(1, model.getPatternIndexes().size());

        String count = client.get(baseUrl() + "/count/" + indexName, HttpStatus.OK_200);
        PatternIndex patternIndex = model.getPatternIndexes().get(indexName);
        assertEquals(model.getNumOfUnitsInPatternIndex(patternIndex), Long.valueOf(count).longValue());

    }
}
