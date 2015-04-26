package com.troupmar.graphaware.controller;

import com.troupmar.graphaware.*;
import com.troupmar.graphaware.exception.InvalidCypherException;
import com.troupmar.graphaware.exception.InvalidCypherMatchException;
import com.troupmar.graphaware.exception.PatternIndexNotFoundException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.HashSet;
import java.util.Map;

import static org.springframework.web.bind.annotation.RequestMethod.*;

/**
 * Created by Martin on 26.04.15.
 */

@Controller
@RequestMapping("/pattern-index")
public class PatternIndexApi {
    private final GraphDatabaseService database;
    private final PatternIndexModel model;

    @Autowired
    public PatternIndexApi(GraphDatabaseService database) {
        this.database = database;
        this.model = PatternIndexModel.getInstance(database);
    }

    @RequestMapping(value = "/{indexName}/{pattern}", method = PUT)
    @ResponseStatus(HttpStatus.CREATED)
    public void createPatternIndex(@PathVariable String indexName, @PathVariable String pattern) throws InvalidCypherMatchException {
        PatternQuery patternQuery = new PatternQuery(pattern, database);
        model.buildNewIndex(patternQuery, indexName);
    }

    @RequestMapping(value = "/{indexName}/{query}", method = POST)
    @ResponseBody
    public String getPatterns(@PathVariable String indexName, @PathVariable String query)
            throws InvalidCypherException, InvalidCypherMatchException, PatternIndexNotFoundException {
        CypherQuery cypherQuery = new CypherQuery(query, database);
        HashSet<Map<String, Object>> result = model.getResultFromIndex(cypherQuery, indexName);
        return model.resultToString(result, PrintTypes.JSON);
    }

    @RequestMapping(value = "/{indexName}", method = DELETE)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void deletePatternIndex(@PathVariable String indexName) {
        model.removePatternIndexByName(indexName);
    }

    @RequestMapping(value = "/count", method = RequestMethod.GET)
    @ResponseBody
    public long count() {
        return model.getPatternIndexes().size();
    }

    @RequestMapping(value = "/{indexName}/count", method = RequestMethod.GET)
    @ResponseBody
    public long count(@PathVariable String indexName) {
        if (model.getPatternIndexes().containsKey(indexName)) {
            PatternIndex patternIndex = model.getPatternIndexes().get(indexName);
            return model.getNumOfUnitsInPatternIndex(patternIndex);
        }
        return 0;
    }

}
