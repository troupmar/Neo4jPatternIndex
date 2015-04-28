package com.troupmar.graphaware.controller;

import com.troupmar.graphaware.*;
import com.troupmar.graphaware.exception.InvalidCypherException;
import com.troupmar.graphaware.exception.InvalidCypherMatchException;
import com.troupmar.graphaware.exception.PatternIndexAlreadyExistsException;
import com.troupmar.graphaware.exception.PatternIndexNotFoundException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
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

    @RequestMapping(value = "/{indexName}/{pattern}", method = POST)
    @ResponseStatus(HttpStatus.CREATED)
    public void createPatternIndex(@PathVariable String indexName, @PathVariable String pattern)
            throws InvalidCypherMatchException, UnsupportedEncodingException, PatternIndexAlreadyExistsException {
        PatternQuery patternQuery = new PatternQuery(URLDecoder.decode(pattern, "UTF-8"), database);
        model.buildNewIndex(patternQuery, indexName);
    }

    @RequestMapping(value = "/{indexName}/{query}", method = GET)
    @ResponseBody
    public String getPatterns(@PathVariable String indexName, @PathVariable String query)
            throws InvalidCypherException, InvalidCypherMatchException, PatternIndexNotFoundException, UnsupportedEncodingException {
        CypherQuery cypherQuery = new CypherQuery(URLDecoder.decode(query, "UTF-8"), database);
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

    @RequestMapping(value = "/count/{indexName}", method = RequestMethod.GET)
    @ResponseBody
    public long count(@PathVariable String indexName) {
        if (model.getPatternIndexes().containsKey(indexName)) {
            PatternIndex patternIndex = model.getPatternIndexes().get(indexName);
            return model.getNumOfUnitsInPatternIndex(patternIndex);
        }
        return 0;
    }

    @ExceptionHandler(InvalidCypherMatchException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public void handleInvalidCypherMatch() {
    }

    @ExceptionHandler(InvalidCypherException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public void handleInvalidCypher() {
    }

    @ExceptionHandler(PatternIndexNotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public void handlePatternIndexNotFound() {
    }

    @ExceptionHandler(PatternIndexAlreadyExistsException.class)
    @ResponseStatus(HttpStatus.CONFLICT)
    public void handlePatternIndexAlreadyExists() {
    }
}
