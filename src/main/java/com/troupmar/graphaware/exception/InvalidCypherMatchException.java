package com.troupmar.graphaware.exception;

/**
 * Created by Martin on 06.04.15.
 */
public class InvalidCypherMatchException extends Exception {
    public InvalidCypherMatchException() {
        super("Cypher MATCH clause of query does not fill its requirements. " +
                "It has to be in valid cypher format and all nodes and relationships need to have name defined");
    }
}
