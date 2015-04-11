package com.troupmar.graphaware.exception;

/**
 * Created by Martin on 09.04.15.
 */
public class InvalidCypherException extends Exception {
    public InvalidCypherException() {
        super("Cypher query does not fill its requirements. " +
                "It has to be in valid cypher format and all nodes and relationships names must be defined.");
    }
}
