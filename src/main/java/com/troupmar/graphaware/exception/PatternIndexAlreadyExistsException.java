package com.troupmar.graphaware.exception;

/**
 * Created by Martin on 28.04.15.
 */
public class PatternIndexAlreadyExistsException extends Exception {
    public PatternIndexAlreadyExistsException() {
        super("Trying to build index with name or query that already exists in the database.");
    }
}
