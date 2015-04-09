package com.troupmar.graphaware.exception;

/**
 * Created by Martin on 09.04.15.
 */
public class PatternIndexNotFoundException extends Exception {
    public PatternIndexNotFoundException() {
        super("Trying to execute query on top of unknown index. Either the index was not build yet, or its name is incorrect.");
    }
}
