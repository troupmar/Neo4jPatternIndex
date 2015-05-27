package com.troupmar.graphaware.performance;

import com.graphaware.test.performance.PerformanceTest;
import com.graphaware.test.performance.PerformanceTestSuite;

/**
 * Created by Martin on 16.04.15.
 */
public class PerformanceTestQueries extends PerformanceTestSuite {

    /**
     * {@inheritDoc}
     */
    @Override
    protected PerformanceTest[] getPerfTests() {
        return new PerformanceTest[]{
                //new GetWithDefaultQueryTest(),
                new GetWithPatternIndexTest(),
                //new CreatePatternIndexTest(),
                //new CreateRelationshipDefaultTest(),
                //new CreateRelationshipInIndexTest(),
                //new DeleteNodeDefaultTest(),
                //new DeleteNodeInIndexTest(),
                //new DeleteRelationshipDefaultTest(),
                //new DeleteRelationshipInIndexTest(),
                //new GetTransactionsWithPatternIndexTest(),
                //new CreateTransactionRelInIndexTest(),
                //new ChangeNodeDefaultTest(),
                //new ChangeNodeInIndexTest(),
        };


    }

}