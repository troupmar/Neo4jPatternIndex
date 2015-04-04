package com.troupmar.graphaware.performance;

import com.graphaware.test.performance.PerformanceTest;
import com.graphaware.test.performance.PerformanceTestSuite;

public class PerformanceTestQueries extends PerformanceTestSuite {

    /**
     * {@inheritDoc}
     */
    @Override
    protected PerformanceTest[] getPerfTests() {
        return new PerformanceTest[]{
                //new GetTrianglesWithSingleNodePTest()
                //new GetTrianglesWithAllNodesPTest()
                new GetTrianglesDBSinglePTest()
        };
    }

}