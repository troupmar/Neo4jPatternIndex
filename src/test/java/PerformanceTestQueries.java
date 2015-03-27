import com.graphaware.test.performance.PerformanceTest;
import com.graphaware.test.performance.PerformanceTestSuite;

/**
 * Created by Jaroslav on 3/25/15.
 */
public class PerformanceTestQueries extends PerformanceTestSuite {

    /**
     * {@inheritDoc}
     */
    @Override
    protected PerformanceTest[] getPerfTests() {
        return new PerformanceTest[]{
                new PerformanceTestCypherTriangleCount()//,
                //new PerformanceTestCypherTriangleReturnNodes()
        };
    }


}