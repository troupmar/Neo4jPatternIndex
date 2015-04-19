package com.troupmar.graphaware.transactionHandle;

import com.graphaware.common.policy.NodeInclusionPolicy;
import com.troupmar.graphaware.NodeLabels;
import org.neo4j.graphdb.Node;

import static com.troupmar.graphaware.NodeLabels.*;

/**
 * Created by Martin on 16.04.15.
 */
public class MetaNodeInclusionPolicy implements NodeInclusionPolicy {


    @Override
    public boolean include(Node node) {
        return !node.hasLabel(_META_);
    }
}
