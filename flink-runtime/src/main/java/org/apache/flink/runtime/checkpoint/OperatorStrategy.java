package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.jobgraph.OperatorID;

/** The checkpoint would be triggered by a certain operator. */
public class OperatorStrategy implements CheckpointTriggeringStrategy {
    /**
     * @param operatorID the ID of the operator that would be responsible for triggering
     *     checkpoints.
     */
    public OperatorStrategy(OperatorID operatorID) {}
}
