package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.jobgraph.OperatorID;

public class OperatorCheckpointContext {
    private final CheckpointCoordinator coordinator;

    private final OperatorID operatorID;

    public OperatorCheckpointContext(CheckpointCoordinator coordinator, OperatorID operatorID) {
        this.coordinator = coordinator;
        this.operatorID = operatorID;
    }
}
