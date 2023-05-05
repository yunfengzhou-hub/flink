package org.apache.flink.runtime.checkpoint;

/** The checkpoint would be triggered periodically with a designated interval. */
public class PeriodicStrategy implements CheckpointTriggeringStrategy {
    public PeriodicStrategy(long checkpointInterval) {
        // interval is specified by the constructor parameter.
    }

    public PeriodicStrategy() {
        // interval is specified by execution.checkpointing.interval.
    }
}
