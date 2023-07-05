package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.PublicEvolving;

/**
 * OperatorAttributes element provides Job Manager with information that can be used to optimize the
 * job performance.
 */
@PublicEvolving
public class OperatorAttributes {
    private final boolean isEmittingRecordsWithTimestamp;

    private OperatorAttributes(boolean isEmittingRecordsWithTimestamp) {
        this.isEmittingRecordsWithTimestamp = isEmittingRecordsWithTimestamp;
    }

    /**
     * Returns true if the operator might assign additional timestamps to the output records. False
     * if the operator only forward timestamps of existing input records to the output.
     */
    public boolean getIsEmittingRecordsWithTimestamp() {
        return isEmittingRecordsWithTimestamp;
    }

    @PublicEvolving
    public static class Builder {
        private boolean isEmittingRecordsWithTimestamp;

        public Builder() {
            this.isEmittingRecordsWithTimestamp = true;
        }

        public Builder setIsEmittingRecordsWithTimestamp(boolean isEmittingRecordsWithTimestamp) {
            this.isEmittingRecordsWithTimestamp = isEmittingRecordsWithTimestamp;
            return this;
        }

        public OperatorAttributes build() {
            return new OperatorAttributes(isEmittingRecordsWithTimestamp);
        }
    }
}
