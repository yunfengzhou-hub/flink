package org.apache.flink.runtime.io.network.api;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.RuntimeEvent;
import org.apache.flink.runtime.jobgraph.OperatorID;

import java.io.IOException;

/**
 * A {@link RuntimeEvent} used to approximate the time a record needs to travel through the
 * dataflow.
 */
public class LatencyMarkerEvent extends RuntimeEvent {

    // ------------------------------------------------------------------------

    /** The time the latency mark is denoting. */
    private final long markedTime;

    private final OperatorID operatorId;

    private final int subtaskIndex;

    public LatencyMarkerEvent(long markedTime, OperatorID operatorId, int subtaskIndex) {
        this.markedTime = markedTime;
        this.operatorId = operatorId;
        this.subtaskIndex = subtaskIndex;
    }

    public long getMarkedTime() {
        return markedTime;
    }

    public OperatorID getOperatorId() {
        return operatorId;
    }

    public int getSubtaskIndex() {
        return subtaskIndex;
    }

    // ------------------------------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LatencyMarkerEvent that = (LatencyMarkerEvent) o;

        if (markedTime != that.markedTime) {
            return false;
        }
        if (!operatorId.equals(that.operatorId)) {
            return false;
        }
        return subtaskIndex == that.subtaskIndex;
    }

    @Override
    public int hashCode() {
        int result = (int) (markedTime ^ (markedTime >>> 32));
        result = 31 * result + operatorId.hashCode();
        result = 31 * result + subtaskIndex;
        return result;
    }

    @Override
    public String toString() {
        return "LatencyMarkerEvent{"
                + "markedTime="
                + markedTime
                + ", operatorId="
                + operatorId
                + ", subtaskIndex="
                + subtaskIndex
                + '}';
    }

    @Override
    public void write(DataOutputView out) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void read(DataInputView in) throws IOException {
        throw new UnsupportedOperationException();
    }
}
