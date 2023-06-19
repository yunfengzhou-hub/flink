package org.apache.flink.runtime.flush;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.RuntimeEvent;

import java.io.IOException;

/**
 * A {@link RuntimeEvent} signaling that the receiver operator should perform a flush operation. A
 * flush operation means that after processing this event, the operator should have forwarded all
 * output records that could be correctly inferred from previously received input records to
 * downstream operators or external systems. After the output records have been flushed, this event
 * should also be forwarded to downstream.
 *
 * <p>If the Flink job has enabled exactly-once checkpoint, which means some operators can only
 * flush results on checkpoints, operators in the Flink job would not receive flush events.
 */
public class FlushEvent extends RuntimeEvent {
    /**
     * Returns the ID of this FlushEvent. An ID is a monotonically increasing number that signals
     * the order of flush events.
     *
     * <p>If the receiver operator of this event has more than one input, the operator may only
     * flush results when the maximum ID of flush events received from all inputs has increased.
     */
    public long getFlushEventId() {
        return 0L;
    }

    @Override
    public void write(DataOutputView out) throws IOException {
        throw new UnsupportedOperationException("This method should never be called");
    }

    @Override
    public void read(DataInputView in) throws IOException {
        throw new UnsupportedOperationException("This method should never be called");
    }
}
