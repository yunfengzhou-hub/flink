/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.streamrecord;

import org.apache.flink.annotation.PublicEvolving;

/**
 * A {@link ControlEvent} signaling that the receiver operator should perform a flush operation. A
 * flush operation means that after processing this event, the operator should have forwarded all
 * output records that could be correctly inferred from previously received input records to
 * downstream operators or external systems. After the output records have been flushed, the
 * operator should also forward this event to downstream.
 *
 * <p>If the Flink job has enabled exactly-once checkpoint, which means some operators can only
 * flush results on checkpoints, operators in the Flink job would not receive flush events.
 */
@PublicEvolving
public class FlushEvent extends ControlEvent {
    /** A monotonically increasing number that signals the order of flush events. */
    protected final long flushEventId;

    public FlushEvent(long flushEventId) {
        this.flushEventId = flushEventId;
    }

    /**
     * Returns the ID of this FlushEvent. An ID is a monotonically increasing number that signals
     * the order of flush events.
     *
     * <p>If the receiver operator of this event has more than one input, the operator may only
     * flush results when the maximum ID of flush events received from all inputs has increased.
     */
    public long getFlushEventId() {
        return flushEventId;
    }

    /**
     * Returns a boolean denoting the flushing behavior an operator should follow when processing
     * the next incoming StreamRecords. If true, operators should flush records automatically at
     * real-time interval. If false, operators may retain outputs until the next {@link FlushEvent}.
     */
    public boolean enableAutoFlush() {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets the strategy describing how receiver operators should perform flush operations for the
     * incoming records.
     */
    public FlushStrategy getFlushStrategy() {
        return FlushStrategy.NO_ACTIVE_FLUSH;
    }
}
