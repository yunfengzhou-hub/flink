package org.apache.flink.streaming.runtime.streamrecord;

import org.apache.flink.annotation.Internal;

import javax.annotation.Nullable;

import java.time.Duration;

@Internal
public class AllowedLatencyEvent extends StreamElement {
    /** None or positive value. If None, it means no lateness requirement. */
    private final Duration allowedLateness;

    public AllowedLatencyEvent(@Nullable Duration allowedLateness) {
        this.allowedLateness = allowedLateness;
    }

    public Duration getAllowedLatency() {
        return allowedLateness;
    }
}
