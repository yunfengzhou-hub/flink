package org.apache.flink.streaming.runtime.streamrecord;

import org.apache.flink.annotation.Internal;

import javax.annotation.Nullable;

import java.time.Duration;

@Internal
public class AllowedLatenessEvent extends StreamElement {
    /** None or positive value. If None, it means no lateness requirement. */
    private final Duration allowedLateness;

    public AllowedLatenessEvent(@Nullable Duration allowedLateness) {
        this.allowedLateness = allowedLateness;
    }

    public Duration getAllowedLateness() {
        return allowedLateness;
    }
}
