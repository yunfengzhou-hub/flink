package org.apache.flink.streaming.runtime.streamrecord;

public interface AllowedLatencyEventHandler {
    void handleAllowedLatencyEvent(AllowedLatencyEvent allowedLatencyEvent);
}
