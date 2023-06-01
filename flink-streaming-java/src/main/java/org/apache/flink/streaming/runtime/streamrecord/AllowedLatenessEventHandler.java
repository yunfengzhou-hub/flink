package org.apache.flink.streaming.runtime.streamrecord;

public interface AllowedLatenessEventHandler {
    void handleAllowedLatenessEvent(AllowedLatenessEvent allowedLatenessEvent);
}
