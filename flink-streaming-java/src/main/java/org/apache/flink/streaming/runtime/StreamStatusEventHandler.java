package org.apache.flink.streaming.runtime;

public interface StreamStatusEventHandler {
    default boolean handleStreamStatusEvent(StreamStatusEvent event) {
        if (event instanceof StreamTypeUpdateEvent) {
            handleStreamTypeUpdateEvent((StreamTypeUpdateEvent) event);
            return true;
        }

        return false;
    }

    void handleStreamTypeUpdateEvent(StreamTypeUpdateEvent event);
}
