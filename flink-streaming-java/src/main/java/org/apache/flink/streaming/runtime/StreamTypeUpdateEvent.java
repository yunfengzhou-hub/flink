package org.apache.flink.streaming.runtime;

import org.apache.flink.api.connector.source.Boundedness;

public class StreamTypeUpdateEvent extends StreamStatusEvent {
    public final Boundedness boundedness;

    public StreamTypeUpdateEvent(Boundedness boundedness) {
        this.boundedness = boundedness;
    }
}
