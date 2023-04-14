package org.apache.flink.streaming.runtime;

import org.apache.flink.api.connector.source.SourceEvent;

// TODO: rename or restructure this class to improve api.
public interface SourceEventToStreamStatusEventConverter {
    StreamStatusEvent convert(SourceEvent event);
}
