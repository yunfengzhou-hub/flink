package org.apache.flink.runtime.jobgraph.tasks;

import org.apache.flink.runtime.flush.FlushEvent;

public interface FlushableTask {
    void flush(FlushEvent event);
}
