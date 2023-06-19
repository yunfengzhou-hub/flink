package org.apache.flink.runtime.jobgraph.tasks;

import org.apache.flink.runtime.flush.FlushRuntimeEvent;

public interface FlushableTask {
    void triggerFlush(FlushRuntimeEvent event);
}
