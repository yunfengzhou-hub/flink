package org.apache.flink.runtime.state.cache;

import org.apache.flink.util.function.RunnableWithException;

public interface StateWithCache<K> {
    void setCurrentKey(K key);
    RunnableWithException notifyLocalSnapshotStarted(long checkpointId) throws Exception;
    void notifyLocalSnapshotFinished(long checkpointId);
}
