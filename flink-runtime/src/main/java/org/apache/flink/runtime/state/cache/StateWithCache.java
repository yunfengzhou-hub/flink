package org.apache.flink.runtime.state.cache;

public interface StateWithCache<K> {
    void setCurrentKey(K key);
    void notifyLocalSnapshotStarted(long checkpointId) throws Exception;
    void notifyLocalSnapshotFinished(long checkpointId);
}
