package org.apache.flink.runtime.state.cache;

public interface StateWithCache<K> {
    void setCurrentKey(K key);
    void removeOutdatedState() throws Exception;
}
