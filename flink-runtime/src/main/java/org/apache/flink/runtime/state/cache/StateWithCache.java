package org.apache.flink.runtime.state.cache;

public interface StateWithCache {
    void sync() throws Exception;

    void removeOutdatedState() throws Exception;
}
