package org.apache.flink.runtime.state.cache;

public interface StateWithCache {
    void removeOutdatedState() throws Exception;
}
