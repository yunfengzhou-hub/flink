package org.apache.flink.runtime.state.cache;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class ValueStateWithCache<K, N, V> implements ValueState<V>, StateWithCache {
    private final ValueState<V> state;
    private final ValueState<V> stateForCache;

    private final KeyedStateBackend<K> keyedStateBackend;

    private final KeyedStateBackend<K> keyedStateBackendForCache;

    private final Set<K> keysInCache;

    public ValueStateWithCache(
            N namespace,
            TypeSerializer<N> namespaceSerializer,
            ValueStateDescriptor<V> stateDescriptor,
            ValueState<V> state,
            ValueState<V> stateForCache,
            KeyedStateBackend<K> keyedStateBackend,
            KeyedStateBackend<K> keyedStateBackendForCache)
            throws Exception {
        this.state = state;
        this.stateForCache = stateForCache;
        this.keyedStateBackend = keyedStateBackend;
        this.keyedStateBackendForCache = keyedStateBackendForCache;
        this.keysInCache = new HashSet<>();
        keyedStateBackendForCache.applyToAllKeys(
                namespace,
                namespaceSerializer,
                stateDescriptor,
                (key, ignoredState) -> keysInCache.add(key));
    }

    @Override
    public V value() throws IOException {
        if (!keysInCache.contains(keyedStateBackend.getCurrentKey())) {
            stateForCache.update(state.value());
            keysInCache.add(keyedStateBackend.getCurrentKey());
        }
        return stateForCache.value();
    }

    @Override
    public void update(V value) throws IOException {
        stateForCache.update(value);
        keysInCache.add(keyedStateBackend.getCurrentKey());
    }

    @Override
    public void sync() throws Exception {
        Preconditions.checkState(keyedStateBackend.getCurrentKey() == null);
        Preconditions.checkState(keyedStateBackendForCache.getCurrentKey() == null);
        for (K key : keysInCache) {
            keyedStateBackend.setCurrentKey(key);
            keyedStateBackendForCache.setCurrentKey(key);
            state.update(stateForCache.value());
            stateForCache.clear();
        }
        keyedStateBackend.setCurrentKey(null);
        keyedStateBackendForCache.setCurrentKey(null);
        keysInCache.clear();
    }

    @Override
    public void removeOutdatedState() {
        K currentKey = keyedStateBackend.getCurrentKey();
        for (K key : keysInCache) {
            keyedStateBackend.setCurrentKey(key);
            state.clear();
        }
        keyedStateBackend.setCurrentKey(currentKey);
    }

    @Override
    public void clear() {
        stateForCache.clear();
        keysInCache.add(keyedStateBackend.getCurrentKey());
    }
}
