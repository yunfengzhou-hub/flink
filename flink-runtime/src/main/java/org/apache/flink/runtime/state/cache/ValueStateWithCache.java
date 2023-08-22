package org.apache.flink.runtime.state.cache;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyedStateBackend;

import org.apache.flink.shaded.guava30.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava30.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava30.com.google.common.cache.RemovalNotification;

import java.io.IOException;

public class ValueStateWithCache<K, N, V> implements ValueState<V>, StateWithCache {
    private final ValueState<V> state;
    private final ValueState<V> stateForCache;

    private final KeyedStateBackend<K> keyedStateBackend;

    private final KeyedStateBackend<K> keyedStateBackendForCache;

    private final Cache<K, Boolean> keysCache;

    public ValueStateWithCache(
            N namespace,
            TypeSerializer<N> namespaceSerializer,
            ValueStateDescriptor<V> stateDescriptor,
            ValueState<V> state,
            ValueState<V> stateForCache,
            KeyedStateBackend<K> keyedStateBackend,
            KeyedStateBackend<K> keyedStateBackendForCache,
            int keySize)
            throws Exception {
        this.state = state;
        this.stateForCache = stateForCache;
        this.keyedStateBackend = keyedStateBackend;
        this.keyedStateBackendForCache = keyedStateBackendForCache;
        this.keysCache =
                CacheBuilder.newBuilder()
                        .maximumSize(keySize)
                        .removalListener(this::removalCallback)
                        .build();

        keyedStateBackendForCache.applyToAllKeys(
                namespace,
                namespaceSerializer,
                stateDescriptor,
                (key, ignoredState) -> keysCache.put(key, true));
    }

    @Override
    public V value() throws IOException {
        K currentKey = keyedStateBackend.getCurrentKey();
        if (keysCache.getIfPresent(currentKey) == null) {
            stateForCache.update(state.value());
        }
        keysCache.put(currentKey, true);
        return stateForCache.value();
    }

    @Override
    public void update(V value) throws IOException {
        keysCache.put(keyedStateBackendForCache.getCurrentKey(), true);
        stateForCache.update(value);
    }

    @Override
    public void removeOutdatedState() {
        K currentKey = keyedStateBackend.getCurrentKey();
        for (K key : keysCache.asMap().keySet()) {
            keyedStateBackend.setCurrentKey(key);
            state.clear();
        }
        keyedStateBackend.setCurrentKey(currentKey);
    }

    @Override
    public void clear() {
        keysCache.put(keyedStateBackend.getCurrentKey(), true);
        stateForCache.clear();
    }

    private void removalCallback(RemovalNotification<K, Boolean> notification) {
        switch (notification.getCause()) {
            case REPLACED:
                return;
            case SIZE:
                K currentKey = keyedStateBackendForCache.getCurrentKey();
                K keyToRemove = notification.getKey();
                keyedStateBackend.setCurrentKey(keyToRemove);
                keyedStateBackendForCache.setCurrentKey(keyToRemove);
                try {
                    state.update(stateForCache.value());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                stateForCache.clear();
                keyedStateBackend.setCurrentKey(currentKey);
                keyedStateBackendForCache.setCurrentKey(currentKey);
                return;
            default:
                throw new UnsupportedOperationException(notification.getCause().toString());
        }
    }
}
