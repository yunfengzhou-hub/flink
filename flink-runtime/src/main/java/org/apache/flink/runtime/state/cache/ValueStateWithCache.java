package org.apache.flink.runtime.state.cache;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyedStateBackend;

import java.io.IOException;
import java.util.Map;

public class ValueStateWithCache<K, N, V> implements ValueState<V>, StateWithCache<K> {
    private final N namespace;
    private final TypeSerializer<N> namespaceSerializer;
    private final ValueStateDescriptor<V> stateDescriptor;
    private final ValueState<V> state;
    private final ValueState<V> stateForCache;

    private final KeyedStateBackend<K> keyedStateBackend;

    private final KeyedStateBackend<K> keyedStateBackendForCache;

    private final Map<K, V> lruCache;

    private K currentKey;

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
        this.namespace = namespace;
        this.namespaceSerializer = namespaceSerializer;
        this.stateDescriptor = stateDescriptor;
        this.state = state;
        this.stateForCache = stateForCache;
        this.keyedStateBackend = keyedStateBackend;
        this.keyedStateBackendForCache = keyedStateBackendForCache;
        this.lruCache = new LinkedHashMapLRUCache<>(keySize, this::removalCallback);

        keyedStateBackendForCache.applyToAllKeys(
                namespace,
                namespaceSerializer,
                stateDescriptor,
                (key, ignoredState) -> lruCache.put(key, ignoredState.value()));
    }

    @Override
    public V value() throws IOException {
        V currentValue = lruCache.get(currentKey);
        if (currentValue == null) {
            keyedStateBackend.setCurrentKey(currentKey);
            currentValue = state.value();
            lruCache.put(currentKey, currentValue);
        }
        return currentValue;
    }

    @Override
    public void update(V value) throws IOException {
        lruCache.put(currentKey, value);
    }

    @Override
    public final void setCurrentKey(K key) {
        currentKey = key;
    }

    @Override
    public void removeOutdatedState() throws Exception {
        keyedStateBackendForCache.applyToAllKeys(
                namespace,
                namespaceSerializer,
                stateDescriptor,
                (key, state) -> state.clear()
        );
        for (Map.Entry<K, V> entry : lruCache.entrySet()) {
            keyedStateBackend.setCurrentKey(entry.getKey());
            state.clear();
            keyedStateBackendForCache.setCurrentKey(entry.getKey());
            stateForCache.update(entry.getValue());
        }
    }

    @Override
    public void clear() {
        lruCache.put(currentKey, null);
    }

    private void removalCallback(K keyToRemove, V value) {
        keyedStateBackend.setCurrentKey(keyToRemove);
        try {
            state.update(value);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
