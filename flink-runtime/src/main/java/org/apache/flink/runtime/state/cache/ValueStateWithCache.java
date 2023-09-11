package org.apache.flink.runtime.state.cache;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;

import java.io.IOException;

public class ValueStateWithCache<K, N, V> extends AbstractStateWithCache<K, N, V, ValueState<V>> implements ValueState<V> {
    public ValueStateWithCache(
            N namespace,
            TypeSerializer<N> namespaceSerializer,
            ValueStateDescriptor<V> cacheStateDescriptor,
            ValueState<V> state,
            ValueState<V> stateForCache,
            KeyedStateBackend<K> keyedStateBackend,
            KeyedStateBackend<K> keyedStateBackendForCache,
            int keySize)
            throws Exception {
        super(
                namespace,
                namespaceSerializer,
                cacheStateDescriptor,
                state,
                stateForCache,
                keyedStateBackend,
                keyedStateBackendForCache,
                keySize
        );
    }

    @Override
    public V value() throws IOException {
        V currentValue = getFromCache();
        if (currentValue == null) {
            keyedStateBackend.setCurrentKey(currentKey);
            currentValue = getValue(state);
            addToCache(currentValue);
        }
        return currentValue;
    }

    @Override
    public void update(V value) throws IOException {
        addToCache(value);
    }

    @Override
    public void clear() {
        addToCache(null);
    }

    static <K, N, V> ValueStateWithCache<K, N, V> getOrCreateKeyedState(
            TypeSerializer<N> namespaceSerializer,
            ValueStateDescriptor<V> stateDescriptor,
            CheckpointableKeyedStateBackend<K> backend,
            CheckpointableKeyedStateBackend<K> backendForCache,
            int keySize)
            throws Exception {
        ValueState<V> state = backend.getOrCreateKeyedState(namespaceSerializer, stateDescriptor);
        ValueState<V> stateForCache =
                backendForCache.getOrCreateKeyedState(
                        namespaceSerializer, stateDescriptor);
        return new ValueStateWithCache<>(
                (N) VoidNamespace.INSTANCE,
                namespaceSerializer,
                stateDescriptor,
                state,
                stateForCache,
                backend,
                backendForCache,
                keySize);
    }

    static <K, N, V> ValueStateWithCache<K, N, V> getPartitionedState(
            N namespace,
            TypeSerializer<N> namespaceSerializer,
            ValueStateDescriptor<V> stateDescriptor,
            CheckpointableKeyedStateBackend<K> backend,
            CheckpointableKeyedStateBackend<K> backendForCache,
            int keySize) throws Exception {
        ValueState<V> state = backend.getPartitionedState(namespace, namespaceSerializer, stateDescriptor);
        ValueState<V> stateForCache =
                backendForCache.getPartitionedState(
                        namespace, namespaceSerializer, stateDescriptor);
        return new ValueStateWithCache<>(
                namespace,
                namespaceSerializer,
                stateDescriptor,
                state,
                stateForCache,
                backend,
                backendForCache,
                keySize);
    }

    @Override
    protected final V getValue(ValueState<V> state) throws IOException {
        return state.value();
    }

    @Override
    protected final void updateValue(ValueState<V> state, V value) throws IOException {
        state.update(value);
    }
}
