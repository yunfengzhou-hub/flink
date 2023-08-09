package org.apache.flink.runtime.state.cache;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.SavepointResources;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.TestableKeyedStateBackend;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.RunnableFuture;
import java.util.stream.Stream;

public class KeyedStateBackendWithCache<K>
        implements CheckpointableKeyedStateBackend<K>, TestableKeyedStateBackend<K> {
    private final CheckpointableKeyedStateBackend<K> backend;
    private final CheckpointableKeyedStateBackend<K> backendForCache;

    private final Map<String, StateWithCache> states;

    public KeyedStateBackendWithCache(
            CheckpointableKeyedStateBackend<K> backend,
            CheckpointableKeyedStateBackend<K> backendForCache) {
        this.backend = Preconditions.checkNotNull(backend);
        this.backendForCache = Preconditions.checkNotNull(backendForCache);
        this.states = new HashMap<>();
    }

    @Override
    public void setCurrentKey(K newKey) {
        backend.setCurrentKey(newKey);
        backendForCache.setCurrentKey(newKey);
    }

    @Override
    public K getCurrentKey() {
        return backend.getCurrentKey();
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return backend.getKeySerializer();
    }

    @Override
    public <N, S extends State, T> void applyToAllKeys(
            N namespace,
            TypeSerializer<N> namespaceSerializer,
            StateDescriptor<S, T> stateDescriptor,
            KeyedStateFunction<K, S> function)
            throws Exception {
        backendForCache.applyToAllKeys(namespace, namespaceSerializer, stateDescriptor, function);
        backend.applyToAllKeys(namespace, namespaceSerializer, stateDescriptor, function);
    }

    @Override
    public <N> Stream<K> getKeys(String state, N namespace) {
        return Stream.concat(
                backendForCache.getKeys(state, namespace), backend.getKeys(state, namespace));
    }

    @Override
    public <N> Stream<Tuple2<K, N>> getKeysAndNamespaces(String state) {
        return Stream.concat(
                backendForCache.getKeysAndNamespaces(state), backend.getKeysAndNamespaces(state));
    }

    @Override
    public <N, S extends State, T> S getOrCreateKeyedState(
            TypeSerializer<N> namespaceSerializer, StateDescriptor<S, T> stateDescriptor)
            throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public <N, S extends State> S getPartitionedState(
            N namespace,
            TypeSerializer<N> namespaceSerializer,
            StateDescriptor<S, ?> stateDescriptor)
            throws Exception {
        S state = backend.getPartitionedState(namespace, namespaceSerializer, stateDescriptor);
        if (!(stateDescriptor instanceof ValueStateDescriptor)) {
            // TODO: Support all states.
            return state;
        }
        S stateForCache =
                backendForCache.getPartitionedState(
                        namespace, namespaceSerializer, stateDescriptor);
        ValueStateWithCache result =
                new ValueStateWithCache(
                        namespace,
                        namespaceSerializer,
                        (ValueStateDescriptor) stateDescriptor,
                        (ValueState) state,
                        (ValueState) stateForCache,
                        backend,
                        backendForCache);
        // TODO: avoid recreating state with cache.
        states.put(stateDescriptor.getName(), result);
        return (S) result;
    }

    public void sync() throws Exception {
        for (StateWithCache state : states.values()) {
            state.sync();
        }
    }

    @Override
    public void dispose() {
        backendForCache.dispose();
        backend.dispose();
    }

    @Override
    public void registerKeySelectionListener(KeySelectionListener<K> listener) {
        backendForCache.registerKeySelectionListener(listener);
        backend.registerKeySelectionListener(listener);
    }

    @Override
    public boolean deregisterKeySelectionListener(KeySelectionListener<K> listener) {
        return backendForCache.deregisterKeySelectionListener(listener)
                && backend.deregisterKeySelectionListener(listener);
    }

    @Nonnull
    @Override
    public <N, SV, SEV, S extends State, IS extends S> IS createOrUpdateInternalState(
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<S, SV> stateDesc,
            @Nonnull
                    StateSnapshotTransformer.StateSnapshotTransformFactory<SEV>
                            snapshotTransformFactory)
            throws Exception {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            KeyGroupedInternalPriorityQueue<T> create(
                    @Nonnull String stateName,
                    @Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public KeyGroupRange getKeyGroupRange() {
        return backend.getKeyGroupRange();
    }

    @Nonnull
    @Override
    public SavepointResources<K> savepoint() throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        backend.close();
        backendForCache.close();
    }

    @Nonnull
    @Override
    public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
            long checkpointId,
            long timestamp,
            @Nonnull CheckpointStreamFactory streamFactory,
            @Nonnull CheckpointOptions checkpointOptions)
            throws Exception {
        for (StateWithCache state : states.values()) {
            state.removeOutdatedState();
        }
        RunnableFuture<SnapshotResult<KeyedStateHandle>> future =
                backend.snapshot(checkpointId, timestamp, streamFactory, checkpointOptions);
        RunnableFuture<SnapshotResult<KeyedStateHandle>> futureForCache =
                backendForCache.snapshot(checkpointId, timestamp, streamFactory, checkpointOptions);
        return new RunnableFutureWithCache(future, futureForCache);
    }

    @Override
    public int numKeyValueStateEntries() {
        if (!(backend instanceof TestableKeyedStateBackend)
                || !(backendForCache instanceof TestableKeyedStateBackend)) {
            throw new UnsupportedOperationException();
        }
        return ((TestableKeyedStateBackend<?>) backend).numKeyValueStateEntries()
                + ((TestableKeyedStateBackend<?>) backendForCache).numKeyValueStateEntries();
    }
}
