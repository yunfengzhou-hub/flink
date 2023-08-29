package org.apache.flink.runtime.state.cache;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ListStateWithCache<K, N, V>
        extends AbstractStateWithCache<K, N, List<V>, ListState<V>>
        implements InternalListState<K, N, V> {

    public ListStateWithCache(
            N namespace,
            TypeSerializer<N> namespaceSerializer,
            ListStateDescriptor<V> cacheStateDescriptor,
            InternalListState<K, N, V> state,
            InternalListState<K, N, V> stateForCache,
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
    public Iterable<V> get() throws Exception {
        List<V> currentValue = getFromCache();
        System.out.println(currentValue);
        if (currentValue == null) {
            keyedStateBackend.setCurrentKey(currentKey);
            Iterable<V> tmpCurrentValue = state.get();
            if (tmpCurrentValue != null) {
                currentValue = new ArrayList<>();
                tmpCurrentValue.forEach(currentValue::add);
            }
            addToCache(currentValue);
        }
        System.out.println(currentValue);
        return currentValue;
    }

    @Override
    public void add(V value) throws Exception {
        Preconditions.checkNotNull(value);
        List<V> list = getFromCache();
        if (list == null) {
            list = new ArrayList<>();
        }
        list.add(value);
        addToCache(list);
    }

    @Override
    public void update(List<V> values) throws Exception {
        Preconditions.checkNotNull(values);
        for (V value: values) {
            Preconditions.checkNotNull(value);
        }
        if (values.isEmpty()) {
            values = null;
        } else {
            values = new ArrayList<>(values);
        }
        addToCache(values);
    }

    @Override
    public void addAll(List<V> values) throws Exception {
        Preconditions.checkNotNull(values);
        if (values.isEmpty()) {
            return;
        }
        for (V value: values) {
            Preconditions.checkNotNull(value);
        }
        List<V> list = getFromCache();
        if (list == null) {
            list = new ArrayList<>();
        }
        list.addAll(values);
        addToCache(list);
    }

    @Override
    public void clear() {
        addToCache(null);
    }

    static <K, N, V> ListStateWithCache<K, N, V> getOrCreateKeyedState(
            TypeSerializer<N> namespaceSerializer,
            ListStateDescriptor<V> stateDescriptor,
            CheckpointableKeyedStateBackend<K> backend,
            CheckpointableKeyedStateBackend<K> backendForCache,
            int keySize)
            throws Exception {
        InternalListState<K, N, V> state = (InternalListState<K, N, V>) backend.getOrCreateKeyedState(namespaceSerializer, stateDescriptor);
        InternalListState<K, N, V> stateForCache =
                (InternalListState<K, N, V>) backendForCache.getOrCreateKeyedState(namespaceSerializer, stateDescriptor);
        return new ListStateWithCache<>(
                (N) VoidNamespace.INSTANCE,
                namespaceSerializer,
                stateDescriptor,
                state,
                stateForCache,
                backend,
                backendForCache,
                keySize);
    }

    static <K, N, V> ListStateWithCache<K, N, V> getPartitionedState(
            N namespace,
            TypeSerializer<N> namespaceSerializer,
            ListStateDescriptor<V> stateDescriptor,
            CheckpointableKeyedStateBackend<K> backend,
            CheckpointableKeyedStateBackend<K> backendForCache,
            int keySize) throws Exception {
        InternalListState<K, N, V> state = (InternalListState<K, N, V>) backend.getPartitionedState(namespace, namespaceSerializer, stateDescriptor);
        InternalListState<K, N, V> stateForCache =
                (InternalListState<K, N, V>) backendForCache.getPartitionedState(
                        namespace, namespaceSerializer, stateDescriptor);
        return new ListStateWithCache<>(
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
    public List<V> getInternal() throws Exception {
        return (List<V>) get();
    }

    @Override
    public void updateInternal(List<V> valueToStore) throws Exception {
        update(valueToStore);
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return ((InternalKvState<K, N, V>) stateForCache).getKeySerializer();
    }

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {
        return ((InternalKvState<K, N, V>) stateForCache).getNamespaceSerializer();
    }

    @Override
    public TypeSerializer<List<V>> getValueSerializer() {
        return ((InternalKvState<K, N, List<V>>) stateForCache).getValueSerializer();
    }

    @Override
    public void setCurrentNamespace(N namespace) {
        ((InternalKvState<K, N, V>) state).setCurrentNamespace(namespace);
        ((InternalKvState<K, N, V>) stateForCache).setCurrentNamespace(namespace);
    }

    @Override
    public byte[] getSerializedValue(
            byte[] serializedKeyAndNamespace,
            TypeSerializer<K> safeKeySerializer,
            TypeSerializer<N> safeNamespaceSerializer,
            TypeSerializer<List<V>> safeValueSerializer) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public StateIncrementalVisitor<K, N, List<V>> getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    protected List<V> getValue(ListState<V> state) throws Exception {
        return (List<V>) state.get();
    }

    @Override
    protected void updateValue(ListState<V> state, List<V> value) throws Exception {
        state.update(value);
    }
}
