package org.apache.flink.runtime.state.cache;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.heap.HeapMapState;
import org.apache.flink.util.function.RunnableWithException;

import java.io.IOException;

public class ValueStateWithCacheV2<K, N, V> implements ValueState<V>, StateWithCache<K> {
    private static final long NO_CHECKPOINT_ID = -1;
    private final N namespace;
    private final TypeSerializer<N> namespaceSerializer;
    private final MapStateDescriptor<K, V> cacheStateDescriptor;
    private final ValueState<V> state;
    private final HeapMapState<K, ?, K, V> stateForCache;

    private final KeyedStateBackend<K> keyedStateBackend;

    private final KeyedStateBackend<K> keyedStateBackendForCache;

    private LinkedHashMapLRUCache<K, V> lruCache;

    private K currentKey;

    private V currentValue;

    private boolean isCurrentValueSet;

    private long currentlyReferencingCheckpointID;

    public ValueStateWithCacheV2(
            N namespace,
            TypeSerializer<N> namespaceSerializer,
            MapStateDescriptor<K, V> cacheStateDescriptor,
            ValueState<V> state,
            MapState<K, V> stateForCache,
            KeyedStateBackend<K> keyedStateBackend,
            KeyedStateBackend<K> keyedStateBackendForCache,
            int keySize)
            throws Exception {
        this.namespace = namespace;
        this.namespaceSerializer = namespaceSerializer;
        this.cacheStateDescriptor = cacheStateDescriptor;
        this.state = state;
        this.stateForCache = (HeapMapState<K, ?, K, V>) stateForCache;
        this.keyedStateBackend = keyedStateBackend;
        this.keyedStateBackendForCache = keyedStateBackendForCache;
        this.lruCache = new LinkedHashMapLRUCache<>(keySize, this::removalCallback);
        this.currentlyReferencingCheckpointID = NO_CHECKPOINT_ID;
        this.isCurrentValueSet = false;

        keyedStateBackendForCache.applyToAllKeys(
                namespace,
                namespaceSerializer,
                cacheStateDescriptor,
                (key, cacheState) -> cacheState.entries().forEach(
                        entry -> lruCache.put(entry.getKey(), entry.getValue())));
    }

    @Override
    public V value() throws IOException {
        if (isCurrentValueSet) {
            return currentValue;
        }
        currentValue = lruCache.get(currentKey);
        if (currentValue == null) {
            keyedStateBackend.setCurrentKey(currentKey);
            currentValue = state.value();
        }
        isCurrentValueSet = true;
        return currentValue;
    }

    @Override
    public void update(V value) throws IOException {
        currentValue = value;
        isCurrentValueSet = true;
    }

    @Override
    public final void setCurrentKey(K key) {
        flushCurrentValueToCache();
        currentKey = key;
    }

    @Override
    public RunnableWithException notifyLocalSnapshotStarted(long checkpointId) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void notifyLocalSnapshotFinished(long checkpointId) {
        if (currentlyReferencingCheckpointID == checkpointId) {
            currentlyReferencingCheckpointID = NO_CHECKPOINT_ID;
        }
    }

    @Override
    public void clear() {
        currentValue = null;
        isCurrentValueSet = true;
    }

    private void flushCurrentValueToCache() {
        if (isCurrentValueSet) {
            if (currentlyReferencingCheckpointID != NO_CHECKPOINT_ID) {
                currentlyReferencingCheckpointID = NO_CHECKPOINT_ID;
                lruCache = lruCache.clone();
            }
            lruCache.put(currentKey, currentValue);
            isCurrentValueSet = false;
        }
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
