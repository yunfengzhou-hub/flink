package org.apache.flink.runtime.state.cache;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.heap.HeapMapState;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

public class ValueStateWithCache<K, N, V> implements ValueState<V>, StateWithCache<K> {
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

    private long currentlyReferencingCheckpointID;

    public ValueStateWithCache(
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

        keyedStateBackendForCache.applyToAllKeys(
                namespace,
                namespaceSerializer,
                cacheStateDescriptor,
                (key, cacheState) -> cacheState.entries().forEach(
                        entry -> lruCache.put(entry.getKey(), entry.getValue())));
    }

    @Override
    public V value() throws IOException {
        V currentValue = lruCache.get(currentKey);
        if (currentValue == null) {
            keyedStateBackend.setCurrentKey(currentKey);
            currentValue = state.value();
            if (currentlyReferencingCheckpointID != NO_CHECKPOINT_ID) {
                currentlyReferencingCheckpointID = NO_CHECKPOINT_ID;
                lruCache = lruCache.clone();
            }
            lruCache.put(currentKey, currentValue);
        }
        return currentValue;
    }

    @Override
    public void update(V value) throws IOException {
        if (currentlyReferencingCheckpointID != NO_CHECKPOINT_ID) {
            currentlyReferencingCheckpointID = NO_CHECKPOINT_ID;
            lruCache = lruCache.clone();
        }
        lruCache.put(currentKey, value);
    }

    @Override
    public final void setCurrentKey(K key) {
        currentKey = key;
    }

    @Override
    public void notifyLocalSnapshotStarted(long checkpointId) throws Exception {
        // TODO: verify this when max concurrent checkpoint > 1
        Preconditions.checkState(currentlyReferencingCheckpointID == NO_CHECKPOINT_ID);
        currentlyReferencingCheckpointID = checkpointId;
        keyedStateBackendForCache.applyToAllKeys(
                namespace,
                namespaceSerializer,
                cacheStateDescriptor,
                (key, state) -> state.clear()
        );
        keyedStateBackendForCache.setCurrentKey(currentKey);
        stateForCache.update(lruCache);
    }

    @Override
    public void notifyLocalSnapshotFinished(long checkpointId) {
        if (currentlyReferencingCheckpointID == checkpointId) {
            currentlyReferencingCheckpointID = NO_CHECKPOINT_ID;
        }
    }

    @Override
    public void clear() {
        if (currentlyReferencingCheckpointID != NO_CHECKPOINT_ID) {
            currentlyReferencingCheckpointID = NO_CHECKPOINT_ID;
            lruCache = lruCache.clone();
        }
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
