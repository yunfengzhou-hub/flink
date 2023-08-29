package org.apache.flink.runtime.state.cache;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.RunnableWithException;

import java.util.List;
import java.util.Map;

public abstract class AbstractStateWithCache<K, N, V, S extends State> implements StateWithCache<K> {
    private static final long NO_CHECKPOINT_ID = -1;
    private final N namespace;
    private final TypeSerializer<N> namespaceSerializer;
    private final StateDescriptor<S, V> cacheStateDescriptor;
    protected final S state;
    protected final S stateForCache;

    protected final KeyedStateBackend<K> keyedStateBackend;

    protected final KeyedStateBackend<K> keyedStateBackendForCache;

    private LinkedHashMapLRUCache<K, V> lruCache;

    protected K currentKey;

    private long currentlyReferencingCheckpointID;

    public AbstractStateWithCache(
            N namespace,
            TypeSerializer<N> namespaceSerializer,
            StateDescriptor<S, V> cacheStateDescriptor,
            S state,
            S stateForCache,
            KeyedStateBackend<K> keyedStateBackend,
            KeyedStateBackend<K> keyedStateBackendForCache,
            int keySize) throws Exception {
        this.namespace = namespace;
        this.namespaceSerializer = namespaceSerializer;
        this.cacheStateDescriptor = cacheStateDescriptor;
        this.state = state;
        this.stateForCache = stateForCache;
        this.keyedStateBackend = keyedStateBackend;
        this.keyedStateBackendForCache = keyedStateBackendForCache;
        this.lruCache = new LinkedHashMapLRUCache<>(keySize, this::removalCallback);
        this.currentlyReferencingCheckpointID = NO_CHECKPOINT_ID;

        keyedStateBackendForCache.applyToAllKeys(
                namespace,
                namespaceSerializer,
                cacheStateDescriptor,
                (key, cacheState) -> lruCache.put(key, getValue(cacheState)));
    }

    @Override
    public final void setCurrentKey(K key) {
        currentKey = key;
    }

    @Override
    public RunnableWithException notifyLocalSnapshotStarted(long checkpointId) throws Exception {
        // TODO: verify this when max concurrent checkpoint > 1
        Preconditions.checkState(currentlyReferencingCheckpointID == NO_CHECKPOINT_ID);
        currentlyReferencingCheckpointID = checkpointId;
        return () -> {
            keyedStateBackendForCache.applyToAllKeys(
                    namespace,
                    namespaceSerializer,
                    cacheStateDescriptor,
                    (key, state) -> state.clear()
            );
            for (Map.Entry<K, V> entry: lruCache.entrySet()) {
                keyedStateBackendForCache.setCurrentKey(entry.getKey());
                updateValue(stateForCache, entry.getValue());
            }
        };
    }

    @Override
    public void notifyLocalSnapshotFinished(long checkpointId) {
        if (currentlyReferencingCheckpointID == checkpointId) {
            currentlyReferencingCheckpointID = NO_CHECKPOINT_ID;
        }
    }

    protected final V getFromCache() {
        return lruCache.get(currentKey);
    }

    protected final void addToCache(V value) {
        if (currentlyReferencingCheckpointID != NO_CHECKPOINT_ID) {
            currentlyReferencingCheckpointID = NO_CHECKPOINT_ID;
            lruCache = lruCache.clone();
        }
        lruCache.put(currentKey, value);
    }

    protected abstract V getValue(S state) throws Exception;

    protected abstract void updateValue(S state, V value) throws Exception;

    private void removalCallback(K keyToRemove, V value) {
        keyedStateBackend.setCurrentKey(keyToRemove);
        try {
            updateValue(state, value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
