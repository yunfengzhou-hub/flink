package org.apache.flink.runtime.state.cache;

import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.StateHandleID;

import javax.annotation.Nullable;

public class KeyedStateHandleWithCache implements KeyedStateHandle {
    @Nullable private final KeyedStateHandle handle;
    @Nullable private final KeyedStateHandle handleForCache;

    private final StateHandleID stateHandleID;

    private KeyedStateHandleWithCache(
            @Nullable KeyedStateHandle handle, @Nullable KeyedStateHandle handleForCache) {
        if (handle == null && handleForCache == null) {
            throw new UnsupportedOperationException();
        }
        this.handle = handle;
        this.handleForCache = handleForCache;
        this.stateHandleID =
                new StateHandleID(
                        (handle == null ? "" : handle.getStateHandleId().getKeyString())
                                + "-"
                                + (handleForCache == null
                                        ? ""
                                        : handleForCache.getStateHandleId().getKeyString()));
    }

    @Nullable
    public KeyedStateHandle getHandle() {
        return handle;
    }

    @Nullable
    public KeyedStateHandle getHandleForCache() {
        return handleForCache;
    }

    @Override
    public void registerSharedStates(SharedStateRegistry stateRegistry, long checkpointID) {
        if (handle != null) {
            handle.registerSharedStates(stateRegistry, checkpointID);
        }
        if (handleForCache != null) {
            handleForCache.registerSharedStates(stateRegistry, checkpointID);
        }
    }

    @Override
    public long getCheckpointedSize() {
        long size = 0;
        if (handle != null) {
            size += handle.getCheckpointedSize();
        }
        if (handleForCache != null) {
            size += handleForCache.getCheckpointedSize();
        }
        return size;
    }

    @Override
    public KeyGroupRange getKeyGroupRange() {
        if (handle != null) {
            return handle.getKeyGroupRange();
        } else {
            assert handleForCache != null;
            return handleForCache.getKeyGroupRange();
        }
    }

    @Nullable
    @Override
    public KeyedStateHandle getIntersection(KeyGroupRange keyGroupRange) {
        return KeyedStateHandleWithCache.create(
                handle == null ? null : handle.getIntersection(keyGroupRange),
                handleForCache == null ? null : handleForCache.getIntersection(keyGroupRange));
    }

    @Override
    public StateHandleID getStateHandleId() {
        return stateHandleID;
    }

    @Override
    public void discardState() throws Exception {
        if (handle != null) {
            handle.discardState();
        }
        if (handleForCache != null) {
            handleForCache.discardState();
        }
    }

    @Override
    public long getStateSize() {
        long size = 0;
        if (handle != null) {
            size += handle.getStateSize();
        }
        if (handleForCache != null) {
            size += handleForCache.getStateSize();
        }
        return size;
    }

    public static @Nullable KeyedStateHandleWithCache create(
            @Nullable KeyedStateHandle handle, @Nullable KeyedStateHandle handleForCache) {
        if (handle == null && handleForCache == null) {
            return null;
        }
        return new KeyedStateHandleWithCache(handle, handleForCache);
    }
}
