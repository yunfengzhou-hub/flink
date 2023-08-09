package org.apache.flink.runtime.state.cache;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.ConfigurableStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

public class StateBackendWithCache implements ConfigurableStateBackend {
    private final StateBackend stateBackend;
    private final StateBackend stateBackendForCache;

    public StateBackendWithCache(StateBackend stateBackend, StateBackend stateBackendForCache) {
        this.stateBackend = stateBackend;
        this.stateBackendForCache = stateBackendForCache;
    }

    @Override
    public <K> CheckpointableKeyedStateBackend<K> createKeyedStateBackend(
            Environment env,
            JobID jobID,
            String operatorIdentifier,
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            TaskKvStateRegistry kvStateRegistry,
            TtlTimeProvider ttlTimeProvider,
            MetricGroup metricGroup,
            @Nonnull Collection<KeyedStateHandle> stateHandles,
            CloseableRegistry cancelStreamRegistry)
            throws Exception {

        Collection<KeyedStateHandleWithCache> stateHandlesWithCache = new ArrayList<>();
        for (KeyedStateHandle handle : stateHandles) {
            if (handle == null) {
                continue;
            }
            if (!(handle instanceof KeyedStateHandleWithCache)) {
                throw new UnsupportedOperationException(String.valueOf(handle.getClass()));
            }
            stateHandlesWithCache.add((KeyedStateHandleWithCache) handle);
        }

        return new KeyedStateBackendWithCache<>(
                stateBackend.createKeyedStateBackend(
                        env,
                        jobID,
                        operatorIdentifier,
                        keySerializer,
                        numberOfKeyGroups,
                        keyGroupRange,
                        kvStateRegistry,
                        ttlTimeProvider,
                        metricGroup,
                        stateHandlesWithCache.stream()
                                .map(KeyedStateHandleWithCache::getHandle)
                                .filter(Objects::nonNull)
                                .collect(Collectors.toList()),
                        cancelStreamRegistry),
                stateBackendForCache.createKeyedStateBackend(
                        env,
                        jobID,
                        operatorIdentifier,
                        keySerializer,
                        numberOfKeyGroups,
                        keyGroupRange,
                        kvStateRegistry,
                        ttlTimeProvider,
                        metricGroup,
                        stateHandlesWithCache.stream()
                                .map(KeyedStateHandleWithCache::getHandleForCache)
                                .filter(Objects::nonNull)
                                .collect(Collectors.toList()),
                        cancelStreamRegistry));
    }

    @Override
    public OperatorStateBackend createOperatorStateBackend(
            Environment env,
            String operatorIdentifier,
            @Nonnull Collection<OperatorStateHandle> stateHandles,
            CloseableRegistry cancelStreamRegistry)
            throws Exception {
        return stateBackend.createOperatorStateBackend(
                env, operatorIdentifier, stateHandles, cancelStreamRegistry);
    }

    @Override
    public boolean useManagedMemory() {
        return stateBackend.useManagedMemory() || stateBackendForCache.useManagedMemory();
    }

    @Override
    public StateBackend configure(ReadableConfig config, ClassLoader classLoader)
            throws IllegalConfigurationException {
        if (!(stateBackend instanceof ConfigurableStateBackend)
                || !(stateBackendForCache instanceof ConfigurableStateBackend)) {
            throw new UnsupportedOperationException();
        }
        return new StateBackendWithCache(
                ((ConfigurableStateBackend) stateBackend).configure(config, classLoader),
                ((ConfigurableStateBackend) stateBackendForCache).configure(config, classLoader));
    }
}
