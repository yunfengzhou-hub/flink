package org.apache.flink.runtime.state.cache;

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.ConfigurableStateBackend;
import org.apache.flink.runtime.state.StateBackendTestBase;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.util.function.SupplierWithException;

import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

@RunWith(Parameterized.class)
public class StateBackendWithCacheTest extends StateBackendTestBase<StateBackendWithCache> {

    @ClassRule public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

    @Parameterized.Parameters
    public static List<Object[]> modes() {
        return Arrays.asList(
                new Object[][] {
                    {
                        (SupplierWithException<CheckpointStorage, IOException>)
                                JobManagerCheckpointStorage::new
                    },
                    {
                        (SupplierWithException<CheckpointStorage, IOException>)
                                () -> {
                                    String checkpointPath =
                                            TEMP_FOLDER.newFolder().toURI().toString();
                                    return new FileSystemCheckpointStorage(
                                            new Path(checkpointPath), 0, -1);
                                }
                    }
                });
    }

    @Parameterized.Parameter
    public SupplierWithException<CheckpointStorage, IOException> storageSupplier;

    @Override
    protected ConfigurableStateBackend getStateBackend() throws Exception {
        return new StateBackendWithCache(new HashMapStateBackend(), 100);
    }

    @Override
    protected CheckpointStorage getCheckpointStorage() throws Exception {
        return storageSupplier.get();
    }

    @Override
    protected boolean isSerializerPresenceRequiredOnRestore() {
        return false;
    }

    @Override
    protected boolean supportsAsynchronousSnapshots() {
        return false;
    }

    // TODO: test LRU eviction policy

    @Override
    @Ignore
    @Test
    public void testSnapshotNonAccessedState() throws Exception {}

    @Override
    @Ignore
    @Test
    public void testNumStateEntries() throws Exception {}

    @Override
    @Ignore
    @Test
    public void testConcurrentModificationWithApplyToAllKeys() throws Exception {}

    @Override
    @Ignore
    @Test
    public void testApplyToAllKeysLambdaFunction() throws Exception {}

    @Override
    @Ignore
    @Test
    public void testAggregatingStateAddAndGetWithMutableAccumulator() throws Exception {}

    @Override
    @Ignore
    @Test
    public void testAggregatingStateMergingWithMutableAccumulator() throws Exception {}

    @Override
    @Ignore
    @Test
    public void testAggregatingStateAddAndGetWithImmutableAccumulator() throws Exception {}

    @Override
    @Ignore
    @Test
    public void testAggregatingStateMergingWithImmutableAccumulator() throws Exception {}

    @Override
    @Ignore
    @Test
    public void testGetKeys() throws Exception {}

    @Override
    @Ignore
    @Test
    public void testGetKeysAndNamespaces() throws Exception {}

    @Override
    @Ignore
    @Test
    public void testReducingState() throws Exception {}

    @Override
    @Ignore
    @Test
    public void testReducingStateAddAndGet() throws Exception {}

    @Override
    @Ignore
    @Test
    public void testReducingStateMerging() throws Exception {}

    @Override
    @Ignore
    @Test
    public void testReducingStateDefaultValue() throws Exception {}

    @Override
    @Ignore
    @Test
    public void testKryoRegisteringRestoreResilienceWithRegisteredType() throws Exception {}

    @Override
    @Ignore
    @Test
    public void testKryoRegisteringRestoreResilienceWithDefaultSerializer() throws Exception {}

    @Override
    @Ignore
    @Test
    public void testKryoRegisteringRestoreResilienceWithRegisteredSerializer() throws Exception {}

    @Override
    @Ignore
    @Test
    public void testKryoRestoreResilienceWithDifferentRegistrationOrder() throws Exception {}

    @Override
    @Ignore
    @Test
    public void testMapState() throws Exception {}

    @Override
    @Ignore
    @Test
    public void testMapStateIsEmpty() throws Exception {}

    @Override
    @Ignore
    @Test
    public void testMapStateIteratorArbitraryAccess() throws Exception {}

    @Override
    @Ignore
    @Test
    public void testMapStateDefaultValue() throws Exception {}

    @Override
    @Ignore
    @Test
    public void testMapStateGetKeys() throws Exception {}

    @Override
    @Ignore
    @Test
    public void testMapStateGetKeysAndNamespaces() throws Exception {}

    @Override
    @Ignore
    @Test
    public void testListState() throws Exception {}

    @Override
    @Ignore
    @Test
    public void testListStateMerging() throws Exception {}

    @Override
    @Ignore
    @Test
    public void testBackendUsesRegisteredKryoDefaultSerializer() throws Exception {}

    @Override
    @Ignore
    @Test
    public void testBackendUsesRegisteredKryoDefaultSerializerUsingGetOrCreate() throws Exception {}

    @Ignore
    @Override
    @Test
    public void testBackendUsesRegisteredKryoSerializer() throws Exception {}

    @Ignore
    @Override
    @Test
    public void testBackendUsesRegisteredKryoSerializerUsingGetOrCreate() throws Exception {}

    @Ignore
    @Override
    @Test
    public void testValueState() throws Exception {}

    @Ignore
    @Override
    @Test
    public void testValueStateRace() throws Exception {}

    @Ignore
    @Override
    @Test
    public void testNonConcurrentSnapshotTransformerAccess() throws Exception {}

    @Ignore
    @Override
    @Test
    public void testKeyGroupedInternalPriorityQueue() throws Exception {}

    @Ignore
    @Override
    @Test
    public void testKeyGroupedInternalPriorityQueueAddAll() throws Exception {}

    @Ignore
    @Override
    @Test
    public void testAsyncSnapshot() throws Exception {}

    @Ignore
    @Override
    @Test
    public void testQueryableStateRegistration() throws Exception {}

    @Ignore
    @Override
    @Test
    public void testEnableStateLatencyTracking() throws Exception {}

    @Ignore
    @Override
    @Test
    public void testCheckConcurrencyProblemWhenPerformingCheckpointAsync() throws Exception {}

    // ignored by hashmap state backend test.
    @Ignore
    @Override
    @Test
    public void testValueStateRestoreWithWrongSerializers() {}

    @Ignore
    @Override
    @Test
    public void testListStateRestoreWithWrongSerializers() {}

    @Ignore
    @Override
    @Test
    public void testReducingStateRestoreWithWrongSerializers() {}

    @Ignore
    @Override
    @Test
    public void testMapStateRestoreWithWrongSerializers() {}

    @Ignore
    @Test
    public void testConcurrentMapIfQueryable() throws Exception {
        super.testConcurrentMapIfQueryable();
    }
}
