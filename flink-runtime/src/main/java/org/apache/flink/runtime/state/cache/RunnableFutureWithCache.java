package org.apache.flink.runtime.state.cache;

import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.util.function.RunnableWithException;

import javax.annotation.Nonnull;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class RunnableFutureWithCache implements RunnableFuture<SnapshotResult<KeyedStateHandle>> {
    private final List<RunnableWithException> runnables;

    private final CheckpointableKeyedStateBackend<?> backend;
    private final CheckpointableKeyedStateBackend<?> backendForCache;
    private final long checkpointId;
    private final long timestamp;
    private final @Nonnull CheckpointStreamFactory streamFactory;
    private final @Nonnull CheckpointOptions checkpointOptions;

    private final Runnable completionCallback;
    private RunnableFuture<SnapshotResult<KeyedStateHandle>> future;
    private RunnableFuture<SnapshotResult<KeyedStateHandle>> futureForCache;

    private boolean isCompletionCallbackInvoked;

    private boolean isRunInvoked;

    RunnableFutureWithCache(
            List<RunnableWithException> runnables,
            CheckpointableKeyedStateBackend<?> backend,
            CheckpointableKeyedStateBackend<?> backendForCache,
            long checkpointId,
            long timestamp,
            @Nonnull CheckpointStreamFactory streamFactory,
            @Nonnull CheckpointOptions checkpointOptions,
            Runnable completionCallback) {
        this.runnables = runnables;
        this.backend = backend;
        this.backendForCache = backendForCache;
        this.checkpointId = checkpointId;
        this.timestamp = timestamp;
        this.streamFactory = streamFactory;
        this.checkpointOptions = checkpointOptions;

//        this.future = future;
//        this.futureForCache = futureForCache;
        this.completionCallback = completionCallback;
        this.isCompletionCallbackInvoked = false;
        this.isRunInvoked = false;
    }

    @Override
    public void run() {
        isRunInvoked = true;
        for (RunnableWithException runnable: runnables) {
            try {
                runnable.run();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        try {
            future = backend.snapshot(checkpointId, timestamp, streamFactory, checkpointOptions);
            futureForCache =
                    backendForCache.snapshot(checkpointId, timestamp, streamFactory, checkpointOptions);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        future.run();
        futureForCache.run();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        if (!isRunInvoked) {
            run();
        }
        boolean success = true;
        success &= future.cancel(mayInterruptIfRunning);
        success &= futureForCache.cancel(mayInterruptIfRunning);
        return success;
    }

    @Override
    public boolean isCancelled() {
        if (!isRunInvoked) {
            run();
        }
        boolean result = true;
        result &= future.isCancelled();
        result &= futureForCache.isCancelled();
        return result;
    }

    @Override
    public boolean isDone() {
        if (!isRunInvoked) {
            run();
        }
        boolean result = true;
        result &= future.isDone();
        result &= futureForCache.isDone();
        return result;
    }

    @Override
    public SnapshotResult<KeyedStateHandle> get() throws InterruptedException, ExecutionException {
        if (!isRunInvoked) {
            run();
        }
        SnapshotResult<KeyedStateHandle> result = mergeResults(future.get(), futureForCache.get());
        if (!isCompletionCallbackInvoked) {
            isCompletionCallbackInvoked = true;
            completionCallback.run();
        }
        return result;
    }

    @Override
    public SnapshotResult<KeyedStateHandle> get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        if (!isRunInvoked) {
            run();
        }
        SnapshotResult<KeyedStateHandle> result = mergeResults(future.get(timeout, unit), futureForCache.get(timeout, unit));
        if (!isCompletionCallbackInvoked) {
            isCompletionCallbackInvoked = true;
            completionCallback.run();
        }
        return result;
    }

    private SnapshotResult<KeyedStateHandle> mergeResults(
            SnapshotResult<KeyedStateHandle> result,
            SnapshotResult<KeyedStateHandle> resultForCache) {
        KeyedStateHandle jobManagerState =
                KeyedStateHandleWithCache.create(
                        result.getJobManagerOwnedSnapshot(),
                        resultForCache.getJobManagerOwnedSnapshot());
        KeyedStateHandle localState =
                KeyedStateHandleWithCache.create(
                        result.getTaskLocalSnapshot(), resultForCache.getTaskLocalSnapshot());

        if (localState != null) {
            if (jobManagerState == null) {
                throw new UnsupportedOperationException();
            }
            return SnapshotResult.withLocalState(jobManagerState, localState);
        }
        return SnapshotResult.of(jobManagerState);
    }
}
