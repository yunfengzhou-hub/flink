package org.apache.flink.runtime.state.cache;

import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class RunnableFutureWithCache implements RunnableFuture<SnapshotResult<KeyedStateHandle>> {
    private final RunnableFuture<SnapshotResult<KeyedStateHandle>> future;
    private final RunnableFuture<SnapshotResult<KeyedStateHandle>> futureForCache;

    private final Runnable completionCallback;

    private boolean isCompletionCallbackInvoked;

    RunnableFutureWithCache(
            RunnableFuture<SnapshotResult<KeyedStateHandle>> future,
            RunnableFuture<SnapshotResult<KeyedStateHandle>> futureForCache,
            Runnable completionCallback) {
        this.future = future;
        this.futureForCache = futureForCache;
        this.completionCallback = completionCallback;
        this.isCompletionCallbackInvoked = false;
    }

    @Override
    public void run() {
        future.run();
        futureForCache.run();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        boolean success = true;
        success &= future.cancel(mayInterruptIfRunning);
        success &= futureForCache.cancel(mayInterruptIfRunning);
        return success;
    }

    @Override
    public boolean isCancelled() {
        boolean result = true;
        result &= future.isCancelled();
        result &= futureForCache.isCancelled();
        return result;
    }

    @Override
    public boolean isDone() {
        boolean result = true;
        result &= future.isDone();
        result &= futureForCache.isDone();
        return result;
    }

    @Override
    public SnapshotResult<KeyedStateHandle> get() throws InterruptedException, ExecutionException {
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
