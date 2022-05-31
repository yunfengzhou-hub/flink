package org.apache.flink.runtime.operators.coordination;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

/**
 * A class that controls the communication between operator coordinator and its subtasks during
 * checkpoint to preserve exactly-once semantics of event handling.
 */
public class EventControllerOperatorCoordinator implements OperatorCoordinator {
    private final OperatorCoordinator internalCoordinator;

    public EventControllerOperatorCoordinator(OperatorCoordinator internalCoordinator) {
        this.internalCoordinator = internalCoordinator;
    }

    @Override
    public void start() throws Exception {
        internalCoordinator.start();
    }

    @Override
    public void close() throws Exception {
        internalCoordinator.close();
    }

    @Override
    public void handleEventFromOperator(int subtask, OperatorEvent event) throws Exception {
        internalCoordinator.handleEventFromOperator(subtask, event);
    }

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture)
            throws Exception {
        internalCoordinator.checkpointCoordinator(checkpointId, resultFuture);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        internalCoordinator.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        internalCoordinator.notifyCheckpointAborted(checkpointId);
    }

    @Override
    public void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData)
            throws Exception {
        internalCoordinator.resetToCheckpoint(checkpointId, checkpointData);
    }

    @Override
    public void subtaskFailed(int subtask, @Nullable Throwable reason) {
        internalCoordinator.subtaskFailed(subtask, reason);
    }

    @Override
    public void subtaskReset(int subtask, long checkpointId) {
        internalCoordinator.subtaskReset(subtask, checkpointId);
    }

    @Override
    public void subtaskReady(int subtask, SubtaskGateway gateway) {
        internalCoordinator.subtaskReady(subtask, gateway);
    }
}
