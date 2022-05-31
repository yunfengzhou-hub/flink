package org.apache.flink.runtime.operators.coordination;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

public class DummyCoordinator extends AbstractOperatorCoordinator {
    @Override
    public void start() throws Exception {}

    @Override
    public void close() throws Exception {}

    @Override
    public void handleEvent(int subtask, OperatorEvent event) throws Exception {
        runAsyncAction(
                () -> {
                    for (int i = 0; i < 100; i++) {
                        System.out.println("Dummy blocking operation.");
                        Thread.sleep(100);
                    }
                },
                "asyncAction");
        sentEvent(subtask, new CloseGatewayEvent());
    }

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture)
            throws Exception {
        super.checkpointCoordinator(checkpointId, resultFuture);
        System.out.println("Custom checkpoint logic.");
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    @Override
    public void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData)
            throws Exception {}

    @Override
    public void subtaskFailed(int subtask, @Nullable Throwable reason) {}

    @Override
    public void subtaskReset(int subtask, long checkpointId) {}

    @Override
    public void subtaskReady(int subtask, SubtaskGateway gateway) {
        super.subtaskReady(subtask, gateway);
        System.out.println("Custom logic here.");
    }
}
