package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.util.function.ThrowingRunnable;

import java.util.concurrent.CompletableFuture;

/**
 * Base class for {@link OperatorCoordinator}s. It mainly provides the following functions.
 *
 * <ul>
 *   <li>supports running given logics in asynchronous way and manages all asynchronously running
 *       threads.
 *   <li>manages subtask status and their {@link
 *       org.apache.flink.runtime.operators.coordination.OperatorCoordinator.SubtaskGateway}.
 *   <li>controls the communication between operator coordinator and its subtasks during checkpoint.
 * </ul>
 */
public abstract class AbstractOperatorCoordinator implements OperatorCoordinator {
    /** Executes the provided job in asynchronous way. */
    protected void runAsyncAction(
            final ThrowingRunnable<Throwable> action, final String actionName) {}

    /**
     * Sends an operator event to the given subtask.
     *
     * <p>NOTE: communication with subtask would be temporarily blocked during checkpoint, so
     * subtask would only receive the operator event after checkpoint has finished.
     */
    protected void sentEvent(int subtask, OperatorEvent evt) {}

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture)
            throws Exception {}

    @Override
    public void subtaskReady(int subtask, SubtaskGateway gateway) {}

    @Override
    public final void handleEventFromOperator(int subtask, OperatorEvent event) throws Exception {
        if (event instanceof CloseGatewayEvent) {
            System.out.println();
        } else {
            handleEvent(subtask, event);
        }
    }

    /**
     * Hands an operator-specific OperatorEvent coming from a parallel Operator instances (one of
     * the parallel subtasks).
     *
     * @throws Exception Any exception thrown by this method results in a full job failure and
     *     recovery.
     */
    public abstract void handleEvent(int subtask, OperatorEvent event) throws Exception;
}
