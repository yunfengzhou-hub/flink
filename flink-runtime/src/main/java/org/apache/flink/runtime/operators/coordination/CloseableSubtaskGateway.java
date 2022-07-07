/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.util.concurrent.FutureUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * An {@link org.apache.flink.runtime.operators.coordination.OperatorCoordinator.SubtaskGateway}
 * that can be temporarily closed, blocking events from going through, buffering them, and releasing
 * them later. It is used for "alignment" of operator event streams with checkpoint barrier
 * injection, similar to how the input channels are aligned during a common checkpoint.
 *
 * <p>This class is NOT thread safe, but assumed to be used in a single threaded context. To guard
 * that, one can register a "main thread executor" (as used by the mailbox components like RPC
 * components) via {@link #setMainThreadExecutorForValidation(ComponentMainThreadExecutor)}.
 */
class CloseableSubtaskGateway implements OperatorCoordinator.SubtaskGateway {

    /** The wrapped gateway that actually performs the event-sending operation. */
    private final OperatorCoordinator.SubtaskGateway innerGateway;

    private static final long NO_CHECKPOINT = Long.MIN_VALUE;

    private final List<BlockedEvent> blockedEvents = new ArrayList<>();

    private long currentCheckpointId;

    private long lastCheckpointId;

    private boolean isClosed;

    @Nullable private ComponentMainThreadExecutor mainThreadExecutor;

    CloseableSubtaskGateway(OperatorCoordinator.SubtaskGateway innerGateway) {
        this.innerGateway = innerGateway;
        this.currentCheckpointId = NO_CHECKPOINT;
        this.lastCheckpointId = Long.MIN_VALUE;
    }

    void setMainThreadExecutorForValidation(ComponentMainThreadExecutor mainThreadExecutor) {
        this.mainThreadExecutor = mainThreadExecutor;
    }

    /**
     * Marks the gateway for the next checkpoint. This remembers the checkpoint ID and will only
     * allow closing the gateway for this specific checkpoint.
     *
     * <p>This is the gateway's mechanism to detect situations where multiple coordinator
     * checkpoints would be attempted overlapping, which is currently not supported (the gateway
     * doesn't keep a list of events blocked per checkpoint). It also helps to identify situations
     * where the checkpoint was aborted even before the gateway was closed (by finding out that the
     * {@code currentCheckpointId} was already reset to {@code NO_CHECKPOINT}.
     */
    void markForCheckpoint(long checkpointId) {
        checkRunsInMainThread();

        if (currentCheckpointId != NO_CHECKPOINT && currentCheckpointId != checkpointId) {
            throw new IllegalStateException(
                    String.format(
                            "Cannot mark for checkpoint %d, already marked for checkpoint %d",
                            checkpointId, currentCheckpointId));
        }
        if (checkpointId > lastCheckpointId) {
            currentCheckpointId = checkpointId;
            lastCheckpointId = checkpointId;
        } else {
            throw new IllegalStateException(
                    String.format(
                            "Regressing checkpoint IDs. Previous checkpointId = %d, new checkpointId = %d",
                            lastCheckpointId, checkpointId));
        }
    }

    /**
     * Closes the gateway. All events sent through this gateway are blocked until the gateway is
     * re-opened. If the gateway is already closed, this does nothing.
     *
     * @return True if the gateway is closed, false if the checkpointId is incorrect.
     */
    boolean tryCloseGateway(long checkpointId) {
        if (checkpointId == currentCheckpointId) {
            isClosed = true;
            return true;
        }
        return false;
    }

    boolean isClosed() {
        return isClosed;
    }

    void openGatewayAndUnmarkCheckpoint(long expectedCheckpointId) {
        checkRunsInMainThread();

        if (expectedCheckpointId != currentCheckpointId) {
            return;
        }
        openGatewayAndUnmarkCheckpoint();
    }

    /** Opens the gateway, releasing all buffered events. */
    void openGatewayAndUnmarkCheckpoint() {
        checkRunsInMainThread();

        currentCheckpointId = NO_CHECKPOINT;
        if (!isClosed) {
            return;
        }

        for (BlockedEvent blockedEvent : blockedEvents) {
            CompletableFuture<Acknowledge> result =
                    innerGateway.sendEvent(blockedEvent.operatorEvent);
            FutureUtils.forward(result, blockedEvent.future);
        }
        blockedEvents.clear();

        isClosed = false;
    }

    @Override
    public CompletableFuture<Acknowledge> sendEvent(OperatorEvent evt) {
        if (isClosed) {
            CompletableFuture<Acknowledge> sendResult = new CompletableFuture<>();
            blockedEvents.add(new BlockedEvent(evt, sendResult));
            return sendResult;
        } else {
            return innerGateway.sendEvent(evt);
        }
    }

    @Override
    public ExecutionAttemptID getExecution() {
        return innerGateway.getExecution();
    }

    @Override
    public int getSubtask() {
        return innerGateway.getSubtask();
    }

    private void checkRunsInMainThread() {
        if (mainThreadExecutor != null) {
            mainThreadExecutor.assertRunningInMainThread();
        }
    }

    private static final class BlockedEvent {

        private final OperatorEvent operatorEvent;
        private final CompletableFuture<Acknowledge> future;

        private BlockedEvent(OperatorEvent operatorEvent, CompletableFuture<Acknowledge> future) {
            this.operatorEvent = operatorEvent;
            this.future = future;
        }
    }
}
