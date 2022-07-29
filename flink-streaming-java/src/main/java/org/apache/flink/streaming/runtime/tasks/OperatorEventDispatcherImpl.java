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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.AcknowledgeCheckpointEvent;
import org.apache.flink.runtime.operators.coordination.AcknowledgeCloseGatewayEvent;
import org.apache.flink.runtime.operators.coordination.CloseGatewayEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventDispatcher;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.SerializedValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An implementation of the {@link OperatorEventDispatcher}.
 *
 * <p>This class is intended for single threaded use from the stream task mailbox.
 */
@Internal
public final class OperatorEventDispatcherImpl implements OperatorEventDispatcher {

    private final Map<OperatorID, OperatorEventHandler> handlers;

    private final ClassLoader classLoader;

    private final TaskOperatorEventGateway toCoordinator;

    private final Map<OperatorID, OperatorEventGatewayImpl> gatewayMap;

    public OperatorEventDispatcherImpl(
            ClassLoader classLoader, TaskOperatorEventGateway toCoordinator) {
        this.classLoader = checkNotNull(classLoader);
        this.toCoordinator = checkNotNull(toCoordinator);
        this.handlers = new HashMap<>();
        this.gatewayMap = new HashMap<>();
    }

    void dispatchEventToHandlers(
            OperatorID operatorID, SerializedValue<OperatorEvent> serializedEvent)
            throws FlinkException {
        final OperatorEvent evt;
        try {
            evt = serializedEvent.deserializeValue(classLoader);
        } catch (IOException | ClassNotFoundException e) {
            throw new FlinkException("Could not deserialize operator event", e);
        }

        if (evt instanceof CloseGatewayEvent) {
            OperatorEventGatewayImpl gateway = getOperatorEventGateway(operatorID);
            gateway.closeGateway();
            gateway.sendEventToCoordinator(
                    new AcknowledgeCloseGatewayEvent((CloseGatewayEvent) evt), false);
            return;
        }

        final OperatorEventHandler handler = handlers.get(operatorID);
        if (handler != null) {
            handler.handleOperatorEvent(evt);
        } else {
            throw new FlinkException("Operator not registered for operator events");
        }
    }

    void initializeOperatorEventGatewayIfExists(StreamOperator<?> operator) throws Exception {
        if (gatewayMap.containsKey(operator.getOperatorID())) {
            getOperatorEventGateway(operator.getOperatorID())
                    .initializeState(getOperatorStateBackend(operator));
        }
    }

    void snapshotOperatorEventGatewayIfExists(StreamOperator<?> operator) throws Exception {
        if (gatewayMap.containsKey(operator.getOperatorID())) {
            getOperatorEventGateway(operator.getOperatorID())
                    .snapshotState(getOperatorStateBackend(operator));
        }
    }

    void notifyOperatorSnapshotCompletedIfExists(StreamOperator<?> operator, long checkpointId) {
        if (gatewayMap.containsKey(operator.getOperatorID())) {
            OperatorEventGatewayImpl gateway = getOperatorEventGateway(operator.getOperatorID());
            gateway.sendEventToCoordinator(
                    new AcknowledgeCheckpointEvent(checkpointId, getSubtaskIndex(operator)), false);
            gateway.openGateway();
        }
    }

    @Override
    public void registerEventHandler(OperatorID operator, OperatorEventHandler handler) {
        final OperatorEventHandler prior = handlers.putIfAbsent(operator, handler);
        if (prior != null) {
            throw new IllegalStateException("already a handler registered for this operatorId");
        }
    }

    @Override
    public OperatorEventGatewayImpl getOperatorEventGateway(OperatorID operatorId) {
        return gatewayMap.computeIfAbsent(
                operatorId, key -> new OperatorEventGatewayImpl(toCoordinator, key));
    }

    private OperatorStateBackend getOperatorStateBackend(StreamOperator<?> operator) {
        if (operator instanceof AbstractStreamOperator) {
            return ((AbstractStreamOperator<?>) operator).getOperatorStateBackend();
        } else if (operator instanceof AbstractStreamOperatorV2) {
            return ((AbstractStreamOperatorV2<?>) operator).getOperatorStateBackend();
        } else {
            throw new IllegalStateException(
                    "Operator "
                            + operator
                            + " should extend AbstractStreamOperator or AbstractStreamOperatorV2"
                            + " to provide OperatorStateBackend for OperatorEventGateway.");
        }
    }

    private int getSubtaskIndex(StreamOperator<?> operator) {
        if (operator instanceof AbstractStreamOperator) {
            return ((AbstractStreamOperator<?>) operator)
                    .getRuntimeContext()
                    .getIndexOfThisSubtask();
        } else if (operator instanceof AbstractStreamOperatorV2) {
            return ((AbstractStreamOperatorV2<?>) operator)
                    .getRuntimeContext()
                    .getIndexOfThisSubtask();
        } else {
            throw new IllegalStateException(
                    "Operator "
                            + operator
                            + " should extend AbstractStreamOperator or AbstractStreamOperatorV2"
                            + " to provide a RuntimeContext.");
        }
    }

    // ------------------------------------------------------------------------

    private static final class OperatorEventGatewayImpl implements OperatorEventGateway {

        private static final String BLOCKED_EVENTS_STATE_KEY = "blockedOperatorEvents";

        private final TaskOperatorEventGateway toCoordinator;

        private final OperatorID operatorId;

        private final ListStateDescriptor<SerializedOperatorEvent> descriptor;

        private final List<SerializedOperatorEvent> blockedEvents;

        private boolean isClosed;

        private OperatorEventGatewayImpl(
                TaskOperatorEventGateway toCoordinator, OperatorID operatorId) {
            this.toCoordinator = toCoordinator;
            this.descriptor =
                    new ListStateDescriptor<>(
                            BLOCKED_EVENTS_STATE_KEY, SerializedOperatorEvent.class);
            this.operatorId = operatorId;
            this.blockedEvents = new ArrayList<>();
            this.isClosed = false;
        }

        @Override
        public void sendEventToCoordinator(OperatorEvent event) {
            sendEventToCoordinator(event, true);
        }

        private void sendEventToCoordinator(OperatorEvent event, boolean canBeBlocked) {
            final SerializedOperatorEvent serializedEvent;
            try {
                serializedEvent = new SerializedOperatorEvent(event);
            } catch (IOException e) {
                // this is not a recoverable situation, so we wrap this in an
                // unchecked exception and let it bubble up
                throw new FlinkRuntimeException("Cannot serialize operator event", e);
            }

            if (isClosed && canBeBlocked) {
                blockedEvents.add(serializedEvent);
            } else {
                toCoordinator.sendOperatorEventToCoordinator(operatorId, serializedEvent);
            }
        }

        private void closeGateway() {
            isClosed = true;
        }

        private void openGateway() {
            isClosed = false;
            sendBlockedEvents();
        }

        private void initializeState(OperatorStateStore operatorStateStore) throws Exception {
            operatorStateStore.getListState(descriptor).get().forEach(blockedEvents::add);
            sendBlockedEvents();
        }

        private void snapshotState(OperatorStateStore operatorStateStore) throws Exception {
            operatorStateStore.getListState(descriptor).update(blockedEvents);
        }

        private void sendBlockedEvents() {
            for (SerializedValue<OperatorEvent> blockedEvent : blockedEvents) {
                toCoordinator.sendOperatorEventToCoordinator(operatorId, blockedEvent);
            }
            blockedEvents.clear();
        }

        /**
         * A wrapper class of serialized operator event to resolve Java's limit on the usage of
         * classes with generics.
         */
        private static class SerializedOperatorEvent extends SerializedValue<OperatorEvent> {
            public SerializedOperatorEvent(OperatorEvent value) throws IOException {
                super(value);
            }
        }
    }
}
