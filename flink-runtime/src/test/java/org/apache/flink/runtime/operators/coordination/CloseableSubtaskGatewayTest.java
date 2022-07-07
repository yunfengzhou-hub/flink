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

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.operators.coordination.EventReceivingTasks.EventWithSubtask;
import org.apache.flink.runtime.operators.coordination.util.IncompleteFuturesTracker;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.concurrent.Executors;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Unit tests for the {@link CloseableSubtaskGateway}. */
public class CloseableSubtaskGatewayTest {

    @Test
    public void eventsPassThroughOpenGateway() {
        final EventReceivingTasks receiver = EventReceivingTasks.createForRunningTasks();
        final CloseableSubtaskGateway gateway =
                new CloseableSubtaskGateway(
                        new SubtaskGatewayImpl(
                                receiver.getAccessForSubtask(11),
                                Executors.directExecutor(),
                                new IncompleteFuturesTracker()));

        final OperatorEvent event = new TestOperatorEvent();
        final CompletableFuture<Acknowledge> future = gateway.sendEvent(event);

        assertEquals(receiver.events, Collections.singletonList(new EventWithSubtask(event, 11)));
        assertTrue(future.isDone());
    }

    @Test
    public void shuttingMarkedGateway() {
        final CloseableSubtaskGateway gateway =
                new CloseableSubtaskGateway(new RejectingSubtaskGateway());

        gateway.markForCheckpoint(200L);
        final boolean shut = gateway.tryCloseGateway(200L);

        assertTrue(shut);
    }

    @Test
    public void notShuttingUnmarkedGateway() {
        final CloseableSubtaskGateway gateway =
                new CloseableSubtaskGateway(new RejectingSubtaskGateway());

        final boolean shut = gateway.tryCloseGateway(123L);

        assertFalse(shut);
    }

    @Test
    public void notShuttingGatewayForOtherMark() {
        final CloseableSubtaskGateway gateway =
                new CloseableSubtaskGateway(new RejectingSubtaskGateway());

        gateway.markForCheckpoint(100L);
        final boolean shut = gateway.tryCloseGateway(123L);

        assertFalse(shut);
    }

    @Test
    public void eventsBlockedByClosedGateway() {
        final EventReceivingTasks receiver = EventReceivingTasks.createForRunningTasks();
        final CloseableSubtaskGateway gateway =
                new CloseableSubtaskGateway(
                        new SubtaskGatewayImpl(
                                receiver.getAccessForSubtask(1),
                                Executors.directExecutor(),
                                new IncompleteFuturesTracker()));

        gateway.markForCheckpoint(1L);
        gateway.tryCloseGateway(1L);

        final CompletableFuture<Acknowledge> future = gateway.sendEvent(new TestOperatorEvent());

        assertTrue(receiver.events.isEmpty());
        assertFalse(future.isDone());
    }

    @Test
    public void eventsReleasedAfterOpeningGateway() {
        final EventReceivingTasks receiver = EventReceivingTasks.createForRunningTasks();
        final CloseableSubtaskGateway gateway0 =
                new CloseableSubtaskGateway(
                        new SubtaskGatewayImpl(
                                receiver.getAccessForSubtask(0),
                                Executors.directExecutor(),
                                new IncompleteFuturesTracker()));
        final CloseableSubtaskGateway gateway3 =
                new CloseableSubtaskGateway(
                        new SubtaskGatewayImpl(
                                receiver.getAccessForSubtask(3),
                                Executors.directExecutor(),
                                new IncompleteFuturesTracker()));
        List<CloseableSubtaskGateway> gateways = Arrays.asList(gateway3, gateway0);

        gateways.forEach(x -> x.markForCheckpoint(17L));
        gateways.forEach(x -> x.tryCloseGateway(17L));

        final OperatorEvent event1 = new TestOperatorEvent();
        final OperatorEvent event2 = new TestOperatorEvent();
        final CompletableFuture<Acknowledge> future1 = gateway3.sendEvent(event1);
        final CompletableFuture<Acknowledge> future2 = gateway0.sendEvent(event2);

        gateways.forEach(CloseableSubtaskGateway::openGatewayAndUnmarkCheckpoint);

        assertTrue(
                receiver.events.containsAll(
                        Arrays.asList(
                                new EventWithSubtask(event1, 3), new EventWithSubtask(event2, 0))));
        assertTrue(future1.isDone());
        assertTrue(future2.isDone());
    }

    @Test
    public void releasedEventsForwardSendFailures() {
        final EventReceivingTasks receiver =
                EventReceivingTasks.createForRunningTasksFailingRpcs(new FlinkException("test"));
        final CloseableSubtaskGateway gateway =
                new CloseableSubtaskGateway(
                        new SubtaskGatewayImpl(
                                receiver.getAccessForSubtask(10),
                                Executors.directExecutor(),
                                new IncompleteFuturesTracker()));

        gateway.markForCheckpoint(17L);
        gateway.tryCloseGateway(17L);

        final CompletableFuture<Acknowledge> future = gateway.sendEvent(new TestOperatorEvent());
        gateway.openGatewayAndUnmarkCheckpoint();

        assertTrue(future.isCompletedExceptionally());
    }

    private static final class RejectingSubtaskGateway
            implements OperatorCoordinator.SubtaskGateway {
        @Override
        public CompletableFuture<Acknowledge> sendEvent(OperatorEvent evt) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ExecutionAttemptID getExecution() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getSubtask() {
            throw new UnsupportedOperationException();
        }
    }
}
