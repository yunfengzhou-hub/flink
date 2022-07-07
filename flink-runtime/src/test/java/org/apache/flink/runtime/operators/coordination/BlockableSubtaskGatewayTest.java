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
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** Unit tests for the {@link BlockableSubtaskGateway}. */
public class BlockableSubtaskGatewayTest {

    @Test
    public void eventsPassThroughOpenValve() {
        final EventReceivingTasks sender = EventReceivingTasks.createForRunningTasks();
        final BlockableSubtaskGateway gateway =
                new BlockableSubtaskGateway(
                        new SubtaskGatewayImpl(
                                sender.getAccessForSubtask(11),
                                Executors.directExecutor(),
                                new IncompleteFuturesTracker()));

        final OperatorEvent event = new TestOperatorEvent();
        final CompletableFuture<Acknowledge> future = gateway.sendEvent(event);

        assertThat(sender.events, contains(new EventWithSubtask(event, 11)));
        assertTrue(future.isDone());
    }

    @Test
    public void shuttingMarkedValve() {
        final BlockableSubtaskGateway gateway =
                new BlockableSubtaskGateway(new RejectingSubtaskGateway());

        gateway.markForCheckpoint(200L);
        final boolean shut = gateway.tryShutValve(200L);

        assertTrue(shut);
    }

    @Test
    public void notShuttingUnmarkedValve() {
        final BlockableSubtaskGateway gateway =
                new BlockableSubtaskGateway(new RejectingSubtaskGateway());

        final boolean shut = gateway.tryShutValve(123L);

        assertFalse(shut);
    }

    @Test
    public void notShuttingValveForOtherMark() {
        final BlockableSubtaskGateway gateway =
                new BlockableSubtaskGateway(new RejectingSubtaskGateway());

        gateway.markForCheckpoint(100L);
        final boolean shut = gateway.tryShutValve(123L);

        assertFalse(shut);
    }

    @Test
    public void eventsBlockedByClosedValve() {
        final EventReceivingTasks sender = EventReceivingTasks.createForRunningTasks();
        final BlockableSubtaskGateway gateway =
                new BlockableSubtaskGateway(
                        new SubtaskGatewayImpl(
                                sender.getAccessForSubtask(1),
                                Executors.directExecutor(),
                                new IncompleteFuturesTracker()));

        gateway.markForCheckpoint(1L);
        gateway.tryShutValve(1L);

        final CompletableFuture<Acknowledge> future = gateway.sendEvent(new TestOperatorEvent());

        assertTrue(sender.events.isEmpty());
        assertFalse(future.isDone());
    }

    @Test
    public void eventsReleasedAfterOpeningValve() {
        final EventReceivingTasks sender = EventReceivingTasks.createForRunningTasks();
        final BlockableSubtaskGateway gateway0 =
                new BlockableSubtaskGateway(
                        new SubtaskGatewayImpl(
                                sender.getAccessForSubtask(0),
                                Executors.directExecutor(),
                                new IncompleteFuturesTracker()));
        final BlockableSubtaskGateway gateway3 =
                new BlockableSubtaskGateway(
                        new SubtaskGatewayImpl(
                                sender.getAccessForSubtask(3),
                                Executors.directExecutor(),
                                new IncompleteFuturesTracker()));
        List<BlockableSubtaskGateway> gateways = Arrays.asList(gateway3, gateway0);

        gateways.forEach(x -> x.markForCheckpoint(17L));
        gateways.forEach(x -> x.tryShutValve(17L));

        final OperatorEvent event1 = new TestOperatorEvent();
        final OperatorEvent event2 = new TestOperatorEvent();
        final CompletableFuture<Acknowledge> future1 = gateway3.sendEvent(event1);
        final CompletableFuture<Acknowledge> future2 = gateway0.sendEvent(event2);

        gateways.forEach(BlockableSubtaskGateway::openValveAndUnmarkCheckpoint);

        assertThat(
                sender.events,
                contains(new EventWithSubtask(event1, 3), new EventWithSubtask(event2, 0)));
        assertTrue(future1.isDone());
        assertTrue(future2.isDone());
    }

    @Test
    public void releasedEventsForwardSendFailures() {
        final EventReceivingTasks sender =
                EventReceivingTasks.createForRunningTasksFailingRpcs(new FlinkException("test"));
        final BlockableSubtaskGateway gateway =
                new BlockableSubtaskGateway(
                        new SubtaskGatewayImpl(
                                sender.getAccessForSubtask(10),
                                Executors.directExecutor(),
                                new IncompleteFuturesTracker()));

        gateway.markForCheckpoint(17L);
        gateway.tryShutValve(17L);

        final CompletableFuture<Acknowledge> future = gateway.sendEvent(new TestOperatorEvent());
        gateway.openValveAndUnmarkCheckpoint();

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
