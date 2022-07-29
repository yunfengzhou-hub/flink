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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.CoordinatorEventsExactlyOnceITCase;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.CoordinatedOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

/**
 * Integration test case that validates the exactly-once mechanism for operator events sent from an
 * operator to its operator coordinator around checkpoint.
 *
 * <p>It extends {@link CoordinatorEventsExactlyOnceITCase} to reuse its helper classes and methods.
 *
 * <p>See also {@link CoordinatorEventsToStreamOperatorRecipientExactlyOnceITCase} for integration
 * tests about operator event sent in the reversed direction.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class StreamOperatorEventsExactlyOnceITCase extends CoordinatorEventsExactlyOnceITCase {
    //    @ClassRule
    //    public static final MiniClusterResource MINI_CLUSTER =
    //            new MiniClusterResource(
    //                    new MiniClusterResourceConfiguration.Builder()
    //                            .setNumberTaskManagers(2)
    //                            .setNumberSlotsPerTaskManager(2)
    //                            .build());

    private static final List<Integer> receivedEvents = new ArrayList<>();

    private static final int numEvents = 100;

    private static final int delay = 1;

    private StreamExecutionEnvironment env;

    @Before
    public void setup() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(100);
    }

    @After
    public void teardown() {
        receivedEvents.clear();
    }

    @Test
    public void testCheckpoint() throws Exception {
        executeAndVerifyResult();
    }

    @Test
    public void testUnalignedCheckpoint() throws Exception {
        env.getCheckpointConfig().enableUnalignedCheckpoints();
        executeAndVerifyResult();
    }

    private void executeAndVerifyResult() throws Exception {
        env.fromSequence(0, numEvents - 1)
                .map(
                        x -> {
                            Thread.sleep(delay);
                            return x.intValue();
                        })
                //                .transform(
                //                        "blockCheckpointBarrier",
                //                        TypeInformation.of(Integer.class),
                //                        new BlockCheckpointBarrierOperator<>())
                .disableChaining()
                .transform(
                        "eventSending",
                        TypeInformation.of(Integer.class),
                        new EventSendingOperatorFactory())
                .addSink(new DiscardingSink<>());

        MINI_CLUSTER.getMiniCluster().executeJobBlocking(env.getStreamGraph().getJobGraph());

        checkListContainsSequence(receivedEvents, numEvents);
    }

    /**
     * A wrapper operator factory for {@link EventReceivingCoordinator} and {@link
     * EventSendingOperator}.
     */
    private static class EventSendingOperatorFactory extends AbstractStreamOperatorFactory<Integer>
            implements CoordinatedOperatorFactory<Integer>,
                    OneInputStreamOperatorFactory<Integer, Integer> {
        @Override
        public OperatorCoordinator.Provider getCoordinatorProvider(
                String operatorName, OperatorID operatorID) {
            return new OperatorCoordinator.Provider() {

                @Override
                public OperatorID getOperatorId() {
                    return operatorID;
                }

                @Override
                public OperatorCoordinator create(OperatorCoordinator.Context context) {
                    return new EventReceivingCoordinator(context, operatorID, numEvents);
                }
            };
        }

        @Override
        public <T extends StreamOperator<Integer>> T createStreamOperator(
                StreamOperatorParameters<Integer> parameters) {
            final OperatorID operatorId = parameters.getStreamConfig().getOperatorID();
            OperatorEventGateway gateway =
                    parameters.getOperatorEventDispatcher().getOperatorEventGateway(operatorId);
            EventSendingOperator operator = new EventSendingOperator(gateway);
            operator.setup(
                    parameters.getContainingTask(),
                    parameters.getStreamConfig(),
                    parameters.getOutput());
            return (T) operator;
        }

        @Override
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return EventSendingOperator.class;
        }
    }

    /**
     * The coordinator that receives integer events, updates a global event list, and checkpoints
     * them in snapshots.
     */
    private static class EventReceivingCoordinator implements OperatorCoordinator {

        private final Context context;

        private final TestScript testScript;

        private final int failAtMessage;

        private EventReceivingCoordinator(Context context, OperatorID operatorID, int numEvents) {
            this.context = context;
            this.testScript = TestScript.getForOperator(operatorID.toString());
            this.failAtMessage = numEvents / 3 + new Random().nextInt(numEvents / 3);
        }

        @Override
        public void start() throws Exception {}

        @Override
        public void close() throws Exception {}

        @Override
        public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event)
                throws Exception {
            if (subtask != 0 || !(event instanceof IntegerEvent)) {
                throw new Exception(
                        String.format("Don't recognize event '%s' from task %d.", event, subtask));
            }

            receivedEvents.add(((IntegerEvent) event).value);

            if (receivedEvents.size() >= failAtMessage && !testScript.hasAlreadyFailed()) {
                testScript.recordHasFailed();
                context.failJob(new Exception("test failure"));
            }
        }

        @Override
        public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture)
                throws Exception {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(receivedEvents);
            byte[] bytes = bos.toByteArray();
            resultFuture.complete(bytes);
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {}

        @Override
        public void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData)
                throws Exception {
            receivedEvents.clear();
            if (checkpointData == null) {
                return;
            }
            ByteArrayInputStream bis = new ByteArrayInputStream(checkpointData);
            ObjectInputStream ois = new ObjectInputStream(bis);
            receivedEvents.addAll((List<Integer>) ois.readObject());
        }

        @Override
        public void subtaskReset(int subtask, long checkpointId) {}

        @Override
        public void executionAttemptFailed(
                int subtask, int attemptNumber, @Nullable Throwable reason) {}

        @Override
        public void executionAttemptReady(int subtask, int attemptNumber, SubtaskGateway gateway) {}
    }

    /** The stream operator that sends out an integer event for each received integer record. */
    private static class EventSendingOperator extends AbstractStreamOperator<Integer>
            implements OneInputStreamOperator<Integer, Integer> {

        private final OperatorEventGateway gateway;

        private EventSendingOperator(OperatorEventGateway gateway) {
            this.gateway = gateway;
        }

        @Override
        public void processElement(StreamRecord<Integer> element) throws Exception {
            gateway.sendEventToCoordinator(new IntegerEvent(element.getValue()));
        }
    }
}
