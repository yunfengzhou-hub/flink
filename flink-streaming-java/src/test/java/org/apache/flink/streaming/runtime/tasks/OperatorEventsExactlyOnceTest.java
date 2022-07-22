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
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
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
import org.junit.ClassRule;
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

import static org.apache.flink.runtime.operators.coordination.CoordinatorEventsExactlyOnceITCase.IntegerEvent;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test case that validates the exactly-once mechanism for operator events sent from an
 * operator to its operator coordinator around checkpoint.
 *
 * <p>See also {@link CoordinatorEventsExactlyOnceTest} for integration tests about operator event
 * sent in the reversed direction.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class OperatorEventsExactlyOnceTest {
    @ClassRule
    public static final MiniClusterResource MINI_CLUSTER =
            new MiniClusterResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(2)
                            .setNumberSlotsPerTaskManager(2)
                            .build());

    private static final List<OperatorEvent> receivedEvents = new ArrayList<>();

    private static final int numEvents = 100;

    private static final int delay = 10;

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
    public void test() throws Exception {
        executeAndVerifyResult();
    }

    @Test
    public void testUnalignedCheckpoint() throws Exception {
        env.getCheckpointConfig().enableUnalignedCheckpoints();
        executeAndVerifyResult();
    }

    private void executeAndVerifyResult() throws Exception {
        DataStream<Integer> stream =
                env.fromSequence(0, numEvents)
                        .map(
                                x -> {
                                    Thread.sleep(delay);
                                    return x.intValue();
                                });
        stream =
                stream.transform(
                        "eventSending",
                        TypeInformation.of(Integer.class),
                        new EventSendingOperatorFactory());

        stream.addSink(new DiscardingSink<>());
        MINI_CLUSTER.getMiniCluster().executeJobBlocking(env.getStreamGraph().getJobGraph());
        List<IntegerEvent> expectedEvents = new ArrayList<>();
        for (int i = 0; i <= numEvents; i++) {
            expectedEvents.add(new IntegerEvent(i));
        }
        assertThat(receivedEvents)
                .containsExactlyInAnyOrder(expectedEvents.toArray(new IntegerEvent[0]));
    }

    /**
     * A wrapper operator factory for {@link EventSendingOperator} and {@link
     * EventReceivingCoordinator}.
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

        private final CoordinatorEventsExactlyOnceITCase.TestScript testScript;

        private final int failAtMessage;

        private EventReceivingCoordinator(Context context, OperatorID operatorID, int numEvents) {
            this.context = context;
            this.testScript =
                    CoordinatorEventsExactlyOnceITCase.TestScript.getForOperator(
                            operatorID.toString());
            this.failAtMessage = numEvents / 3 + new Random().nextInt(numEvents / 3);
        }

        @Override
        public void start() throws Exception {}

        @Override
        public void close() throws Exception {}

        @Override
        public void handleEventFromOperator(int subtask, OperatorEvent event) throws Exception {
            if (subtask != 0 || !(event instanceof IntegerEvent)) {
                throw new Exception(
                        String.format("Don't recognize event '%s' from task %d.", event, subtask));
            }

            receivedEvents.add(event);

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
            if (checkpointData == null) {
                return;
            }
            ByteArrayInputStream bis = new ByteArrayInputStream(checkpointData);
            ObjectInputStream ois = new ObjectInputStream(bis);
            receivedEvents.clear();
            receivedEvents.addAll((List<OperatorEvent>) ois.readObject());
        }

        @Override
        public void subtaskFailed(int subtask, @Nullable Throwable reason) {}

        @Override
        public void subtaskReset(int subtask, long checkpointId) {}

        @Override
        public void subtaskReady(int subtask, SubtaskGateway gateway) {}
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
