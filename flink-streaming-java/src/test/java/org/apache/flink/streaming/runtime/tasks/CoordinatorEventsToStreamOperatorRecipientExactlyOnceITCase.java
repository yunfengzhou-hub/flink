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

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.operators.coordination.CoordinatorEventsExactlyOnceITCase;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.CoordinatedOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test case that validates the exactly-once mechanism for operator events sent around
 * checkpoint. This class is an extension to {@link CoordinatorEventsExactlyOnceITCase}, further
 * verifying the exactly-once semantics of events in the following conditions:
 *
 * <h2>Stream operator recipient</h2>
 *
 * <p>In {@link CoordinatorEventsExactlyOnceITCase}, the test cases focus on verifying the
 * correctness of operator coordinator's behavior. It uses a custom {@link AbstractInvokable}
 * subclass that mocks the behavior of coordinator's recipients. This test class uses actual stream
 * operators as the recipient of coordinator events, verifying that stream operators can correctly
 * handle received operator events and inform coordinators of completing checkpoint.
 *
 * <h2>Non-source stream task</h2>
 *
 * <p>In {@link CoordinatorEventsExactlyOnceITCase}, the two tested tasks are executed independently
 * of each other. They do not have the upstream-downstream relationship as operators usually do in a
 * streaming job, and thus both of them are treated as source tasks. This test class further
 * verifies situations when the tested operators are not sources, which means when checkpoint
 * barriers are injected into sources, these operators may not have started checkpoint yet.
 *
 * <h2>Unaligned checkpoint</h2>
 *
 * <p>This class tests both aligned and unaligned checkpoints to verify that the correctness of the
 * event delivery behavior around checkpoint is not affected by this condition.
 *
 * <h2>Non-global failure</h2>
 *
 * <p>In {@link CoordinatorEventsExactlyOnceITCase}, failures occur at the coordinators' side, so
 * they will cause the whole Flink job to fail over. In this class, test cases are added when there
 * might only be fail-overs on the subtasks' side, while the coordinators are not affected. In this
 * case the production infrastructure code needs to work together with user code (implementations
 * inside the coordinator and operator subclass) to ensure the exactly-once semantics of operator
 * event delivery.
 */
public class CoordinatorEventsToStreamOperatorRecipientExactlyOnceITCase
        extends CoordinatorEventsExactlyOnceITCase {

    private static final AtomicBoolean SHOULD_CLOSE_SOURCE = new AtomicBoolean(false);

    private static final int NUM_EVENTS = 100;

    private static final int DELAY = 1;

    private StreamExecutionEnvironment env;

    @Before
    public void setup() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(100);
        SHOULD_CLOSE_SOURCE.set(false);
    }

    @Test
    public void testCoordinatorSendEvents() throws Exception {
        executeAndVerifyResults(
                env, new EventReceivingOperatorFactory<>("eventReceiving", NUM_EVENTS, DELAY));
    }

    @Test
    public void testUnalignedCheckpoint() throws Exception {
        env.getCheckpointConfig().enableUnalignedCheckpoints();
        executeAndVerifyResults(
                env, new EventReceivingOperatorFactory<>("eventReceiving", NUM_EVENTS, DELAY));
    }

    @Test
    public void testFailingCheckpoint() throws Exception {
        executeAndVerifyResults(
                env,
                new FailingCheckpointOperatorFactory<>("failingCheckpoint", NUM_EVENTS, DELAY));
        assertThat(TestScript.getForOperator("failingCheckpoint-subtask0").hasAlreadyFailed())
                .isTrue();
    }

    private void executeAndVerifyResults(
            StreamExecutionEnvironment env, EventReceivingOperatorFactory<Long, Long> factory)
            throws Exception {
        // Differs the parallelism of source operators from that of tested operators, so that when
        // checkpoint barriers are injected into sources, the tested operators have not started
        // checkpoint yet.
        DataStream<Long> stream =
                env.addSource(new IdlingSourceFunction<>(), TypeInformation.of(Long.class))
                        .setParallelism(2);
        stream =
                stream.transform(factory.name, TypeInformation.of(Long.class), factory)
                        .setParallelism(1);
        stream.addSink(new DiscardingSink<>());

        JobExecutionResult executionResult =
                MINI_CLUSTER
                        .getMiniCluster()
                        .executeJobBlocking(env.getStreamGraph().getJobGraph());

        long count = executionResult.getAccumulatorResult(EventReceivingOperator.COUNTER_NAME);
        assertThat(count).isEqualTo(NUM_EVENTS);
    }

    /** A mock source function that does not collect any stream record and finishes on demand. */
    private static class IdlingSourceFunction<T> extends RichSourceFunction<T>
            implements ParallelSourceFunction<T> {
        private boolean isCancelled = false;

        @Override
        public void run(SourceContext<T> ctx) throws Exception {
            while (!isCancelled && !SHOULD_CLOSE_SOURCE.get()) {
                Thread.sleep(100);
            }
        }

        @Override
        public void cancel() {
            isCancelled = true;
        }
    }

    /**
     * A wrapper operator factory for {@link EventReceivingOperator} and {@link
     * EventSendingCoordinator}.
     */
    private static class EventReceivingOperatorFactory<IN, OUT>
            extends AbstractStreamOperatorFactory<OUT>
            implements CoordinatedOperatorFactory<OUT>, OneInputStreamOperatorFactory<IN, OUT> {

        protected final String name;

        protected final int numEvents;

        private final int delay;

        public EventReceivingOperatorFactory(String name, int numEvents, int delay) {
            this.name = name;
            this.numEvents = numEvents;
            this.delay = delay;
        }

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
                    return new EventSendingCoordinator(context, name, numEvents, delay);
                }
            };
        }

        @Override
        public <T extends StreamOperator<OUT>> T createStreamOperator(
                StreamOperatorParameters<OUT> parameters) {
            EventReceivingOperator<OUT> operator = new EventReceivingOperator<>();
            operator.setup(
                    parameters.getContainingTask(),
                    parameters.getStreamConfig(),
                    parameters.getOutput());
            parameters
                    .getOperatorEventDispatcher()
                    .registerEventHandler(parameters.getStreamConfig().getOperatorID(), operator);
            return (T) operator;
        }

        @Override
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return EventReceivingOperator.class;
        }
    }

    /**
     * The stream operator that receives the events and accumulates the numbers. The task is
     * stateful and checkpoints the accumulator.
     */
    private static class EventReceivingOperator<T> extends AbstractStreamOperator<T>
            implements OneInputStreamOperator<T, T>, OperatorEventHandler {

        protected static final String COUNTER_NAME = "numEvents";

        protected final LongCounter counter = new LongCounter();

        protected ListState<Long> state;

        @Override
        public void open() throws Exception {
            super.open();
            getRuntimeContext().addAccumulator(COUNTER_NAME, counter);
        }

        @Override
        public void processElement(StreamRecord<T> element) throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public void handleOperatorEvent(OperatorEvent evt) {
            if (evt instanceof IntegerEvent) {
                counter.add(1L);
            } else if (evt instanceof EndEvent) {
                try {
                    state.update(Collections.singletonList(counter.getLocalValue()));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                SHOULD_CLOSE_SOURCE.set(true);
            } else {
                throw new UnsupportedOperationException();
            }
        }

        @Override
        public void snapshotState(StateSnapshotContext context) throws Exception {
            super.snapshotState(context);
            state.update(Collections.singletonList(counter.getLocalValue()));
        }

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);

            state =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>(
                                            "counterState", BasicTypeInfo.LONG_TYPE_INFO));

            counter.resetLocal();
            Iterator<Long> iterator = state.get().iterator();
            if (iterator.hasNext()) {
                counter.add(iterator.next());
            }
            Preconditions.checkArgument(!iterator.hasNext());

            // signal the coordinator to start
            getContainingTask()
                    .getEnvironment()
                    .getOperatorCoordinatorEventGateway()
                    .sendOperatorEventToCoordinator(
                            getOperatorID(),
                            new SerializedValue<>(
                                    new StartEvent(counter.getLocalValue().intValue() - 1)));
        }
    }

    /**
     * A wrapper operator factory for {@link FailingCheckpointOperator} and {@link
     * EventSendingCoordinator}.
     */
    private static class FailingCheckpointOperatorFactory<IN, OUT>
            extends EventReceivingOperatorFactory<IN, OUT> {
        public FailingCheckpointOperatorFactory(String name, int numEvents, int delay) {
            super(name, numEvents, delay);
        }

        @Override
        public <T extends StreamOperator<OUT>> T createStreamOperator(
                StreamOperatorParameters<OUT> parameters) {
            EventReceivingOperator<OUT> operator = new FailingCheckpointOperator<>(name, numEvents);
            operator.setup(
                    parameters.getContainingTask(),
                    parameters.getStreamConfig(),
                    parameters.getOutput());
            parameters
                    .getOperatorEventDispatcher()
                    .registerEventHandler(parameters.getStreamConfig().getOperatorID(), operator);
            return (T) operator;
        }
    }

    /**
     * A subclass of {@link EventReceivingOperator} whose subtask would fail during a specific
     * checkpoint. It is required that the parallelism of this operator is 1 in order for it to work
     * correctly.
     */
    private static class FailingCheckpointOperator<T> extends EventReceivingOperator<T> {

        private final String name;

        private final int failAtCheckpointAfterMessage;

        private TestScript testScript;

        private FailingCheckpointOperator(String name, int numEvents) {
            this.name = name;
            this.failAtCheckpointAfterMessage =
                    numEvents * 2 / 3 + new Random().nextInt(numEvents / 6);
        }

        @Override
        public void setup(
                StreamTask<?, ?> containingTask,
                StreamConfig config,
                Output<StreamRecord<T>> output) {
            super.setup(containingTask, config, output);
            Preconditions.checkState(containingTask.getIndexInSubtaskGroup() == 0);
            this.testScript = TestScript.getForOperator(name + "-subtask0");
        }

        @Override
        public void handleOperatorEvent(OperatorEvent evt) {
            if (evt instanceof IntegerEvent) {
                counter.add(1L);
            } else if (evt instanceof EndEvent) {
                try {
                    state.update(Collections.singletonList(counter.getLocalValue()));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                if (testScript.hasAlreadyFailed()) {
                    SHOULD_CLOSE_SOURCE.set(true);
                }
            } else {
                throw new UnsupportedOperationException();
            }
        }

        @Override
        public void snapshotState(StateSnapshotContext context) throws Exception {
            super.snapshotState(context);
            if (counter.getLocalValue() > failAtCheckpointAfterMessage
                    && !testScript.hasAlreadyFailed()) {
                testScript.recordHasFailed();
                throw new RuntimeException();
            }
        }
    }
}
