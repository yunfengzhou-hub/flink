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
import org.apache.flink.runtime.operators.coordination.CoordinatorEventsExactlyOnceITCase;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
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
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.runtime.operators.coordination.CoordinatorEventsExactlyOnceITCase.EndEvent;
import static org.apache.flink.runtime.operators.coordination.CoordinatorEventsExactlyOnceITCase.EventSendingCoordinator;
import static org.apache.flink.runtime.operators.coordination.CoordinatorEventsExactlyOnceITCase.IntegerEvent;
import static org.apache.flink.runtime.operators.coordination.CoordinatorEventsExactlyOnceITCase.StartEvent;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test case that validates the exactly-once mechanism for operator events sent around
 * checkpoint. This class is an extension to {@link
 * org.apache.flink.runtime.operators.coordination.CoordinatorEventsExactlyOnceITCase}, further
 * verifying the exactly-once semantics of events when the flink job is constructed using actual
 * stream operators.
 */
public class CoordinatorEventsExactlyOnceTest {
    @ClassRule
    public static final MiniClusterResource MINI_CLUSTER =
            new MiniClusterResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(2)
                            .setNumberSlotsPerTaskManager(2)
                            .build());

    private static final AtomicBoolean shouldCloseSource = new AtomicBoolean(false);

    private static final int numEvents = 100;

    private static final int delay = 1;

    private StreamExecutionEnvironment env;

    @Before
    public void setup() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(100);
        shouldCloseSource.set(false);
    }

    @Test
    public void test() throws Exception {
        long count =
                executeAndGetNumReceivedEvents(
                        env,
                        new EventReceivingOperatorFactory<>("eventReceiving", numEvents, delay));
        assertThat(count).isEqualTo(numEvents);
    }

    @Test
    public void testUnalignedCheckpoint() throws Exception {
        env.getCheckpointConfig().enableUnalignedCheckpoints();
        long count =
                executeAndGetNumReceivedEvents(
                        env,
                        new EventReceivingOperatorFactory<>("eventReceiving", numEvents, delay));
        assertThat(count).isEqualTo(numEvents);
    }

    @Test
    public void testFailingCheckpoint() throws Exception {
        executeAndGetNumReceivedEvents(
                env, new FailingCheckpointOperatorFactory<>("failingCheckpoint", numEvents, delay));
    }

    private long executeAndGetNumReceivedEvents(
            StreamExecutionEnvironment env, OneInputStreamOperatorFactory<Long, Long> factory)
            throws Exception {
        DataStream<Long> stream =
                env.addSource(new IdlingSourceFunction<>(), TypeInformation.of(Long.class))
                        .setParallelism(2);
        stream =
                stream.transform("eventReceiving", TypeInformation.of(Long.class), factory)
                        .setParallelism(1);
        stream.addSink(new DiscardingSink<>());

        JobExecutionResult executionResult =
                MINI_CLUSTER
                        .getMiniCluster()
                        .executeJobBlocking(env.getStreamGraph().getJobGraph());

        return executionResult.getAccumulatorResult(EventReceivingOperator.COUNTER_NAME);
    }

    /** A mock source function that does not collect any stream record and finishes on demand. */
    private static class IdlingSourceFunction<T> extends RichSourceFunction<T>
            implements ParallelSourceFunction<T> {
        private boolean isCancelled = false;

        @Override
        public void run(SourceContext<T> ctx) throws Exception {
            while (!isCancelled && !shouldCloseSource.get()) {
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
        private final String name;
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

            // signal the coordinator to start
            getContainingTask()
                    .getEnvironment()
                    .getOperatorCoordinatorEventGateway()
                    .sendOperatorEventToCoordinator(
                            getOperatorID(), new SerializedValue<>(new StartEvent()));
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
                shouldCloseSource.set(true);
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
        }
    }

    private static class FailingCheckpointOperatorFactory<IN, OUT>
            extends EventReceivingOperatorFactory<IN, OUT> {
        public FailingCheckpointOperatorFactory(String name, int numEvents, int delay) {
            super(name, numEvents, delay);
        }

        @Override
        public <T extends StreamOperator<OUT>> T createStreamOperator(
                StreamOperatorParameters<OUT> parameters) {
            EventReceivingOperator<OUT> operator = new CheckpointFailingOperator<>(numEvents);
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
     * checkpoint.
     */
    private static class CheckpointFailingOperator<T> extends EventReceivingOperator<T> {

        private final int failAtCheckpointAfterMessage;

        private CoordinatorEventsExactlyOnceITCase.TestScript testScript;

        private CheckpointFailingOperator(int numEvents) {
            this.failAtCheckpointAfterMessage =
                    numEvents * 2 / 3 + new Random().nextInt(numEvents / 6);
        }

        @Override
        public void setup(
                StreamTask<?, ?> containingTask,
                StreamConfig config,
                Output<StreamRecord<T>> output) {
            super.setup(containingTask, config, output);
            if (containingTask.getIndexInSubtaskGroup() == 0) {
                this.testScript =
                        CoordinatorEventsExactlyOnceITCase.TestScript.getForOperator(
                                getOperatorName() + "-subtask0");
            } else {
                this.testScript = null;
            }
        }

        @Override
        public void snapshotState(StateSnapshotContext context) throws Exception {
            super.snapshotState(context);
            if (counter.getLocalValue() > failAtCheckpointAfterMessage
                    && testScript != null
                    && !testScript.hasAlreadyFailed()) {
                testScript.recordHasFailed();
                throw new RuntimeException();
            }
        }
    }
}
