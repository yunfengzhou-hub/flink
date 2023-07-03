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

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceEnumerator;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceReader;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceSplit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.CloseableIterator;

import org.junit.After;
import org.junit.Test;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL;
import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL_DURING_BACKLOG;
import static org.assertj.core.api.Assertions.assertThat;

public class CheckpointIntervalDuringBacklogITCase {
    private static final int NUM_SPLITS = 2;
    private static final List<Long> EXPECTED_RESULT =
            LongStream.rangeClosed(0, 39).boxed().collect(Collectors.toList());

    @After
    public void tearDown() {
        CheckpointRecordingOperator.reset();
    }

    @Test
    public void testHybridSourceWithCheckpoint() throws Exception {
        Source<Long, ?, ?> source =
                HybridSource.builder(
                                new TestingNumberSequenceSource(
                                        0, EXPECTED_RESULT.size() / 2 - 1, NUM_SPLITS, true))
                        .addSource(
                                new TestingNumberSequenceSource(
                                        EXPECTED_RESULT.size() / 2,
                                        EXPECTED_RESULT.size() - 1,
                                        NUM_SPLITS,
                                        true))
                        .build();

        Configuration configuration = new Configuration();
        configuration.set(CHECKPOINTING_INTERVAL, Duration.ofMillis(100));
        configuration.set(CHECKPOINTING_INTERVAL_DURING_BACKLOG, Duration.ofMillis(200));
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);

        runAndVerifyResult(env, source);

        assertThat(CheckpointRecordingOperator.checkpointCounterBeforeSwitchSource.get())
                .isGreaterThan(0);
        assertThat(CheckpointRecordingOperator.checkpointCounterAfterSwitchSource.get())
                .isGreaterThan(0);
    }

    @Test
    public void testHybridSourceWithCheckpoint2() throws Exception {
        Source<Long, ?, ?> source =
                HybridSource.builder(
                                new TestingNumberSequenceSource(
                                        0, EXPECTED_RESULT.size() / 2 - 1, NUM_SPLITS, false))
                        .addSource(
                                new TestingNumberSequenceSource(
                                        EXPECTED_RESULT.size() / 2,
                                        EXPECTED_RESULT.size() - 1,
                                        NUM_SPLITS,
                                        true))
                        .build();

        Configuration configuration = new Configuration();
        configuration.set(CHECKPOINTING_INTERVAL, Duration.ofMillis(100));
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);

        runAndVerifyResult(env, source);

        assertThat(CheckpointRecordingOperator.checkpointCounterBeforeSwitchSource.get())
                .isEqualTo(0);
        assertThat(CheckpointRecordingOperator.checkpointCounterAfterSwitchSource.get())
                .isGreaterThan(0);
    }

    private void runAndVerifyResult(StreamExecutionEnvironment env, Source<Long, ?, ?> source) throws Exception {

        final DataStream<Long> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "hybrid-source")
                        .returns(Long.class)
                        .transform(
                                "CheckpointRecordingOperator",
                                Types.LONG,
                                new CheckpointRecordingOperator<>());

        final List<Long> result = new ArrayList<>();
        try (CloseableIterator<Long> iterator = stream.executeAndCollect()) {
            while (iterator.hasNext()) {
                result.add(iterator.next());
            }
        }

        Collections.sort(result);
        assertThat(result).containsExactly(EXPECTED_RESULT.toArray(new Long[0]));
    }

    private static class CheckpointRecordingOperator<T> extends AbstractStreamOperator<T>
            implements OneInputStreamOperator<T, T> {
        private static final AtomicInteger checkpointCounterBeforeSwitchSource =
                new AtomicInteger(0);
        private static final AtomicInteger checkpointCounterAfterSwitchSource =
                new AtomicInteger(0);

        private int numRecords;

        private CheckpointRecordingOperator() {
            numRecords = 0;
        }

        private static void reset() {
            checkpointCounterBeforeSwitchSource.set(0);
            checkpointCounterAfterSwitchSource.set(0);
        }

        @Override
        public void processElement(StreamRecord<T> element) {
            numRecords++;
            output.collect(element);
        }

        @Override
        public void snapshotState(StateSnapshotContext context) {
            if (numRecords < EXPECTED_RESULT.size() / 2) {
                checkpointCounterBeforeSwitchSource.incrementAndGet();
            } else {
                checkpointCounterAfterSwitchSource.incrementAndGet();
            }
        }
    }

    /**
     * This is an enumerator for the {@link NumberSequenceSource}, which only responds to the split
     * requests after the next checkpoint is complete. That way, we naturally draw the split
     * processing across checkpoints without artificial sleep statements.
     */
    private static final class AssignAfterCheckpointEnumerator<
                    SplitT extends IteratorSourceSplit<?, ?>>
            extends IteratorSourceEnumerator<SplitT> {
        private final Queue<Integer> pendingRequests = new ArrayDeque<>();
        private final SplitEnumeratorContext<?> context;

        public AssignAfterCheckpointEnumerator(
                SplitEnumeratorContext<SplitT> context, Collection<SplitT> splits) {
            super(context, splits);
            this.context = context;
        }

        @Override
        public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
            pendingRequests.add(subtaskId);
        }

        @Override
        public Collection<SplitT> snapshotState(long checkpointId) throws Exception {
            // this will be enqueued in the enumerator thread, so it will actually run after this
            // method (the snapshot operation) is complete!
            context.runInCoordinatorThread(this::fullFillPendingRequests);

            return super.snapshotState(checkpointId);
        }

        private void fullFillPendingRequests() {
            for (int subtask : pendingRequests) {
                // respond only to requests for which we still have registered readers
                if (!context.registeredReaders().containsKey(subtask)) {
                    continue;
                }
                super.handleSplitRequest(subtask, null);
            }
            pendingRequests.clear();
        }
    }

    private static class TestingNumberSequenceSource extends NumberSequenceSource {
        private static final long serialVersionUID = 1L;

        private final boolean isWaitForCheckpoint;
        private final int numSplits;
        private final long numAllowedMessageBeforeCheckpoint;

        public TestingNumberSequenceSource(
                long from, long to, int numSplits, boolean isWaitForCheckpoint) {
            super(from, to);
            this.numSplits = numSplits;
            this.isWaitForCheckpoint = isWaitForCheckpoint;
            if (isWaitForCheckpoint) {
                this.numAllowedMessageBeforeCheckpoint = (to - from) / numSplits;
            } else {
                this.numAllowedMessageBeforeCheckpoint = Long.MAX_VALUE;
            }
        }

        @Override
        public SplitEnumerator<NumberSequenceSplit, Collection<NumberSequenceSplit>>
                createEnumerator(final SplitEnumeratorContext<NumberSequenceSplit> enumContext) {
            final List<NumberSequenceSplit> splits =
                    splitNumberRange(getFrom(), getTo(), numSplits);
            if (isWaitForCheckpoint) {
                return new AssignAfterCheckpointEnumerator<>(enumContext, splits);
            } else {
                return new IteratorSourceEnumerator<>(enumContext, splits);
            }
        }

        @Override
        public SourceReader<Long, NumberSequenceSplit> createReader(
                SourceReaderContext readerContext) {
            return new CheckpointListeningIteratorSourceReader<>(
                    readerContext, numAllowedMessageBeforeCheckpoint);
        }
    }

    private static class CheckpointListeningIteratorSourceReader<
                    E, IterT extends Iterator<E>, SplitT extends IteratorSourceSplit<E, IterT>>
            extends IteratorSourceReader<E, IterT, SplitT> {
        private boolean checkpointed = false;
        private long messagesProduced = 0;
        private final long numAllowedMessageBeforeCheckpoint;

        public CheckpointListeningIteratorSourceReader(
                SourceReaderContext context, long waitForCheckpointAfterMessages) {
            super(context);
            this.numAllowedMessageBeforeCheckpoint = waitForCheckpointAfterMessages;
        }

        @Override
        public InputStatus pollNext(ReaderOutput<E> output) {
            if (messagesProduced < numAllowedMessageBeforeCheckpoint || checkpointed) {
                messagesProduced++;
                return super.pollNext(output);
            } else {
                return InputStatus.NOTHING_AVAILABLE;
            }
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            checkpointed = true;
        }
    }
}
