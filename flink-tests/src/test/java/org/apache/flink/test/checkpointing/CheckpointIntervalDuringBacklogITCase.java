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
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.test.util.NumberSequenceSourceBlockableByCheckpoint;
import org.apache.flink.util.CloseableIterator;

import org.junit.After;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
                                new NumberSequenceSourceBlockableByCheckpoint(
                                        0, EXPECTED_RESULT.size() / 2 - 1, NUM_SPLITS, true))
                        .addSource(
                                new NumberSequenceSourceBlockableByCheckpoint(
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
                                new NumberSequenceSourceBlockableByCheckpoint(
                                        0, EXPECTED_RESULT.size() / 2 - 1, NUM_SPLITS, false))
                        .addSource(
                                new NumberSequenceSourceBlockableByCheckpoint(
                                        EXPECTED_RESULT.size() / 2,
                                        EXPECTED_RESULT.size() - 1,
                                        NUM_SPLITS,
                                        false))
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
}
