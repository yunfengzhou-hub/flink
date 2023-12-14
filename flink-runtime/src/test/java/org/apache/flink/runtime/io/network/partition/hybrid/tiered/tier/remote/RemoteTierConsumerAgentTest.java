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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote;

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexRange;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageInputChannelId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.PartitionFileReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.TestingPartitionFileReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.AvailabilityNotifier;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageConsumerSpec;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RemoteTierConsumerAgent}. */
class RemoteTierConsumerAgentTest {
    private static final int bufferSize = 10;

    @TempDir private File tempFolder;

    private String remoteStoragePath;

    private TieredStoragePartitionId partitionId;

    private DynamicBiFunction<Integer, Integer, PartitionFileReader.ReadBufferResult>
            readBufferSupplier;

    private RemoteTierConsumerAgent remoteTierConsumerAgent;

    @BeforeEach
    void before() {
        remoteStoragePath = Path.fromLocalFile(tempFolder).getPath();
        partitionId = new TieredStoragePartitionId(new ResultPartitionID());
        readBufferSupplier = new DynamicBiFunction<>();
    }

    @Test
    void testGetEmptyBuffer() {
        remoteTierConsumerAgent =
                new RemoteTierConsumerAgent(
                        Collections.singletonList(
                                new TieredStorageConsumerSpec(
                                        partitionId,
                                        new TieredStorageInputChannelId(0),
                                        new ResultSubpartitionIndexRange(0))),
                        new RemoteStorageScanner(remoteStoragePath),
                        new TestingPartitionFileReader.Builder().build(),
                        1024);
        assertThat(
                        remoteTierConsumerAgent.getNextBuffer(
                                partitionId, new TieredStorageInputChannelId(0)))
                .isEmpty();
    }

    @Test
    void testGetBuffer() {
        PartitionFileReader partitionFileReader =
                new TestingPartitionFileReader.Builder()
                        .setReadBufferSupplier(
                                (bufferIndex, segmentId) ->
                                        new PartitionFileReader.ReadBufferResult(
                                                Collections.singletonList(
                                                        BufferBuilderTestUtils.buildSomeBuffer(
                                                                bufferSize)),
                                                false,
                                                null))
                        .build();
        TestingRemoteStorageScanner scanner = new TestingRemoteStorageScanner(remoteStoragePath);
        remoteTierConsumerAgent =
                new RemoteTierConsumerAgent(
                        Collections.singletonList(
                                new TieredStorageConsumerSpec(
                                        partitionId,
                                        new TieredStorageInputChannelId(0),
                                        new ResultSubpartitionIndexRange(0))),
                        scanner,
                        partitionFileReader,
                        1024);
        scanner.notifyAvailable(partitionId, new TieredStorageSubpartitionId(0));
        Optional<Buffer> optionalBuffer =
                remoteTierConsumerAgent.getNextBuffer(
                        partitionId, new TieredStorageInputChannelId(0));
        assertThat(optionalBuffer)
                .hasValueSatisfying(
                        buffer -> assertThat(buffer.readableBytes()).isEqualTo(bufferSize));
    }

    @Test
    void testGetBufferWithMultipleSubpartitions() {
        PartitionFileReader partitionFileReader =
                new TestingPartitionFileReader.Builder()
                        .setReadBufferSupplier(readBufferSupplier)
                        .build();
        TestingRemoteStorageScanner scanner = new TestingRemoteStorageScanner(remoteStoragePath);
        remoteTierConsumerAgent =
                new RemoteTierConsumerAgent(
                        Collections.singletonList(
                                new TieredStorageConsumerSpec(
                                        partitionId,
                                        new TieredStorageInputChannelId(0),
                                        new ResultSubpartitionIndexRange(0, 1))),
                        scanner,
                        partitionFileReader,
                        1024);

        PartitionFileReader.ReadBufferResult bufferResult =
                new PartitionFileReader.ReadBufferResult(
                        Collections.singletonList(
                                BufferBuilderTestUtils.buildSomeBuffer(bufferSize)),
                        false,
                        null);

        int sp0BufferIndex = 0;
        int sp1BufferIndex = 0;

        scanner.notifyAvailable(partitionId, new TieredStorageSubpartitionId(0));
        testAndVerifyBufferIndexAndSize(bufferResult, sp0BufferIndex++);
        testAndVerifyBufferIndexAndSize(bufferResult, sp0BufferIndex++);
        testAndVerifyBufferIndexAndSize(null, sp0BufferIndex);
        testAndVerifyBufferIndexAndSize(null, sp0BufferIndex);

        scanner.notifyAvailable(partitionId, new TieredStorageSubpartitionId(1));
        testAndVerifyBufferIndexAndSize(null, sp0BufferIndex);

        bufferResult.getReadBuffers().get(0).setDataType(Buffer.DataType.EVENT_BUFFER);
        testAndVerifyBufferIndexAndSize(bufferResult, sp0BufferIndex++);
        testAndVerifyBufferIndexAndSize(bufferResult, sp1BufferIndex++);
        testAndVerifyBufferIndexAndSize(null, sp0BufferIndex, sp1BufferIndex);
        testAndVerifyBufferIndexAndSize(null);
    }

    private void testAndVerifyBufferIndexAndSize(
            PartitionFileReader.ReadBufferResult bufferResult, int... expectedBufferIndexes) {
        AtomicInteger index = new AtomicInteger(0);
        readBufferSupplier.set(
                (bufferIndex, segmentId) -> {
                    assertThat(bufferIndex)
                            .isEqualTo(expectedBufferIndexes[index.getAndIncrement()]);
                    return bufferResult;
                });
        Optional<Buffer> optionalBuffer =
                remoteTierConsumerAgent.getNextBuffer(
                        partitionId, new TieredStorageInputChannelId(0));

        if (bufferResult != null) {
            assertThat(optionalBuffer)
                    .hasValueSatisfying(
                            buffer -> assertThat(buffer.readableBytes()).isEqualTo(bufferSize));
        } else {
            assertThat(optionalBuffer).isEmpty();
        }
        assertThat(index).hasValue(expectedBufferIndexes.length);
    }

    private static class DynamicBiFunction<T, U, R> implements BiFunction<T, U, R> {
        private BiFunction<T, U, R> biFunction = (bufferIndex, segmentId) -> null;

        private void set(BiFunction<T, U, R> biFunction) {
            this.biFunction = biFunction;
        }

        @Override
        public R apply(T t, U u) {
            return biFunction.apply(t, u);
        }
    }

    private static class TestingRemoteStorageScanner extends RemoteStorageScanner {
        private final List<AvailabilityNotifier> notifiers;

        public TestingRemoteStorageScanner(String baseRemoteStoragePath) {
            super(baseRemoteStoragePath);
            this.notifiers = new ArrayList<>();
        }

        @Override
        public void start() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void watchSegment(
                TieredStoragePartitionId partitionId,
                TieredStorageSubpartitionId subpartitionId,
                int segmentId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void run() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void registerAvailabilityAndPriorityNotifier(AvailabilityNotifier retriever) {
            notifiers.add(retriever);
        }

        public void notifyAvailable(
                TieredStoragePartitionId partitionId, TieredStorageSubpartitionId subpartitionId) {
            notifiers.forEach(x -> x.notifyAvailable(partitionId, subpartitionId));
        }
    }
}
