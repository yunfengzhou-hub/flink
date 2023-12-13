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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty;

import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.TestInputChannel;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.util.ExceptionUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.DATA_BUFFER;
import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.NONE;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link NettyConnectionReader}. */
class NettyConnectionReaderTest {

    private static final int INPUT_CHANNEL_INDEX = 0;

    private CompletableFuture<Integer> requiredSegmentIdFuture;

    @BeforeEach
    void before() {
        requiredSegmentIdFuture = new CompletableFuture<>();
    }

    @Test
    void testReadBuffer() {
        int bufferNumber = 1;
        Supplier<InputChannel> inputChannelSupplier =
                createInputChannelSupplier(bufferNumber, requiredSegmentIdFuture);
        NettyConnectionReader reader = createNettyConnectionReader(inputChannelSupplier);
        reader.notifyRequiredSegmentId(new TieredStorageSubpartitionId(0), 0);
        Optional<Buffer> buffer = reader.readBuffer();
        assertThat(buffer).isPresent();
        assertThat(buffer.get().isBuffer()).isTrue();
        assertThat(requiredSegmentIdFuture).isNotDone();
    }

    @Test
    void testReadEmptyBuffer() {
        int bufferNumber = 0;
        Supplier<InputChannel> inputChannelSupplier =
                createInputChannelSupplier(bufferNumber, requiredSegmentIdFuture);
        NettyConnectionReader reader = createNettyConnectionReader(inputChannelSupplier);
        reader.notifyRequiredSegmentId(new TieredStorageSubpartitionId(0), 0);
        Optional<Buffer> buffer = reader.readBuffer();
        assertThat(buffer).isNotPresent();
        assertThat(requiredSegmentIdFuture).isNotDone();
    }

    @Test
    void testReadDifferentSegments() throws Exception {
        int bufferNumber = 0;
        Supplier<InputChannel> inputChannelSupplier =
                createInputChannelSupplier(bufferNumber, requiredSegmentIdFuture);
        NettyConnectionReader reader = createNettyConnectionReader(inputChannelSupplier);
        reader.notifyRequiredSegmentId(new TieredStorageSubpartitionId(0), 0);
        reader.readBuffer();
        assertThat(requiredSegmentIdFuture).isNotDone();
        reader.notifyRequiredSegmentId(new TieredStorageSubpartitionId(0), 1);
        reader.readBuffer();
        assertThat(requiredSegmentIdFuture.get()).isEqualTo(1);
    }

    private static Supplier<InputChannel> createInputChannelSupplier(
            int bufferNumber, CompletableFuture<Integer> requiredSegmentIdFuture) {
        TestInputChannel inputChannel =
                new TestInputChannel(
                        new SingleInputGateBuilder().build(),
                        INPUT_CHANNEL_INDEX,
                        requiredSegmentIdFuture);
        try {
            for (int index = 0; index < bufferNumber; ++index) {
                inputChannel.read(
                        new NetworkBuffer(
                                MemorySegmentFactory.allocateUnpooledSegment(0),
                                FreeingBufferRecycler.INSTANCE,
                                DATA_BUFFER),
                        index == bufferNumber - 1 ? NONE : DATA_BUFFER);
            }
        } catch (IOException | InterruptedException e) {
            ExceptionUtils.rethrow(e, "Failed to create test input channel.");
        }
        return () -> inputChannel;
    }

    private static NettyConnectionReader createNettyConnectionReader(
            Supplier<InputChannel> inputChannelSupplier) {
        return new NettyConnectionReaderImpl(inputChannelSupplier);
    }
}
