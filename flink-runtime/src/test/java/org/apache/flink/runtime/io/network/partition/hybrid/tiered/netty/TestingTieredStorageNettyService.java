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

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageInputChannelId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

/** Test implementation for {@link TieredStorageNettyService}. */
public class TestingTieredStorageNettyService implements TieredStorageNettyService {

    private final BiConsumer<TieredStoragePartitionId, NettyServiceProducer>
            registerProducerConsumer;

    private final BiFunction<
                    TieredStoragePartitionId,
                    TieredStorageInputChannelId,
                    CompletableFuture<NettyConnectionReader>>
            registerConsumerFunction;

    private TestingTieredStorageNettyService(
            BiConsumer<TieredStoragePartitionId, NettyServiceProducer> registerProducerConsumer,
            BiFunction<
                            TieredStoragePartitionId,
                            TieredStorageInputChannelId,
                            CompletableFuture<NettyConnectionReader>>
                    registerConsumerFunction) {
        this.registerProducerConsumer = registerProducerConsumer;
        this.registerConsumerFunction = registerConsumerFunction;
    }

    @Override
    public void registerProducer(
            TieredStoragePartitionId partitionId, NettyServiceProducer serviceProducer) {
        registerProducerConsumer.accept(partitionId, serviceProducer);
    }

    @Override
    public CompletableFuture<NettyConnectionReader> registerConsumer(
            TieredStoragePartitionId partitionId, TieredStorageInputChannelId inputChannelId) {
        return registerConsumerFunction.apply(partitionId, inputChannelId);
    }

    /** Builder for {@link TestingTieredStorageNettyService}. */
    public static class Builder {

        private BiConsumer<TieredStoragePartitionId, NettyServiceProducer>
                registerProducerConsumer = (partitionId, nettyServiceProducer) -> {};

        private BiFunction<
                        TieredStoragePartitionId,
                        TieredStorageInputChannelId,
                        CompletableFuture<NettyConnectionReader>>
                registerConsumerFunction = (tieredStoragePartitionId, inputChannelId) -> null;

        public Builder() {}

        public TestingTieredStorageNettyService.Builder setRegisterProducerConsumer(
                BiConsumer<TieredStoragePartitionId, NettyServiceProducer>
                        registerProducerConsumer) {
            this.registerProducerConsumer = registerProducerConsumer;
            return this;
        }

        public TestingTieredStorageNettyService.Builder setRegisterConsumerFunction(
                BiFunction<
                                TieredStoragePartitionId,
                                TieredStorageInputChannelId,
                                CompletableFuture<NettyConnectionReader>>
                        registerConsumerFunction) {
            this.registerConsumerFunction = registerConsumerFunction;
            return this;
        }

        public TestingTieredStorageNettyService build() {
            return new TestingTieredStorageNettyService(
                    registerProducerConsumer, registerConsumerFunction);
        }
    }
}
