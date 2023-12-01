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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.disk;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageInputChannelId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.AvailabilityNotifier;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageConsumerSpec;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/** The data client is used to fetch data from disk tier. */
public class DiskTierConsumerAgent implements TierConsumerAgent {

    private final Map<
                    TieredStoragePartitionId,
                    Map<TieredStorageInputChannelId, CompletableFuture<NettyConnectionReader>>>
            nettyConnectionReaders = new HashMap<>();

    private final Map<
                    TieredStoragePartitionId,
                    Map<TieredStorageSubpartitionId, TieredStorageInputChannelId>>
            inputChannels = new HashMap<>();

    public DiskTierConsumerAgent(
            List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs,
            TieredStorageNettyService nettyService) {
        for (TieredStorageConsumerSpec spec : tieredStorageConsumerSpecs) {
            TieredStoragePartitionId partitionId = spec.getPartitionId();
            TieredStorageInputChannelId inputChannelId = spec.getInputChannelId();
            for (TieredStorageSubpartitionId subpartitionId : spec.getSubpartitionIds()) {
                inputChannels
                        .computeIfAbsent(partitionId, ignored -> new HashMap<>())
                        .put(subpartitionId, inputChannelId);
            }
            nettyConnectionReaders
                    .computeIfAbsent(partitionId, ignore -> new HashMap<>())
                    .put(
                            inputChannelId,
                            nettyService.registerConsumer(partitionId, inputChannelId));
        }
    }

    @Override
    public void start() {
        // noop
    }

    @Override
    public void registerAvailabilityNotifier(AvailabilityNotifier notifier) {
        // noop
    }

    @Override
    public Optional<Buffer> getNextBuffer(
            TieredStoragePartitionId partitionId, TieredStorageInputChannelId inputChannelId) {
        try {
            return nettyConnectionReaders.get(partitionId).get(inputChannelId).get().readBuffer();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Failed to get next buffer.", e);
        }
    }

    @Override
    public void notifyRequiredSegmentId(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId) {
        TieredStorageInputChannelId inputChannelId =
                inputChannels.get(partitionId).get(subpartitionId);
        try {
            nettyConnectionReaders
                    .get(partitionId)
                    .get(inputChannelId)
                    .get()
                    .notifyRequiredSegmentId(subpartitionId, segmentId);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Failed to notify required segment id.", e);
        }
    }

    @Override
    public void close() throws IOException {
        // noop
    }
}
