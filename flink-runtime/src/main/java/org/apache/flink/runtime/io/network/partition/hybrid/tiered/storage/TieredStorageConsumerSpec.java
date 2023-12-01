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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage;

import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageInputChannelId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;

import java.util.Iterator;

/** Describe the different data sources in {@link TieredStorageConsumerClient}. */
public class TieredStorageConsumerSpec {

    private final TieredStoragePartitionId tieredStoragePartitionId;

    private final TieredStorageInputChannelId tieredStorageInputChannelId;

    private final ResultSubpartitionIndexSet tieredStorageSubpartitionIndexSet;

    public TieredStorageConsumerSpec(
            TieredStoragePartitionId tieredStoragePartitionId,
            TieredStorageInputChannelId tieredStorageInputChannelId,
            ResultSubpartitionIndexSet tieredStorageSubpartitionIndexSet) {
        this.tieredStoragePartitionId = tieredStoragePartitionId;
        this.tieredStorageInputChannelId = tieredStorageInputChannelId;
        this.tieredStorageSubpartitionIndexSet = tieredStorageSubpartitionIndexSet;
    }

    public TieredStoragePartitionId getPartitionId() {
        return tieredStoragePartitionId;
    }

    public TieredStorageInputChannelId getInputChannelId() {
        return tieredStorageInputChannelId;
    }

    public Iterable<TieredStorageSubpartitionId> getSubpartitionIds() {
        return () ->
                new Iterator<TieredStorageSubpartitionId>() {
                    private final Iterator<Integer> iterator =
                            tieredStorageSubpartitionIndexSet.values().iterator();

                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public TieredStorageSubpartitionId next() {
                        return new TieredStorageSubpartitionId(iterator.next());
                    }
                };
    }
}
