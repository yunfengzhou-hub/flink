/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.flush;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.source.coordinator.SourceCoordinator;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.configuration.ExecutionOptions.ALLOWED_LATENCY;

/**
 * The FlushCoordinator periodically triggers flush events on a Flink job based on the latency
 * requirements acquired from a job's configuration.
 */
@Internal
public class FlushCoordinator {
    private static final String GLOBAL_ALLOWED_LATENCY_KEY = "";

    private final Map<String, Duration> allowedLatencyMap;

    private final Map<String, SourceCoordinator<?, ?>> coordinatorMap;

    private Duration allowedLatency;

    private Duration flushInterval = Duration.ofSeconds(1);

    private volatile boolean isClosed = false;

    private FlushCoordinatorContext context;

    public FlushCoordinator(Configuration configuration) {
        allowedLatencyMap = new HashMap<>();
        allowedLatency = configuration.get(ALLOWED_LATENCY);
        if (allowedLatency != null) {
            allowedLatencyMap.put(GLOBAL_ALLOWED_LATENCY_KEY, allowedLatency);
        }
        coordinatorMap = new HashMap<>();
    }

    /** Initializes the coordinator. Sets access to the context. */
    public void setup(FlushCoordinatorContext context) {
        this.context = context;
    }

    /** Starts the coordinator. */
    public void start() {
        System.out.println("FlushCoordinator.start");
        while (!isClosed) {
            try {
                Thread.sleep(flushInterval.toMillis());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            System.out.println("Execution::triggerFlush " + context.getSourceExecutions().size());

            context.getSourceExecutions().forEach(Execution::triggerFlush);
        }
    }

    /** Closes the coordinator. */
    public void close() {
        isClosed = true;
    }

    public void registerSourceCoordinator(SourceCoordinator<?, ?> coordinator) {
        coordinatorMap.put(coordinator.getOperatorID().toHexString(), coordinator);
    }

    /** Configures the latency requirement of a certain source operator. */
    public void setAllowedLatency(OperatorID operatorID, Duration allowedLatency) {
        //        String key = operatorID.toHexString();
        //        Preconditions.checkArgument(coordinatorMap.containsKey(key));
        //        if (allowedLatency != null) {
        //            allowedLatencyMap.put(key, allowedLatency);
        //        } else {
        //            allowedLatencyMap.remove(key);
        //        }
        //
        //        Duration newAllowedLatency;
        //        if (!allowedLatencyMap.isEmpty()) {
        //            newAllowedLatency =
        //                    allowedLatencyMap.values().stream()
        //                            .reduce(
        //                                    (duration, duration2) -> {
        //                                        if (duration.compareTo(duration2) < 0) {
        //                                            return duration;
        //                                        }
        //                                        return duration2;
        //                                    })
        //                            .get();
        //        } else {
        //            newAllowedLatency = null;
        //        }
        //
        //        if (!Objects.equals(newAllowedLatency, this.allowedLatency)) {
        //            coordinatorMap.values().forEach(x ->
        // x.updateFlushInterval(newAllowedLatency));
        //            this.allowedLatency = newAllowedLatency;
        //        }
    }

    /**
     * A {@link FlushCoordinatorContext} provides the information a {@link FlushCoordinator} might
     * use when running.
     */
    interface FlushCoordinatorContext {
        CheckpointCoordinator getCheckpointCoordinator();

        Set<SourceCoordinator<?, ?>> getSourceCoordinators();

        Set<Execution> getSourceExecutions();
    }
}
