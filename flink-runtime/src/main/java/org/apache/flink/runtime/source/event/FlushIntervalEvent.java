package org.apache.flink.runtime.source.event;

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

import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import java.time.Duration;

/**
 * An {@link OperatorEvent} sent from SourceCoordinator to SourceOperators signaling the change of
 * flush interval.
 */
public class FlushIntervalEvent implements OperatorEvent {
    private final Duration flushInterval;

    public FlushIntervalEvent(Duration flushInterval) {
        this.flushInterval = flushInterval;
    }

    public Duration getFlushInterval() {
        return flushInterval;
    }
}
