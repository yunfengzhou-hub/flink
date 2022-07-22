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

package org.apache.flink.runtime.operators.coordination;

/**
 * An {@link OperatorEvent} sent from a subtask to its {@link OperatorCoordinator} to signal that
 * the checkpoint of an individual task is completed.
 */
public class AcknowledgeCheckpointEvent implements OperatorEvent {

    /** The ID of the checkpoint that this event is related to. */
    final long checkpointId;

    public AcknowledgeCheckpointEvent(long checkpointId) {
        this.checkpointId = checkpointId;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(checkpointId);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof AcknowledgeCheckpointEvent)) {
            return false;
        }
        AcknowledgeCheckpointEvent event = (AcknowledgeCheckpointEvent) obj;
        return event.checkpointId == this.checkpointId;
    }

    @Override
    public String toString() {
        return "AcknowledgeCheckpointEvent (" + checkpointId + ')';
    }
}
