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

package org.apache.flink.streaming.runtime.streamrecord;

import org.apache.flink.annotation.Internal;

/** The strategy how receiver operators should perform flush operation for the incoming records. */
@Internal
public enum FlushStrategy {
    /**
     * Unless the operator receives the next {@link FlushEvent} or the stream ended its input, the
     * operator needs not explicitly flush buffered records.
     */
    NO_ACTIVE_FLUSH,

    /**
     * The operator should forward each output record to downstream operator without using buffer.
     */
    FLUSH_EVERY_RECORD,
}
