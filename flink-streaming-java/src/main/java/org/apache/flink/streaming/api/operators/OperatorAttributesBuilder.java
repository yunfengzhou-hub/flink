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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Experimental;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/** The builder class for {@link OperatorAttributes}. */
@Experimental
public class OperatorAttributesBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(OperatorAttributesBuilder.class);
    @Nullable private Boolean outputOnlyAfterEndOfStream = null;

    /**
     * Set to true if and only if the operator only emits records after all its inputs have ended.
     * If it is not set, the default value false is used.
     */
    public OperatorAttributesBuilder setOutputOnlyAfterEndOfStream(
            boolean outputOnlyAfterEndOfStream) {
        this.outputOnlyAfterEndOfStream = outputOnlyAfterEndOfStream;
        return this;
    }

    /** If any operator attribute is not set explicitly, we will log it at DEBUG level. */
    public OperatorAttributes build() {
        if (outputOnlyAfterEndOfStream == null) {
            LOG.debug(
                    "OperatorAttributes outputOnlyAfterEndOfStream is null, set to the default value of false.");
            outputOnlyAfterEndOfStream = false;
        }

        return new OperatorAttributes(outputOnlyAfterEndOfStream);
    }
}
