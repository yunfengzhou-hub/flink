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

package org.apache.flink.cep;

import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.rule.Rule;
import org.apache.flink.cep.rule.request.RuleRequest;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.List;

/**
 * Utility class for complex event processing.
 *
 * <p>Methods which transform a {@link DataStream} into a {@link PatternStream} to do CEP.
 */
public class CEP {
    /**
     * Creates a {@link PatternStream} from an input data stream and a pattern.
     *
     * @param input DataStream containing the input events
     * @param pattern Pattern specification which shall be detected
     * @param <T> Type of the input events
     * @return Resulting pattern stream
     */
    public static <T> PatternStream<T> pattern(DataStream<T> input, Pattern<T, ?> pattern) {
        return new PatternStream<>(input, pattern);
    }

    /**
     * Creates a {@link PatternStream} from an input data stream and a pattern.
     *
     * @param input DataStream containing the input events
     * @param pattern Pattern specification which shall be detected
     * @param comparator Comparator to sort events with equal timestamps
     * @param <T> Type of the input events
     * @return Resulting pattern stream
     */
    public static <T> PatternStream<T> pattern(
            DataStream<T> input, Pattern<T, ?> pattern, EventComparator<T> comparator) {
        final PatternStream<T> stream = new PatternStream<>(input, pattern);
        return stream.withComparator(comparator);
    }

    /**
     * Creates a {@link PatternStream} from an input data stream and a {@link RuleRequest} stream.
     *
     * @param dataInput DataStream containing the input events
     * @param ruleInput DataStream containing the rule update operations
     * @param <T> Type of the input events
     * @param <R> Type of the output matches
     * @return Resulting pattern stream
     */
    public static <T, R> PatternStream<T> pattern(
            DataStream<T> dataInput, DataStream<RuleRequest<T, R>> ruleInput) {
        return null;
    }

    /**
     * Creates a {@link PatternStream} from an input data stream and a {@link RuleRequest} stream.
     *
     * @param dataInput DataStream containing the input events
     * @param ruleInput DataStream containing the rule update operations
     * @param initRules Rules that the pattern stream initially sets up with.
     * @param <T> Type of the input events
     * @param <R> Type of the output matches
     * @return Resulting pattern stream
     */
    public static <T, R> PatternStream<T> pattern(
            DataStream<T> dataInput,
            DataStream<RuleRequest<T, R>> ruleInput,
            List<Rule<T, R>> initRules) {
        return null;
    }
}
