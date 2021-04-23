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

package org.apache.flink.streaming.api.functional;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import org.junit.Test;

public class ValidationTest {
    @Test(expected = IllegalArgumentException.class)
    public void testStatefulFunction() throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> stream = env.fromElements("hello")
                .map(new RichMapFunction<>() {
                    @Override
                    public String map(String s) throws Exception {
                        return s.toUpperCase();
                    }
                });
        DataStream.toFunction(stream);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMultipleSource() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        DataStream<Integer> input1 = env.fromElements(1);
        DataStream<Integer> input2 = env.fromElements(1);
        DataStream<Integer> unioned = input1.union(input2);
        DataStream.toFunction(unioned);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWindowFunction() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        DataStream<Integer> stream = env.fromElements(1)
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sum(1);

        DataStream.toFunction(stream);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testKeyedFunction() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        DataStream<Integer> stream = env.fromElements(1)
                .keyBy((KeySelector<Integer, Object>) integer -> integer)
                .sum(1);

        DataStream.toFunction(stream);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testIterate() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        DataStream<Integer> source = env.fromElements(1);
        IterativeStream<Integer> stream = source
                .map(x -> x + 1)
                .iterate();
        DataStream<Integer> feedback = stream.filter((FilterFunction<Integer>) integer -> integer>0);
        stream.closeWith(feedback);
        DataStream<Integer> result = stream.filter((FilterFunction<Integer>) integer -> integer<0);

        DataStream.toFunction(result);
    }
}
