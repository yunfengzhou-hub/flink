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
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class CorrectnessTest {
    @Test
    public void testBasicCorrectness() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        DataStream<String> stream = env.fromElements("hello")
                .map(String::toUpperCase);
        assertEquals(Collections.singletonList("HELLO"), DataStream.toFunction(stream).apply(Collections.singletonList("hello")));
    }

    @Test
    public void testChainedOperation() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        DataStream<String> stream = env.fromElements(1)
                .map(x -> x + "x")
                .map(String::toUpperCase);
        assertEquals(Collections.singletonList("1X"), DataStream.toFunction(stream).apply(Collections.singletonList(1)));
    }

    @Test
    public void testConnectedStream() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        DataStream<Integer> input = env.fromElements(1);
        DataStream<Integer> branch1 = input.map(x -> x+1);
        DataStream<Integer> branch2 = input.map(x -> x-1);
        DataStream<Integer> connected = branch1.connect(branch2).map(new CoMapFunction<>() {
            @Override
            public Integer map1(Integer value) throws Exception {
                if(value<1) value+=2;
                return value;
            }

            @Override
            public Integer map2(Integer value) throws Exception {
                if(value<1) value+=2;
                return value;
            }
        });
        assertEquals(Arrays.asList(2,2), DataStream.toFunction(connected).apply(Collections.singletonList(1)));
    }

    @Test
    public void testUnionStream() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        DataStream<Integer> input = env.fromElements(1);
        DataStream<Integer> branch1 = input.map(x -> x+1);
        DataStream<Integer> branch2 = input.map(x -> x-1);
        DataStream<Integer> unioned = branch1.union(branch2).map(new MapFunction<>() {
            @Override
            public Integer map(Integer value) throws Exception {
                if(value<1) value+=2;
                return value;
            }
        });
        assertEquals(Arrays.asList(2,2), DataStream.toFunction(unioned).apply(Collections.singletonList(1)));
    }

    @Test
    public void testFlatMap() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        DataStream<Integer> stream = env.fromElements(1)
                .flatMap(new FlatMapFunction<Integer, Integer>() {
                    @Override
                    public void flatMap(
                            Integer integer,
                            Collector<Integer> collector) throws Exception {
                        collector.collect(integer+1);
                        collector.collect(integer+1);
                    }
                });

        assertEquals(Arrays.asList(2,2), DataStream.toFunction(stream).apply(Collections.singletonList(1)));
    }

    @Test
    public void testFilter() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        DataStream<Integer> stream = env.fromElements(1)
                .filter((FilterFunction<Integer>) integer -> false);

        assertEquals(Collections.emptyList(), DataStream.toFunction(stream).apply(Collections.singletonList(1)));
    }

    @Test
    public void testCoFlatMap() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        DataStream<Integer> input = env.fromElements(1);
        DataStream<Integer> branch1 = input.map(x -> x+1);
        DataStream<Integer> branch2 = input.map(x -> x-1);
        DataStream<Integer> connected = branch1.connect(branch2).flatMap(new CoFlatMapFunction<>() {
            @Override
            public void flatMap1(Integer value, Collector<Integer> out) throws Exception {
                if(value<1) value+=2;
                out.collect(value);
            }

            @Override
            public void flatMap2(Integer value, Collector<Integer> out) throws Exception {
                if(value<1) value+=2;
                out.collect(value);
            }
        });
        assertEquals(Arrays.asList(2, 2), DataStream.toFunction(connected).apply(Collections.singletonList(1)));
    }

    @Test
    public void testProject() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        DataStream<Tuple3<String, Integer, Boolean>> input = env.fromElements(new Tuple3<>("hello", 1, true));
        DataStream<Tuple2<Boolean, String>> stream = input.project(2, 0);

        assertEquals(Collections.singletonList(new Tuple2<>(true, "hello")), DataStream.toFunction(stream).apply(Collections.singletonList(new Tuple3<>("hello", 1, true))));
    }

    @Test
    public void testStartNewChain() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        DataStream<String> stream = env.fromElements("hello")
                .map(String::toUpperCase)
                .map(String::toUpperCase)
                .startNewChain()
                .map(String::toUpperCase)
                .map(String::toUpperCase);
        assertEquals(Collections.singletonList("HELLO"), DataStream.toFunction(stream).apply(Collections.singletonList("hello")));
    }

    @Test
    public void testDisableChaining() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        DataStream<String> stream = env.fromElements("hello")
                .map(String::toUpperCase)
                .map(String::toUpperCase)
                .disableChaining()
                .map(String::toUpperCase)
                .map(String::toUpperCase);
        assertEquals(Collections.singletonList("HELLO"), DataStream.toFunction(stream).apply(Collections.singletonList("hello")));
    }

    @Test
    public void testNotClearOperators() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        DataStream<String> stream = env.fromElements("hello")
                .map(String::toUpperCase);
        assertEquals(Collections.singletonList("HELLO"), DataStream.toFunction(stream).apply(Collections.singletonList("hello")));
        assertEquals(Collections.singletonList("HELLO"), DataStream.toFunction(stream).apply(Collections.singletonList("hello")));
        assertEquals(Collections.singletonList("HELLO"), DataStream.toFunction(stream).apply(Collections.singletonList("hello")));
    }

    @Test
    public void testOperatorID() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        DataStream<String> stream = env.fromElements("hello")
                .map(String::toUpperCase).uid("operator 6")
                .map(String::toLowerCase).uid("operator 5")
                .map(String::toUpperCase).uid("operator 4")
                .map(String::toLowerCase).uid("operator 3")
                .map(String::toUpperCase).uid("operator 2")
                .map(String::toLowerCase).uid("operator 1")
                .map(String::toUpperCase);
        assertEquals(Collections.singletonList("HELLO"), DataStream.toFunction(stream).apply(Collections.singletonList("hello")));
    }

    @Test
    public void testIrrelevantBranch() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        DataStream<String> input = env.fromElements("hello");
        DataStream<String> stream = input.map(String::toUpperCase);
        DataStream<String> stream2 = stream.map(x -> x+" " +x);

        input.map((MapFunction<String, Object>) s -> {
            throw new Exception();
        });

        stream.map((MapFunction<String, Object>) s -> {
            throw new Exception();
        });

        stream2.map((MapFunction<String, Object>) s -> {
            throw new Exception();
        });

        assertEquals(Collections.singletonList("HELLO"), DataStream.toFunction(stream).apply(Collections.singletonList("hello")));
        assertEquals(Collections.singletonList("HELLO HELLO"), DataStream.toFunction(stream2).apply(Collections.singletonList("hello")));
    }

    @Test
    public void testPartitioning() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        DataStream<String> stream = env.fromElements("hello")
                .shuffle()
                .map(String::toUpperCase);

        assertEquals(Collections.singletonList("HELLO"), DataStream.toFunction(stream).apply(Collections.singletonList("hello")));
    }
}
