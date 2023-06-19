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

package org.apache.flink.connector.base.lateness;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.connector.base.source.reader.mocks.MockBaseSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.apache.flink.configuration.ExecutionOptions.ALLOWED_LATENCY;

public class FlushEventTest {
    @Test
    public void test() throws Exception {
        Configuration configuration =
                new Configuration().set(ALLOWED_LATENCY, Duration.ofSeconds(2));
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(2);
        //        env.enableCheckpointing(100);
        int numSplits = 1;
        int numRecordsPerSplit = 5;

        Source source =
                HybridSource.builder(
                                new MockBaseSource(
                                        numSplits, numRecordsPerSplit, Boundedness.BOUNDED))
                        .addSource(
                                new MockBaseSource(
                                        numSplits, numRecordsPerSplit, 5, Boundedness.BOUNDED))
                        .setAllowedLatencyForLastSource(Duration.ZERO)
                        .build();

        DataStream<Integer> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "My Source")
                        .returns(Integer.class)
                        .map(x -> x)
                        .disableChaining();

        stream.addSink(new MySinkFunction()).setParallelism(3);

        env.execute();
    }

    private static class MySinkFunction extends RichSinkFunction<Integer> {
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }

        @Override
        public void invoke(Integer value, Context context) {
            System.out.println("received value " + value);
        }

        @Override
        public void flush() {
            System.out.println("flush()");
        }
    }

    private static void print(Number number) {
        System.out.println("Number: " + number);
    }

    private static void print(Long number) {
        System.out.println("Long: " + number);
    }

    public static void main(String[] args) {
        print(1L);
        print(2L);
        Number number = 3L;
        print(number);
    }
}
