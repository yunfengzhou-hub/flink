package org.apache.flink.connector.base;

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

public class FlushTest {
    @Test
    public void test() throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.disableOperatorChaining();
        int numSplits = 1;
        int numRecordsPerSplit = 5;

        Source source =
                HybridSource.builder(
                                new MockBaseSource(
                                        numSplits, numRecordsPerSplit, Boundedness.BOUNDED))
                        .addSource(
                                new MockBaseSource(
                                        numSplits, numRecordsPerSplit, 5, Boundedness.BOUNDED))
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
}
