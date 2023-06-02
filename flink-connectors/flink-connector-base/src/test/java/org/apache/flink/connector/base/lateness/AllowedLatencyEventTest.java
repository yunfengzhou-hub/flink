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
import org.apache.flink.streaming.runtime.streamrecord.AllowedLatencyEvent;
import org.apache.flink.streaming.runtime.streamrecord.AllowedLatencyEventHandler;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.apache.flink.configuration.ExecutionOptions.ALLOWED_LATENCY;

public class AllowedLatencyEventTest {
    @Test
    public void test() throws Exception {
        Configuration configuration = new Configuration().set(ALLOWED_LATENCY, Duration.ZERO);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);
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
                        .returns(Integer.class);

        stream.addSink(new MySinkFunction());

        env.execute();
    }

    private static class MySinkFunction extends RichSinkFunction<Integer>
            implements AllowedLatencyEventHandler {
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println(parameters);
        }

        @Override
        public void invoke(Integer value, Context context) {
            System.out.println("received value " + value);
        }

        @Override
        public void handleAllowedLatencyEvent(AllowedLatencyEvent allowedLatencyEvent) {
            System.out.println(
                    "received allowedLatency " + allowedLatencyEvent.getAllowedLatency());
        }
    }
}
