package org.apache.flink.connector.base.lateness;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.connector.base.source.reader.mocks.MockBaseSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.runtime.streamrecord.AllowedLatenessEvent;
import org.apache.flink.streaming.runtime.streamrecord.AllowedLatenessEventHandler;

import org.junit.jupiter.api.Test;

public class AllowedLatenessEventTest {
    @Test
    public void test() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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

    private static class MySinkFunction
            implements SinkFunction<Integer>, AllowedLatenessEventHandler {
        @Override
        public void invoke(Integer value, Context context) {
            System.out.println("received value " + value);
        }

        @Override
        public void handleAllowedLatenessEvent(AllowedLatenessEvent allowedLatenessEvent) {
            System.out.println(
                    "received allowedLatenessEvent " + allowedLatenessEvent.getAllowedLateness());
        }
    }
}
