package org.apache.flink.streaming.api.graph;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import org.junit.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TimestampOptimizeCheckTest {
    @Test
    public void test() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Long> stream = env.fromSequence(1, 5);
        stream = stream.map(x -> x);
        stream.addSink(new DiscardingSink<>());
        assertTrue(StreamingJobGraphGenerator.isTimestampOptimized(env.getStreamGraph()));
    }

    @Test
    public void testWindow() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Long> stream = env.fromSequence(1, 5);
        stream =
                stream.windowAll(EventTimeSessionWindows.withGap(Time.seconds(1)))
                        .reduce((ReduceFunction<Long>) (x, y) -> x + y);
        stream.addSink(new DiscardingSink<>());
        assertFalse(StreamingJobGraphGenerator.isTimestampOptimized(env.getStreamGraph()));
    }

    @Test
    public void testIntervalJoin() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Long> orangeStream = env.fromSequence(1, 5);
        DataStream<Long> greenStream = env.fromSequence(1, 5);

        DataStream<String> resultStream =
                orangeStream
                        .keyBy(x -> x)
                        .intervalJoin(greenStream.keyBy(x -> x))
                        .between(Time.milliseconds(-2), Time.milliseconds(1))
                        .process(
                                new ProcessJoinFunction<Long, Long, String>() {
                                    @Override
                                    public void processElement(
                                            Long left,
                                            Long right,
                                            ProcessJoinFunction<Long, Long, String>.Context ctx,
                                            Collector<String> out) {}
                                });

        resultStream.addSink(new DiscardingSink<>());
        assertFalse(StreamingJobGraphGenerator.isTimestampOptimized(env.getStreamGraph()));
    }

    @Test
    public void testCustomProcessWithoutTimestamp() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Long> stream = env.fromSequence(1, 5);
        stream =
                stream.process(
                        new ProcessFunction<Long, Long>() {
                            @Override
                            public void processElement(
                                    Long value,
                                    ProcessFunction<Long, Long>.Context ctx,
                                    Collector<Long> out) {
                                out.collect(value);
                            }
                        });
        stream.addSink(new DiscardingSink<>());
        assertTrue(StreamingJobGraphGenerator.isTimestampOptimized(env.getStreamGraph()));
    }

    @Test
    public void testCustomProcessWithTimestamp() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Long> stream = env.fromSequence(1, 5);
        stream =
                stream.process(
                        new ProcessFunction<Long, Long>() {
                            @Override
                            public void processElement(
                                    Long value,
                                    ProcessFunction<Long, Long>.Context ctx,
                                    Collector<Long> out) {
                                if (ctx.timestamp() > 0) {
                                    out.collect(value);
                                }
                            }
                        });
        stream.addSink(new DiscardingSink<>());
        assertFalse(StreamingJobGraphGenerator.isTimestampOptimized(env.getStreamGraph()));
    }

    @Test
    public void testAssignTimestampsAndWatermarks() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Long> stream = env.fromSequence(1, 5);
        stream = stream.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps());
        stream.addSink(new DiscardingSink<>());
        assertFalse(StreamingJobGraphGenerator.isTimestampOptimized(env.getStreamGraph()));
    }

    @Test
    public void testAssignNoWatermarks() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Long> stream = env.fromSequence(1, 5);
        stream = stream.assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks());
        stream.addSink(new DiscardingSink<>());
        assertFalse(StreamingJobGraphGenerator.isTimestampOptimized(env.getStreamGraph()));
    }
}
