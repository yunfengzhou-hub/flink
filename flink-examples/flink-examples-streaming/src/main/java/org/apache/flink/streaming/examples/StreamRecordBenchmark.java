package org.apache.flink.streaming.examples;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class StreamRecordBenchmark {

    public static void main(String[] args) throws Exception {
        for (int i = 0; i < 5; i ++) {
            benchmark(false);
        }
        for (int i = 0; i < 5; i ++) {
            benchmark(true);
        }
    }


    private static void poc() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(PipelineOptions.AUTO_WATERMARK_INTERVAL, Duration.ZERO);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.disableOperatorChaining();
        //        env.getConfig().setAutoWatermarkInterval(0);
        DataStream<Long> stream = env.fromSequence(1, 5);
        stream = stream.map(x -> x);
        stream.addSink(new PrintSinkFunction<>());
        env.execute();
    }

    private static void benchmark(boolean isEmittingRecordWithTimestamp) throws Exception {
        final int NUM_RECORDS = 500000000;
        try (StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment()) {
            env.getConfig().setAutoWatermarkInterval(0);
            env.getConfig().setOperatorTimestamp(false);
            env.getConfig().enableObjectReuse();
            env.disableOperatorChaining();
            DataStream<Boolean> stream = env.fromSequence(0, NUM_RECORDS).map(x -> true);
            for (int i = 0; i < 10; i++) {
                stream = stream.process(new MyProcessFunction<>(isEmittingRecordWithTimestamp));
            }
            stream.addSink(new DiscardingSink<>());
            long startTime = System.currentTimeMillis();
            env.execute();
            long endTime = System.currentTimeMillis();
            System.out.printf(
                    "Record serialization enabled: %s\t Num records: %s execution time: %s\n",
                    !isEmittingRecordWithTimestamp,
                    NUM_RECORDS,
                    endTime - startTime);
        }
    }

    public static class MyProcessFunction<T> extends ProcessFunction<T, T> {
        private final boolean isEmittingRecordWithTimestamp;

        private MyProcessFunction(boolean isEmittingRecordWithTimestamp) {
            this.isEmittingRecordWithTimestamp = isEmittingRecordWithTimestamp;
        }

        @Override
        public void processElement(
                T value,
                ProcessFunction<T, T>.Context ctx,
                Collector<T> out) {
            out.collect(value);
        }

        @Override
        public boolean isEmittingRecordWithTimestamp() {
            return isEmittingRecordWithTimestamp;
        }
    }
}
