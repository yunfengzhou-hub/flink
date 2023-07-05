package org.apache.flink.streaming.test;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;

import java.time.Duration;

public class StreamRecordBenchmark {

    public static void main(String[] args) throws Exception {
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

    private static void benchmark() throws Exception {
        try (StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createRemoteEnvironment("localhost", 8082)) {

            env.disableOperatorChaining();
            DataStream<Long> stream = env.fromSequence(1, 500000000L);
            for (int i = 0; i < 10; i++) {
                stream = stream.map(x -> x);
            }
            stream.addSink(new DiscardingSink<>());
            env.execute();
        }
    }
}
