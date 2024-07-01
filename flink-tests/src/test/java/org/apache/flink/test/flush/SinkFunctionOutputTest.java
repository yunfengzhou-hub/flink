package org.apache.flink.test.flush;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import org.junit.Test;

public class SinkFunctionOutputTest {
    @Test
    public void test() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromSequence(0, 10).addSink(new MySinkFunction<>());
        env.execute();
    }

    private static class MySinkFunction<T> implements SinkFunction<T> {
        @Override
        public void invoke(T value, Context context) throws Exception {
            SinkFunction.super.invoke(value, context);
        }
    }
}
