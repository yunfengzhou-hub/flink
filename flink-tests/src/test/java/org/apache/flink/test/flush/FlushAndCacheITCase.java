package org.apache.flink.test.flush;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.cache.ValueStateWithCache;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

import org.junit.jupiter.api.Test;

import static org.apache.flink.configuration.StateBackendOptions.STATE_CACHE_BACKEND;
import static org.apache.flink.configuration.StateBackendOptions.STATE_CACHE_BACKEND_KEY_SIZE;

public class FlushAndCacheITCase {
    @Test
    public void test() throws Exception {
        Configuration config = new Configuration();
        config.set(STATE_CACHE_BACKEND, "hashmap");
        config.set(STATE_CACHE_BACKEND_KEY_SIZE, 100);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        //        env.getConfig().setCacheBackendKeySize(10);
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.getCheckpointConfig().setCheckpointInterval(10);
        env.setParallelism(1);
        env.setStateBackend(new EmbeddedRocksDBStateBackend());

        env.fromSequence(0L, (long) 1e2)
                .keyBy(x -> x % 100)
                .transform("myOperator", Types.LONG, new MyOperator<>())
                .addSink(new DiscardingSink<>());
        env.execute();
    }

    private static class MyOperator<T> extends AbstractStreamOperator<T>
            implements OneInputStreamOperator<T, T> {
        private boolean isRecordProcessedBeforeFlush = false;

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);

            ValueStateDescriptor<Long> descriptor =
                    new ValueStateDescriptor<>("valueState", Types.LONG);
            ValueState<Long> state = context.getKeyedStateStore().getState(descriptor);
            Preconditions.checkState(state instanceof ValueStateWithCache);
        }

        @Override
        public void processElement(StreamRecord<T> element) {
            System.out.println("processElement " + element.getValue());
            isRecordProcessedBeforeFlush = true;
            output.collect(element);
        }

        //        @Override
        //        public void flush() {
        //            System.out.println("flush");
        //            Preconditions.checkState(isRecordProcessedBeforeFlush);
        //            isRecordProcessedBeforeFlush = false;
        //        }
        //
        //        @Override
        //        public void endInput() {
        //            System.out.println("endInput");
        //            Preconditions.checkState(!isRecordProcessedBeforeFlush);
        //        }
    }
}
