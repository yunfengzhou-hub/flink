package org.apache.flink.test.flush;

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
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

import org.junit.jupiter.api.Test;

import static org.apache.flink.configuration.StateBackendOptions.STATE_CACHE_BACKEND;

public class FlushAndCacheITCase {
    @Test
    public void test() throws Exception {
        Configuration config = new Configuration();
        config.set(STATE_CACHE_BACKEND, "hashmap");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.getConfig().setMaxFlushInterval(10);
        //        env.getCheckpointConfig().setCheckpointInterval(10);
        env.setParallelism(1);
        env.setStateBackend(new EmbeddedRocksDBStateBackend());

        env.fromSequence(0L, (long) 1e5)
                .keyBy(x -> x % 100)
                .transform("myOperator", Types.LONG, new MyOperator<>())
                .addSink(new DiscardingSink<>());
        env.execute();
    }

    private static class MyOperator<T> extends AbstractStreamOperator<T>
            implements OneInputStreamOperator<T, T>, BoundedOneInput {
        private boolean isRecordProcessedBeforeFlush = false;

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);

            ValueStateDescriptor<Long> descriptor =
                    new ValueStateDescriptor<>("valueState", Types.LONG);
            ValueState<Long> state = context.getKeyedStateStore().getState(descriptor);
            System.out.println(state.getClass());
            Preconditions.checkState(state instanceof ValueStateWithCache);
        }

        @Override
        public void processElement(StreamRecord<T> element) {
            isRecordProcessedBeforeFlush = true;
            output.collect(element);
        }

        @Override
        public void flush() {
            Preconditions.checkState(isRecordProcessedBeforeFlush);
            isRecordProcessedBeforeFlush = false;
        }

        @Override
        public void endInput() {
            Preconditions.checkState(!isRecordProcessedBeforeFlush);
        }
    }
}
