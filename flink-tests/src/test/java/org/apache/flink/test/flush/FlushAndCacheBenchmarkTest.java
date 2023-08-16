package org.apache.flink.test.flush;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.apache.flink.configuration.StateBackendOptions.STATE_CACHE_BACKEND;

public class FlushAndCacheBenchmarkTest {
    private static final long NUM_RECORDS = (long)1e7;

    private static final long CHECKPOINT_INTERVAL = 1000;

    private static final long FLUSH_INTERVAL = 1000;

    private static final KeySelector<Long, Long> keySelector0 = x -> System.currentTimeMillis();

    private static final KeySelector<Long, Long> keySelector1 = x -> 1L;

    private static final KeySelector<Long, Long> keySelector2 = x -> x / 500000;

    private static final KeySelector<Long, Long> keySelector3 = x -> x / 5000;

    @Test
    public void testBaseline() throws Exception {
        Configuration config = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.getConfig().enableObjectReuse();
        env.getCheckpointConfig().setCheckpointInterval(CHECKPOINT_INTERVAL);
        env.setParallelism(1);
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        test(env);
    }

    @Test
    public void testBaselineWithHashMapStateBackend() throws Exception {
        Configuration config = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.getConfig().enableObjectReuse();
        env.getCheckpointConfig().setCheckpointInterval(CHECKPOINT_INTERVAL);
        env.setParallelism(1);
        env.setStateBackend(new HashMapStateBackend());
        test(env, keySelector0);
    }

    @Test
    public void testBaselineWithHashMapStateBackend2() throws Exception {
        Configuration config = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.getConfig().enableObjectReuse();
        env.getCheckpointConfig().setCheckpointInterval(CHECKPOINT_INTERVAL);
        env.setParallelism(1);
        env.setStateBackend(new HashMapStateBackend());
        test(env, keySelector2);
    }

    @Test
    public void testBaselineWithHashMapStateBackend3() throws Exception {
        Configuration config = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.getConfig().enableObjectReuse();
        env.getCheckpointConfig().setCheckpointInterval(CHECKPOINT_INTERVAL);
        env.setParallelism(1);
        env.setStateBackend(new HashMapStateBackend());
        test(env, keySelector3);
    }

    @Test
    public void testBaselineWithHashMapStateBackend1() throws Exception {
        Configuration config = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.getConfig().enableObjectReuse();
        env.getCheckpointConfig().setCheckpointInterval(CHECKPOINT_INTERVAL);
        env.setParallelism(1);
        env.setStateBackend(new HashMapStateBackend());
        test(env, keySelector1);
    }

    @Test
    public void testBaselineWithFlushEnabled() throws Exception {
        Configuration config = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.getConfig().enableObjectReuse();
        env.getConfig().setMaxFlushInterval(FLUSH_INTERVAL);
        env.getCheckpointConfig().setCheckpointInterval(CHECKPOINT_INTERVAL);
        env.setParallelism(1);
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        test(env);
    }

    @Test
    public void testBaselineWithFlushAndInMemoryCacheEnabled() throws Exception {
        Configuration config = new Configuration();
        config.set(STATE_CACHE_BACKEND, "hashmap");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.getConfig().enableObjectReuse();
        env.getConfig().setMaxFlushInterval(FLUSH_INTERVAL);
        env.getCheckpointConfig().setCheckpointInterval(CHECKPOINT_INTERVAL);
        env.setParallelism(1);
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        test(env, keySelector0);
    }

    private void test(StreamExecutionEnvironment env) throws Exception {
        test(env, keySelector2);
    }

    private void test(StreamExecutionEnvironment env, KeySelector<Long, Long> keySelector) throws Exception {
        env.fromSequence(0L, NUM_RECORDS)
                .keyBy(keySelector)
                .transform("myOperator", Types.TUPLE(Types.LONG, Types.LONG), new MyOperator())
                .addSink(new DiscardingSink<>());
        JobExecutionResult executionResult = env.execute();
        System.out.println("Total time: " + executionResult.getNetRuntime());
    }

    private static class MyOperator extends AbstractStreamOperator<Tuple2<Long, Long>>
            implements OneInputStreamOperator<Long, Tuple2<Long, Long>> {
        private ValueState<Long> state;
        private ValueStateDescriptor<Long> descriptor;

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);

            descriptor = new ValueStateDescriptor<>("valueState", Types.LONG);
            state = context.getKeyedStateStore().getState(descriptor);
        }

        @Override
        public void processElement(StreamRecord<Long> element) throws IOException {
            if (state.value() == null) {
                state.update(0L);
            } else {
                state.update(state.value() + 1);
            }
            if (getExecutionConfig().getMaxFlushInterval() <= 0) {
                output.collect(new  StreamRecord<>(new Tuple2<>((Long) getCurrentKey(), state.value())));
            }
        }

        @Override
        public void flush() throws Exception {
            getKeyedStateBackend().applyToAllKeysSinceLastFlush(
                    VoidNamespace.INSTANCE,
                    VoidNamespaceSerializer.INSTANCE,
                    descriptor,
                    (key, state) -> output.collect(new StreamRecord<>(new Tuple2<>((Long) key, state.value())))
            );
        }
    }
}
