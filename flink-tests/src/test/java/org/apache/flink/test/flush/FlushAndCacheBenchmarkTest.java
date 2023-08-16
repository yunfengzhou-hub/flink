package org.apache.flink.test.flush;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.StateInitializationContext;
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
    private static final long NUM_RECORDS = (long) 1e7;

    private static final long CHECKPOINT_INTERVAL = 1000;

    private static final long FLUSH_INTERVAL = 100;

    @Test
    public void testRocksDB() throws Exception {
        Configuration config = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.getConfig().enableObjectReuse();
        env.getCheckpointConfig().setCheckpointInterval(CHECKPOINT_INTERVAL);
        env.setParallelism(1);
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        test(env);
    }

    @Test
    public void testHashMap() throws Exception {
        Configuration config = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.getConfig().enableObjectReuse();
        env.getCheckpointConfig().setCheckpointInterval(CHECKPOINT_INTERVAL);
        env.setParallelism(1);
        env.setStateBackend(new HashMapStateBackend());
        test(env);
    }

    @Test
    public void testHashMapAndHashMapCache() throws Exception {
        Configuration config = new Configuration();
        config.set(STATE_CACHE_BACKEND, "hashmap");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.getConfig().enableObjectReuse();
        env.getConfig().setMaxFlushInterval(FLUSH_INTERVAL);
        env.getCheckpointConfig().setCheckpointInterval(CHECKPOINT_INTERVAL);
        env.setParallelism(1);
        env.setStateBackend(new HashMapStateBackend());
        test(env);
    }

    @Test
    public void testRocksDBAndHashMapCache() throws Exception {
        Configuration config = new Configuration();
        config.set(STATE_CACHE_BACKEND, "hashmap");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.getConfig().enableObjectReuse();
        env.getConfig().setMaxFlushInterval(FLUSH_INTERVAL);
        env.getCheckpointConfig().setCheckpointInterval(CHECKPOINT_INTERVAL);
        env.setParallelism(1);
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        test(env);
    }

    @Test
    public void testRocksDBAndRocksDBCache() throws Exception {
        Configuration config = new Configuration();
        config.set(STATE_CACHE_BACKEND, "rocksdb");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.getConfig().enableObjectReuse();
        env.getConfig().setMaxFlushInterval(FLUSH_INTERVAL);
        env.getCheckpointConfig().setCheckpointInterval(CHECKPOINT_INTERVAL);
        env.setParallelism(1);
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        test(env);
    }

    private void test(StreamExecutionEnvironment env) throws Exception {
        env.fromSequence(0L, NUM_RECORDS)
                // mock real workloads where records with the same key tend to appear adjacently.
                .keyBy(x -> System.currentTimeMillis())
                .transform("myOperator", Types.TUPLE(Types.LONG, Types.LONG), new MyOperator())
                .addSink(new DiscardingSink<>());
        JobExecutionResult executionResult = env.execute();
        System.out.println("Total time: " + executionResult.getNetRuntime());
    }

    private static class MyOperator extends AbstractStreamOperator<Tuple2<Long, Long>>
            implements OneInputStreamOperator<Long, Tuple2<Long, Long>> {
        private ValueState<Long> state;

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);

            ValueStateDescriptor<Long> descriptor =
                    new ValueStateDescriptor<>("valueState", Types.LONG);
            state = context.getKeyedStateStore().getState(descriptor);
        }

        @Override
        public void processElement(StreamRecord<Long> element) throws IOException {
            if (state.value() == null) {
                state.update(0L);
            } else {
                state.update(state.value() + 1);
            }
            output.collect(new StreamRecord<>(new Tuple2<>((Long) getCurrentKey(), state.value())));
        }
    }
}
