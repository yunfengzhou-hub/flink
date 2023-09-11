package org.apache.flink.test.flush;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.TestLogger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import static org.apache.flink.configuration.CheckpointingOptions.CHECKPOINTS_DIRECTORY;
import static org.apache.flink.configuration.CheckpointingOptions.CHECKPOINT_STORAGE;
import static org.apache.flink.configuration.CheckpointingOptions.INCREMENTAL_CHECKPOINTS;
import static org.apache.flink.configuration.CoreOptions.DEFAULT_PARALLELISM;
import static org.apache.flink.configuration.PipelineOptions.OBJECT_REUSE;
import static org.apache.flink.configuration.RestartStrategyOptions.RESTART_STRATEGY;
import static org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND;
import static org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND_CACHE_SIZE;
import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL;

public class FlushAndCacheBenchmarkTest2 extends TestLogger {
    @TempDir Path tmp;

    private static final long NUM_RECORDS = (long) 2e7;

    private static final Duration CHECKPOINT_INTERVAL = Duration.ofMillis(1000);

    private static final int KEY_SIZE_THRESHOLD = 500;

    private static final int CACHE_KEY_SIZE_0 = KEY_SIZE_THRESHOLD / 2;

    private static final int CACHE_KEY_SIZE_50 = KEY_SIZE_THRESHOLD;

    private static final int CACHE_KEY_SIZE_100 = 2 * KEY_SIZE_THRESHOLD;

    private Configuration config;

    @BeforeEach
    public void before() throws IOException {
        String checkpointDir = "file://" + Files.createTempDirectory(tmp, "test").toString();
        config = new Configuration();
        config.set(RESTART_STRATEGY, "none");
        config.set(CHECKPOINT_STORAGE, "filesystem");
        config.set(CHECKPOINTS_DIRECTORY, checkpointDir);
        config.set(OBJECT_REUSE, true);
        config.set(CHECKPOINTING_INTERVAL, CHECKPOINT_INTERVAL);
        config.set(DEFAULT_PARALLELISM, 1);
        config.set(STATE_BACKEND, "rocksdb");
        config.set(INCREMENTAL_CHECKPOINTS, true);
    }

    @Test
    public void testRocksDB() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        test(env);
    }

    @Test
    public void testHashMap() throws Exception {
        config.set(STATE_BACKEND, "hashmap");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        test(env);
    }

    @Test
    public void testRocksDBWithCache0() throws Exception {
        config.set(STATE_BACKEND_CACHE_SIZE, CACHE_KEY_SIZE_0);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        test(env);
    }

    @Test
    public void testRocksDBWithCache50() throws Exception {
        config.set(STATE_BACKEND_CACHE_SIZE, CACHE_KEY_SIZE_50);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        test(env);
    }

    @Test
    public void testRocksDBWithCache100() throws Exception {
        config.set(STATE_BACKEND_CACHE_SIZE, CACHE_KEY_SIZE_100);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        test(env);
    }

    private void test(StreamExecutionEnvironment env) throws Exception {
        String methodName = Thread.currentThread().getStackTrace()[2].getMethodName();
        for (int i = 0; i < 6; i++) {
            env.fromSequence(0L, NUM_RECORDS)
                    .keyBy(new MyKeySelector())
                    .transform("myOperator", Types.TUPLE(Types.LONG, Types.LONG), new MyOperator())
                    .addSink(new DiscardingSink<>());
            JobExecutionResult executionResult = env.execute();
            System.out.println(methodName + "\t" + executionResult.getNetRuntime());
        }
    }

    private static class MyKeySelector implements KeySelector<Long, Long> {
        @Override
        public Long getKey(Long value) {
            // If cache size < threshold, cache hit rate = 0%
            // Else if cache size < 2 * threshold, cache hit rate = 50%
            // Else cache size >= 2 * threshold, cache hit rate = 100%
            long mod = value % KEY_SIZE_THRESHOLD;
            long offset = (value / (2 * KEY_SIZE_THRESHOLD)) % 2;
            return mod + KEY_SIZE_THRESHOLD * offset;
        }
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
            Long value = state.value();
            if (value == null) {
                value = 0L;
            }
            value += 1;
            state.update(value);
            output.collect(new StreamRecord<>(new Tuple2<>((Long) getCurrentKey(), value)));
        }
    }
}
