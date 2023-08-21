package org.apache.flink.test.flush;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
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
import static org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND;
import static org.apache.flink.configuration.StateBackendOptions.STATE_CACHE_BACKEND;
import static org.apache.flink.configuration.StateBackendOptions.STATE_CACHE_BACKEND_KEY_SIZE;
import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL;

public class FlushAndCacheBenchmarkTest extends TestLogger {
    @TempDir Path tmp;

    private static final long NUM_RECORDS = (long) 1e7;

    private static final Duration CHECKPOINT_INTERVAL = Duration.ofMillis(1000);

    private static final int CACHE_KEY_SIZE = (int) 100;

    private static final int NUM_KEYS = (int) 1e4;

    private Configuration config;

    @BeforeEach
    public void before() throws IOException {
        String checkpointDir = "file://" + Files.createTempDirectory(tmp, "test").toString();
        config = new Configuration();
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

//    @Test
//    public void testHashMap() throws Exception {
//        config.set(STATE_BACKEND, "hashmap");
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
//        test(env);
//    }

//    @Test
//    public void testHashMapAndHashMapCache() throws Exception {
//        Configuration config = new Configuration();
//        config.set(CHECKPOINT_STORAGE, "filesystem");
//        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir);
//        config.set(STATE_CACHE_BACKEND, "hashmap");
//        config.set(STATE_CACHE_BACKEND_KEY_SIZE, CACHE_KEY_SIZE);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
//        env.getConfig().enableObjectReuse();
//        env.getCheckpointConfig().setCheckpointInterval(CHECKPOINT_INTERVAL);
//        env.setParallelism(1);
//        env.setStateBackend(new HashMapStateBackend());
//        test(env);
//    }
//
//    @Test
//    public void testRocksDBAndHashMapCache() throws Exception {
//        Configuration config = new Configuration();
//        config.set(CHECKPOINT_STORAGE, "filesystem");
//        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir);
//        config.set(STATE_CACHE_BACKEND, "hashmap");
//        config.set(STATE_CACHE_BACKEND_KEY_SIZE, CACHE_KEY_SIZE);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
//        env.getConfig().enableObjectReuse();
//        env.getCheckpointConfig().setCheckpointInterval(CHECKPOINT_INTERVAL);
//        env.setParallelism(1);
//        env.setStateBackend(new EmbeddedRocksDBStateBackend());
//        test(env);
//    }
//
    @Test
    public void testRocksDBAndHashMapCache1() throws Exception {
        config.set(STATE_CACHE_BACKEND, "hashmap");
        config.set(STATE_CACHE_BACKEND_KEY_SIZE, 1);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        test(env);
    }

    @Test
    public void testRocksDBAndHashMapCache1000() throws Exception {
        config.set(STATE_CACHE_BACKEND, "hashmap");
        config.set(STATE_CACHE_BACKEND_KEY_SIZE, 1000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        test(env);
    }

    @Test
    public void testRocksDBAndHashMapCache10002() throws Exception {
        config.set(STATE_CACHE_BACKEND, "hashmap");
        config.set(STATE_CACHE_BACKEND_KEY_SIZE, 1000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        test(env);
    }

//    @Test
//    public void testRocksDBAndRocksDBCache() throws Exception {
//        Configuration config = new Configuration();
//        config.set(CHECKPOINT_STORAGE, "filesystem");
//        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir);
//        config.set(STATE_CACHE_BACKEND, "rocksdb");
//        config.set(STATE_CACHE_BACKEND_KEY_SIZE, CACHE_KEY_SIZE);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
//        env.getConfig().enableObjectReuse();
//        env.getCheckpointConfig().setCheckpointInterval(CHECKPOINT_INTERVAL);
//        env.setParallelism(1);
//        env.setStateBackend(new EmbeddedRocksDBStateBackend());
//        test(env);
//    }

    private void test(StreamExecutionEnvironment env) throws Exception {
        env.fromSequence(0L, NUM_RECORDS)
                // mock real workloads where records with the same key tend to appear adjacently.
                .keyBy(x -> x % NUM_KEYS)
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
            Long value = state.value();
            if (value == null) {
                state.update(0L);
            } else {
                state.update(value + 1);
            }
            output.collect(new StreamRecord<>(new Tuple2<>((Long) getCurrentKey(), value)));
        }
    }
}
