package org.apache.flink.test.flush;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.concurrent.atomic.AtomicLong;

/**
 * An aggregation operator with user-defined flush function.
 *
 * <p>The operator will buffer inputs in memory until a flush operation is triggered.
 */
public class MyAggregator extends AbstractStreamOperator<Tuple2<Integer, Long>>
        implements OneInputStreamOperator<Integer, Tuple2<Integer, Long>>, BoundedOneInput {
    private ValueStateDescriptor<Long> bundleDescriptor;
    private ValueState<Long> bundle;
    private ValueState<Long> store;
    private long visits = 0;

    private transient boolean isRecordReceived;

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        store =
                context.getKeyedStateStore()
                        .getState(new ValueStateDescriptor<>("store", Long.class));
        bundleDescriptor = new ValueStateDescriptor<>("bundle", Types.LONG);
        this.bundle = context.getKeyedStateStore().getState(bundleDescriptor);
    }

    @Override
    public void open() throws Exception {
        super.open();
    }

    @Override
    public void processElement(StreamRecord<Integer> element) throws Exception {
        isRecordReceived = true;
        Integer input = element.getValue();
        if (getExecutionConfig().getMaxFlushInterval() > 0) {
            Long bundleValue = bundle.value();
            // get a new value after adding this element to bundle
            Long newBundleValue = bundleValue == null ? 1 : bundleValue + 1;
            bundle.update(newBundleValue);
        } else {
            visits += 1;
            Long storeValue = store.value();
            Long newStoreValue = storeValue == null ? 1 : storeValue + 1;
            store.update(newStoreValue);
            output.collect(new StreamRecord<>(new Tuple2<>(input, newStoreValue)));
        }
    }

    @Override
    public void finish() throws Exception {
        finishBundle();
        System.out.println("visits: " + visits);
    }

    public void finishBundle() throws Exception {
        if (bundle == null) {
            return;
        }

        AtomicLong keyCnt = new AtomicLong(0L);
        getKeyedStateBackend()
                .applyToAllKeys(
                        VoidNamespace.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        bundleDescriptor,
                        new FinishBundleFunction(keyCnt));
        System.out.println("finishBundle " + keyCnt);
        bundle.clear();
    }

    @Override
    public void flush() throws Exception {
        if (!isRecordReceived) {
            return;
        }
        finishBundle();
    }

    @Override
    public void endInput() throws Exception {
        System.out.println("MyAggregator endInput");
    }

    private final class FinishBundleFunction
            implements KeyedStateFunction<Object, ValueState<Long>> {
        private final AtomicLong keyCnt;

        private FinishBundleFunction(AtomicLong keyCnt) {
            this.keyCnt = keyCnt;
        }

        @Override
        public void process(Object key, ValueState<Long> state) throws Exception {
            int k = (int) key;
            long v = state.value();
            keyCnt.getAndIncrement();
            visits += 1;
            setKeyContextElement1(new StreamRecord<>(k));
            Long storeValue = store.value();
            Long newStoreValue = storeValue == null ? v : storeValue + v;
            store.update(newStoreValue);
            output.collect(new StreamRecord<>(new Tuple2<>(k, newStoreValue)));
        }
    }
}
