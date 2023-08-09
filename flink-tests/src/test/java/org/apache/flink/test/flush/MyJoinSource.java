package org.apache.flink.test.flush;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.concurrent.TimeUnit;

/** A parallel source to generate data stream for joining operation. */
public class MyJoinSource extends RichParallelSourceFunction<Integer> {
    private long numValues;
    private long numValuesOnThisTask;
    private int numPreGeneratedData;
    private long pause;

    public MyJoinSource(long numValues, long pause) {
        this.numValues = numValues;
        this.pause = pause;
    }

    @Override
    public final void open(Configuration parameters) throws Exception {
        super.open(parameters);
        int taskIdx = getRuntimeContext().getIndexOfThisSubtask();
        int numTasks = getRuntimeContext().getNumberOfParallelSubtasks();
        long div = numValues / numTasks;
        long mod = numValues % numTasks;
        numValuesOnThisTask = mod > taskIdx ? div + 1 : div;
        //        numPreGeneratedData = (int) Math.min(Math.max(numValues / 50, 10), 100000000);
        numPreGeneratedData = (int) 1e6;
    }

    @Override
    public void run(SourceContext ctx) throws Exception {
        long cnt = 0;
        while (cnt < this.numValuesOnThisTask) {
            if (cnt % pause == 0) {
                TimeUnit.MILLISECONDS.sleep(100);
            }
            ctx.collect((int) (cnt % this.numPreGeneratedData));
            cnt += 1;
        }
    }

    @Override
    public void cancel() {}
}
