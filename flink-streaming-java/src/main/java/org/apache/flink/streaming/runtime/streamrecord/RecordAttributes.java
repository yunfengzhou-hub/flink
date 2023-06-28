package org.apache.flink.streaming.runtime.streamrecord;

public class RecordAttributes extends StreamElement {
    /**
     * If it returns true, then the records received after this element are stale
     * and an operator can optionally buffer records until isBacklog=false. This
     * allows an operator to optimize throughput at the cost of processing latency.
     *
     * <p> It should return true iff any source operator in the upstream
     * reports isProcessingBacklog=true.
     */
    public boolean getIsBacklog() {
        return false;
    }
}
