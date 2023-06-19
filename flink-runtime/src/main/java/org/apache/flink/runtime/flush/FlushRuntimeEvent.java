package org.apache.flink.runtime.flush;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.RuntimeEvent;

import java.io.IOException;

public class FlushRuntimeEvent extends RuntimeEvent {
    @Override
    public void write(DataOutputView out) throws IOException {
        throw new UnsupportedOperationException("This method should never be called");
    }

    @Override
    public void read(DataInputView in) throws IOException {
        throw new UnsupportedOperationException("This method should never be called");
    }
}
