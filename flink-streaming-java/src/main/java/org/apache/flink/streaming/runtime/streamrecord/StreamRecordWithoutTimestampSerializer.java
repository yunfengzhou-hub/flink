package org.apache.flink.streaming.runtime.streamrecord;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public final class StreamRecordWithoutTimestampSerializer<T> extends TypeSerializer<StreamElement> {

    private final TypeSerializer<T> typeSerializer;

    public StreamRecordWithoutTimestampSerializer(TypeSerializer<T> serializer) {
        if (serializer instanceof StreamElementSerializer) {
            throw new RuntimeException(
                    "StreamRecordSerializer given to StreamRecordSerializer as value TypeSerializer: "
                            + serializer);
        }
        this.typeSerializer = requireNonNull(serializer);
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public StreamElementSerializer<T> duplicate() {
        throw new UnsupportedOperationException();
    }

    @Override
    public StreamElement createInstance() {
        return new StreamRecord<T>(typeSerializer.createInstance());
    }

    @Override
    public StreamElement copy(StreamElement from) {
        throw new UnsupportedOperationException();
    }

    @Override
    public StreamElement copy(StreamElement from, StreamElement reuse) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof StreamRecordWithoutTimestampSerializer) {
            StreamRecordWithoutTimestampSerializer<?> other =
                    (StreamRecordWithoutTimestampSerializer<?>) obj;

            return typeSerializer.equals(other.typeSerializer);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return typeSerializer.hashCode();
    }

    @Override
    public TypeSerializerSnapshot<StreamElement> snapshotConfiguration() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void serialize(StreamElement value, DataOutputView target) throws IOException {
        typeSerializer.serialize((T) value.asRecord().getValue(), target);
    }

    @Override
    public StreamElement deserialize(DataInputView source) throws IOException {
        return new StreamRecord<>(typeSerializer.deserialize(source));
    }

    @Override
    public StreamElement deserialize(StreamElement reuse, DataInputView source) throws IOException {
        T value = typeSerializer.deserialize(source);
        StreamRecord<T> reuseRecord = reuse.asRecord();
        reuseRecord.replace(value);
        return reuseRecord;
    }
}
