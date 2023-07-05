/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.streamrecord;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An element in a data stream. Can be a record or a Watermark. */
@Internal
public abstract class StreamElement {
    private static final Logger LOG = LoggerFactory.getLogger(StreamElement.class);

    /**
     * Checks whether this element is a watermark.
     *
     * @return True, if this element is a watermark, false otherwise.
     */
    public final boolean isWatermark() {
        return getClass() == Watermark.class;
    }

    /**
     * Checks whether this element is a watermark status.
     *
     * @return True, if this element is a watermark status, false otherwise.
     */
    public final boolean isWatermarkStatus() {
        return getClass() == WatermarkStatus.class;
    }

    /**
     * Checks whether this element is a record.
     *
     * @return True, if this element is a record, false otherwise.
     */
    public final boolean isRecord() {
        return getClass() == StreamRecord.class;
    }

    /**
     * Checks whether this element is a latency marker.
     *
     * @return True, if this element is a latency marker, false otherwise.
     */
    public final boolean isLatencyMarker() {
        return getClass() == LatencyMarker.class;
    }

    /**
     * Casts this element into a StreamRecord.
     *
     * @return This element as a stream record.
     * @throws java.lang.ClassCastException Thrown, if this element is actually not a stream record.
     */
    @SuppressWarnings("unchecked")
    public final <E> StreamRecord<E> asRecord() {
        return (StreamRecord<E>) this;
    }

    /**
     * Casts this element into a Watermark.
     *
     * @return This element as a Watermark.
     * @throws java.lang.ClassCastException Thrown, if this element is actually not a Watermark.
     */
    public final Watermark asWatermark() {
        return (Watermark) this;
    }

    /**
     * Casts this element into a WatermarkStatus.
     *
     * @return This element as a WatermarkStatus.
     * @throws java.lang.ClassCastException Thrown, if this element is actually not a Watermark
     *     Status.
     */
    public final WatermarkStatus asWatermarkStatus() {
        return (WatermarkStatus) this;
    }

    /**
     * Casts this element into a LatencyMarker.
     *
     * @return This element as a LatencyMarker.
     * @throws java.lang.ClassCastException Thrown, if this element is actually not a LatencyMarker.
     */
    public final LatencyMarker asLatencyMarker() {
        return (LatencyMarker) this;
    }

    public static <T> TypeSerializer<StreamElement> createSerializer(
            TypeSerializer<T> serializer, boolean isOptimized) {
        if (!isOptimized) {
            LOG.info("creating normal stream element serializer");
            return new StreamElementSerializer<>(serializer);
        } else {
            LOG.info("creating optimized stream element serializer");
            return new StreamRecordWithoutTimestampSerializer<>(serializer);
        }
    }
}
