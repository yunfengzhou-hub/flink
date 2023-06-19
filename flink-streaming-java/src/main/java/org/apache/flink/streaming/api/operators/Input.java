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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.ControlEvent;
import org.apache.flink.streaming.runtime.streamrecord.FlushEvent;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

/**
 * {@link Input} interface used in {@link MultipleInputStreamOperator}. Most likely you don't want
 * to implement this interface on your own. Instead you can use {@link AbstractInput} and {@link
 * AbstractStreamOperatorV2} to implement {@link MultipleInputStreamOperator}, or just {@link
 * AbstractStreamOperatorV2} to implement {@link OneInputStreamOperator}.
 */
@PublicEvolving
public interface Input<IN> {
    default void processElement(StreamElement element) throws Exception {
        if (element instanceof FlushEvent) {
            processFlushEvent((FlushEvent) element);
            return;
            //        } else if (element instanceof StreamRecord) {
            //            processElement((StreamRecord<IN>) element);
            //            return;
            //        } else if (element instanceof Watermark) {
            //            processWatermark((Watermark) element);
            //            return;
            //        } else if (element instanceof WatermarkStatus) {
            //            processWatermarkStatus((WatermarkStatus) element);
            //            return;
            //        } else if (element instanceof LatencyMarker) {
            //            processLatencyMarker((LatencyMarker) element);
            //            return;
        }
        // TODO: Migrate all StreamElement subclasses to use this method.

        throw new UnsupportedOperationException(element.getClass().getCanonicalName());
    }

    default void flush() {}

    /**
     * Processes one element that arrived on this input of the {@link MultipleInputStreamOperator}.
     * This method is guaranteed to not be called concurrently with other methods of the operator.
     */
    void processElement(StreamRecord<IN> element) throws Exception;

    /**
     * Processes a {@link Watermark} that arrived on the first input of this two-input operator.
     * This method is guaranteed to not be called concurrently with other methods of the operator.
     *
     * @see org.apache.flink.streaming.api.watermark.Watermark
     */
    void processWatermark(Watermark mark) throws Exception;

    /**
     * Processes a {@link WatermarkStatus} that arrived on this input of the {@link
     * MultipleInputStreamOperator}. This method is guaranteed to not be called concurrently with
     * other methods of the operator.
     *
     * @see WatermarkStatus
     */
    void processWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception;

    /**
     * Processes a {@link LatencyMarker} that arrived on the first input of this two-input operator.
     * This method is guaranteed to not be called concurrently with other methods of the operator.
     *
     * @see org.apache.flink.streaming.runtime.streamrecord.LatencyMarker
     */
    void processLatencyMarker(LatencyMarker latencyMarker) throws Exception;

    /**
     * Set the correct key context before processing the {@code record}. Used for example to extract
     * key from the {@code record} and pass that key to the state backends. This method is
     * guaranteed to not be called concurrently with other methods of the operator.
     */
    void setKeyContextElement(StreamRecord<IN> record) throws Exception;

    /**
     * Processes a {@link ControlEvent} that arrived on the input of this operator. See JavaDocs of
     * {@link ControlEvent}'s subclasses for the behaviors each operator should take upon receiving
     * the corresponding control event.
     */
    default void processControlEvent(ControlEvent event) {
        if (event instanceof FlushEvent) {
            // No-op for FlushEvents by default.
            return;
        }

        throw new UnsupportedOperationException();
    }

    /**
     * Processes a {@link FlushEvent} that arrived at this input.
     *
     * <p>If the operator works without buffer and outputs results as soon as possible, this method
     * can be ignored.
     *
     * <p>Otherwise, the operator should trigger calculations on the buffered inputs and forward
     * results to the downstream operators. When the operator finishes processing this event, all
     * output records that could be correctly inferred from previously received input records should
     * have all been flushed downstream.
     *
     * <p>Besides, the operator should also adjust its flushing behavior towards stream records
     * received after this event according to {@link FlushEvent#getFlushStrategy()}. See {@link
     * org.apache.flink.streaming.runtime.streamrecord.FlushStrategy} for a list of strategies an
     * operator should support.
     */
    default void processFlushEvent(FlushEvent flushEvent) {}
}
