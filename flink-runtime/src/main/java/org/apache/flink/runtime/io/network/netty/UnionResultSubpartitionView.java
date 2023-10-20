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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredResultSubpartitionViewSorter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageResultSubpartitionView;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * A wrapper to union the output from multiple {@link ResultSubpartitionView}s. <strong>This class
 * can only be used in BATCH execution mode.</strong>
 */
public class UnionResultSubpartitionView
        implements ResultSubpartitionView, BufferAvailabilityListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(UnionResultSubpartitionView.class);

    private final Object lock = new Object();

    private final TieredResultSubpartitionViewSorter sorter;

    private TieredStorageResultSubpartitionView currentView;

    private final BufferAvailabilityListener availabilityListener;

    private final int expectedNumViews;

    private final Deque<ResultSubpartition.BufferAndBacklog> cachedBuffers = new LinkedList<>();

    private final Map<ResultSubpartitionView, Integer> viewSegmentIds = new HashMap<>();

    private boolean isReleased;

    private int sequenceNumber;

    private boolean isLastBufferPartialRecord = false;
    private int numCachedBufferCompleteRecord = 0;

    public UnionResultSubpartitionView(
            int expectedNumViews, BufferAvailabilityListener availabilityListener) {
        List<TieredStorageResultSubpartitionView> tieredViews = new ArrayList<>();
        this.expectedNumViews = expectedNumViews;
        this.sorter = new TieredResultSubpartitionViewSorter(tieredViews);
        this.currentView = null;

        this.isReleased = false;
        this.sequenceNumber = 0;
        this.availabilityListener = availabilityListener;
    }

    private void markViewStatus(
            TieredStorageResultSubpartitionView view,
            TieredResultSubpartitionViewSorter.ViewStatus status) {
        sorter.markViewStatus(view, status);
    }

    @Nullable
    @Override
    public ResultSubpartition.BufferAndBacklog getNextBuffer() throws IOException {
        synchronized (lock) {
            cacheBuffer();
            ResultSubpartition.BufferAndBacklog result = cachedBuffers.pollFirst();
            if (result != null) {
                if (isBufferWithPartialRecord(result.buffer())) {
                    numCachedBufferCompleteRecord--;
                }

                result =
                        new ResultSubpartition.BufferAndBacklog(
                                result.buffer(),
                                cachedBuffers.size(),
                                result.getNextDataType(),
                                sequenceNumber++);
            }

            return result;
        }
    }

    private void cacheBuffer() throws IOException {
        while (numCachedBufferCompleteRecord < 2) {
            ResultSubpartition.BufferAndBacklog buffer = null;

            if (currentView == null) {
                currentView = sorter.getNextView();
            }

            if (currentView != null) {
                buffer = currentView.getNextBuffer();
            }

            if (buffer == null) {
                if (!isLastBufferPartialRecord
                        && currentView != null
                        && (sorter.getFinishedSize() + 1 < expectedNumViews)) {
                    markViewStatus(
                            currentView, TieredResultSubpartitionViewSorter.ViewStatus.UNAVAILABLE);
                    currentView = null;
                }
                break;
            }

            isLastBufferPartialRecord = isBufferWithPartialRecord(buffer.buffer());
            if (isLastBufferPartialRecord) {
                numCachedBufferCompleteRecord++;
            }

            if (buffer.buffer().getDataType() == Buffer.DataType.END_OF_DATA
                    && (sorter.getFinishedSize() + 1 < expectedNumViews)) {
                markViewStatus(currentView, TieredResultSubpartitionViewSorter.ViewStatus.FINISHED);
                currentView = null;
                continue;
            }

            if (buffer.getNextDataType() == Buffer.DataType.END_OF_DATA
                    && (sorter.getFinishedSize() + 1 < expectedNumViews)) {
                markViewStatus(currentView, TieredResultSubpartitionViewSorter.ViewStatus.FINISHED);
                currentView = null;
                buffer =
                        new ResultSubpartition.BufferAndBacklog(
                                buffer.buffer(),
                                buffer.buffersInBacklog(),
                                Buffer.DataType.NONE,
                                buffer.getSequenceNumber());
            }

            if (buffer.buffer().getDataType() == Buffer.DataType.END_OF_SEGMENT) {
                int nextSegmentId = viewSegmentIds.get(currentView) + 1;
                viewSegmentIds.put(currentView, nextSegmentId);
                currentView.notifyRequiredSegmentId(nextSegmentId);
                continue;
            }

            if (buffer.getNextDataType() == Buffer.DataType.END_OF_SEGMENT) {
                buffer =
                        new ResultSubpartition.BufferAndBacklog(
                                buffer.buffer(),
                                buffer.buffersInBacklog(),
                                Buffer.DataType.NONE,
                                buffer.getSequenceNumber());
            }

            if (!cachedBuffers.isEmpty()
                    && cachedBuffers.peekLast().getNextDataType() == Buffer.DataType.NONE) {
                ResultSubpartition.BufferAndBacklog tmpBuffer = cachedBuffers.removeLast();
                tmpBuffer =
                        new ResultSubpartition.BufferAndBacklog(
                                tmpBuffer.buffer(),
                                tmpBuffer.buffersInBacklog(),
                                buffer.buffer().getDataType(),
                                tmpBuffer.getSequenceNumber());
                cachedBuffers.addLast(tmpBuffer);
            }

            if (!isLastBufferPartialRecord
                    && currentView != null
                    && !currentView.isAvailable(true)
                    && (sorter.getFinishedSize() + 1 < expectedNumViews)) {
                markViewStatus(
                        currentView, TieredResultSubpartitionViewSorter.ViewStatus.UNAVAILABLE);
                currentView = null;
            }

            if (!cachedBuffers.isEmpty()) {
                Preconditions.checkState(
                        cachedBuffers.peekLast().getNextDataType()
                                == buffer.buffer().getDataType());
            }

            cachedBuffers.addLast(buffer);
        }
    }

    private boolean isBufferWithPartialRecord(Buffer buffer) {
        // TODO: judge isLastBufferPartialRecord by DataType.DATA_BUFFER_WITH_PARTIAL_RECORD.
        //            return
        //                    !buffer.buffer().getDataType().isEvent() &&
        //  !buffer.getNextDataType().isEvent();
        return buffer.getDataType() == Buffer.DataType.DATA_BUFFER;
    }

    private void printToLog(String str) {
        LOGGER.info(this.hashCode() + " " + str);
    }

    private String getString(ResultSubpartitionView view) {
        if (view == null) {
            return null;
        }

        return "[" + view.hashCode() + " " + view.getBacklog() + " " + view.isAvailable(true) + "]";
    }

    private String getString(ResultSubpartition.BufferAndBacklog result) {
        if (result == null) {
            return "null";
        }

        return "["
                + result.buffer().getDataType().toString()
                + " "
                + result.getNextDataType()
                + " "
                + result.getSequenceNumber()
                + "]";
    }

    @Override
    public void notifyDataAvailable() {
        // Used by pipelined/sort shuffle, which is not supported by this class yet.
        throw new UnsupportedOperationException();
    }

    @Override
    public void notifyDataAvailable(ResultSubpartitionView view) {
        Preconditions.checkNotNull(view);
        viewSegmentIds.putIfAbsent(view, 0);
        synchronized (lock) {
            if (sorter.getViewStatus((TieredStorageResultSubpartitionView) view)
                    == TieredResultSubpartitionViewSorter.ViewStatus.FINISHED) {
                return;
            }

            markViewStatus(
                    (TieredStorageResultSubpartitionView) view,
                    TieredResultSubpartitionViewSorter.ViewStatus.ACTIVE);
            if (!cachedBuffers.isEmpty()) {
                return;
            }

            try {
                cacheBuffer();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            if (cachedBuffers.isEmpty()) {
                return;
            }
        }
        availabilityListener.notifyDataAvailable(this);
    }

    @Override
    public void notifyPriorityEvent(int priorityBufferNumber) {
        // Only used by pipelined shuffle, which is not supported by this class yet.
        throw new UnsupportedOperationException();
    }

    @Override
    public void releaseAllResources() throws IOException {
        if (currentView != null) {
            markViewStatus(currentView, TieredResultSubpartitionViewSorter.ViewStatus.FINISHED);
            currentView = null;
        }
        sorter.releaseAllResources();
        isReleased = true;
    }

    @Override
    public boolean isReleased() {
        return isReleased;
    }

    @Override
    public void resumeConsumption() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void acknowledgeAllDataProcessed() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Throwable getFailureCause() {
        throw new UnsupportedOperationException();
    }

    @Override
    public AvailabilityWithBacklog getAvailabilityAndBacklog(boolean isCreditAvailable) {
        synchronized (lock) {
            try {
                cacheBuffer();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            if (cachedBuffers.isEmpty()) {
                return new AvailabilityWithBacklog(false, 0);
            }

            return new AvailabilityWithBacklog(
                    isCreditAvailable || cachedBuffers.peekFirst().buffer().getDataType().isEvent(),
                    (int)
                            cachedBuffers.stream()
                                    .filter(x -> x.buffer().getDataType().isBuffer())
                                    .count());
        }
    }

    @Override
    public void notifyRequiredSegmentId(int segmentId) {
        // TODO: override this method when remote storage is supported.
        ResultSubpartitionView.super.notifyRequiredSegmentId(segmentId);
    }

    @Override
    public boolean isAvailable(boolean isCreditAvailable) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getBacklog() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int unsynchronizedGetNumberOfQueuedBuffers() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void notifyNewBufferSize(int newBufferSize) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return this.hashCode() + " " + getString(currentView);
    }
}
