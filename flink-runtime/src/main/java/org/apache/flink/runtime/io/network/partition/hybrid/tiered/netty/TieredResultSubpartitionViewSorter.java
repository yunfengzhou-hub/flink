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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/** TieredResultSubpartitionViewSorter. */
public class TieredResultSubpartitionViewSorter {
    private static final int CONTINUOUS_BUFFER_THRESHOLD = 8;

    private final Object lock = new Object();

    private final List<
                    Tuple2<
                            Predicate<TieredStorageResultSubpartitionView>,
                            Set<TieredStorageResultSubpartitionView>>>
            activeViews = new ArrayList<>();

    private final Set<TieredStorageResultSubpartitionView> unavailableViews = new HashSet<>();

    private final Set<TieredStorageResultSubpartitionView> finishedViews = new HashSet<>();

    public TieredResultSubpartitionViewSorter(
            Collection<TieredStorageResultSubpartitionView> views) {
        initializeRules();

        views.forEach(this::addOrUpdateView);
    }

    private void initializeRules() {
        addBucket(x -> true);
    }

    private void addBucket(Predicate<TieredStorageResultSubpartitionView> predicate) {
        activeViews.add(Tuple2.of(predicate, new HashSet<>()));
    }

    @Nullable
    public TieredStorageResultSubpartitionView getNextView() {
        synchronized (lock) {
            for (Tuple2<
                            Predicate<TieredStorageResultSubpartitionView>,
                            Set<TieredStorageResultSubpartitionView>>
                    tuple2 : activeViews) {
                if (!tuple2.f1.isEmpty()) {
                    return tuple2.f1.iterator().next();
                }
            }
            return null;
        }
    }

    public int getFinishedSize() {
        synchronized (lock) {
            return finishedViews.size();
        }
    }

    public int getUnfinishedSize() {
        synchronized (lock) {
            int size = unavailableViews.size();
            for (Tuple2<
                            Predicate<TieredStorageResultSubpartitionView>,
                            Set<TieredStorageResultSubpartitionView>>
                    tuple2 : activeViews) {
                size += tuple2.f1.size();
            }
            return size;
        }
    }

    public int getActiveSize() {
        synchronized (lock) {
            int size = 0;
            for (Tuple2<
                            Predicate<TieredStorageResultSubpartitionView>,
                            Set<TieredStorageResultSubpartitionView>>
                    tuple2 : activeViews) {
                size += tuple2.f1.size();
            }
            return size;
        }
    }

    public boolean isViewBest(TieredStorageResultSubpartitionView view) {
        synchronized (lock) {
            for (Tuple2<
                            Predicate<TieredStorageResultSubpartitionView>,
                            Set<TieredStorageResultSubpartitionView>>
                    tuple2 : activeViews) {
                if (!tuple2.f1.isEmpty()) {
                    return tuple2.f1.contains(view);
                }
            }
            throw new IllegalStateException();
        }
    }

    //    private void remove(TieredStorageResultSubpartitionView view) {
    //        synchronized (lock) {
    //            if (finishedViews.contains(view)) {
    //                return;
    //            }
    //
    //            for (Tuple2<
    //                            Predicate<TieredStorageResultSubpartitionView>,
    //                            Set<TieredStorageResultSubpartitionView>>
    //                    tuple2 : activeViews) {
    //                if (tuple2.f1.remove(view)) {
    //                    finishedViews.add(view);
    //                    return;
    //                }
    //            }
    //
    //            throw new IllegalStateException();
    //        }
    //    }

    /** ViewStatus. */
    public enum ViewStatus {
        ACTIVE,
        UNAVAILABLE,
        FINISHED
    }

    public void markViewStatus(TieredStorageResultSubpartitionView view, ViewStatus status) {
        Preconditions.checkNotNull(view);
        synchronized (lock) {
            switch (status) {
                case ACTIVE:
                    unavailableViews.remove(view);
                    addOrUpdateView(view);
                    break;
                case UNAVAILABLE:
                    Preconditions.checkState(!finishedViews.contains(view));
                    unavailableViews.add(view);
                    for (Tuple2<
                                    Predicate<TieredStorageResultSubpartitionView>,
                                    Set<TieredStorageResultSubpartitionView>>
                            tuple2 : activeViews) {
                        if (tuple2.f1.remove(view)) {
                            break;
                        }
                    }
                    break;
                case FINISHED:
                    finishedViews.add(view);
                    unavailableViews.remove(view);
                    for (Tuple2<
                                    Predicate<TieredStorageResultSubpartitionView>,
                                    Set<TieredStorageResultSubpartitionView>>
                            tuple2 : activeViews) {
                        if (tuple2.f1.remove(view)) {
                            break;
                        }
                    }
                    break;
            }
        }
    }

    public void releaseAllResources() throws IOException {
        synchronized (lock) {
            if (getUnfinishedSize() > 0) {
                List<Integer> unavailableCodes =
                        unavailableViews.stream()
                                .map(Object::hashCode)
                                .collect(Collectors.toList());
                List<Integer> activeCodes = new ArrayList<>();
                for (Tuple2<
                                Predicate<TieredStorageResultSubpartitionView>,
                                Set<TieredStorageResultSubpartitionView>>
                        tuple2 : activeViews) {
                    activeCodes.addAll(
                            tuple2.f1.stream().map(Object::hashCode).collect(Collectors.toList()));
                }
                throw new IllegalStateException(
                        "unfinished size "
                                + getUnfinishedSize()
                                + " "
                                + getActiveSize()
                                + " "
                                + unavailableCodes
                                + " "
                                + activeCodes);
            }

            //            Preconditions.checkState(
            //                    getUnfinishedSize() == 0,
            //                    "unfinished size " + getUnfinishedSize()
            //                            + " " + getActiveSize()
            //                            + " " + (unavailableViews.isEmpty()?
            // "[]":unavailableViews.stream().map(Object::hashCode).collect(Collectors.toList()))
            //                            + " " + (activeViews.isEmpty()?
            // "[]":activeViews.stream().map(Object::hashCode).collect(Collectors.toList())));
            for (TieredStorageResultSubpartitionView view : finishedViews) {
                view.releaseAllResources();
            }
        }
    }

    public ViewStatus getViewStatus(TieredStorageResultSubpartitionView view) {
        synchronized (lock) {
            if (finishedViews.contains(view)) {
                return ViewStatus.FINISHED;
            }

            if (unavailableViews.contains(view)) {
                return ViewStatus.UNAVAILABLE;
            }

            for (Tuple2<
                            Predicate<TieredStorageResultSubpartitionView>,
                            Set<TieredStorageResultSubpartitionView>>
                    tuple2 : activeViews) {
                if (tuple2.f1.contains(view)) {
                    return ViewStatus.ACTIVE;
                }
            }

            return null;
        }
    }

    private void addOrUpdateView(TieredStorageResultSubpartitionView view) {
        synchronized (lock) {
            boolean isMatchFound = false;
            if (finishedViews.contains(view)) {
                throw new IllegalStateException(String.valueOf(view.hashCode()));
            }
            Preconditions.checkState(!finishedViews.contains(view));

            for (Tuple2<
                            Predicate<TieredStorageResultSubpartitionView>,
                            Set<TieredStorageResultSubpartitionView>>
                    tuple2 : activeViews) {
                if (!isMatchFound && tuple2.f0.test(view)) {
                    isMatchFound = true;
                    if (tuple2.f1.contains(view)) {
                        return;
                    } else {
                        tuple2.f1.add(view);
                        continue;
                    }
                }

                if (tuple2.f1.contains(view)) {
                    tuple2.f1.remove(view);
                    return;
                }
            }

            Preconditions.checkState(isMatchFound);
        }
    }
}
