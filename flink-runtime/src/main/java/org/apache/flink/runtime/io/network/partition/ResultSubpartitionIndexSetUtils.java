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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

/** Utility class to serialize and deserialize {@link ResultSubpartitionIndexSet}s. */
public class ResultSubpartitionIndexSetUtils {
    private static final int TAG_INDEX_RANGE = 0;

    public static void serialize(ResultSubpartitionIndexSet indexSet, ByteBuf target) {
        if (indexSet instanceof ResultSubpartitionIndexRange) {
            target.writeInt(TAG_INDEX_RANGE);
            ResultSubpartitionIndexRange indexRange = (ResultSubpartitionIndexRange) indexSet;
            target.writeInt(indexRange.getStartIndex());
            target.writeInt(indexRange.getEndIndex());
            return;
        }

        throw new UnsupportedOperationException(
                "Unsupported IndexSet type: " + indexSet.getClass());
    }

    public static int getSerializedSize(ResultSubpartitionIndexSet indexSet) {
        if (indexSet instanceof ResultSubpartitionIndexRange) {
            return Integer.BYTES * 3;
        }

        throw new UnsupportedOperationException(
                "Unsupported IndexSet type: " + indexSet.getClass());
    }

    public static ResultSubpartitionIndexSet deserialize(ByteBuf source) {
        int tag = source.readInt();
        if (tag == TAG_INDEX_RANGE) {
            int startIndex = source.readInt();
            int endIndex = source.readInt();
            return new ResultSubpartitionIndexRange(startIndex, endIndex);
        }

        throw new UnsupportedOperationException("Unsupported IndexSet type: " + tag);
    }
}
