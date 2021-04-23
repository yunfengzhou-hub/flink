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

package org.apache.flink.streaming.api.functional;

import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayList;
import java.util.List;

class TwoInputEmbedGraphVertex extends EmbedGraphVertex {
    private final TwoInputStreamOperator operator;
    protected final List<StreamRecord> input1 = new ArrayList<>();
    protected final List<StreamRecord> input2 = new ArrayList<>();

    public TwoInputEmbedGraphVertex(
            int id,
            TwoInputStreamOperator operator){
        super(id);
        this.operator = operator;
    }

    @Override
    public List<StreamRecord> getInput(int typeNumber) {
        if(typeNumber == 1){
            return input1;
        }else if(typeNumber == 2){
            return input2;
        }else{
            throw new RuntimeException(String.format("Illegal typeNumber: %d", typeNumber));
        }
    }

    @Override
    public void run() {
        for(StreamRecord record : input1){
            try {
                operator.processElement1(record);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        for(StreamRecord record : input2){
            try {
                operator.processElement2(record);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
