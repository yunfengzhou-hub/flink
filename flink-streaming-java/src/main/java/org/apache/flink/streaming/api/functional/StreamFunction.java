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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functional.environment.EmbedRuntimeEnvironment;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

class StreamFunction<T, R> implements Function<List<T>, List<R>> {
    private final DataStream<R> dataStream;

    StreamFunction(DataStream<R> dataStream) {
        this.dataStream = dataStream;
        StreamGraph graph = getStreamGraph(dataStream);
        validateGraph(graph);
    }

    private static StreamGraph getStreamGraph(DataStream<?> dataStream){
        StreamExecutionEnvironment env = dataStream.getExecutionEnvironment();

        StreamGraphGenerator generator = new StreamGraphGenerator(
                Collections.singletonList(dataStream.getTransformation()),
                env.getConfig(),
                env.getCheckpointConfig(),
                new Configuration());

        generator.setRuntimeExecutionMode(RuntimeExecutionMode.BATCH)
                .setStateBackend(env.getStateBackend())
                .setSavepointDir(env.getDefaultSavepointDirectory())
                .setChaining(env.isChainingEnabled())
                .setUserArtifacts(env.getCachedFiles())
                .setDefaultBufferTimeout(env.getBufferTimeout());

        return generator.setJobName("Stream Function").generate();
    }

    private static void validateGraph(StreamGraph graph) {
        List<StreamNode> nodes = new ArrayList<>(graph.getStreamNodes());

        int sourceCount = 0;
        for(StreamNode node:nodes){
            StreamOperator<?> operator = node.getOperator();

            if(operator instanceof StreamSource){
                sourceCount ++;
                if(sourceCount > 1){
                    throw new IllegalArgumentException(String.format("%s only allows one single stream source.", StreamFunction.class));
                }
                continue;
            }

            if(!(operator instanceof AbstractStreamOperator)){
                throw new IllegalArgumentException(String.format("%s only supports %s. %s is not supported yet.",
                        StreamFunction.class, AbstractStreamOperator.class, AbstractStreamOperatorV2.class));
            }

            if(operator instanceof AbstractUdfStreamOperator){
                if(((AbstractUdfStreamOperator<?, ?>) operator).getUserFunction() instanceof RichFunction){
                    throw new IllegalArgumentException("Stateful/Rich functions are not supported yet.");
                }
            }

        }
    }

    /**
     * Applies the computation logic of stored StreamGraph to the input data. Output is collected and returned.
     *
     * @param ts input data of the StreamGraph.
     * @return result after applying the computation logic to input data.
     */
    @Override
    public List<R> apply(List<T> ts){
        StreamGraph graph = getStreamGraph(dataStream);
        List<StreamNode> nodes = new ArrayList<>(graph.getStreamNodes());
        Map<Integer, List<StreamRecord>> nodeOutputs = new HashMap<>();
        List<StreamRecord> outputList = new ArrayList<>();
        topologicalSort(nodes);

        StreamTask<?, ?> task;
        try {
            task = new OneInputStreamTask<>(new EmbedRuntimeEnvironment());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        StreamConfig streamConfig = new StreamConfig(new Configuration());
        streamConfig.setOperatorID(new OperatorID());
        streamConfig.setOperatorName("operator name");

        for(StreamNode node: nodes) {
            outputList = new ArrayList<>();
            nodeOutputs.put(node.getId(), outputList);
            StreamOperator operator = node.getOperator();

            if (operator instanceof StreamSource) {
                for(T t:ts){
                    outputList.add(new StreamRecord<>(t));
                }
                continue;
            }

            EmbedGraphVertex vertex = EmbedGraphVertex.createEmbedGraphVertex(node);
            vertex.setOutput(outputList);

            EmbedOutput<StreamRecord> output = new EmbedOutput<>();
            output.addCollector(outputList);

            ((AbstractStreamOperator)operator).setup(task, streamConfig, output);
            try {
                operator.open();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            for(StreamEdge edge : node.getInEdges()){
                int sourceId = edge.getSourceId();

                for(StreamRecord record:nodeOutputs.get(sourceId)){
                    vertex.getInput(edge.getTypeNumber()).add(new StreamRecord(record.getValue()));
                }
            }

            vertex.run();
        }

        List<R> result = new ArrayList<>();
        for(StreamRecord r:outputList){
            result.add((R) r.getValue());
        }
        return result;
    }

    private void topologicalSort(List<StreamNode> nodes){
        nodes.sort(Comparator.comparingInt(StreamNode::getId));
    }
}
