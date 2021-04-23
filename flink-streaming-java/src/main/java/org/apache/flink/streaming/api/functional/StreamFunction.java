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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

class StreamFunction<T, R> implements Function<List<T>, List<R>> {
    private final StreamGraph graph;

    StreamFunction(DataStream<R> dataStream) {
        graph = getStreamGraph(dataStream);
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
    public List<R> apply(List<T> ts) {
        // initializing operators
        List<Runnable> runners = new ArrayList<>();
        Map<Integer, List<List<StreamRecord>>> sourceOutputs = new HashMap<>();
        List<StreamRecord> outputEntry = new ArrayList<>();

        List<StreamNode> nodes = new ArrayList<>(graph.getStreamNodes());

        Map<Integer, EmbedGraphVertex> vertexMap = new HashMap<>();
        Map<Integer, EmbedOutput<StreamRecord>> outputMap = new HashMap<>();

        for(StreamNode node: nodes){
            StreamOperator operator = node.getOperator();

            if(operator instanceof StreamSource){
                sourceOutputs.put(node.getId(), new ArrayList<>());
                continue;
            }

            EmbedGraphVertex vertex = EmbedGraphVertex.createEmbedGraphVertex(node);

            EmbedOutput<StreamRecord> output = new EmbedOutput<>();
            output.addCollector(vertex.getOutput());
            outputMap.put(node.getId(), output);
            outputEntry = vertex.getOutput();

            runners.add(vertex);
            vertexMap.put(node.getId(), vertex);
        }

        try {
            initializeOperators(nodes, outputMap);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        connectVertices(nodes, sourceOutputs, outputMap, vertexMap);

        // begin to process input data
        for(T t:ts){
            for(List<List<StreamRecord>> outputs:sourceOutputs.values()){
                for(List<StreamRecord> output:outputs){
                    output.add(new StreamRecord(t));
                }
            }
        }

        // run vertices in topological order
        for(Runnable runner: runners) {
            runner.run();
        }

        List<R> result = new ArrayList<>();
        for(StreamRecord<R> record:outputEntry){
            result.add(record.getValue());
        }
        return result;
    }

    private static void initializeOperators(List<StreamNode> nodes, Map<Integer, EmbedOutput<StreamRecord>> outputMap) throws Exception {
        StreamTask<?, ?> task = new OneInputStreamTask<>(new EmbedRuntimeEnvironment());
        StreamConfig streamConfig = new StreamConfig(new Configuration());
        streamConfig.setOperatorID(new OperatorID());
        streamConfig.setOperatorName("operator name");

        for(StreamNode node: nodes){
            StreamOperator<?> operator = node.getOperator();

            if(operator instanceof StreamSource){
                continue;
            }

            ((AbstractStreamOperator)operator).setup(task, streamConfig, outputMap.get(node.getId()));
            operator.open();
        }
    }

    private static void connectVertices(
            List<StreamNode> nodes, Map<Integer,
            List<List<StreamRecord>>> sourceOutputs,
            Map<Integer, EmbedOutput<StreamRecord>> outputMap,
            Map<Integer, EmbedGraphVertex> vertexMap){
        for(StreamNode node: nodes){
            for(StreamEdge edge : node.getOutEdges()){
                int sourceId = edge.getSourceId();
                int targetId = edge.getTargetId();

                if(sourceOutputs.containsKey(sourceId)){
                    sourceOutputs.get(sourceId).add(vertexMap.get(targetId).getInput(edge.getTypeNumber()));
                }else{
                    outputMap.get(sourceId).addCollector(vertexMap.get(targetId).getInput(edge.getTypeNumber()));
                }
            }
        }
    }
}
