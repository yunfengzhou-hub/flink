package org.apache.flink.cep;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.rule.Rule;
import org.apache.flink.cep.rule.request.AddRuleRequest;
import org.apache.flink.cep.rule.request.RuleRequest;
import org.apache.flink.cep.rule.response.RuleResponse;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import org.junit.Test;

import java.util.List;
import java.util.Map;

public class DynamicTest {
    @Test
    public void testDynamic() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Event> dataInput =
                env.fromElements(
                        new Event(1, "barfoo", 1.0),
                        new Event(2, "start", 2.0),
                        new Event(3, "foobar", 3.0),
                        new SubEvent(4, "foo", 4.0, 1.0),
                        new Event(5, "middle", 5.0),
                        new SubEvent(6, "middle", 6.0, 2.0),
                        new SubEvent(7, "bar", 3.0, 3.0),
                        new Event(42, "42", 42.0),
                        new Event(8, "end", 1.0));

        Rule<Event, Object> rule =
                new Rule.Builder<Event, Object>().filter(null).keyBy(null).build();
        String ruleID = "AddRuleRequest";

        RuleRequest<Event, Object> ruleRequest = new AddRuleRequest<>(ruleID, rule);

        DataStream<RuleRequest<Event, Object>> ruleInput = env.fromElements(ruleRequest);

        final OutputTag<RuleResponse<Event, Object>> outputTag =
                new OutputTag<>(RuleResponse.OUTPUT_TAG_ID) {};

        SingleOutputStreamOperator<Object> stream =
                CEP.pattern(dataInput, ruleInput)
                        .process(
                                new PatternProcessFunction<Event, Object>() {
                                    @Override
                                    public void processMatch(
                                            Map<String, List<Event>> match,
                                            Context ctx,
                                            Collector<Object> out)
                                            throws Exception {}
                                });

        DataStream<Object> responseStream =
                stream.getSideOutput(outputTag)
                        .map(
                                new MapFunction<RuleResponse<Event, Object>, Object>() {
                                    @Override
                                    public Object map(
                                            RuleResponse<Event, Object> eventObjectRuleAck)
                                            throws Exception {
                                        return null;
                                    }
                                });

        env.execute();
    }
}
