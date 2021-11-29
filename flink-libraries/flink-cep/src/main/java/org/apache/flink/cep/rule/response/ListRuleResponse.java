package org.apache.flink.cep.rule.response;

import org.apache.flink.cep.rule.Rule;
import org.apache.flink.cep.rule.request.ListRuleRequest;

import java.util.Map;

/**
 * Class that is returned after a {@link ListRuleRequest} is handled.
 *
 * @param <IN> type of input data
 * @param <OUT> type of output data
 */
public class ListRuleResponse<IN, OUT> extends RuleResponse<IN, OUT> {
    /**
     * Rules that actively provide service after the rule request is applied.
     *
     * <p>Key is ID of the rule.
     */
    private Map<String, Rule<IN, OUT>> ruleMap;
}
