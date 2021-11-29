package org.apache.flink.cep.rule.request;

import org.apache.flink.cep.rule.Rule;

/**
 * Request that describes adding a {@link Rule}.
 *
 * @param <IN> type of input data
 * @param <OUT> type of output data
 */
public class AddRuleRequest<IN, OUT> implements RuleRequest<IN, OUT> {
    /**
     * Constructor.
     *
     * @param ruleID The ID of the rule.
     * @param rule The rule to be added.
     */
    public AddRuleRequest(String ruleID, Rule<IN, OUT> rule) {}
}
