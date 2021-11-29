package org.apache.flink.cep.rule.request;

import org.apache.flink.cep.rule.Rule;

/**
 * Request that describes updating a {@link Rule}.
 *
 * <p>When a rule is replaced, all partially matched results of that rule is deleted.
 *
 * @param <IN> type of input data
 * @param <OUT> type of output data
 */
public class UpdateRuleRequest<IN, OUT> implements RuleRequest<IN, OUT> {
    /**
     * Constructor.
     *
     * @param ruleID The ID of the rule.
     * @param rule The rule to be updated.
     */
    public UpdateRuleRequest(String ruleID, Rule<IN, OUT> rule) {}
}
