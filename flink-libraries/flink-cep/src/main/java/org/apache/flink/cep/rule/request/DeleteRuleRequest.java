package org.apache.flink.cep.rule.request;

import org.apache.flink.cep.rule.Rule;

/**
 * Request that describes deleting a {@link Rule}.
 *
 * <p>When a rule is deleted, all partially matched results of that rule is also deleted.
 *
 * @param <IN> type of input data
 * @param <OUT> type of output data
 */
public class DeleteRuleRequest<IN, OUT> implements RuleRequest<IN, OUT> {
    /**
     * Constructor.
     *
     * @param ruleID The ID of the rule to be deleted.
     */
    public DeleteRuleRequest(String ruleID) {}
}
