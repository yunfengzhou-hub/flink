package org.apache.flink.cep.rule.request;

import org.apache.flink.cep.rule.Rule;

/**
 * Request that describes listing all {@link Rule}s active.
 *
 * @param <IN> type of input data
 * @param <OUT> type of output data
 */
public class ListRuleRequest<IN, OUT> implements RuleRequest<IN, OUT> {
    public ListRuleRequest() {}
}
