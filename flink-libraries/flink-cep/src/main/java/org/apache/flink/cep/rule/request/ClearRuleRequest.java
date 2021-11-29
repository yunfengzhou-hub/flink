package org.apache.flink.cep.rule.request;

import org.apache.flink.cep.rule.Rule;

/**
 * Request that describes deleting all {@link Rule}s.
 *
 * @param <IN> type of input data
 * @param <OUT> type of output data
 */
public class ClearRuleRequest<IN, OUT> implements RuleRequest<IN, OUT> {
    public ClearRuleRequest() {}
}
