package org.apache.flink.cep.rule.request;

import org.apache.flink.cep.rule.Rule;

/**
 * Request that calls for {@link Rule} changes.
 *
 * @param <IN> type of input data
 * @param <OUT> type of output data
 */
public interface RuleRequest<IN, OUT> {}
