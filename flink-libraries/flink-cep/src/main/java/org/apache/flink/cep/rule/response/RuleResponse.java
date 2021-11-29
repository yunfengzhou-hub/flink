package org.apache.flink.cep.rule.response;

import org.apache.flink.cep.rule.request.RuleRequest;

/**
 * Class that is returned after a {@link RuleRequest} is handled.
 *
 * @param <IN> type of input data
 * @param <OUT> type of output data
 */
public class RuleResponse<IN, OUT> {
    /** ID of the side outputs that are related to rule response. */
    public static final String OUTPUT_TAG_ID = RuleResponse.class.getName();

    /** The request that this response corresponds to. */
    private RuleRequest<IN, OUT> ruleRequest;

    /** Whether the response shows a success or failure. */
    private Status status;

    /**
     * Status showing whether a {@link RuleRequest} successes or not. Can contain detailed error type.
     */
    public enum Status {
        SUCCESS,
        FAIL
    }
}
