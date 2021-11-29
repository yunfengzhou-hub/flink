package org.apache.flink.cep.rule;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;

import javax.annotation.Nullable;

/**
 * Base class for a rule definition.
 *
 * <p>A rule refers to a certain {@link Pattern}, how the pattern is matched, and what to do after
 * the pattern is matched.
 *
 * @param <IN> type of input data
 * @param <OUT> type of output data
 */
@PublicEvolving
public class Rule<IN, OUT> {
    /** The pattern to be matched. */
    private Pattern<IN, ?> pattern;

    /**
     * Function that selects input data that needs to match the pattern.
     *
     * <p>If not set, the default behavior is to select all input data.
     */
    @Nullable private FilterFunction<IN> filter;

    /**
     * Function that gets the key of input data.
     *
     * <p>Only data with the same key can be chained to match a pattern. If not set, the default
     * behavior is to reuse current key if is KeyedStream, or allocate the same key for all input
     * data if is not KeyedStream.
     */
    @Nullable private KeySelector<IN, Object> keySelector;

    /**
     * The pattern select function which is called for each detected pattern sequence.
     *
     * <p>If not set, the default behavior is to do nothing on a matched sequence.
     */
    @Nullable private PatternProcessFunction<IN, OUT> patternProcessFunction;

    /**
     * Builder for constructing a {@link Rule}.
     *
     * @param <IN> type of input data
     * @param <OUT> type of output data
     */
    public static class Builder<IN, OUT> {
        /** Set the pattern of this rule. */
        public Builder<IN, OUT> pattern(Pattern<IN, ?> pattern) {
            return this;
        }

        /** Set the keySelector of this rule. */
        public Builder<IN, OUT> keyBy(KeySelector<IN, ?> keySelector) {
            return this;
        }

        /** Set the filterFunction of this rule. */
        public Builder<IN, OUT> filter(FilterFunction<IN> filter) {
            return this;
        }

        /** Set the patternProcessFunction of this rule. */
        public Builder<IN, OUT> process(PatternProcessFunction<IN, OUT> patternProcessFunction) {
            return this;
        }

        /** Generate the corresponding rule. */
        public Rule<IN, OUT> build() {
            return new Rule<>();
        }
    }
}
