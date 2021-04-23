package org.apache.flink.streaming.util.functional;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class SingleInputFunction<T, R>  implements Function<T, R> {
    private final Function<List<T>, R> function;

    public SingleInputFunction(
            Function<List<T>, R> function){
        this.function = function;
    }

    @Override
    public R apply(T t) {
        return function.apply(Collections.singletonList(t));
    }
}
