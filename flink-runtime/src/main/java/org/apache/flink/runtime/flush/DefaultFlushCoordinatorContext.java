package org.apache.flink.runtime.flush;

import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DefaultFlushCoordinatorContext implements FlushCoordinator.FlushCoordinatorContext {
    private final Map<JobVertexID, ExecutionJobVertex> tasks;

    public DefaultFlushCoordinatorContext(Map<JobVertexID, ExecutionJobVertex> tasks) {
        this.tasks = tasks;
    }

    @Override
    public Set<Execution> getSourceExecutions() {
        Set<Execution> sourceExecutions = new HashSet<>();
        tasks.values()
                .forEach(
                        x -> {
                            if (x.getJobVertex().isInputVertex()) {
                                for (ExecutionVertex vertex : x.getTaskVertices()) {
                                    sourceExecutions.add(vertex.getCurrentExecutionAttempt());
                                }
                            }
                        });
        return sourceExecutions;
    }
}
