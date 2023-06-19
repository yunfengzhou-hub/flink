package org.apache.flink.runtime.flush;

import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.source.coordinator.SourceCoordinator;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DefaultFlushCoordinatorContext implements FlushCoordinator.FlushCoordinatorContext {
    private final Map<JobVertexID, ExecutionJobVertex> tasks;

    public DefaultFlushCoordinatorContext(Map<JobVertexID, ExecutionJobVertex> tasks) {
        this.tasks = tasks;
    }

    public void addSourceExecution(Execution execution) {
        System.out.println("addSourceExecution");
        //        sourceExecutions.add(execution);
    }

    @Override
    public CheckpointCoordinator getCheckpointCoordinator() {
        return null;
    }

    @Override
    public Set<SourceCoordinator<?, ?>> getSourceCoordinators() {
        return null;
    }

    @Override
    public Set<Execution> getSourceExecutions() {
        Set<Execution> sourceExecutions = new HashSet<>();
        System.out.println("tasks.values() " + tasks.values().size());
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
        //        return sourceExecutions;
    }
}
