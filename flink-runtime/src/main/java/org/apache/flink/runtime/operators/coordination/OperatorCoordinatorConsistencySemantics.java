package org.apache.flink.runtime.operators.coordination;

/**
 * The consistency semantics requirement for the communication between an OperatorCoordinator and its subtasks.
 */
public enum OperatorCoordinatorConsistencySemantics {
    /**
     * The default semantics for communications.
     *
     * After failing over, subtasks have all the messages generated before the last checkpoint snapshot,
     * While OperatorCoordinators might lose some of the latest messages.
     */
    DEFAULT,

    /**
     * The exactly-once semantics for communications.
     *
     * Even after failing over, all messages between the OperatorCoordinator and
     * its subtasks would be sent exactly-once.
     *
     * The cost is that messages sent during checkpointing period might not arrive until
     * the checkpoint is completed or aborted, meaning higher latency for these messages.
     *
     * If the communications require request-response semantics, this option should not be used
     * since it might infinitely block the communication channel.
     */
    EXACTLY_ONCE
}
