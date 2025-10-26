package org.apache.flink.extensions.reconfiguration;

import net.michaelkoepf.spegauge.api.sut.DataDistrSplitStats;

import org.apache.flink.extensions.controller.ControlMessage;
import org.apache.flink.extensions.controller.TaskUtilizationStats;

import java.util.concurrent.CompletableFuture;

/**
 * Interface that makes contained method available to the controller package.
 * TODO: We could also use reflection to call the methods, but I need to check how that works in Scala.
 */
public interface ReconfigurableExecutionVertex {
    CompletableFuture<?> sendControlMessage(ControlMessage message);

    CompletableFuture<TaskUtilizationStats> sendMonitoringMessage();

    CompletableFuture<Double> sendThroughputMonitoringMessage();

    CompletableFuture<DataDistrSplitStats> sendSplitPhaseMonitoringMessage();
}
