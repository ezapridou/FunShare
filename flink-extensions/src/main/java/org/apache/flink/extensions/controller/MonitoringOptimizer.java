package org.apache.flink.extensions.controller;

import net.michaelkoepf.spegauge.api.sut.DataDistrSplitStats;

import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.extensions.reconfiguration.ReconfigurableExecutionVertex;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public abstract class MonitoringOptimizer extends Optimizer {
    protected void sendTaskUtilStatsMonitoringMessage(
            Set<String> downstreamTasks, Map<String, ExecutionVertex> operatorToExecutionVertexMap,
            ArrayList<CompletableFuture<TaskUtilizationStats>> futures) {
        for (String taskName : downstreamTasks) {
            ExecutionVertex task = operatorToExecutionVertexMap.get(taskName);
            futures.add(((ReconfigurableExecutionVertex) task).sendMonitoringMessage());
        }
    }

    protected void sendThroughputMonitoringMessage(
            Set<String> tasks, Map<String, ExecutionVertex> operatorToExecutionVertexMap,
            ArrayList<CompletableFuture<Double>> futures) {
        for (String taskName: tasks) {
            ExecutionVertex task = operatorToExecutionVertexMap.get(taskName);
            futures.add(((ReconfigurableExecutionVertex) task).sendThroughputMonitoringMessage());
        }
    }

    protected void getSplitPhaseStats(
            Set<String> tasks, Map<String, ExecutionVertex> operatorToExecutionVertexMap,
            ArrayList<CompletableFuture<DataDistrSplitStats>> futures) {
        for (String taskName: tasks) {
            ExecutionVertex task = operatorToExecutionVertexMap.get(taskName);
            futures.add(((ReconfigurableExecutionVertex) task).sendSplitPhaseMonitoringMessage());
        }
    }
}
