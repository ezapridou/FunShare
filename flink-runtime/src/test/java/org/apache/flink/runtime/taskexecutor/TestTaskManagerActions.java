/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.taskexecutor;

import net.michaelkoepf.spegauge.api.sut.ReconfigurableSourceData;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.StateMap;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotTable;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerActions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/** Customized {@link TaskManagerActions} that wait for ExecutionState changes. */
public class TestTaskManagerActions implements TaskManagerActions {

    private final JobMasterGateway jobMasterGateway;
    private final TaskSlotTable<Task> taskSlotTable;
    private final TaskManagerActionListeners taskManagerActionListeners =
            new TaskManagerActionListeners();

    public TestTaskManagerActions(
            TaskSlotTable<Task> taskSlotTable, JobMasterGateway jobMasterGateway) {
        this.taskSlotTable = taskSlotTable;
        this.jobMasterGateway = jobMasterGateway;
    }

    public void addListener(
            ExecutionAttemptID eid, ExecutionState executionState, CompletableFuture<Void> future) {
        taskManagerActionListeners.addListener(eid, executionState, future);
    }

    @Override
    public void notifyFatalError(String message, Throwable cause) {}

    @Override
    public void failTask(ExecutionAttemptID executionAttemptID, Throwable cause) {
        if (taskSlotTable != null) {
            taskSlotTable.getTask(executionAttemptID).failExternally(cause);
        }
    }

    @Override
    public void updateTaskExecutionState(TaskExecutionState taskExecutionState) {
        Optional<CompletableFuture<Void>> listenerFuture =
                taskManagerActionListeners.getListenerFuture(
                        taskExecutionState.getID(), taskExecutionState.getExecutionState());
        if (listenerFuture.isPresent()) {
            listenerFuture.get().complete(null);
        }
        if (jobMasterGateway != null) {
            CompletableFuture<Acknowledge> futureAcknowledge =
                    jobMasterGateway.updateTaskExecutionState(taskExecutionState);

            futureAcknowledge.whenComplete(
                    (ack, throwable) -> {
                        if (throwable != null) {
                            failTask(taskExecutionState.getID(), throwable);
                        }
                    });
        }
    }

    private static class TaskManagerActionListeners {
        private final Map<ExecutionAttemptID, List<Tuple2<ExecutionState, CompletableFuture<Void>>>>
                expectExecutionStateAndFutures;

        private TaskManagerActionListeners() {
            this.expectExecutionStateAndFutures = new HashMap<>();
        }

        private void addListener(
                ExecutionAttemptID eid,
                ExecutionState executionState,
                CompletableFuture<Void> future) {
            final List<Tuple2<ExecutionState, CompletableFuture<Void>>>
                    expectExecutionStateAndFutureList =
                            expectExecutionStateAndFutures.getOrDefault(eid, new ArrayList<>());
            expectExecutionStateAndFutureList.add(Tuple2.of(executionState, future));
            expectExecutionStateAndFutures.put(eid, expectExecutionStateAndFutureList);
        }

        private Optional<CompletableFuture<Void>> getListenerFuture(
                ExecutionAttemptID eid, ExecutionState executionState) {
            List<Tuple2<ExecutionState, CompletableFuture<Void>>> expectStateAndFutureList =
                    expectExecutionStateAndFutures.get(eid);
            if (expectExecutionStateAndFutures == null) {
                return Optional.empty();
            }
            for (Tuple2<ExecutionState, CompletableFuture<Void>> expectStateAndFuture :
                    expectStateAndFutureList) {
                if (expectStateAndFuture.f0 == executionState) {
                    return Optional.of(expectStateAndFuture.f1);
                }
            }
            return Optional.empty();
        }
    }

    @Override
    public void sendState(
            int senderId, Map<Integer, Map<String, Map<Integer, StateMap<?, ?, ?>>>> state,
            Map<ResourceID, Map<Integer, Map<Integer, Map<Integer, byte[]>>>> stateForOtherTMs,
            int numOfSenders, int numOfReceivers, Map<Integer, String> stateNames,
            boolean stateMigrationInvolvingMultipleTMs, int numOfSenderTMs, int activeGroupId) {
        // do nothing
    }

    @Override
    public void sendState(
            int senderId, Map<Integer, Map<String, Map<Integer, StateMap<?, ?, ?>>>> state,
            Map<ResourceID, Map<Integer, Map<Integer, Map<Integer, byte[]>>>> stateForOtherTMs,
            Map<Integer, Map<Integer, HashMap<?, ?>>> deduplicationMaps,
            Map<Integer, List<HeapPriorityQueueElement>> triggers,
            Map<ResourceID, Map<Integer, Map<Integer, HashMap<byte[], byte[]>>>> deduplicationMapsForOtherTMs,
            Map<ResourceID, Map<Integer, List<byte[]>>> triggersForOtherTMs,
            int numOfSenders, int numOfReceivers, Map<Integer, String> stateNames,
            boolean stateMigrationInvolvingMultipleTMs, int numOfSenderTMs, int activeGroupId) {
        // do nothing
    }

    @Override
    public void sendStatePassiveQuery(
            int senderId,
            Map<Integer, Map<String, Map<Integer, StateMap<?, ?, ?>>>> state,
            Map<ResourceID, Map<Integer, Map<Integer, Map<Integer, byte[]>>>> stateForOtherTMs,
            Map<Integer, String> stateNames,
            int numOfSenders, int numOfReceivers, int activeGroupId) {
        // do nothing
    }

    @Override
    public void sendReceiverExecutionAttemptID(
            int receiverId, ExecutionAttemptID executionAttemptID,
            int numOfSenders, int numOfReceivers,
            boolean stateMigrationInvolvingMultipleTMs, int numOfSenderTMs,
            int activeGroupId) {
        // do nothing
    }

    @Override
    public void sendStateOfDownstream(
            int senderId, Map<String, StateMap<?, ?, ?>[]> state,
            Map<String, byte[][]> serializedState,
            int numOfSenders, ResourceID receiverTMId,
            boolean multipleTMsInvolvedInMigration,
            int numOfSenderTMs, int activeGroupId) {
        // do nothing
    }

    @Override
    public void sendStateOfDownstream(
            int senderId, Map<String, StateMap<?, ?, ?>[]> state,
            Map<String, byte[][]> serializedState,
            HashMap<?, ?>[] deduplicationMaps,
            HashMap<byte[], byte[]>[] dedupMapsForOtherTMs,
            HeapPriorityQueueElement[] triggers,
            byte[][] triggersForOtherTMs, int queueSize,
            int numOfSenders, ResourceID receiverTMId,
            boolean multipleTMsInvolvedInMigration,
            int numOfSenderTMs, int activeGroupId) {
        // do nothing
    }

    @Override
    public void sendDownstreamReceiverExecutionAttemptID(
            int receiverId, ExecutionAttemptID executionAttemptID, int numOfReceivers, int activeGroupId) {
        // do nothing
    }

    @Override
    public void sendLastTupleData(
            int senderId, ReconfigurableSourceData tupleData,
            int numOfSources, int numOfTMs, int activeGroupId) {
        // do nothing
    }

    @Override
    public void sendSourceTaskExecutionAttemptID(
            int taskId, ExecutionAttemptID executionAttemptID,
            int numOfSources, int numOfTMs, ResourceID taskManagerID, int activeGroupId) {
        // do nothing
    }

    @Override
    public void sendDriverJobIDAndHostname(
            String driverJobID, String hostname, String driverPort,
            Set<ResourceID> taskManagerIDsPassiveQuery, int activeGroupId) {
        // do nothing
    }
}
