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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.StateMap;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;

import net.michaelkoepf.spegauge.api.sut.ReconfigurableSourceData;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Interface for the communication of the {@link Task} with the {@link TaskExecutor}. */
public interface TaskManagerActions {

    /**
     * Notifies the task manager about a fatal error occurred in the task.
     *
     * @param message Message to report
     * @param cause Cause of the fatal error
     */
    void notifyFatalError(String message, Throwable cause);

    /**
     * Tells the task manager to fail the given task.
     *
     * @param executionAttemptID Execution attempt ID of the task to fail
     * @param cause Cause of the failure
     */
    void failTask(ExecutionAttemptID executionAttemptID, Throwable cause);

    /**
     * Notifies the task manager about the task execution state update.
     *
     * @param taskExecutionState Task execution state update
     */
    void updateTaskExecutionState(TaskExecutionState taskExecutionState);

    /**
     * Sends to the task manager state for state migration.
     */
    void sendState(int senderId, Map<Integer, Map<String, Map<Integer, StateMap<?, ?, ?>>>> state,
                   Map<ResourceID, Map<Integer, Map<Integer, Map<Integer, byte[]>>>> stateForOtherTMs,
                   int numOfSenders, int numOfReceivers, Map<Integer, String> stateNames,
                   boolean stateMigrationInvolvingMultipleTMs, int numOfSenderTMs, int activeGroupId);

    /**
     * Sends to the task manager state for state migration.
     * This version is used when the sender is a window operator
     */
    void sendState(int senderId, Map<Integer, Map<String, Map<Integer, StateMap<?, ?, ?>>>> state,
                   Map<ResourceID, Map<Integer, Map<Integer, Map<Integer, byte[]>>>> stateForOtherTMs,
                   Map<Integer, Map<Integer, HashMap<?, ?>>> deduplicationMaps,
                   Map<Integer, List<HeapPriorityQueueElement>> triggers,
                   Map<ResourceID, Map<Integer, Map<Integer, HashMap<byte[], byte[]>>>> deduplicationMapsForOtherTMs,
                   Map<ResourceID, Map<Integer, List<byte[]>>> triggersForOtherTMs,
                   int numOfSenders, int numOfReceivers, Map<Integer, String> stateNames,
                   boolean stateMigrationInvolvingMultipleTMs, int numOfSenderTMs, int activeGroupId);

    /**
     * Sends to the task manager state for state migration.
     * This version is used when the sender belongs to the passive query.
     */
    void sendStatePassiveQuery(int senderId,
                               Map<Integer, Map<String, Map<Integer, StateMap<?, ?, ?>>>> state,
                               Map<ResourceID, Map<Integer, Map<Integer, Map<Integer, byte[]>>>> stateForOtherTMs,
                               Map<Integer, String> stateNames,
                               int numOfSenders, int numOfReceivers, int activeGroupId);

    /**
     * Sends to the task manager state for state migration.
     * This version is used when the sender is a downstream task (state migration happens between
     * different groups)
     */
    void sendStateOfDownstream(int senderId, Map<String, StateMap<?, ?, ?>[]> state,
                               Map<String, byte[][]> serializedState,
                               int numOfSenders, ResourceID receiverTMId,
                               boolean multipleTMsInvolvedInMigration,
                               int numOfSenderTMs, int activeGroupId);

    /**
     * Sends to the task manager state for state migration.
     * This version is used when the receiver is a downstream task (state migration happens between
     * different groups) and a window operator
     */
    void sendStateOfDownstream(int senderId, Map<String, StateMap<?, ?, ?>[]> state,
                               Map<String, byte[][]> serializedState,
                               HashMap<?, ?>[] deduplicationMaps,
                               HashMap<byte[], byte[]>[] dedupMapsForOtherTMs,
                               HeapPriorityQueueElement[] triggers,
                               byte[][] triggersForOtherTMs, int queueSize,
                               int numOfSenders, ResourceID receiverTMId,
                               boolean multipleTMsInvolvedInMigration,
                               int numOfSenderTMs, int activeGroupId);

    /**
     * Informs the task manager who is the receiver for state migration
     */
    void sendReceiverExecutionAttemptID(int receiverId, ExecutionAttemptID executionAttemptID,
                                        int numOfSenders, int numOfReceivers,
                                        boolean stateMigrationInvolvingMultipleTMs, int numOfSenderTMs,
                                        int activeGroupId);

    /**
     * Informs the task manager who is the receiver for state migration
     * This version is used when the receiver is a downstream task (state migration happens between
     * different groups)
     */
    void sendDownstreamReceiverExecutionAttemptID(int receiverId, ExecutionAttemptID executionAttemptID,
                                        int numOfReceivers, int activeGroupId);

    /**
     * Sends to the task manager information about the last tuple processed by a source task before
     * reconfiguration
     */
    void sendLastTupleData(int senderId, ReconfigurableSourceData tupleData, int numOfSources,
                           int numOfTMs, int activeGroupId);

    /**
     * Sends to the task manager the execution attempt ID and index of the sources of the new pipeline
     */
    void sendSourceTaskExecutionAttemptID(int taskId, ExecutionAttemptID executionAttemptID,
                                          int numOfSources, int numOfTMs, ResourceID taskManagerID,
                                          int activeGroupId);

    /**
     * Sends to the task manager the job ID of the driver that must be copied to the new query
     */
    void sendDriverJobIDAndHostname(String driverJobID, String hostname, String driverPort,
                                    Set<ResourceID> taskManagerIDsPassiveQuery,
                                    int activeGroupId);
}
