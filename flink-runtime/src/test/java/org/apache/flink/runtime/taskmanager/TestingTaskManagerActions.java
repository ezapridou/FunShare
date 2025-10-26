/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.taskmanager;

import net.michaelkoepf.spegauge.api.sut.ReconfigurableSourceData;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.StateMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/** Testing implementation of {@link TaskManagerActions}. */
public class TestingTaskManagerActions implements TaskManagerActions {

    private final BiConsumer<String, Throwable> notifyFatalErrorConsumer;

    private final BiConsumer<ExecutionAttemptID, Throwable> failTaskConsumer;

    private final Consumer<TaskExecutionState> updateTaskExecutionStateConsumer;

    private TestingTaskManagerActions(
            BiConsumer<String, Throwable> notifyFatalErrorConsumer,
            BiConsumer<ExecutionAttemptID, Throwable> failTaskConsumer,
            Consumer<TaskExecutionState> updateTaskExecutionStateConsumer) {
        this.notifyFatalErrorConsumer = notifyFatalErrorConsumer;
        this.failTaskConsumer = failTaskConsumer;
        this.updateTaskExecutionStateConsumer = updateTaskExecutionStateConsumer;
    }

    @Override
    public void notifyFatalError(String message, Throwable cause) {
        notifyFatalErrorConsumer.accept(message, cause);
    }

    @Override
    public void failTask(ExecutionAttemptID executionAttemptID, Throwable cause) {
        failTaskConsumer.accept(executionAttemptID, cause);
    }

    @Override
    public void updateTaskExecutionState(TaskExecutionState taskExecutionState) {
        updateTaskExecutionStateConsumer.accept(taskExecutionState);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {

        private BiConsumer<String, Throwable> notifyFatalErrorConsumer = (ignoredA, ignoredB) -> {};
        private BiConsumer<ExecutionAttemptID, Throwable> failTaskConsumer =
                (ignoredA, ignoredB) -> {};
        private Consumer<TaskExecutionState> updateTaskExecutionStateConsumer = ignored -> {};

        private Builder() {}

        public Builder setNotifyFatalErrorConsumer(
                BiConsumer<String, Throwable> notifyFatalErrorConsumer) {
            this.notifyFatalErrorConsumer = notifyFatalErrorConsumer;
            return this;
        }

        public Builder setFailTaskConsumer(
                BiConsumer<ExecutionAttemptID, Throwable> failTaskConsumer) {
            this.failTaskConsumer = failTaskConsumer;
            return this;
        }

        public Builder setUpdateTaskExecutionStateConsumer(
                Consumer<TaskExecutionState> updateTaskExecutionStateConsumer) {
            this.updateTaskExecutionStateConsumer = updateTaskExecutionStateConsumer;
            return this;
        }

        public TestingTaskManagerActions build() {
            return new TestingTaskManagerActions(
                    notifyFatalErrorConsumer, failTaskConsumer, updateTaskExecutionStateConsumer);
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
