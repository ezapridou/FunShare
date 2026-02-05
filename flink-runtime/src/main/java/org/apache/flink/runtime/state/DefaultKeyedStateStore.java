/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackend;
import org.apache.flink.runtime.state.heap.StateMap;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Default implementation of KeyedStateStore that currently forwards state registration to a {@link
 * RuntimeContext}.
 */
public class DefaultKeyedStateStore implements KeyedStateStore {

    protected final KeyedStateBackend<?> keyedStateBackend;
    protected final ExecutionConfig executionConfig;

    public DefaultKeyedStateStore(
            KeyedStateBackend<?> keyedStateBackend, ExecutionConfig executionConfig) {
        this.keyedStateBackend = Preconditions.checkNotNull(keyedStateBackend);
        this.executionConfig = Preconditions.checkNotNull(executionConfig);
    }

    @Override
    public <T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties) {
        requireNonNull(stateProperties, "The state properties must not be null");
        try {
            stateProperties.initializeSerializerUnlessSet(executionConfig);
            return getPartitionedState(stateProperties);
        } catch (Exception e) {
            throw new RuntimeException("Error while getting state", e);
        }
    }

    @Override
    public <T> ListState<T> getListState(ListStateDescriptor<T> stateProperties) {
        requireNonNull(stateProperties, "The state properties must not be null");
        try {
            stateProperties.initializeSerializerUnlessSet(executionConfig);
            ListState<T> originalState = getPartitionedState(stateProperties);
            return new UserFacingListState<>(originalState);
        } catch (Exception e) {
            throw new RuntimeException("Error while getting state", e);
        }
    }

    @Override
    public <T> ReducingState<T> getReducingState(ReducingStateDescriptor<T> stateProperties) {
        requireNonNull(stateProperties, "The state properties must not be null");
        try {
            stateProperties.initializeSerializerUnlessSet(executionConfig);
            return getPartitionedState(stateProperties);
        } catch (Exception e) {
            throw new RuntimeException("Error while getting state", e);
        }
    }

    @Override
    public <IN, ACC, OUT> AggregatingState<IN, OUT> getAggregatingState(
            AggregatingStateDescriptor<IN, ACC, OUT> stateProperties) {
        requireNonNull(stateProperties, "The state properties must not be null");
        try {
            stateProperties.initializeSerializerUnlessSet(executionConfig);
            return getPartitionedState(stateProperties);
        } catch (Exception e) {
            throw new RuntimeException("Error while getting state", e);
        }
    }

    @Override
    public <UK, UV> MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV> stateProperties) {
        requireNonNull(stateProperties, "The state properties must not be null");
        try {
            stateProperties.initializeSerializerUnlessSet(executionConfig);
            MapState<UK, UV> originalState = getPartitionedState(stateProperties);
            return new UserFacingMapState<>(originalState);
        } catch (Exception e) {
            throw new RuntimeException("Error while getting state", e);
        }
    }

    protected <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor)
            throws Exception {
        return keyedStateBackend.getPartitionedState(
                VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescriptor);
    }

    public void printState(String taskName) {
        if (keyedStateBackend instanceof HeapKeyedStateBackend) {
            ((HeapKeyedStateBackend) keyedStateBackend).printState(taskName);
        }
        else{
            throw new UnsupportedOperationException("The keyedStateBackend is not HeapKeyedStateBackend");
        }
    }

    public Map<Integer, Map<String, Map<Integer, StateMap<?, ?, ?>>>> getEntriesForMigration(
            int newDOP, int taskIndex, int maxParallelism,
            Map<Integer, ResourceID> partitionToResourceIDMap,
            Map<ResourceID, Map<Integer, Map<Integer, Map<Integer, byte[]>>>> stateForOtherTMs,
            Map<Integer, String> stateNameDict, List<Integer> partitionIdToTaskId){
        if (keyedStateBackend instanceof HeapKeyedStateBackend) {
            return ((HeapKeyedStateBackend) keyedStateBackend).getEntriesForMigration(
                    newDOP, taskIndex, maxParallelism, partitionToResourceIDMap, stateForOtherTMs,
                    stateNameDict, partitionIdToTaskId);
        }
        else{
            throw new UnsupportedOperationException("The keyedStateBackend is not HeapKeyedStateBackend");
        }
    }

    public Map<Integer, Map<String, Map<Integer, StateMap<?, ?, ?>>>> getEntireStateForMigration(
            int newDOP, int taskIndex, int maxParallelism,
            Map<Integer, ResourceID> partitionToResourceIDMapActive,
            Map<Integer, ResourceID> partitionToResourceIDMapPassive,
            Map<ResourceID, Map<Integer, Map<Integer, Map<Integer, byte[]>>>> stateForOtherTMs,
            Map<Integer, String> stateNameDict, List<Integer> partitionIdToTaskId){
        if (keyedStateBackend instanceof HeapKeyedStateBackend) {
            return ((HeapKeyedStateBackend) keyedStateBackend).getEntireStateForMigration(
                    newDOP, taskIndex, maxParallelism, partitionToResourceIDMapActive,
                    partitionToResourceIDMapPassive, stateForOtherTMs, stateNameDict, partitionIdToTaskId);
        }
        else{
            throw new UnsupportedOperationException("The keyedStateBackend is not HeapKeyedStateBackend");
        }
    }

    public Map<Integer, Map<String, Map<Integer, StateMap<?, ?, ?>>>> getEntriesInRangeForMigration(
            int newDOP, int taskIndex, int maxParallelism, long start, long end, long start2, long end2,
            Map<Integer, ResourceID> partitionToResourceIDMapActive,
            Map<Integer, ResourceID> partitionToResourceIDMapPassive,
            Map<ResourceID, Map<Integer, Map<Integer, Map<Integer, byte[]>>>> stateForOtherTMs,
            Map<Integer, String> stateNameDict, List<Integer> partitionIdToTaskId) {
        if (keyedStateBackend instanceof HeapKeyedStateBackend) {
            return ((HeapKeyedStateBackend) keyedStateBackend).getEntriesInRangeForMigration(
                    newDOP, taskIndex, maxParallelism, start, end, start2, end2,
                    partitionToResourceIDMapActive, partitionToResourceIDMapPassive, stateForOtherTMs,
                    stateNameDict, partitionIdToTaskId);
        }
        else{
            throw new UnsupportedOperationException("The keyedStateBackend is not HeapKeyedStateBackend");
        }
    }

    public Map<String, StateMap<?, ?, ?>[]> getEntriesForMigrationDownstream(
            boolean stateMustBeSerialized, Map<String, byte[][]> stateForOtherTM
            ) {
        if (keyedStateBackend instanceof HeapKeyedStateBackend) {
            return ((HeapKeyedStateBackend) keyedStateBackend).getEntriesForMigrationDownstream(
                    stateMustBeSerialized, stateForOtherTM);
        }
        else{
            throw new UnsupportedOperationException("The keyedStateBackend is not HeapKeyedStateBackend");
        }
    }

    public void incorporateReceivedState(Map<String, Map<Integer, StateMap<?, ?, ?>>> newStateMaps,
                                         Map<String, Map<Integer, StateMap<?, ?, ?>>> newStateMapsPassiveQuery,
                                         int newDOP,
                                         int operatorIndex) {
        if (keyedStateBackend instanceof HeapKeyedStateBackend) {
            ((HeapKeyedStateBackend) keyedStateBackend).incorporateReceivedState(
                    newStateMaps, newStateMapsPassiveQuery, newDOP, operatorIndex);
        }
        else{
            throw new UnsupportedOperationException("The keyedStateBackend is not HeapKeyedStateBackend");
        }
    }

    public void incorporateReceivedState(Map<String, StateMap<?, ?, ?>[]> newStateMaps) {
        if (keyedStateBackend instanceof HeapKeyedStateBackend) {
            ((HeapKeyedStateBackend) keyedStateBackend).incorporateReceivedState(newStateMaps);
        }
        else{
            throw new UnsupportedOperationException("The keyedStateBackend is not HeapKeyedStateBackend");
        }
    }

    public void incorporateReceivedSerializedState(
            Map<Integer, Map<Integer, byte[]>> newState,
            Map<Integer, Map<Integer, byte[]>> statePassiveQuery,
            Map<Integer, String> stateNameDict) {
        if (keyedStateBackend instanceof HeapKeyedStateBackend) {
            ((HeapKeyedStateBackend) keyedStateBackend).incorporateReceivedSerializedState(
                    newState, statePassiveQuery, stateNameDict);
        }
        else{
            throw new UnsupportedOperationException("The keyedStateBackend is not HeapKeyedStateBackend");
        }
    }

    public void incorporateReceivedSerializedStateDownstream(Map<String, byte[][]> newState) {
        if (keyedStateBackend instanceof HeapKeyedStateBackend) {
            ((HeapKeyedStateBackend) keyedStateBackend).incorporateReceivedSerializedStateDownstream(newState);
        }
        else{
            throw new UnsupportedOperationException("The keyedStateBackend is not HeapKeyedStateBackend");
        }
    }
}
