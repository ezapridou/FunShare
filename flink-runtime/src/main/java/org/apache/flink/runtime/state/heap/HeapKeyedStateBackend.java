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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.HeapPriorityQueuesManager;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.SavepointResources;
import org.apache.flink.runtime.state.SnapshotExecutionType;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.SnapshotStrategy;
import org.apache.flink.runtime.state.SnapshotStrategyRunner;
import org.apache.flink.runtime.state.StateSnapshotRestore;
import org.apache.flink.runtime.state.StateSnapshotTransformer.StateSnapshotTransformFactory;
import org.apache.flink.runtime.state.StateSnapshotTransformers;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.StateMigrationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.RunnableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A {@link AbstractKeyedStateBackend} that keeps state on the Java Heap and will serialize state to
 * streams provided by a {@link CheckpointStreamFactory} upon checkpointing.
 *
 * @param <K> The key by which state is keyed.
 */
public class HeapKeyedStateBackend<K> extends AbstractKeyedStateBackend<K> {

    private static final Logger LOG = LoggerFactory.getLogger(HeapKeyedStateBackend.class);

    private static final Map<Class<? extends StateDescriptor>, StateFactory> STATE_FACTORIES =
            Stream.of(
                            Tuple2.of(
                                    ValueStateDescriptor.class,
                                    (StateFactory) HeapValueState::create),
                            Tuple2.of(
                                    ListStateDescriptor.class,
                                    (StateFactory) HeapListState::create),
                            Tuple2.of(
                                    MapStateDescriptor.class, (StateFactory) HeapMapState::create),
                            Tuple2.of(
                                    AggregatingStateDescriptor.class,
                                    (StateFactory) HeapAggregatingState::create),
                            Tuple2.of(
                                    ReducingStateDescriptor.class,
                                    (StateFactory) HeapReducingState::create))
                    .collect(Collectors.toMap(t -> t.f0, t -> t.f1));

    /** Map of registered Key/Value states. */
    private final Map<String, StateTable<K, ?, ?>> registeredKVStates;

    /** The configuration for local recovery. */
    private final LocalRecoveryConfig localRecoveryConfig;

    /** The snapshot strategy for this backend. */
    private final SnapshotStrategy<KeyedStateHandle, ?> checkpointStrategy;

    private final SnapshotExecutionType snapshotExecutionType;

    private final StateTableFactory<K> stateTableFactory;

    /** Factory for state that is organized as priority queue. */
    private final HeapPriorityQueuesManager priorityQueuesManager;

    public HeapKeyedStateBackend(
            TaskKvStateRegistry kvStateRegistry,
            TypeSerializer<K> keySerializer,
            ClassLoader userCodeClassLoader,
            ExecutionConfig executionConfig,
            TtlTimeProvider ttlTimeProvider,
            LatencyTrackingStateConfig latencyTrackingStateConfig,
            CloseableRegistry cancelStreamRegistry,
            StreamCompressionDecorator keyGroupCompressionDecorator,
            Map<String, StateTable<K, ?, ?>> registeredKVStates,
            Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates,
            LocalRecoveryConfig localRecoveryConfig,
            HeapPriorityQueueSetFactory priorityQueueSetFactory,
            HeapSnapshotStrategy<K> checkpointStrategy,
            SnapshotExecutionType snapshotExecutionType,
            StateTableFactory<K> stateTableFactory,
            InternalKeyContext<K> keyContext) {
        super(
                kvStateRegistry,
                keySerializer,
                userCodeClassLoader,
                executionConfig,
                ttlTimeProvider,
                latencyTrackingStateConfig,
                cancelStreamRegistry,
                keyGroupCompressionDecorator,
                keyContext);
        this.registeredKVStates = registeredKVStates;
        this.localRecoveryConfig = localRecoveryConfig;
        this.checkpointStrategy = checkpointStrategy;
        this.snapshotExecutionType = snapshotExecutionType;
        this.stateTableFactory = stateTableFactory;
        this.priorityQueuesManager =
                new HeapPriorityQueuesManager(
                        registeredPQStates,
                        priorityQueueSetFactory,
                        keyContext.getKeyGroupRange(),
                        keyContext.getNumberOfKeyGroups());
        LOG.info("Initializing heap keyed state backend with stream factory.");
    }

    // ------------------------------------------------------------------------
    //  state backend operations
    // ------------------------------------------------------------------------

    @Nonnull
    @Override
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            KeyGroupedInternalPriorityQueue<T> create(
                    @Nonnull String stateName,
                    @Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
        return priorityQueuesManager.createOrUpdate(stateName, byteOrderedElementSerializer);
    }

    private <N, V> StateTable<K, N, V> tryRegisterStateTable(
            TypeSerializer<N> namespaceSerializer,
            StateDescriptor<?, V> stateDesc,
            @Nonnull StateSnapshotTransformFactory<V> snapshotTransformFactory)
            throws StateMigrationException {

        @SuppressWarnings("unchecked")
        StateTable<K, N, V> stateTable =
                (StateTable<K, N, V>) registeredKVStates.get(stateDesc.getName());

        TypeSerializer<V> newStateSerializer = stateDesc.getSerializer();

        if (stateTable != null) {
            RegisteredKeyValueStateBackendMetaInfo<N, V> restoredKvMetaInfo =
                    stateTable.getMetaInfo();

            restoredKvMetaInfo.updateSnapshotTransformFactory(snapshotTransformFactory);

            // fetch current serializer now because if it is incompatible, we can't access
            // it anymore to improve the error message
            TypeSerializer<N> previousNamespaceSerializer =
                    restoredKvMetaInfo.getNamespaceSerializer();

            TypeSerializerSchemaCompatibility<N> namespaceCompatibility =
                    restoredKvMetaInfo.updateNamespaceSerializer(namespaceSerializer);
            if (namespaceCompatibility.isCompatibleAfterMigration()
                    || namespaceCompatibility.isIncompatible()) {
                throw new StateMigrationException(
                        "For heap backends, the new namespace serializer ("
                                + namespaceSerializer
                                + ") must be compatible with the old namespace serializer ("
                                + previousNamespaceSerializer
                                + ").");
            }

            restoredKvMetaInfo.checkStateMetaInfo(stateDesc);

            // fetch current serializer now because if it is incompatible, we can't access
            // it anymore to improve the error message
            TypeSerializer<V> previousStateSerializer = restoredKvMetaInfo.getStateSerializer();

            TypeSerializerSchemaCompatibility<V> stateCompatibility =
                    restoredKvMetaInfo.updateStateSerializer(newStateSerializer);

            if (stateCompatibility.isIncompatible()) {
                throw new StateMigrationException(
                        "For heap backends, the new state serializer ("
                                + newStateSerializer
                                + ") must not be incompatible with the old state serializer ("
                                + previousStateSerializer
                                + ").");
            }

            stateTable.setMetaInfo(restoredKvMetaInfo);
        } else {
            RegisteredKeyValueStateBackendMetaInfo<N, V> newMetaInfo =
                    new RegisteredKeyValueStateBackendMetaInfo<>(
                            stateDesc.getType(),
                            stateDesc.getName(),
                            namespaceSerializer,
                            newStateSerializer,
                            snapshotTransformFactory);

            stateTable = stateTableFactory.newStateTable(keyContext, newMetaInfo, keySerializer);
            registeredKVStates.put(stateDesc.getName(), stateTable);
        }

        return stateTable;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <N> Stream<K> getKeys(String state, N namespace) {
        if (!registeredKVStates.containsKey(state)) {
            return Stream.empty();
        }

        final StateSnapshotRestore stateSnapshotRestore = registeredKVStates.get(state);
        StateTable<K, N, ?> table = (StateTable<K, N, ?>) stateSnapshotRestore;
        return table.getKeys(namespace);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <N> Stream<Tuple2<K, N>> getKeysAndNamespaces(String state) {
        if (!registeredKVStates.containsKey(state)) {
            return Stream.empty();
        }

        final StateSnapshotRestore stateSnapshotRestore = registeredKVStates.get(state);
        StateTable<K, N, ?> table = (StateTable<K, N, ?>) stateSnapshotRestore;
        return table.getKeysAndNamespaces();
    }

    @Override
    @Nonnull
    public <N, SV, SEV, S extends State, IS extends S> IS createInternalState(
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<S, SV> stateDesc,
            @Nonnull StateSnapshotTransformFactory<SEV> snapshotTransformFactory)
            throws Exception {
        StateFactory stateFactory = STATE_FACTORIES.get(stateDesc.getClass());
        if (stateFactory == null) {
            String message =
                    String.format(
                            "State %s is not supported by %s",
                            stateDesc.getClass(), this.getClass());
            throw new FlinkRuntimeException(message);
        }
        StateTable<K, N, SV> stateTable =
                tryRegisterStateTable(
                        namespaceSerializer,
                        stateDesc,
                        getStateSnapshotTransformFactory(stateDesc, snapshotTransformFactory));
        return stateFactory.createState(stateDesc, stateTable, getKeySerializer());
    }

    @SuppressWarnings("unchecked")
    private <SV, SEV> StateSnapshotTransformFactory<SV> getStateSnapshotTransformFactory(
            StateDescriptor<?, SV> stateDesc,
            StateSnapshotTransformFactory<SEV> snapshotTransformFactory) {
        if (stateDesc instanceof ListStateDescriptor) {
            return (StateSnapshotTransformFactory<SV>)
                    new StateSnapshotTransformers.ListStateSnapshotTransformFactory<>(
                            snapshotTransformFactory);
        } else if (stateDesc instanceof MapStateDescriptor) {
            return (StateSnapshotTransformFactory<SV>)
                    new StateSnapshotTransformers.MapStateSnapshotTransformFactory<>(
                            snapshotTransformFactory);
        } else {
            return (StateSnapshotTransformFactory<SV>) snapshotTransformFactory;
        }
    }

    @Nonnull
    @Override
    public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
            final long checkpointId,
            final long timestamp,
            @Nonnull final CheckpointStreamFactory streamFactory,
            @Nonnull CheckpointOptions checkpointOptions)
            throws Exception {

        SnapshotStrategyRunner<KeyedStateHandle, ?> snapshotStrategyRunner =
                new SnapshotStrategyRunner<>(
                        "Heap backend snapshot",
                        checkpointStrategy,
                        cancelStreamRegistry,
                        snapshotExecutionType);
        return snapshotStrategyRunner.snapshot(
                checkpointId, timestamp, streamFactory, checkpointOptions);
    }

    @Nonnull
    @Override
    public SavepointResources<K> savepoint() {

        HeapSnapshotResources<K> snapshotResources =
                HeapSnapshotResources.create(
                        registeredKVStates,
                        priorityQueuesManager.getRegisteredPQStates(),
                        keyGroupCompressionDecorator,
                        keyGroupRange,
                        keySerializer,
                        numberOfKeyGroups);

        return new SavepointResources<>(snapshotResources, snapshotExecutionType);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // Nothing to do
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        // nothing to do
    }

    @Override
    public <N, S extends State, T> void applyToAllKeys(
            final N namespace,
            final TypeSerializer<N> namespaceSerializer,
            final StateDescriptor<S, T> stateDescriptor,
            final KeyedStateFunction<K, S> function,
            final PartitionStateFactory partitionStateFactory)
            throws Exception {

        try (Stream<K> keyStream = getKeys(stateDescriptor.getName(), namespace)) {

            // we copy the keys into list to avoid the concurrency problem
            // when state.clear() is invoked in function.process().
            final List<K> keys = keyStream.collect(Collectors.toList());

            final S state =
                    partitionStateFactory.get(namespace, namespaceSerializer, stateDescriptor);

            for (K key : keys) {
                setCurrentKey(key);
                function.process(key, state);
            }
        }
    }

    @Override
    public String toString() {
        return "HeapKeyedStateBackend";
    }

    /** Returns the total number of state entries across all keys/namespaces. */
    @VisibleForTesting
    @Override
    public int numKeyValueStateEntries() {
        int sum = 0;
        for (StateSnapshotRestore state : registeredKVStates.values()) {
            sum += ((StateTable<?, ?, ?>) state).size();
        }
        return sum;
    }

    /** Returns the total number of state entries across all keys for the given namespace. */
    @VisibleForTesting
    public int numKeyValueStateEntries(Object namespace) {
        int sum = 0;
        for (StateTable<?, ?, ?> state : registeredKVStates.values()) {
            sum += state.sizeOfNamespace(namespace);
        }
        return sum;
    }

    @VisibleForTesting
    public LocalRecoveryConfig getLocalRecoveryConfig() {
        return localRecoveryConfig;
    }

    private interface StateFactory {
        <K, N, SV, S extends State, IS extends S> IS createState(
                StateDescriptor<S, SV> stateDesc,
                StateTable<K, N, SV> stateTable,
                TypeSerializer<K> keySerializer)
                throws Exception;
    }

    /**
     * This method is used for debugging purposes
     * @param taskName
     */
    public void printState(String taskName) {
        String res = taskName + " HeapKeyedStateBackend: RegisteredKVStates: ";
        for (Map.Entry<String, StateTable<K, ?, ?>> entry : registeredKVStates.entrySet()){
            res += entry.getKey() + " - " + entry.getValue().toStringCustom() + "\n ";
        }
        System.out.println(res);
    }

    public Map<Integer, Map<String, Map<Integer, StateMap<K, ?, ?>>>> getEntriesForMigration(
            int newDOP, int taskIndex, int maxParallelism,
            Map<Integer, ResourceID> partitionToResourceIDMap,
            // resource id-> partition id -> state id -> keygroup id -> state
            Map<ResourceID, Map<Integer, Map<Integer, Map<Integer, byte[]>>>> stateForOtherTMs,
            Map<Integer, String> stateNameDict, List<Integer> partitionIdToTaskId) {
        Map<Integer, Map<String, Map<Integer, StateMap<K, ?, ?>>>> stateToMigrate =
                new java.util.HashMap<>(newDOP);

        int idx = 0;
        for (Map.Entry<String, StateTable<K, ?, ?>> KVStates : registeredKVStates.entrySet()) {
            stateNameDict.put(idx, KVStates.getKey());
            // per partition to-be-migrated keyGroups of a specific KVState
            Map<Integer, Map<Integer, StateMap<K, ?, ?>>> keyGroupsToMigrate = KVStates.getValue()
                    .getEntriesForMigration(newDOP, taskIndex, maxParallelism,
                            partitionToResourceIDMap, stateForOtherTMs, partitionIdToTaskId, idx);
            idx++;

            addKeyGroupsToState(keyGroupsToMigrate, stateToMigrate, KVStates.getKey());
        }
        return stateToMigrate;
    }

    public Map<Integer, Map<String, Map<Integer, StateMap<K, ?, ?>>>> getEntireStateForMigration(
            int newDOP, int taskIndex, int maxParallelism,
            Map<Integer, ResourceID> partitionToResourceIDMapActive,
            Map<Integer, ResourceID> partitionToResourceIDMapPassive,
            Map<ResourceID, Map<Integer, Map<Integer, Map<Integer, byte[]>>>> stateForOtherTMs,
            Map<Integer, String> stateNameDict, List<Integer> partitionIdToTaskId) {
        Map<Integer, Map<String, Map<Integer, StateMap<K, ?, ?>>>> stateToMigrate =
                new java.util.HashMap<>(newDOP);

        int idx = 0;
        for (Map.Entry<String, StateTable<K, ?, ?>> KVStates : registeredKVStates.entrySet()) {
            stateNameDict.put(idx, KVStates.getKey());
            // per partition to-be-migrated keyGroups of a specific KVState
            Map<Integer, Map<Integer, StateMap<K, ?, ?>>> keyGroupsToMigrate = KVStates.getValue()
                    .getEntireStateForMigration(newDOP, taskIndex, maxParallelism,
                            partitionToResourceIDMapActive, partitionToResourceIDMapPassive,
                            stateForOtherTMs, partitionIdToTaskId, idx);

            addKeyGroupsToState(keyGroupsToMigrate, stateToMigrate, KVStates.getKey());
        }
        return stateToMigrate;
    }

    public Map<Integer, Map<String, Map<Integer, StateMap<K, ?, ?>>>> getEntriesInRangeForMigration(
            int newDOP, int taskIndex, int maxParallelism, long start, long end, long start2, long end2,
            Map<Integer, ResourceID> partitionToResourceIDMapActive,
            Map<Integer, ResourceID> partitionToResourceIDMapPassive,
            Map<ResourceID, Map<Integer, Map<Integer, Map<Integer, byte[]>>>> stateForOtherTMs,
            Map<Integer, String> stateNameDict, List<Integer> partitionIdToTaskId) {
        Map<Integer, Map<String, Map<Integer, StateMap<K, ?, ?>>>> stateToMigrate =
                new java.util.HashMap<>(newDOP);

        int idx = 0;
        for (Map.Entry<String, StateTable<K, ?, ?>> KVStates : registeredKVStates.entrySet()) {
            stateNameDict.put(idx, KVStates.getKey());
            // per partition to-be-migrated keyGroups of a specific KVState
            Map<Integer, Map<Integer, StateMap<K, ?, ?>>> keyGroupsToMigrate = KVStates.getValue()
                    .getEntriesInRangeForMigration(newDOP, taskIndex, maxParallelism, start, end,
                            start2, end2, partitionToResourceIDMapActive,
                            partitionToResourceIDMapPassive, stateForOtherTMs, partitionIdToTaskId, idx);

            addKeyGroupsToState(keyGroupsToMigrate, stateToMigrate, KVStates.getKey());
        }
        return stateToMigrate;
    }

    public Map<String, StateMap<K, ?, ?>[]> getEntriesForMigrationDownstream(
            boolean stateMustBeSerialized, Map<String, byte[][]> stateForOtherTM
            ) {
        Map<String, StateMap<K, ?, ?>[]> stateToMigrate = new java.util.HashMap<>(registeredKVStates.size());
        for (Map.Entry<String, StateTable<K, ?, ?>> KVStates : registeredKVStates.entrySet()) {
            StateMap<K, ?, ?>[] state = KVStates.getValue()
                    .getEntriesForMigrationDownstream(stateMustBeSerialized, stateForOtherTM, KVStates.getKey());
            if (stateToMigrate != null) {
                stateToMigrate.put(KVStates.getKey(), state);
            }
        }
        return stateToMigrate;
    }

    public void incorporateReceivedState(Map<String, Map<Integer, StateMap<K, ?, ?>>> newStateMaps,
                                         Map<String, Map<Integer, StateMap<K, ?, ?>>> newStateMapsPassiveQuery,
                                         int newDOP, int operatorIndex) {
        for (Map.Entry<String, StateTable<K, ?, ?>> KVStates : registeredKVStates.entrySet()) {
            Map<Integer, StateMap<K, ?, ?>> stateOfPassiveQuery = newStateMapsPassiveQuery == null ?
                    null : newStateMapsPassiveQuery.get(KVStates.getKey());
            Map<Integer, StateMap<K, ?, ?>> newKVState = newStateMaps == null ?
                    null : newStateMaps.get(KVStates.getKey());
            KVStates.getValue().incorporateReceivedState(
                    newKVState, stateOfPassiveQuery,
                    newDOP, operatorIndex);
        }
    }

    public void incorporateReceivedState(Map<String, StateMap<K, ?, ?>[]> newStateMaps) {
        for (Map.Entry<String, StateTable<K, ?, ?>> KVStates : registeredKVStates.entrySet()) {
            KVStates.getValue().incorporateReceivedState(newStateMaps.get(KVStates.getKey()));
        }
    }

    public void incorporateReceivedSerializedState(
            Map<Integer, Map<Integer, byte[]>> newState,
            Map<Integer, Map<Integer, byte[]>> statePassiveQuery,
            Map<Integer, String> stateNameDict) {
        for (Map.Entry<Integer, String> stateName : stateNameDict.entrySet()) {
            if (!registeredKVStates.containsKey(stateName.getValue())){
                throw new RuntimeException("State " + stateName.getValue() + " not found in registeredKVStates");
            }
            Map<Integer, byte[]> newStateWithName = newState == null ? null : newState.get(stateName.getKey());
            Map<Integer, byte[]> statePassiveWithName = statePassiveQuery == null ? null : statePassiveQuery.get(stateName.getKey());
            registeredKVStates.get(stateName.getValue()).incorporateReceivedSerializedState(
                    newStateWithName, statePassiveWithName);
        }
    }

    public void incorporateReceivedSerializedStateDownstream(Map<String, byte[][]> newState) {
        for (Map.Entry<String, StateTable<K, ?, ?>> KVStates : registeredKVStates.entrySet()) {
            KVStates.getValue().incorporateReceivedSerializedStateDownstream(newState.get(KVStates.getKey()));
        }
    }

    private void addKeyGroupsToState(Map<Integer, Map<Integer, StateMap<K, ?, ?>>> keyGroupsToMigrate,
                                     Map<Integer, Map<String, Map<Integer, StateMap<K, ?, ?>>>> stateToMigrate,
                                     String KVStateKey) {
        // iterate over the partitions
        for (Map.Entry<Integer, Map<Integer, StateMap<K, ?, ?>>> statePerPartition :
                keyGroupsToMigrate.entrySet()) {

            // get/create an entry for the partition in the stateToMigrate map
            Map<String, Map<Integer, StateMap<K, ?, ?>>> KVStatesOfPartition =
                    stateToMigrate.computeIfAbsent(
                            statePerPartition.getKey(),
                            x -> new java.util.HashMap<>());

            // add the stateMaps of the specific KVState to the partition
            KVStatesOfPartition.put(KVStateKey, statePerPartition.getValue());
        }
    }
}
