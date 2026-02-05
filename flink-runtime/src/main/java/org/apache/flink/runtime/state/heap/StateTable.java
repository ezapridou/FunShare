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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.state.IterableStateSnapshot;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyGroupRangeNonContinuous;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateSnapshotKeyGroupReader;
import org.apache.flink.runtime.state.StateSnapshotRestore;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.internal.InternalKvState.StateIncrementalVisitor;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Base class for state tables. Accesses to state are typically scoped by the currently active key,
 * as provided through the {@link InternalKeyContext}.
 *
 * @param <K> type of key
 * @param <N> type of namespace
 * @param <S> type of state
 */
public abstract class StateTable<K, N, S>
        implements StateSnapshotRestore, Iterable<StateEntry<K, N, S>> {

    /**
     * The key context view on the backend. This provides information, such as the currently active
     * key.
     */
    protected final InternalKeyContext<K> keyContext;

    /** Combined meta information such as name and serializers for this state. */
    protected RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo;

    /** The serializer of the key. */
    protected final TypeSerializer<K> keySerializer;

    /** The offset to the contiguous key groups. */
    protected int keyGroupOffset;

    protected int dop = 1;

    /**
     * Map for holding the actual state objects. The outer array represents the key-groups. All
     * array positions will be initialized with an empty state map.
     */
    protected StateMap<K, N, S>[] keyGroupedStateMaps;

    private int operatorIndex = 99;

    /**
     * @param keyContext the key context provides the key scope for all put/get/delete operations.
     * @param metaInfo the meta information, including the type serializer for state copy-on-write.
     * @param keySerializer the serializer of the key.
     */
    public StateTable(
            InternalKeyContext<K> keyContext,
            RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo,
            TypeSerializer<K> keySerializer) {
        this.keyContext = Preconditions.checkNotNull(keyContext);
        this.metaInfo = Preconditions.checkNotNull(metaInfo);
        this.keySerializer = Preconditions.checkNotNull(keySerializer);

        this.keyGroupOffset = keyContext.getKeyGroupRange().getStartKeyGroup();
        if (keyContext.getKeyGroupRange() instanceof KeyGroupRangeNonContinuous) {
            this.dop = ((KeyGroupRangeNonContinuous) keyContext.getKeyGroupRange()).getDop();
        }

        @SuppressWarnings("unchecked")
        StateMap<K, N, S>[] state =
                (StateMap<K, N, S>[])
                        new StateMap[keyContext.getKeyGroupRange().getNumberOfKeyGroups()];
        this.keyGroupedStateMaps = state;
        for (int i = 0; i < this.keyGroupedStateMaps.length; i++) {
            this.keyGroupedStateMaps[i] = createStateMap();
        }
    }

    protected abstract StateMap<K, N, S> createStateMap();

    @Override
    @Nonnull
    public abstract IterableStateSnapshot<K, N, S> stateSnapshot();

    // Main interface methods of StateTable -------------------------------------------------------

    /**
     * Returns whether this {@link StateTable} is empty.
     *
     * @return {@code true} if this {@link StateTable} has no elements, {@code false} otherwise.
     * @see #size()
     */
    public boolean isEmpty() {
        return size() == 0;
    }

    /**
     * Returns the total number of entries in this {@link StateTable}. This is the sum of both
     * sub-tables.
     *
     * @return the number of entries in this {@link StateTable}.
     */
    public int size() {
        int count = 0;
        for (StateMap<K, N, S> stateMap : keyGroupedStateMaps) {
            count += stateMap.size();
        }
        return count;
    }

    /**
     * Returns the state of the mapping for the composite of active key and given namespace.
     *
     * @param namespace the namespace. Not null.
     * @return the states of the mapping with the specified key/namespace composite key, or {@code
     *     null} if no mapping for the specified key is found.
     */
    public S get(N namespace) {
        return get(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace);
    }

    /**
     * Returns whether this table contains a mapping for the composite of active key and given
     * namespace.
     *
     * @param namespace the namespace in the composite key to search for. Not null.
     * @return {@code true} if this map contains the specified key/namespace composite key, {@code
     *     false} otherwise.
     */
    public boolean containsKey(N namespace) {
        return containsKey(
                keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace);
    }

    /**
     * Maps the composite of active key and given namespace to the specified state.
     *
     * @param namespace the namespace. Not null.
     * @param state the state. Can be null.
     */
    public void put(N namespace, S state) {
        put(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace, state);
    }

    /**
     * Removes the mapping for the composite of active key and given namespace. This method should
     * be preferred over {@link #removeAndGetOld(N)} when the caller is not interested in the old
     * state.
     *
     * @param namespace the namespace of the mapping to remove. Not null.
     */
    public void remove(N namespace) {
        remove(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace);
    }

    /**
     * Removes the mapping for the composite of active key and given namespace, returning the state
     * that was found under the entry.
     *
     * @param namespace the namespace of the mapping to remove. Not null.
     * @return the state of the removed mapping or {@code null} if no mapping for the specified key
     *     was found.
     */
    public S removeAndGetOld(N namespace) {
        return removeAndGetOld(
                keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace);
    }

    /**
     * Applies the given {@link StateTransformationFunction} to the state (1st input argument),
     * using the given value as second input argument. The result of {@link
     * StateTransformationFunction#apply(Object, Object)} is then stored as the new state. This
     * function is basically an optimization for get-update-put pattern.
     *
     * @param namespace the namespace. Not null.
     * @param value the value to use in transforming the state. Can be null.
     * @param transformation the transformation function.
     * @throws Exception if some exception happens in the transformation function.
     */
    public <T> void transform(
            N namespace, T value, StateTransformationFunction<S, T> transformation)
            throws Exception {
        K key = keyContext.getCurrentKey();
        checkKeyNamespacePreconditions(key, namespace);

        int keyGroup = keyContext.getCurrentKeyGroupIndex();
        StateMap<K, N, S> stateMap = getMapForKeyGroup(keyGroup);
        stateMap.transform(key, namespace, value, transformation);
    }

    // For queryable state ------------------------------------------------------------------------

    /**
     * Returns the state for the composite of active key and given namespace. This is typically used
     * by queryable state.
     *
     * @param key the key. Not null.
     * @param namespace the namespace. Not null.
     * @return the state of the mapping with the specified key/namespace composite key, or {@code
     *     null} if no mapping for the specified key is found.
     */
    public S get(K key, N namespace) {
        int keyGroup =
                KeyGroupRangeAssignment.assignToKeyGroup(key, keyContext.getNumberOfKeyGroups());
        return get(key, keyGroup, namespace);
    }

    public Stream<K> getKeys(N namespace) {
        return Arrays.stream(keyGroupedStateMaps)
                .flatMap(
                        stateMap ->
                                StreamSupport.stream(
                                        Spliterators.spliteratorUnknownSize(stateMap.iterator(), 0),
                                        false))
                .filter(entry -> entry.getNamespace().equals(namespace))
                .map(StateEntry::getKey);
    }

    public Stream<Tuple2<K, N>> getKeysAndNamespaces() {
        return Arrays.stream(keyGroupedStateMaps)
                .flatMap(
                        stateMap ->
                                StreamSupport.stream(
                                        Spliterators.spliteratorUnknownSize(stateMap.iterator(), 0),
                                        false))
                .map(entry -> Tuple2.of(entry.getKey(), entry.getNamespace()));
    }

    public StateIncrementalVisitor<K, N, S> getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords) {
        return new StateEntryIterator(recommendedMaxNumberOfReturnedRecords);
    }

    // ------------------------------------------------------------------------

    private S get(K key, int keyGroupIndex, N namespace) {
        checkKeyNamespacePreconditions(key, namespace);

        StateMap<K, N, S> stateMap = getMapForKeyGroup(keyGroupIndex);

        if (stateMap == null) {
            return null;
        }

        return stateMap.get(key, namespace);
    }

    private boolean containsKey(K key, int keyGroupIndex, N namespace) {
        checkKeyNamespacePreconditions(key, namespace);

        StateMap<K, N, S> stateMap = getMapForKeyGroup(keyGroupIndex);

        return stateMap != null && stateMap.containsKey(key, namespace);
    }

    private void checkKeyNamespacePreconditions(K key, N namespace) {
        Preconditions.checkNotNull(
                key, "No key set. This method should not be called outside of a keyed context.");
        Preconditions.checkNotNull(namespace, "Provided namespace is null.");
    }

    private void remove(K key, int keyGroupIndex, N namespace) {
        checkKeyNamespacePreconditions(key, namespace);

        StateMap<K, N, S> stateMap = getMapForKeyGroup(keyGroupIndex);
        stateMap.remove(key, namespace);
    }

    private S removeAndGetOld(K key, int keyGroupIndex, N namespace) {
        checkKeyNamespacePreconditions(key, namespace);

        StateMap<K, N, S> stateMap = getMapForKeyGroup(keyGroupIndex);

        return stateMap.removeAndGetOld(key, namespace);
    }

    // ------------------------------------------------------------------------
    //  access to maps
    // ------------------------------------------------------------------------

    /** Returns the internal data structure. */
    @VisibleForTesting
    public StateMap<K, N, S>[] getState() {
        return keyGroupedStateMaps;
    }

    public int getKeyGroupOffset() {
        return keyGroupOffset;
    }

    @VisibleForTesting
    public StateMap<K, N, S> getMapForKeyGroup(int keyGroupIndex) {
        final int pos = indexToOffset(keyGroupIndex);
        if (pos >= 0 && pos < keyGroupedStateMaps.length) {
            return keyGroupedStateMaps[pos];
        } else {
            return null;
        }
    }

    /** Translates a key-group id to the internal array offset. */
    private int indexToOffset(int index) {
        return (index - keyGroupOffset) / dop;
    }

    private int offsetToIndex(int offset) {
        return offset * dop + keyGroupOffset;
    }

    // Meta data setter / getter and toString -----------------------------------------------------

    public TypeSerializer<K> getKeySerializer() {
        return keySerializer;
    }

    public TypeSerializer<S> getStateSerializer() {
        return metaInfo.getStateSerializer();
    }

    public TypeSerializer<N> getNamespaceSerializer() {
        return metaInfo.getNamespaceSerializer();
    }

    public RegisteredKeyValueStateBackendMetaInfo<N, S> getMetaInfo() {
        return metaInfo;
    }

    public void setMetaInfo(RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo) {
        this.metaInfo = metaInfo;
    }

    /**
     * This method is used for debugging purposes.
     */
    public String toStringCustom() {
        String res = "StateTable: [";
        for (StateMap<K, N, S> keyGroupedStateMap : keyGroupedStateMaps){
            res += " StateMap: [";
            for (Iterator<StateEntry<K, N, S>> it = keyGroupedStateMap.iterator(); it.hasNext(); ){
                StateEntry<K, N, S> next = it.next();
                res += " Key: " + next.getKey().toString()
                        + " Namespace: " + next.getNamespace().getClass().getName()
                        + " State " + next.getState().getClass().getName();
                break;
            }
            res += keyGroupedStateMap.size();
            res += "], ";
        }
        res += "] ";
        return res;
    }

    public StateEntry<K, N, S> getRandomEntry() {
        for (StateMap<K, N, S> keyGroupedStateMap : keyGroupedStateMaps) {
            for (Iterator<StateEntry<K, N, S>> it = keyGroupedStateMap.iterator(); it.hasNext(); ) {
                return it.next();
            }
        }
        return null;
    }

    /**
     * Returns the entries that need to be migrated because the parallelism changed.
     * This function will return state entries that must change the worker due to the parallelism change
     * return type: Map<Partition, Map<keyGroupIndex, StateMap>>
     */
    public Map<Integer, Map<Integer, StateMap<K, ?, ?>>> getEntriesForMigration(
            int newDOP, int taskIndex, int maxParallelism,
            Map<Integer, ResourceID> taskToResourceIDMap,
            Map<ResourceID, Map<Integer, Map<Integer, Map<Integer, byte[]>>>> stateForOtherTMs,
            List<Integer> partitionIdToTaskId, int stateIdx) {

        Map<Integer, Map<Integer, StateMap<K, ?, ?>>> stateToMigrate = new HashMap<>(newDOP);
        for (int pos = 0; pos < keyGroupedStateMaps.length; pos++) {
            int keyGroupIndex = offsetToIndex(pos);
            //int oldPartition = getPartitionOfKeyGroup(keyGroupIndex, oldDOP, maxParallelism);
            int newPartition = getPartitionOfKeyGroup(keyGroupIndex, newDOP, maxParallelism);
            // System.out.println("Old partition: " + oldPartition + " New partition: " + newPartition);

            ResourceID resourceIDofThisTask = taskToResourceIDMap.get(taskIndex);
            int receiverTaskIdx = partitionIdToTaskId.get(newPartition);
            if (taskIndex != receiverTaskIdx){
                // this keygroup must be migrated
                if (resourceIDofThisTask.equals(taskToResourceIDMap.get(receiverTaskIdx))){
                    // the key group should be sent to a task in the same task manager
                    // add it in the list for the new partition
                    Map<Integer, StateMap<K, ?, ?>> entries = stateToMigrate.computeIfAbsent(
                            receiverTaskIdx, k -> new HashMap<>(keyGroupedStateMaps.length));
                    entries.put(keyGroupIndex, keyGroupedStateMaps[pos]);
                }
                else {
                    // the key group should be sent to a task in a different task manager
                    byte[] serializedState = serializeStateMap(keyGroupedStateMaps[pos]);
                    stateForOtherTMs
                            .computeIfAbsent(taskToResourceIDMap.get(receiverTaskIdx),
                                    k -> new HashMap<>(newDOP))
                            .computeIfAbsent(receiverTaskIdx,
                                    k -> new HashMap<>(2))
                            .computeIfAbsent(stateIdx, k -> new HashMap<>(keyGroupedStateMaps.length))
                            .put(keyGroupIndex, serializedState);
                }
            }
        }
        return stateToMigrate;
    }

    /**
     * Returns the entries that need to be migrated when a query moves to a group that has different
     * filters.
     * This function returns all state entries and then the receiver will merge the received and
     * previous state
     * It is used when the filter is applied on a different attribute than the one on which the state
     * is organized.
     * return type: Map<Partition, Map<keyGroupIndex, StateMap>>
     * @param newDOP
     * @param maxParallelism
     * @return
     */
    public Map<Integer, Map<Integer, StateMap<K, ?, ?>>> getEntireStateForMigration(
            int newDOP, int taskIndex, int maxParallelism,
            Map<Integer, ResourceID> partitionToResourceIDMapActive,
            Map<Integer, ResourceID> partitionToResourceIDMapPassive,
            Map<ResourceID, Map<Integer, Map<Integer, Map<Integer, byte[]>>>> stateForOtherTMs,
            List<Integer> partitionIdToTaskId, int idx) {
        Map<Integer, Map<Integer, StateMap<K, ?, ?>>> stateToMigrate = new HashMap<>(newDOP);
        for (int pos = 0; pos < keyGroupedStateMaps.length; pos++) {
            int keyGroupIndex = offsetToIndex(pos);
            int newPartition = getPartitionOfKeyGroup(keyGroupIndex, newDOP, maxParallelism);
            //int oldPartition = getPartitionOfKeyGroup(keyGroupIndex, oldDOP, maxParallelism);

            ResourceID resourceIDofThisTask = partitionToResourceIDMapPassive.get(taskIndex);

            int receiverTaskIndex = partitionIdToTaskId.get(newPartition);
            ResourceID resourceIDofReceiver = partitionToResourceIDMapActive.get(receiverTaskIndex);

            if (resourceIDofReceiver.equals(resourceIDofThisTask)) {
                // add it in the list for the new partition
                Map<Integer, StateMap<K, ?, ?>> entries = stateToMigrate.computeIfAbsent(
                        receiverTaskIndex,
                        k -> new HashMap<>(keyGroupedStateMaps.length));
                entries.put(keyGroupIndex, keyGroupedStateMaps[pos]);
            } else {
                // the key group should be sent to a task in a different task manager
                byte[] serializedState = serializeStateMap(keyGroupedStateMaps[pos]);
                stateForOtherTMs
                        .computeIfAbsent(resourceIDofReceiver,
                                k -> new HashMap<>(newDOP))
                        .computeIfAbsent(receiverTaskIndex,
                                k -> new HashMap<>(2))
                        .computeIfAbsent(idx, k -> new HashMap<>(keyGroupedStateMaps.length))
                        .put(keyGroupIndex, serializedState);
            }
        }
        return stateToMigrate;
    }

    /**
     * This function is used when a query moves to a group that has different filters. It returns the
     * entries that correspond to keys that would pass the filters of the query but not the filters
     * of the group.
     * It is used when the filter is applied on the same attribute as the attribute on which the state
     * is organized.
     * This function receives the start and end of two data ranges because the dif in the data ranges
     * between the group and the query can be in two ranges.
     * If you want to use the function for one range only, you can set start2 to 0 and end2 to -1.
     * @param newDOP
     * @param maxParallelism
     * @param start
     * @param end
     * @param start2
     * @param end2
     */
    public Map<Integer, Map<Integer, StateMap<K, ?, ?>>> getEntriesInRangeForMigration(
            int newDOP, int taskIndex, int maxParallelism, long start, long end, long start2, long end2,
            Map<Integer, ResourceID> partitionToResourceIDMapActive,
            Map<Integer, ResourceID> partitionToResourceIDMapPassive,
            Map<ResourceID, Map<Integer, Map<Integer, Map<Integer, byte[]>>>> stateForOtherTMs,
            List<Integer> partitionIdToTaskId, int idx) {
        Map<Integer, Map<Integer, StateMap<K, ?, ?>>> stateToMigrate = new HashMap<>(newDOP);
        for (int pos = 0; pos < keyGroupedStateMaps.length; pos++) {
            int keyGroupIndex = offsetToIndex(pos);
            int newPartition = getPartitionOfKeyGroup(keyGroupIndex, newDOP, maxParallelism);
            //int oldPartition = getPartitionOfKeyGroup(keyGroupIndex, oldDOP, maxParallelism);

            Iterator<StateEntry<K, N, S>> it = keyGroupedStateMaps[pos].iterator();
            StateMap<K, N, S> stateMapToMigrate = createStateMap();
            while (it.hasNext()){
                StateEntry<K, N, S> next = it.next();
                if (!(next.getKey() instanceof Long) && !(next.getKey() instanceof Integer)) {
                    throw new RuntimeException(
                            "Attempting to migrate state based on a filter range for a non-integer key");
                }
                long key = (long)next.getKey();
                if ((key >= start && key <= end) || (key >= start2 && key <= end2)){
                    // this key should be migrated
                    stateMapToMigrate.put(next.getKey(), next.getNamespace(), next.getState());
                }
            }
            if (stateMapToMigrate.size() > 0){
                ResourceID resourceIDofThisTask = partitionToResourceIDMapPassive.get(taskIndex);

                int receiverTaskIndex = partitionIdToTaskId.get(newPartition);
                ResourceID resourceIDofReceiver = partitionToResourceIDMapActive.get(receiverTaskIndex);

                if (resourceIDofReceiver.equals(resourceIDofThisTask)) {
                    Map<Integer, StateMap<K, ?, ?>> entries = stateToMigrate.computeIfAbsent(
                            receiverTaskIndex,
                            k -> new HashMap<>(keyGroupedStateMaps.length));
                    entries.put(keyGroupIndex, stateMapToMigrate);
                } else {
                    // the key group should be sent to a task in a different task manager
                    byte[] serializedState = serializeStateMap(stateMapToMigrate);
                    stateForOtherTMs
                            .computeIfAbsent(resourceIDofReceiver,
                                    k -> new HashMap<>(newDOP))
                            .computeIfAbsent(receiverTaskIndex,
                                    k -> new HashMap<>(keyGroupedStateMaps.length))
                            .computeIfAbsent(idx, k -> new HashMap<>(keyGroupedStateMaps.length))
                            .put(keyGroupIndex, serializedState);
                }
            }
        }
        return stateToMigrate;
    }

    /**
     * Returns all the state maps
     * Used for state migration among downstream operators
     */
    public StateMap<K, ?, ?>[] getEntriesForMigrationDownstream(
            boolean stateMustBeSerialized,
            Map<String, byte[][]> stateForOtherTM, String stateName) {
        if (!stateMustBeSerialized) {
            // this state will be sent to a task in the same task manager
            return keyGroupedStateMaps.clone();
        }
        else {
            // this state will be sent to a task in a different task manager
            byte[][] serializedState = new byte[keyGroupedStateMaps.length][];
            for (int pos = 0; pos < keyGroupedStateMaps.length; pos++) {
                serializedState[pos] = serializeStateMap(keyGroupedStateMaps[pos]);
            }
            stateForOtherTM.put(stateName, serializedState);
            return null;
        }
    }

    public void incorporateReceivedState(Map<Integer, StateMap<K, ?, ?>> newStateMaps,
                                         Map<Integer, StateMap<K, ?, ?>> newStateMapsPassiveQuery,
                                         int newDOP, int operatorIndex) {
        this.operatorIndex = operatorIndex;
        int oldKeyGroupOffset = keyGroupOffset;
        int oldDOP = dop;

        this.keyContext.setKeyGroupRange(newDOP, operatorIndex);
        this.keyGroupOffset = keyContext.getKeyGroupRange().getStartKeyGroup();
        this.dop = newDOP;

        StateMap<K, N, S>[] newKeyGroupedStateMaps = (StateMap<K, N, S>[])
                new StateMap[keyContext.getKeyGroupRange().getNumberOfKeyGroups()];
        for (int pos = 0; pos < newKeyGroupedStateMaps.length; pos++) {
            int keyGroupIndex = offsetToIndex(pos);
            int oldPosition = (keyGroupIndex - oldKeyGroupOffset) / oldDOP;

            if (newStateMaps != null && newStateMaps.containsKey(keyGroupIndex)){
                newKeyGroupedStateMaps[pos] = (StateMap<K, N, S>) newStateMaps.get(keyGroupIndex);
            } else if (oldPosition >= 0
                    && oldPosition < keyGroupedStateMaps.length){
                newKeyGroupedStateMaps[pos] = keyGroupedStateMaps[oldPosition];
            } else {
                // this will happen when this position will get filled upon receiving the
                // serialized state
                newKeyGroupedStateMaps[pos] = createStateMap();
            }
        }

        // if newStateMapsPassiveQuery is not null, then we need to aggregate it
        if (newStateMapsPassiveQuery != null) {
            for (int pos = 0; pos < newKeyGroupedStateMaps.length; pos++) {
                int keyGroupIndex = offsetToIndex(pos);
                if (newStateMapsPassiveQuery.containsKey(keyGroupIndex)){
                    StateMap<K, N, S> stateMap = (StateMap<K, N, S>) newStateMapsPassiveQuery
                            .get(keyGroupIndex);
                    if (newKeyGroupedStateMaps[pos] == null){
                        newKeyGroupedStateMaps[pos] = stateMap;
                    } else {
                        StateMap<K, N, S> stateMapToAggregate = stateMap;
                        StateMap<K, N, S> stateMapToAggregateTo = newKeyGroupedStateMaps[pos];
                        for (Iterator<StateEntry<K, N, S>> it = stateMapToAggregate.iterator(); it.hasNext(); ){
                            StateEntry<K, N, S> next = it.next();
                            stateMapToAggregateTo.put(next.getKey(), next.getNamespace(), next.getState());
                        }
                    }
                }
            }
        }

        // TODO GroupShare this is here for debugging
        /*String res = "OperatorIndex: " + operatorIndex + " Key Group Range: "
                + keyContext.getKeyGroupRange().toString() + " new StateTable: [";
        for (StateMap<K, N, S> keyGroupedStateMap : newKeyGroupedStateMaps){
            res += " StateMap: [";
            for (Iterator<StateEntry<K, N, S>> it = keyGroupedStateMap.iterator(); it.hasNext(); ){
                StateEntry<K, N, S> next = it.next();
                res += " Key: " + next.getKey().toString()
                        + " Namespace: " + next.getNamespace().getClass().getName()
                        + " State " + next.getState().getClass().getName();
                break;
            }
            res += "], ";
        }
        res += "] ";
        System.out.println(res);*/
        this.keyGroupedStateMaps = newKeyGroupedStateMaps;
    }

    public void incorporateReceivedState(StateMap<K, ?, ?>[] newState) {
        this.keyGroupedStateMaps = (StateMap<K, N, S>[]) newState;
    }

    public void incorporateReceivedSerializedState(
            Map<Integer, byte[]> newState, Map<Integer, byte[]> newStatePassiveQuery) {
        TypeSerializer<K> keySerializer = getKeySerializer().duplicate();
        TypeSerializer<N> namespaceSerializer = getNamespaceSerializer().duplicate();
        TypeSerializer<S> stateSerializer = getStateSerializer().duplicate();

        if (newState != null) {
            addSerializedStateIntoStateMap(
                    newState, keySerializer, namespaceSerializer, stateSerializer);
        }
        if (newStatePassiveQuery != null) {
            addSerializedStateIntoStateMap(
                    newStatePassiveQuery, keySerializer, namespaceSerializer, stateSerializer);
        }
    }

    public void incorporateReceivedSerializedStateDownstream(byte[][] newState) {
        TypeSerializer<K> keySerializer = getKeySerializer().duplicate();
        TypeSerializer<N> namespaceSerializer = getNamespaceSerializer().duplicate();
        TypeSerializer<S> stateSerializer = getStateSerializer().duplicate();

        try {
            for (int pos = 0; pos < newState.length; pos++) {
                keyGroupedStateMaps[pos] = createStateMap();
                byte[] serializedState = newState[pos];
                        ByteArrayInputStreamWithPos inputStream = new ByteArrayInputStreamWithPos(
                        serializedState);
                DataInputView dataInputView = new DataInputViewStreamWrapper(inputStream);
                int size = dataInputView.readInt();
                for (int i = 0; i < size; i++) {
                    N namespace = namespaceSerializer.deserialize(dataInputView);
                    K key = keySerializer.deserialize(dataInputView);
                    S state = stateSerializer.deserialize(dataInputView);
                    keyGroupedStateMaps[pos].put(key, namespace, state);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Error while deserializing state for migration", e);
        }
    }

    private byte[] serializeStateMap(StateMap<K, N, S> stateMap) {
        ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos();
        DataOutputView outputView = new DataOutputViewStreamWrapper(outputStream);
        try {
            StateMapSnapshot snapshot = stateMap.stateSnapshot();
            snapshot.writeState(
                    getKeySerializer().duplicate(),
                    getNamespaceSerializer().duplicate(),
                    getStateSerializer().duplicate(),
                    outputView,
                    null);
            snapshot.release();
        } catch (Exception e) {
            throw new RuntimeException("Error while serializing state for migration", e);
        }
        return outputStream.toByteArray();
    }

    private void addSerializedStateIntoStateMap(
            Map<Integer, byte[]> newState, TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer, TypeSerializer<S> stateSerializer) {
        try{
            for (Map.Entry<Integer, byte[]> keyGroupNewState : newState.entrySet()) {
                int keyGroupIndex = keyGroupNewState.getKey();
                int pos = indexToOffset(keyGroupIndex);
                // if this assert fails, it means that we did not update correctly the parallelism
                assert (pos >= 0 && pos < keyGroupedStateMaps.length);
                StateMap<K, N, S> stateMap = keyGroupedStateMaps[pos];

                byte[] serializedState = keyGroupNewState.getValue();
                ByteArrayInputStreamWithPos inputStream = new ByteArrayInputStreamWithPos(
                        serializedState);
                DataInputView dataInputView = new DataInputViewStreamWrapper(inputStream);
                int size = dataInputView.readInt();

                for (int i = 0; i < size; i++) {
                    N namespace = namespaceSerializer.deserialize(dataInputView);
                    K key = keySerializer.deserialize(dataInputView);
                    S state = stateSerializer.deserialize(dataInputView);
                    stateMap.put(key, namespace, state);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Error while deserializing state for migration", e);
        }
    }

    // Snapshot / Restore -------------------------------------------------------------------------

    public void put(K key, int keyGroup, N namespace, S state) {
        checkKeyNamespacePreconditions(key, namespace);

        StateMap<K, N, S> stateMap = getMapForKeyGroup(keyGroup);
        // TODO GroupShare this is here for debugging
        if (stateMap == null) {
            System.out.println("StateMap null. Key: " + key.toString() + " KeyGroup: " + keyGroup
                    + " KeyGroupRange: " + keyContext.getKeyGroupRange().toString()
                    + " Operator index: " + operatorIndex
                    + " KeyGroupOffset: " + keyGroupOffset
                    + " DOP: " + dop
                    + " StateTable size: " + keyGroupedStateMaps.length
                    + " position: " + indexToOffset(keyGroup));
        }
        stateMap.put(key, namespace, state);
    }

    @Override
    public Iterator<StateEntry<K, N, S>> iterator() {
        return Arrays.stream(keyGroupedStateMaps)
                .filter(Objects::nonNull)
                .flatMap(
                        stateMap ->
                                StreamSupport.stream(
                                        Spliterators.spliteratorUnknownSize(stateMap.iterator(), 0),
                                        false))
                .iterator();
    }

    // For testing --------------------------------------------------------------------------------

    @VisibleForTesting
    public int sizeOfNamespace(Object namespace) {
        int count = 0;
        for (StateMap<K, N, S> stateMap : keyGroupedStateMaps) {
            count += stateMap.sizeOfNamespace(namespace);
        }

        return count;
    }

    @Nonnull
    @Override
    public StateSnapshotKeyGroupReader keyGroupReader(int readVersion) {
        return StateTableByKeyGroupReaders.readerForVersion(this, readVersion);
    }

    private int getPartitionOfKeyGroup(int keyGroupIndex, int dop, int maxParallelism) {
        //return keyGroupIndex * dop / maxParallelism;
        return keyGroupIndex % dop;
    }

    // StateEntryIterator
    // ---------------------------------------------------------------------------------------------

    class StateEntryIterator implements StateIncrementalVisitor<K, N, S> {

        final int recommendedMaxNumberOfReturnedRecords;

        int keyGroupIndex;

        StateIncrementalVisitor<K, N, S> stateIncrementalVisitor;

        StateEntryIterator(int recommendedMaxNumberOfReturnedRecords) {
            this.recommendedMaxNumberOfReturnedRecords = recommendedMaxNumberOfReturnedRecords;
            this.keyGroupIndex = 0;
            next();
        }

        private void next() {
            while (keyGroupIndex < keyGroupedStateMaps.length) {
                StateMap<K, N, S> stateMap = keyGroupedStateMaps[keyGroupIndex++];
                StateIncrementalVisitor<K, N, S> visitor =
                        stateMap.getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
                if (visitor.hasNext()) {
                    stateIncrementalVisitor = visitor;
                    return;
                }
            }
        }

        @Override
        public boolean hasNext() {
            while (stateIncrementalVisitor == null || !stateIncrementalVisitor.hasNext()) {
                if (keyGroupIndex == keyGroupedStateMaps.length) {
                    return false;
                }
                StateIncrementalVisitor<K, N, S> visitor =
                        keyGroupedStateMaps[keyGroupIndex++].getStateIncrementalVisitor(
                                recommendedMaxNumberOfReturnedRecords);
                if (visitor.hasNext()) {
                    stateIncrementalVisitor = visitor;
                    break;
                }
            }
            return true;
        }

        @Override
        public Collection<StateEntry<K, N, S>> nextEntries() {
            if (!hasNext()) {
                return null;
            }

            return stateIncrementalVisitor.nextEntries();
        }

        @Override
        public void remove(StateEntry<K, N, S> stateEntry) {
            keyGroupedStateMaps[keyGroupIndex - 1].remove(
                    stateEntry.getKey(), stateEntry.getNamespace());
        }

        @Override
        public void update(StateEntry<K, N, S> stateEntry, S newValue) {
            keyGroupedStateMaps[keyGroupIndex - 1].put(
                    stateEntry.getKey(), stateEntry.getNamespace(), newValue);
        }
    }
}
