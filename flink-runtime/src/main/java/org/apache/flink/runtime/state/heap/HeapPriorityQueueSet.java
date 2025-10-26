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

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.state.KeyExtractorFunction;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyGroupRangeNonContinuous;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.PriorityComparator;
import org.apache.flink.util.CloseableIterator;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A heap-based priority queue with set semantics, based on {@link HeapPriorityQueue}. The heap is
 * supported by hash set for fast contains (de-duplication) and deletes. Object identification
 * happens based on {@link #equals(Object)}.
 *
 * <p>Possible future improvements:
 *
 * <ul>
 *   <li>We could also implement shrinking for the heap and the deduplication set.
 *   <li>We could replace the deduplication maps with more efficient custom implementations. In
 *       particular, a hash set would be enough if it could return existing elements on unsuccessful
 *       adding, etc..
 * </ul>
 *
 * @param <T> type of the contained elements.
 */
public class HeapPriorityQueueSet<T extends HeapPriorityQueueElement> extends HeapPriorityQueue<T>
        implements KeyGroupedInternalPriorityQueue<T> {

    /** Function to extract the key from contained elements. */
    private final KeyExtractorFunction<T> keyExtractor;

    /**
     * This array contains one hash set per key-group. The sets are used for fast de-duplication and
     * deletes of elements.
     */
    private HashMap<T, T>[] deduplicationMapsByKeyGroup;

    /** The key-group range of elements that are managed by this queue. */
    private KeyGroupRange keyGroupRange;

    /** The total number of key-groups of the job. */
    private int totalNumberOfKeyGroups;

    /**
     * Creates an empty {@link HeapPriorityQueueSet} with the requested initial capacity.
     *
     * @param elementPriorityComparator comparator for the priority of contained elements.
     * @param keyExtractor function to extract a key from the contained elements.
     * @param minimumCapacity the minimum and initial capacity of this priority queue.
     * @param keyGroupRange the key-group range of the elements in this set.
     * @param totalNumberOfKeyGroups the total number of key-groups of the job.
     */
    @SuppressWarnings("unchecked")
    public HeapPriorityQueueSet(
            @Nonnull PriorityComparator<T> elementPriorityComparator,
            @Nonnull KeyExtractorFunction<T> keyExtractor,
            @Nonnegative int minimumCapacity,
            @Nonnull KeyGroupRange keyGroupRange,
            @Nonnegative int totalNumberOfKeyGroups) {

        super(elementPriorityComparator, minimumCapacity);

        this.keyExtractor = keyExtractor;

        this.totalNumberOfKeyGroups = totalNumberOfKeyGroups;
        this.keyGroupRange = keyGroupRange;

        final int keyGroupsInLocalRange = keyGroupRange.getNumberOfKeyGroups();
        final int deduplicationSetSize = 1 + minimumCapacity / keyGroupsInLocalRange;
        this.deduplicationMapsByKeyGroup = new HashMap[keyGroupsInLocalRange];
        for (int i = 0; i < keyGroupsInLocalRange; ++i) {
            deduplicationMapsByKeyGroup[i] = new HashMap<>(deduplicationSetSize);
        }
    }

    @Override
    @Nullable
    public T poll() {
        final T toRemove = super.poll();
        return toRemove != null ? getDedupMapForElement(toRemove).remove(toRemove) : null;
    }

    /**
     * Adds the element to the queue. In contrast to the superclass and to maintain set semantics,
     * this happens only if no such element is already contained (determined by {@link
     * #equals(Object)}).
     *
     * @return <code>true</code> if the operation changed the head element or if is it unclear if
     *     the head element changed. Only returns <code>false</code> iff the head element was not
     *     changed by this operation.
     */
    @Override
    public boolean add(@Nonnull T element) {
        return getDedupMapForElement(element).putIfAbsent(element, element) == null
                && super.add(element);
    }

    /**
     * In contrast to the superclass and to maintain set semantics, removal here is based on
     * comparing the given element via {@link #equals(Object)}.
     *
     * @return <code>true</code> if the operation changed the head element or if is it unclear if
     *     the head element changed. Only returns <code>false</code> iff the head element was not
     *     changed by this operation.
     */
    @Override
    public boolean remove(@Nonnull T toRemove) {
        T storedElement = getDedupMapForElement(toRemove).remove(toRemove);
        return storedElement != null && super.remove(storedElement);
    }

    @Override
    public void clear() {
        super.clear();
        for (HashMap<?, ?> elementHashMap : deduplicationMapsByKeyGroup) {
            elementHashMap.clear();
        }
    }

    private HashMap<T, T> getDedupMapForKeyGroup(@Nonnegative int keyGroupId) {
        return deduplicationMapsByKeyGroup[globalKeyGroupToLocalIndex(keyGroupId)];
    }

    private HashMap<T, T> getDedupMapForElement(T element) {
        int keyGroup =
                KeyGroupRangeAssignment.assignToKeyGroup(
                        keyExtractor.extractKeyFromElement(element), totalNumberOfKeyGroups);
        return getDedupMapForKeyGroup(keyGroup);
    }

    private int globalKeyGroupToLocalIndex(int keyGroup) {
        checkArgument(
                keyGroupRange.contains(keyGroup),
                "%s does not contain key group %s",
                keyGroupRange,
                keyGroup);
        int dop = ((KeyGroupRangeNonContinuous)keyGroupRange).getDop();
        return (keyGroup - keyGroupRange.getStartKeyGroup()) / dop;
    }

    @Nonnull
    @Override
    public Set<T> getSubsetForKeyGroup(int keyGroupId) {
        return getDedupMapForKeyGroup(keyGroupId).keySet();
    }

    public Map<Integer, Map<Integer, HashMap<?, ?>>> getDeduplicationMapsForMigration(
            int newDOP, int oldDOP, int taskIndex, int maxParallelism,
            Map<Integer, ResourceID> partitionToResourceIDMap,
            Map<ResourceID, Map<Integer, Map<Integer, HashMap<?, ?>>>> dedupMapsForOtherTMs,
            List<Integer> partitionIdToTaskId) {
        Map<Integer, Map<Integer, HashMap<?, ?>>> deduplicationMapsToMigrate = new HashMap<>(newDOP);
        for (int pos = 0; pos < deduplicationMapsByKeyGroup.length; pos++) {
            int keyGroupIndex = keyGroupRange.getStartKeyGroup() + pos * oldDOP;
            //int oldPartition = getPartitionOfKeyGroup(keyGroupIndex, oldDOP, maxParallelism);
            int newPartition = getPartitionOfKeyGroup(keyGroupIndex, newDOP);

            ResourceID resourceIDofThisTask = partitionToResourceIDMap.get(taskIndex);
            int receiverTaskIdx = partitionIdToTaskId.get(newPartition);
            if (taskIndex != receiverTaskIdx) {
                if (resourceIDofThisTask.equals(partitionToResourceIDMap.get(receiverTaskIdx))) {
                    Map<Integer, HashMap<?, ?>> mapForPartition =
                            deduplicationMapsToMigrate.computeIfAbsent(
                                    receiverTaskIdx,
                                    k -> new HashMap<>(deduplicationMapsByKeyGroup.length));
                    mapForPartition
                            .put(keyGroupIndex, deduplicationMapsByKeyGroup[pos]);
                }
                else {
                    Map<Integer, Map<Integer, HashMap<?, ?>>> dedupMapsForOtherTM =
                            dedupMapsForOtherTMs.computeIfAbsent(
                                    resourceIDofThisTask, k -> new HashMap<>(newDOP));
                    Map<Integer, HashMap<?, ?>> dedupMapsForPartition =
                            dedupMapsForOtherTM.computeIfAbsent(
                                    receiverTaskIdx, k -> new HashMap<>(deduplicationMapsByKeyGroup.length));
                    dedupMapsForPartition
                            .put(keyGroupIndex, deduplicationMapsByKeyGroup[pos]);
                }
            }
        }
        return deduplicationMapsToMigrate;
    }

    public HashMap<?, ?>[] getDeduplicationMapsForMigration() {
        return deduplicationMapsByKeyGroup.clone();
    }

    public void incorporateDeduplicationMapsFromMigration(
            Map<Integer, HashMap<T, T>> newDeduplicationMaps, int newDOP, int operatorIndex, int oldDOP) {
        CloseableIterator<T> it = super.iterator();
        List<T> elementsToDelete = new ArrayList<>();
        while (it.hasNext()) {
            T element = it.next();
            if (element != null) {
                int keyGroupIndex =
                        KeyGroupRangeAssignment.assignToKeyGroup(
                                keyExtractor.extractKeyFromElement(element), totalNumberOfKeyGroups);
                int oldPartition = getPartitionOfKeyGroup(keyGroupIndex, oldDOP);
                int newPartition = getPartitionOfKeyGroup(keyGroupIndex, newDOP);
                if (oldPartition != newPartition) {
                    elementsToDelete.add(element);
                }
            }
        }
        for (T element : elementsToDelete) {
            super.remove(element);
        }
        int oldKeyGroupRangeStart = keyGroupRange.getStartKeyGroup();
        setKeyGroupRange(newDOP, operatorIndex);

        HashMap<T, T>[] newDeduplicationMapsByKeyGroup
                = new HashMap[keyGroupRange.getNumberOfKeyGroups()];

        for (int pos = 0; pos < newDeduplicationMapsByKeyGroup.length; pos++) {
            int keyGroupIndex = keyGroupRange.getStartKeyGroup() + pos * newDOP;
            int oldPosition = (keyGroupIndex - oldKeyGroupRangeStart) / oldDOP;
            if (newDeduplicationMaps != null && newDeduplicationMaps.containsKey(keyGroupIndex)) {
                newDeduplicationMapsByKeyGroup[pos] = newDeduplicationMaps.get(keyGroupIndex);
            } else if (oldPosition >= 0 && oldPosition < deduplicationMapsByKeyGroup.length) {
                newDeduplicationMapsByKeyGroup[pos] = deduplicationMapsByKeyGroup[oldPosition];
            } else {
                newDeduplicationMapsByKeyGroup[pos] = new HashMap<>();
            }
        }
        this.deduplicationMapsByKeyGroup = newDeduplicationMapsByKeyGroup;
    }

    public void incorporateDeduplicationMapsFromMigration(HashMap<T, T>[] newDeduplicationMaps) {
        this.deduplicationMapsByKeyGroup = newDeduplicationMaps;
    }

    public void addInDedupMaps(int keyGroupIndex, T elementKey, T elementVal) {
        getDedupMapForKeyGroup(keyGroupIndex).put(elementKey, elementVal);
    }

    public Map<Integer, List<T>> getElementsForMigration(
            int newDOP, int taskIndex, int maxParallelism,
            Map<Integer, ResourceID> partitionToResourceIDMap,
            Map<ResourceID, Map<Integer, List<HeapPriorityQueueElement>>> triggersForOtherTMs,
            List<Integer> partitionIdToTaskId) {
        Map<Integer, List<T>> elements = new HashMap<>(newDOP);
        CloseableIterator<T> it = super.iterator();
        List<T> elementsToDelete = new ArrayList<>();
        while (it.hasNext()) {
            T element = it.next();
            if (element != null) {
                int keyGroupIndex =
                        KeyGroupRangeAssignment.assignToKeyGroup(
                                keyExtractor.extractKeyFromElement(element), totalNumberOfKeyGroups);
                //int oldPartition = getPartitionOfKeyGroup(keyGroupIndex, oldDOP, maxParallelism);
                int newPartition = getPartitionOfKeyGroup(keyGroupIndex, newDOP);

                ResourceID resourceIDofThisTask = partitionToResourceIDMap.get(taskIndex);
                int receiverTaskIdx = partitionIdToTaskId.get(newPartition);
                if (taskIndex != receiverTaskIdx) {
                    if (resourceIDofThisTask.equals(partitionToResourceIDMap.get(receiverTaskIdx))) {
                        List<T> elementsPerPartition = elements.computeIfAbsent(
                                receiverTaskIdx, x -> new ArrayList<>());
                        elementsPerPartition.add(element);
                    }
                    else {
                        Map<Integer, List<HeapPriorityQueueElement>> triggersForOtherTM =
                                triggersForOtherTMs.computeIfAbsent(
                                        resourceIDofThisTask, k -> new HashMap<>(newDOP));
                        List<HeapPriorityQueueElement> triggersForPartition =
                                triggersForOtherTM.computeIfAbsent(
                                        receiverTaskIdx, k -> new ArrayList<>());
                        triggersForPartition.add(element);
                    }
                    elementsToDelete.add(element);
                }
            }
        }
        for (T element : elementsToDelete) {
            super.remove(element);
        }
        return elements;
    }

    public T[] getElementsForMigration() {
        return super.cloneQueue();
    }

    public int getQueueSize() {
        return super.size();
    }

    public void incorporateElementsFromMigration(List<T> elements) {
        if (elements == null) {
            return;
        }
        for (T element : elements) {
            add(element);
        }
    }

    public void incorporateElementsFromMigration(T[] queue, int size) {
        super.queue = queue;
        super.size = size;
    }

    private void setKeyGroupRange(int parallelism, int operatorIndex) {
        this.keyGroupRange = KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(
                totalNumberOfKeyGroups, parallelism, operatorIndex);
    }

    private int getPartitionOfKeyGroup(int keyGroupIndex, int dop) {
        return keyGroupIndex % dop;
    }

    public void changeParallelism(int newDOP, int operatorIndex, int oldDOP) {
        //int oldKeyGroupRangeStart = keyGroupRange.getStartKeyGroup();
        setKeyGroupRange(newDOP, operatorIndex);

        HashMap<T, T>[] newDeduplicationMapsByKeyGroup
                = new HashMap[keyGroupRange.getNumberOfKeyGroups()];

        for (int pos = 0; pos < newDeduplicationMapsByKeyGroup.length; pos++) {
            newDeduplicationMapsByKeyGroup[pos] = new HashMap<>();
            /*int keyGroupIndex = keyGroupRange.getStartKeyGroup() + pos * newDOP;
            int oldPosition = (keyGroupIndex - oldKeyGroupRangeStart) / oldDOP;
            if (oldPosition >= 0 && oldPosition < deduplicationMapsByKeyGroup.length) {
                newDeduplicationMapsByKeyGroup[pos] = deduplicationMapsByKeyGroup[oldPosition];
            } else {
                newDeduplicationMapsByKeyGroup[pos] = new HashMap<>();
            }*/
        }
        this.deduplicationMapsByKeyGroup = newDeduplicationMapsByKeyGroup;
        super.clear();
    }
}
