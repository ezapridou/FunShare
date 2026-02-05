package org.apache.flink.extensions.controller;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SubsetGenerator {
    List<Integer> list;

    public SubsetGenerator(Set<Integer> inputSet) {
        // Convert set to list for indexed access
        this.list = new ArrayList<>(inputSet);
    }
    public Set<Set<Integer>> generateSubsets(int subsetSize) {
        // Validate input
        if (subsetSize < 0 || subsetSize > list.size()) {
            throw new IllegalArgumentException("Subset size must be between 0 and the size of the input set");
        }

        Set<Set<Integer>> result = new HashSet<>();

        // Generate combinations using recursive helper function
        generateSubsetsHelper(subsetSize, 0, new HashSet<>(), result);

        return result;
    }

    private void generateSubsetsHelper(
            int subsetSize,
            int startIndex,
            Set<Integer> currentSubset,
            Set<Set<Integer>> result) {

        // Base case: if we've collected enough elements
        if (currentSubset.size() == subsetSize) {
            result.add(new HashSet<>(currentSubset));
            return;
        }

        // If we can't collect enough elements from remaining items, return
        if (startIndex + (subsetSize - currentSubset.size()) > list.size()) {
            return;
        }

        // For each position, we have two choices:
        // 1. Include the current element
        // 2. Skip the current element
        for (int i = startIndex; i < list.size(); i++) {
            currentSubset.add(list.get(i));
            generateSubsetsHelper(subsetSize, i + 1, currentSubset, result);
            currentSubset.remove(list.get(i));
        }
    }
}
