package org.apache.flink.extensions.controller;

import net.michaelkoepf.spegauge.api.sut.DataDistrSplitStats;
import net.michaelkoepf.spegauge.api.sut.DataRange;

import net.michaelkoepf.spegauge.api.sut.FilterDataDistrMergeStats;
import net.michaelkoepf.spegauge.api.sut.JoinDataDistrMergeStats;

import org.apache.flink.extensions.reconfiguration.ReconfigurableExecutionVertex;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class FunShareOptimizer extends MonitoringOptimizer {
    double GC_THRESHOLD_MERGE = 0.9;
    protected int DOPinIsolation;
    protected int initialDelay;
    protected int optimizationStartDelay;
    protected int mergeInterval; // in ms
    protected int splitInterval = 10000; // in ms
    protected boolean[][] querySets;
    protected HashSet<Integer>[] groups;
    protected boolean[] isGroupActive;
    protected HashMap<String, ResourceID> taskToResourceIDMap;
    protected String queryCategory;

    HashMap<Integer, ArrayList<Integer>> activeChannelsPerGroup;

    DataRange[] nonOverlappingDataRanges;
    HashMap<Integer, HashSet<Integer>> queryToNonOverlappingDataRanges;

    protected int[] dopPerGroup;
    
    private int monitoringDuration = 1000;

    private int inputRate;
    private int windowSizeInSec = 60;
    private int resourceIncStep = Integer.MAX_VALUE;


    public void init(String jobID,
                     HashMap<Integer, HashMap<String, HashSet<String>>> graphWithMapping,
                     HashMap<String, ExecutionVertex> operatorToExecutionVertexMap,
                     HashMap<String, ArrayList<ExecutionJobVertex>> targetVertices,
                     HashMap<String, HashMap<Integer, HashSet<String>>> targetWorkers,
                     HashMap<String, ArrayList<Integer>> queryCategories,
                     HashMap<Integer, DataRange> filtersPerQuery,
                     HashMap<Integer, HashSet<String>> sources,
                     int DOPinIsolation, int DOPmax, int driverPort, int initialDelay,
                     int optimizationStartDelay, int interval, int inputRate) {
        init(jobID, graphWithMapping, targetWorkers, queryCategories, filtersPerQuery, sources,
                DOPmax, driverPort);
        this.DOPinIsolation = DOPinIsolation;
        this.initialDelay = initialDelay;
        this.optimizationStartDelay = optimizationStartDelay;
        this.mergeInterval = interval;
        this.inputRate = inputRate;

        int numOfQueries = getNumOfQueries();

        // QUERYSETS
        querySets = new boolean[numOfQueries][numOfQueries];
        for (int i = 0; i < numOfQueries; i++) {
            querySets[i][i] = true;
        }

        // GROUPS
        // map from group id to set of query ids
        groups = new HashSet[numOfQueries];
        isGroupActive = new boolean[numOfQueries];
        dopPerGroup = new int[numOfQueries];
        for (int i = 0; i < numOfQueries; i++) {
            HashSet<Integer> queries = new HashSet<>();
            queries.add(i);
            groups[i] = queries;
            isGroupActive[i] = true;
            dopPerGroup[i] = DOPinIsolation;
        }

        queryCategory = targetWorkers.keySet().iterator().next();

        activeChannelsPerGroup = new HashMap<>();
        // Initialize active channels for isolation execution
        for (int group = 0; group < getNumOfQueries(); group++) {
            ArrayList<Integer> activeChannels = new ArrayList<>(DOPinIsolation * getNumOfQueries());
            for (int i = 0; i < DOPinIsolation; i++) {
                activeChannels.add(group * DOPinIsolation + i);
            }
            activeChannelsPerGroup.put(group, activeChannels);
        }

        nonOverlappingDataRanges = getNonOverlappingDataRanges(filtersPerQuery);
        queryToNonOverlappingDataRanges = getQueryToNonOverlappingDataRanges(
                nonOverlappingDataRanges);

        String print = ("MonitoringDataRanges " );
        for (DataRange dr : nonOverlappingDataRanges) {
            print += dr + " ";
        }
        System.out.println(print);
        System.out.println("QueryToDataRangeMap " + queryToNonOverlappingDataRanges);

        start(operatorToExecutionVertexMap, targetVertices);
    }

    @Override
    public void start(
            Map<String, ExecutionVertex> operatorToExecutionVertexMap,
            Map<String, ArrayList<ExecutionJobVertex>> targetVertices) {

        Timer initialReconfigurationTimer = new Timer();
        TimerTask initialReconfigurationTask = getInitialReconfTask(operatorToExecutionVertexMap);

        Timer mergeTimer = new Timer();
        TimerTask mergeTask = getMergeTask(operatorToExecutionVertexMap);

        Timer splitTimer = new Timer();
        TimerTask splitTask = getSplitTask(operatorToExecutionVertexMap);

        if (!targetWorkers.isEmpty()) {
            initialReconfigurationTimer.schedule(initialReconfigurationTask, initialDelay);
            mergeTimer.schedule(mergeTask, optimizationStartDelay, mergeInterval);
            splitTimer.schedule(splitTask, optimizationStartDelay + 60000, splitInterval);
        }
    }

    private TimerTask getInitialReconfTask(Map<String, ExecutionVertex> operatorToExecutionVertexMap) {
        TimerTask initialReconfigurationTask = new TimerTask() {
            @Override
            public void run() {
                // map from operator name to TaskManagerLocation
                // getting the task to resource id map in the init function would not work as
                // resources have not yet been assigned
                taskToResourceIDMap = getTaskToResourceIDMap(operatorToExecutionVertexMap);
                reconfigureQueriesToStartWithDOPInIsolation(queryCategory, operatorToExecutionVertexMap, taskToResourceIDMap);
            }
        };
        return initialReconfigurationTask;
    }

    private TimerTask getMergeTask(Map<String, ExecutionVertex> operatorToExecutionVertexMap) {
        TimerTask mergeTask = new TimerTask() {
            private int iteration = 0;

            @Override
            public void run() {
                int currentIteration = iteration;
                iteration++;

                executeMergeStep(currentIteration, operatorToExecutionVertexMap);

            }
        };
        return mergeTask;
    }

    private TimerTask getSplitTask(Map<String, ExecutionVertex> operatorToExecutionVertexMap) {
        TimerTask splitTask = new TimerTask() {
            private int iteration = 0;
            @Override
            public void run() {
                int currentIteration = iteration;
                iteration++;

                checkForSplits(currentIteration, operatorToExecutionVertexMap);
            }
        };
        return splitTask;
    }

    protected void checkForSplits(
            int currentIteration, Map<String, ExecutionVertex> operatorToExecutionVertexMap) {
        System.out.println("Checking for splits. Iteration " + currentIteration);

        // split stats per query
        CompletableFuture<Double>[] throughputFutures = getThroughputMeasurements(operatorToExecutionVertexMap);
        CompletableFuture<List<DataDistrSplitStats>>[] splitStatsFutures = getSplitStats(operatorToExecutionVertexMap);
        for (int groupId = 0; groupId < getNumOfQueries(); groupId++) {
            if (!isGroupActive[groupId] || groups[groupId].size() == 1) {
                continue;
            }
            try {
                double groupThroughput = throughputFutures[groupId].get();
                if (groupThroughput >= inputRate * 0.95) {
                    System.out.println("Group " + groupId + " sustains the input rate. No need to check for splits.");
                    continue;
                }
                DataDistrSplitStats aggregatedStats = new DataDistrSplitStats();
                List<DataDistrSplitStats> splitStats = splitStatsFutures[groupId].get();
                for (DataDistrSplitStats stats : splitStats) {
                    System.out.println("Split stats for group " + groupId + ": " + stats.toString());
                }
                if (splitStats.size() == 0) {
                    System.out.println("Stats received for group " + groupId);
                    continue;
                }
                // aggregate stats per query
                int measurementCount = 0;
                for (DataDistrSplitStats statsOfTask : splitStats) {
                    if (statsOfTask.actualSampleSizeA != statsOfTask.targetSampleSizeA ||
                            statsOfTask.actualSampleSizeB != statsOfTask.targetSampleSizeB) {
                        continue;
                    }
                    for (Map.Entry<Integer, Integer> entry : statsOfTask.selectedNumElementsPerQuery.entrySet()) {
                        int queryId = entry.getKey();
                        aggregatedStats.selectedNumElementsPerQuery.merge(
                                queryId, entry.getValue(), Integer::sum);
                    }
                    for (Map.Entry<Integer, Integer> entry : statsOfTask.joinMatchesPerQuery.entrySet()) {
                        int queryId = entry.getKey();
                        aggregatedStats.joinMatchesPerQuery.merge(
                                queryId, entry.getValue(), Integer::sum);
                    }
                    aggregatedStats.actualSampleSizeA += statsOfTask.actualSampleSizeA;
                    aggregatedStats.actualSampleSizeB += statsOfTask.actualSampleSizeB;
                    measurementCount++;
                }

                if (measurementCount == 0) {
                    System.out.println("No stats for group " + groupId + " as the sample size was not reached.");
                    continue;
                }
                HashSet<DataRange> rangesOfGroup = getNonOverlappingRangesOfGroup(groups[groupId]);
                double groupSelectivity = getSelectivityFromRanges(rangesOfGroup);
                for (Map.Entry<Integer, Integer> entry : aggregatedStats.selectedNumElementsPerQuery.entrySet()){
                    int queryId = entry.getKey();
                    double selectivityAfterGroupFilter = ((double)entry.getValue()) /
                            (aggregatedStats.actualSampleSizeA + aggregatedStats.actualSampleSizeB);
                    double selectivity = groupSelectivity * selectivityAfterGroupFilter;
                    // as I might receive stats only by some subtasks, here I must calculate the
                    // join matches in the full sample (all the subtasks)
                    // eg. if 8 out of 16 subtasks send stats and the total is 16000 join matches
                    // per subtask, then the join matches per subtask would be 16000 / 8 = 2000
                    // and the total for all subtasks (which reflects the entire data distribution):
                    // 2000 * 16 = 32000
                    int numOfTasks = dopPerGroup[groupId];
                    double joinMatchesInSample = (((double)aggregatedStats
                            .joinMatchesPerQuery.getOrDefault(entry.getKey(), 0))
                            / measurementCount) * numOfTasks;

                    double joinMatchesUpscaled = upscaleJoinMatches(
                            joinMatchesInSample, inputRate * selectivity * windowSizeInSec,
                            (aggregatedStats.targetSampleSizeA + aggregatedStats.targetSampleSizeB) * numOfTasks);

                    // estimate per query performance in isolation
                    double throughputInIsolation = Double.min(
                            estimateThroughput(queryCategory, selectivity, joinMatchesUpscaled), inputRate);
                    System.out.println("Split stats: query " + queryId
                            + " selectivity " + selectivity
                            + " join matches " + joinMatchesInSample
                            + " join matches upscaled " + joinMatchesUpscaled
                            + " group selectivity " + groupSelectivity
                            + " ranges of group " + rangesOfGroup
                            + " numOfTasks " + numOfTasks
                            + " measurement count " + measurementCount
                            + " estimated throughput in isolation " + throughputInIsolation
                            + " measured group throughput " + groupThroughput);
                    // if the query is penalized
                    if (throughputInIsolation * 0.95 > groupThroughput) {
                        System.out.println("WARNING: Query " + queryId + " is penalized. Throughput in isolation: " +
                                throughputInIsolation + " measured group throughput: " + groupThroughput);

                        if (dopPerGroup[groupId] < DOPinIsolation * groups[groupId].size()) {
                            int dopIncrease = Math.min(resourceIncStep,
                                    DOPinIsolation * groups[groupId].size() - dopPerGroup[groupId]);
                            int newDop = dopPerGroup[groupId] + dopIncrease;
                            reconfigureQueryToChangeDOP(groupId, dopPerGroup[groupId], newDop,
                                    queryCategory, operatorToExecutionVertexMap, taskToResourceIDMap,
                                    true);
                            dopPerGroup[groupId] = newDop;
                        } else {
                            splitGroup(groupId, queryId, operatorToExecutionVertexMap);
                        }
                        // break after triggering a split as the performance of the group
                        // will increase thanks to the split or the resource increase
                        break;
                    }
                }

            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    protected void executeMergeStep(
            int currentIteration, Map<String, ExecutionVertex> operatorToExecutionVertexMap) {
        System.out.println("Checking for merges. Iteration " + currentIteration);

        int numOfActiveGroups = 0;
        for (int i = 0; i < getNumOfQueries(); i++) {
            if (isGroupActive[i]) {
                numOfActiveGroups++;
            }
        }
        if (numOfActiveGroups == 1) {
            System.out.println("Only one group is active. No merges possible.");
            return;
        }

        // active group -> groups to be merged
        HashMap<Integer, HashSet<Integer>> merges = new HashMap<>(getNumOfQueries());
        HashMap<Integer, HashSet<Integer>> groupsPrevState = new HashMap<>(getNumOfQueries());
        boolean mergeFound;

        // Monitoring
        CompletableFuture<List<TaskUtilizationStats>>[] idleTimesFutures = sendMonitoringMessageTaskUtil(operatorToExecutionVertexMap);
        int monitoringGroup = startMonitoringDataDistr(operatorToExecutionVertexMap);
        System.out.println("Monitoring group: " + monitoringGroup);
        try {
            Thread.sleep(monitoringDuration);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        List<FilterDataDistrMergeStats> filterDataDistrStats =
                getDataDistrStatsFilter(monitoringGroup, operatorToExecutionVertexMap);

        HashMap<DataRange, Double> parsedDataDistrStatsFilrer = parseDataDistrStatsFilter(filterDataDistrStats);

        String print = "Selectivities stats: ";
        for (Map.Entry<DataRange, Double> entry : parsedDataDistrStatsFilrer.entrySet()) {
            print += entry.getKey().start + "-" + entry.getKey().end + " -> " + entry.getValue() + ", ";
        }

        JoinDataDistrMergeStats joinDataDistrStats = null;
        if (queryCategory.equals("2_way_join")) {
            joinDataDistrStats =
                    getDataDistrStatsJoin(monitoringGroup, operatorToExecutionVertexMap);

            print += "\nJoin matches stats: ";
            for (Map.Entry<AbstractMap.SimpleImmutableEntry<DataRange, DataRange>, Long> entry : joinDataDistrStats.joinMatchesPerDataRangePair.entrySet()) {
                print += entry.getKey().getKey().start + "-" + entry.getKey().getKey().end + " x " +
                        entry.getKey().getValue().start + "-" + entry.getKey().getValue().end
                        + " -> " + entry.getValue() + ", ";
            }
            print += "\nTotal elements stats: ";
            for (Map.Entry<DataRange, Long> entry : joinDataDistrStats.totalNumElementsPerDataRange.entrySet()) {
                print += entry.getKey().start + "-" + entry.getKey().end + " -> " + entry.getValue()
                        + ", ";
            }
        }
        System.out.println(print);

        do {
            mergeFound = false;
            double minGC = Double.MAX_VALUE;
            int activeGroup = -1; // the group that will execute the new merged query plan
            int passiveGroup = -1; // the group that will get merged and become inactive

            for (int groupId = 0; groupId < getNumOfQueries(); groupId++) {
                if (!isGroupActive[groupId]) {
                    continue;
                }
                for (int groupId2 = groupId + 1; groupId2 < getNumOfQueries(); groupId2++) {
                    if (!isGroupActive[groupId2]) {
                        continue;
                    }
                    // do not merge groups that have no overlapping data ranges
                    boolean noOverlappingDataRangesExist = true;
                    for (int dr : queryToNonOverlappingDataRanges.get(groupId)) {
                        if (queryToNonOverlappingDataRanges.get(groupId2).contains(dr)) {
                            noOverlappingDataRangesExist = false;
                            break;
                        }
                    }
                    if (noOverlappingDataRangesExist) {
                        continue;
                    }

                    double extraWorkByGroup1, extraWorkByGroup2;
                    if (queryCategory.equals("2_way_join")) {
                        extraWorkByGroup1 = getComputationRatioJoin(
                                groups[groupId],
                                groups[groupId2],
                                parsedDataDistrStatsFilrer,
                                joinDataDistrStats);
                        extraWorkByGroup2 = getComputationRatioJoin(
                                groups[groupId2],
                                groups[groupId],
                                parsedDataDistrStatsFilrer,
                                joinDataDistrStats);
                    }
                    else if (queryCategory.equals("aggregation")){
                        extraWorkByGroup1 = getComputationRatioAggregation(
                                groups[groupId],
                                groups[groupId2],
                                parsedDataDistrStatsFilrer);
                        extraWorkByGroup2 = getComputationRatioAggregation(
                                groups[groupId2],
                                groups[groupId],
                                parsedDataDistrStatsFilrer);
                    } else {
                        throw new RuntimeException("Query category not supported.");
                    }

                    try {
                        double resourcesAvailableForExtraWorkByGroup1 = getResourceRatio(
                                dopPerGroup[groupId], dopPerGroup[groupId2], idleTimesFutures[groupId2].get());
                        double resourcesAvailableForExtraWorkByGroup2 = getResourceRatio(
                                dopPerGroup[groupId2], dopPerGroup[groupId], idleTimesFutures[groupId].get());

                        double GCForGroup1 = extraWorkByGroup2
                                / resourcesAvailableForExtraWorkByGroup2; // cost of including group2 into group1
                        double GCForGroup2 = extraWorkByGroup1
                                / resourcesAvailableForExtraWorkByGroup1;  // cost of including group1 into group2
                        double GC = Math.max(GCForGroup1, GCForGroup2);

                        System.out.println("GC for merging " + groupId + " and " + groupId2 +
                                ": " + GC + "( " + extraWorkByGroup2 + " / " + resourcesAvailableForExtraWorkByGroup2 + ", " +
                                extraWorkByGroup1 + " / " + resourcesAvailableForExtraWorkByGroup1 + " )");

                        if (GC < GC_THRESHOLD_MERGE && GC < minGC) {
                            minGC = GC;
                            if (extraWorkByGroup1 > extraWorkByGroup2) {
                                // group1 is the active group
                                activeGroup = groupId;
                                passiveGroup = groupId2;
                            } else {
                                // group2 is the active group
                                activeGroup = groupId2;
                                passiveGroup = groupId;
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            if (minGC < Double.MAX_VALUE) {
                mergeFound = true;
                HashSet<Integer> mergesForActiveGroup = merges.computeIfAbsent(activeGroup, k -> new HashSet<>());
                mergesForActiveGroup.add(passiveGroup);
                if (merges.containsKey(passiveGroup)) {
                    mergesForActiveGroup.addAll(merges.get(passiveGroup));
                    merges.remove(passiveGroup);
                }
                if (!groupsPrevState.containsKey(activeGroup)) {
                    groupsPrevState.put(activeGroup, new HashSet<>(groups[activeGroup]));
                }
                if (!groupsPrevState.containsKey(passiveGroup)) {
                    groupsPrevState.put(passiveGroup, new HashSet<>(groups[passiveGroup]));
                }
                Iterator<Integer> it = groups[passiveGroup].iterator();
                while (it.hasNext()) {
                    int queryToBeMoved = it.next();
                    groups[activeGroup].add(queryToBeMoved);
                }
                groups[passiveGroup].clear();
                isGroupActive[passiveGroup] = false;
            }
        } while (mergeFound);
        for (Map.Entry<Integer, HashSet<Integer>> entry : merges.entrySet()) {
            int activeGroup = entry.getKey();
            HashSet<Integer> passiveGroups = entry.getValue();
            mergeGroups(activeGroup, passiveGroups, groupsPrevState, operatorToExecutionVertexMap,
                    parsedDataDistrStatsFilrer, joinDataDistrStats);
        }
        int totalResourcesInUse = 0;
        for (int i = 0; i < getNumOfQueries(); i++) {
            if (isGroupActive[i]) {
                totalResourcesInUse += dopPerGroup[i];
            }
        }
        String print1 = "Groups: \n";
        for (int i = 0; i < getNumOfQueries(); i++) {
            if (isGroupActive[i]) {
                print1 += "group: " + i + " " + groups[i] + ": " + dopPerGroup[i] + " ";
                for (int query : groups[i]) {
                    print1 +=  filtersPerQuery.get(query) + ", ";
                }
                print1 += "\n";
            }
        }
        System.out.println("End of merging phase. Iteration " + currentIteration +
                " Total resources in use: " + totalResourcesInUse
                + " out of " + DOPinIsolation * getNumOfQueries() + " "
                + print1);
    }

    protected void mergeGroups(
            int activeGroup, Set<Integer> passiveGroups,
            HashMap<Integer, HashSet<Integer>> groupsPrevState,
            Map<String, ExecutionVertex> operatorToExecutionVertexMap,
            HashMap<DataRange, Double> filterDataDistrStats,
            JoinDataDistrMergeStats joinDataDistrStats) {
        List<CompletableFuture<?>> futures1 = new ArrayList<>();
        List<Long> roundtripTimes1 = new ArrayList<>();

        List<CompletableFuture<?>> futures2 = new ArrayList<>();
        List<Long> roundtripTimes2 = new ArrayList<>();

        ArrayList<Integer> oldActiveChannelsActiveGroup = new ArrayList<>(activeChannelsPerGroup.get(activeGroup));
        Set<Integer> queriesToBeMoved = new HashSet<>();
        int oldDOPfromAllPassive = 0;
        for (int passiveGroup : passiveGroups) {
            queriesToBeMoved.addAll(groupsPrevState.get(passiveGroup));
            activeChannelsPerGroup.get(activeGroup).addAll(activeChannelsPerGroup.get(passiveGroup));
            oldDOPfromAllPassive += dopPerGroup[passiveGroup];
        }

        int oldDOPActiveGroup = dopPerGroup[activeGroup];
        int newDOPActiveGroup = calculateNewDOPActiveGroup(
                oldDOPActiveGroup, oldDOPfromAllPassive, groupsPrevState.get(activeGroup),
                queriesToBeMoved, passiveGroups, groupsPrevState, filterDataDistrStats,
                joinDataDistrStats);
        if (newDOPActiveGroup == 0) {
            throw new RuntimeException("New DOP for active group " + activeGroup + " is 0.");
        }
        int newDOPPassiveGroup = DOPinIsolation;
        boolean share = true;

        System.out.println("Group change: Merging groups " + passiveGroups + " into " + activeGroup +
                " new DOP for active group: " + newDOPActiveGroup
                + " max would be: " + DOPinIsolation * (groups[activeGroup].size() + queriesToBeMoved.size()));

        HashMap<Integer, Integer> oldDOPsPassiveGroup = new HashMap<>(passiveGroups.size());
        HashMap<Integer, MIGRATION_TYPE_FROM_PASSIVE_QUERY> migrationTypesPassiveQueryMap = new HashMap<>(passiveGroups.size());
        HashMap<Integer, ArrayList<Integer>> oldActiveChannelsPassiveGroups = new HashMap<>(passiveGroups.size());
        for (int passiveGroup: passiveGroups) {
            int oldDOPPassiveGroup = dopPerGroup[passiveGroup];
            oldDOPsPassiveGroup.put(passiveGroup, oldDOPPassiveGroup);
            ArrayList<Integer> oldActiveChannelsPassiveGroup = new ArrayList<>(activeChannelsPerGroup.get(passiveGroup));
            oldActiveChannelsPassiveGroups.put(passiveGroup, oldActiveChannelsPassiveGroup);

            MIGRATION_TYPE_FROM_PASSIVE_QUERY migrationTypePassiveQuery = getMigrationTypeFromPassive(
                    groupsPrevState.get(activeGroup), groupsPrevState.get(passiveGroup), queryCategory);
            migrationTypesPassiveQueryMap.put(passiveGroup, migrationTypePassiveQuery);
            ReconfigurationControlMessage.RangeForMigration rangeForMigration = null;
            if (migrationTypePassiveQuery == MIGRATION_TYPE_FROM_PASSIVE_QUERY.KEY_RANGE) {
                //rangeForMigration = getRangeForMigration(activeQuery, passiveQuery);
                throw new RuntimeException("Reconfiguration with partial state migration for multiple "
                        + "queries is not supported yet. For 2 queries, it is available");
            }

            activeChannelsPerGroup.get(passiveGroup).clear();
            ControlMessage controlMessagePassiveGroup = getControlMessageForPassiveGroup(
                    oldDOPActiveGroup, newDOPActiveGroup, oldDOPPassiveGroup, newDOPPassiveGroup,
                    activeGroup, passiveGroup, passiveGroups, queryCategory,
                    queriesToBeMoved, share, querySets[passiveGroup], migrationTypePassiveQuery,
                    null, taskToResourceIDMap, oldActiveChannelsActiveGroup,
                    activeChannelsPerGroup.get(activeGroup),
                    activeChannelsPerGroup.get(passiveGroup), oldActiveChannelsPassiveGroup);
            sendControlMessage(controlMessagePassiveGroup, passiveGroup, operatorToExecutionVertexMap,
                    futures2, roundtripTimes2);

            Iterator<Integer> it = groupsPrevState.get(passiveGroup).iterator();
            while (it.hasNext()) {
                int queryToBeMoved = it.next();
                groups[activeGroup].add(queryToBeMoved);
            }
        }

        ControlMessage controlMessageForActiveGroup = getControlMessageForActiveGroup(
                oldDOPActiveGroup, newDOPActiveGroup,
                activeGroup, passiveGroups, queryCategory,
                queriesToBeMoved, share, querySets[activeGroup], migrationTypesPassiveQueryMap,
                taskToResourceIDMap, oldActiveChannelsActiveGroup,
                activeChannelsPerGroup.get(activeGroup), oldActiveChannelsPassiveGroups);
        sendControlMessage(controlMessageForActiveGroup, activeGroup, operatorToExecutionVertexMap,
                futures1, roundtripTimes1);

        dopPerGroup[activeGroup] = newDOPActiveGroup;
        for (int passiveGroup : passiveGroups) {
            dopPerGroup[passiveGroup] = newDOPPassiveGroup;
        }

        printRoundTripTime(futures1, roundtripTimes1, "[Active Group]");
        printRoundTripTime(futures2, roundtripTimes2, "[Passive Group]");
    }

    private int calculateNewDOPActiveGroup(
            int oldDOPactive, int oldDOPpassive, Set<Integer> activeGroup, Set<Integer> queriesToBeMoved,
            Set<Integer> passiveGroups, HashMap<Integer, HashSet<Integer>> groupsPrevState,
            HashMap<DataRange, Double> filterDataDistrStats,
            JoinDataDistrMergeStats joinDataDistrStats) {
        int maxNewDOP = oldDOPpassive + oldDOPactive;
        int newDOPNeeded = 0;
        for (int passiveGroup : passiveGroups) {
            Set<Integer> queriesBeingAdded = new HashSet<>();
            for (int passiveGroup2 : passiveGroups) {
                if (passiveGroup2 != passiveGroup) {
                    queriesBeingAdded.addAll(groupsPrevState.get(passiveGroup2));
                }
            }
            queriesBeingAdded.addAll(activeGroup);
            double dop;
            if (queryCategory.equals("2_way_join"))
                dop = calculateResourcesNeededJoin(queriesBeingAdded, groupsPrevState.get(passiveGroup), filterDataDistrStats,
                        joinDataDistrStats, dopPerGroup[passiveGroup]);
            else
                dop = calculateResourcesNeededAggregation(queriesBeingAdded, groupsPrevState.get(passiveGroup), filterDataDistrStats,
                    dopPerGroup[passiveGroup]);
            if (dop > newDOPNeeded) {
                newDOPNeeded = (int) Math.ceil(dop);
            }
        }

        double dop;
        if (queryCategory.equals("2_way_join"))
            dop = calculateResourcesNeededJoin(queriesToBeMoved, activeGroup, filterDataDistrStats,
                joinDataDistrStats, oldDOPactive);
        else
            dop = calculateResourcesNeededAggregation(queriesToBeMoved, activeGroup, filterDataDistrStats,
                oldDOPactive);
        if (dop > newDOPNeeded) {
            newDOPNeeded = (int) Math.ceil(dop);
        }
        if (newDOPNeeded > maxNewDOP || newDOPNeeded < 0) {
            // we should never hit this. Just adding it for debugging purposes
            throw new RuntimeException("New dop for group " + activeGroup + " not achievable. " +
                    "New dop needed: " + newDOPNeeded + " max dop: " + maxNewDOP);
        }
        System.out.println("DOP needed for adding " + queriesToBeMoved + " into " +
                activeGroup + ": " + newDOPNeeded
                + " old DOP: " + oldDOPactive + " max new dop: " + maxNewDOP);
        return newDOPNeeded;
    }

    private int calculateResourcesNeededJoin(
            Set<Integer> queriesToBeMoved, Set<Integer> group,
            HashMap<DataRange, Double> filterDataDistrStats,
            JoinDataDistrMergeStats joinDataDistrStats, int oldDOP) {
        double computationRatio = getComputationRatioJoin(queriesToBeMoved, group,
                filterDataDistrStats, joinDataDistrStats);
        double resourceIncreaseNeeded = ((computationRatio / GC_THRESHOLD_MERGE) * oldDOP) /
                ( 1 - computationRatio / GC_THRESHOLD_MERGE);
        return oldDOP + (int) Math.ceil(resourceIncreaseNeeded);
    }

    private int calculateResourcesNeededAggregation(
            Set<Integer> queriesToBeMoved, Set<Integer> group,
            HashMap<DataRange, Double> filterDataDistrStats, int oldDOP) {
        double computationRatio = getComputationRatioAggregation(queriesToBeMoved, group,
                filterDataDistrStats);
        double resourceIncreaseNeeded = ((computationRatio / GC_THRESHOLD_MERGE) * oldDOP) /
                ( 1 - computationRatio / GC_THRESHOLD_MERGE);
        return oldDOP + (int) Math.ceil(resourceIncreaseNeeded);
    }

    // Including group 1 into group 2
    protected double getResourceRatio(int resourcesOfGroup1, int resourcesOfGroup2, List<TaskUtilizationStats> avgIdleMsPerSecGroup2) {
        double avg = avgIdleMsPerSecGroup2.stream().mapToDouble(s -> s.idleTimeMsPerSecond + s.backPressuredTimeMsPerSecond).average().orElse(0);
        double idleResourcesOfGroup2 = resourcesOfGroup2 * avg / 1000.0;
        return ((double)resourcesOfGroup1 + idleResourcesOfGroup2) / (resourcesOfGroup2 + resourcesOfGroup1);

    }

    // Computation cost ratio of including group1 into group2
    protected double getComputationRatioJoin(Set<Integer> group1, Set<Integer> group2,
                                             HashMap<DataRange, Double> filterDataDistrStats,
                                             JoinDataDistrMergeStats joinDataDistrStats) {
        HashSet<DataRange> rangesOfGroup2 = getNonOverlappingRangesOfGroup(group2);
        Set<Integer> newGroup = new HashSet<>(group2.size() + group1.size());
        newGroup.addAll(group2);
        newGroup.addAll(group1);
        HashSet<DataRange> rangesOfNewGroup = getNonOverlappingRangesOfGroup(newGroup);
        double selectivityGroup2 = getSelectivity(rangesOfGroup2, filterDataDistrStats);
        double selectivityNewGroup = getSelectivity(rangesOfNewGroup, filterDataDistrStats);
        double joinMatchesGroup2 = getUpscaledJoinMatchesFromRanges(
                rangesOfGroup2, joinDataDistrStats, inputRate * selectivityGroup2 * windowSizeInSec);
        double joinMatchesNewGroup = getUpscaledJoinMatchesFromRanges(
                rangesOfNewGroup, joinDataDistrStats, inputRate * selectivityNewGroup * windowSizeInSec);

        // cost = cost of new group - cost of old group / cost of new group
        double costOfGroup2 = Math.max(0, getCostEstimationJoin(selectivityGroup2, joinMatchesGroup2));
        double costOfNewGroup = Math.max(0, getCostEstimationJoin(selectivityNewGroup, joinMatchesNewGroup));

        double computationRatio = costOfNewGroup != 0 ?
                (costOfNewGroup - costOfGroup2) / costOfNewGroup : 0;

        System.out.println("Computation ratio for adding group " + group1 + " into group " + group2 +
                computationRatio +
                " selectivity group 2: " + selectivityGroup2 + " selectivity new group: " + selectivityNewGroup
                + " ranges of group 2: " + rangesOfGroup2 + " ranges of new group: " + rangesOfNewGroup +
                " join matches group 2: " + joinMatchesGroup2 + " join matches new group: " + joinMatchesNewGroup);
        return computationRatio;
    }

    protected double getComputationRatioAggregation(Set<Integer> group1, Set<Integer> group2,
                                             HashMap<DataRange, Double> filterDataDistrStats) {
        HashSet<DataRange> rangesOfGroup2 = getNonOverlappingRangesOfGroup(group2);
        Set<Integer> newGroup = new HashSet<>(group2.size() + group1.size());
        newGroup.addAll(group2);
        newGroup.addAll(group1);
        HashSet<DataRange> rangesOfNewGroup = getNonOverlappingRangesOfGroup(newGroup);
        double selectivityGroup2 = getSelectivity(rangesOfGroup2, filterDataDistrStats);
        double selectivityNewGroup = getSelectivity(rangesOfNewGroup, filterDataDistrStats);

        // cost = cost of new group - cost of old group / cost of new group
        double costOfGroup2 = getCostEstimationAggregation(selectivityGroup2);
        double costOfNewGroup = getCostEstimationAggregation(selectivityNewGroup);

        double computationRatio = (costOfNewGroup - costOfGroup2) / costOfNewGroup;

        System.out.println("Computation ratio for adding group " + group1 + " into group " + group2 +
                computationRatio +
                " selectivity group 2: " + selectivityGroup2 + " selectivity new group: " + selectivityNewGroup
                + " ranges of group 2: " + rangesOfGroup2 + " ranges of new group: " + rangesOfNewGroup);
        return computationRatio;
    }

    private double estimateThroughput(String queryCategory, double selectivity, double joinMatches) {
        double perTupleCostMicrosec = getCostEstimationJoin(selectivity, joinMatches);
        return 1000000 / perTupleCostMicrosec;
    }

    /**
     * This function basically returns the estimated cost per tuple in microseconds
     * @param selectivity
     * @param joinMatches
     * @return
     */
    private double getCostEstimationJoin(double selectivity, double joinMatches) {
        return 63.3398769231 + 19.0587060289 * ((selectivity - 0.524615384615384) / 0.29227530184914)
                + 19.0587060289 * ((joinMatches - 521.73) / 290.667787688969);

    }

    // This function basically returns the estimated cost per tuple in microseconds
    private double getCostEstimationAggregation(double selectivity) {
        return 30.20906 * selectivity + 0.92372;
    }

    /**
     * (2 way join)
     * Follows similar logic to the function upscale join matches
     */
    private double getUpscaledJoinMatchesFromRanges(
            HashSet<DataRange> dataRanges, JoinDataDistrMergeStats joinDataDistrStats, double targetSize) {
        long joinMatches = 0;
        long totalElements = 0;
        for (DataRange dataRange1 : dataRanges) {
            for (DataRange dataRange2 : dataRanges) {
                if (dataRange1.start > dataRange2.start) {
                    continue;
                }
                AbstractMap.SimpleImmutableEntry<DataRange, DataRange> pair =
                        new AbstractMap.SimpleImmutableEntry<>(dataRange1, dataRange2);
                joinMatches += joinDataDistrStats.joinMatchesPerDataRangePair.getOrDefault(pair, 0L);
            }
            totalElements += joinDataDistrStats.totalNumElementsPerDataRange.getOrDefault(dataRange1, 0L);
        }
        return joinMatches * targetSize / Math.pow(totalElements, 2);
    }

    private double getSelectivity(HashSet<DataRange> ranges, HashMap<DataRange, Double> filterDataDistrStats) {
        double selectivity = 0;
        for (DataRange range : ranges) {
            selectivity += getSelectivity(range, filterDataDistrStats);
        }
        return selectivity;
    }

    private double getSelectivity(DataRange range, HashMap<DataRange, Double> filterDataDistrStats) {
        return filterDataDistrStats.getOrDefault(range, 0.0);
    }

    private HashSet<DataRange> getNonOverlappingRangesOfGroup(Set<Integer> queriesOfGroup) {
        HashSet<DataRange> rangesOfGroup = new HashSet<>();
        for (int queryId : queriesOfGroup) {
            rangesOfGroup.addAll(queryToNonOverlappingDataRanges.get(queryId).stream()
                    .map(dataRangeIndex -> nonOverlappingDataRanges[dataRangeIndex])
                    .collect(Collectors.toList()));
        }
        return rangesOfGroup;
    }

    private void splitGroup(int groupId, int penalizedQueryId,
                            Map<String, ExecutionVertex> operatorToExecutionVertexMap) {
        Set<Integer> subgroupToSplit = new HashSet<>(1);
        subgroupToSplit.add(penalizedQueryId);
        Set<Integer> newGroups = findEmptyGroups(1);
        removeQueriesFromGroup(subgroupToSplit, groupId, newGroups, operatorToExecutionVertexMap);
    }

    private Set<Integer> findEmptyGroups(int size) {
        Set<Integer> emptyGroups = new HashSet<>(size);
        for (int i = 0; i < getNumOfQueries(); i++) {
            if (groups[i].isEmpty()) {
                emptyGroups.add(i);
            }
            if (emptyGroups.size() == size) {
                break;
            }
        }
        if (emptyGroups.size() < size) {
            throw new RuntimeException("Not enough empty groups to split the subgroup");
        }
        return emptyGroups;
    }

    protected void removeQueriesFromGroup(Set<Integer> queriesToBeSplit, int groupIdOldGroup,
                                          Set<Integer> groupIdNewGroups,
                                      Map<String, ExecutionVertex> operatorToExecutionVertexMap) {
        assert queriesToBeSplit.size() == groupIdNewGroups.size();
        System.out.println("Group change: Removing queries " + queriesToBeSplit
                + " from group " + groupIdOldGroup +
                " and adding them to groups " + groupIdNewGroups);

        List<CompletableFuture<?>> futures1 = new ArrayList<>();
        List<Long> roundtripTimes1 = new ArrayList<>();

        List<CompletableFuture<?>> futures2 = new ArrayList<>();
        List<Long> roundtripTimes2 = new ArrayList<>();

        int oldDOPforOldGroup = dopPerGroup[groupIdOldGroup];
        int newDOPforOldGroup = DOPinIsolation * (groups[groupIdOldGroup].size() - queriesToBeSplit.size());
        boolean share = false;

        ArrayList<Integer> oldActiveChannelsOldGroup = new ArrayList<>(activeChannelsPerGroup.get(groupIdOldGroup));
        HashMap<Integer, Integer> oldDOPsNewGroups = new HashMap<>(groupIdNewGroups.size());
        HashMap<Integer, MIGRATION_TYPE_FROM_PASSIVE_QUERY> migrationTypePassiveQueryMap = new HashMap<>(groupIdNewGroups.size());
        for (int groupIdNewGroup: groupIdNewGroups){
            int queryId = queriesToBeSplit.iterator().next();
            queriesToBeSplit.remove(queryId);
            Set<Integer> queriesToBeMoved = new HashSet<>(1);
            queriesToBeMoved.add(queryId);

            int oldDOPforNewGroup = dopPerGroup[groupIdNewGroup];
            oldDOPsNewGroups.put(groupIdNewGroup, oldDOPforNewGroup);
            int newDOPforNewGroup = DOPinIsolation * (groups[groupIdNewGroup].size() + 1);

            MIGRATION_TYPE_FROM_PASSIVE_QUERY migrationTypePassiveQuery =
                    MIGRATION_TYPE_FROM_PASSIVE_QUERY.NO_MIGRATION;
            migrationTypePassiveQueryMap.put(groupIdNewGroup, migrationTypePassiveQuery);

            // optimization for the case of subgroup with size 1

            int channel = groupIdNewGroup;
            for (int i = 1; i <= newDOPforNewGroup; i++) {
                assert activeChannelsPerGroup.get(groupIdOldGroup).contains(channel);
                activeChannelsPerGroup.get(groupIdOldGroup).remove(channel);
                activeChannelsPerGroup.get(groupIdNewGroup).add(channel);
                channel++;
            }

            groups[groupIdOldGroup].remove(queryId);
            groups[groupIdNewGroup].add(queryId);
            isGroupActive[groupIdNewGroup] = true;

            ControlMessage controlMessageNewGroup = getControlMessageForPassiveGroup(
                    oldDOPforOldGroup, newDOPforOldGroup, oldDOPforNewGroup, newDOPforNewGroup,
                    groupIdOldGroup, groupIdNewGroup, groupIdNewGroups, queryCategory,
                    queriesToBeMoved, share, querySets[groupIdNewGroup], migrationTypePassiveQuery,
                    null, taskToResourceIDMap, oldActiveChannelsOldGroup,
                    activeChannelsPerGroup.get(groupIdOldGroup),
                    activeChannelsPerGroup.get(groupIdNewGroup), null);
            sendControlMessage(controlMessageNewGroup, groupIdNewGroup, operatorToExecutionVertexMap,
                    futures2, roundtripTimes2);
        }

        ControlMessage controlMessageOldGroup = getControlMessageForActiveGroup(
                oldDOPforOldGroup, newDOPforOldGroup,
                groupIdOldGroup, groupIdNewGroups, queryCategory,
                queriesToBeSplit, share, querySets[groupIdOldGroup], migrationTypePassiveQueryMap,
                taskToResourceIDMap, oldActiveChannelsOldGroup,
                activeChannelsPerGroup.get(groupIdOldGroup), null);
        sendControlMessage(controlMessageOldGroup, groupIdOldGroup, operatorToExecutionVertexMap,
                futures1, roundtripTimes1);

        dopPerGroup[groupIdOldGroup] = newDOPforOldGroup;
        for (int groupIdNewGroup : groupIdNewGroups) {
            dopPerGroup[groupIdNewGroup] = DOPinIsolation;
        }

        printRoundTripTime(futures1, roundtripTimes1, "[Active Group]");
        printRoundTripTime(futures2, roundtripTimes2, "[Passive Group]");
    }

    private void reconfigureQueryToChangeDOP(
            int queryId, int oldDOP, int newDOP, String queryCategory,
            Map<String, ExecutionVertex> operatorToExecutionVertexMap,
            HashMap<String, ResourceID> taskToResourceIDMap, boolean enableSplitMonitoring) {
        List<CompletableFuture<?>> futures = new ArrayList<>();
        List<Long> roundtripTimes = new ArrayList<>();
        ArrayList<Integer> oldActiveChannels;
        if (activeChannelsPerGroup.get(queryId).size() >= oldDOP) {
            oldActiveChannels = activeChannelsPerGroup.get(queryId);
        }
        else {
            // this happens in the 1st reconfiguration that configures queries to run in isolation
            oldActiveChannels = new ArrayList<>(oldDOP);
            for (int i = 0; i < oldDOP; i++) {
                oldActiveChannels.add(i);
            }
        }
        ControlMessage controlMessage = getControlMessageForChangingDOP(oldDOP, newDOP, queryId,
                queryCategory, taskToResourceIDMap, oldActiveChannels,
                activeChannelsPerGroup.get(queryId), enableSplitMonitoring);
        sendControlMessage(controlMessage, queryId, operatorToExecutionVertexMap, futures,
                roundtripTimes);
        printRoundTripTime(futures, roundtripTimes,
                "[Configuring Group " + queryId + " to run with dop " + newDOP + "]");
    }

    protected void reconfigureQueriesToStartWithDOPInIsolation(
            String queryCategory,
            Map<String, ExecutionVertex> operatorToExecutionVertexMap,
            HashMap<String, ResourceID> taskToResourceIDMap) {
        int oldDOP = getNumOfQueries() * DOPinIsolation;
        for (int i = 0; i < getNumOfQueries(); i++) {
            reconfigureQueryToChangeDOP(i, oldDOP, DOPinIsolation, queryCategory,
                    operatorToExecutionVertexMap, taskToResourceIDMap, false);
        }
    }

    // returns the average percentage of idle time in ms per second
    private CompletableFuture<List<TaskUtilizationStats>>[] sendMonitoringMessageTaskUtil(
            Map<String, ExecutionVertex> operatorToExecutionVertexMap) {
        CompletableFuture<List<TaskUtilizationStats>>[] idleTimesFutures = new CompletableFuture[getNumOfQueries()];
        for (int groupId = 0; groupId < getNumOfQueries(); groupId++) {
            if (!isGroupActive[groupId]) {
                continue;
            }
            ArrayList<CompletableFuture<TaskUtilizationStats>> futures = new ArrayList<>();
            Set<String> targetTasks = filterTargetTasksWithDOP(
                    targetWorkers.get(queryCategory).get(groupId), activeChannelsPerGroup.get(groupId),
                    dopPerGroup[groupId]);

            sendTaskUtilStatsMonitoringMessage(targetTasks, operatorToExecutionVertexMap, futures);

            CompletableFuture<List<TaskUtilizationStats>> futureList = CompletableFuture.allOf(
                            futures.toArray(new CompletableFuture[futures.size()]))
                    .thenApply(v -> futures.stream()
                            .map(CompletableFuture::join)
                            .collect(Collectors.toList()));
            idleTimesFutures[groupId] = futureList;
        }
        return idleTimesFutures;
    }

    private CompletableFuture<List<DataDistrSplitStats>>[] getSplitStats(
            Map<String, ExecutionVertex> operatorToExecutionVertexMap) {
        CompletableFuture<List<DataDistrSplitStats>>[] splitStatsFutures = new CompletableFuture[getNumOfQueries()];
        for (int groupId = 0; groupId < getNumOfQueries(); groupId++) {
            if (!isGroupActive[groupId] || groups[groupId].size() == 1) {
                continue;
            }
            ArrayList<CompletableFuture<DataDistrSplitStats>> futures = new ArrayList<>();
            Set<String> targetTasks = filterTargetTasksWithDOP(
                    targetWorkers.get(queryCategory).get(groupId), activeChannelsPerGroup.get(groupId),
                    dopPerGroup[groupId]);
            System.out.println("Target tasks for split monitoring: " + targetTasks + " for group " + groupId +
                    " active channels: " + activeChannelsPerGroup.get(groupId) + " DOP: " + dopPerGroup[groupId]);

            getSplitPhaseStats(targetTasks, operatorToExecutionVertexMap, futures);

            CompletableFuture<List<DataDistrSplitStats>> futureList = CompletableFuture.allOf(
                            futures.toArray(new CompletableFuture[futures.size()]))
                    .thenApply(v -> futures.stream()
                            .map(CompletableFuture::join)
                            .collect(Collectors.toList()));

            splitStatsFutures[groupId] = futureList;//avgThroughputFuture;
        }
        return splitStatsFutures;
    }

    private CompletableFuture<Double>[] getThroughputMeasurements(
            Map<String, ExecutionVertex> operatorToExecutionVertexMap) {
        CompletableFuture<Double>[] tputFutures = new CompletableFuture[getNumOfQueries()];
        for (int groupId = 0; groupId < getNumOfQueries(); groupId++) {
            if (!isGroupActive[groupId] || groups[groupId].size() == 1) {
                continue;
            }
            ArrayList<CompletableFuture<Double>> futures = new ArrayList<>();
            Set<String> sourceTasks = sources.get(groupId);

            sendThroughputMonitoringMessage(sourceTasks, operatorToExecutionVertexMap, futures);

            CompletableFuture<List<Double>> futureList = CompletableFuture.allOf(
                            futures.toArray(new CompletableFuture[futures.size()]))
                    .thenApply(v -> futures.stream()
                            .map(CompletableFuture::join)
                            .collect(Collectors.toList()));

            CompletableFuture<Double> avgThroughputFuture = futureList.thenApply(
                    tputs -> tputs.stream().mapToDouble(Double::doubleValue).sum()
            );
            tputFutures[groupId] = avgThroughputFuture;
        }
        return tputFutures;
    }

    private int startMonitoringDataDistr(
            Map<String, ExecutionVertex> operatorToExecutionVertexMap) {
        int groupWithMaxRange = 0;
        int maxSel = 0;
        for (int i = 0; i < getNumOfQueries(); i++) {
            if (isGroupActive[i]) {
                int totalSel = 0;
                for (Integer rangeIdx : queryToNonOverlappingDataRanges.get(i)) {
                    DataRange range = nonOverlappingDataRanges[rangeIdx];
                    totalSel += range.end - range.start + 1;
                }
                if (totalSel > maxSel) {
                    maxSel = totalSel;
                    groupWithMaxRange = i;
                }
            }
        }
        HashSet<Integer> activeQueriesOfMonitoringGroup = groups[groupWithMaxRange];
        List<CompletableFuture<?>> futures = new ArrayList<>();
        Set<String> receivers = getFilterTasksForGroup(groupWithMaxRange);
        
        StartMonitoringDataDistrControlMessage controlMessage = new StartMonitoringDataDistrControlMessage(
                graphWithMapping.get(groupWithMaxRange), queryToNonOverlappingDataRanges,
                nonOverlappingDataRanges, activeQueriesOfMonitoringGroup);

        for (String workerName : receivers) {
            ExecutionVertex worker = operatorToExecutionVertexMap.get(workerName);
            futures.add(((ReconfigurableExecutionVertex) worker).sendControlMessage(controlMessage));
        }

        return groupWithMaxRange;
    }

    private List<FilterDataDistrMergeStats> getDataDistrStatsFilter(
            int groupId, Map<String, ExecutionVertex> operatorToExecutionVertexMap) {
        ArrayList<CompletableFuture<?>> futures = new ArrayList<>();
        Set<String> receivers = getFilterTasksForGroup(groupId);

        StopMonitoringDataDistrControlMessage controlMessage =
                new StopMonitoringDataDistrControlMessage(
                        graphWithMapping.get(groupId), StopMonitoringDataDistrControlMessage.Type.FILTER);

        for (String workerName : receivers) {
            ExecutionVertex worker = operatorToExecutionVertexMap.get(workerName);
            futures.add(((ReconfigurableExecutionVertex) worker).sendControlMessage(controlMessage));
        }

        CompletableFuture<List<FilterDataDistrMergeStats>> futureList = CompletableFuture.allOf(
                futures.toArray(new CompletableFuture[futures.size()]))
                .thenApply(v -> (List<FilterDataDistrMergeStats>) futures.stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toList()));

        List<FilterDataDistrMergeStats> filterStats;
        try {
            filterStats = futureList.get();
            String p = ("Received filter stats: \n" );
            for (FilterDataDistrMergeStats stats : filterStats) {
                p += stats + "\n";
            }
            System.out.println(p);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }

        return filterStats;
    }

    private HashMap<DataRange, Double> parseDataDistrStatsFilter(List<FilterDataDistrMergeStats> filterStats){
        FilterDataDistrMergeStats aggregatedStats = new FilterDataDistrMergeStats();
        for (FilterDataDistrMergeStats stats : filterStats) {
            for (Map.Entry<DataRange, Long> entry : stats.selectivityPerDataRange.entrySet()) {
                DataRange range = entry.getKey();
                Long selectivity = entry.getValue();
                aggregatedStats.selectivityPerDataRange.put(range,
                        aggregatedStats.selectivityPerDataRange.getOrDefault(range, 0L) + selectivity);
            }
            aggregatedStats.totalNumElements += stats.totalNumElements;
        }

        HashMap<DataRange, Double> aggregatedSelectivitiesPerRange = new HashMap<>();
        for (Map.Entry<DataRange, Long> entry : aggregatedStats.selectivityPerDataRange.entrySet()) {
            DataRange range = entry.getKey();
            Long selectivity = entry.getValue();
            aggregatedSelectivitiesPerRange.put(range, selectivity / (double) aggregatedStats.totalNumElements);
        }

        return aggregatedSelectivitiesPerRange;
    }

    private JoinDataDistrMergeStats getDataDistrStatsJoin(
            int groupId, Map<String, ExecutionVertex> operatorToExecutionVertexMap) {
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        ArrayList<CompletableFuture<?>> futures = new ArrayList<>();
        Set<String> receivers = getJoinTasksForGroup(groupId);

        StopMonitoringDataDistrControlMessage controlMessage =
                new StopMonitoringDataDistrControlMessage(
                        graphWithMapping.get(groupId), StopMonitoringDataDistrControlMessage.Type.JOIN);

        for (String workerName : receivers) {
            ExecutionVertex worker = operatorToExecutionVertexMap.get(workerName);
            futures.add(((ReconfigurableExecutionVertex) worker).sendControlMessage(controlMessage));
        }

        CompletableFuture<List<JoinDataDistrMergeStats>> futureList = CompletableFuture.allOf(
                        futures.toArray(new CompletableFuture[futures.size()]))
                .thenApply(v -> (List<JoinDataDistrMergeStats>) futures.stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toList()));

        List<JoinDataDistrMergeStats> joinStats;
        try {
            joinStats = futureList.get();
            String p = ("Received join stats: \n" );
            for (JoinDataDistrMergeStats stats : joinStats) {
                p += stats + "\n";
            }
            System.out.println(p);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }

        JoinDataDistrMergeStats aggregatedStats = new JoinDataDistrMergeStats();
        for (JoinDataDistrMergeStats stats : joinStats) {
            if ((stats.actualSampleSizeA != stats.targetSampleSizeA ||
                stats.actualSampleSizeB != stats.targetSampleSizeB) && stats.actualSampleSizeA != 0) {
                System.out.println("WARNING: Sample size for join stats was not reached!");
            }
            for (Map.Entry<AbstractMap.SimpleImmutableEntry<DataRange, DataRange>, Long> entry :
                    stats.joinMatchesPerDataRangePair.entrySet()) {
                AbstractMap.SimpleImmutableEntry<DataRange, DataRange> ranges = entry.getKey();
                Long val = entry.getValue();
                aggregatedStats.joinMatchesPerDataRangePair.put(ranges,
                        aggregatedStats.joinMatchesPerDataRangePair.getOrDefault(ranges, 0L) + val);
            }
            for (Map.Entry<DataRange, Long> entry : stats.totalNumElementsPerDataRange.entrySet()) {
                DataRange range = entry.getKey();
                Long total = entry.getValue();
                aggregatedStats.totalNumElementsPerDataRange.put(range,
                        aggregatedStats.totalNumElementsPerDataRange.getOrDefault(range, 0L) + total);
            }
        }

        return aggregatedStats;
    }

    private Set<String> filterTargetTasksWithDOP(
            Set<String> targetTasks, ArrayList<Integer> activeChannels, int dop) {
        Set<String> res = new HashSet<>();
        for (String taskName : targetTasks) {
            String[] temp = taskName.split("-");
            int index = Integer.parseInt(temp[temp.length - 1]);
            int channelIndex = activeChannels.indexOf(index);
            if (channelIndex >=0 && channelIndex < dop) {
                res.add(taskName);
            }
        }
        return res;
    }

    private Set<String> getFilterTasksForGroup(int groupId) {
        return getTasksForGroupByName(groupId, "QuerySetAssigner");
    }

    private Set<String> getJoinTasksForGroup(int groupId) {
        return getTasksForGroupByName(groupId, "Join");
    }

    private Set<String> getTasksForGroupByName(int groupId, String name) {
        Set<String> res = new HashSet<>();
        for (String taskName : graphWithMapping.get(groupId).keySet()) {
            if (taskName.contains(name)) {
                res.add(taskName);
            }
        }
        return res;
    }

    private DataRange[] getNonOverlappingDataRanges(Map<Integer, DataRange> filtersPerQuery) {
        // Step 1: Collect all unique points where ranges start or end
        TreeSet<Long> points = new TreeSet<>();
        for (DataRange range : filtersPerQuery.values()) {
            points.add(range.start);
            points.add(range.end + 1); // Add point after end to create proper segments
        }

        // Step 2: Create non-overlapping ranges between consecutive points
        List<DataRange> nonOverlappingRanges = new ArrayList<>();
        Long prevPoint = null;
        for (Long point : points) {
            if (prevPoint != null) {
                // Only create a range if at least one original range contains this interval
                boolean isContainedInAnyRange = false;
                for (DataRange originalRange : filtersPerQuery.values()) {
                    if (originalRange.start <= prevPoint && originalRange.end >= point - 1) {
                        isContainedInAnyRange = true;
                        break;
                    }
                }

                if (isContainedInAnyRange) {
                    nonOverlappingRanges.add(new DataRange(prevPoint, point - 1));
                }
            }
            prevPoint = point;
        }

        DataRange[] res = new DataRange[nonOverlappingRanges.size()];
        for (int i = 0; i < nonOverlappingRanges.size(); i++) {
            res[i] = nonOverlappingRanges.get(i);
        }
        return res;
    }

    private HashMap<Integer, HashSet<Integer>> getQueryToNonOverlappingDataRanges(
            DataRange[] nonOverlappingDataRanges) {
        HashMap<Integer, HashSet<Integer>> queryToRanges = new HashMap<>();

        // Initialize empty sets for each query
        for (Integer queryId : filtersPerQuery.keySet()) {
            queryToRanges.put(queryId, new HashSet<>());
        }

        // For each non-overlapping range, find which queries contain it
        for (int rangeIndex = 0; rangeIndex < nonOverlappingDataRanges.length; rangeIndex++) {
            DataRange currentRange = nonOverlappingDataRanges[rangeIndex];

            // Check each query's range
            for (Map.Entry<Integer, DataRange> entry : filtersPerQuery.entrySet()) {
                Integer queryId = entry.getKey();
                DataRange queryRange = entry.getValue();

                // If the current range is contained within the query's range
                if (queryRange.start <= currentRange.start && queryRange.end >= currentRange.end) {
                    queryToRanges.get(queryId).add(rangeIndex);
                }
            }
        }
        return queryToRanges;
    }

    /**
     * (2 way join)
     * Returns the estimated number of joinMatches per tuple in a set of targetSize elements
     * based on the number of join matches in a sample of sampleSize elements
     * @param joinMatchesInSample how many join matches are in the sample
     * @param targetSize how many elements we want to estimate the join matches for (!! This is the
     * number of tuples that the join receives in a window)
     * @param sampleSize how many elements the sample has
     */
    private double upscaleJoinMatches(double joinMatchesInSample, double targetSize, int sampleSize) {
        // the number of joinMatches in targetSize elements is
        // joinMatchesInSample * (targetSize / sampleSize)^2 (power of 2 because it is 2 way join)
        // the number of joinMatches per tuple is
        // joinMatchesInSample * (targetSize / sampleSize)^2 / targetSize
        return joinMatchesInSample * targetSize/
                Math.pow(sampleSize, 2);
    }

    private double getSelectivityFromRanges(HashSet<DataRange> ranges) {
        double selectivity = 0;
        for (DataRange range : ranges) {
            selectivity += (range.end - range.start + 1) / 100.0;
        }
        return selectivity;
    }

}
