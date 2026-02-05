package org.apache.flink.extensions.controller;

import net.michaelkoepf.spegauge.api.sut.DataRange;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.extensions.reconfiguration.ReconfigurableExecutionVertex;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * IMPORTANT NOTE:
 * TLDR: This trait must be serializable. Do not include in the state of the class any non-serializable objects.
 * (e.g. the ExecutionVertex object is not serializable)
 * Ofc any objects inheriting this class must be serializable as well.
 *
 * Details: When a control message is sent to a worker, this incurs an RPC call. Flink serializes
 * the control message but also the object graph of the control message. This means that the Optimizer
 * will be serialized as well.
 */
public abstract class Optimizer implements Serializable {
    HashSet<String> emptySet = new HashSet<>(1);
    HashMap<ResourceID, HashSet<String>> emptyMap = new HashMap<>(1);
    // this is constant as we have specific query templates
    HashMap<String, Boolean> filterOnAttributeUsedToOrganizeState = new HashMap<>(Map.of(
            "2_way_join", false,
            "3_way_join", false,
            "aggregation", false // in some cases can be true
    ));

    String jobID = "";
    HashMap<Integer, HashMap<String, HashSet<String>>> graphWithMapping = null;
    HashMap<String, HashMap<Integer, HashSet<String>>> targetWorkers = null;
    HashMap<Integer, HashSet<String>> sources = null;
    int DOPmax = 0;
    int driverPort = 0;
    HashMap<String, ArrayList<Integer>> queryCategories = null;
    HashMap<Integer, DataRange> filtersPerQuery = null;

    protected void init(String jobID,
                      HashMap<Integer, HashMap<String, HashSet<String>>> graphWithMapping,
                      HashMap<String, HashMap<Integer, HashSet<String>>> targetWorkers,
                      HashMap<String, ArrayList<Integer>> queryCategories,
                      HashMap<Integer, DataRange> filtersPerQuery,
                      HashMap<Integer, HashSet<String>> sources,
                      int DOPmax, int driverPort) {
        this.jobID = jobID;
        this.graphWithMapping = graphWithMapping;
        this.targetWorkers = targetWorkers;
        this.queryCategories = queryCategories;
        this.filtersPerQuery = filtersPerQuery;
        this.sources = sources;
        this.DOPmax = DOPmax;
        this.driverPort = driverPort;
    }

    public abstract void start(Map<String, ExecutionVertex> operatorToExecutionVertexMap,
               Map<String, ArrayList<ExecutionJobVertex>> targetVertices);

    protected ControlMessage getControlMessageForActiveGroup(
            int oldDOP, int newDOP,
            int activeGroup,
            Set<Integer> passiveGroups, String queryCategory,
            Set<Integer> queriesToBeMoved, boolean share,
            boolean[] querySetOfActiveGroup,
            HashMap<Integer, MIGRATION_TYPE_FROM_PASSIVE_QUERY> migrationTypesPassiveGroups,
            HashMap<String, ResourceID> taskToResourceIDMap,
            ArrayList<Integer> oldActiveChannelsForActiveGroup,
            ArrayList<Integer> activeChannelsForActiveGroup,
            HashMap<Integer, ArrayList<Integer>> oldActiveChannelsForPassiveGroup) {
        //Consumer<Object[]> callback = createConsumerWithCallbackFunction(controlMessageID);

        HashSet<String> senders = new HashSet<>(targetWorkers.size());
        HashSet<String> receivers = new HashSet<>(targetWorkers.size());
        getSendersAndReceivers(activeGroup, queryCategory, senders, receivers,
                oldActiveChannelsForActiveGroup, activeChannelsForActiveGroup, oldDOP, newDOP);

        HashMap<ResourceID, HashMap<String, Integer>> numOfSenders = getNumOfTasks(senders, taskToResourceIDMap);
        HashMap<ResourceID, HashMap<String, Integer>> numOfReceivers = getNumOfTasks(receivers, taskToResourceIDMap);
        HashSet<String> upstreamTasks = getUpstreamOperators(activeGroup, queryCategory);

        HashSet<String> tasksToChangeInputStatus = new HashSet<>();
        // add to TasksToChangeInputStatus the tasks that follow the upstreamTasks, and the target workers
        addTasksToChangeInputStatus(upstreamTasks, tasksToChangeInputStatus, activeGroup);
        addTasksToChangeInputStatus(targetWorkers.get(queryCategory).get(activeGroup), tasksToChangeInputStatus, activeGroup);

        HashSet<String> downstreamOperatorsOfInterest = new HashSet<>();
        Map<Integer, HashSet<String>> downstreamOperators =  getDownstreamOperators(activeGroup, queryCategory);
        for (int queryToBeMoved : queriesToBeMoved) {
            downstreamOperatorsOfInterest.addAll(downstreamOperators.getOrDefault(queryToBeMoved, emptySet));
            querySetOfActiveGroup[queryToBeMoved] = share;
        }

        HashSet<String> downstreamReceivers = share ?
                downstreamOperatorsOfInterest :
                emptySet;
        HashSet<String> downstreamSenders = share ?
                emptySet :
                downstreamOperatorsOfInterest;

        Set<String> sendersPassiveGroups = getSendersOfPassiveGroups(passiveGroups,
                migrationTypesPassiveGroups, queryCategory, oldActiveChannelsForPassiveGroup);
        // increase number of senders
        incNumSendersPassiveQuery(sendersPassiveGroups, numOfSenders, taskToResourceIDMap);

        HashMap<Integer, ResourceID> partitionToResourceIDMap = new HashMap<>();
        addResourceIDPerPartition(taskToResourceIDMap, receivers, partitionToResourceIDMap);
        addResourceIDPerPartition(taskToResourceIDMap, senders, partitionToResourceIDMap);

        HashMap<Integer, ResourceID> partitionToResourceIDMapPassive = new HashMap<>();
        addResourceIDPerPartition(taskToResourceIDMap, sendersPassiveGroups, partitionToResourceIDMapPassive);

        HashMap<Integer, ResourceID> partitionToResourceIDMapDownstreamSenders = new HashMap<>();
        addResourceIDPerPartition(taskToResourceIDMap, downstreamSenders, partitionToResourceIDMapDownstreamSenders);
        HashMap<Integer, ResourceID> partitionToResourceIDMapDownstreamReceivers = new HashMap<>();
        addResourceIDPerPartition(taskToResourceIDMap, downstreamReceivers, partitionToResourceIDMapDownstreamReceivers);

        Set<ResourceID> TMsOfReceivers = getTMsOfTasks(taskToResourceIDMap, receivers);
        Set<ResourceID> TMsOfSenders = getTMsOfTasks(taskToResourceIDMap, senders);
        if (!sendersPassiveGroups.isEmpty()) {
            Set<ResourceID> TMsOfSendersPassive = getTMsOfTasks(taskToResourceIDMap, sendersPassiveGroups);
            TMsOfSenders.addAll(TMsOfSendersPassive);
        }
        int numberOfSenderTMsInvolvedInMigration = TMsOfSenders.size();
        boolean multipleTMsInvolvedInMigration = TMsOfReceivers.size() > 1 || TMsOfSenders.size() > 1 ||
                !TMsOfReceivers.containsAll(TMsOfSenders);

        Set<ResourceID> TMsOfSendersDownstream = getTMsOfTasks(taskToResourceIDMap, downstreamSenders);
        Set<ResourceID> TMsOfReceiversDownstream = getTMsOfTasks(taskToResourceIDMap, downstreamReceivers);
        int numberOfSenderTMsInvolvedInMigrationDownstream = TMsOfSendersDownstream.size();
        boolean multipleTMsInvolvedInMigrationDownstream = TMsOfReceiversDownstream.size() > 1 ||
                TMsOfSendersDownstream.size() > 1 ||
                !TMsOfReceiversDownstream.containsAll(TMsOfSendersDownstream);

        HashMap<ResourceID, HashSet<String>> activeSourcesPerResourceID =
                getTasksPerResourceID(sources.get(activeGroup), taskToResourceIDMap);
        Set<String> sourcesOfPassiveGroups = new HashSet<>();
        for (int passiveGroup : passiveGroups) {
            sourcesOfPassiveGroups.addAll(sources.get(passiveGroup));
        }
        HashMap<ResourceID, HashSet<String>> passiveSourcesPerResourceID =
                getTasksPerResourceID(sourcesOfPassiveGroups, taskToResourceIDMap);
        HashMap<String, ResourceID> sourceToResourceIDMap = getTaskToResourceIDMapForTasks(
                sources.get(activeGroup), taskToResourceIDMap);

        // if querysetofactivegroup contains only one true value
        // then we set the enableMonitoring to false
        int numberOfQueriesInActiveGroup = 0;
        for (boolean queryActive : querySetOfActiveGroup) {
            if (queryActive) {
                numberOfQueriesInActiveGroup++;
            }
        }
        boolean enableSplitMonitoring = numberOfQueriesInActiveGroup > 1;

        System.out.println("ControlMessage of active group: " + activeGroup
                + " oldDOP: " + oldDOP
                + " newDOP: " + newDOP
                + " DOPmax: " + DOPmax
                + " share: " + share
                + " senders: [" + senders
                + "] receivers: [" + receivers
                + "] numOfSenders: " + numOfSenders
                + " numOfReceivers: " + numOfReceivers
                + " migrationTypePassiveQuery: " + migrationTypesPassiveGroups
                + " upstreamTasks: [" + upstreamTasks
                + "] tasksToChangeInputStatus: [" + tasksToChangeInputStatus
                + "] downstreamSenders: [" + downstreamSenders
                + "] downstreamReceivers: [" + downstreamReceivers
                + "] sourcesActive: [" + activeSourcesPerResourceID
                + "] sourcesPassive: [" + passiveSourcesPerResourceID
                + "] querySet: " + querySetOfActiveGroup
                + " activeChannelsForActiveGroup: " + activeChannelsForActiveGroup
                + " partitionToResourceIDMap: " + partitionToResourceIDMap
                + " partitionToResourceIDMapDownstreamSenders: " + partitionToResourceIDMapDownstreamSenders
                + " partitionToResourceIDMapDownstreamReceivers: " + partitionToResourceIDMapDownstreamReceivers
                + " numberOfSenderTMsInvolvedInMigration: " + numberOfSenderTMsInvolvedInMigration
                + " multipleTMsInvolvedInMigration: " + multipleTMsInvolvedInMigration
                + " numberOfSenderTMsInvolvedInMigrationDownstream: " + numberOfSenderTMsInvolvedInMigrationDownstream
                + " enableSplitMonitoring: " + enableSplitMonitoring);

        return new ReconfigurationControlMessage(graphWithMapping.get(activeGroup), senders,
                receivers, numOfSenders,
                numOfReceivers, null, null, emptySet,
                upstreamTasks, tasksToChangeInputStatus, downstreamSenders, downstreamReceivers,
                activeSourcesPerResourceID, passiveSourcesPerResourceID,
                sourceToResourceIDMap, querySetOfActiveGroup, oldDOP, newDOP, DOPmax, 0,
                0, share, driverPort, taskToResourceIDMap, partitionToResourceIDMap,
                partitionToResourceIDMapDownstreamSenders, partitionToResourceIDMapDownstreamReceivers,
                partitionToResourceIDMapPassive,
                numberOfSenderTMsInvolvedInMigration, multipleTMsInvolvedInMigration,
                numberOfSenderTMsInvolvedInMigrationDownstream, multipleTMsInvolvedInMigrationDownstream,
                true, activeChannelsForActiveGroup, null,
                activeGroup, enableSplitMonitoring);
    }

    protected ControlMessage getControlMessageForPassiveGroup(
            int oldDOPActive, int newDOPActive, int oldDOPPassive, int newDOPPassive,
            int activeGroup, int passiveGroup, Set<Integer> passiveGroups, String queryCategory,
            Set<Integer> queriesToBeMoved, boolean share,
            boolean[] querySetOfPassiveGroup,
            MIGRATION_TYPE_FROM_PASSIVE_QUERY migrationTypePassiveQuery,
            ReconfigurationControlMessage.RangeForMigration rangeForMigration,
            HashMap<String, ResourceID> taskToResourceIDMap,
            ArrayList<Integer> oldActiveChannelsForActiveGroup,
            ArrayList<Integer> activeChannelsForActiveGroup,
            ArrayList<Integer> activeChannelsForPassiveGroup,
            ArrayList<Integer> oldActiveChannelsForPassiveGroup) {
        //Consumer<Object[]> callback = createConsumerWithCallbackFunction(controlMessageID);

        HashSet<String> downstreamOperatorsOfInterest = new HashSet<>();
        Map<Integer, HashSet<String>> downstreamOperators =  getDownstreamOperators(passiveGroup, queryCategory);
        for (int queryToBeMoved : queriesToBeMoved) {
            downstreamOperatorsOfInterest.addAll(downstreamOperators.getOrDefault(queryToBeMoved, emptySet));
            querySetOfPassiveGroup[queryToBeMoved] = !share;
        }

        HashSet<String> downstreamReceivers = share ?
                emptySet :
                downstreamOperatorsOfInterest;
        HashSet<String> downstreamSenders = share ?
                downstreamOperatorsOfInterest :
                emptySet;

        HashSet<String> sendersPassiveQuery = new HashSet<>();
        HashMap<ResourceID, HashMap<String, Integer>> numOfSenders = null;
        HashMap<ResourceID, HashMap<String, Integer>> numOfReceivers = null;
        HashMap<Integer, ResourceID> partitionToResourceIDMap = new HashMap<>();
        HashMap<Integer, ResourceID> partitionToResourceIDMapPassive = new HashMap<>();
        int numberOfSenderTMsInvolvedInMigration = 0;
        boolean multipleTMsInvolvedInMigration = false;
        HashSet<String> upstreamTasks = emptySet;
        HashSet<String> tasksToChangeInputStatus = emptySet;
        if (migrationTypePassiveQuery != MIGRATION_TYPE_FROM_PASSIVE_QUERY.NO_MIGRATION) {
            HashSet<String> senders = new HashSet<>(targetWorkers.size());
            HashSet<String> receivers = new HashSet<>(targetWorkers.size());
            getSendersAndReceivers(
                    activeGroup, queryCategory, senders, receivers,
                    oldActiveChannelsForActiveGroup, activeChannelsForActiveGroup, oldDOPActive,
                    newDOPActive);
            numOfSenders = getNumOfTasks(senders, taskToResourceIDMap);
            numOfReceivers = getNumOfTasks(receivers, taskToResourceIDMap);
            getSendersPassive(passiveGroup, queryCategory, sendersPassiveQuery,
                    oldActiveChannelsForPassiveGroup);
            incNumSendersPassiveQuery(sendersPassiveQuery, numOfSenders, taskToResourceIDMap);
            addResourceIDPerPartition(taskToResourceIDMap, receivers, partitionToResourceIDMap);
            addResourceIDPerPartition(taskToResourceIDMap, senders, partitionToResourceIDMap);
            addResourceIDPerPartition(
                    taskToResourceIDMap,
                    sendersPassiveQuery,
                    partitionToResourceIDMapPassive);
            Set<ResourceID> TMsOfReceivers = getTMsOfTasks(taskToResourceIDMap, receivers);
            Set<ResourceID> TMsOfSenders = getTMsOfTasks(taskToResourceIDMap, senders);
            Set<ResourceID> TMsOfSendersPassive = getTMsOfTasks(taskToResourceIDMap, sendersPassiveQuery);
            TMsOfSenders.addAll(TMsOfSendersPassive);
            numberOfSenderTMsInvolvedInMigration = TMsOfSenders.size();
            multipleTMsInvolvedInMigration = TMsOfReceivers.size() > 1 || TMsOfSenders.size() > 1 ||
                    !TMsOfReceivers.containsAll(TMsOfSenders);
        }
        if (!share) {
            upstreamTasks = getUpstreamOperators(passiveGroup, queryCategory);
            tasksToChangeInputStatus = new HashSet<>();
            // add to TasksToChangeInputStatus the tasks that follow the upstreamTasks, and the target workers
            addTasksToChangeInputStatus(upstreamTasks, tasksToChangeInputStatus, passiveGroup);
            addTasksToChangeInputStatus(
                    targetWorkers.get(queryCategory).get(passiveGroup),
                    tasksToChangeInputStatus,
                    passiveGroup);
            // TODO? set senders, receivers, numOfSenders, numOfReceivers, DOPS!!, partitionToResourceIDMap,
            // numOfSenderTMsInvolvedInMigration, multipleTMsInvolvedInMigration, multipleTMsInvolvedInMigration,
            //activeChannelsPassiveGroup
        }

        HashMap<Integer, ResourceID> partitionToResourceIDMapDownstreamSenders = new HashMap<>();
        addResourceIDPerPartition(taskToResourceIDMap, downstreamSenders, partitionToResourceIDMapDownstreamSenders);
        HashMap<Integer, ResourceID> partitionToResourceIDMapDownstreamReceivers = new HashMap<>();
        addResourceIDPerPartition(taskToResourceIDMap, downstreamReceivers, partitionToResourceIDMapDownstreamReceivers);

        Set<ResourceID> TMsOfSendersDownstream = getTMsOfTasks(taskToResourceIDMap, downstreamSenders);
        Set<ResourceID> TMsOfReceiversDownstream = getTMsOfTasks(taskToResourceIDMap, downstreamReceivers);
        int numberOfSenderTMsInvolvedInMigrationDownstream = TMsOfSendersDownstream.size();
        boolean multipleTMsInvolvedInMigrationDownstream = TMsOfReceiversDownstream.size() > 1 || TMsOfSendersDownstream.size() > 1 ||
                !TMsOfReceiversDownstream.containsAll(TMsOfSendersDownstream);

        HashSet<String> allPassiveSources = new HashSet<>(16 * passiveGroups.size());
        for (int passiveGroupId : passiveGroups) {
            allPassiveSources.addAll(sources.get(passiveGroupId));
        }
        HashMap<ResourceID, HashSet<String>> passiveSourcesPerResourceID = getTasksPerResourceID(allPassiveSources, taskToResourceIDMap);
        HashMap<String, ResourceID> sourceToResourceIDMap = getTaskToResourceIDMapForTasks(allPassiveSources, taskToResourceIDMap);

        System.out.println("ControlMessage of passive group: " + passiveGroup
                + " oldDOPActive: " + oldDOPActive
                + " newDOPActive: " + newDOPActive
                + " DOPmax: " + DOPmax
                + " oldDOPPassive: " + oldDOPPassive
                + " newDOPPasive: " + newDOPPassive
                + " share: " + share
                + " numOfReceivers: [" + numOfReceivers
                + "] sendersPassiveQuery: [" + sendersPassiveQuery
                + "] downstreamSenders: [" + downstreamSenders
                + "] downstreamReceivers: [" + downstreamReceivers
                + "] sources: [" + sources.get(passiveGroup)
                + "] querySet: " + querySetOfPassiveGroup
                + " activeChannelsForActiveGroup: " + activeChannelsForActiveGroup
                + " activeChannelsForPassiveGroup: " + activeChannelsForPassiveGroup
                + " upstreamTasks: [" + upstreamTasks
                + "] tasksToChangeInputStatus: [" + tasksToChangeInputStatus
                + " partitionToResourceIDMap: " + partitionToResourceIDMap
                + " partitionToResourceIDMapDownstreamSenders: " + partitionToResourceIDMapDownstreamSenders
                + " partitionToResourceIDMapDownstreamReceivers: " + partitionToResourceIDMapDownstreamReceivers
                + " numberOfSenderTMsInvolvedInMigrationDownstream: " + numberOfSenderTMsInvolvedInMigrationDownstream
                + " partitionToResourceIDMapPassive: " + partitionToResourceIDMapPassive);

        return new ReconfigurationControlMessage(graphWithMapping.get(passiveGroup), emptySet, emptySet, numOfSenders,
                numOfReceivers, migrationTypePassiveQuery, rangeForMigration, sendersPassiveQuery,
                upstreamTasks, tasksToChangeInputStatus, downstreamSenders, downstreamReceivers,
                emptyMap, passiveSourcesPerResourceID, sourceToResourceIDMap,
                querySetOfPassiveGroup, oldDOPActive, newDOPActive, DOPmax, oldDOPPassive,
                newDOPPassive, share, driverPort, taskToResourceIDMap,
                partitionToResourceIDMap, partitionToResourceIDMapDownstreamSenders,
                partitionToResourceIDMapDownstreamReceivers, partitionToResourceIDMapPassive,
                numberOfSenderTMsInvolvedInMigration, multipleTMsInvolvedInMigration,
                numberOfSenderTMsInvolvedInMigrationDownstream, multipleTMsInvolvedInMigrationDownstream,
                false, activeChannelsForActiveGroup, activeChannelsForPassiveGroup,
                activeGroup, false);
    }

    protected ControlMessage getControlMessageForChangingDOP(
            int oldDOP, int newDOP, int group, String queryCategory,
            HashMap<String, ResourceID> taskToResourceIDMap, ArrayList<Integer> oldActiveChannelsForGroup,
            ArrayList<Integer> activeChannelsForGroup, boolean enableSplitMonitoring) {
        //Consumer<Object[]> callback = createConsumerWithCallbackFunction(controlMessageID);

        HashSet<String> senders = new HashSet<>(targetWorkers.size());
        HashSet<String> receivers = new HashSet<>(targetWorkers.size());
        getSendersAndReceivers(group, queryCategory, senders, receivers,
                oldActiveChannelsForGroup, activeChannelsForGroup, oldDOP, newDOP);
        HashMap<ResourceID, HashMap<String, Integer>> numOfSenders = getNumOfTasks(senders, taskToResourceIDMap);
        HashMap<ResourceID, HashMap<String, Integer>> numOfReceivers = getNumOfTasks(receivers, taskToResourceIDMap);
        HashSet<String> upstreamTasks = getUpstreamOperators(group, queryCategory);

        HashSet<String> tasksToChangeInputStatus = new HashSet<>();
        // add to TasksToChangeInputStatus the tasks that follow the upstreamTasks, and the target workers
        addTasksToChangeInputStatus(upstreamTasks, tasksToChangeInputStatus, group);
        addTasksToChangeInputStatus(targetWorkers.get(queryCategory).get(group), tasksToChangeInputStatus, group);

        HashMap<Integer, ResourceID> partitionToResourceIDMap = new HashMap<>();
        addResourceIDPerPartition(taskToResourceIDMap, receivers, partitionToResourceIDMap);
        addResourceIDPerPartition(taskToResourceIDMap, senders, partitionToResourceIDMap);

        Set<ResourceID> TMsOfReceivers = getTMsOfTasks(taskToResourceIDMap, receivers);
        Set<ResourceID> TMsOfSenders = getTMsOfTasks(taskToResourceIDMap, senders);
        int numberOfSenderTMsInvolvedInMigration = TMsOfSenders.size();
        boolean multipleTMsInvolvedInMigration = TMsOfReceivers.size() > 1 || TMsOfSenders.size() > 1 ||
                !TMsOfReceivers.containsAll(TMsOfSenders);

        System.out.println("ControlMessage for changing the parallelism of group: " + group
                + " oldDOP: " + oldDOP
                + " newDOP: " + newDOP
                + " DOPmax: " + DOPmax
                + " senders: [" + senders
                + "] receivers: [" + receivers
                + "] numOfSenders: " + numOfSenders
                + " numOfReceivers: " + numOfReceivers
                + " upstreamTasks: [" + String.join(", ", upstreamTasks)
                + "] tasksToChangeInputStatus: [" + String.join(", ", tasksToChangeInputStatus) + "]"
                + " partitionToResourceIDMap: " + partitionToResourceIDMap
                + " numberOfSenderTMsInvolvedInMigration: " + numberOfSenderTMsInvolvedInMigration
                + " multipleTMsInvolvedInMigration: " + multipleTMsInvolvedInMigration
                + " activeChannels: " + activeChannelsForGroup);


        return new ReconfigurationControlMessage(graphWithMapping.get(group), senders, receivers, numOfSenders, numOfReceivers,
                MIGRATION_TYPE_FROM_PASSIVE_QUERY.NO_MIGRATION, null, emptySet,
                upstreamTasks, tasksToChangeInputStatus, emptySet, emptySet,
                emptyMap, emptyMap, null, null, oldDOP, newDOP, DOPmax,
                0, 0, false, driverPort, taskToResourceIDMap,
                partitionToResourceIDMap, null,
                null, null,
                numberOfSenderTMsInvolvedInMigration, multipleTMsInvolvedInMigration,
                0,
                false, true, activeChannelsForGroup,
                null, group, enableSplitMonitoring);
    }

    protected void sendControlMessage(
            ControlMessage controlMessage, int group,
            Map<String, ExecutionVertex> operatorToExecutionVertexMap,
            List<CompletableFuture<?>> futures,
            List<Long> roundtripTimes) {
        for (String sourceWorkerName : FriesAlg.getSources(graphWithMapping.get(group))) {
            ExecutionVertex worker = operatorToExecutionVertexMap.get(sourceWorkerName);
            long currentTime = System.currentTimeMillis();
            futures.add(((ReconfigurableExecutionVertex) worker).sendControlMessage(controlMessage).thenRun(() -> {
                long finishedTime = System.currentTimeMillis();
                roundtripTimes.add(finishedTime - currentTime);
            }));
        }
    }

    protected int getNumOfQueries() {
        return graphWithMapping.size();
    }

    // returns (QueryId -> DownstreamTasks)
    // i.e., the downstream tasks of each query
    protected Map<Integer, HashSet<String>> getDownstreamOperators(int groupId, String queryCategory) {
        Map<Integer, HashSet<String>> result = new HashMap<>();

        for (String worker : targetWorkers.get(queryCategory).get(groupId)) {
            findDownstream(worker, graphWithMapping.get(groupId), result);
        }
        return result;
    }

    protected void printRoundTripTime(List<CompletableFuture<?>> futures,
                                              List<Long> roundtripTimes, String controlMsgInfo) {
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).thenRun(() -> {
            double averageTime = roundtripTimes.stream().mapToLong(Long::longValue).average().orElse(0.0);
            System.out.println(controlMsgInfo + " average roundtrip time = " + averageTime);
        });
    }

    protected HashMap<String, ResourceID> getTaskToResourceIDMap(
            Map<String, ExecutionVertex> operatorToExecutionVertexMap) {
        // map from operator name to TaskManagerLocation
        HashMap<String, ResourceID> taskToResourceIDMap = new HashMap<>();
        for (Map.Entry<String, ExecutionVertex> entry : operatorToExecutionVertexMap.entrySet()) {
            taskToResourceIDMap.put(entry.getKey(), entry.getValue().getCurrentAssignedResourceLocation().getResourceID());
        }
        return taskToResourceIDMap;
    }

    // input: 2 queries
    protected MIGRATION_TYPE_FROM_PASSIVE_QUERY getMigrationTypeFromPassive(
            int activeQuery, int passiveQuery, String queryCategory) {

        if (!filtersPerQuery.containsKey(activeQuery) || !filtersPerQuery.containsKey(passiveQuery)) {
            assert !filtersPerQuery.containsKey(activeQuery) && !filtersPerQuery.containsKey(passiveQuery) :
                    "Only one of the queries has filters";
            return MIGRATION_TYPE_FROM_PASSIVE_QUERY.NO_MIGRATION;
        }

        if (filtersPerQuery.get(activeQuery).start <= filtersPerQuery.get(passiveQuery).start &&
                filtersPerQuery.get(activeQuery).end >= filtersPerQuery.get(passiveQuery).end) {
            return MIGRATION_TYPE_FROM_PASSIVE_QUERY.NO_MIGRATION;
        } else if (filterOnAttributeUsedToOrganizeState.get(queryCategory)) {
            return MIGRATION_TYPE_FROM_PASSIVE_QUERY.KEY_RANGE;
        } else {
            return MIGRATION_TYPE_FROM_PASSIVE_QUERY.ENTIRE_STATE;
        }
    }

    // input: 2 queries
    protected ReconfigurationControlMessage.RangeForMigration getRangeForMigration(int activeQuery, int passiveQuery) {
        long start = -1, end = -1, start2 = -1, end2 = -1;
        if (filtersPerQuery.get(activeQuery).start > filtersPerQuery.get(passiveQuery).start) {
            start = filtersPerQuery.get(passiveQuery).start;
            end = filtersPerQuery.get(activeQuery).start - 1;
        }
        if (filtersPerQuery.get(activeQuery).end < filtersPerQuery.get(passiveQuery).end) {
            start2 = filtersPerQuery.get(activeQuery).end + 1;
            end2 = filtersPerQuery.get(passiveQuery).end;
        }
        return new ReconfigurationControlMessage.RangeForMigration(start, end, start2, end2);
    }

    protected MIGRATION_TYPE_FROM_PASSIVE_QUERY getMigrationTypeFromPassive(
            HashSet<Integer> activeGroup, HashSet<Integer> passiveGroup, String queryCategory) {
        if (areRangesContained(activeGroup, passiveGroup)) {
            return MIGRATION_TYPE_FROM_PASSIVE_QUERY.NO_MIGRATION;
        } else if (filterOnAttributeUsedToOrganizeState.get(queryCategory)) {
            return MIGRATION_TYPE_FROM_PASSIVE_QUERY.KEY_RANGE;
        } else {
            return MIGRATION_TYPE_FROM_PASSIVE_QUERY.ENTIRE_STATE;
        }
    }

    protected List<DataRange> mergeQueryRanges(
            Set<Integer> queryIds) {

        // Extract ranges for the specified queries
        List<DataRange> ranges = new ArrayList<>();
        for (Integer queryId : queryIds) {
            DataRange range = new DataRange(filtersPerQuery.get(queryId).start, filtersPerQuery.get(queryId).end);
            if (range != null) {
                ranges.add(range);
            }
        }
        // If no valid ranges found, return empty list
        if (ranges.isEmpty()) {
            return Collections.emptyList();
        }

        // Sort ranges by start value
        ranges.sort((a, b) -> Long.compare(a.start, b.start));

        // Merge overlapping ranges
        List<DataRange> mergedRanges = new ArrayList<>();
        DataRange currentRange = ranges.get(0);

        for (int i = 1; i < ranges.size(); i++) {
            DataRange nextRange = ranges.get(i);

            // If current and next range overlap or are adjacent
            if (currentRange.end >= nextRange.start - 1) {
                // Merge the ranges by updating the end point
                currentRange.end = Math.max(currentRange.end, nextRange.end);
            } else {
                // No overlap, add current range to result and move to next
                mergedRanges.add(currentRange);
                currentRange = nextRange;
            }
        }

        // Add the last range
        mergedRanges.add(currentRange);

        return mergedRanges;
    }

    private boolean areRangesContained(
            Set<Integer> setA,
            Set<Integer> setB) {

        // Get merged ranges for setA
        List<DataRange> mergedRangesA = mergeQueryRanges(setA);
        System.out.println(mergedRangesA);

        // For each query in setB, check if its range is contained in any of the merged ranges from setA
        for (Integer queryId : setB) {
            DataRange rangeB = filtersPerQuery.get(queryId);
            if (rangeB == null) continue;

            boolean isContained = false;
            for (DataRange mergedRange : mergedRangesA) {
                if (mergedRange.start <= rangeB.start && mergedRange.end >= rangeB.end) {
                    isContained = true;
                    break;
                }
            }

            if (!isContained) {
                return false;
            }
        }

        return true;
    }

    private HashMap<ResourceID, HashSet<String>> getTasksPerResourceID(Set<String> tasks,
                                                                       Map<String, ResourceID> taskToResourceIDMap) {
        HashMap<ResourceID, HashSet<String>> result = new HashMap<>();
        for (String source : tasks) {
            ResourceID resourceID = taskToResourceIDMap.get(source);
            result.computeIfAbsent(resourceID, k -> new HashSet<>()).add(source);
        }
        return result;
    }

    private HashMap<String, ResourceID> getTaskToResourceIDMapForTasks(
            Set<String> tasks, Map<String, ResourceID> taskToResourceIDMap) {
        HashMap<String, ResourceID> result = new HashMap<>();
        for (String task : tasks) {
            result.put(task, taskToResourceIDMap.get(task));
        }
        return result;
    }

    private void addResourceIDPerPartition(Map<String, ResourceID> taskToResourceIDMap,
                                           Set<String> receivers,
                                           Map<Integer, ResourceID> partitionToResourceID) {
        if (receivers != null) {
            for (String task : taskToResourceIDMap.keySet()) {
                if (receivers.contains(task)) {
                    int partition = Integer.parseInt(task.split("-")[1]);
                    partitionToResourceID.put(partition, taskToResourceIDMap.get(task));
                }
            }
        }
    }

    private Set<ResourceID> getTMsOfTasks(Map<String, ResourceID> taskToResourceIDMap,
                                              Set<String> tasks) {
        Set<ResourceID> result = new HashSet<>();
        for (String task : tasks) {
            result.add(taskToResourceIDMap.get(task));
        }
        return result;
    }

    /**
     * This function is being used when the migration involves senders from the passive query.
     * It adds to the numOfSenders map the number of passive senders.
     * @param sendersPassiveQuery
     * @param numOfSenders
     * @param taskToResourceIDMap
     */
    private void incNumSendersPassiveQuery(Set<String> sendersPassiveQuery,
                                           HashMap<ResourceID, HashMap<String, Integer>> numOfSenders,
                                           Map<String, ResourceID> taskToResourceIDMap) {
        for (String task : sendersPassiveQuery) {
            String[] temp = task.split("GID");
            String taskName = temp[0];
            ResourceID resourceID = taskToResourceIDMap.get(task);
            numOfSenders.computeIfAbsent(resourceID, k -> new HashMap<>());
            Map<String, Integer> innerMap = numOfSenders.get(resourceID);
            innerMap.put(taskName, innerMap.getOrDefault(taskName, 0) + 1);
        }
    }

    private HashMap<ResourceID, HashMap<String, Integer>> getNumOfTasks(Set<String> tasks,
                                                                        Map<String, ResourceID> taskToResourceIDMap) {
        HashMap<ResourceID, HashMap<String, Integer>> result = new HashMap<>();
        for (String task : tasks) {
            String[] temp = task.split("GID");
            String taskName = temp[0];
            ResourceID resourceID = taskToResourceIDMap.get(task);
            Map<String, Integer> innerMap = result.computeIfAbsent(resourceID, k -> new HashMap<>());
            innerMap.put(taskName, innerMap.getOrDefault(taskName, 0) + 1);
        }
        return result;
    }

    private void getSendersAndReceivers(
            int groupId, String queryCategory, Set<String> senders,
            Set<String> receivers, ArrayList<Integer> oldIndexToChannel,
            ArrayList<Integer> indexToChannel, int oldDOP, int newDOP) {
        for (String worker : targetWorkers.get(queryCategory).get(groupId)) {
            if (worker.contains("Stateful")) {
                String[] temp = worker.split("-");
                int channel = Integer.parseInt(temp[temp.length - 1]);
                int channelIndexOld = oldIndexToChannel.indexOf(channel);
                if (channelIndexOld >= 0 && channelIndexOld < oldDOP) {
                    senders.add(worker);
                }
                int channelIndex = indexToChannel.indexOf(channel);
                if (channelIndex >= 0 && channelIndex < newDOP) {
                    receivers.add(worker);
                }
            }
        }
    }

    private void getSendersPassive(int groupId, String queryCategory,
                                   Set<String> senders, ArrayList<Integer> indexToChannel) {
        if (indexToChannel == null) {
            return;
        }
        for (String worker : targetWorkers.get(queryCategory).get(groupId)) {
            if (worker.contains("Stateful")) {
                String[] temp = worker.split("-");
                int channel = Integer.parseInt(temp[temp.length - 1]);
                if (indexToChannel.contains(channel)) {
                    senders.add(worker);
                }
            }
        }
    }

    private HashSet<String> getUpstreamOperators(int groupId, String queryCategory) {
        HashSet<String> result = new HashSet<>();
        for (Map.Entry<String, HashSet<String>> entry : graphWithMapping.get(groupId).entrySet()) {
            String task = entry.getKey();
            Set<String> downstreamTasks = entry.getValue();
            if (isUpstream(downstreamTasks, graphWithMapping.get(groupId),
                    targetWorkers.get(queryCategory).get(groupId))) {
                result.add(task);
            }
        }
        return result;
    }

    private boolean isUpstream(Set<String> downstreamTasks,
                               HashMap<String, HashSet<String>> queryGraph,
                               Set<String> targetWorkers) {
        if (downstreamTasks.isEmpty() || targetWorkers == null) {
            return false;
        }
        for (String downstream : downstreamTasks) {
            if (targetWorkers != null && targetWorkers.contains(downstream)) {
                return true;
            }
        }
        for (String downstream : downstreamTasks) {
            if (isUpstream(queryGraph.get(downstream), queryGraph, targetWorkers)) {
                return true;
            }
        }
        return false;
    }

    private void findDownstream(String root, HashMap<String, HashSet<String>> queryGraph,
                                Map<Integer, HashSet<String>> downstreamOfGroup) {
        for (String downstreamTask : queryGraph.get(root)) {
            String[] temp = downstreamTask.split("GID")[0].split("QID");
            // if temp.length <= 1, this means that the task is still in the shared part of the query plan
            if (temp.length > 1) {
                int queryId = Integer.parseInt(temp[1]);
                downstreamOfGroup.computeIfAbsent(queryId, k -> new HashSet<>());
                if (!downstreamOfGroup.get(queryId).contains(downstreamTask)) {
                    if (downstreamTask.contains("Stateful")) {
                        downstreamOfGroup.get(queryId).add(downstreamTask);
                    }
                    findDownstream(downstreamTask, queryGraph, downstreamOfGroup);
                }
            }
        }
    }

    private void addTasksToChangeInputStatus(Set<String> upstreamTasks,
                                             Set<String> tasksToChangeInputStatus, int groupId) {
        for (String upstreamTask : upstreamTasks) {
            if (!upstreamTask.startsWith("Source")) {
                for (String downstreamTask : graphWithMapping.get(groupId).get(upstreamTask)) {
                    if (!tasksToChangeInputStatus.contains(downstreamTask)) {
                        tasksToChangeInputStatus.add(downstreamTask);
                    }
                }
            }
        }
    }

    private Consumer<Object[]> createConsumerWithCallbackFunction(int controlMessageID) {
        return new Consumer<Object[]>() {
            @Override
            public void accept(Object[] t) {
                System.setProperty(t[2].toString() + "-" + t[1].toString(), String.valueOf(controlMessageID));
                System.out.println("Job " + jobID + " received iteration(" + t[2] + "-" + t[1] + ") " + controlMessageID + " time=" + System.currentTimeMillis());
            }
        };
    }

    private Set<String> getSendersOfPassiveGroups(
            Set<Integer> passiveGroups,
            HashMap<Integer, MIGRATION_TYPE_FROM_PASSIVE_QUERY> migrationTypesPassiveQuery,
            String queryCategory, HashMap<Integer, ArrayList<Integer>> oldActiveChannelsForPassiveGroup) {
        Set<String> sendersPassiveGroups = new HashSet<>();
        for (int passiveGroup : passiveGroups) {
            if (migrationTypesPassiveQuery.get(passiveGroup) != MIGRATION_TYPE_FROM_PASSIVE_QUERY.NO_MIGRATION) {
                getSendersPassive(passiveGroup, queryCategory,
                        sendersPassiveGroups, oldActiveChannelsForPassiveGroup.getOrDefault(passiveGroup, null));
            }
        }
        return sendersPassiveGroups;
    }
}
