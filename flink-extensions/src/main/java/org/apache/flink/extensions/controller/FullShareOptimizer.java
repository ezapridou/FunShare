package org.apache.flink.extensions.controller;

import net.michaelkoepf.spegauge.api.sut.DataRange;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;

import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * This optimizer reconfigures the query groups using a fixed interval.
 * Disclaimer: This optimizer contains hardcoded logic.
 * E.g. it assumes the active group is group 0.
 * It assumes there is enough data for all the iterations to complete.
 * Use this optimizer for testing purposes.
 *
 * This optimizer also works for queries with filters.
 */
public class FullShareOptimizer extends Optimizer {
    private transient int DOPinIsolation;
    private transient int iterations;
    private transient boolean repeated;
    private transient int initialDelay;
    private transient int interval;

    public void init(String jobID,
                     HashMap<Integer, HashMap<String, HashSet<String>>> graphWithMapping,
                     HashMap<String,ExecutionVertex> operatorToExecutionVertexMap,
                     HashMap<String, ArrayList<ExecutionJobVertex>> targetVertices,
                     HashMap<String, HashMap<Integer, HashSet<String>>> targetWorkers,
                     HashMap<String, ArrayList<Integer>> queryCategories,
                     HashMap<Integer, DataRange> filtersPerQuery,
                     HashMap<Integer, HashSet<String>> sources,
                     int DOPinIsolation, int DOPmax, int driverPort,
                     int iterations, boolean repeated, int initialDelay, int interval) {
        init(jobID, graphWithMapping, targetWorkers, queryCategories, filtersPerQuery, sources,
                DOPmax, driverPort);
        this.DOPinIsolation = DOPinIsolation;
        this.iterations = iterations;
        this.repeated = repeated;
        this.initialDelay = initialDelay;
        this.interval = interval;
        start(operatorToExecutionVertexMap, targetVertices);
    }

    @Override
    public void start(Map<String, ExecutionVertex> operatorToExecutionVertexMap,
                      Map<String, ArrayList<ExecutionJobVertex>> targetVertices) {

        assert iterations <= getNumOfQueries();

        int numOfQueries = getNumOfQueries();
        int sharingDOP = DOPinIsolation * numOfQueries;

        int activeGroup = 0;
        int activeQuery = 0;

        boolean[][] querySets = new boolean[numOfQueries][numOfQueries];
        for (int i = 0; i < numOfQueries; i++) {
            querySets[i][i] = true;
        }

        String queryCategory = targetWorkers.keySet().iterator().next();

        ArrayList<Integer> activeChannelsActiveQuery = new ArrayList<>();

        for (int i = 0; i < DOPinIsolation; i++) {
            activeChannelsActiveQuery.add(i);
        }

        Timer t = new Timer();
        TimerTask task = new TimerTask() {
            private int iteration = 0;

            @Override
            public void run() {
                int currentIteration = iteration;
                iteration++;

                if (currentIteration >= iterations) {
                    this.cancel();
                }

                List<CompletableFuture<?>> futures1 = new ArrayList<>();
                List<Long> roundtripTimes1 = new ArrayList<>();

                List<CompletableFuture<?>> futures2 = new ArrayList<>();
                List<Long> roundtripTimes2 = new ArrayList<>();

                int passiveQuery = currentIteration;
                int passiveGroup = currentIteration;

                MIGRATION_TYPE_FROM_PASSIVE_QUERY migrationTypePassiveQuery;
                ReconfigurationControlMessage.RangeForMigration rangeForMigration = null;

                // map from operator name to TaskManagerLocation
                HashMap<String, ResourceID> taskToResourceIDMap = getTaskToResourceIDMap(operatorToExecutionVertexMap);

                if (currentIteration == 0) {
                    // change parallelism of active group
                    int oldDOP = numOfQueries * DOPinIsolation;
                    int newDOP = DOPinIsolation;
                    ArrayList<Integer> oldActiveChannels = new ArrayList<>(oldDOP);
                    for (int i = 0; i < oldDOP; i++) {
                        oldActiveChannels.add(i);
                    }
                    ControlMessage controlMessage = getControlMessageForChangingDOP(oldDOP, newDOP, activeGroup,
                            queryCategory, taskToResourceIDMap, oldActiveChannels, activeChannelsActiveQuery, false);
                    sendControlMessage(controlMessage, activeGroup, operatorToExecutionVertexMap, futures1, roundtripTimes1);
                } else if (currentIteration < iterations) {
                    int queryToBeMoved = passiveQuery;
                    Set<Integer> queriesToBeMoved = new HashSet<>(1);
                    queriesToBeMoved.add(queryToBeMoved);

                    // count the true values in querySets[activeQuery]
                    int pastNumOfQueries = 0;
                    for (int i = 0; i < numOfQueries; i++) {
                        if (querySets[activeQuery][i]) {
                            pastNumOfQueries++;
                        }
                    }
                    int oldDOPActive = pastNumOfQueries * DOPinIsolation;
                    int newDOPActive = oldDOPActive + DOPinIsolation;
                    int oldDOPPassive = DOPinIsolation;
                    int newDOPPassive = DOPinIsolation;
                    boolean share = true;
                    migrationTypePassiveQuery = getMigrationTypeFromPassive(activeQuery, passiveQuery, queryCategory);

                    if (migrationTypePassiveQuery == MIGRATION_TYPE_FROM_PASSIVE_QUERY.KEY_RANGE) {
                        rangeForMigration = getRangeForMigration(activeQuery, passiveQuery);
                    }

                    ArrayList<Integer> oldActiveChannelsPassiveGroup = new ArrayList<>(oldDOPPassive);
                    ArrayList<Integer> oldActiveChannelsActiveQuery = new ArrayList<>(activeChannelsActiveQuery);
                    int nextActiveChannel = activeChannelsActiveQuery.get(activeChannelsActiveQuery.size() - 1) + 1;
                    for (int i = 0; i < DOPinIsolation; i++) {
                        activeChannelsActiveQuery.add(nextActiveChannel);
                        oldActiveChannelsPassiveGroup.add(nextActiveChannel);
                        nextActiveChannel++;
                    }
                    // only one passive group
                    HashMap<Integer, Integer> oldDOPsPassive = new HashMap<>();
                    oldDOPsPassive.put(passiveQuery, oldDOPPassive);
                    HashSet<Integer> passiveGroups = new HashSet<>();
                    passiveGroups.add(passiveGroup);
                    HashMap<Integer, MIGRATION_TYPE_FROM_PASSIVE_QUERY> migrationTypesPassiveGroups = new HashMap<>();
                    migrationTypesPassiveGroups.put(passiveQuery, migrationTypePassiveQuery);
                    HashMap<Integer, ArrayList<Integer>> oldActiveChannelsPassiveGroups = new HashMap<>();
                    oldActiveChannelsPassiveGroups.put(passiveQuery, oldActiveChannelsPassiveGroup);

                    ControlMessage controlMessageActiveGroup = getControlMessageForActiveGroup(
                            oldDOPActive, newDOPActive, activeGroup,
                            passiveGroups, queryCategory, queriesToBeMoved, share, querySets[activeQuery],
                            migrationTypesPassiveGroups, taskToResourceIDMap, oldActiveChannelsActiveQuery,
                            activeChannelsActiveQuery, oldActiveChannelsPassiveGroups);
                    sendControlMessage(controlMessageActiveGroup, activeGroup,
                            operatorToExecutionVertexMap, futures1, roundtripTimes1);
                    ControlMessage controlMessagePassiveGroup = getControlMessageForPassiveGroup(
                            oldDOPActive, newDOPActive, oldDOPPassive, newDOPPassive, activeGroup,
                            passiveGroup, passiveGroups, queryCategory, queriesToBeMoved, share,
                            querySets[passiveQuery],
                            migrationTypePassiveQuery, rangeForMigration, taskToResourceIDMap,
                            oldActiveChannelsActiveQuery,
                            activeChannelsActiveQuery, null, oldActiveChannelsPassiveGroup);
                    sendControlMessage(controlMessagePassiveGroup, passiveGroup,
                            operatorToExecutionVertexMap, futures2, roundtripTimes2);
                }

                printRoundTripTime(futures1, roundtripTimes1, "[Active Group]");
                if (currentIteration > 0 && currentIteration < iterations) {
                    printRoundTripTime(futures2, roundtripTimes2, "[Passive Group]");
                }
                System.out.println("Job " + jobID + " sent iteration " + currentIteration + " time=" + System.currentTimeMillis());
            }
        };

        if (!targetWorkers.isEmpty()) {
            if (repeated) {
                t.schedule(task, initialDelay, interval);
            } else {
                t.schedule(task, initialDelay);
            }
        }
    }
}
